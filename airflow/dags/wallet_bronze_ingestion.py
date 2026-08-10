"""Bronze ingestion DAG for the Digital Wallet platform.

Deliberately thin: every task calls a function that already exists in
ingestion/main.py. No pipeline logic lives here.

TRANSACTIONS FALLBACK — made visible in the Airflow UI
----------------------------------------------------------------------
main.py's run_transactions_pipeline() tries PostgreSQL first, falls back
to the API only if Postgres fails. Wrapping that whole function as one
task (the simpler option) hides the fallback inside a single box in the
Airflow graph.

This version exposes it as two tasks instead:

  postgres_transactions_task            (tries Postgres; may fail)
        |
        v  trigger_rule="all_failed"
  api_transactions_task                 (only runs if Postgres failed)
        |
        v  trigger_rule="one_success"  (fed by BOTH tasks above)
  transactions_complete

If Postgres succeeds: postgres_transactions_task is green,
api_transactions_task is skipped (grey), transactions_complete is green.

If Postgres fails and API succeeds: postgres_transactions_task is RED
(this is expected, not a bug — it's showing you the fallback actually
fired), api_transactions_task is green, transactions_complete is green.
Airflow computes overall DAG run success from leaf tasks, so a red
non-leaf task here does not fail the DAG run.

If both fail: transactions_complete has no successful upstream and is
marked upstream_failed — the DAG run fails, matching
run_transactions_pipeline() returning False.
"""

from __future__ import annotations

import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# Paths as seen INSIDE the container (see docker-compose.yml volume mounts).
DBT_PROJECT_DIR = "/opt/airflow/project/dbt/wallet_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profile"

from ingestion.config import get_databricks_config, get_runtime_config
from ingestion.databricks_writer import BronzeWriter
from ingestion.main import (
    run_api_pipeline,
    run_csv_pipeline,
    run_postgres_pipeline,
    run_postgres_transactions_pipeline,
)
from ingestion.utils import WatermarkStore


default_args = {
    "owner": "wallet-data-platform",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}


def _writer() -> BronzeWriter:
    return BronzeWriter(config=get_databricks_config())


def _watermark_store() -> WatermarkStore:
    return WatermarkStore(state_dir=get_runtime_config().state_dir)


@dag(
    dag_id="wallet_bronze_ingestion",
    description="CSV -> PostgreSQL -> Transactions (Postgres primary, API fallback) into Bronze",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bronze", "wallet-data-platform"],
)
def wallet_bronze_ingestion():

    @task
    def ensure_schema(batch_id: str) -> str:
        _writer().ensure_schema_exists()
        return batch_id

    @task
    def csv_task(batch_id: str) -> None:
        if not run_csv_pipeline(_writer(), batch_id):
            raise AirflowException("CSV ingestion failed")

    @task
    def postgres_task(batch_id: str) -> None:
        if not run_postgres_pipeline(_writer(), _watermark_store(), batch_id):
            raise AirflowException("PostgreSQL reference-table ingestion failed")

    @task
    def postgres_transactions_task(batch_id: str) -> None:
        if not run_postgres_transactions_pipeline(_writer(), _watermark_store(), batch_id):
            raise AirflowException("PostgreSQL transactions ingestion failed — see task log")

    @task(trigger_rule=TriggerRule.ALL_FAILED)
    def api_transactions_task(batch_id: str) -> None:
        """Only runs if postgres_transactions_task failed — this IS the
        fallback, made visible instead of hidden inside one function call.
        """
        if not run_api_pipeline(_writer(), _watermark_store(), batch_id):
            raise AirflowException("API fallback for transactions also failed")

    @task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def transactions_complete() -> None:
        """Leaf task. Succeeds if EITHER upstream task succeeded — this is
        what makes the overall DAG run count as successful when the
        fallback path was the one that actually worked.
        """
        return None

    # dbt CLI commands run with --project-dir/--profiles-dir explicit
    # rather than relying on cwd, since Airflow's execution directory
    # isn't guaranteed. --full-refresh is intentionally NOT passed here —
    # normal runs should use fact_transactions' incremental/merge logic;
    # a full refresh is a manual, deliberate action, not a scheduled one.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"dbt snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    batch_id = "{{ dag_run.run_id }}"

    ready = ensure_schema(batch_id)
    csv_result = csv_task(batch_id)
    postgres_result = postgres_task(batch_id)
    pg_txn = postgres_transactions_task(batch_id)
    api_txn = api_transactions_task(batch_id)
    done = transactions_complete()

    ready >> csv_result >> postgres_result >> pg_txn
    pg_txn >> api_txn
    [pg_txn, api_txn] >> done

    # Snapshots must run BEFORE dbt run, since dim_* models (SCD2) read
    # from the snapshot tables (customers_snapshot, wallet_accounts_snapshot,
    # branches_snapshot, merchants_snapshot) — running models first would
    # build Gold dimensions against stale/missing snapshot history.
    done >> dbt_snapshot >> dbt_run >> dbt_test


wallet_bronze_ingestion()