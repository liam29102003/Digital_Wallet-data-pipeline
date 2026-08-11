"""Bronze ingestion + dbt DAG for the Digital Wallet platform.

Deliberately thin: every ingestion task calls a function that already
exists in ingestion/main.py. No pipeline logic lives here.

TRANSACTIONS FALLBACK — made visible in the Airflow UI
----------------------------------------------------------------------
main.py's run_transactions_pipeline() tries PostgreSQL first, falls back
to the API only if Postgres fails. This DAG exposes that as two tasks:

  postgres_transactions_task            (tries Postgres; may fail)
        |
        v  trigger_rule="all_failed"
  api_transactions_task                 (only runs if Postgres failed)
        |
        v  trigger_rule="one_success"  (fed by BOTH tasks above)
  transactions_complete

A red postgres_transactions_task with a green api_transactions_task is
expected behavior (the fallback firing), not a bug — Airflow computes
DAG run success from leaf tasks, so this does not fail the DAG run.

DBT STAGE ORDERING (after ingestion completes)
----------------------------------------------------------------------
  dbt_run_staging (tag:silver) -> dbt_snapshot -> dbt_run_gold (tag:gold) -> dbt_test

Staging runs first because dbt_snapshot's SQL ref()s the staging views —
those must physically exist first, including on a brand-new catalog.
Gold dimensions read from snapshot tables, so dbt_run_gold must follow
dbt_snapshot, using this run's freshly-captured history.

OBSERVABILITY
----------------------------------------------------------------------
Every ingestion task and every dbt stage writes one row into
observability.pipeline_run_log (see ingestion/metrics_writer.py) — a
permanent, queryable run history, distinct from Airflow's own transient
task-status UI and from dbt's own console output. dbt stages are parsed
from dbt's run_results.json artifact immediately after each BashOperator
runs, since dbt overwrites that file on every invocation.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from ingestion.config import get_databricks_config, get_runtime_config
from ingestion.databricks_writer import BronzeWriter
from ingestion.main import (
    run_api_pipeline,
    run_csv_pipeline,
    run_postgres_pipeline,
    run_postgres_transactions_pipeline,
)
from ingestion.metrics_writer import MetricsWriter, PipelineRunMetric
from ingestion.utils import WatermarkStore

# Paths as seen INSIDE the container (see docker-compose.yml volume mounts).
DBT_PROJECT_DIR = "/opt/airflow/project/dbt/wallet_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profile"
DBT_RUN_RESULTS_PATH = Path(DBT_PROJECT_DIR) / "target" / "run_results.json"

default_args = {
    "owner": "wallet-data-platform",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}


def _writer() -> BronzeWriter:
    return BronzeWriter(config=get_databricks_config())


def _watermark_store() -> WatermarkStore:
    return WatermarkStore(state_dir=get_runtime_config().state_dir)


def _metrics() -> MetricsWriter:
    return MetricsWriter(bronze_writer=_writer())


def _log_dbt_stage_metrics(batch_id: str, pipeline_name: str, started_at: datetime.datetime) -> None:
    """Parse dbt's own run_results.json (written fresh by every dbt
    invocation) and persist a summarized PipelineRunMetric row. Runs
    with trigger_rule=ALL_DONE on the calling task so a failed dbt stage
    still gets its failure counts captured, not just silently skipped.
    """
    if not DBT_RUN_RESULTS_PATH.exists():
        # dbt didn't even get far enough to write results (e.g. connection
        # failure before any model ran) — log what we know and move on.
        _metrics().log(
            PipelineRunMetric(
                run_id=batch_id,
                stage="dbt",
                pipeline_name=pipeline_name,
                status="failed",
                started_at=started_at,
                error_message="run_results.json not found — dbt likely failed before producing results",
            )
        )
        return

    with open(DBT_RUN_RESULTS_PATH, "r", encoding="utf-8") as f:
        run_results = json.load(f)

    statuses = [r["status"] for r in run_results.get("results", [])]
    passed = statuses.count("pass") + statuses.count("success")
    failed = statuses.count("fail") + statuses.count("error")
    warned = statuses.count("warn")
    overall_status = "failed" if failed > 0 else "success"

    _metrics().log(
        PipelineRunMetric(
            run_id=batch_id,
            stage="dbt",
            pipeline_name=pipeline_name,
            status=overall_status,
            started_at=started_at,
            tests_passed=passed,
            tests_failed=failed,
            tests_warned=warned,
        )
    )


@dag(
    dag_id="wallet_bronze_ingestion",
    description="CSV -> PostgreSQL -> Transactions -> dbt Silver/Snapshot/Gold/Test, with run-history logging",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bronze", "wallet-data-platform"],
)
def wallet_bronze_ingestion():

    @task
    def ensure_schema(batch_id: str) -> str:
        writer = _writer()
        writer.ensure_schema_exists()
        MetricsWriter(bronze_writer=writer).ensure_schema_exists()
        return batch_id

    @task
    def csv_task(batch_id: str) -> None:
        if not run_csv_pipeline(_writer(), batch_id, _metrics()):
            raise AirflowException("CSV ingestion failed")

    @task
    def postgres_task(batch_id: str) -> None:
        if not run_postgres_pipeline(_writer(), _watermark_store(), batch_id, _metrics()):
            raise AirflowException("PostgreSQL reference-table ingestion failed")

    @task
    def postgres_transactions_task(batch_id: str) -> None:
        if not run_postgres_transactions_pipeline(_writer(), _watermark_store(), batch_id, _metrics()):
            raise AirflowException("PostgreSQL transactions ingestion failed — see task log")

    @task(trigger_rule=TriggerRule.ALL_FAILED)
    def api_transactions_task(batch_id: str) -> None:
        """Only runs if postgres_transactions_task failed — this IS the
        fallback, made visible instead of hidden inside one function call.
        """
        if not run_api_pipeline(_writer(), _watermark_store(), batch_id, _metrics()):
            raise AirflowException("API fallback for transactions also failed")

    @task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def transactions_complete() -> None:
        """Leaf task. Succeeds if EITHER upstream task succeeded — this is
        what makes the overall DAG run count as successful when the
        fallback path was the one that actually worked.
        """
        return None

    # --- dbt stages ---------------------------------------------------
    #
    # --full-refresh is intentionally NOT passed anywhere — normal runs
    # rely on fact_transactions' incremental/merge logic; a full refresh
    # is a manual, deliberate action, not an automated one.

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"dbt run --select tag:silver "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_staging_metrics(batch_id: str) -> None:
        _log_dbt_stage_metrics(batch_id, "dbt_run_staging", datetime.datetime.now(datetime.timezone.utc))

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"dbt snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_snapshot_metrics(batch_id: str) -> None:
        _log_dbt_stage_metrics(batch_id, "dbt_snapshot", datetime.datetime.now(datetime.timezone.utc))

    # Gold dimensions (dim_customers, dim_wallet, dim_branch, dim_merchant)
    # read from the snapshot tables, not staging directly — so this must
    # run AFTER dbt_snapshot, using this run's freshly-captured history.
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            f"dbt run --select tag:gold "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_gold_metrics(batch_id: str) -> None:
        _log_dbt_stage_metrics(batch_id, "dbt_run_gold", datetime.datetime.now(datetime.timezone.utc))

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_test_metrics(batch_id: str) -> None:
        _log_dbt_stage_metrics(batch_id, "dbt_test", datetime.datetime.now(datetime.timezone.utc))

    # --- wiring ---------------------------------------------------------

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

    staging_metrics = log_staging_metrics(batch_id)
    snapshot_metrics = log_snapshot_metrics(batch_id)
    gold_metrics = log_gold_metrics(batch_id)
    test_metrics = log_test_metrics(batch_id)

    # Correct dependency order, matching the actual ref() chain:
    #   bronze -> stg_* (silver, views) -> snapshots -> dim_*/fact (gold)
    # Each dbt stage is immediately followed by its own metrics-logging
    # task (reading run_results.json before the NEXT dbt command
    # overwrites it), then the pipeline proceeds to the next stage.
    done >> dbt_run_staging >> staging_metrics >> dbt_snapshot
    dbt_snapshot >> snapshot_metrics >> dbt_run_gold
    dbt_run_gold >> gold_metrics >> dbt_test
    dbt_test >> test_metrics


wallet_bronze_ingestion()