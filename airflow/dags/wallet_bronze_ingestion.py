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


from ingestion.dbt_metrics import dbt_results_to_metrics


def _log_dbt_stage_metrics(batch_id: str, pipeline_name: str, started_at: datetime.datetime) -> None:
    if not DBT_RUN_RESULTS_PATH.exists():
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

    metrics = _metrics()
    for metric in dbt_results_to_metrics(run_results, batch_id, pipeline_name, started_at):
        metrics.log(metric)


@dag(
    dag_id="wallet_bronze_ingestion",
    description="CSV / PostgreSQL / Transactions in parallel -> dbt Silver/Snapshot/Gold/Observability/Test",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bronze", "wallet-data-platform"],
    # LocalExecutor's actual concurrency is bounded by parallelism/
    # max_active_tasks_per_dag in airflow.cfg — the three extraction
    # branches below are independent in the graph, but this setting is
    # what determines whether they're actually scheduled concurrently
    # or just made eligible to be.
    max_active_tasks=8,
)
def wallet_bronze_ingestion():

    @task
    def ensure_schema(batch_id: str) -> str:
        writer = _writer()
        writer.ensure_schema_exists()
        MetricsWriter(bronze_writer=writer).ensure_schema_exists()
        return batch_id

    # ------------------------------------------------------------------
    # Three Bronze extraction branches. None of these read each other's
    # output — CSV reference data, Postgres customers/wallets, and
    # transactions are three independent source systems with no FK
    # enforcement at Bronze — so they only need to share ensure_schema
    # as a common upstream, not each other.
    # ------------------------------------------------------------------

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
        """Only runs if postgres_transactions_task failed — the fallback,
        kept sequential after pg_txn since it's only meaningful once pg_txn
        has actually failed. This is the one place in the DAG where a
        true dependency (not just an authoring artifact) makes the chain
        correct.
        """
        if not run_api_pipeline(_writer(), _watermark_store(), batch_id, _metrics()):
            raise AirflowException("API fallback for transactions also failed")

    @task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def transactions_complete() -> None:
        """Succeeds if EITHER pg_txn or api_txn succeeded — collapses the
        primary/fallback pair into a single success/fail signal before
        it's combined with the other two branches below.
        """
        return None

    # ------------------------------------------------------------------
    # Fan-in gate. Default trigger rule is ALL_SUCCESS, which is exactly
    # what's needed here: dbt genuinely can't start until CSV, Postgres,
    # AND the transactions source (whichever one worked) have all landed
    # in Bronze — this is a real data dependency, not an artifact of how
    # the tasks were wired.
    # ------------------------------------------------------------------
    @task
    def ingestion_complete() -> None:
        return None

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
        bash_command=f"dbt snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_snapshot_metrics(batch_id: str) -> None:
        _log_dbt_stage_metrics(batch_id, "dbt_snapshot", datetime.datetime.now(datetime.timezone.utc))

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

    # ------------------------------------------------------------------
    # Observability layer (vw_pipeline_batch_health). This was previously
    # missing entirely from the DAG: the model is tagged "observability"
    # in dbt_project.yml, but only tag:silver and tag:gold were ever run,
    # so the view was never materialized and `dbt test` failed with
    # TABLE_OR_VIEW_NOT_FOUND on its not_null tests. Its sources are the
    # raw observability.* tables written directly by the Python ingestion
    # layer (metrics_writer.py / quarantine.py / reconciliation.py), so it
    # has no dependency on staging/snapshot/gold — it only needs to exist
    # before `dbt test` runs. Kept sequential after gold purely to match
    # the DAG's existing stage-by-stage metrics-logging pattern, not
    # because of an actual data dependency.
    # ------------------------------------------------------------------
    dbt_run_observability = BashOperator(
        task_id="dbt_run_observability",
        bash_command=(
            f"dbt run --select tag:observability "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_observability_metrics(batch_id: str) -> None:
        _log_dbt_stage_metrics(batch_id, "dbt_run_observability", datetime.datetime.now(datetime.timezone.utc))

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_test_metrics(batch_id: str) -> None:
        _log_dbt_stage_metrics(batch_id, "dbt_test", datetime.datetime.now(datetime.timezone.utc))

    batch_id = "{{ dag_run.run_id }}"

    ready = ensure_schema(batch_id)

    # --- fan out: three parallel-eligible branches off ensure_schema ---
    csv_result = csv_task(batch_id)
    postgres_result = postgres_task(batch_id)
    pg_txn = postgres_transactions_task(batch_id)
    api_txn = api_transactions_task(batch_id)
    txn_done = transactions_complete()

    ready >> csv_result
    ready >> postgres_result
    ready >> pg_txn
    pg_txn >> api_txn
    [pg_txn, api_txn] >> txn_done

    # --- fan in: dbt waits on all three branches, not one chain ---
    ingest_done = ingestion_complete()
    [csv_result, postgres_result, txn_done] >> ingest_done

    staging_metrics = log_staging_metrics(batch_id)
    snapshot_metrics = log_snapshot_metrics(batch_id)
    gold_metrics = log_gold_metrics(batch_id)
    observability_metrics = log_observability_metrics(batch_id)
    test_metrics = log_test_metrics(batch_id)

    ingest_done >> dbt_run_staging >> staging_metrics >> dbt_snapshot
    dbt_snapshot >> snapshot_metrics >> dbt_run_gold
    dbt_run_gold >> gold_metrics >> dbt_run_observability
    dbt_run_observability >> observability_metrics >> dbt_test
    dbt_test >> test_metrics


wallet_bronze_ingestion()