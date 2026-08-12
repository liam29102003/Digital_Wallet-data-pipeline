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

    done >> dbt_run_staging >> staging_metrics >> dbt_snapshot
    dbt_snapshot >> snapshot_metrics >> dbt_run_gold
    dbt_run_gold >> gold_metrics >> dbt_test
    dbt_test >> test_metrics


wallet_bronze_ingestion()