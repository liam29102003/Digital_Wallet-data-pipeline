from __future__ import annotations

import sys
from datetime import datetime, timezone

from ingestion.api_ingestion import ApiIngestion
from ingestion.config import (
    get_api_config,
    get_csv_config,
    get_databricks_config,
    get_postgres_config,
    get_runtime_config,
)
from ingestion.csv_ingestion import CsvIngestion
from ingestion.databricks_writer import BronzeWriter
from ingestion.logger import get_logger
from ingestion.metrics_writer import MetricsWriter, PipelineRunMetric
from ingestion.postgres_ingestion import PostgresIngestion
from ingestion.postgres_transactions_ingestion import PostgresTransactionsIngestion
from ingestion.utils import Timer, WatermarkStore

logger = get_logger(__name__)


def _log_metric(
    metrics: MetricsWriter,
    run_id: str,
    pipeline_name: str,
    started_at: datetime,
    success: bool,
    rows_processed: int | None = None,
    error_message: str | None = None,
) -> None:
    metrics.log(
        PipelineRunMetric(
            run_id=run_id,
            stage="ingestion",
            pipeline_name=pipeline_name,
            status="success" if success else "failed",
            started_at=started_at,
            rows_processed=rows_processed,
            error_message=error_message,
        )
    )


def run_csv_pipeline(writer: BronzeWriter, batch_id: str, metrics: MetricsWriter | None = None) -> bool:
    started_at = datetime.now(timezone.utc)
    csv_config = get_csv_config()
    pipeline = CsvIngestion(config=csv_config, writer=writer, batch_id=batch_id)
    result = pipeline.run()

    if metrics is not None:
        total_rows = sum(result.table_row_counts.values())
        error = f"failed_tables={result.failed_tables}" if result.failed_tables else None
        _log_metric(metrics, batch_id, "csv", started_at, result.success, total_rows, error)

    return result.success


def run_postgres_pipeline(
    writer: BronzeWriter, watermark_store: WatermarkStore, batch_id: str, metrics: MetricsWriter | None = None
) -> bool:
    started_at = datetime.now(timezone.utc)
    postgres_config = get_postgres_config()
    pipeline = PostgresIngestion(
        config=postgres_config, writer=writer, watermark_store=watermark_store, batch_id=batch_id
    )
    result = pipeline.run()

    if metrics is not None:
        total_rows = sum(result.table_row_counts.values())
        error = f"failed_tables={result.failed_tables}" if result.failed_tables else None
        _log_metric(metrics, batch_id, "postgres", started_at, result.success, total_rows, error)

    return result.success


def run_postgres_transactions_pipeline(
    writer: BronzeWriter, watermark_store: WatermarkStore, batch_id: str, metrics: MetricsWriter | None = None
) -> bool:
    started_at = datetime.now(timezone.utc)
    postgres_config = get_postgres_config()
    pipeline = PostgresTransactionsIngestion(
        config=postgres_config, writer=writer, watermark_store=watermark_store, batch_id=batch_id
    )
    result = pipeline.run()

    if metrics is not None:
        error = "PostgreSQL transactions ingestion failed" if result.failed else None
        _log_metric(
            metrics, batch_id, "postgres_transactions", started_at, not result.failed,
            result.rows_written, error,
        )

    return not result.failed


def run_api_pipeline(
    writer: BronzeWriter, watermark_store: WatermarkStore, batch_id: str, metrics: MetricsWriter | None = None
) -> bool:
    started_at = datetime.now(timezone.utc)
    api_config = get_api_config()
    pipeline = ApiIngestion(
        config=api_config, writer=writer, watermark_store=watermark_store, batch_id=batch_id
    )
    result = pipeline.run()

    if metrics is not None:
        error = "API transactions ingestion failed" if result.failed else None
        _log_metric(
            metrics, batch_id, "api_transactions_fallback", started_at, not result.failed,
            result.rows_written, error,
        )

    return not result.failed


def run_transactions_pipeline(
    writer: BronzeWriter, watermark_store: WatermarkStore, batch_id: str, metrics: MetricsWriter | None = None
) -> bool:
    logger.info("--- Transactions: trying PostgreSQL (primary) ---")
    if run_postgres_transactions_pipeline(writer, watermark_store, batch_id, metrics):
        logger.info("Transactions loaded from PostgreSQL (primary source).")
        return True

    logger.warning("PostgreSQL transactions ingestion failed — falling back to API (secondary source).")
    if run_api_pipeline(writer, watermark_store, batch_id, metrics):
        logger.info("Transactions loaded from API (fallback source).")
        return True

    logger.error("Both PostgreSQL and API transactions ingestion failed this run.")
    return False


def main() -> int:
    runtime_config = get_runtime_config()
    batch_id = runtime_config.batch_id
    logger.info("########## Bronze ingestion run started | batch_id=%s ##########", batch_id)

    databricks_config = get_databricks_config()
    writer = BronzeWriter(config=databricks_config)
    watermark_store = WatermarkStore(state_dir=runtime_config.state_dir)
    metrics = MetricsWriter(bronze_writer=writer)

    try:
        writer.ensure_schema_exists()
        metrics.ensure_schema_exists()
    except Exception:
        logger.exception("Could not confirm/create Bronze or observability schema — aborting run.")
        return 1

    pipeline_results: dict[str, bool] = {}

    with Timer("Full Bronze ingestion run"):
        logger.info("--- Stage 1/3: CSV ---")
        pipeline_results["csv"] = run_csv_pipeline(writer, batch_id, metrics)

        logger.info("--- Stage 2/3: PostgreSQL (customers, wallet_accounts) ---")
        pipeline_results["postgres"] = run_postgres_pipeline(writer, watermark_store, batch_id, metrics)

        logger.info("--- Stage 3/3: Transactions (PostgreSQL primary, API fallback) ---")
        pipeline_results["transactions"] = run_transactions_pipeline(writer, watermark_store, batch_id, metrics)

    logger.info("########## Bronze ingestion run summary | batch_id=%s ##########", batch_id)
    for pipeline_name, success in pipeline_results.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info("  %-10s -> %s", pipeline_name, status)

    if all(pipeline_results.values()):
        logger.info("All pipelines completed successfully.")
        return 0

    logger.error("One or more pipelines failed. Check logs above / logs/ingestion.log for details.")
    return 1


if __name__ == "__main__":
    sys.exit(main())