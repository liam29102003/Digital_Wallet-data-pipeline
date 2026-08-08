"""Entry point: runs all Bronze ingestion pipelines in order.

    python ingestion/main.py

Order: CSV -> PostgreSQL (customers/wallet_accounts) -> Transactions.
The transactions stage tries PostgreSQL first and falls back to the
Node.js API if PostgreSQL ingestion fails — see run_transactions_pipeline()
docstring for how this maps onto Airflow later. Each pipeline is
isolated: a failure in one does not prevent the others from running, but
the process exits with a non-zero status if any pipeline failed, so it's
safe to wire into cron today and an Airflow DAG (one task per run_*
function) tomorrow.
"""

from __future__ import annotations

import sys

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
from ingestion.postgres_ingestion import PostgresIngestion
from ingestion.postgres_transactions_ingestion import PostgresTransactionsIngestion
from ingestion.utils import Timer, WatermarkStore

logger = get_logger(__name__)


def run_csv_pipeline(writer: BronzeWriter, batch_id: str) -> bool:
    csv_config = get_csv_config()
    pipeline = CsvIngestion(config=csv_config, writer=writer, batch_id=batch_id)
    result = pipeline.run()
    return result.success


def run_postgres_pipeline(writer: BronzeWriter, watermark_store: WatermarkStore, batch_id: str) -> bool:
    postgres_config = get_postgres_config()
    pipeline = PostgresIngestion(
        config=postgres_config, writer=writer, watermark_store=watermark_store, batch_id=batch_id
    )
    result = pipeline.run()
    return result.success


def run_postgres_transactions_pipeline(
    writer: BronzeWriter, watermark_store: WatermarkStore, batch_id: str
) -> bool:
    postgres_config = get_postgres_config()
    pipeline = PostgresTransactionsIngestion(
        config=postgres_config, writer=writer, watermark_store=watermark_store, batch_id=batch_id
    )
    result = pipeline.run()
    return not result.failed


def run_api_pipeline(writer: BronzeWriter, watermark_store: WatermarkStore, batch_id: str) -> bool:
    api_config = get_api_config()
    pipeline = ApiIngestion(
        config=api_config, writer=writer, watermark_store=watermark_store, batch_id=batch_id
    )
    result = pipeline.run()
    return not result.failed


def run_transactions_pipeline(writer: BronzeWriter, watermark_store: WatermarkStore, batch_id: str) -> bool:
    """Loads bronze.transactions with PostgreSQL as the primary source
    and the Node.js API as a fallback source.

    No orchestrator is wired in yet, so the fallback is expressed here as
    a plain sequential check: try PostgreSQL, and only call the API
    pipeline if PostgreSQL ingestion failed. When this moves into
    Airflow, run_postgres_transactions_pipeline and run_api_pipeline
    become two independent tasks connected by a trigger rule (e.g.
    TriggerRule.ONE_FAILED on the API task) instead of a Python
    if-statement — the pipelines themselves don't need to change, only
    how they're invoked.

    The two sources commit to separate watermark keys
    (postgres.transactions vs api.transactions), so each one resumes
    independently from its own last-committed checkpoint regardless of
    which source served the previous run. transaction_id is the natural
    key for fact_transactions' merge strategy downstream, so any overlap
    introduced by a failover window is de-duplicated there.
    """
    logger.info("--- Transactions: trying PostgreSQL (primary) ---")
    if run_postgres_transactions_pipeline(writer, watermark_store, batch_id):
        logger.info("Transactions loaded from PostgreSQL (primary source).")
        return True

    logger.warning("PostgreSQL transactions ingestion failed — falling back to API (secondary source).")
    if run_api_pipeline(writer, watermark_store, batch_id):
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

    try:
        writer.ensure_schema_exists()
    except Exception:
        logger.exception("Could not confirm/create Bronze schema — aborting run.")
        return 1

    pipeline_results: dict[str, bool] = {}

    with Timer("Full Bronze ingestion run"):
        logger.info("--- Stage 1/3: CSV ---")
        pipeline_results["csv"] = run_csv_pipeline(writer, batch_id)

        logger.info("--- Stage 2/3: PostgreSQL (customers, wallet_accounts) ---")
        pipeline_results["postgres"] = run_postgres_pipeline(writer, watermark_store, batch_id)

        logger.info("--- Stage 3/3: Transactions (PostgreSQL primary, API fallback) ---")
        pipeline_results["transactions"] = run_transactions_pipeline(writer, watermark_store, batch_id)

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