"""PostgreSQL ingestion pipeline.

Extracts the operational tables (customers, wallet_accounts) from the
Digital_Money database incrementally, using each table's updated_at
column as the watermark. Watermarks are persisted via WatermarkStore so
reruns only pull rows changed since the last successful load.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import psycopg2
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ingestion.config import POSTGRES_INCREMENTAL_TABLES, REQUIRED_COLUMNS, PostgresConfig, SourceSystem
from ingestion.databricks_writer import BronzeWriter
from ingestion.exceptions import SourceConnectionError
from ingestion.logger import get_logger
from ingestion.utils import Timer, WatermarkStore, add_ingestion_metadata, ensure_non_empty, validate_required_columns

logger = get_logger(__name__)


@dataclass
class PostgresIngestionResult:
    table_row_counts: Dict[str, int] = field(default_factory=dict)
    failed_tables: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return not self.failed_tables


class PostgresIngestion:
    """Extracts, validates, and loads PostgreSQL operational tables into Bronze."""

    def __init__(
        self,
        config: PostgresConfig,
        writer: BronzeWriter,
        watermark_store: WatermarkStore,
        batch_id: str,
    ) -> None:
        self.config = config
        self.writer = writer
        self.watermark_store = watermark_store
        self.batch_id = batch_id

    @retry(
        retry=retry_if_exception_type(psycopg2.OperationalError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _connect(self):
        try:
            return psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.database,
                user=self.config.user,
                password=self.config.password,
                connect_timeout=self.config.connect_timeout,
            )
        except psycopg2.OperationalError as exc:
            logger.warning("PostgreSQL connection attempt failed: %s", exc)
            raise

    def extract_incremental(self, table_name: str, watermark_column: str) -> pd.DataFrame:
        """Extract only rows changed since the last saved watermark."""
        watermark_key = f"postgres.{table_name}"
        last_watermark: Optional[str] = self.watermark_store.get(watermark_key)

        qualified_table = f"{self.config.schema}.{table_name}"
        logger.info("Extraction started: table='%s' source=postgres mode=incremental", table_name)

        try:
            with self._connect() as conn:
                if last_watermark:
                    query = (
                        f"SELECT * FROM {qualified_table} "
                        f"WHERE {watermark_column} > %s ORDER BY {watermark_column} ASC"
                    )
                    df = pd.read_sql(query, conn, params=(last_watermark,))
                else:
                    logger.info("No prior watermark for '%s' — performing initial full load.", table_name)
                    query = f"SELECT * FROM {qualified_table} ORDER BY {watermark_column} ASC"
                    df = pd.read_sql(query, conn)
        except psycopg2.OperationalError as exc:
            raise SourceConnectionError(f"Could not connect to PostgreSQL for table '{table_name}': {exc}") from exc
        except Exception as exc:  # noqa: BLE001
            raise SourceConnectionError(f"Query failed for table '{table_name}': {exc}") from exc

        logger.info("Extracted %d rows from PostgreSQL table '%s'", len(df), table_name)
        return df

    

    def _reconcile_pending_write(self, table_name: str, watermark_key: str) -> None:
        """If a previous run crashed between writing to Bronze and
        committing its watermark, roll back the orphaned rows so this
        run starts from a clean, known state."""
        pending = self.watermark_store.get_pending(watermark_key)
        if not pending or not isinstance(pending, tuple) or len(pending) != 2:
            return

        stale_batch_id, stale_watermark = pending
        logger.warning(
            "Found uncommitted write for '%s' from a previous run (batch_id=%s, target_watermark=%s) — "
            "rolling back before proceeding.",
            table_name, stale_batch_id, stale_watermark,
        )
        try:
            self.writer.delete_batch(table_name, stale_batch_id)
        except Exception:
            logger.exception(
                "Rollback failed for '%s' batch_id=%s — leaving pending entry for the next retry.",
                table_name, stale_batch_id,
            )
            raise
        self.watermark_store.discard_pending(watermark_key)

    def run(self) -> PostgresIngestionResult:
        result = PostgresIngestionResult()
        logger.info("=== PostgreSQL ingestion pipeline started (%d tables) ===", len(POSTGRES_INCREMENTAL_TABLES))

        with Timer("PostgreSQL ingestion pipeline"):
            for table_name, watermark_column in POSTGRES_INCREMENTAL_TABLES.items():
                watermark_key = f"postgres.{table_name}"
                try:
                    self._reconcile_pending_write(table_name, watermark_key)

                    df = self.extract_incremental(table_name, watermark_column)
                    ensure_non_empty(df, table_name, allow_empty=True)

                    if df.empty:
                        result.table_row_counts[table_name] = 0
                        continue

                    validate_required_columns(df, REQUIRED_COLUMNS[table_name], table_name)
                    stamped = add_ingestion_metadata(df, SourceSystem.POSTGRES, self.batch_id)

                    new_watermark = df[watermark_column].max()
                    if isinstance(new_watermark, pd.Timestamp):
                        new_watermark = new_watermark.isoformat()

                    # Phase 1: declare intent BEFORE writing.
                    self.watermark_store.begin(watermark_key, self.batch_id, str(new_watermark))

                    rows_written = self.writer.write_table(stamped, table_name)
                    result.table_row_counts[table_name] = rows_written
                    logger.info("Bronze write success: table='%s' rows=%d", table_name, rows_written)

                    # Phase 2: only now is it safe to advance the watermark.
                    self.watermark_store.commit(watermark_key, self.batch_id)
                except Exception:
                    logger.exception("PostgreSQL ingestion failed for table '%s'", table_name)
                    result.failed_tables.append(table_name)
                    # Deliberately do NOT discard_pending here if begin() already
                    # ran — a pending entry left behind is exactly what triggers
                    # reconciliation + rollback on the next run.

        logger.info(
            "=== PostgreSQL ingestion pipeline finished: %d succeeded, %d failed ===",
            len(result.table_row_counts), len(result.failed_tables),
        )
        return result
