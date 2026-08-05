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

    def run(self) -> PostgresIngestionResult:
        result = PostgresIngestionResult()
        logger.info("=== PostgreSQL ingestion pipeline started (%d tables) ===", len(POSTGRES_INCREMENTAL_TABLES))

        with Timer("PostgreSQL ingestion pipeline"):
            for table_name, watermark_column in POSTGRES_INCREMENTAL_TABLES.items():
                try:
                    df = self.extract_incremental(table_name, watermark_column)
                    ensure_non_empty(df, table_name, allow_empty=True)

                    if df.empty:
                        result.table_row_counts[table_name] = 0
                        continue

                    validate_required_columns(df, REQUIRED_COLUMNS[table_name], table_name)
                    stamped = add_ingestion_metadata(df, SourceSystem.POSTGRES, self.batch_id)
                    rows_written = self.writer.write_table(stamped, table_name)
                    result.table_row_counts[table_name] = rows_written
                    logger.info("Bronze write success: table='%s' rows=%d", table_name, rows_written)

                    new_watermark = df[watermark_column].max()
                    if isinstance(new_watermark, pd.Timestamp):
                        new_watermark = new_watermark.isoformat()
                    self.watermark_store.set(f"postgres.{table_name}", str(new_watermark))
                except Exception:
                    logger.exception("PostgreSQL ingestion failed for table '%s'", table_name)
                    result.failed_tables.append(table_name)

        logger.info(
            "=== PostgreSQL ingestion pipeline finished: %d succeeded, %d failed ===",
            len(result.table_row_counts),
            len(result.failed_tables),
        )
        return result
