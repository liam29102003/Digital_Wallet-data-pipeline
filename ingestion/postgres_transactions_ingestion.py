"""PostgreSQL transactions ingestion pipeline.

Extracts the `transactions` table from the Digital_Money database
incrementally, using `transaction_timestamp` as the watermark — mirroring
the extraction pattern in `postgres_ingestion.py`, but kept as its own
standalone pipeline (not folded into `PostgresIngestion` /
POSTGRES_INCREMENTAL_TABLES) for one reason: this is the PRIMARY source
for `bronze.transactions`, with the Node.js API pipeline (`api_ingestion.py`)
acting as a FALLBACK source for the same logical table when PostgreSQL is
unreachable or the extraction otherwise fails.

Keeping this pipeline's success/failure a standalone, independently
callable unit (rather than lumping it in with customers/wallet_accounts,
which have no fallback) is deliberate: it's what lets `main.py` express
"try PostgreSQL, fall back to API" as a simple sequential check today,
and maps directly onto two Airflow tasks connected by a trigger rule
(e.g. TriggerRule.ONE_FAILED on the API task) later, with no pipeline
rewrite — only the orchestration layer changes.

Watermark isolation: this pipeline commits to the `postgres.transactions`
watermark key, completely separate from the API pipeline's
`api.transactions` key. Each source tracks how far ITS OWN incremental
extraction has progressed independently, so:
  - A PostgreSQL success does not advance (or reset) the API watermark,
    and vice versa.
  - If PostgreSQL fails today and API fills in as a fallback, then
    PostgreSQL recovers tomorrow, PostgreSQL resumes from its own
    last-committed watermark — it doesn't need to know what API loaded
    in the interim. Any rows re-delivered by both sources around a
    failover window are absorbed safely downstream, since
    fact_transactions merges on transaction_id (see fact_transactions.sql).

Crash safety: identical two-phase commit protocol as postgres_ingestion.py
and api_ingestion.py (WatermarkStore.begin -> write Bronze ->
WatermarkStore.commit), so a crash mid-write never double-writes rows on
retry. See ingestion/utils.py::WatermarkStore for the full protocol.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import psycopg2
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ingestion.config import (
    POSTGRES_TRANSACTIONS_TABLE,
    POSTGRES_TRANSACTIONS_WATERMARK_COLUMN,
    REQUIRED_COLUMNS,
    PostgresConfig,
    SourceSystem,
)
from ingestion.databricks_writer import BronzeWriter
from ingestion.exceptions import SourceConnectionError
from ingestion.logger import get_logger
from ingestion.utils import Timer, WatermarkStore, add_ingestion_metadata, ensure_non_empty, validate_required_columns

logger = get_logger(__name__)

_WATERMARK_KEY = f"postgres.{POSTGRES_TRANSACTIONS_TABLE}"


@dataclass
class PostgresTransactionsIngestionResult:
    rows_written: int = 0
    failed: bool = False


class PostgresTransactionsIngestion:
    """Extracts, validates, and loads bronze.transactions from PostgreSQL.

    Primary source for transactions; see module docstring for the
    fallback relationship with ApiIngestion.
    """

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
            logger.warning("PostgreSQL connection attempt failed (transactions): %s", exc)
            raise

    def extract_incremental(self) -> pd.DataFrame:
        """Extract only transaction rows newer than the last committed watermark."""
        last_watermark: Optional[str] = self.watermark_store.get(_WATERMARK_KEY)
        qualified_table = f"{self.config.schema}.{POSTGRES_TRANSACTIONS_TABLE}"

        logger.info(
            "Extraction started: table='%s' source=postgres mode=incremental",
            POSTGRES_TRANSACTIONS_TABLE,
        )

        try:
            with self._connect() as conn:
                if last_watermark:
                    query = (
                        f"SELECT * FROM {qualified_table} "
                        f"WHERE {POSTGRES_TRANSACTIONS_WATERMARK_COLUMN} > %s "
                        f"ORDER BY {POSTGRES_TRANSACTIONS_WATERMARK_COLUMN} ASC"
                    )
                    df = pd.read_sql(query, conn, params=(last_watermark,))
                else:
                    logger.info(
                        "No prior PostgreSQL watermark for 'transactions' — performing initial full load."
                    )
                    query = (
                        f"SELECT * FROM {qualified_table} "
                        f"ORDER BY {POSTGRES_TRANSACTIONS_WATERMARK_COLUMN} ASC"
                    )
                    df = pd.read_sql(query, conn)
        except psycopg2.OperationalError as exc:
            raise SourceConnectionError(
                f"Could not connect to PostgreSQL for table 'transactions': {exc}"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise SourceConnectionError(f"Query failed for table 'transactions': {exc}") from exc

        logger.info("Extracted %d rows from PostgreSQL table 'transactions'", len(df))
        return df

    def _reconcile_pending_write(self) -> None:
        """Roll back an orphaned Bronze write from a previous run that
        crashed between writing to Bronze and committing its watermark."""
        pending = self.watermark_store.get_pending(_WATERMARK_KEY)
        if pending is None:
            return

        stale_batch_id, stale_watermark = pending
        logger.warning(
            "Found uncommitted write for 'transactions' (postgres) from a previous run "
            "(batch_id=%s, target_watermark=%s) — rolling back before proceeding.",
            stale_batch_id,
            stale_watermark,
        )
        try:
            self.writer.delete_batch(POSTGRES_TRANSACTIONS_TABLE, stale_batch_id)
        except Exception:
            logger.exception(
                "Rollback failed for 'transactions' (postgres) batch_id=%s — leaving pending "
                "entry for the next retry.",
                stale_batch_id,
            )
            raise
        self.watermark_store.discard_pending(_WATERMARK_KEY)


    def _get_target_watermark(self, conn, last_watermark, date_from=None, date_to=None):
        """Cheap aggregate query to learn the max watermark value among
        rows this run will extract, WITHOUT pulling any row data."""
        qualified_table = f"{self.config.schema}.{POSTGRES_TRANSACTIONS_TABLE}"
        where_clause, params = self._build_where_clause(last_watermark, date_from, date_to)
        try:
            with conn.cursor() as cur:
                query = f"SELECT MAX({POSTGRES_TRANSACTIONS_WATERMARK_COLUMN}) FROM {qualified_table}{where_clause}"
                cur.execute(query, params)
                row = cur.fetchone()
                return row[0] if row else None
        except Exception as exc:  # noqa: BLE001
            raise SourceConnectionError(f"Failed to determine target watermark for 'transactions': {exc}") from exc

    def _build_where_clause(self, last_watermark, date_from=None, date_to=None):
        """Builds the WHERE clause + params for either mode:
        - Testing/backfill: bounded [date_from, date_to) window, ignores watermark.
        - Normal incremental: > last_watermark, or no filter on first-ever run.
        """
        col = POSTGRES_TRANSACTIONS_WATERMARK_COLUMN
        if date_from and date_to:
            return f" WHERE {col} >= %s AND {col} < %s", (date_from, date_to)
        if last_watermark:
            return f" WHERE {col} > %s", (last_watermark,)
        return "", ()

    def _stream_chunks(self, conn, last_watermark, chunk_size, date_from=None, date_to=None):
        qualified_table = f"{self.config.schema}.{POSTGRES_TRANSACTIONS_TABLE}"
        cursor_name = f"txn_stream_{self.batch_id}"
        where_clause, params = self._build_where_clause(last_watermark, date_from, date_to)

        try:
            with conn.cursor(name=cursor_name) as cur:
                cur.itersize = chunk_size
                query = (
                    f"SELECT * FROM {qualified_table}{where_clause} "
                    f"ORDER BY {POSTGRES_TRANSACTIONS_WATERMARK_COLUMN} ASC"
                )
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                while True:
                    rows = cur.fetchmany(chunk_size)
                    if not rows:
                        break
                    yield pd.DataFrame(rows, columns=columns)
        except psycopg2.OperationalError as exc:
            raise SourceConnectionError(f"Connection lost while streaming 'transactions': {exc}") from exc
        except Exception as exc:  # noqa: BLE001
            raise SourceConnectionError(f"Streaming extraction failed for 'transactions': {exc}") from exc

    def run(self) -> PostgresTransactionsIngestionResult:
        result = PostgresTransactionsIngestionResult()
        backfill_mode = bool(self.config.transactions_date_from and self.config.transactions_date_to)

        logger.info("=== PostgreSQL transactions ingestion pipeline started (streaming) ===")

        with Timer("PostgreSQL transactions ingestion pipeline"):
            try:
                date_from = date_to = None
                last_watermark = None

                if backfill_mode:
                    date_from = self.config.transactions_date_from
                    date_to = self.config.transactions_date_to
                    logger.warning(
                        "Bounded date-range test pull active: date_from=%s date_to=%s "
                        "(ignoring stored watermark; watermark will NOT be updated by this run)",
                        date_from, date_to,
                    )
                else:
                    self._reconcile_pending_write()
                    last_watermark = self.watermark_store.get(_WATERMARK_KEY)

                chunk_size = self.config.transactions_chunk_size

                with self._connect() as conn:
                    target_watermark = self._get_target_watermark(conn, last_watermark, date_from, date_to)

                    if target_watermark is None:
                        logger.info("No transactions found for the requested window — nothing to write.")
                        return result

                    if not backfill_mode:
                        target_watermark_str = (
                            target_watermark.isoformat() if hasattr(target_watermark, "isoformat")
                            else str(target_watermark)
                        )
                        # Phase 1: declare intent BEFORE writing the first chunk.
                        self.watermark_store.begin(_WATERMARK_KEY, self.batch_id, target_watermark_str)

                    for chunk_df in self._stream_chunks(conn, last_watermark, chunk_size, date_from, date_to):
                        result.chunks_written += 1
                        validate_required_columns(
                            chunk_df, REQUIRED_COLUMNS[POSTGRES_TRANSACTIONS_TABLE], POSTGRES_TRANSACTIONS_TABLE
                        )
                        stamped = add_ingestion_metadata(chunk_df, SourceSystem.POSTGRES, self.batch_id)
                        rows_written = self.writer.write_table(stamped, POSTGRES_TRANSACTIONS_TABLE)
                        result.rows_written += rows_written
                        logger.info(
                            "Bronze write progress: table='%s' chunk=%d rows_in_chunk=%d rows_total=%d",
                            POSTGRES_TRANSACTIONS_TABLE, result.chunks_written, rows_written, result.rows_written,
                        )

                if backfill_mode:
                    logger.info("Bounded test pull complete — stored watermark left unchanged.")
                else:
                    # Phase 2: only now, after every chunk has landed, is it
                    # safe to advance the watermark.
                    self.watermark_store.commit(_WATERMARK_KEY, self.batch_id)
            except Exception:
                logger.exception("PostgreSQL transactions ingestion failed")
                result.failed = True

        logger.info(
            "=== PostgreSQL transactions ingestion pipeline finished: chunks=%d rows_written=%d failed=%s ===",
            result.chunks_written, result.rows_written, result.failed,
        )
        return result