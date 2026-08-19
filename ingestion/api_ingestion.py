from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.config import NATURAL_KEY_COLUMNS, REQUIRED_COLUMNS, ApiConfig, SourceSystem
from ingestion.databricks_writer import BronzeWriter
from ingestion.exceptions import ApiResponseError, SourceConnectionError
from ingestion.logger import get_logger
from ingestion.quarantine import QuarantineWriter, split_quarantined_rows
from ingestion.reconciliation import ReconciliationResult, ReconciliationWriter
from ingestion.utils import Timer, WatermarkStore, add_ingestion_metadata, ensure_non_empty, validate_required_columns

logger = get_logger(__name__)

_TABLE_NAME = "transactions"
_WATERMARK_COLUMN = "transaction_timestamp"
_WATERMARK_KEY = f"api.{_TABLE_NAME}"

_RETRYABLE_EXCEPTIONS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
)


@dataclass
class ApiIngestionResult:
    rows_written: int = 0
    pages_fetched: int = 0
    failed: bool = False


class ApiIngestion:

    def __init__(
        self,
        config: ApiConfig,
        writer: BronzeWriter,
        watermark_store: WatermarkStore,
        batch_id: str,
        quarantine_writer: "QuarantineWriter | None" = None,
        reconciliation_writer: "ReconciliationWriter | None" = None,
    ) -> None:
        self.config = config
        self.writer = writer
        self.watermark_store = watermark_store
        self.batch_id = batch_id
        self.quarantine_writer = quarantine_writer
        self.reconciliation_writer = reconciliation_writer
        self._session = requests.Session()
        if config.auth_token:
            self._session.headers.update({"X-API-Key": config.auth_token})

    @retry(
        retry=retry_if_exception_type(_RETRYABLE_EXCEPTIONS),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        reraise=True,
    )
    def _fetch_page(self, page: int, date_from: Optional[str], date_to: Optional[str] = None) -> Dict:
        params = {
            "page": page,
            "page_size": self.config.page_size,
            # Ascending sort keeps pages in watermark order so the running
            # max of transaction_timestamp is a safe incremental checkpoint.
            "sort": "transaction_timestamp",
        }
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to

        logger.info("Calling API: %s params=%s", self.config.transactions_url, params)
        try:
            response = self._session.get(
                self.config.transactions_url,
                params=params,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            logger.warning("API request timed out (page=%d): %s — will retry", page, exc)
            raise
        except requests.exceptions.ConnectionError as exc:
            logger.warning("API connection error (page=%d): %s — will retry", page, exc)
            raise
        except requests.exceptions.HTTPError as exc:
            raise SourceConnectionError(f"API returned an error status on page {page}: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiResponseError(f"API response for page {page} was not valid JSON: {exc}") from exc

        if "data" not in payload:
            raise ApiResponseError(f"API response for page {page} is missing the 'data' field: {payload}")

        return payload

    def _fetch_all_pages(self, date_from: Optional[str], date_to: Optional[str] = None) -> List[dict]:
        all_records: List[dict] = []
        page = 1
        has_more = True

        while has_more:
            payload = self._fetch_page(page, date_from, date_to)
            records = payload.get("data", [])
            all_records.extend(records)
            logger.info("Fetched page %d: %d records (running total: %d)", page, len(records), len(all_records))

            pagination = payload.get("pagination") or {}
            links = payload.get("links") or {}

            if links.get("next"):
                has_more = True
            elif pagination.get("total_pages") is not None:
                current_page = pagination.get("page", page)
                has_more = current_page < pagination["total_pages"]
            else:
                
                has_more = len(records) == self.config.page_size and len(records) > 0

            page += 1

        return all_records

    def _reconcile_pending_write(self) -> None:
        
        pending = self.watermark_store.get_pending(_WATERMARK_KEY)
        if pending is None:
            return

        stale_batch_id, stale_watermark = pending
        logger.warning(
            "Found uncommitted write for '%s' from a previous run (batch_id=%s, target_watermark=%s) — "
            "rolling back before proceeding.",
            _TABLE_NAME,
            stale_batch_id,
            stale_watermark,
        )
        try:
            self.writer.delete_batch(_TABLE_NAME, stale_batch_id)
        except Exception:
            logger.exception(
                "Rollback failed for '%s' batch_id=%s — leaving pending entry for the next retry.",
                _TABLE_NAME,
                stale_batch_id,
            )
            raise
        self.watermark_store.discard_pending(_WATERMARK_KEY)

    def run(self) -> ApiIngestionResult:
        result = ApiIngestionResult()
        logger.info("=== API ingestion pipeline started (table='%s') ===", _TABLE_NAME)

        with Timer("API ingestion pipeline"):
            try:
                backfill_mode = bool(self.config.fixed_date_from)
                last_watermark: Optional[str] = None

                if backfill_mode:
                    date_from = self.config.fixed_date_from
                    date_to = self.config.fixed_date_to or self.config.fixed_date_from
                    logger.warning(
                        "Fixed date backfill active: date_from=%s date_to=%s "
                        "(ignoring stored watermark; watermark will NOT be updated by this run)",
                        date_from,
                        date_to,
                    )
                else:
                    self._reconcile_pending_write()

                    last_watermark = self.watermark_store.get(_WATERMARK_KEY)
                    date_from = pd.to_datetime(last_watermark).strftime("%Y-%m-%d") if last_watermark else None
                    date_to = None

                logger.info(
                    "Extraction started: table='%s' source=api mode=%s",
                    _TABLE_NAME,
                    "backfill" if backfill_mode else "incremental",
                )
                records = self._fetch_all_pages(date_from, date_to)
                result.pages_fetched = -(-len(records) // max(self.config.page_size, 1))  # ceil div, informational

                df = pd.DataFrame.from_records(records)
                ensure_non_empty(df, _TABLE_NAME, allow_empty=True)

                if df.empty:
                    logger.info("No transactions returned for the requested window — nothing to write.")
                    return result

                validate_required_columns(df, REQUIRED_COLUMNS[_TABLE_NAME], _TABLE_NAME)

                df[_WATERMARK_COLUMN] = pd.to_datetime(df[_WATERMARK_COLUMN])

                if not backfill_mode and last_watermark:
                    before_filter = len(df)
                    df = df[df[_WATERMARK_COLUMN] > pd.to_datetime(last_watermark)]
                    logger.info(
                        "Client-side watermark filter: %d rows -> %d rows (cutoff=%s)",
                        before_filter,
                        len(df),
                        last_watermark,
                    )

                ensure_non_empty(df, _TABLE_NAME, allow_empty=True)
                if df.empty:
                    logger.info("No new transactions after client-side watermark filtering — nothing to write.")
                    return result

                extracted_count = len(df)


                clean_df, bad_df = split_quarantined_rows(
                    df, NATURAL_KEY_COLUMNS.get(_TABLE_NAME, [])
                )
                quarantined_count = len(bad_df)
                if self.quarantine_writer is not None and quarantined_count:
                    self.quarantine_writer.write(bad_df, _TABLE_NAME, SourceSystem.API, self.batch_id)

                stamped = add_ingestion_metadata(clean_df, SourceSystem.API, self.batch_id)

                if not backfill_mode:

                    new_watermark = df[_WATERMARK_COLUMN].max()
                    self.watermark_store.begin(_WATERMARK_KEY, self.batch_id, str(new_watermark))

                rows_written = self.writer.write_table(stamped, _TABLE_NAME)
                result.rows_written = rows_written
                logger.info("Bronze write success: table='%s' rows=%d", _TABLE_NAME, rows_written)

                if backfill_mode:
                    logger.info("Backfill run complete — stored watermark left unchanged.")
                else:
                    self.watermark_store.commit(_WATERMARK_KEY, self.batch_id)


                if self.reconciliation_writer is not None:
                    self.reconciliation_writer.log(
                        ReconciliationResult(
                            table_name=_TABLE_NAME,
                            source_system=SourceSystem.API,
                            extracted_count=extracted_count,
                            written_count=rows_written,
                            quarantined_count=quarantined_count,
                        ),
                        run_id=self.batch_id,
                    )
            except Exception:
                logger.exception("API ingestion failed for table '%s'", _TABLE_NAME)
                result.failed = True
        logger.info("=== API ingestion pipeline finished: rows_written=%d failed=%s ===", result.rows_written, result.failed)
        return result