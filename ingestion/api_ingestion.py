"""Node.js transactions API ingestion pipeline.

Pulls transactions incrementally (via `transaction_timestamp`), paginated,
with retry/backoff on transient failures (timeouts, 5xx). Response
payloads are validated before being handed off to Bronze.

API contract (documented service):

    GET {base_url}/api/v1/transactions

    Query params used here:
      page        - page number
      page_size   - records per page
      sort        - "transaction_timestamp" (ascending, so pages arrive in
                    watermark order and the max-seen value is a safe
                    checkpoint)
      date_from   - "YYYY-MM-DD", inclusive from the start of that day
      date_to     - "YYYY-MM-DD", inclusive through the end of that day
                    (not used here — we always pull through "now")

    200 OK
    {
        "data": [ {"transaction_id": ..., "transaction_timestamp": ..., ...}, ... ],
        "pagination": {"page": 1, "total_pages": 5, ...},
        "links": {"next": "...", ...},
        "filters": {},
        "range_filters": {},
        "sort": [],
        "fields": [],
        "page_aggregations": {...},
        "meta": {...}
    }

Note on incremental filtering: the API's date_from/date_to filters only
have day-level granularity, not a precise timestamp cutoff. So this
pipeline requests everything from the watermark's date onward, then
applies an exact `> last_watermark` filter client-side before writing to
Bronze and before computing the new watermark. This avoids both
re-ingesting already-loaded rows and skipping rows that landed later in
the same day as the last run.
"""

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

from ingestion.config import REQUIRED_COLUMNS, ApiConfig, SourceSystem
from ingestion.databricks_writer import BronzeWriter
from ingestion.exceptions import ApiResponseError, SourceConnectionError
from ingestion.logger import get_logger
from ingestion.utils import Timer, WatermarkStore, add_ingestion_metadata, ensure_non_empty, validate_required_columns

logger = get_logger(__name__)

_TABLE_NAME = "transactions"
_WATERMARK_COLUMN = "transaction_timestamp"

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
    """Extracts, validates, and loads the transactions table into Bronze."""

    def __init__(
        self,
        config: ApiConfig,
        writer: BronzeWriter,
        watermark_store: WatermarkStore,
        batch_id: str,
    ) -> None:
        self.config = config
        self.writer = writer
        self.watermark_store = watermark_store
        self.batch_id = batch_id
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
                # Fallback heuristic when neither links nor pagination
                # metadata is present: a full page probably means more
                # records exist; a short/empty page means we're done.
                has_more = len(records) == self.config.page_size and len(records) > 0

            page += 1

        return all_records

    def run(self) -> ApiIngestionResult:
        result = ApiIngestionResult()
        logger.info("=== API ingestion pipeline started (table='%s') ===", _TABLE_NAME)

        with Timer("API ingestion pipeline"):
            try:
                watermark_key = f"api.{_TABLE_NAME}"
                backfill_mode = bool(self.config.fixed_date_from)

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
                    last_watermark = self.watermark_store.get(watermark_key)
                    # API date filters are day-granular only, so request from
                    # the watermark's date onward and filter precisely below.
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

                stamped = add_ingestion_metadata(df, SourceSystem.API, self.batch_id)

                rows_written = self.writer.write_table(stamped, _TABLE_NAME)
                result.rows_written = rows_written
                logger.info("Bronze write success: table='%s' rows=%d", _TABLE_NAME, rows_written)

                if backfill_mode:
                    logger.info("Backfill run complete — stored watermark left unchanged.")
                else:
                    new_watermark = df[_WATERMARK_COLUMN].max()
                    self.watermark_store.set(watermark_key, str(new_watermark))
            except Exception:
                logger.exception("API ingestion failed for table '%s'", _TABLE_NAME)
                result.failed = True

        logger.info("=== API ingestion pipeline finished: rows_written=%d failed=%s ===", result.rows_written, result.failed)
        return result