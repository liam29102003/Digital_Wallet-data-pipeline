"""Reusable helpers shared by every ingestion pipeline.

- add_ingestion_metadata: stamps the three required Bronze metadata columns
- validate_required_columns: schema contract check
- WatermarkStore: tiny local JSON-backed incremental-load checkpoint store
- Timer: context manager for logging execution time
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional

import pandas as pd

from ingestion.exceptions import EmptyDatasetError, SchemaValidationError
from ingestion.logger import get_logger

logger = get_logger(__name__)


def add_ingestion_metadata(df: pd.DataFrame, source_system: str, batch_id: str) -> pd.DataFrame:
    """Append the three mandatory Bronze metadata columns.

    Required on every Bronze table: _ingested_at, source_system, batch_id.
    """
    stamped = df.copy()
    stamped["_ingested_at"] = datetime.now(timezone.utc)
    stamped["source_system"] = source_system
    stamped["batch_id"] = batch_id
    return stamped


def validate_required_columns(
    df: pd.DataFrame, required_columns: List[str], table_name: str
) -> None:
    """Raise SchemaValidationError if any required column is missing."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise SchemaValidationError(
            f"Table '{table_name}' is missing required columns: {missing}. "
            f"Found columns: {list(df.columns)}"
        )
    logger.info("Schema validation passed for '%s' (%d columns checked)", table_name, len(required_columns))


def ensure_non_empty(df: pd.DataFrame, table_name: str, allow_empty: bool = False) -> None:
    """Raise EmptyDatasetError unless the dataset is allowed to be empty.

    Incremental loads legitimately return zero rows when there's nothing
    new since the last watermark, so callers pass allow_empty=True there.
    """
    if df.empty and not allow_empty:
        raise EmptyDatasetError(f"Table '{table_name}' returned zero rows and an empty result was not expected.")
    if df.empty:
        logger.info("Table '%s' returned zero new rows (nothing new since last watermark).", table_name)


@contextmanager
def Timer(label: str) -> Iterator[None]:
    """Context manager that logs the wall-clock time a block took."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.info("%s completed in %.2fs", label, elapsed)


@dataclass
class WatermarkStore:
    """Local JSON-backed checkpoint store for incremental extraction.

    This is intentionally a thin, swappable interface: the only two
    operations any ingestion module needs are get() and set(). When this
    project moves to Airflow, this class can be re-implemented on top of
    an Airflow Variable, a Delta control table, or a small metadata DB
    without any change to postgres_ingestion.py / api_ingestion.py.
    """

    state_dir: Path

    def __post_init__(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._file = self.state_dir / "watermarks.json"
        if not self._file.exists():
            self._file.write_text(json.dumps({}), encoding="utf-8")

    def _read_all(self) -> dict:
        try:
            return json.loads(self._file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Watermark file was corrupt/empty — resetting to {}")
            return {}

    def get(self, key: str) -> Optional[str]:
        """Return the last saved watermark value for `key`, or None."""
        value = self._read_all().get(key)
        logger.info("Loaded watermark for '%s': %s", key, value or "<none — full load>")
        return value

    def set(self, key: str, value: str) -> None:
        """Persist the new watermark value for `key`."""
        data = self._read_all()
        data[key] = value
        self._file.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        logger.info("Updated watermark for '%s' -> %s", key, value)


@dataclass
class WatermarkStore:
    """Local JSON-backed checkpoint store for incremental extraction.

    Implements a two-phase commit protocol so a Bronze write and its
    watermark update are never "half-done" across a crash:

      1. begin(key, batch_id, new_value) — record intent BEFORE writing
         to Bronze. get(key) still returns the OLD (last committed) value.
      2. <caller writes to Bronze>
      3. commit(key, batch_id) — only after the write is confirmed,
         promote the pending value to the committed watermark.

    If a run dies between (1) and (3), get_pending(key) on the next run
    surfaces the orphaned batch_id so the caller can roll back the
    partial Bronze write (DELETE WHERE batch_id = X) before retrying —
    turning an at-least-once pipeline into an effectively-once one
    without needing a MERGE/upsert into Bronze.
    """

    state_dir: Path

    def __post_init__(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._file = self.state_dir / "watermarks.json"
        if not self._file.exists():
            self._file.write_text(json.dumps({}), encoding="utf-8")

    def _read_all(self) -> dict:
        try:
            return json.loads(self._file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Watermark file was corrupt/empty — resetting to {}")
            return {}

    def _write_all(self, data: dict) -> None:
        self._file.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")

    def get(self, key: str) -> Optional[str]:
        """Return the last COMMITTED watermark for `key`, or None.

        Never returns a pending/uncommitted value — an in-flight or
        crashed write must not influence the next extraction window.
        """
        entry = self._read_all().get(key, {})
        value = entry.get("value")
        logger.info("Loaded watermark for '%s': %s", key, value or "<none — full load>")
        return value

    def get_pending(self, key: str) -> Optional[tuple[str, str]]:
        """Return (batch_id, pending_value) if a prior run left an
        uncommitted write for `key`, else None. Callers should check
        this before extracting and reconcile (roll back) if present.
        """
        entry = self._read_all().get(key, {})
        if "pending_batch_id" in entry:
            return entry["pending_batch_id"], entry["pending_value"]
        return None

    def begin(self, key: str, batch_id: str, new_value: str) -> None:
        """Record intent to move the watermark to `new_value`, tagged
        with `batch_id`. Call this BEFORE writing to Bronze."""
        data = self._read_all()
        entry = data.get(key, {})
        entry["pending_batch_id"] = batch_id
        entry["pending_value"] = new_value
        data[key] = entry
        self._write_all(data)
        logger.info("Watermark pending for '%s': batch_id=%s new_value=%s", key, batch_id, new_value)

    def commit(self, key: str, batch_id: str) -> None:
        """Promote the pending value to committed, IF it matches
        `batch_id`. Call this only after the Bronze write succeeds."""
        data = self._read_all()
        entry = data.get(key, {})
        if entry.get("pending_batch_id") != batch_id:
            logger.warning(
                "commit() called for '%s' with batch_id=%s but no matching pending entry — ignoring.",
                key, batch_id,
            )
            return
        entry["value"] = entry.pop("pending_value")
        entry.pop("pending_batch_id", None)
        data[key] = entry
        self._write_all(data)
        logger.info("Watermark committed for '%s' -> %s (batch_id=%s)", key, entry["value"], batch_id)

    def discard_pending(self, key: str) -> None:
        """Clear a pending entry after its Bronze rows have been rolled
        back, so the next extraction starts clean from the last
        committed watermark."""
        data = self._read_all()
        entry = data.get(key, {})
        entry.pop("pending_batch_id", None)
        entry.pop("pending_value", None)
        data[key] = entry
        self._write_all(data)
        logger.info("Discarded pending watermark for '%s'", key)

    def set(self, key: str, value: str) -> None:
        """Direct, single-phase set — bypasses the commit protocol.
        Kept for callers (e.g. full-load CSV tables) that don't need
        crash-safe two-phase commit."""
        data = self._read_all()
        entry = data.get(key, {})
        entry["value"] = value
        data[key] = entry
        self._write_all(data)
        logger.info("Updated watermark for '%s' -> %s", key, value)
