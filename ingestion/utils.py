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
    stamped = df.copy()
    stamped["_ingested_at"] = datetime.now(timezone.utc)
    stamped["source_system"] = source_system
    stamped["batch_id"] = batch_id
    return stamped


def validate_required_columns(
    df: pd.DataFrame, required_columns: List[str], table_name: str
) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise SchemaValidationError(
            f"Table '{table_name}' is missing required columns: {missing}. "
            f"Found columns: {list(df.columns)}"
        )
    logger.info("Schema validation passed for '%s' (%d columns checked)", table_name, len(required_columns))


def ensure_non_empty(df: pd.DataFrame, table_name: str, allow_empty: bool = False) -> None:
    if df.empty and not allow_empty:
        raise EmptyDatasetError(f"Table '{table_name}' returned zero rows and an empty result was not expected.")
    if df.empty:
        logger.info("Table '%s' returned zero new rows (nothing new since last watermark).", table_name)


@contextmanager
def Timer(label: str) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.info("%s completed in %.2fs", label, elapsed)


@dataclass
class WatermarkStore:
    

    state_dir: Path

    def __post_init__(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._file = self.state_dir / "watermarks.json"
        if not self._file.exists():
            self._file.write_text(json.dumps({}), encoding="utf-8")

    @staticmethod
    def _normalize_entry(raw) -> dict:
        if raw is None:
            return {}
        if isinstance(raw, str):
            return {"value": raw}
        return dict(raw)

    def _read_all(self) -> dict:
        try:
            return json.loads(self._file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Watermark file was corrupt/empty — resetting to {}")
            return {}

    def _write_all(self, data: dict) -> None:
        self._file.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")

    def get(self, key: str) -> Optional[str]:
        entry = self._normalize_entry(self._read_all().get(key))
        value = entry.get("value")
        logger.info("Loaded watermark for '%s': %s", key, value or "<none — full load>")
        return value

    def get_pending(self, key: str) -> Optional[tuple[str, str]]:
        entry = self._normalize_entry(self._read_all().get(key))
        if "pending_batch_id" in entry:
            return entry["pending_batch_id"], entry["pending_value"]
        return None

    def begin(self, key: str, batch_id: str, new_value: str) -> None:
        data = self._read_all()
        entry = self._normalize_entry(data.get(key))
        entry["pending_batch_id"] = batch_id
        entry["pending_value"] = new_value
        data[key] = entry
        self._write_all(data)
        logger.info("Watermark pending for '%s': batch_id=%s new_value=%s", key, batch_id, new_value)

    def commit(self, key: str, batch_id: str) -> None:
        
        data = self._read_all()
        entry = self._normalize_entry(data.get(key))
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
        data = self._read_all()
        entry = self._normalize_entry(data.get(key))
        entry.pop("pending_batch_id", None)
        entry.pop("pending_value", None)
        data[key] = entry
        self._write_all(data)
        logger.info("Discarded pending watermark for '%s'", key)

    def set(self, key: str, value: str) -> None:
        data = self._read_all()
        entry = self._normalize_entry(data.get(key))
        entry["value"] = value
        data[key] = entry
        self._write_all(data)
        logger.info("Updated watermark for '%s' -> %s", key, value)