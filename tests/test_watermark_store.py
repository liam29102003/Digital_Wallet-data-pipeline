# tests/test_watermark_store.py
from pathlib import Path

from ingestion.utils import WatermarkStore


def test_commit_promotes_pending_value(tmp_path: Path):
    store = WatermarkStore(state_dir=tmp_path)
    store.begin("postgres.customers", "batch_1", "2026-01-01T00:00:00")
    assert store.get("postgres.customers") is None  # not committed yet

    store.commit("postgres.customers", "batch_1")
    assert store.get("postgres.customers") == "2026-01-01T00:00:00"
    assert store.get_pending("postgres.customers") is None


def test_get_pending_surfaces_uncommitted_write(tmp_path: Path):
    store = WatermarkStore(state_dir=tmp_path)
    store.begin("postgres.customers", "batch_crashed", "2026-01-02T00:00:00")
    # Simulate a crash: no commit() call happens.

    pending = store.get_pending("postgres.customers")
    assert pending == ("batch_crashed", "2026-01-02T00:00:00")
    assert store.get("postgres.customers") is None  # still no committed watermark


def test_discard_pending_clears_orphaned_entry(tmp_path: Path):
    store = WatermarkStore(state_dir=tmp_path)
    store.begin("postgres.customers", "batch_crashed", "2026-01-02T00:00:00")
    store.discard_pending("postgres.customers")

    assert store.get_pending("postgres.customers") is None
    assert store.get("postgres.customers") is None


def test_commit_ignores_mismatched_batch_id(tmp_path: Path):
    store = WatermarkStore(state_dir=tmp_path)
    store.begin("postgres.customers", "batch_1", "2026-01-01T00:00:00")

    store.commit("postgres.customers", "some_other_batch")  # stale/duplicate commit call
    assert store.get("postgres.customers") is None  # unaffected