import json
from pathlib import Path

import pytest

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



def _seed_legacy_file(state_dir: Path, data: dict) -> Path:
    state_dir.mkdir(parents=True, exist_ok=True)
    watermark_file = state_dir / "watermarks.json"
    watermark_file.write_text(json.dumps(data), encoding="utf-8")
    return watermark_file


class TestLegacyFormatCompatibility:
    def test_get_reads_legacy_bare_string_entry(self, tmp_path: Path):
        _seed_legacy_file(tmp_path, {"postgres.customers": "2024-01-01T00:00:00"})

        store = WatermarkStore(state_dir=tmp_path)
        assert store.get("postgres.customers") == "2024-01-01T00:00:00"

    def test_get_pending_on_legacy_entry_reports_no_pending_write(self, tmp_path: Path):
        
        _seed_legacy_file(tmp_path, {"postgres.customers": "2024-01-01T00:00:00"})

        store = WatermarkStore(state_dir=tmp_path)
        assert store.get_pending("postgres.customers") is None

    def test_begin_on_a_legacy_entry_does_not_lose_the_committed_value(self, tmp_path: Path):
        _seed_legacy_file(tmp_path, {"postgres.customers": "2024-01-01T00:00:00"})

        store = WatermarkStore(state_dir=tmp_path)
        store.begin("postgres.customers", "batch_new", "2026-01-01T00:00:00")

       
        assert store.get("postgres.customers") == "2024-01-01T00:00:00"
        assert store.get_pending("postgres.customers") == ("batch_new", "2026-01-01T00:00:00")

        store.commit("postgres.customers", "batch_new")
        assert store.get("postgres.customers") == "2026-01-01T00:00:00"

    def test_mixed_legacy_and_new_format_entries_in_same_file(self, tmp_path: Path):
        
        _seed_legacy_file(
            tmp_path,
            {
                "postgres.customers": "2024-01-01T00:00:00",  # legacy
                "postgres.wallet_accounts": {"value": "2025-06-01T00:00:00"},  # new shape
            },
        )

        store = WatermarkStore(state_dir=tmp_path)
        assert store.get("postgres.customers") == "2024-01-01T00:00:00"
        assert store.get("postgres.wallet_accounts") == "2025-06-01T00:00:00"
        assert store.get_pending("postgres.customers") is None
        assert store.get_pending("postgres.wallet_accounts") is None

    def test_set_on_legacy_entry_overwrites_with_new_shape(self, tmp_path: Path):
        
        _seed_legacy_file(tmp_path, {"csv.branches": "2024-01-01T00:00:00"})

        store = WatermarkStore(state_dir=tmp_path)
        store.set("csv.branches", "2026-01-01T00:00:00")

        assert store.get("csv.branches") == "2026-01-01T00:00:00"