# tests/test_postgres_transactions_ingestion.py
"""Unit tests for PostgresTransactionsIngestion — no live database required.

NOTE ON THIS REVISION
----------------------------------------------------------------------
run() was rewritten to stream chunks via _connect() -> _get_target_watermark()
-> _stream_chunks(), and no longer calls extract_incremental() at all.
The previous version of TestRun still patched extract_incremental(), which
meant those tests exercised a dead code path: with _connect() left
unmocked, run() would attempt a real psycopg2 connection, hit the
tenacity retry/backoff decorator, and either hang/slow the suite or fail
for the wrong reason — while the assertions happened to still pass
(for the rollback test) purely because reconciliation runs before any
connection is attempted. That's a false-positive test, not real coverage.

This revision:
  - Fixes TestRun to mock the actual call chain (_connect, ­
    _get_target_watermark, _stream_chunks) instead of the retired
    extract_incremental().
  - Adds TestBuildWhereClause, unit-testing the three windowing branches
    directly (backfill / incremental / first-ever full load), since
    run() no longer exercises this indirectly.
  - Adds a regression test for the psycopg2 named-cursor bug that was
    just fixed: cur.description is None until after the first
    fetchmany() call, so column extraction must be deferred lazily
    inside the fetch loop rather than read once up front.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import psycopg2
import pytest

from ingestion.config import PostgresConfig
from ingestion.exceptions import SourceConnectionError
from ingestion.postgres_transactions_ingestion import (
    _WATERMARK_KEY,
    PostgresTransactionsIngestion,
)


def _make_config(**overrides) -> PostgresConfig:
    # IMPORTANT: transactions_date_from/transactions_date_to are pinned to
    # "" here on purpose. PostgresConfig pulls them from the environment
    # (POSTGRES_TRANSACTIONS_DATE_FROM / _DATE_TO) via default_factory, so
    # without this override, any local .env left over from manual backfill
    # testing silently flips every "incremental mode" test in this file
    # into backfill mode — begin()/commit()/_reconcile_pending_write() all
    # get skipped, and failures show up as confusing AttributeErrors far
    # from the real cause. Tests that specifically want backfill mode pass
    # those two fields explicitly via overrides.
    defaults = dict(
        host="localhost", port=5432, database="Digital_Money",
        user="test_user", password="test_password", schema="public",
        connect_timeout=10,
        transactions_date_from="", transactions_date_to="",
    )
    defaults.update(overrides)
    return PostgresConfig(**defaults)


def _make_pipeline(watermark_store=None, writer=None, config=None) -> PostgresTransactionsIngestion:
    return PostgresTransactionsIngestion(
        config=config or _make_config(),
        writer=writer or MagicMock(),
        watermark_store=watermark_store or MagicMock(),
        batch_id="batch_test",
    )


def _fake_conn():
    """A MagicMock usable as `with self._connect() as conn:`."""
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    return conn


# ---------------------------------------------------------------------------
# extract_incremental: still the query-building/connection-error paths,
# unchanged behavior — kept as-is, these are unaffected by the streaming
# rewrite since extract_incremental() itself still exists as a method.
# ---------------------------------------------------------------------------

class TestExtractIncremental:
    def test_no_prior_watermark_performs_full_load(self):
        watermark_store = MagicMock()
        watermark_store.get.return_value = None
        pipeline = _make_pipeline(watermark_store=watermark_store)

        fake_conn = _fake_conn()

        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch("ingestion.postgres_transactions_ingestion.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = pd.DataFrame({"transaction_id": ["T1"]})
                pipeline.extract_incremental()

        args, kwargs = mock_read_sql.call_args
        assert "WHERE" not in args[0]

    def test_prior_watermark_performs_filtered_incremental_load(self):
        watermark_store = MagicMock()
        watermark_store.get.return_value = "2026-01-01T00:00:00"
        pipeline = _make_pipeline(watermark_store=watermark_store)

        fake_conn = _fake_conn()

        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch("ingestion.postgres_transactions_ingestion.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = pd.DataFrame({"transaction_id": ["T1"]})
                pipeline.extract_incremental()

        args, kwargs = mock_read_sql.call_args
        assert "WHERE transaction_timestamp >" in args[0]
        assert kwargs["params"] == ("2026-01-01T00:00:00",)

    def test_operational_error_wrapped_as_source_connection_error(self):
        pipeline = _make_pipeline()
        with patch.object(pipeline, "_connect", side_effect=psycopg2.OperationalError("db down")):
            with pytest.raises(SourceConnectionError):
                pipeline.extract_incremental()


# ---------------------------------------------------------------------------
# _build_where_clause: the three windowing branches run() actually relies
# on today. Previously only exercised indirectly (and incorrectly) via
# extract_incremental() in TestRun — now tested directly.
# ---------------------------------------------------------------------------

class TestBuildWhereClause:
    def test_backfill_window_uses_bounded_range(self):
        pipeline = _make_pipeline()
        clause, params = pipeline._build_where_clause(
            last_watermark=None, date_from="2026-01-01", date_to="2026-01-08"
        )
        assert "transaction_timestamp >= %s" in clause
        assert "transaction_timestamp < %s" in clause
        assert params == ("2026-01-01", "2026-01-08")

    def test_incremental_with_prior_watermark_uses_exclusive_lower_bound(self):
        pipeline = _make_pipeline()
        clause, params = pipeline._build_where_clause(
            last_watermark="2026-01-01T00:00:00", date_from=None, date_to=None
        )
        assert clause.strip() == "WHERE transaction_timestamp > %s"
        assert params == ("2026-01-01T00:00:00",)

    def test_first_ever_run_has_no_where_clause(self):
        pipeline = _make_pipeline()
        clause, params = pipeline._build_where_clause(
            last_watermark=None, date_from=None, date_to=None
        )
        assert clause == ""
        assert params == ()

    def test_date_from_equal_date_to_still_builds_exclusive_upper_bound(self):
        """Documented gotcha: date_from == date_to yields an empty result
        set at query time (exclusive upper bound), not an error here.
        _build_where_clause's job is just to emit the clause/params
        correctly — the empty-window behavior is enforced by the SQL
        itself, not by this method, so this only pins the clause shape.
        """
        pipeline = _make_pipeline()
        clause, params = pipeline._build_where_clause(
            last_watermark=None, date_from="2026-01-01", date_to="2026-01-01"
        )
        assert params == ("2026-01-01", "2026-01-01")
        assert "< %s" in clause  # upper bound is strict, so from == to => no rows


# ---------------------------------------------------------------------------
# _stream_chunks: regression test for the psycopg2 named-cursor bug —
# cur.description is None until AFTER the first fetchmany() call.
# ---------------------------------------------------------------------------

class _FakeNamedCursor:
    """Mimics a psycopg2 named (server-side) cursor: .description is None
    until the first fetchmany() has actually been executed against the
    server, then becomes populated for every call after that."""

    def __init__(self, batches):
        self._batches = list(batches)  # list of list-of-tuples
        self._call_count = 0
        self.itersize = None
        self.description = None  # None until first fetchmany()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params):
        self._query = query
        self._params = params

    def fetchmany(self, size):
        if self._call_count >= len(self._batches):
            return []
        rows = self._batches[self._call_count]
        self._call_count += 1
        # Only populated AFTER a fetch has actually happened — this is
        # the exact behavior that broke naive "read description up
        # front" implementations.
        self.description = [("transaction_id",), ("amount",)]
        return rows


class TestStreamChunksLazyColumnInit:
    def test_description_none_before_first_fetch_does_not_break_column_extraction(self):
        batches = [
            [("T1", 10.0), ("T2", 20.0)],
            [("T3", 30.0)],
        ]
        fake_cursor = _FakeNamedCursor(batches)

        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cursor

        pipeline = _make_pipeline()

        chunks = list(pipeline._stream_chunks(fake_conn, last_watermark=None, chunk_size=2))

        assert len(chunks) == 2
        assert list(chunks[0].columns) == ["transaction_id", "amount"]
        assert list(chunks[1].columns) == ["transaction_id", "amount"]
        assert chunks[0]["transaction_id"].tolist() == ["T1", "T2"]
        assert chunks[1]["transaction_id"].tolist() == ["T3"]

    def test_operational_error_during_streaming_wrapped_as_source_connection_error(self):
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.__exit__.return_value = False
        fake_cursor.execute.side_effect = psycopg2.OperationalError("connection lost")
        fake_conn.cursor.return_value = fake_cursor

        pipeline = _make_pipeline()

        with pytest.raises(SourceConnectionError):
            list(pipeline._stream_chunks(fake_conn, last_watermark=None, chunk_size=2))


# ---------------------------------------------------------------------------
# run(): corrected to mock the actual streaming call chain.
# ---------------------------------------------------------------------------

class TestRun:
    def test_successful_run_commits_own_watermark_key(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        watermark_store.get.return_value = None
        writer = MagicMock()
        writer.write_table.side_effect = lambda df, table_name: len(df)
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        fake_conn = _fake_conn()
        fake_chunks = [
            pd.DataFrame({"transaction_id": ["T1", "T2"], "transaction_timestamp": pd.to_datetime(["2026-01-01", "2026-01-02"])}),
            pd.DataFrame({"transaction_id": ["T3"], "transaction_timestamp": pd.to_datetime(["2026-01-03"])}),
        ]

        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch.object(pipeline, "_get_target_watermark", return_value=pd.Timestamp("2026-01-03")):
                with patch.object(pipeline, "_stream_chunks", return_value=iter(fake_chunks)):
                    with patch("ingestion.postgres_transactions_ingestion.validate_required_columns"):
                        result = pipeline.run()

        assert not result.failed
        assert result.chunks_written == 2
        assert result.rows_written == 3

        begin_args = watermark_store.begin.call_args.args
        assert begin_args[0] == _WATERMARK_KEY
        assert begin_args[1] == "batch_test"

        watermark_store.commit.assert_called_once_with(_WATERMARK_KEY, "batch_test")

    def test_no_rows_in_window_skips_begin_and_commit(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        watermark_store.get.return_value = None
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        fake_conn = _fake_conn()

        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch.object(pipeline, "_get_target_watermark", return_value=None):
                result = pipeline.run()

        assert not result.failed
        assert result.chunks_written == 0
        assert result.rows_written == 0
        watermark_store.begin.assert_not_called()
        watermark_store.commit.assert_not_called()
        writer.write_table.assert_not_called()

    def test_backfill_mode_ignores_and_does_not_update_watermark(self):
        watermark_store = MagicMock()
        writer = MagicMock()
        writer.write_table.side_effect = lambda df, table_name: len(df)
        config = _make_config(
            transactions_date_from="2026-01-01", transactions_date_to="2026-01-02"
        )
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer, config=config)

        fake_conn = _fake_conn()
        fake_chunks = [pd.DataFrame({"transaction_id": ["T1"], "transaction_timestamp": pd.to_datetime(["2026-01-01"])})]

        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch.object(pipeline, "_get_target_watermark", return_value=pd.Timestamp("2026-01-01")):
                with patch.object(pipeline, "_stream_chunks", return_value=iter(fake_chunks)):
                    with patch("ingestion.postgres_transactions_ingestion.validate_required_columns"):
                        result = pipeline.run()

        assert not result.failed
        assert result.rows_written == 1
        # Backfill runs must never touch the stored watermark.
        watermark_store.begin.assert_not_called()
        watermark_store.commit.assert_not_called()
        # And reconciliation (which only applies to the incremental path)
        # must not run either, since get_pending is only ever consulted
        # via _reconcile_pending_write() in the non-backfill branch.
        watermark_store.get_pending.assert_not_called()

    def test_pending_write_is_rolled_back_before_extracting(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = ("stale_batch", "2026-01-01T00:00:00")
        watermark_store.get.return_value = None
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        fake_conn = _fake_conn()

        # Nothing new to write after reconciliation — keeps this test
        # focused purely on the rollback behavior.
        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch.object(pipeline, "_get_target_watermark", return_value=None):
                pipeline.run()

        writer.delete_batch.assert_called_once_with("transactions", "stale_batch")
        watermark_store.discard_pending.assert_called_once_with(_WATERMARK_KEY)

    def test_exception_during_streaming_marks_result_failed_and_preserves_pending_watermark(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        watermark_store.get.return_value = None
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        fake_conn = _fake_conn()

        def boom(*args, **kwargs):
            raise SourceConnectionError("connection dropped mid-stream")
            yield  # pragma: no cover - makes this a generator function

        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch.object(pipeline, "_get_target_watermark", return_value=pd.Timestamp("2026-01-03")):
                with patch.object(pipeline, "_stream_chunks", side_effect=boom):
                    result = pipeline.run()

        assert result.failed
        # begin() already ran before the failure — its pending entry must
        # be left in place (NOT discarded) so the next run's
        # _reconcile_pending_write() rolls back the partial Bronze write.
        watermark_store.discard_pending.assert_not_called()
        watermark_store.commit.assert_not_called()