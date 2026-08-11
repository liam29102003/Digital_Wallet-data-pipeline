
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
        pipeline = _make_pipeline()
        clause, params = pipeline._build_where_clause(
            last_watermark=None, date_from="2026-01-01", date_to="2026-01-01"
        )
        assert params == ("2026-01-01", "2026-01-01")
        assert "< %s" in clause  # upper bound is strict, so from == to => no rows



class _FakeNamedCursor:
    """Mimics a psycopg2 named (server-side) cursor: .description is None
    until the first fetchmany() has actually been executed against the
    server, then becomes populated for every call after that."""

    def __init__(self, batches):
        self._batches = list(batches) 
        self._call_count = 0
        self.itersize = None
        self.description = None  

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
        watermark_store.begin.assert_not_called()
        watermark_store.commit.assert_not_called()
        watermark_store.get_pending.assert_not_called()

    def test_pending_write_is_rolled_back_before_extracting(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = ("stale_batch", "2026-01-01T00:00:00")
        watermark_store.get.return_value = None
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        fake_conn = _fake_conn()

        
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
        
        watermark_store.discard_pending.assert_not_called()
        watermark_store.commit.assert_not_called()