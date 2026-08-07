"""Unit tests for PostgresIngestion — no live database required.

Covers the pieces that carry the most real-world risk: watermark
selection (first run vs. incremental), connection retry/backoff behavior,
error wrapping into our own exception hierarchy, and run()'s
all-or-nothing-per-table orchestration (one table failing must not stop
the others, and the watermark must only advance on a successful write).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import psycopg2
import pytest

from ingestion.config import PostgresConfig
from ingestion.exceptions import SourceConnectionError
from ingestion.postgres_ingestion import PostgresIngestion


def _make_config() -> PostgresConfig:
    # Bypass env/.env entirely — every field is required, so pass explicit
    # values (mirrors the pattern used in test_csv_ingestion.py).
    return PostgresConfig(
        host="localhost",
        port=5432,
        database="Digital_Money",
        user="test_user",
        password="test_password",
        schema="public",
        connect_timeout=10,
    )


def _make_pipeline(watermark_store=None, writer=None) -> PostgresIngestion:
    return PostgresIngestion(
        config=_make_config(),
        writer=writer or MagicMock(),
        watermark_store=watermark_store or MagicMock(),
        batch_id="batch_test",
    )


# ---------------------------------------------------------------------------
# extract_incremental: watermark selection
# ---------------------------------------------------------------------------

class TestExtractIncremental:
    def test_no_prior_watermark_performs_full_load(self):
        watermark_store = MagicMock()
        watermark_store.get.return_value = None
        pipeline = _make_pipeline(watermark_store=watermark_store)

        fake_conn = MagicMock()
        fake_conn.__enter__.return_value = fake_conn
        fake_conn.__exit__.return_value = False

        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch("ingestion.postgres_ingestion.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = pd.DataFrame({"customer_id": ["C1"]})
                pipeline.extract_incremental("customers", "updated_at")

        args, kwargs = mock_read_sql.call_args
        query = args[0]
        assert "WHERE" not in query
        assert "params" not in kwargs or kwargs.get("params") is None

    def test_prior_watermark_performs_filtered_incremental_load(self):
        watermark_store = MagicMock()
        watermark_store.get.return_value = "2024-01-01T00:00:00"
        pipeline = _make_pipeline(watermark_store=watermark_store)

        fake_conn = MagicMock()
        fake_conn.__enter__.return_value = fake_conn
        fake_conn.__exit__.return_value = False

        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch("ingestion.postgres_ingestion.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = pd.DataFrame({"customer_id": ["C1"]})
                pipeline.extract_incremental("customers", "updated_at")

        args, kwargs = mock_read_sql.call_args
        query = args[0]
        assert "WHERE updated_at >" in query
        assert kwargs["params"] == ("2024-01-01T00:00:00",)

    def test_operational_error_wrapped_as_source_connection_error(self):
        pipeline = _make_pipeline()
        with patch.object(pipeline, "_connect", side_effect=psycopg2.OperationalError("db down")):
            with pytest.raises(SourceConnectionError):
                pipeline.extract_incremental("customers", "updated_at")

    def test_query_failure_wrapped_as_source_connection_error(self):
        pipeline = _make_pipeline()
        fake_conn = MagicMock()
        fake_conn.__enter__.return_value = fake_conn
        fake_conn.__exit__.return_value = False

        with patch.object(pipeline, "_connect", return_value=fake_conn):
            with patch("ingestion.postgres_ingestion.pd.read_sql", side_effect=ValueError("bad query")):
                with pytest.raises(SourceConnectionError):
                    pipeline.extract_incremental("customers", "updated_at")


# ---------------------------------------------------------------------------
# _connect: retry/backoff behavior (the piece unique to Postgres)
# ---------------------------------------------------------------------------

class TestConnectRetry:
    def test_retries_on_operational_error_then_succeeds(self, monkeypatch):
        # Retry backoff would otherwise sleep for real between attempts.
        monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

        pipeline = _make_pipeline()
        fake_connection = MagicMock()
        connect_mock = MagicMock(
            side_effect=[
                psycopg2.OperationalError("attempt 1 failed"),
                psycopg2.OperationalError("attempt 2 failed"),
                fake_connection,
            ]
        )

        with patch("ingestion.postgres_ingestion.psycopg2.connect", connect_mock):
            result = pipeline._connect()

        assert result is fake_connection
        assert connect_mock.call_count == 3

    def test_exhausts_retries_and_raises_operational_error(self, monkeypatch):
        monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

        pipeline = _make_pipeline()
        connect_mock = MagicMock(side_effect=psycopg2.OperationalError("db unreachable"))

        with patch("ingestion.postgres_ingestion.psycopg2.connect", connect_mock):
            with pytest.raises(psycopg2.OperationalError):
                pipeline._connect()

        # stop_after_attempt(3) — exactly three tries, not more, not fewer.
        assert connect_mock.call_count == 3


# ---------------------------------------------------------------------------
# run(): per-table isolation + watermark advancement
# ---------------------------------------------------------------------------

class TestRun:
    def test_successful_run_writes_all_tables_and_advances_watermarks(self):
        watermark_store = MagicMock()
        watermark_store.get.return_value = None
        writer = MagicMock()
        writer.write_table.side_effect = lambda df, table_name: len(df)
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        def fake_extract(table_name, watermark_column):
            return pd.DataFrame(
                {
                    "id": ["A1", "A2"],
                    watermark_column: pd.to_datetime(["2024-01-01", "2024-01-02"]),
                }
            )

        with patch.object(pipeline, "extract_incremental", side_effect=fake_extract):
            result = pipeline.run()

        assert result.success
        assert result.table_row_counts == {"customers": 2, "wallet_accounts": 2}
        assert writer.write_table.call_count == 2
        # Watermark should advance to the max of the extracted watermark column.
        set_calls = {call.args[0]: call.args[1] for call in watermark_store.set.call_args_list}
        assert set_calls["postgres.customers"] == "2024-01-02T00:00:00"
        assert set_calls["postgres.wallet_accounts"] == "2024-01-02T00:00:00"

    def test_empty_incremental_result_is_recorded_without_watermark_update(self):
        watermark_store = MagicMock()
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        with patch.object(pipeline, "extract_incremental", return_value=pd.DataFrame()):
            result = pipeline.run()

        assert result.success
        assert result.table_row_counts == {"customers": 0, "wallet_accounts": 0}
        writer.write_table.assert_not_called()
        watermark_store.set.assert_not_called()

    def test_one_table_failing_does_not_block_the_other(self):
        watermark_store = MagicMock()
        watermark_store.get.return_value = None
        writer = MagicMock()
        writer.write_table.side_effect = lambda df, table_name: len(df)
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        def fake_extract(table_name, watermark_column):
            if table_name == "customers":
                raise SourceConnectionError("customers source unreachable")
            return pd.DataFrame({"id": ["W1"], watermark_column: pd.to_datetime(["2024-01-01"])})

        with patch.object(pipeline, "extract_incremental", side_effect=fake_extract):
            result = pipeline.run()

        assert not result.success
        assert result.failed_tables == ["customers"]
        assert result.table_row_counts == {"wallet_accounts": 1}
        # Only the table that actually succeeded should have written/advanced.
        assert writer.write_table.call_count == 1