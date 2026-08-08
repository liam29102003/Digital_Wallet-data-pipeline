# tests/test_postgres_transactions_ingestion.py
"""Unit tests for PostgresTransactionsIngestion — no live database required."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import psycopg2
import pytest

from ingestion.config import PostgresConfig
from ingestion.exceptions import SourceConnectionError
from ingestion.postgres_transactions_ingestion import PostgresTransactionsIngestion


def _make_config() -> PostgresConfig:
    return PostgresConfig(
        host="localhost", port=5432, database="Digital_Money",
        user="test_user", password="test_password", schema="public", connect_timeout=10,
    )


def _make_pipeline(watermark_store=None, writer=None) -> PostgresTransactionsIngestion:
    return PostgresTransactionsIngestion(
        config=_make_config(),
        writer=writer or MagicMock(),
        watermark_store=watermark_store or MagicMock(),
        batch_id="batch_test",
    )


class TestExtractIncremental:
    def test_no_prior_watermark_performs_full_load(self):
        watermark_store = MagicMock()
        watermark_store.get.return_value = None
        pipeline = _make_pipeline(watermark_store=watermark_store)

        fake_conn = MagicMock()
        fake_conn.__enter__.return_value = fake_conn
        fake_conn.__exit__.return_value = False

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

        fake_conn = MagicMock()
        fake_conn.__enter__.return_value = fake_conn
        fake_conn.__exit__.return_value = False

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


class TestRun:
    def test_successful_run_commits_own_watermark_key(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        writer = MagicMock()
        writer.write_table.side_effect = lambda df, table_name: len(df)
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        fake_df = pd.DataFrame(
            {"transaction_id": ["T1", "T2"], "transaction_timestamp": pd.to_datetime(["2026-01-01", "2026-01-02"])}
        )
        with patch.object(pipeline, "extract_incremental", return_value=fake_df):
            with patch("ingestion.postgres_transactions_ingestion.validate_required_columns"):
                result = pipeline.run()

        assert not result.failed
        assert result.rows_written == 2
        # Must use its own key, isolated from api.transactions.
        begin_key = watermark_store.begin.call_args.args[0]
        assert begin_key == "postgres.transactions"

    def test_pending_write_is_rolled_back_before_extracting(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = ("stale_batch", "2026-01-01T00:00:00")
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        with patch.object(pipeline, "extract_incremental", return_value=pd.DataFrame()):
            pipeline.run()

        writer.delete_batch.assert_called_once_with("transactions", "stale_batch")
        watermark_store.discard_pending.assert_called_once_with("postgres.transactions")