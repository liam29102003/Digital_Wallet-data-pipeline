"""Unit tests for ingestion.metrics_writer — no live Spark/Databricks
connection required.

WHAT MATTERS MOST HERE
----------------------------------------------------------------------
MetricsWriter is explicitly best-effort: a failure while WRITING a metric
must never propagate and break the actual pipeline it's describing (see
the try/except wrapping the whole body of log()). That contract is the
single most important thing to pin down — if it regresses silently, a
transient observability-table hiccup could start taking down real
ingestion/dbt runs, which defeats the entire point of the layer.

Also covered:
  - PipelineRunMetric.duration_seconds / to_row() — the shape written
    into observability.pipeline_run_log, since a schema mismatch here
    would only surface at write time against a real cluster.
  - ensure_schema_exists() building the right catalog-qualified schema
    name, and wrapping its own failure as BronzeWriteError (this one is
    NOT best-effort — a missing schema should stop the run early via
    main()'s existing try/except around ensure_schema()/ensure_schema_exists()).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from ingestion.exceptions import BronzeWriteError
from ingestion.metrics_writer import METRICS_SCHEMA, METRICS_TABLE, MetricsWriter, PipelineRunMetric


def _make_metric(**overrides) -> PipelineRunMetric:
    defaults = dict(
        run_id="batch_test",
        stage="ingestion",
        pipeline_name="csv",
        status="success",
        started_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc),
        rows_processed=42,
    )
    defaults.update(overrides)
    return PipelineRunMetric(**defaults)


def _make_metrics_writer(bronze_writer=None) -> MetricsWriter:
    return MetricsWriter(bronze_writer=bronze_writer or MagicMock())


# ---------------------------------------------------------------------------
# PipelineRunMetric: shape and derived fields
# ---------------------------------------------------------------------------

class TestPipelineRunMetric:
    def test_duration_seconds_computed_from_started_and_ended_at(self):
        metric = _make_metric(
            started_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc),
        )
        assert metric.duration_seconds == 30.0

    def test_ended_at_defaults_to_now_if_not_provided(self):
        before = datetime.now(timezone.utc)
        metric = PipelineRunMetric(
            run_id="batch_test",
            stage="ingestion",
            pipeline_name="csv",
            status="success",
            started_at=before,
        )
        after = datetime.now(timezone.utc)

        assert before <= metric.ended_at <= after

    def test_to_row_includes_all_expected_columns(self):
        metric = _make_metric(
            tests_passed=10, tests_failed=2, tests_warned=1, error_message=None
        )
        row = metric.to_row()

        expected_keys = {
            "run_id", "stage", "pipeline_name", "status", "started_at", "ended_at",
            "duration_seconds", "rows_processed", "tests_passed", "tests_failed",
            "tests_warned", "error_message",
        }
        assert set(row.keys()) == expected_keys
        assert row["run_id"] == "batch_test"
        assert row["stage"] == "ingestion"
        assert row["status"] == "success"
        assert row["tests_passed"] == 10
        assert row["tests_failed"] == 2
        assert row["tests_warned"] == 1

    def test_to_row_preserves_none_for_unset_optional_fields(self):
        # A CSV/Postgres ingestion metric never sets tests_* fields —
        # confirm they serialize as None, not 0 or missing, so the Delta
        # table correctly distinguishes "not applicable" from "zero".
        metric = _make_metric()
        row = metric.to_row()

        assert row["tests_passed"] is None
        assert row["tests_failed"] is None
        assert row["tests_warned"] is None
        assert row["error_message"] is None


# ---------------------------------------------------------------------------
# MetricsWriter.ensure_schema_exists: NOT best-effort — failures propagate
# ---------------------------------------------------------------------------

class TestEnsureSchemaExists:
    def test_creates_schema_qualified_with_catalog(self):
        bronze_writer = MagicMock()
        bronze_writer.config.catalog = "digital_wallet"
        spark = MagicMock()
        bronze_writer._get_spark.return_value = spark

        writer = _make_metrics_writer(bronze_writer)
        writer.ensure_schema_exists()

        spark.sql.assert_called_once_with(f"CREATE SCHEMA IF NOT EXISTS digital_wallet.{METRICS_SCHEMA}")

    def test_failure_is_wrapped_as_bronze_write_error_and_propagates(self):
        bronze_writer = MagicMock()
        bronze_writer.config.catalog = "digital_wallet"
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("warehouse unreachable")
        bronze_writer._get_spark.return_value = spark

        writer = _make_metrics_writer(bronze_writer)

        with pytest.raises(BronzeWriteError):
            writer.ensure_schema_exists()


# ---------------------------------------------------------------------------
# MetricsWriter.log: best-effort — failures are swallowed, never raised
# ---------------------------------------------------------------------------

class TestLog:
    def test_successful_write_uses_append_mode_and_merge_schema(self):
        bronze_writer = MagicMock()
        bronze_writer.config.catalog = "digital_wallet"
        spark = MagicMock()
        spark_df = MagicMock()
        spark.createDataFrame.return_value = spark_df
        bronze_writer._get_spark.return_value = spark

        writer = _make_metrics_writer(bronze_writer)
        writer.log(_make_metric())

        spark_df.write.format.assert_called_once_with("delta")
        write_mode_call = spark_df.write.format.return_value.mode
        write_mode_call.assert_called_once_with("append")

        option_call = write_mode_call.return_value.option
        option_call.assert_called_once_with("mergeSchema", "true")

        save_call = option_call.return_value.saveAsTable
        save_call.assert_called_once_with(f"digital_wallet.{METRICS_SCHEMA}.{METRICS_TABLE}")

    def test_write_failure_is_swallowed_not_raised(self):
        # This is the single most important behavior in this module: a
        # broken observability write must never take down the pipeline
        # it's trying to describe.
        bronze_writer = MagicMock()
        bronze_writer.config.catalog = "digital_wallet"
        bronze_writer._get_spark.side_effect = RuntimeError("spark session dead")

        writer = _make_metrics_writer(bronze_writer)

        # Must NOT raise.
        writer.log(_make_metric())

    def test_write_failure_during_save_is_also_swallowed(self):
        bronze_writer = MagicMock()
        bronze_writer.config.catalog = "digital_wallet"
        spark = MagicMock()
        spark.createDataFrame.side_effect = RuntimeError("bad dataframe conversion")
        bronze_writer._get_spark.return_value = spark

        writer = _make_metrics_writer(bronze_writer)

        # Must NOT raise, even though the underlying write blew up.
        writer.log(_make_metric())

    def test_log_passes_a_single_row_dataframe_matching_to_row(self):
        bronze_writer = MagicMock()
        bronze_writer.config.catalog = "digital_wallet"
        spark = MagicMock()
        bronze_writer._get_spark.return_value = spark

        writer = _make_metrics_writer(bronze_writer)
        metric = _make_metric(pipeline_name="postgres_transactions", rows_processed=999)
        writer.log(metric)

        # createDataFrame was called with a pandas DataFrame — assert on
        # its content rather than its type, since we don't want this test
        # coupled to pandas internals.
        call_args = spark.createDataFrame.call_args
        passed_df = call_args.args[0]
        assert len(passed_df) == 1
        assert passed_df.iloc[0]["pipeline_name"] == "postgres_transactions"
        assert passed_df.iloc[0]["rows_processed"] == 999