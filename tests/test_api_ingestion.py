
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from ingestion.api_ingestion import ApiIngestion, _WATERMARK_KEY
from ingestion.config import ApiConfig
from ingestion.exceptions import ApiResponseError, SourceConnectionError


def _make_config(**overrides) -> ApiConfig:
    defaults = dict(
        base_url="http://localhost:3000/api",
        transactions_endpoint="/transactions",
        timeout_seconds=5,
        max_retries=3,
        retry_backoff_seconds=1,
        page_size=2,
        auth_token="",
        fixed_date_from="",
        fixed_date_to="",
    )
    defaults.update(overrides)
    return ApiConfig(**defaults)


def _make_pipeline(watermark_store=None, writer=None, config=None) -> ApiIngestion:
    return ApiIngestion(
        config=config or _make_config(),
        writer=writer or MagicMock(),
        watermark_store=watermark_store or MagicMock(),
        batch_id="batch_test",
    )


def _fake_response(payload=None, raise_for_status_exc=None, bad_json=False):
    resp = MagicMock()
    if raise_for_status_exc:
        resp.raise_for_status.side_effect = raise_for_status_exc
    else:
        resp.raise_for_status.return_value = None
    if bad_json:
        resp.json.side_effect = ValueError("not json")
    else:
        resp.json.return_value = payload
    return resp


def _record(txn_id: str, ts: str) -> dict:
    return {
        "transaction_id": txn_id,
        "wallet_id": "WALL-0000001",
        "merchant_id": "MERCH-00001",
        "payment_method_id": "PM-01",
        "device_id": "DEV-01",
        "transaction_timestamp": ts,
        "amount": 10.0,
        "transaction_fee": 0.1,
        "cashback": 0.0,
        "loyalty_points": 1,
        "status": "Success",
        "transaction_type": "Purchase",
        "location_city": "Singapore",
        "currency": "SGD",
        "fraud_flag": "false",
    }


class TestFetchPage:
    def test_http_error_wrapped_as_source_connection_error(self):
        pipeline = _make_pipeline()
        pipeline._session = MagicMock()
        pipeline._session.get.return_value = _fake_response(
            raise_for_status_exc=requests.exceptions.HTTPError("500 server error")
        )

        with pytest.raises(SourceConnectionError):
            pipeline._fetch_page(1, date_from=None)

    def test_invalid_json_wrapped_as_api_response_error(self):
        pipeline = _make_pipeline()
        pipeline._session = MagicMock()
        pipeline._session.get.return_value = _fake_response(bad_json=True)

        with pytest.raises(ApiResponseError):
            pipeline._fetch_page(1, date_from=None)

    def test_missing_data_field_wrapped_as_api_response_error(self):
        pipeline = _make_pipeline()
        pipeline._session = MagicMock()
        pipeline._session.get.return_value = _fake_response(payload={"pagination": {}})

        with pytest.raises(ApiResponseError):
            pipeline._fetch_page(1, date_from=None)

    def test_timeout_retries_then_succeeds(self, monkeypatch):
        
        monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

        pipeline = _make_pipeline()
        pipeline._session = MagicMock()
        good_response = _fake_response(payload={"data": []})
        pipeline._session.get.side_effect = [
            requests.exceptions.Timeout("timed out"),
            requests.exceptions.Timeout("timed out again"),
            good_response,
        ]

        result = pipeline._fetch_page(1, date_from=None)

        assert result == {"data": []}
        assert pipeline._session.get.call_count == 3

    def test_connection_error_exhausts_retries_and_raises(self, monkeypatch):
        monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)

        pipeline = _make_pipeline()
        pipeline._session = MagicMock()
        pipeline._session.get.side_effect = requests.exceptions.ConnectionError("refused")

        with pytest.raises(requests.exceptions.ConnectionError):
            pipeline._fetch_page(1, date_from=None)

        # stop_after_attempt(4) in the decorator — exactly four tries.
        assert pipeline._session.get.call_count == 4



class TestFetchAllPagesPagination:
    def test_terminates_via_links_next(self):
        pipeline = _make_pipeline()
        page1 = {
            "data": [_record("T1", "2026-01-01T00:00:00"), _record("T2", "2026-01-01T01:00:00")],
            "links": {"next": "http://localhost:3000/api/transactions?page=2"},
        }
        page2 = {
            "data": [_record("T3", "2026-01-01T02:00:00")],
            "links": {"next": None},
        }
        with patch.object(pipeline, "_fetch_page", side_effect=[page1, page2]) as mock_fetch:
            records = pipeline._fetch_all_pages(date_from=None)

        assert [r["transaction_id"] for r in records] == ["T1", "T2", "T3"]
        assert mock_fetch.call_count == 2

    def test_terminates_via_pagination_total_pages(self):
        pipeline = _make_pipeline()
        page1 = {
            "data": [_record("T1", "2026-01-01T00:00:00"), _record("T2", "2026-01-01T01:00:00")],
            "pagination": {"page": 1, "total_pages": 2},
        }
        page2 = {
            "data": [_record("T3", "2026-01-01T02:00:00")],
            "pagination": {"page": 2, "total_pages": 2},
        }
        with patch.object(pipeline, "_fetch_page", side_effect=[page1, page2]) as mock_fetch:
            records = pipeline._fetch_all_pages(date_from=None)

        assert [r["transaction_id"] for r in records] == ["T1", "T2", "T3"]
        assert mock_fetch.call_count == 2

    def test_terminates_via_length_heuristic_when_no_metadata_present(self):
        pipeline = _make_pipeline()
        page1 = {"data": [_record("T1", "2026-01-01T00:00:00"), _record("T2", "2026-01-01T01:00:00")]}
        page2 = {"data": [_record("T3", "2026-01-01T02:00:00")]}  # short page -> stop

        with patch.object(pipeline, "_fetch_page", side_effect=[page1, page2]) as mock_fetch:
            records = pipeline._fetch_all_pages(date_from=None)

        assert [r["transaction_id"] for r in records] == ["T1", "T2", "T3"]
        assert mock_fetch.call_count == 2

    def test_length_heuristic_stops_immediately_on_empty_page(self):
        pipeline = _make_pipeline()
        page1 = {"data": []}

        with patch.object(pipeline, "_fetch_page", side_effect=[page1]) as mock_fetch:
            records = pipeline._fetch_all_pages(date_from=None)

        assert records == []
        assert mock_fetch.call_count == 1

    def test_length_heuristic_does_not_loop_forever_on_full_final_page(self):
        pipeline = _make_pipeline()
        page1 = {"data": [_record("T1", "2026-01-01T00:00:00"), _record("T2", "2026-01-01T01:00:00")]}
        page2 = {"data": []}

        with patch.object(pipeline, "_fetch_page", side_effect=[page1, page2]) as mock_fetch:
            records = pipeline._fetch_all_pages(date_from=None)

        assert [r["transaction_id"] for r in records] == ["T1", "T2"]
        assert mock_fetch.call_count == 2

class TestRun:
    def test_incremental_run_filters_records_not_newer_than_watermark(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        watermark_store.get.return_value = "2026-01-01T01:00:00"
        writer = MagicMock()
        writer.write_table.side_effect = lambda df, table_name: len(df)
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        records = [
            _record("T1", "2026-01-01T00:00:00"),
            _record("T2", "2026-01-01T01:00:00"),
            _record("T3", "2026-01-01T02:00:00"),
        ]
        with patch.object(pipeline, "_fetch_all_pages", return_value=records):
            result = pipeline.run()

        assert not result.failed
        assert result.rows_written == 1

        write_args = writer.write_table.call_args
        written_df = write_args.args[0]
        assert list(written_df["transaction_id"]) == ["T3"]

        watermark_store.begin.assert_called_once()
        begin_args = watermark_store.begin.call_args.args
        assert begin_args[0] == _WATERMARK_KEY
        assert begin_args[1] == "batch_test"
        watermark_store.commit.assert_called_once_with(_WATERMARK_KEY, "batch_test")

    def test_first_run_with_no_watermark_writes_everything(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        watermark_store.get.return_value = None
        writer = MagicMock()
        writer.write_table.side_effect = lambda df, table_name: len(df)
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        records = [_record("T1", "2026-01-01T00:00:00"), _record("T2", "2026-01-01T01:00:00")]
        with patch.object(pipeline, "_fetch_all_pages", return_value=records):
            result = pipeline.run()

        assert not result.failed
        assert result.rows_written == 2
        watermark_store.begin.assert_called_once()
        watermark_store.commit.assert_called_once()

    def test_backfill_mode_never_touches_watermark(self):
        watermark_store = MagicMock()
        writer = MagicMock()
        writer.write_table.side_effect = lambda df, table_name: len(df)
        config = _make_config(fixed_date_from="2026-01-01", fixed_date_to="2026-01-02")
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer, config=config)

        records = [_record("T1", "2026-01-01T00:00:00")]
        with patch.object(pipeline, "_fetch_all_pages", return_value=records):
            result = pipeline.run()

        assert not result.failed
        assert result.rows_written == 1
        watermark_store.begin.assert_not_called()
        watermark_store.commit.assert_not_called()
        watermark_store.get_pending.assert_not_called()

    def test_empty_fetch_result_short_circuits_without_writing(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        watermark_store.get.return_value = None
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        with patch.object(pipeline, "_fetch_all_pages", return_value=[]):
            result = pipeline.run()

        assert not result.failed
        assert result.rows_written == 0
        writer.write_table.assert_not_called()
        watermark_store.begin.assert_not_called()
        watermark_store.commit.assert_not_called()

    def test_filter_leaves_nothing_new_short_circuits_without_writing(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        watermark_store.get.return_value = "2026-01-02T00:00:00"
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        records = [_record("T1", "2026-01-01T00:00:00")]
        with patch.object(pipeline, "_fetch_all_pages", return_value=records):
            result = pipeline.run()

        assert not result.failed
        assert result.rows_written == 0
        writer.write_table.assert_not_called()
        watermark_store.begin.assert_not_called()

    def test_pending_write_is_rolled_back_before_extracting(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = ("stale_batch", "2026-01-01T00:00:00")
        watermark_store.get.return_value = None
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        with patch.object(pipeline, "_fetch_all_pages", return_value=[]):
            pipeline.run()

        writer.delete_batch.assert_called_once_with("transactions", "stale_batch")
        watermark_store.discard_pending.assert_called_once_with(_WATERMARK_KEY)

    def test_failure_during_fetch_marks_result_failed_and_preserves_pending_watermark(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        watermark_store.get.return_value = None
        writer = MagicMock()
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        with patch.object(pipeline, "_fetch_all_pages", side_effect=SourceConnectionError("api down")):
            result = pipeline.run()

        assert result.failed
        watermark_store.discard_pending.assert_not_called()
        watermark_store.commit.assert_not_called()

    def test_failure_after_begin_preserves_pending_watermark_for_next_run(self):
        watermark_store = MagicMock()
        watermark_store.get_pending.return_value = None
        watermark_store.get.return_value = None
        writer = MagicMock()
        writer.write_table.side_effect = RuntimeError("bronze write failed")
        pipeline = _make_pipeline(watermark_store=watermark_store, writer=writer)

        records = [_record("T1", "2026-01-01T00:00:00")]
        with patch.object(pipeline, "_fetch_all_pages", return_value=records):
            result = pipeline.run()

        assert result.failed
        watermark_store.begin.assert_called_once()
        watermark_store.commit.assert_not_called()
        watermark_store.discard_pending.assert_not_called()