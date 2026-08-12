from __future__ import annotations

from datetime import datetime, timezone

from ingestion.dbt_metrics import dbt_results_to_metrics


def _make_run_results(*node_results: dict) -> dict:
    return {"results": list(node_results)}


def _node(unique_id: str, status: str, **overrides) -> dict:
    base = {"unique_id": unique_id, "status": status, "execution_time": 0.5}
    base.update(overrides)
    return base


class TestDbtResultsToMetrics:
    def test_all_passing_produces_only_rollup_row(self):
        run_results = _make_run_results(
            _node("model.wallet_dbt.stg_customers", "success"),
            _node("test.wallet_dbt.not_null_stg_customers_customer_id", "pass"),
        )
        metrics = dbt_results_to_metrics(
            run_results, "batch_1", "dbt_test", datetime(2026, 1, 1, tzinfo=timezone.utc)
        )

        assert len(metrics) == 1
        assert metrics[0].status == "success"
        assert metrics[0].tests_passed == 2
        assert metrics[0].tests_failed == 0
        assert metrics[0].table_name is None

    def test_failing_test_produces_rollup_plus_detail_row(self):
        run_results = _make_run_results(
            _node("test.wallet_dbt.not_null_stg_customers_customer_id", "pass"),
            _node(
                "test.wallet_dbt.unique_fact_transactions_transaction_id",
                "fail",
                failures=3,
                message="Got 3 results, configured to fail if != 0",
            ),
        )
        metrics = dbt_results_to_metrics(
            run_results, "batch_1", "dbt_test", datetime(2026, 1, 1, tzinfo=timezone.utc)
        )

        assert len(metrics) == 2
        rollup, detail = metrics
        assert rollup.tests_failed == 1

        assert detail.table_name == "test.wallet_dbt.unique_fact_transactions_transaction_id"
        assert detail.status == "fail"
        assert detail.rows_processed == 3
        assert detail.error_message == "Got 3 results, configured to fail if != 0"

    def test_warn_severity_test_produces_a_detail_row_too(self):
        run_results = _make_run_results(
            _node(
                "test.wallet_dbt.accepted_values_stg_customers_risk_level",
                "warn",
                failures=12,
            ),
        )
        metrics = dbt_results_to_metrics(
            run_results, "batch_1", "dbt_test", datetime(2026, 1, 1, tzinfo=timezone.utc)
        )

        assert len(metrics) == 2
        assert metrics[0].tests_warned == 1
        assert metrics[1].status == "warn"
        assert metrics[1].rows_processed == 12

    def test_run_stage_error_node_also_produces_detail_row(self):
        run_results = _make_run_results(
            _node(
                "model.wallet_dbt.fact_transactions",
                "error",
                message="Runtime Error: relation bronze.transactions not found",
            ),
        )
        metrics = dbt_results_to_metrics(
            run_results, "batch_1", "dbt_run_gold", datetime(2026, 1, 1, tzinfo=timezone.utc)
        )

        assert metrics[0].status == "failed"
        assert metrics[1].table_name == "model.wallet_dbt.fact_transactions"
        assert metrics[1].status == "error"

    def test_missing_execution_time_falls_back_to_now_for_ended_at(self):
        run_results = _make_run_results(
            _node("test.wallet_dbt.some_test", "fail", execution_time=None),
        )
        before = datetime.now(timezone.utc)
        metrics = dbt_results_to_metrics(
            run_results, "batch_1", "dbt_test", datetime(2026, 1, 1, tzinfo=timezone.utc)
        )
        after = datetime.now(timezone.utc)

        assert before <= metrics[1].ended_at <= after