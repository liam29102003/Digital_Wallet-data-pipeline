
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ingestion import main as main_module

class TestRunTransactionsPipeline:
    def test_postgres_success_short_circuits_and_never_calls_api(self):
        writer = MagicMock()
        watermark_store = MagicMock()

        with patch.object(main_module, "run_postgres_transactions_pipeline", return_value=True) as pg:
            with patch.object(main_module, "run_api_pipeline") as api:
                result = main_module.run_transactions_pipeline(writer, watermark_store, "batch_1")

        assert result is True
        pg.assert_called_once_with(writer, watermark_store, "batch_1")
        api.assert_not_called()

    def test_postgres_failure_falls_back_to_api_and_api_succeeds(self):
        writer = MagicMock()
        watermark_store = MagicMock()

        with patch.object(main_module, "run_postgres_transactions_pipeline", return_value=False) as pg:
            with patch.object(main_module, "run_api_pipeline", return_value=True) as api:
                result = main_module.run_transactions_pipeline(writer, watermark_store, "batch_1")

        assert result is True
        pg.assert_called_once_with(writer, watermark_store, "batch_1")
        api.assert_called_once_with(writer, watermark_store, "batch_1")

    def test_both_postgres_and_api_fail_returns_false(self):
        writer = MagicMock()
        watermark_store = MagicMock()

        with patch.object(main_module, "run_postgres_transactions_pipeline", return_value=False):
            with patch.object(main_module, "run_api_pipeline", return_value=False) as api:
                result = main_module.run_transactions_pipeline(writer, watermark_store, "batch_1")

        assert result is False
        api.assert_called_once()

    def test_postgres_is_always_tried_first(self):
        # Order matters for the "primary vs. fallback" contract — assert
        # Postgres is attempted before API is even considered, using a
        # shared call-order list rather than trusting mock bookkeeping
        # alone.
        call_order = []

        def fake_postgres(*args, **kwargs):
            call_order.append("postgres")
            return False

        def fake_api(*args, **kwargs):
            call_order.append("api")
            return True

        with patch.object(main_module, "run_postgres_transactions_pipeline", side_effect=fake_postgres):
            with patch.object(main_module, "run_api_pipeline", side_effect=fake_api):
                main_module.run_transactions_pipeline(MagicMock(), MagicMock(), "batch_1")

        assert call_order == ["postgres", "api"]


# ---------------------------------------------------------------------------
# main(): overall exit status and per-pipeline isolation
# ---------------------------------------------------------------------------

def _patch_main_dependencies(**pipeline_results):
    """Patch every external dependency main() touches, returning the
    context managers needed so callers can still assert on individual
    mocks inside a `with` block.

    pipeline_results keys: 'csv', 'postgres', 'transactions' (bool each).
    Defaults to True (success) for any key not provided.
    """
    results = {"csv": True, "postgres": True, "transactions": True}
    results.update(pipeline_results)

    writer = MagicMock()
    watermark_store = MagicMock()

    return (
        patch.object(main_module, "get_runtime_config", return_value=MagicMock(
            batch_id="batch_1", state_dir="./state_test"
        )),
        patch.object(main_module, "get_databricks_config", return_value=MagicMock()),
        patch.object(main_module, "BronzeWriter", return_value=writer),
        patch.object(main_module, "WatermarkStore", return_value=watermark_store),
        patch.object(main_module, "run_csv_pipeline", return_value=results["csv"]),
        patch.object(main_module, "run_postgres_pipeline", return_value=results["postgres"]),
        patch.object(main_module, "run_transactions_pipeline", return_value=results["transactions"]),
        writer,
        watermark_store,
    )


class TestMain:
    def test_all_pipelines_succeed_returns_zero(self):
        (p1, p2, p3, p4, p5, p6, p7, writer, watermark_store) = _patch_main_dependencies()
        with p1, p2, p3, p4, p5, p6, p7:
            exit_code = main_module.main()

        assert exit_code == 0
        writer.ensure_schema_exists.assert_called_once()

    def test_any_pipeline_failure_returns_nonzero(self):
        (p1, p2, p3, p4, p5, p6, p7, writer, watermark_store) = _patch_main_dependencies(csv=False)
        with p1, p2, p3, p4, p5, p6, p7:
            exit_code = main_module.main()

        assert exit_code == 1

    def test_transactions_failure_alone_returns_nonzero_even_if_others_succeed(self):
        (p1, p2, p3, p4, p5, p6, p7, writer, watermark_store) = _patch_main_dependencies(transactions=False)
        with p1, p2, p3, p4, p5, p6, p7:
            exit_code = main_module.main()

        assert exit_code == 1

    def test_one_pipeline_failing_does_not_block_the_others_from_running(self):
        # CSV "fails" but Postgres and transactions must still run —
        # per-pipeline isolation is the whole point of the try/except
        # structure in each run_* wrapper.
        (p1, p2, p3, p4, p5, p6, p7, writer, watermark_store) = _patch_main_dependencies(csv=False)
        with p1, p2, p3, p4:
            with patch.object(main_module, "run_csv_pipeline", return_value=False) as csv_fn:
                with patch.object(main_module, "run_postgres_pipeline", return_value=True) as pg_fn:
                    with patch.object(main_module, "run_transactions_pipeline", return_value=True) as txn_fn:
                        exit_code = main_module.main()

        csv_fn.assert_called_once()
        pg_fn.assert_called_once()
        txn_fn.assert_called_once()
        assert exit_code == 1  # still fails overall, but everything ran

    def test_schema_creation_failure_aborts_before_any_pipeline_runs(self):
        writer = MagicMock()
        writer.ensure_schema_exists.side_effect = RuntimeError("cannot reach warehouse")

        with patch.object(main_module, "get_runtime_config", return_value=MagicMock(
            batch_id="batch_1", state_dir="./state_test"
        )):
            with patch.object(main_module, "get_databricks_config", return_value=MagicMock()):
                with patch.object(main_module, "BronzeWriter", return_value=writer):
                    with patch.object(main_module, "WatermarkStore", return_value=MagicMock()):
                        with patch.object(main_module, "run_csv_pipeline") as csv_fn:
                            with patch.object(main_module, "run_postgres_pipeline") as pg_fn:
                                with patch.object(main_module, "run_transactions_pipeline") as txn_fn:
                                    exit_code = main_module.main()

        assert exit_code == 1
        csv_fn.assert_not_called()
        pg_fn.assert_not_called()
        txn_fn.assert_not_called()

    def test_pipelines_run_in_documented_order_csv_then_postgres_then_transactions(self):
        call_order = []

        def track(name):
            def _fn(*args, **kwargs):
                call_order.append(name)
                return True
            return _fn

        with patch.object(main_module, "get_runtime_config", return_value=MagicMock(
            batch_id="batch_1", state_dir="./state_test"
        )):
            with patch.object(main_module, "get_databricks_config", return_value=MagicMock()):
                with patch.object(main_module, "BronzeWriter", return_value=MagicMock()):
                    with patch.object(main_module, "WatermarkStore", return_value=MagicMock()):
                        with patch.object(main_module, "run_csv_pipeline", side_effect=track("csv")):
                            with patch.object(main_module, "run_postgres_pipeline", side_effect=track("postgres")):
                                with patch.object(main_module, "run_transactions_pipeline", side_effect=track("transactions")):
                                    main_module.main()

        assert call_order == ["csv", "postgres", "transactions"]

