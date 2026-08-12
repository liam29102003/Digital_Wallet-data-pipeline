"""Pure parsing of dbt's run_results.json into PipelineRunMetric rows.

Deliberately has NO Airflow imports. The DAG file (airflow/dags/...) is
only importable inside the Docker image (Airflow isn't installed in the
local dev/test environment — see pytest.ini / requirements.txt), so any
logic worth unit-testing has to live somewhere pytest can reach without
Airflow on the path. This module is that place; the DAG just calls into
it.
"""

from __future__ import annotations

import datetime
from typing import List

from ingestion.metrics_writer import PipelineRunMetric


def dbt_results_to_metrics(
    run_results: dict,
    run_id: str,
    pipeline_name: str,
    started_at: datetime.datetime,
) -> List[PipelineRunMetric]:
    """Transform dbt's run_results.json -> a list of PipelineRunMetric rows.

    Produces:
      - one rollup row (stage-level pass/fail/warn counts) for a fast
        at-a-glance status, same shape as before this change.
      - one detail row per node that did NOT pass, carrying the dbt
        unique_id in table_name, its real status, dbt's failure message,
        and (for test nodes) the failing row count in rows_processed —
        so "which test failed and why" is answerable with a WHERE clause
        instead of opening run_results.json by hand.
    """
    results = run_results.get("results", [])
    statuses = [r["status"] for r in results]
    passed = statuses.count("pass") + statuses.count("success")
    failed = statuses.count("fail") + statuses.count("error")
    warned = statuses.count("warn")
    overall_status = "failed" if failed > 0 else "success"

    metrics = [
        PipelineRunMetric(
            run_id=run_id,
            stage="dbt",
            pipeline_name=pipeline_name,
            status=overall_status,
            started_at=started_at,
            tests_passed=passed,
            tests_failed=failed,
            tests_warned=warned,
        )
    ]

    for result in results:
        status = result.get("status")
        if status in ("pass", "success"):
            continue

        exec_seconds = result.get("execution_time")
        node_ended = (
            started_at + datetime.timedelta(seconds=exec_seconds)
            if exec_seconds is not None
            else datetime.datetime.now(datetime.timezone.utc)
        )

        metrics.append(
            PipelineRunMetric(
                run_id=run_id,
                stage="dbt",
                pipeline_name=pipeline_name,
                table_name=result.get("unique_id"),
                status=status,
                started_at=started_at,
                ended_at=node_ended,
                rows_processed=result.get("failures"),
                error_message=result.get("message"),
            )
        )

    return metrics