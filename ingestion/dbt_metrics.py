
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