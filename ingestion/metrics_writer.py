
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from ingestion.databricks_writer import BronzeWriter
from ingestion.exceptions import BronzeWriteError
from ingestion.logger import get_logger

logger = get_logger(__name__)

METRICS_SCHEMA = "observability"
METRICS_TABLE = "pipeline_run_log"


@dataclass
class PipelineRunMetric:
    run_id: str
    stage: str
    pipeline_name: str
    status: str
    started_at: datetime
    ended_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    table_name: Optional[str] = None          # NEW
    rows_processed: Optional[int] = None
    tests_passed: Optional[int] = None
    tests_failed: Optional[int] = None
    tests_warned: Optional[int] = None
    error_message: Optional[str] = None

    @property
    def duration_seconds(self) -> float:
        return (self.ended_at - self.started_at).total_seconds()

    def to_row(self) -> dict:
        return {
            "run_id": self.run_id,
            "stage": self.stage,
            "pipeline_name": self.pipeline_name,
            "table_name": self.table_name,     # NEW
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_seconds": self.duration_seconds,
            "rows_processed": self.rows_processed,
            "tests_passed": self.tests_passed,
            "tests_failed": self.tests_failed,
            "tests_warned": self.tests_warned,
            "error_message": self.error_message,
        }




class MetricsWriter:
    

    def __init__(self, bronze_writer: BronzeWriter) -> None:
        self._bronze_writer = bronze_writer

    def ensure_schema_exists(self) -> None:
        spark = self._bronze_writer._get_spark()
        catalog = self._bronze_writer.config.catalog
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{METRICS_SCHEMA}")
        except Exception as exc:  # noqa: BLE001
            raise BronzeWriteError(
                f"Failed to create/confirm observability schema '{catalog}.{METRICS_SCHEMA}': {exc}"
            ) from exc

    def log(self, metric: PipelineRunMetric) -> None:
        try:
            catalog = self._bronze_writer.config.catalog
            full_table_name = f"{catalog}.{METRICS_SCHEMA}.{METRICS_TABLE}"

            df = pd.DataFrame([metric.to_row()])
            spark = self._bronze_writer._get_spark()
            spark_df = spark.createDataFrame(df)
            (
                spark_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(full_table_name)
            )
            logger.info(
                "Metrics logged: run_id=%s stage=%s pipeline=%s status=%s duration=%.2fs",
                metric.run_id, metric.stage, metric.pipeline_name, metric.status,
                metric.duration_seconds,
            )
        except Exception:
            logger.exception(
                "Failed to write pipeline run metric (run_id=%s pipeline=%s) — "
                "this does NOT fail the pipeline itself, only the observability record.",
                metric.run_id, metric.pipeline_name,
            )