from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from ingestion.databricks_writer import BronzeWriter
from ingestion.logger import get_logger

logger = get_logger(__name__)

RECONCILIATION_SCHEMA = "observability"
RECONCILIATION_TABLE = "reconciliation_log"


@dataclass
class ReconciliationResult:
    """Compares what was extracted from a source against what actually
    landed, accounting for rows deliberately set aside by quarantine.

    matched = True means extracted_count == written_count +
    quarantined_count exactly — every row is accounted for as either
    "written" or "known-rejected". matched = False means rows went
    missing somewhere between extraction and the Bronze write that
    neither a write failure nor quarantine explains — e.g. a silent
    pagination gap in the API source, or a chunk dropped mid-stream.
    """

    table_name: str
    source_system: str
    extracted_count: int
    written_count: int
    quarantined_count: int = 0

    @property
    def unexplained_gap(self) -> int:
        return self.extracted_count - (self.written_count + self.quarantined_count)

    @property
    def matched(self) -> bool:
        return self.unexplained_gap == 0


@dataclass
class ReconciliationWriter:
    """Logs every reconciliation check to observability.reconciliation_log
    — both matches and mismatches. Logging matches too (not just
    failures) is deliberate: it's what lets you later ask "has this
    table ever actually reconciled cleanly?" instead of only seeing
    silence and assuming everything was fine.

    Best-effort like MetricsWriter, not strict like QuarantineWriter —
    a failure to WRITE a reconciliation record is a monitoring gap, not
    a data-loss risk, since the underlying rows themselves are already
    safely in Bronze or quarantine by the time this runs.
    """

    bronze_writer: BronzeWriter

    def ensure_schema_exists(self) -> None:
        spark = self.bronze_writer._get_spark()
        catalog = self.bronze_writer.config.catalog
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{RECONCILIATION_SCHEMA}")

    def log(self, result: ReconciliationResult, run_id: str) -> None:
        catalog = self.bronze_writer.config.catalog
        full_table_name = f"{catalog}.{RECONCILIATION_SCHEMA}.{RECONCILIATION_TABLE}"

        row = pd.DataFrame([{
            "run_id": run_id,
            "table_name": result.table_name,
            "source_system": result.source_system,
            "checked_at": datetime.now(timezone.utc),
            "extracted_count": result.extracted_count,
            "written_count": result.written_count,
            "quarantined_count": result.quarantined_count,
            "unexplained_gap": result.unexplained_gap,
            "matched": result.matched,
        }])

        try:
            spark = self.bronze_writer._get_spark()
            spark_df = spark.createDataFrame(row)
            (
                spark_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(full_table_name)
            )
            if result.matched:
                logger.info(
                    "Reconciliation OK: table='%s' extracted=%d written=%d quarantined=%d",
                    result.table_name, result.extracted_count, result.written_count, result.quarantined_count,
                )
            else:
                logger.error(
                    "Reconciliation MISMATCH: table='%s' extracted=%d written=%d quarantined=%d "
                    "unexplained_gap=%d — %d row(s) went missing between extraction and Bronze",
                    result.table_name, result.extracted_count, result.written_count,
                    result.quarantined_count, result.unexplained_gap, result.unexplained_gap,
                )
        except Exception:
            logger.exception(
                "Failed to persist reconciliation record for table '%s' (run_id=%s) — "
                "this does NOT fail the pipeline, only the monitoring record.",
                result.table_name, run_id,
            )