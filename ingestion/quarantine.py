from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Tuple

import pandas as pd

from ingestion.databricks_writer import BronzeWriter
from ingestion.logger import get_logger

logger = get_logger(__name__)

QUARANTINE_SCHEMA = "observability"
QUARANTINE_TABLE = "quarantine_records"


def split_quarantined_rows(
    df: pd.DataFrame,
    natural_key_columns: List[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split a DataFrame into (clean_rows, quarantined_rows).

    A row is quarantined if ANY of its natural key columns is null — a
    null natural key breaks dedup, SCD2 snapshotting, and every
    downstream join identically regardless of source system, so it's
    the one row-level invariant applied uniformly here. Column-level
    problems (a whole required column missing) are NOT this function's
    job — validate_required_columns() already raises hard on that,
    upstream of this call, because that's a schema problem, not a
    row problem.
    """
    missing_key_cols = [c for c in natural_key_columns if c not in df.columns]
    if missing_key_cols:
        return df, df.iloc[0:0].copy()

    is_bad = df[natural_key_columns].isnull().any(axis=1)
    clean = df[~is_bad].copy()
    quarantined = df[is_bad].copy()

    if not quarantined.empty:
        def _reason(row) -> str:
            nulls = [c for c in natural_key_columns if pd.isnull(row[c])]
            return f"null natural key column(s): {', '.join(nulls)}"

        quarantined["_quarantine_reason"] = quarantined.apply(_reason, axis=1)

    return clean, quarantined


@dataclass
class QuarantineWriter:
    """Writes rejected rows to observability.quarantine_records instead
    of silently dropping them or failing the whole table's batch.

    NOT best-effort like MetricsWriter: losing a metric is tolerable,
    silently losing the only record of *why* data was rejected is not.
    Failures here are re-raised, not swallowed — but by the point this
    runs, the clean rows have normally already written successfully, so
    a quarantine-write failure doesn't undo good work, it just needs to
    be seen.
    """

    bronze_writer: BronzeWriter

    def ensure_schema_exists(self) -> None:
        spark = self.bronze_writer._get_spark()
        catalog = self.bronze_writer.config.catalog
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{QUARANTINE_SCHEMA}")

    def write(
        self,
        quarantined_df: pd.DataFrame,
        table_name: str,
        source_system: str,
        batch_id: str,
    ) -> int:
        if quarantined_df.empty:
            return 0

        catalog = self.bronze_writer.config.catalog
        full_table_name = f"{catalog}.{QUARANTINE_SCHEMA}.{QUARANTINE_TABLE}"

        record = pd.DataFrame({
            "run_id": batch_id,
            "table_name": table_name,
            "source_system": source_system,
            "quarantined_at": datetime.now(timezone.utc),
            "reason": quarantined_df["_quarantine_reason"].values,
            # Full original row preserved as JSON so nothing is lost —
            # if a rule turns out to be a false positive later, the row
            # is parked, not gone.
            "row_data": quarantined_df.drop(columns=["_quarantine_reason"]).apply(
                lambda r: r.to_json(date_format="iso"), axis=1
            ).values,
        })

        try:
            spark = self.bronze_writer._get_spark()
            spark_df = spark.createDataFrame(record)
            (
                spark_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(full_table_name)
            )
            logger.warning(
                "Quarantined %d row(s) from table '%s' (batch_id=%s) — see %s",
                len(record), table_name, batch_id, full_table_name,
            )
        except Exception:
            logger.exception(
                "Failed to persist %d quarantined row(s) for table '%s' "
                "(batch_id=%s) — these rows were EXCLUDED from the Bronze "
                "write but their quarantine record could not be saved.",
                len(record), table_name, batch_id,
            )
            raise

        return len(record)