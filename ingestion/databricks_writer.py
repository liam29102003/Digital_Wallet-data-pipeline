
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from ingestion.config import DatabricksConfig
from ingestion.exceptions import BronzeWriteError
from ingestion.logger import get_logger

logger = get_logger(__name__)


@dataclass
class BronzeWriter:

    config: DatabricksConfig
    _spark: Optional[object] = None  # lazily-created Spark session (Databricks Connect)

    def _get_spark(self):
        if self._spark is not None:
            return self._spark

        try:
            from databricks.connect import DatabricksSession
        except ImportError as exc:  # pragma: no cover - environment issue
            raise BronzeWriteError(
                "databricks-connect is not installed. Run: pip install databricks-connect"
            ) from exc

        try:
            builder = DatabricksSession.builder.remote(
                host=f"https://{self.config.server_hostname}",
                token=self.config.token,
            )
            if self.config.cluster_id:
                builder = builder.clusterId(self.config.cluster_id)
            else:
                builder = builder.serverless(True)
            self._spark = builder.getOrCreate()
            logger.info("Databricks Connect session established (%s)", self.config.server_hostname)
        except Exception as exc:  # noqa: BLE001 - surface as our own error type
            raise BronzeWriteError(f"Failed to establish Databricks session: {exc}") from exc

        return self._spark

    def ensure_schema_exists(self) -> None:
        spark = self._get_spark()
        full_schema = f"{self.config.catalog}.{self.config.bronze_schema}"
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
            logger.info("Confirmed Bronze schema exists: %s", full_schema)
        except Exception as exc:  # noqa: BLE001
            raise BronzeWriteError(f"Failed to create/confirm schema '{full_schema}': {exc}") from exc

    def write_table(self, df: pd.DataFrame, table_name: str, mode: str = "append") -> int:
        
        if df.empty:
            logger.info("Skipping write for '%s' — DataFrame is empty.", table_name)
            return 0

        full_table_name = f"{self.config.catalog}.{self.config.bronze_schema}.{table_name}"

        try:
            spark = self._get_spark()
            spark_df = spark.createDataFrame(df)
            (
                spark_df.write.format("delta")
                .mode(mode)
                .option("mergeSchema", "true")
                .saveAsTable(full_table_name)
            )
            row_count = len(df)
            logger.info("Wrote %d rows to Bronze table '%s' (mode=%s)", row_count, full_table_name, mode)
            return row_count
        except BronzeWriteError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise BronzeWriteError(f"Failed to write Bronze table '{full_table_name}': {exc}") from exc

    def delete_batch(self, table_name: str, batch_id: str) -> None:
        
        full_table_name = f"{self.config.catalog}.{self.config.bronze_schema}.{table_name}"
        try:
            spark = self._get_spark()
            deleted = spark.sql(
                f"DELETE FROM {full_table_name} WHERE batch_id = '{batch_id}'"
            )
            logger.warning("Rolled back orphaned batch '%s' from Bronze table '%s'", batch_id, full_table_name)
        except Exception as exc:  # noqa: BLE001
            raise BronzeWriteError(
                f"Failed to roll back orphaned batch '{batch_id}' from '{full_table_name}': {exc}"
            ) from exc