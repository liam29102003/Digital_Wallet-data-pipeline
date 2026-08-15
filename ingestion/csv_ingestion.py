from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import pandas as pd

from ingestion.config import CSV_TABLE_FILES, NATURAL_KEY_COLUMNS, REQUIRED_COLUMNS, CsvConfig, SourceSystem
from ingestion.databricks_writer import BronzeWriter
from ingestion.exceptions import MalformedSourceDataError, SourceConnectionError
from ingestion.logger import get_logger
from ingestion.quarantine import QuarantineWriter, split_quarantined_rows
from ingestion.reconciliation import ReconciliationResult, ReconciliationWriter
from ingestion.utils import Timer, add_ingestion_metadata, ensure_non_empty, validate_required_columns

logger = get_logger(__name__)

from datetime import datetime, timezone
from ingestion.utils import Timer, TableRunResult, add_ingestion_metadata, ensure_non_empty, validate_required_columns



@dataclass
class CsvIngestionResult:
    table_row_counts: Dict[str, int] = field(default_factory=dict)
    failed_tables: List[str] = field(default_factory=list)
    table_results: List[TableRunResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return not self.failed_tables


class CsvIngestion:
    """Extracts, validates, and loads all CSV reference tables into Bronze."""

    def __init__(
        self,
        config: CsvConfig,
        writer: BronzeWriter,
        batch_id: str,
        quarantine_writer: "QuarantineWriter | None" = None,
        reconciliation_writer: "ReconciliationWriter | None" = None,
    ) -> None:
        self.config = config
        self.writer = writer
        self.batch_id = batch_id
        self.quarantine_writer = quarantine_writer
        self.reconciliation_writer = reconciliation_writer

    def _read_csv(self, file_path: Path, table_name: str) -> pd.DataFrame:
        if not file_path.exists():
            raise SourceConnectionError(f"CSV file not found for table '{table_name}': {file_path}")

        try:
            df = pd.read_csv(file_path)
        except pd.errors.EmptyDataError as exc:
            raise MalformedSourceDataError(f"CSV file for '{table_name}' is empty/has no columns: {file_path}") from exc
        except pd.errors.ParserError as exc:
            raise MalformedSourceDataError(f"CSV file for '{table_name}' is malformed: {file_path} ({exc})") from exc

        logger.info("Read %d rows from %s", len(df), file_path.name)
        return df

    def extract_table(self, table_name: str) -> pd.DataFrame:
        """Extract, validate, and stamp metadata for a single CSV table."""
        filename = CSV_TABLE_FILES[table_name]
        file_path = self.config.input_dir / filename

        logger.info("Extraction started: table='%s' source=csv file=%s", table_name, file_path)
        df = self._read_csv(file_path, table_name)

        ensure_non_empty(df, table_name, allow_empty=False)
        validate_required_columns(df, REQUIRED_COLUMNS[table_name], table_name)

        return add_ingestion_metadata(df, SourceSystem.CSV, self.batch_id)

    def run(self) -> CsvIngestionResult:
        result = CsvIngestionResult()
        logger.info("=== CSV ingestion pipeline started (%d tables) ===", len(CSV_TABLE_FILES))

        with Timer("CSV ingestion pipeline"):
            for table_name in CSV_TABLE_FILES:
                table_started_at = datetime.now(timezone.utc)
                try:
                    df = self.extract_table(table_name)
                    extracted_count = len(df)

                    clean_df, bad_df = split_quarantined_rows(
                        df, NATURAL_KEY_COLUMNS.get(table_name, [])
                    )
                    quarantined_count = len(bad_df)
                    if self.quarantine_writer is not None and quarantined_count:
                        self.quarantine_writer.write(bad_df, table_name, SourceSystem.CSV, self.batch_id)

                    rows_written = self.writer.write_table(clean_df, table_name)

                    if self.reconciliation_writer is not None:
                        self.reconciliation_writer.log(
                            ReconciliationResult(
                                table_name=table_name,
                                source_system=SourceSystem.CSV,
                                extracted_count=extracted_count,
                                written_count=rows_written,
                                quarantined_count=quarantined_count,
                            ),
                            run_id=self.batch_id,
                        )

                    result.table_row_counts[table_name] = rows_written
                    result.table_results.append(TableRunResult(
                        table_name=table_name,
                        rows_written=rows_written,
                        started_at=table_started_at,
                        ended_at=datetime.now(timezone.utc),
                        success=True,
                    ))
                    logger.info("Bronze write success: table='%s' rows=%d", table_name, rows_written)
                except Exception as exc:
                    logger.exception("CSV ingestion failed for table '%s'", table_name)
                    result.failed_tables.append(table_name)
                    result.table_results.append(TableRunResult(
                        table_name=table_name,
                        started_at=table_started_at,
                        ended_at=datetime.now(timezone.utc),
                        success=False,
                        error_message=str(exc),
                    ))

        logger.info(
            "=== CSV ingestion pipeline finished: %d succeeded, %d failed ===",
            len(result.table_row_counts),
            len(result.failed_tables),
        )
        return result