from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ingestion.config import CsvConfig
from ingestion.csv_ingestion import CsvIngestion
from ingestion.exceptions import SourceConnectionError

REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_pipeline(input_dir: Path) -> CsvIngestion:
    config = CsvConfig(input_dir=input_dir)
    writer = MagicMock()
    return CsvIngestion(config=config, writer=writer, batch_id="batch_test")


def test_extract_table_reads_and_stamps_merchants():
    pipeline = _make_pipeline(REPO_ROOT / "datasets")
    df = pipeline.extract_table("merchants")

    assert not df.empty
    assert "merchant_id" in df.columns
    assert "source_system" in df.columns
    assert (df["source_system"] == "csv").all()


def test_extract_table_missing_file_raises(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    with pytest.raises(SourceConnectionError):
        pipeline.extract_table("merchants")


def test_run_writes_all_three_tables_and_reports_success():
    pipeline = _make_pipeline(REPO_ROOT / "datasets")
    result = pipeline.run()

    assert result.success
    assert set(result.table_row_counts.keys()) == {"merchants", "devices", "payment_methods"}