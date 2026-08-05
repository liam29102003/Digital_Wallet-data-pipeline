"""Unit tests for ingestion.utils — no live connections required."""

import pandas as pd
import pytest

from ingestion.exceptions import EmptyDatasetError, SchemaValidationError
from ingestion.utils import add_ingestion_metadata, ensure_non_empty, validate_required_columns


def test_add_ingestion_metadata_adds_all_required_columns():
    df = pd.DataFrame({"a": [1, 2]})
    stamped = add_ingestion_metadata(df, source_system="csv", batch_id="batch_123")

    assert "_ingested_at" in stamped.columns
    assert "source_system" in stamped.columns
    assert "batch_id" in stamped.columns
    assert (stamped["source_system"] == "csv").all()
    assert (stamped["batch_id"] == "batch_123").all()


def test_validate_required_columns_passes_when_present():
    df = pd.DataFrame({"branch_id": [1], "branch_name": ["x"]})
    validate_required_columns(df, ["branch_id", "branch_name"], "branches")  # should not raise


def test_validate_required_columns_raises_when_missing():
    df = pd.DataFrame({"branch_id": [1]})
    with pytest.raises(SchemaValidationError):
        validate_required_columns(df, ["branch_id", "branch_name"], "branches")


def test_ensure_non_empty_raises_when_not_allowed():
    df = pd.DataFrame()
    with pytest.raises(EmptyDatasetError):
        ensure_non_empty(df, "branches", allow_empty=False)


def test_ensure_non_empty_allows_empty_when_permitted():
    df = pd.DataFrame()
    ensure_non_empty(df, "customers", allow_empty=True)  # should not raise
