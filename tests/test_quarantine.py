import pandas as pd

from ingestion.quarantine import split_quarantined_rows


def test_rows_with_null_natural_key_are_quarantined():
    df = pd.DataFrame({
        "customer_id": ["C1", None, "C3"],
        "email": ["a@x.com", "b@x.com", "c@x.com"],
    })

    clean, quarantined = split_quarantined_rows(df, ["customer_id"])

    assert list(clean["customer_id"]) == ["C1", "C3"]
    assert list(quarantined["customer_id"]) == [None]
    assert "null natural key column(s): customer_id" in quarantined["_quarantine_reason"].iloc[0]


def test_no_bad_rows_returns_empty_quarantine_frame():
    df = pd.DataFrame({"customer_id": ["C1", "C2"]})
    clean, quarantined = split_quarantined_rows(df, ["customer_id"])

    assert len(clean) == 2
    assert quarantined.empty


def test_composite_natural_key_flags_row_if_any_column_is_null():
    df = pd.DataFrame({
        "a": ["A1", "A2", None],
        "b": ["B1", None, "B3"],
    })
    clean, quarantined = split_quarantined_rows(df, ["a", "b"])

    assert len(clean) == 1
    assert len(quarantined) == 2


def test_missing_key_column_in_dataframe_quarantines_nothing():
    # Schema-level problem — validate_required_columns() is responsible
    # for catching this upstream, not this function.
    df = pd.DataFrame({"other_col": [1, 2]})
    clean, quarantined = split_quarantined_rows(df, ["customer_id"])

    assert len(clean) == 2
    assert quarantined.empty