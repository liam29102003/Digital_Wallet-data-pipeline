from ingestion.reconciliation import ReconciliationResult


def test_matched_when_extracted_equals_written_plus_quarantined():
    result = ReconciliationResult(
        table_name="customers", source_system="postgres",
        extracted_count=100, written_count=97, quarantined_count=3,
    )
    assert result.matched
    assert result.unexplained_gap == 0


def test_mismatch_flags_unexplained_gap():
    result = ReconciliationResult(
        table_name="transactions", source_system="api",
        extracted_count=500, written_count=480, quarantined_count=5,
    )
    assert not result.matched
    assert result.unexplained_gap == 15  # 500 - (480 + 5)


def test_exact_match_with_zero_quarantine():
    result = ReconciliationResult(
        table_name="merchants", source_system="csv",
        extracted_count=500, written_count=500,
    )
    assert result.matched