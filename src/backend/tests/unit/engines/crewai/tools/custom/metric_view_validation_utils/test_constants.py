"""Tests for metric_view_validation_utils.constants."""
import pytest
from src.engines.crewai.tools.custom.metric_view_validation_utils.constants import (
    SQL_KEYWORDS,
    SQL_FUNCTIONS,
    SQL_IDENTIFIER_EXCLUSIONS,
    AGGREGATION_FUNCTIONS,
    DAX_FUNCTIONS,
    DAX_TO_DB_AGG_MAP,
    PBI_COMMENT_MARKER,
    COMPLEXITY_SIMPLE,
    COMPLEXITY_MEDIUM,
    COMPLEXITY_COMPLEX,
    STATUS_VALID,
    STATUS_INVALID,
    STATUS_SKIPPED,
    STATUS_ERROR,
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_LOW,
)


class TestSqlKeywords:
    def test_common_keywords_present(self):
        for kw in ("SUM", "COUNT", "WHERE", "SELECT", "FROM", "AND", "OR"):
            assert kw in SQL_KEYWORDS

    def test_keywords_are_uppercase(self):
        for kw in SQL_KEYWORDS:
            assert kw == kw.upper(), f"Keyword '{kw}' is not upper-case"


class TestSqlFunctions:
    def test_common_functions_present(self):
        for fn in ("UPPER", "LOWER", "ABS", "ROUND", "YEAR", "MONTH"):
            assert fn in SQL_FUNCTIONS

    def test_functions_are_uppercase(self):
        for fn in SQL_FUNCTIONS:
            assert fn == fn.upper(), f"Function '{fn}' is not upper-case"

    def test_no_duplicates_in_source(self):
        # Sets deduplicate automatically; verify the literal list source length equals set
        # (we can only test that the resulting set itself is a proper set object)
        assert isinstance(SQL_FUNCTIONS, set)


class TestSqlIdentifierExclusions:
    def test_is_union_of_keywords_and_functions(self):
        assert SQL_IDENTIFIER_EXCLUSIONS == SQL_KEYWORDS | SQL_FUNCTIONS

    def test_contains_members_from_both(self):
        assert "WHERE" in SQL_IDENTIFIER_EXCLUSIONS   # from SQL_KEYWORDS
        assert "UPPER" in SQL_IDENTIFIER_EXCLUSIONS   # from SQL_FUNCTIONS


class TestAggregationFunctions:
    def test_standard_aggregations_present(self):
        for agg in ("SUM", "COUNT", "AVG", "MIN", "MAX", "STDDEV"):
            assert agg in AGGREGATION_FUNCTIONS

    def test_dax_x_iterators_present(self):
        for agg in ("SUMX", "COUNTX", "AVERAGEX"):
            assert agg in AGGREGATION_FUNCTIONS


class TestDaxToDbAggMap:
    def test_all_keys_are_dax_functions(self):
        for key in DAX_TO_DB_AGG_MAP:
            assert key in DAX_FUNCTIONS or key in AGGREGATION_FUNCTIONS

    def test_sumx_maps_to_sum(self):
        assert DAX_TO_DB_AGG_MAP["SUMX"] == "SUM"

    def test_countx_maps_to_count(self):
        assert DAX_TO_DB_AGG_MAP["COUNTX"] == "COUNT"

    def test_averagex_maps_to_avg(self):
        assert DAX_TO_DB_AGG_MAP["AVERAGEX"] == "AVG"

    def test_identity_mappings(self):
        assert DAX_TO_DB_AGG_MAP["SUM"] == "SUM"
        assert DAX_TO_DB_AGG_MAP["COUNT"] == "COUNT"
        assert DAX_TO_DB_AGG_MAP["AVG"] == "AVG"


class TestStatusAndConfidenceConstants:
    def test_pbi_comment_marker(self):
        assert PBI_COMMENT_MARKER == "PBI:"

    def test_complexity_values(self):
        assert COMPLEXITY_SIMPLE == "simple"
        assert COMPLEXITY_MEDIUM == "medium"
        assert COMPLEXITY_COMPLEX == "complex"

    def test_status_values(self):
        assert STATUS_VALID == "VALID"
        assert STATUS_INVALID == "INVALID"
        assert STATUS_SKIPPED == "SKIPPED"
        assert STATUS_ERROR == "ERROR"

    def test_confidence_values(self):
        assert CONFIDENCE_HIGH == "high"
        assert CONFIDENCE_MEDIUM == "medium"
        assert CONFIDENCE_LOW == "low"
