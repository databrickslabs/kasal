"""Tests for metric_view_validation_utils.measure_table_mapping_parser."""
import json
import textwrap
import pytest

from src.engines.crewai.tools.custom.metric_view_validation_utils.measure_table_mapping_parser import (
    MeasureTableMappingParser,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_MAPPINGS = [
    {
        "measure_name": "Total Sales",
        "dax_expression": "SUM(fact[amount])",
        "proposed_allocation": "fact_sales",
    },
    {
        "measure_name": "Order Count",
        "dax_expression": "COUNT(fact[order_id])",
        "proposed_allocation": "fact_orders",
    },
    {
        "measure_name": "Revenue",
        "dax_expression": "SUM(fact[revenue])",
        "proposed_allocation": "fact_sales",
    },
]


def _parser_with_data(mappings: list, tmp_path) -> MeasureTableMappingParser:
    """Create a parser backed by a real temp file."""
    json_file = tmp_path / "mappings.json"
    json_file.write_text(json.dumps(mappings))
    return MeasureTableMappingParser(str(json_file))


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit:
    def test_stores_path(self, tmp_path):
        p = tmp_path / "m.json"
        p.write_text("[]")
        parser = MeasureTableMappingParser(str(p))
        assert parser.json_path == str(p)
        assert parser.mappings == []
        assert parser._mappings_index == {}


# ---------------------------------------------------------------------------
# load()
# ---------------------------------------------------------------------------

class TestLoad:
    def test_load_returns_list(self, tmp_path):
        parser = _parser_with_data(SAMPLE_MAPPINGS, tmp_path)
        result = parser.load()
        assert isinstance(result, list)
        assert len(result) == 3

    def test_load_builds_index(self, tmp_path):
        parser = _parser_with_data(SAMPLE_MAPPINGS, tmp_path)
        parser.load()
        assert "total sales" in parser._mappings_index
        assert "order count" in parser._mappings_index

    def test_load_empty_file(self, tmp_path):
        json_file = tmp_path / "empty.json"
        json_file.write_text("[]")
        parser = MeasureTableMappingParser(str(json_file))
        result = parser.load()
        assert result == []

    def test_load_raises_on_missing_file(self):
        parser = MeasureTableMappingParser("/nonexistent/path.json")
        with pytest.raises(OSError):
            parser.load()


# ---------------------------------------------------------------------------
# get_measure_by_name()
# ---------------------------------------------------------------------------

class TestGetMeasureByName:
    def test_exact_match(self, tmp_path):
        parser = _parser_with_data(SAMPLE_MAPPINGS, tmp_path)
        m = parser.get_measure_by_name("Total Sales")
        assert m is not None
        assert m["measure_name"] == "Total Sales"

    def test_case_insensitive(self, tmp_path):
        parser = _parser_with_data(SAMPLE_MAPPINGS, tmp_path)
        m = parser.get_measure_by_name("total sales")
        assert m is not None
        m2 = parser.get_measure_by_name("TOTAL SALES")
        assert m2 is not None

    def test_returns_none_for_missing(self, tmp_path):
        parser = _parser_with_data(SAMPLE_MAPPINGS, tmp_path)
        assert parser.get_measure_by_name("nonexistent") is None

    def test_triggers_lazy_load(self, tmp_path):
        parser = _parser_with_data(SAMPLE_MAPPINGS, tmp_path)
        # No explicit load() call; get_measure_by_name should trigger it
        m = parser.get_measure_by_name("Revenue")
        assert m is not None


# ---------------------------------------------------------------------------
# get_measures_for_table()
# ---------------------------------------------------------------------------

class TestGetMeasuresForTable:
    def test_returns_correct_measures(self, tmp_path):
        parser = _parser_with_data(SAMPLE_MAPPINGS, tmp_path)
        results = parser.get_measures_for_table("fact_sales")
        names = {r["measure_name"] for r in results}
        assert names == {"Total Sales", "Revenue"}

    def test_returns_empty_for_unknown_table(self, tmp_path):
        parser = _parser_with_data(SAMPLE_MAPPINGS, tmp_path)
        results = parser.get_measures_for_table("unknown_table")
        assert results == []

    def test_triggers_lazy_load(self, tmp_path):
        parser = _parser_with_data(SAMPLE_MAPPINGS, tmp_path)
        results = parser.get_measures_for_table("fact_orders")
        assert len(results) == 1
        assert results[0]["measure_name"] == "Order Count"
