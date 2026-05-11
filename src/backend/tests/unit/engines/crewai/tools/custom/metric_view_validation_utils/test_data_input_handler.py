"""Tests for metric_view_validation_utils.data_input_handler (DataInputHandler)."""
import json
import textwrap
import pytest
from unittest.mock import MagicMock, patch

from src.engines.crewai.tools.custom.metric_view_validation_utils.data_input_handler import (
    DataInputHandler,
)

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

SIMPLE_YAML = textwrap.dedent("""\
    measures:
      - name: total_sales
        expr: "SUM(source.amount)"
        comment: ""
      - name: pbi_aliased
        expr: "SUM(source.revenue)"
        comment: "PBI: Revenue Measure"
""")

SAMPLE_MAPPINGS = [
    {"measure_name": "total_sales", "dax_expression": "SUM(fact[amount])"},
    {"measure_name": "Revenue Measure", "dax_expression": "SUM(fact[revenue])"},
    {"measure_name": "no_dax", "dax_expression": "Not available"},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_handler(tmp_path) -> DataInputHandler:
    """Create a DataInputHandler backed by real temp files."""
    yaml_file = tmp_path / "mv.yaml"
    yaml_file.write_text(SIMPLE_YAML)

    json_file = tmp_path / "mapping.json"
    json_file.write_text(json.dumps(SAMPLE_MAPPINGS))

    return DataInputHandler(str(yaml_file), str(json_file))


# ---------------------------------------------------------------------------
# __init__ validation
# ---------------------------------------------------------------------------

class TestInit:
    def test_raises_on_empty_metrics_path(self, tmp_path):
        json_file = tmp_path / "m.json"
        json_file.write_text("[]")
        with pytest.raises(ValueError, match="must be provided"):
            DataInputHandler("", str(json_file))

    def test_raises_on_empty_mapping_path(self, tmp_path):
        yaml_file = tmp_path / "mv.yaml"
        yaml_file.write_text("")
        with pytest.raises(ValueError, match="must be provided"):
            DataInputHandler(str(yaml_file), "")

    def test_raises_on_both_empty(self):
        with pytest.raises(ValueError):
            DataInputHandler("", "")

    def test_initialises_caches_to_none(self, tmp_path):
        h = _make_handler(tmp_path)
        assert h._yaml_measures_cache is None
        assert h._dax_measures_cache is None

    def test_table_mappings_default_empty(self, tmp_path):
        h = _make_handler(tmp_path)
        assert h.table_mappings == {}

    def test_table_mappings_stored(self, tmp_path):
        yaml_file = tmp_path / "mv.yaml"
        yaml_file.write_text(SIMPLE_YAML)
        json_file = tmp_path / "m.json"
        json_file.write_text(json.dumps(SAMPLE_MAPPINGS))
        h = DataInputHandler(str(yaml_file), str(json_file), table_mappings={"fact": "source"})
        assert h.table_mappings == {"fact": "source"}


# ---------------------------------------------------------------------------
# get_yaml_measure()
# ---------------------------------------------------------------------------

class TestGetYamlMeasure:
    def test_returns_measure(self, tmp_path):
        h = _make_handler(tmp_path)
        m = h.get_yaml_measure("total_sales")
        assert m is not None
        assert m["name"] == "total_sales"

    def test_returns_none_for_missing(self, tmp_path):
        h = _make_handler(tmp_path)
        assert h.get_yaml_measure("nonexistent") is None

    def test_raises_on_empty_name(self, tmp_path):
        h = _make_handler(tmp_path)
        with pytest.raises(ValueError, match="measure_name cannot be None or empty"):
            h.get_yaml_measure("")

    def test_raises_on_none_name(self, tmp_path):
        h = _make_handler(tmp_path)
        with pytest.raises(ValueError):
            h.get_yaml_measure(None)


# ---------------------------------------------------------------------------
# get_dax_measure()
# ---------------------------------------------------------------------------

class TestGetDaxMeasure:
    def test_returns_measure(self, tmp_path):
        h = _make_handler(tmp_path)
        m = h.get_dax_measure("total_sales")
        assert m is not None
        assert m["measure_name"] == "total_sales"

    def test_case_insensitive(self, tmp_path):
        h = _make_handler(tmp_path)
        assert h.get_dax_measure("TOTAL_SALES") is not None

    def test_returns_none_for_missing(self, tmp_path):
        h = _make_handler(tmp_path)
        assert h.get_dax_measure("nonexistent") is None

    def test_raises_on_empty_name(self, tmp_path):
        h = _make_handler(tmp_path)
        with pytest.raises(ValueError):
            h.get_dax_measure("")


# ---------------------------------------------------------------------------
# get_all_yaml_measures() – cache behaviour
# ---------------------------------------------------------------------------

class TestGetAllYamlMeasures:
    def test_returns_list(self, tmp_path):
        h = _make_handler(tmp_path)
        measures = h.get_all_yaml_measures()
        assert isinstance(measures, list)
        assert len(measures) == 2

    def test_uses_cache_on_second_call(self, tmp_path):
        h = _make_handler(tmp_path)
        first = h.get_all_yaml_measures()
        # Poison the parser so a second load would fail
        h.mv_parser.extract_measures = lambda: (_ for _ in ()).throw(
            AssertionError("should not be called again")
        )
        second = h.get_all_yaml_measures()
        assert first is second


# ---------------------------------------------------------------------------
# get_all_dax_measures() – cache behaviour
# ---------------------------------------------------------------------------

class TestGetAllDaxMeasures:
    def test_returns_list(self, tmp_path):
        h = _make_handler(tmp_path)
        measures = h.get_all_dax_measures()
        assert isinstance(measures, list)
        assert len(measures) == 3

    def test_uses_cache_on_second_call(self, tmp_path):
        h = _make_handler(tmp_path)
        first = h.get_all_dax_measures()
        h.table_mapping_parser.load = lambda: (_ for _ in ()).throw(
            AssertionError("should not be called again")
        )
        second = h.get_all_dax_measures()
        assert first is second


# ---------------------------------------------------------------------------
# find_matching_dax_for_yaml_measure()
# ---------------------------------------------------------------------------

class TestFindMatchingDaxForYamlMeasure:
    def test_strategy1_exact_match(self, tmp_path):
        h = _make_handler(tmp_path)
        yaml_measure = {"name": "total_sales", "expr": "SUM(source.amount)", "comment": ""}
        result = h.find_matching_dax_for_yaml_measure(yaml_measure)
        assert result is not None
        assert result["measure_name"] == "total_sales"

    def test_strategy2_case_insensitive_match(self, tmp_path):
        # 'Total_Sales' (different case) should still find 'total_sales' via
        # the case-insensitive loop in strategy 2
        h = _make_handler(tmp_path)
        yaml_measure = {"name": "Total_Sales", "expr": "SUM(source.amount)", "comment": ""}
        result = h.find_matching_dax_for_yaml_measure(yaml_measure)
        assert result is not None

    def test_strategy3_pbi_comment_match(self, tmp_path):
        h = _make_handler(tmp_path)
        yaml_measure = {
            "name": "pbi_aliased",
            "expr": "SUM(source.revenue)",
            "comment": "PBI: Revenue Measure",
        }
        result = h.find_matching_dax_for_yaml_measure(yaml_measure)
        assert result is not None
        assert result["measure_name"] == "Revenue Measure"

    def test_returns_none_when_no_match(self, tmp_path):
        h = _make_handler(tmp_path)
        yaml_measure = {"name": "totally_unknown", "expr": "SUM(x.y)", "comment": ""}
        result = h.find_matching_dax_for_yaml_measure(yaml_measure)
        assert result is None

    def test_raises_on_none_yaml_measure(self, tmp_path):
        h = _make_handler(tmp_path)
        with pytest.raises(ValueError, match="yaml_measure cannot be None"):
            h.find_matching_dax_for_yaml_measure(None)

    def test_returns_none_when_name_field_missing(self, tmp_path):
        h = _make_handler(tmp_path)
        result = h.find_matching_dax_for_yaml_measure({"expr": "SUM(x.y)"})
        assert result is None

    def test_empty_dict_raises(self, tmp_path):
        h = _make_handler(tmp_path)
        with pytest.raises(ValueError):
            h.find_matching_dax_for_yaml_measure({})
