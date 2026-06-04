"""Tests for metric_view_validation_utils.databricks_parser (UCMetricsViewParser)."""
import io
import textwrap
import pytest
from unittest.mock import mock_open, patch

from src.engines.crewai.tools.custom.metric_view_validation_utils.databricks_parser import (
    UCMetricsViewParser,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIMPLE_YAML = textwrap.dedent("""\
    measures:
      - name: total_sales
        expr: "SUM(source.amount)"
        comment: "Total sales amount"
        display_name: "Total Sales"
        synonyms:
          - sales
      - name: order_count
        expr: "COUNT(source.order_id)"
        comment: ""
        display_name: "Order Count"
""")

YAML_WITH_FILTER = textwrap.dedent("""\
    measures:
      - name: active_sales
        expr: "SUM(source.amount) FILTER (WHERE source.status = 'active')"
        comment: ""
""")

YAML_WITH_DIVISION = textwrap.dedent("""\
    measures:
      - name: avg_order_value
        expr: "SUM(source.amount) / NULLIF(COUNT(source.order_id), 0)"
        comment: ""
""")

EMPTY_YAML = ""


def _make_parser(yaml_content: str) -> UCMetricsViewParser:
    """Create a parser backed by an in-memory YAML string."""
    parser = UCMetricsViewParser("dummy_path.yaml")
    import yaml
    parser.data = yaml.safe_load(yaml_content)
    return parser


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestInit:
    def test_raises_on_empty_path(self):
        with pytest.raises(ValueError, match="yaml_path cannot be None or empty"):
            UCMetricsViewParser("")

    def test_raises_on_none_path(self):
        with pytest.raises(ValueError, match="yaml_path cannot be None or empty"):
            UCMetricsViewParser(None)

    def test_stores_path(self):
        p = UCMetricsViewParser("/path/to/file.yaml")
        assert p.yaml_path == "/path/to/file.yaml"
        assert p.data is None
        assert p.measures == []


class TestCreateHeadless:
    def test_returns_instance(self):
        p = UCMetricsViewParser.create_headless()
        assert isinstance(p, UCMetricsViewParser)

    def test_yaml_path_is_none(self):
        p = UCMetricsViewParser.create_headless()
        assert p.yaml_path is None

    def test_empty_state(self):
        p = UCMetricsViewParser.create_headless()
        assert p.data is None
        assert p.measures == []
        assert p._measures_index == {}

    def test_can_parse_expression_without_file(self):
        p = UCMetricsViewParser.create_headless()
        result = p._parse_measure("SUM(source.amount)")
        assert result["raw"] == "SUM(source.amount)"
        assert len(result["aggregations"]) == 1


# ---------------------------------------------------------------------------
# load()
# ---------------------------------------------------------------------------

class TestLoad:
    def test_load_reads_yaml(self, tmp_path):
        yaml_file = tmp_path / "mv.yaml"
        yaml_file.write_text(SIMPLE_YAML)
        parser = UCMetricsViewParser(str(yaml_file))
        data = parser.load()
        assert "measures" in data
        assert len(data["measures"]) == 2

    def test_load_empty_yaml(self, tmp_path):
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text(EMPTY_YAML)
        parser = UCMetricsViewParser(str(yaml_file))
        data = parser.load()
        assert data is None


# ---------------------------------------------------------------------------
# extract_measures()
# ---------------------------------------------------------------------------

class TestExtractMeasures:
    def test_extracts_all_measures(self):
        parser = _make_parser(SIMPLE_YAML)
        measures = parser.extract_measures()
        assert len(measures) == 2

    def test_measure_fields(self):
        parser = _make_parser(SIMPLE_YAML)
        measures = parser.extract_measures()
        m = measures[0]
        assert m["name"] == "total_sales"
        assert m["expr"] == "SUM(source.amount)"
        assert m["comment"] == "Total sales amount"
        assert m["display_name"] == "Total Sales"
        assert "parsed_expr" in m

    def test_name_index_populated(self):
        parser = _make_parser(SIMPLE_YAML)
        parser.extract_measures()
        assert "total_sales" in parser._measures_index
        assert "order_count" in parser._measures_index

    def test_empty_yaml_returns_empty_list(self, tmp_path):
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text(EMPTY_YAML)
        parser = UCMetricsViewParser(str(yaml_file))
        measures = parser.extract_measures()
        assert measures == []

    def test_idempotent_second_call(self):
        parser = _make_parser(SIMPLE_YAML)
        first = parser.extract_measures()
        second = parser.extract_measures()
        assert len(first) == len(second)


# ---------------------------------------------------------------------------
# get_measure_by_name()
# ---------------------------------------------------------------------------

class TestGetMeasureByName:
    def test_returns_correct_measure(self):
        parser = _make_parser(SIMPLE_YAML)
        m = parser.get_measure_by_name("total_sales")
        assert m is not None
        assert m["name"] == "total_sales"

    def test_returns_none_for_missing(self):
        parser = _make_parser(SIMPLE_YAML)
        assert parser.get_measure_by_name("nonexistent") is None

    def test_case_sensitive(self):
        parser = _make_parser(SIMPLE_YAML)
        # Index is built with the original case
        assert parser.get_measure_by_name("Total_Sales") is None


# ---------------------------------------------------------------------------
# _parse_measure()
# ---------------------------------------------------------------------------

class TestParseMeasure:
    def _p(self):
        return UCMetricsViewParser.create_headless()

    def test_empty_expr_returns_defaults(self):
        result = self._p()._parse_measure("")
        assert result["raw"] == ""
        assert result["aggregations"] == []
        assert result["filters"] == []
        assert isinstance(result["references"], set)

    def test_none_expr_returns_defaults(self):
        result = self._p()._parse_measure(None)
        assert result["raw"] == ""

    def test_simple_sum(self):
        result = self._p()._parse_measure("SUM(source.amount)")
        assert result["raw"] == "SUM(source.amount)"
        assert len(result["aggregations"]) == 1
        assert result["aggregations"][0]["type"] == "SUM"

    def test_division_detected(self):
        result = self._p()._parse_measure("SUM(source.a) / NULLIF(SUM(source.b), 0)")
        assert result["structure"]["is_division"] is True

    def test_filter_detected(self):
        result = self._p()._parse_measure(
            "SUM(source.amount) FILTER (WHERE source.status = 'active')"
        )
        assert result["structure"]["has_filter"] is True

    def test_coalesce_detected(self):
        result = self._p()._parse_measure("COALESCE(SUM(source.amount), 0)")
        assert result["structure"]["has_coalesce"] is True

    def test_nullif_detected(self):
        result = self._p()._parse_measure("NULLIF(SUM(source.amount), 0)")
        assert result["structure"]["has_nullif"] is True


# ---------------------------------------------------------------------------
# _extract_aggregations()
# ---------------------------------------------------------------------------

class TestExtractAggregations:
    def _p(self):
        return UCMetricsViewParser.create_headless()

    def test_single_sum(self):
        aggs = self._p()._extract_aggregations("SUM(source.amount)")
        assert len(aggs) == 1
        assert aggs[0]["type"] == "SUM"

    def test_multiple_aggregations(self):
        aggs = self._p()._extract_aggregations(
            "SUM(source.amount) / NULLIF(COUNT(source.id), 0)"
        )
        types = {a["type"] for a in aggs}
        assert "SUM" in types
        assert "COUNT" in types

    def test_case_insensitive(self):
        aggs = self._p()._extract_aggregations("sum(source.amount)")
        assert len(aggs) == 1
        assert aggs[0]["type"] == "SUM"

    def test_no_aggregations(self):
        aggs = self._p()._extract_aggregations("source.amount + 1")
        assert aggs == []

    def test_dax_x_iterators_detected(self):
        """SUMX and COUNTX should be picked up after fix #5."""
        aggs = self._p()._extract_aggregations("SUMX(source.amount)")
        assert any(a["type"] == "SUMX" for a in aggs)


# ---------------------------------------------------------------------------
# _extract_filters()
# ---------------------------------------------------------------------------

class TestExtractFilters:
    def _p(self):
        return UCMetricsViewParser.create_headless()

    def test_filter_where_extracted(self):
        expr = "SUM(source.amount) FILTER (WHERE source.status = 'active')"
        filters = self._p()._extract_filters(expr)
        assert len(filters) == 1
        assert filters[0]["type"] == "FILTER_WHERE"

    def test_no_filter(self):
        filters = self._p()._extract_filters("SUM(source.amount)")
        assert filters == []

    def test_in_clause_condition(self):
        expr = "SUM(source.amount) FILTER (WHERE source.type IN ('A', 'B'))"
        filters = self._p()._extract_filters(expr)
        assert len(filters) == 1
        cond = filters[0]["parsed_condition"]
        assert cond["type"] == "IN"

    def test_in_clause_bare_column(self):
        """Bare column (no table prefix) IN clause should be parseable."""
        expr = "SUM(source.amount) FILTER (WHERE bic_cwc_type IN ('PET', 'APET'))"
        filters = self._p()._extract_filters(expr)
        assert len(filters) == 1
        cond = filters[0]["parsed_condition"]
        assert cond["type"] == "IN"
        assert cond["column"] == "bic_cwc_type"
        assert "PET" in cond["values"]
        assert "APET" in cond["values"]


# ---------------------------------------------------------------------------
# _extract_references()
# ---------------------------------------------------------------------------

class TestExtractReferences:
    def _p(self):
        return UCMetricsViewParser.create_headless()

    def test_qualified_ref_extracted(self):
        refs = self._p()._extract_references("SUM(source.amount)")
        assert "source.amount" in refs

    def test_string_literals_ignored(self):
        refs = self._p()._extract_references("FILTER (WHERE source.status = 'active.value')")
        assert "active.value" not in refs

    def test_sql_keywords_not_in_refs(self):
        refs = self._p()._extract_references("SUM(source.amount) WHERE source.id > 0")
        for ref in refs:
            assert ref.upper() not in ("SUM", "WHERE")

    def test_multiple_qualified_refs(self):
        refs = self._p()._extract_references("SUM(fact.amount) / COUNT(fact.order_id)")
        assert "fact.amount" in refs
        assert "fact.order_id" in refs


# ---------------------------------------------------------------------------
# _analyze_structure()
# ---------------------------------------------------------------------------

class TestAnalyzeStructure:
    def _p(self):
        return UCMetricsViewParser.create_headless()

    def test_simple_complexity(self):
        s = self._p()._analyze_structure("SUM(source.amount)")
        assert s["complexity"] == "simple"

    def test_medium_complexity_with_filter(self):
        s = self._p()._analyze_structure("SUM(source.amount) FILTER (WHERE x = 1)")
        assert s["complexity"] == "medium"

    def test_complex_with_division_and_filter(self):
        s = self._p()._analyze_structure(
            "SUM(source.a) / COUNT(source.b) FILTER (WHERE source.c = 1)"
        )
        assert s["complexity"] == "complex"


# ---------------------------------------------------------------------------
# _extract_balanced_parens()
# ---------------------------------------------------------------------------

class TestExtractBalancedParens:
    def _p(self):
        return UCMetricsViewParser.create_headless()

    def test_simple(self):
        content = self._p()._extract_balanced_parens("SUM(source.amount)", 3)
        assert content == "source.amount"

    def test_outer_parens(self):
        # Position 5 is the outer '(' of outer(...) – returns full inner content
        content = self._p()._extract_balanced_parens("outer(inner(a,b),c)", 5)
        assert content == "inner(a,b),c"

    def test_nested(self):
        # Position 11 is the '(' of inner(...) – returns just a,b
        content = self._p()._extract_balanced_parens("outer(inner(a,b),c)", 11)
        assert content == "a,b"

    def test_no_paren_at_start(self):
        content = self._p()._extract_balanced_parens("SUM(source)", 0)
        assert content == ""

    def test_out_of_bounds(self):
        content = self._p()._extract_balanced_parens("abc", 100)
        assert content == ""


# ---------------------------------------------------------------------------
# _parse_condition() — bare column support
# ---------------------------------------------------------------------------

class TestParseCondition:
    def _p(self):
        return UCMetricsViewParser.create_headless()

    def test_parse_condition_bare_column_in(self):
        """Bare column (no table prefix) IN clause should parse correctly."""
        cond = self._p()._parse_condition("bic_cwc_type IN ('PET','APET')")
        assert cond["type"] == "IN"
        assert cond["column"] == "bic_cwc_type"
        assert "PET" in cond["values"]
        assert "APET" in cond["values"]

    def test_parse_condition_qualified_column_in(self):
        """Qualified column (with table prefix) IN clause should still work."""
        cond = self._p()._parse_condition("source.bic_cwc_type IN ('PET','APET')")
        assert cond["type"] == "IN"
        assert cond["column"] == "source.bic_cwc_type"
        assert "PET" in cond["values"]

    def test_parse_condition_bare_column_equals(self):
        """Bare column (no table prefix) equality should parse correctly."""
        cond = self._p()._parse_condition("bic_chversion = '0000'")
        assert cond["type"] == "EQUALS"
        assert cond["column"] == "bic_chversion"
        assert cond["value"] == "0000"

    def test_parse_condition_qualified_column_equals(self):
        """Qualified column equality should still work."""
        cond = self._p()._parse_condition("source.status = 'active'")
        assert cond["type"] == "EQUALS"
        assert cond["column"] == "source.status"
        assert cond["value"] == "active"
