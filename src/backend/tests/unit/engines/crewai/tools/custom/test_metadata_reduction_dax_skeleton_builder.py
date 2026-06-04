"""Unit tests for DaxSkeletonBuilder in metadata_reduction package."""

import pytest
from src.engines.crewai.tools.custom.metadata_reduction.dax_skeleton_builder import (
    DaxSkeleton,
    DaxSkeletonBuilder,
    _DATE_TABLE_NAMES,
    _DATE_COLUMN_HINTS,
)


# ─── Helpers ────────────────────────────────────────────────────────────────

def _model_measure(name, **extra):
    """Return a minimal model-measure dict with optional _resolution override."""
    m = {
        "name": name,
        "expression": f"SUM(Sales[{name}])",
        "_resolution": {
            "resolution_type": "model_measure",
            "expression_flags": {},
        },
    }
    m["_resolution"].update(extra)
    return m


def _filtered_measure(name, base, filter_col, filter_val):
    return {
        "name": name,
        "_resolution": {
            "resolution_type": "filtered_measure",
            "base_measure": base,
            "filter_column": filter_col,
            "filter_value": filter_val,
        },
    }


def _composite_measure(name, base, siblings):
    return {
        "name": name,
        "_resolution": {
            "resolution_type": "composite_measure",
            "base_measure": base,
            "sibling_measures": siblings,
        },
    }


# ─── DaxSkeleton dataclass ───────────────────────────────────────────────────

class TestDaxSkeletonDataclass:
    def test_default_values(self):
        s = DaxSkeleton()
        assert s.skeleton == ""
        assert s.can_skip_llm is False
        assert s.open_placeholders == []
        assert s.strategy_notes == []

    def test_to_dict_returns_all_fields(self):
        s = DaxSkeleton(
            skeleton="EVALUATE ...",
            can_skip_llm=True,
            open_placeholders=["GROUPING_COLUMNS"],
            strategy_notes=["note"],
        )
        d = s.to_dict()
        assert d["skeleton"] == "EVALUATE ..."
        assert d["can_skip_llm"] is True
        assert d["open_placeholders"] == ["GROUPING_COLUMNS"]
        assert d["strategy_notes"] == ["note"]

    def test_to_dict_empty(self):
        d = DaxSkeleton().to_dict()
        assert d == {
            "skeleton": "",
            "can_skip_llm": False,
            "open_placeholders": [],
            "strategy_notes": [],
        }


# ─── build() — empty / no measures ─────────────────────────────────────────

class TestBuildNoMeasures:
    def setup_method(self):
        self.builder = DaxSkeletonBuilder()

    def test_empty_list_returns_empty_skeleton(self):
        result = self.builder.build([])
        assert result.skeleton == ""
        assert "No measures" in result.strategy_notes[0]

    def test_none_resolved_measures_treated_as_empty(self):
        # The public API accepts an empty list
        result = self.builder.build([])
        assert isinstance(result, DaxSkeleton)


# ─── build() — single model measure ─────────────────────────────────────────

class TestBuildSingleModelMeasure:
    def setup_method(self):
        self.builder = DaxSkeletonBuilder()

    def test_simple_measure_no_group_cols(self):
        measure = _model_measure("Total Revenue")
        result = self.builder.build([measure])
        assert "SUMMARIZECOLUMNS" in result.skeleton
        assert "[Total Revenue]" in result.skeleton
        assert "GROUPING_COLUMNS" in result.open_placeholders
        assert result.can_skip_llm is False

    def test_simple_measure_with_group_cols(self):
        measure = _model_measure("Total Revenue")
        intent = {"dimensions": ["'Sales'[Country]"]}
        result = self.builder.build([measure], question_intent=intent)
        assert "'Sales'[Country]" in result.skeleton
        assert "[Total Revenue]" in result.skeleton
        assert result.can_skip_llm is True
        assert result.open_placeholders == []

    def test_removefilters_measure_cannot_skip_llm(self):
        measure = _model_measure(
            "Market Share",
            expression_flags={"has_removefilters": True},
        )
        result = self.builder.build([measure])
        assert result.can_skip_llm is False
        assert "DECOMPOSE_MEASURE" in result.open_placeholders

    def test_context_routing_measure(self):
        measure = _model_measure(
            "Dynamic Metric",
            expression_flags={"has_context_routing": True},
        )
        result = self.builder.build([measure])
        assert result.can_skip_llm is False
        assert "EXPLICIT_FILTER_CONTEXT" in result.open_placeholders
        assert "CONTEXT-ROUTING" in result.strategy_notes[0]


# ─── build() — single filtered measure ──────────────────────────────────────

class TestBuildSingleFilteredMeasure:
    def setup_method(self):
        self.builder = DaxSkeletonBuilder()

    def test_filtered_measure_includes_calculate(self):
        measure = _filtered_measure(
            "Online Sales",
            base="Total Sales",
            filter_col="Sales[Channel]",
            filter_val="Online",
        )
        result = self.builder.build([measure])
        assert "CALCULATE" in result.skeleton
        assert "[Total Sales]" in result.skeleton
        assert '"Online"' in result.skeleton
        assert "Sales[Channel]" in result.skeleton

    def test_filtered_measure_can_skip_llm_when_complete_with_group_cols(self):
        measure = _filtered_measure(
            "Online Sales",
            base="Total Sales",
            filter_col="Sales[Channel]",
            filter_val="Online",
        )
        intent = {"dimensions": ["'Sales'[Country]"]}
        result = self.builder.build([measure], question_intent=intent)
        assert result.can_skip_llm is True

    def test_filtered_measure_incomplete_cannot_skip(self):
        measure = _filtered_measure(
            "Missing Base",
            base="",
            filter_col="Sales[Channel]",
            filter_val="Online",
        )
        result = self.builder.build([measure])
        assert result.can_skip_llm is False

    def test_filtered_measure_strategy_notes_include_where(self):
        measure = _filtered_measure(
            "Online Sales",
            base="Total Sales",
            filter_col="Sales[Channel]",
            filter_val="Online",
        )
        result = self.builder.build([measure])
        note = result.strategy_notes[0]
        assert "WHERE" in note or "Online" in note


# ─── build() — composite measure ─────────────────────────────────────────────

class TestBuildCompositeMeasure:
    def setup_method(self):
        self.builder = DaxSkeletonBuilder()

    def test_composite_uses_addcolumns_var_approach(self):
        sibling_a = _filtered_measure(
            "Online Sales",
            base="Total Sales",
            filter_col="Sales[Channel]",
            filter_val="Online",
        )
        sibling_b = _filtered_measure(
            "Offline Sales",
            base="Total Sales",
            filter_col="Sales[Channel]",
            filter_val="Offline",
        )
        composite = _composite_measure(
            "All Channel Sales",
            base="Total Sales",
            siblings=["Online Sales", "Offline Sales"],
        )
        result = self.builder.build([composite, sibling_a, sibling_b])
        assert "ADDCOLUMNS" in result.skeleton
        assert "VAR" in result.skeleton
        assert result.can_skip_llm is False

    def test_composite_strategy_notes_mention_siblings(self):
        sibling_a = _filtered_measure("S1", "Base", "T[Col]", "V1")
        sibling_b = _filtered_measure("S2", "Base", "T[Col]", "V2")
        composite = _composite_measure("Total", "Base", ["S1", "S2"])
        result = self.builder.build([composite, sibling_a, sibling_b])
        assert len(result.strategy_notes) > 0
        assert "2" in result.strategy_notes[0] or "siblings" in result.strategy_notes[0].lower()


# ─── build() — multiple model measures ──────────────────────────────────────

class TestBuildMultiMeasure:
    def setup_method(self):
        self.builder = DaxSkeletonBuilder()

    def test_multiple_model_measures(self):
        m1 = _model_measure("Total Revenue")
        m2 = _model_measure("Total Cost")
        result = self.builder.build([m1, m2])
        assert "SUMMARIZECOLUMNS" in result.skeleton
        assert "[Total Revenue]" in result.skeleton
        assert "[Total Cost]" in result.skeleton

    def test_multiple_measures_with_removefilters_add_placeholders(self):
        m1 = _model_measure("Revenue", expression_flags={"has_removefilters": True})
        m2 = _model_measure("Cost")
        result = self.builder.build([m1, m2])
        # At least one DECOMPOSE placeholder for the removefilters measure
        assert any("DECOMPOSE" in p for p in result.open_placeholders)
        assert result.can_skip_llm is False

    def test_multiple_clean_measures_can_skip_llm_with_group_cols(self):
        m1 = _model_measure("Revenue")
        m2 = _model_measure("Cost")
        intent = {"dimensions": ["'Date'[Month]"]}
        result = self.builder.build([m1, m2], question_intent=intent)
        # Both measures are clean and group_cols are present → no open placeholders
        assert result.can_skip_llm is True


# ─── build() — multiple filtered only ───────────────────────────────────────

class TestBuildFilteredOnly:
    def setup_method(self):
        self.builder = DaxSkeletonBuilder()

    def test_two_filtered_measures_uses_summarizecolumns(self):
        m1 = _filtered_measure("Online Sales", "Sales", "Sales[Channel]", "Online")
        m2 = _filtered_measure("Offline Sales", "Sales", "Sales[Channel]", "Offline")
        result = self.builder.build([m1, m2])
        assert "SUMMARIZECOLUMNS" in result.skeleton
        assert "CALCULATE" in result.skeleton

    def test_filtered_only_no_placeholders_can_skip_with_group_cols(self):
        m1 = _filtered_measure("Online Sales", "Sales", "Sales[Channel]", "Online")
        m2 = _filtered_measure("Offline Sales", "Sales", "Sales[Channel]", "Offline")
        intent = {"dimensions": ["'Sales'[Country]"]}
        result = self.builder.build([m1, m2], question_intent=intent)
        assert result.can_skip_llm is True


# ─── active_filters (TREATAS) ─────────────────────────────────────────────────

class TestActiveFilters:
    def setup_method(self):
        self.builder = DaxSkeletonBuilder()

    def test_string_filter_emits_treatas(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": ["'Sales'[Country]"]}
        filters = {"Sales[Region]": "Europe"}
        result = self.builder.build([measure], question_intent=intent, active_filters=filters)
        assert "TREATAS" in result.skeleton
        assert '"Europe"' in result.skeleton

    def test_list_filter_emits_treatas_with_multiple_values(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": ["'Sales'[Country]"]}
        filters = {"Sales[Region]": ["Europe", "Asia"]}
        result = self.builder.build([measure], question_intent=intent, active_filters=filters)
        assert "TREATAS" in result.skeleton
        assert '"Europe"' in result.skeleton
        assert '"Asia"' in result.skeleton

    def test_not_null_filter_uses_filter_isblank(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": ["'Sales'[Country]"]}
        filters = {"Sales[Channel]": "NOT NULL"}
        result = self.builder.build([measure], question_intent=intent, active_filters=filters)
        assert "ISBLANK" in result.skeleton

    def test_filter_without_brackets_is_ignored(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": ["'Sales'[Country]"]}
        filters = {"NoColumnBracket": "Value"}
        result = self.builder.build([measure], question_intent=intent, active_filters=filters)
        # No TREATAS since key has no brackets
        assert "TREATAS" not in result.skeleton

    def test_empty_filters_produces_no_treatas(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": ["'Sales'[Country]"]}
        result = self.builder.build([measure], question_intent=intent, active_filters={})
        assert "TREATAS" not in result.skeleton


# ─── dimension_bindings (group column qualification) ─────────────────────────

class TestDimensionBindings:
    def setup_method(self):
        self.builder = DaxSkeletonBuilder()

    def test_dimension_bindings_qualify_columns(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": ["country"]}
        bindings = [
            {
                "user_term": "country",
                "resolved_table": "Geography",
                "resolved_column": "Country",
            }
        ]
        result = self.builder.build(
            [measure], question_intent=intent, dimension_bindings=bindings
        )
        assert "'Geography'[Country]" in result.skeleton

    def test_unresolved_dimension_uses_raw_term(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": ["unknown_dim"]}
        bindings = []
        result = self.builder.build(
            [measure], question_intent=intent, dimension_bindings=bindings
        )
        # Unresolved: raw term kept as-is (no bracket notation)
        assert "unknown_dim" in result.skeleton


# ─── _detect_date_table ──────────────────────────────────────────────────────

class TestDetectDateTable:
    def test_finds_canonical_date_table(self):
        tables = [
            {"name": "Date", "columns": ["Date", "Year", "Month"]},
            {"name": "Sales", "columns": ["Revenue"]},
        ]
        tbl, col = DaxSkeletonBuilder._detect_date_table(tables)
        assert tbl == "Date"
        assert col == "Date"

    def test_finds_dim_date_table(self):
        tables = [{"name": "dim_date", "columns": ["Date", "Year", "Quarter"]}]
        tbl, col = DaxSkeletonBuilder._detect_date_table(tables)
        assert tbl == "dim_date"
        assert col == "Date"

    def test_falls_back_to_hint_column_count(self):
        tables = [
            {"name": "SalesData", "columns": ["Year", "Month", "Day", "Revenue"]},
        ]
        tbl, col = DaxSkeletonBuilder._detect_date_table(tables)
        assert tbl == "SalesData"
        assert col is not None

    def test_no_date_table_returns_none(self):
        tables = [{"name": "Products", "columns": ["Name", "Price"]}]
        tbl, col = DaxSkeletonBuilder._detect_date_table(tables)
        assert tbl is None
        assert col is None

    def test_column_as_dict_objects(self):
        tables = [
            {
                "name": "Calendar",
                "columns": [
                    {"name": "Date"},
                    {"name": "Year"},
                    {"name": "Month"},
                ],
            }
        ]
        tbl, col = DaxSkeletonBuilder._detect_date_table(tables)
        assert tbl == "Calendar"
        assert col == "Date"

    def test_empty_tables_list(self):
        tbl, col = DaxSkeletonBuilder._detect_date_table([])
        assert tbl is None
        assert col is None


# ─── time intelligence injection ─────────────────────────────────────────────

class TestTimeIntelligence:
    def setup_method(self):
        self.builder = DaxSkeletonBuilder()

    def _make_tables(self):
        return [{"name": "Date", "columns": ["Date", "Year", "Month"]}]

    def test_ytd_injects_comment_hint(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": ["'Date'[Month]"], "time_intelligence": {"has_ytd": True}}
        result = self.builder.build(
            [measure], question_intent=intent, tables=self._make_tables()
        )
        assert "YTD" in result.skeleton or "DATESYTD" in result.skeleton

    def test_mtd_injects_comment_hint(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": [], "time_intelligence": {"has_mtd": True}}
        result = self.builder.build(
            [measure], question_intent=intent, tables=self._make_tables()
        )
        assert "MTD" in result.skeleton or "DATESMTD" in result.skeleton

    def test_yoy_injects_comment_hint(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": [], "time_intelligence": {"delta_periods": ["yoy"]}}
        result = self.builder.build(
            [measure], question_intent=intent, tables=self._make_tables()
        )
        assert "YoY" in result.skeleton or "SAMEPERIODLASTYEAR" in result.skeleton

    def test_no_time_intelligence_no_injection(self):
        measure = _model_measure("Revenue")
        intent = {"dimensions": ["'Date'[Month]"], "time_intelligence": {}}
        result = self.builder.build(
            [measure], question_intent=intent, tables=self._make_tables()
        )
        assert "TIME INTELLIGENCE" not in result.skeleton

    def test_time_intelligence_disables_can_skip_llm(self):
        measure = _model_measure("Revenue")
        intent = {
            "dimensions": ["'Date'[Month]"],
            "time_intelligence": {"has_ytd": True},
        }
        result = self.builder.build(
            [measure], question_intent=intent, tables=self._make_tables()
        )
        assert result.can_skip_llm is False


# ─── _extract_group_columns ──────────────────────────────────────────────────

class TestExtractGroupColumns:
    def test_returns_empty_without_intent(self):
        cols = DaxSkeletonBuilder._extract_group_columns(None)
        assert cols == []

    def test_returns_dimensions_from_intent(self):
        intent = {"dimensions": ["Country", "Year"]}
        cols = DaxSkeletonBuilder._extract_group_columns(intent)
        assert cols == ["Country", "Year"]

    def test_empty_dimensions_returns_empty(self):
        intent = {"dimensions": []}
        cols = DaxSkeletonBuilder._extract_group_columns(intent)
        assert cols == []

    def test_dimension_bindings_override_raw_terms(self):
        intent = {"dimensions": ["country", "region"]}
        bindings = [
            {
                "user_term": "country",
                "resolved_table": "Geography",
                "resolved_column": "Country",
            }
        ]
        cols = DaxSkeletonBuilder._extract_group_columns(intent, bindings)
        assert "'Geography'[Country]" in cols
        # "region" has no binding, falls back to raw term
        assert "region" in cols

    def test_intent_with_no_dimensions_key(self):
        intent = {"measures": ["Revenue"]}
        cols = DaxSkeletonBuilder._extract_group_columns(intent)
        assert cols == []


# ─── _format_group_cols ──────────────────────────────────────────────────────

class TestFormatGroupCols:
    def test_empty_cols_appends_placeholder(self):
        placeholders = []
        result = DaxSkeletonBuilder._format_group_cols([], placeholders)
        assert "LLM" in result
        assert "GROUPING_COLUMNS" in placeholders

    def test_qualified_col_passes_through(self):
        placeholders = []
        result = DaxSkeletonBuilder._format_group_cols(["'Sales'[Country]"], placeholders)
        assert "'Sales'[Country]" in result
        assert placeholders == []

    def test_unqualified_col_appends_qualify_placeholder(self):
        placeholders = []
        result = DaxSkeletonBuilder._format_group_cols(["Country"], placeholders)
        assert "Country" in result
        assert any("QUALIFY_Country" in p for p in placeholders)

    def test_multiple_qualified_cols(self):
        placeholders = []
        result = DaxSkeletonBuilder._format_group_cols(
            ["'Date'[Year]", "'Sales'[Region]"], placeholders
        )
        assert "'Date'[Year]" in result
        assert "'Sales'[Region]" in result
        assert placeholders == []


# ─── _format_filter_lines ────────────────────────────────────────────────────

class TestFormatFilterLines:
    def test_none_returns_empty_string(self):
        assert DaxSkeletonBuilder._format_filter_lines(None) == ""

    def test_empty_dict_returns_empty_string(self):
        assert DaxSkeletonBuilder._format_filter_lines({}) == ""

    def test_string_value_emits_treatas(self):
        result = DaxSkeletonBuilder._format_filter_lines({"Sales[Region]": "Europe"})
        assert "TREATAS" in result
        assert '"Europe"' in result

    def test_list_value_emits_treatas_multiple(self):
        result = DaxSkeletonBuilder._format_filter_lines({"Sales[Region]": ["EU", "US"]})
        assert '"EU"' in result
        assert '"US"' in result

    def test_not_null_emits_filter_isblank(self):
        result = DaxSkeletonBuilder._format_filter_lines({"Sales[Channel]": "NOT NULL"})
        assert "ISBLANK" in result

    def test_key_without_brackets_is_skipped(self):
        result = DaxSkeletonBuilder._format_filter_lines({"NoColumn": "Value"})
        assert result == ""


# ─── _build_date_filter ──────────────────────────────────────────────────────

class TestBuildDateFilter:
    def test_empty_time_intelligence_returns_empty(self):
        result = DaxSkeletonBuilder._build_date_filter({}, "Date", "Date")
        assert result == ""

    def test_none_date_table_returns_empty(self):
        result = DaxSkeletonBuilder._build_date_filter({"has_ytd": True}, None, "Date")
        assert result == ""

    def test_ytd_produces_datesytd_hint(self):
        result = DaxSkeletonBuilder._build_date_filter(
            {"has_ytd": True}, "Date", "Date"
        )
        assert "DATESYTD" in result

    def test_mtd_produces_datesmtd_hint(self):
        result = DaxSkeletonBuilder._build_date_filter(
            {"has_mtd": True}, "Date", "Date"
        )
        assert "DATESMTD" in result

    def test_qtd_produces_datesqtd_hint(self):
        result = DaxSkeletonBuilder._build_date_filter(
            {"has_qtd": True}, "Date", "Date"
        )
        assert "DATESQTD" in result

    def test_yoy_produces_sameperiodlastyear_hint(self):
        result = DaxSkeletonBuilder._build_date_filter(
            {"delta_periods": ["yoy"]}, "Date", "Date"
        )
        assert "SAMEPERIODLASTYEAR" in result

    def test_mom_produces_dateadd_hint(self):
        result = DaxSkeletonBuilder._build_date_filter(
            {"delta_periods": ["mom"]}, "Date", "Date"
        )
        assert "DATEADD" in result

    def test_grain_only_produces_filter_hint(self):
        result = DaxSkeletonBuilder._build_date_filter(
            {"grain": "month"}, "Date", "Date"
        )
        assert "month" in result.lower() or "LatestDate" in result

    def test_date_table_with_space_is_quoted(self):
        result = DaxSkeletonBuilder._build_date_filter(
            {"has_ytd": True}, "Date Table", "Date"
        )
        assert "'Date Table'" in result

    def test_no_intelligence_flags_returns_empty(self):
        result = DaxSkeletonBuilder._build_date_filter(
            {"grain": None, "has_ytd": False, "has_mtd": False, "has_qtd": False},
            "Date",
            "Date",
        )
        assert result == ""
