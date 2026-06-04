"""Unit tests for MeasureResolver in metadata_reduction package."""

import pytest
from src.engines.crewai.tools.custom.metadata_reduction.measure_resolver import (
    ExpressionFlags,
    MeasureResolver,
    MeasureType,
    ResolvedMeasure,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────

def _flat_measures():
    return [
        {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"},
        {"name": "Total Cost", "expression": "SUM(Sales[Cost])", "table": "Sales"},
        {"name": "Gross Profit", "expression": "[Total Revenue] - [Total Cost]", "table": "Sales"},
        {
            "name": "Profit Margin",
            "expression": "DIVIDE([Gross Profit], [Total Revenue])",
            "table": "Sales",
        },
        {"name": "Score", "expression": "AVERAGE(DQ[Score])", "table": "DQ"},
    ]


def _tables():
    return [
        {
            "name": "Sales",
            "measures": [
                {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])"},
                {"name": "Total Cost", "expression": "SUM(Sales[Cost])"},
            ],
            "columns": [{"name": "Revenue"}, {"name": "Cost"}],
        },
        {
            "name": "DQ",
            "measures": [{"name": "Score", "expression": "AVERAGE(DQ[Score])"}],
            "columns": [{"name": "Score"}],
        },
    ]


def _sample_data():
    """Sample data that enables filtered-measure decomposition."""
    return {
        "DQ[Dimension]": {
            "sample_values": ["Completeness", "Uniqueness", "Accuracy"]
        }
    }


# ─── MeasureType enum ────────────────────────────────────────────────────────

class TestMeasureTypeEnum:
    def test_string_values(self):
        assert MeasureType.MODEL_MEASURE.value == "model_measure"
        assert MeasureType.FILTERED_MEASURE.value == "filtered_measure"
        assert MeasureType.COMPOSITE_MEASURE.value == "composite_measure"
        assert MeasureType.UNRESOLVED.value == "unresolved"


# ─── ExpressionFlags dataclass ───────────────────────────────────────────────

class TestExpressionFlagsDataclass:
    def test_defaults(self):
        f = ExpressionFlags()
        assert f.safe_for_decompose is True
        assert f.handles_date_internally is False
        assert f.has_removefilters is False
        assert f.has_allselected is False
        assert f.has_allexcept is False
        assert f.uses_calculate is False
        assert f.has_context_routing is False


# ─── ResolvedMeasure.to_dict ─────────────────────────────────────────────────

class TestResolvedMeasureToDict:
    def test_minimal_model_measure_dict(self):
        rm = ResolvedMeasure(
            name="Revenue",
            table="Sales",
            resolution_type=MeasureType.MODEL_MEASURE,
        )
        d = rm.to_dict()
        assert d["name"] == "Revenue"
        assert d["table"] == "Sales"
        assert d["resolution_type"] == "model_measure"
        assert "expression_flags" in d

    def test_base_measure_included_when_set(self):
        rm = ResolvedMeasure(
            name="Online Sales",
            resolution_type=MeasureType.FILTERED_MEASURE,
            base_measure="Total Sales",
        )
        d = rm.to_dict()
        assert d["base_measure"] == "Total Sales"

    def test_base_measure_absent_when_none(self):
        rm = ResolvedMeasure(name="Revenue", resolution_type=MeasureType.MODEL_MEASURE)
        d = rm.to_dict()
        assert "base_measure" not in d

    def test_filter_column_and_value_included_when_set(self):
        rm = ResolvedMeasure(
            name="Online Sales",
            resolution_type=MeasureType.FILTERED_MEASURE,
            base_measure="Total Sales",
            filter_column="Sales[Channel]",
            filter_value="Online",
        )
        d = rm.to_dict()
        assert d["filter_column"] == "Sales[Channel]"
        assert d["filter_value"] == "Online"

    def test_sibling_measures_included_when_set(self):
        rm = ResolvedMeasure(
            name="All Scores",
            resolution_type=MeasureType.COMPOSITE_MEASURE,
            sibling_measures=["Score A", "Score B"],
        )
        d = rm.to_dict()
        assert d["sibling_measures"] == ["Score A", "Score B"]

    def test_sibling_measures_absent_when_empty(self):
        rm = ResolvedMeasure(name="X", resolution_type=MeasureType.MODEL_MEASURE)
        d = rm.to_dict()
        assert "sibling_measures" not in d

    def test_expression_flags_present(self):
        rm = ResolvedMeasure(
            name="Margin",
            resolution_type=MeasureType.MODEL_MEASURE,
            expression_flags=ExpressionFlags(has_removefilters=True),
        )
        d = rm.to_dict()
        assert d["expression_flags"]["has_removefilters"] is True


# ─── MeasureResolver initialization ─────────────────────────────────────────

class TestMeasureResolverInit:
    def test_indexes_flat_measures(self):
        resolver = MeasureResolver(_flat_measures(), _tables())
        assert "Total Revenue" in resolver._measure_map
        assert "Score" in resolver._measure_map

    def test_indexes_table_embedded_measures(self):
        resolver = MeasureResolver([], _tables())
        # Measures inside table dicts should be indexed
        assert "Total Revenue" in resolver._measure_map

    def test_flat_list_takes_precedence_over_table_embedded(self):
        flat = [{"name": "Total Revenue", "expression": "OVERRIDE_EXPR", "table": "Sales"}]
        resolver = MeasureResolver(flat, _tables())
        assert resolver._measure_map["Total Revenue"]["expression"] == "OVERRIDE_EXPR"

    def test_measure_to_table_populated(self):
        resolver = MeasureResolver(_flat_measures(), _tables())
        assert resolver._measure_to_table.get("Total Revenue") == "Sales"

    def test_value_index_populated_from_sample_data(self):
        resolver = MeasureResolver(_flat_measures(), _tables(), sample_data=_sample_data())
        assert "completeness" in resolver._value_index
        assert "uniqueness" in resolver._value_index

    def test_empty_measures_and_tables(self):
        resolver = MeasureResolver([], [])
        assert resolver._measure_map == {}
        assert resolver._measure_to_table == {}


# ─── MeasureResolver.resolve — MODEL_MEASURE ────────────────────────────────

class TestResolveModelMeasure:
    def setup_method(self):
        self.resolver = MeasureResolver(_flat_measures(), _tables())

    def test_known_measure_resolves_to_model_measure(self):
        results = self.resolver.resolve(
            [{"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"}]
        )
        assert len(results) == 1
        assert results[0]["_resolution"]["resolution_type"] == "model_measure"

    def test_original_dict_is_preserved(self):
        selected = [{"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales", "custom_key": "val"}]
        results = self.resolver.resolve(selected)
        assert results[0]["custom_key"] == "val"

    def test_resolution_key_added_to_enriched_dict(self):
        selected = [{"name": "Total Revenue", "expression": "", "table": "Sales"}]
        results = self.resolver.resolve(selected)
        assert "_resolution" in results[0]

    def test_expression_flags_in_resolution(self):
        selected = [{"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"}]
        results = self.resolver.resolve(selected)
        flags = results[0]["_resolution"]["expression_flags"]
        assert "has_removefilters" in flags
        assert "uses_calculate" in flags


# ─── MeasureResolver.resolve — FILTERED_MEASURE ─────────────────────────────

class TestResolveFilteredMeasure:
    def setup_method(self):
        # "Completeness Score" → "Score" (base) WHERE Dimension = "Completeness"
        self.resolver = MeasureResolver(
            _flat_measures(), _tables(), sample_data=_sample_data()
        )

    def test_filtered_measure_decomposition(self):
        results = self.resolver.resolve(
            [{"name": "Completeness Score", "expression": "", "table": ""}]
        )
        res = results[0]["_resolution"]
        assert res["resolution_type"] == "filtered_measure"
        assert res["base_measure"] == "Score"
        assert res["filter_value"] == "Completeness"

    def test_filter_column_references_correct_table(self):
        results = self.resolver.resolve(
            [{"name": "Completeness Score", "expression": "", "table": ""}]
        )
        fc = results[0]["_resolution"]["filter_column"]
        assert "DQ" in fc

    def test_unknown_measure_with_no_decomposition_is_unresolved(self):
        results = self.resolver.resolve(
            [{"name": "Totally Unknown Measure", "expression": "", "table": ""}]
        )
        assert results[0]["_resolution"]["resolution_type"] == "unresolved"

    def test_single_word_name_is_not_decomposed(self):
        results = self.resolver.resolve(
            [{"name": "Revenue", "expression": "", "table": ""}]
        )
        # "Revenue" is not in the measure map, and it's a single word
        # so it's unresolved (not decomposed)
        res = results[0]["_resolution"]
        assert res["resolution_type"] == "unresolved"


# ─── MeasureResolver.resolve — COMPOSITE_MEASURE ────────────────────────────

class TestResolveCompositeMeasure:
    def setup_method(self):
        # Only "Score" is a known model measure in the map.
        # "Completeness Score" and "Uniqueness Score" are NOT in the map so they
        # will be decomposed as filtered_measure via sample_data.
        # "Total Score" references both filtered siblings → composite (2nd pass).
        measures = [
            {"name": "Score", "expression": "AVERAGE(DQ[Score])", "table": "DQ"},
        ]
        tables = [
            {
                "name": "DQ",
                "measures": [{"name": "Score", "expression": "AVERAGE(DQ[Score])"}],
                "columns": [],
            }
        ]
        sample_data = {
            "DQ[Dimension]": {"sample_values": ["Completeness", "Uniqueness"]}
        }
        self.resolver = MeasureResolver(measures, tables, sample_data=sample_data)

    def test_composite_detected_in_second_pass(self):
        results = self.resolver.resolve(
            [
                {"name": "Completeness Score", "expression": "", "table": ""},
                {"name": "Uniqueness Score", "expression": "", "table": ""},
                {
                    "name": "Total Score",
                    "expression": "[Completeness Score] + [Uniqueness Score]",
                    "table": "",
                },
            ]
        )
        total = next(r for r in results if r["name"] == "Total Score")
        assert total["_resolution"]["resolution_type"] == "composite_measure"

    def test_composite_sibling_measures_listed(self):
        results = self.resolver.resolve(
            [
                {"name": "Completeness Score", "expression": "", "table": ""},
                {"name": "Uniqueness Score", "expression": "", "table": ""},
                {
                    "name": "Total Score",
                    "expression": "[Completeness Score] + [Uniqueness Score]",
                    "table": "",
                },
            ]
        )
        total = next(r for r in results if r["name"] == "Total Score")
        siblings = total["_resolution"]["sibling_measures"]
        assert "Completeness Score" in siblings or "Uniqueness Score" in siblings

    def test_composite_not_detected_with_single_sibling(self):
        # Only one filtered sibling — should NOT become composite
        results = self.resolver.resolve(
            [
                {"name": "Completeness Score", "expression": "", "table": ""},
                {
                    "name": "Only Score",
                    "expression": "[Completeness Score]",
                    "table": "",
                },
            ]
        )
        only = next(r for r in results if r["name"] == "Only Score")
        assert only["_resolution"]["resolution_type"] != "composite_measure"


# ─── MeasureResolver._analyze_expression ─────────────────────────────────────

class TestAnalyzeExpression:
    def test_empty_expression_returns_defaults(self):
        flags = MeasureResolver._analyze_expression("")
        assert flags.safe_for_decompose is True
        assert flags.handles_date_internally is False
        assert flags.has_removefilters is False

    def test_none_expression_returns_defaults(self):
        flags = MeasureResolver._analyze_expression(None)
        assert flags.safe_for_decompose is True

    def test_calculate_detected(self):
        flags = MeasureResolver._analyze_expression("CALCULATE(SUM(T[C]))")
        assert flags.uses_calculate is True

    def test_removefilters_detected(self):
        flags = MeasureResolver._analyze_expression("CALCULATE(SUM(T[C]), REMOVEFILTERS(T))")
        assert flags.has_removefilters is True
        assert flags.safe_for_decompose is False

    def test_allselected_detected(self):
        flags = MeasureResolver._analyze_expression("CALCULATE(SUM(T[C]), ALLSELECTED(T))")
        assert flags.has_allselected is True

    def test_allexcept_detected(self):
        flags = MeasureResolver._analyze_expression("CALCULATE(SUM(T[C]), ALLEXCEPT(T, T[Col]))")
        assert flags.has_allexcept is True
        assert flags.safe_for_decompose is False

    def test_userelationship_makes_unsafe(self):
        flags = MeasureResolver._analyze_expression(
            "CALCULATE(SUM(T[C]), USERELATIONSHIP(T1[A], T2[B]))"
        )
        assert flags.safe_for_decompose is False

    def test_crossfilter_makes_unsafe(self):
        flags = MeasureResolver._analyze_expression(
            "CALCULATE(SUM(T[C]), CROSSFILTER(T1[A], T2[B], BOTH))"
        )
        assert flags.safe_for_decompose is False

    def test_treatas_makes_unsafe(self):
        flags = MeasureResolver._analyze_expression(
            "CALCULATE(SUM(T[C]), TREATAS({\"val\"}, T[Col]))"
        )
        assert flags.safe_for_decompose is False

    def test_date_function_detected_datesytd(self):
        flags = MeasureResolver._analyze_expression(
            "CALCULATE(SUM(T[Revenue]), DATESYTD(Date[Date]))"
        )
        assert flags.handles_date_internally is True

    def test_date_function_detected_sameperiodlastyear(self):
        flags = MeasureResolver._analyze_expression(
            "CALCULATE(SUM(T[Revenue]), SAMEPERIODLASTYEAR(Date[Date]))"
        )
        assert flags.handles_date_internally is True

    def test_context_routing_isfiltered(self):
        flags = MeasureResolver._analyze_expression(
            "IF(ISFILTERED(T[Col]), [M1], [M2])"
        )
        assert flags.has_context_routing is True

    def test_context_routing_selectedvalue(self):
        flags = MeasureResolver._analyze_expression(
            "SWITCH(SELECTEDVALUE(T[Col]), \"A\", 1, 0)"
        )
        assert flags.has_context_routing is True

    def test_context_routing_hasonevalue(self):
        flags = MeasureResolver._analyze_expression(
            "IF(HASONEVALUE(T[Col]), VALUES(T[Col]), BLANK())"
        )
        assert flags.has_context_routing is True

    def test_context_routing_isinscope(self):
        flags = MeasureResolver._analyze_expression(
            "IF(ISINSCOPE(T[Col]), SUM(T[V]), BLANK())"
        )
        assert flags.has_context_routing is True

    def test_case_insensitive_detection(self):
        flags = MeasureResolver._analyze_expression(
            "calculate(sum(t[c]), removefilters(t))"
        )
        assert flags.has_removefilters is True
        assert flags.uses_calculate is True

    def test_simple_sum_no_special_flags(self):
        flags = MeasureResolver._analyze_expression("SUM(Sales[Revenue])")
        assert flags.uses_calculate is False
        assert flags.has_removefilters is False
        assert flags.handles_date_internally is False
        assert flags.has_context_routing is False
        assert flags.safe_for_decompose is True


# ─── MeasureResolver._find_measure_match ─────────────────────────────────────

class TestFindMeasureMatch:
    def setup_method(self):
        self.resolver = MeasureResolver(_flat_measures(), _tables())

    def test_exact_match_case_insensitive(self):
        result = self.resolver._find_measure_match("total revenue")
        assert result == "Total Revenue"

    def test_no_match_returns_none(self):
        result = self.resolver._find_measure_match("nonexistent measure")
        assert result is None

    def test_partial_match_returns_none(self):
        result = self.resolver._find_measure_match("revenue")
        assert result is None  # Only exact match allowed


# ─── MeasureResolver.resolve — empty and malformed inputs ────────────────────

class TestResolveEdgeCases:
    def setup_method(self):
        self.resolver = MeasureResolver(_flat_measures(), _tables())

    def test_empty_selection_returns_empty_list(self):
        assert self.resolver.resolve([]) == []

    def test_measure_with_no_name_resolves_as_unresolved(self):
        results = self.resolver.resolve([{"name": "", "expression": "", "table": ""}])
        assert results[0]["_resolution"]["resolution_type"] == "unresolved"

    def test_measure_with_none_expression_does_not_crash(self):
        results = self.resolver.resolve(
            [{"name": "Total Revenue", "expression": None, "table": "Sales"}]
        )
        assert results[0]["_resolution"]["resolution_type"] == "model_measure"

    def test_multiple_measures_all_enriched(self):
        selected = [
            {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"},
            {"name": "Gross Profit", "expression": "[Total Revenue] - [Total Cost]", "table": "Sales"},
        ]
        results = self.resolver.resolve(selected)
        assert len(results) == 2
        for r in results:
            assert "_resolution" in r

    def test_result_count_matches_input_count(self):
        selected = [
            {"name": "Total Revenue", "expression": "", "table": "Sales"},
            {"name": "Unknown Measure", "expression": "", "table": ""},
        ]
        results = self.resolver.resolve(selected)
        assert len(results) == 2

    def test_resolver_with_no_sample_data_does_not_decompose(self):
        # Without sample_data, filtered decomposition cannot work
        resolver = MeasureResolver(_flat_measures(), _tables(), sample_data={})
        results = resolver.resolve(
            [{"name": "Completeness Score", "expression": "", "table": ""}]
        )
        # Should be unresolved — no sample data to find filter values
        assert results[0]["_resolution"]["resolution_type"] == "unresolved"


# ─── MeasureResolver._detect_composites ──────────────────────────────────────

class TestDetectComposites:
    def test_no_filtered_measures_leaves_results_unchanged(self):
        resolver = MeasureResolver(_flat_measures(), _tables())
        results = [
            {
                "name": "Total Revenue",
                "expression": "SUM(Sales[Revenue])",
                "_resolution": {"resolution_type": "model_measure"},
            }
        ]
        from src.engines.crewai.tools.custom.metadata_reduction.measure_resolver import ResolvedMeasure
        resolved_cache = {
            "Total Revenue": ResolvedMeasure(
                name="Total Revenue",
                resolution_type=MeasureType.MODEL_MEASURE,
            )
        }
        # Should not raise and should not change resolution_type
        resolver._detect_composites(results, resolved_cache)
        assert results[0]["_resolution"]["resolution_type"] == "model_measure"

    def test_composite_requires_at_least_two_filtered_siblings(self):
        measures = [
            {"name": "Score", "expression": "SUM(T[V])", "table": "T"},
            {"name": "A Score", "expression": "", "table": ""},
            {"name": "Aggregated", "expression": "[A Score]", "table": ""},
        ]
        tables = [{"name": "T", "measures": [{"name": "Score", "expression": "SUM(T[V])"}], "columns": []}]
        sample_data = {"T[Dim]": {"sample_values": ["A"]}}
        resolver = MeasureResolver(measures, tables, sample_data=sample_data)
        results = resolver.resolve(
            [
                {"name": "A Score", "expression": "", "table": ""},
                {"name": "Aggregated", "expression": "[A Score]", "table": ""},
            ]
        )
        agg = next(r for r in results if r["name"] == "Aggregated")
        # Only one filtered sibling → should NOT be composite
        assert agg["_resolution"]["resolution_type"] != "composite_measure"
