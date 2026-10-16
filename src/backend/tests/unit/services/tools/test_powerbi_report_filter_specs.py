"""Report filters become exact structured values, in Tool 72 as in Tool 79/80.

Pins Power BI's QueryComparisonKind (0 =, 1 >, 2 >=, 3 <, 4 <=; not-equal is
Not around kind 0), refuses anything that cannot be applied exactly, and
refuses legacy comparison text written with the old operator map.
"""

import pytest

from src.services.tools.metadata_reduction.keyword_measure import pick_measure
from src.services.tools.metadata_reduction.report_filters import (
    filter_to_dax,
    parse_filter_value,
    spec_from_pbi_condition,
)
from src.services.tools.powerbi_analysis_tool import PowerBIAnalysisTool
from src.services.tools.powerbi_semantic_model_fetcher_tool import (
    PowerBISemanticModelFetcherTool,
)


def _cmp(kind: int, literal: str = "2020L") -> dict:
    return {
        "Comparison": {"ComparisonKind": kind, "Right": {"Literal": {"Value": literal}}}
    }


def _in(*literals: str) -> dict:
    return {"In": {"Values": [[{"Literal": {"Value": v}}] for v in literals]}}


@pytest.mark.parametrize(
    "condition,spec,dax",
    [
        (_cmp(0), {"operator": "in", "values": [2020]}, "TREATAS({2020}, Cal[Year])"),
        (
            _cmp(1),
            {"operator": ">", "values": [2020]},
            "FILTER(ALL(Cal[Year]), Cal[Year] > 2020)",
        ),
        (
            _cmp(2),
            {"operator": ">=", "values": [2020]},
            "FILTER(ALL(Cal[Year]), Cal[Year] >= 2020)",
        ),
        (
            _cmp(3),
            {"operator": "<", "values": [2020]},
            "FILTER(ALL(Cal[Year]), Cal[Year] < 2020)",
        ),
        (
            _cmp(4),
            {"operator": "<=", "values": [2020]},
            "FILTER(ALL(Cal[Year]), Cal[Year] <= 2020)",
        ),
        (
            {"Not": {"Expression": _cmp(0)}},
            {"operator": "<>", "values": [2020]},
            "FILTER(ALL(Cal[Year]), Cal[Year] <> 2020)",
        ),
    ],
    ids=["kind0", "kind1", "kind2", "kind3", "kind4", "not_kind0"],
)
class TestComparisonKindIsExact:
    def test_spec_and_dax(self, condition, spec, dax):
        assert spec_from_pbi_condition(condition) == spec
        assert filter_to_dax("Cal[Year]", spec) == dax

    def test_tool_72_uses_the_same_mapping(self, condition, spec, dax):
        tool = PowerBIAnalysisTool()
        filter_def = {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "Cal"}},
                    "Property": "Year",
                }
            },
            "filter": {"Where": [{"Condition": condition}]},
        }
        name, value = tool._extract_filter_from_definition(filter_def)
        assert (name, value) == ("Cal[Year]", spec)
        assert tool._generate_dax_filter_condition(name, value) == dax


class TestOnlyExactFiltersAreApplied:
    def test_not_around_a_greater_than_is_refused(self):
        assert spec_from_pbi_condition({"Not": {"Expression": _cmp(1)}}) is None

    @pytest.mark.parametrize(
        "condition",
        [
            {"Comparison": {"ComparisonKind": 2, "Right": {"DateSpan": {"x": 1}}}},
            {"Contains": {"Right": {"Measure": {"Property": "M"}}}},
            {"StartsWith": {"Right": {"Literal": {"Value": "''"}}}},
            _in("'Italy'", "null"),  # values plus blank
            {
                "In": {
                    "Expressions": [{"Column": {}}, {"Column": {}}],
                    "Values": [
                        [{"Literal": {"Value": "'A'"}}, {"Literal": {"Value": "1L"}}]
                    ],
                }
            },
        ],
        ids=[
            "relative_date",
            "contains_measure",
            "empty_prefix",
            "value_and_blank",
            "tuple_in",
        ],
    )
    def test_inexact_conditions_are_not_applied(self, condition):
        assert spec_from_pbi_condition(condition) is None

    def test_in_null_is_blank_not_unfiltered(self):
        spec = spec_from_pbi_condition(_in("null"))
        assert spec == {"operator": "blank", "values": []}
        assert filter_to_dax("Geo[Region]", spec) == (
            "FILTER(ALL(Geo[Region]), ISBLANK(Geo[Region]))"
        )


class TestLegacyText:
    @pytest.mark.parametrize(
        "text,spec",
        [
            ("= 'Italy'", {"operator": "in", "values": ["Italy"]}),
            ("= '2020L'", {"operator": "in", "values": [2020]}),
            ("IN (A, B)", {"operator": "in", "values": ["A", "B"]}),
            ("NOT IN (X)", {"operator": "not_in", "values": ["X"]}),
            ("NOT NULL", {"operator": "not_blank", "values": []}),
            ("STARTS WITH 'AB'", {"operator": "starts_with", "values": ["AB"]}),
        ],
    )
    def test_trusted_forms_are_read(self, text, spec):
        assert parse_filter_value(text) == spec

    @pytest.mark.parametrize("text", [">= 2020L", "!= 2020L", "> 2020", "<= 5", "< 1"])
    def test_comparisons_from_the_old_map_are_refused(self, text, caplog):
        # the old map wrote ">=" for kind 3 (<) and "!=" for kind 1 (>)
        assert parse_filter_value(text) is None
        assert filter_to_dax("Cal[Year]", text) is None


class TestTool72FiltersAndFallback:
    def test_prompt_lists_dax_built_from_values(self):
        tool = PowerBIAnalysisTool()
        context = tool._build_enriched_semantic_context(
            {"measures": [], "tables": []},
            {
                "active_filters": {
                    "Geo[Country]": {"operator": "in", "values": ["Italy"]},
                    "Cal[Year]": "has filter (unknown type)",
                }
            },
        )
        assert 'TREATAS({"Italy"}, Geo[Country])' in context
        assert "has filter" not in context

    def test_auto_wrap_never_filters_on_a_description(self):
        tool = PowerBIAnalysisTool()
        dax = 'EVALUATE SUMMARIZECOLUMNS("R", [Revenue])'
        config = {"active_filters": {"Cal[Year]": "has complex filter"}}
        assert tool._auto_wrap_with_report_filters(dax, config) == dax

    def test_keyword_fallback_is_labelled(self):
        tool = PowerBIAnalysisTool()
        dax = tool._generate_simple_dax(
            "total revenue?", {"measures": [{"name": "Total Revenue"}]}
        )
        text = tool._format_output(
            {
                "user_question": "total revenue?",
                "workspace_id": "w",
                "dataset_id": "d",
                "generated_dax": dax,
                "dax_execution": {"success": True, "row_count": 1, "data": []},
            },
            "markdown",
        )
        assert "keyword fallback" in text

    def test_no_measure_named_means_no_answer(self):
        tool = PowerBIAnalysisTool()
        measures = {"measures": [{"name": "Total Cost"}, {"name": "Margin"}]}
        assert tool._generate_simple_dax("how many employees?", measures) is None


class TestKeywordPick:
    def test_best_overlap_wins_and_substrings_do_not_count(self):
        measures = [{"name": "Total Cost"}, {"name": "Total Revenue"}]
        assert pick_measure("What is total revenue by region?", measures)["name"] == (
            "Total Revenue"
        )
        # "of" inside "profit" used to pick "% of Sales"
        measures = [{"name": "% of Sales"}, {"name": "Gross Profit"}]
        assert pick_measure("What is gross profit?", measures)["name"] == "Gross Profit"

    def test_slicer_merge_fallback_refuses_old_comparison_text(self):
        fetcher = PowerBISemanticModelFetcherTool()
        filters: dict = {}
        fetcher._merge_slicer_defaults_into_filters(
            [{"table": "Cal", "column": "Year", "default_value": ">= 2020L"}], filters
        )
        assert filters == {}
