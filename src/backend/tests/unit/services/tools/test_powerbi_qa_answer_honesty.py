"""A Q&A answer says when it ignored a report filter and when every attempt failed."""

import pytest

from src.services.tools.metadata_reduction.qa_output import attempt_lines
from src.services.tools.powerbi_analysis_tool import PowerBIAnalysisTool
from src.services.tools.powerbi_semantic_model_dax_tool import (
    PowerBISemanticModelDaxTool,
)
from src.services.tools.powerbi_semantic_model_fetcher_tool import (
    PowerBISemanticModelFetcherTool,
)

MODEL = {
    "measures": [{"name": "Total Revenue", "table": "Sales"}],
    "tables": [
        {"name": "Sales", "columns": ["Amount"]},
        {"name": "Geo", "columns": ["Country"]},
    ],
    "relationships": [],
}
FILTERS = {
    "Geo[Country]": {"operator": "in", "values": ["Italy"]},
    "Geo[Region]": {"unapplied": "relative or computed value"},
    "Cal[Year]": {"operator": "in", "values": [2024]},  # table not in the model
}
FAILED = [
    {"attempt": 1, "dax": "EVALUATE x", "success": False, "error": "syntax"},
    {"attempt": 2, "dax": "EVALUATE y", "success": False, "error": "syntax"},
]


def _results(**extra) -> dict:
    return {
        "user_question": "revenue?",
        "workspace_id": "w",
        "dataset_id": "d",
        "generated_dax": "EVALUATE y",
        "dax_execution": {"success": False, "error": "syntax"},
        **extra,
    }


@pytest.mark.parametrize("tool_cls", [PowerBISemanticModelDaxTool, PowerBIAnalysisTool])
class TestTheAnswerIsHonest:
    def test_failed_run_never_reads_as_success(self, tool_cls):
        text = tool_cls()._format_output(_results(dax_attempts=FAILED), "markdown")
        assert "all failed" in text
        assert "Success" not in text
        assert "successful on attempt" not in text

    def test_unapplied_report_filters_are_listed(self, tool_cls):
        tool = tool_cls()
        tool._build_enriched_semantic_context(MODEL, {"active_filters": FILTERS})
        text = tool._format_output(_results(), "markdown")
        line = next(r for r in text.splitlines() if "Report filters not applied" in r)
        assert "Geo[Region] (relative or computed value)" in line
        assert "Geo[Country]" not in line
        if tool_cls is PowerBISemanticModelDaxTool:  # Tool 80 scopes to the model
            assert "Cal[Year] (table not in the model context)" in line

    def test_nothing_is_listed_when_every_filter_applies(self, tool_cls):
        tool = tool_cls()
        tool._build_enriched_semantic_context(
            MODEL, {"active_filters": {"Geo[Country]": "Italy"}}
        )
        assert "not applied" not in tool._format_output(_results(), "markdown")


def test_attempt_history_names_the_successful_attempt():
    lines = attempt_lines([*FAILED, {"attempt": 3, "success": True}])
    assert lines[0].startswith("**Attempts**: 3 (succeeded on attempt 3)")


def test_refused_slicer_selection_is_kept_as_unapplied():
    fetcher = PowerBISemanticModelFetcherTool()
    vis = {
        "filters": [
            {
                "filter": {
                    "Where": [
                        {
                            "Condition": {
                                "Comparison": {
                                    "ComparisonKind": 2,
                                    "Right": {"DateSpan": {"x": 1}},
                                }
                            }
                        }
                    ]
                }
            }
        ]
    }
    from src.services.tools.metadata_reduction.report_filters import (
        slicer_selection_spec,
    )

    selection = slicer_selection_spec(vis, {})
    filters: dict = {}
    fetcher._merge_slicer_defaults_into_filters(
        [{"table": "Cal", "column": "Date", "default_filter": selection}], filters
    )
    assert filters == {"Cal[Date]": {"unapplied": "relative or computed value"}}
