"""Power BI Q&A uses the LLM by default and applies report filters by value.

Two defects made answers wrong while the run reported success:

* the DAX generator skipped the LLM unless a token was pasted into the tool
  config, so default runs answered from a keyword heuristic;
* report filters and slicer selections were cached as display text, so the
  query filtered on ``TREATAS({"= 'Italy'"}, ...)`` and came back blank.
"""

import asyncio
import base64
import json
from unittest.mock import AsyncMock, patch

import pytest

from src.services.tools.metadata_reduction.dax_skeleton_builder import (
    DaxSkeletonBuilder,
)
from src.services.tools.metadata_reduction.report_filters import filter_to_dax
from src.services.tools.powerbi_semantic_model_dax_tool import (
    PowerBISemanticModelDaxTool,
)
from src.services.tools.powerbi_semantic_model_fetcher_tool import (
    PowerBISemanticModelFetcherTool,
)

ITALY = 'TREATAS({"Italy"}, Geo[Country])'
MODEL = {
    "measures": [
        {"name": "Total Cost", "table": "Sales"},
        {"name": "Total Revenue", "table": "Sales"},
    ],
    "tables": [
        {"name": "Sales", "columns": ["Amount"]},
        {"name": "Geo", "columns": ["Country"]},
        {"name": "Cal", "columns": ["Year"]},
    ],
    "relationships": [],
    "sample_data": {},
}
LLM_DAX = 'EVALUATE\nSUMMARIZECOLUMNS(Geo[Country], "Revenue", [Total Revenue])'


def _filter_def(table: str, column: str, where: list) -> dict:
    return {
        "expression": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table}},
                "Property": column,
            }
        },
        "filter": {"Where": where},
        "type": "Categorical",
    }


def _in(literal: str) -> list:
    return [{"Condition": {"In": {"Values": [[{"Literal": {"Value": literal}}]]}}}]


class TestLlmPathNeedsNoPastedToken:
    def test_generation_calls_the_llm_without_token_config(self):
        tool = PowerBISemanticModelDaxTool()
        completion = AsyncMock(return_value=LLM_DAX)
        with patch("src.services.llm.manager.LLMManager.completion", completion):
            dax = asyncio.run(
                tool._generate_dax_with_llm(
                    "What is total revenue by country?", MODEL, {}
                )
            )
        assert completion.await_count == 1
        assert "[Total Revenue]" in dax
        assert "[Total Cost]" not in dax

    def test_self_correction_calls_the_llm_without_token_config(self):
        tool = PowerBISemanticModelDaxTool()
        completion = AsyncMock(return_value=LLM_DAX)
        previous = [{"attempt": 1, "dax": "EVALUATE x", "success": False, "error": "e"}]
        with patch("src.services.llm.manager.LLMManager.completion", completion):
            dax = asyncio.run(
                tool._generate_dax_with_self_correction("revenue?", MODEL, {}, previous)
            )
        assert completion.await_count == 1
        assert dax == LLM_DAX

    def test_keyword_fallback_answer_is_labelled(self):
        tool = PowerBISemanticModelDaxTool()
        dax = tool._generate_deterministic_dax("total revenue?", MODEL, {})
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

    def test_question_with_comparison_signs_is_kept(self):
        tool = PowerBISemanticModelDaxTool()
        question = "Which customers have revenue < 1000 and margin > 5%?"
        pipeline = AsyncMock(return_value="ok")
        with patch.object(tool, "_execute_dax_pipeline", pipeline):
            out = tool._run(
                user_question=question,
                workspace_id="ws-1",
                dataset_id="ds-1",
                access_token="token-1",
                model_context_json=json.dumps({"tables": []}, separators=(",", ":")),
            )
        assert out == "ok"
        config = pipeline.call_args.args[0]
        assert config["user_question"] == question
        assert config["model_context_json"] == '{"tables":[]}'


class TestReportFiltersAreStructured:
    def test_fetcher_stores_value_not_display_text(self):
        fetcher = PowerBISemanticModelFetcherTool()
        name, spec = fetcher._extract_filter_from_definition(
            _filter_def("Geo", "Country", _in("'Italy'"))
        )
        assert name == "Geo[Country]"
        assert spec == {"operator": "in", "values": ["Italy"]}

    def test_unselected_card_and_typed_literal(self):
        fetcher = PowerBISemanticModelFetcherTool()
        report = {
            "filters": json.dumps(
                [
                    _filter_def("Geo", "Country", _in("'Italy'")),
                    _filter_def("Cal", "Year", []),  # card with nothing selected
                    _filter_def("Cal", "Month", _in("3L")),
                ]
            )
        }
        part = {
            "path": "definition/report.json",
            "payload": base64.b64encode(json.dumps(report).encode()).decode(),
        }
        filters = fetcher._parse_tmdl_for_filters([part])
        assert filters["Geo[Country]"] == {"operator": "in", "values": ["Italy"]}
        assert "Cal[Year]" not in filters
        assert filters["Cal[Month]"] == {"operator": "in", "values": [3]}
        assert filter_to_dax("Cal[Month]", filters["Cal[Month]"]) == (
            "TREATAS({3}, Cal[Month])"
        )

    def test_slicer_selection_is_structured(self):
        fetcher = PowerBISemanticModelFetcherTool()
        slicers = [
            {
                "table": "Geo",
                "column": "Country",
                "default_value": '= "Italy"',
                "default_filter": {"operator": "in", "values": ["Italy"]},
            }
        ]
        filters: dict = {}
        fetcher._merge_slicer_defaults_into_filters(slicers, filters)
        assert filters["Geo[Country]"] == {"operator": "in", "values": ["Italy"]}
        assert filter_to_dax("Geo[Country]", filters["Geo[Country]"]) == ITALY


# Both the structured value and the display text already sitting in caches.
@pytest.mark.parametrize(
    "value", [{"operator": "in", "values": ["Italy"]}, "= 'Italy'"]
)
class TestEveryDaxWriterUsesTheValue:
    def test_prompt(self, value):
        tool = PowerBISemanticModelDaxTool()
        context = tool._build_enriched_semantic_context(
            MODEL, {"active_filters": {"Geo[Country]": value}}
        )
        assert ITALY in context
        assert "= 'Italy'" not in context

    def test_deterministic_fallback(self, value):
        tool = PowerBISemanticModelDaxTool()
        dax = tool._generate_deterministic_dax(
            "revenue?", MODEL, {"active_filters": {"Geo[Country]": value}}
        )
        assert ITALY in dax and "= 'Italy'" not in dax

    def test_patch_path(self, value):
        tool = PowerBISemanticModelDaxTool()
        dax = tool._patch_dax_with_active_filters(
            'EVALUATE\nSUMMARIZECOLUMNS(\n    "Result", [Total Revenue]\n)',
            {"active_filters": {"Geo[Country]": value}},
            MODEL,
        )
        assert ITALY in dax and "= 'Italy'" not in dax

    def test_auto_wrap(self, value):
        tool = PowerBISemanticModelDaxTool()
        dax = tool._auto_wrap_with_report_filters(
            'EVALUATE\nSUMMARIZECOLUMNS("Result", [Total Revenue])',
            {"active_filters": {"Geo[Country]": value}},
        )
        assert ITALY in dax and "= 'Italy'" not in dax

    def test_skeleton(self, value):
        lines = DaxSkeletonBuilder._format_filter_lines({"Geo[Country]": value})
        assert ITALY in lines and "= 'Italy'" not in lines


class TestFilterWithoutValueIsSkipped:
    def test_description_only_filter_is_never_a_value(self):
        tool = PowerBISemanticModelDaxTool()
        active = {"Cal[Year]": "has filter (unknown type)"}
        context = tool._build_enriched_semantic_context(
            MODEL, {"active_filters": active}
        )
        assert "has filter" not in context
        assert "ACTIVE FILTERS" not in context
        dax = 'EVALUATE\nSUMMARIZECOLUMNS("Result", [Total Revenue])'
        assert (
            tool._auto_wrap_with_report_filters(dax, {"active_filters": active}) == dax
        )

    def test_comparison_and_exclusion_filters(self):
        assert filter_to_dax("Cal[Year]", {"operator": ">", "values": [2020]}) == (
            "FILTER(ALL(Cal[Year]), Cal[Year] > 2020)"
        )
        # cached text comparisons came from a map shifted by one: not applied
        assert filter_to_dax("Cal[Year]", "> 2020") is None
        assert filter_to_dax("Geo[Country]", "NOT IN (Italy, France)") == (
            'FILTER(ALL(Geo[Country]), NOT Geo[Country] IN {"Italy", "France"})'
        )
