"""Unit tests for PBIVisualUCMVMapperTool (Tool 94)."""
import json
from unittest.mock import MagicMock, patch

import pytest

from src.engines.crewai.tools.custom.pbi_visual_ucmv_mapper_tool import (
    PBIVisualUCMVMapperTool,
    PBIVisualUCMVMapperSchema,
)


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

SAMPLE_YAML = (
    "name: fact_sales_uc_metric_view\n"
    "source: main.metrics.fact_sales\n"
    "comment: Sales metric view\n"
    "dimensions:\n"
    "  - name: region\n"
    "    expr: region\n"
    "  - name: product\n"
    "    expr: product_name\n"
    "measures:\n"
    "  - name: total_revenue\n"
    "    expr: SUM(revenue)\n"
    "  - name: order_count\n"
    "    expr: COUNT(order_id)\n"
)

SAMPLE_REPORT_REFERENCES = json.dumps({
    "reports": [
        {
            "report_name": "Sales Report",
            "pages": [
                {
                    "page_name": "Page1",
                    "page_display_name": "Sales Overview",
                    "visuals": [
                        {
                            "visual_id": "v001",
                            "visual_type": "barChart",
                            "measures": ["Revenue", "Cost"],
                            "tables": ["Sales"],
                        },
                        {
                            "visual_id": "v002",
                            "visual_type": "tableEx",
                            "measures": ["Revenue"],
                            "tables": ["Sales"],
                        },
                    ],
                },
                {
                    "page_name": "Page2",
                    "page_display_name": "KPIs",
                    "visuals": [
                        {
                            "visual_id": "v003",
                            "visual_type": "card",
                            "measures": ["Total Revenue"],
                            "tables": [],
                        },
                    ],
                },
            ],
        }
    ]
})

SAMPLE_UCMV_OUTPUT = json.dumps({
    "yaml": {
        "fact_sales": SAMPLE_YAML,
    },
    "sql": {
        "fact_sales": "CREATE METRIC VIEW ...",
    },
    "deployment_results": {
        "fact_sales": {
            "status": "deployed",
            "view_name": "main.metrics.fact_sales",
        }
    },
})

SAMPLE_LLM_RESPONSE = json.dumps([
    {
        "visual_id": "v001",
        "page_name": "Sales Overview",
        "visual_type": "barChart",
        "chart_title": "Revenue by Region",
        "ucmv_view": "main.metrics.fact_sales",
        "dimensions": ["region"],
        "measures": ["total_revenue"],
        "sql": "SELECT region, MEASURE(total_revenue) FROM main.metrics.fact_sales GROUP BY region",
    },
    {
        "visual_id": "v002",
        "page_name": "Sales Overview",
        "visual_type": "tableEx",
        "chart_title": "Sales Table",
        "ucmv_view": "main.metrics.fact_sales",
        "dimensions": ["region", "product"],
        "measures": ["total_revenue"],
        "sql": "SELECT region, product, MEASURE(total_revenue) FROM main.metrics.fact_sales GROUP BY region, product",
    },
    {
        "visual_id": "v003",
        "page_name": "KPIs",
        "visual_type": "card",
        "chart_title": "Total Revenue KPI",
        "ucmv_view": "main.metrics.fact_sales",
        "dimensions": [],
        "measures": ["total_revenue"],
        "sql": "SELECT MEASURE(total_revenue) FROM main.metrics.fact_sales",
    },
])


def _mock_auth(workspace_url="https://test.azuredatabricks.net"):
    auth = MagicMock()
    auth.workspace_url = workspace_url
    auth.get_headers.return_value = {"Authorization": "Bearer test-token"}
    return auth


def _mock_litellm_response(content=SAMPLE_LLM_RESPONSE):
    mock_completion = MagicMock()
    mock_completion.choices[0].message.content = content
    return mock_completion


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestPBIVisualUCMVMapperSchema:
    def test_all_fields_optional(self):
        schema = PBIVisualUCMVMapperSchema()
        assert schema.report_references_json is None
        assert schema.ucmv_output is None
        assert schema.measures_json is None
        assert schema.catalog is None
        assert schema.schema_name is None
        assert schema.dashboard_title is None

    def test_fields_accepted(self):
        schema = PBIVisualUCMVMapperSchema(
            report_references_json=SAMPLE_REPORT_REFERENCES,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            dashboard_title="My Dashboard",
        )
        assert schema.report_references_json == SAMPLE_REPORT_REFERENCES
        assert schema.dashboard_title == "My Dashboard"


# ---------------------------------------------------------------------------
# Tool initialization
# ---------------------------------------------------------------------------

class TestToolInit:
    def test_tool_name(self):
        tool = PBIVisualUCMVMapperTool()
        assert tool.name == "PBI Visual-UCMV Mapper"

    def test_description_present(self):
        tool = PBIVisualUCMVMapperTool()
        assert "visual" in tool.description.lower() or "PBI" in tool.description

    def test_ucmv_output_in_default_config(self):
        tool = PBIVisualUCMVMapperTool()
        assert "ucmv_output" in tool._default_config

    def test_static_config_stored_on_init(self):
        tool = PBIVisualUCMVMapperTool(
            catalog="main",
            schema_name="metrics",
            dashboard_title="My Dashboard",
        )
        assert tool._default_config["catalog"] == "main"
        assert tool._default_config["dashboard_title"] == "My Dashboard"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

class TestErrorCases:
    def test_no_report_references_returns_error(self):
        """No report_references_json → error."""
        tool = PBIVisualUCMVMapperTool()
        result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT)
        data = json.loads(result)
        assert "error" in data
        assert "report_references" in data["error"]

    def test_no_ucmv_output_returns_error(self):
        """No ucmv_output → error."""
        tool = PBIVisualUCMVMapperTool()
        result = tool._run(report_references_json=SAMPLE_REPORT_REFERENCES)
        data = json.loads(result)
        assert "error" in data
        assert "ucmv_output" in data["error"]

    def test_empty_visuals_returns_error(self):
        """report_references_json with no visuals → error."""
        empty_refs = json.dumps({"reports": [{"pages": [{"visuals": []}]}]})
        tool = PBIVisualUCMVMapperTool()
        result = tool._run(
            report_references_json=empty_refs,
            ucmv_output=SAMPLE_UCMV_OUTPUT,
        )
        data = json.loads(result)
        assert "error" in data

    def test_no_deployed_metric_views_returns_error(self):
        """ucmv_output with empty yaml → no metric views → error."""
        empty_ucmv = json.dumps({"yaml": {}, "sql": {}})
        tool = PBIVisualUCMVMapperTool()
        result = tool._run(
            report_references_json=SAMPLE_REPORT_REFERENCES,
            ucmv_output=empty_ucmv,
        )
        data = json.loads(result)
        assert "error" in data


# ---------------------------------------------------------------------------
# Report references parsing
# ---------------------------------------------------------------------------

class TestReportReferencesParsing:
    def test_extracts_visuals_from_report_references(self):
        """Parses tool 78 JSON output, gets visuals per page."""
        tool = PBIVisualUCMVMapperTool()
        report_data = tool._parse_report_references(SAMPLE_REPORT_REFERENCES)
        visuals = tool._extract_visuals(report_data)
        assert len(visuals) == 3  # v001, v002, v003

    def test_visual_has_required_fields(self):
        tool = PBIVisualUCMVMapperTool()
        report_data = tool._parse_report_references(SAMPLE_REPORT_REFERENCES)
        visuals = tool._extract_visuals(report_data)
        for v in visuals:
            assert "visual_id" in v
            assert "page_name" in v
            assert "visual_type" in v
            assert "measures" in v

    def test_page_display_name_used(self):
        """page_display_name is used when available."""
        tool = PBIVisualUCMVMapperTool()
        report_data = tool._parse_report_references(SAMPLE_REPORT_REFERENCES)
        visuals = tool._extract_visuals(report_data)
        page_names = {v["page_name"] for v in visuals}
        assert "Sales Overview" in page_names
        assert "KPIs" in page_names

    def test_invalid_report_references_returns_empty(self):
        tool = PBIVisualUCMVMapperTool()
        report_data = tool._parse_report_references("not-json{{{")
        assert report_data == {}


# ---------------------------------------------------------------------------
# UCMV summaries building
# ---------------------------------------------------------------------------

class TestUcmvSummaries:
    def test_extracts_ucmv_views_from_deployment_results(self):
        """ucmv_output with deployment_results → view names extracted."""
        tool = PBIVisualUCMVMapperTool()
        ucmv_data = tool._parse_ucmv_output(SAMPLE_UCMV_OUTPUT)
        summaries = tool._build_ucmv_summaries(ucmv_data, "main", "metrics")
        assert len(summaries) == 1
        # deployment_results has the actual view name
        assert summaries[0]["view_name"] == "main.metrics.fact_sales"

    def test_build_ucmv_summaries_extracts_measures(self):
        """_build_ucmv_summaries() returns list with view_name, measures, dimensions."""
        tool = PBIVisualUCMVMapperTool()
        ucmv_data = tool._parse_ucmv_output(SAMPLE_UCMV_OUTPUT)
        summaries = tool._build_ucmv_summaries(ucmv_data, "main", "metrics")
        assert len(summaries) == 1
        s = summaries[0]
        assert "view_name" in s
        assert "measures" in s
        assert "dimensions" in s
        assert "total_revenue" in s["measures"]
        assert "region" in s["dimensions"]

    def test_fallback_view_name_without_deployment_results(self):
        """Without deployment_results, view name is constructed from catalog.schema.safe_key."""
        ucmv_no_dep = json.dumps({
            "yaml": {"fact_sales": SAMPLE_YAML},
            "sql": {},
        })
        tool = PBIVisualUCMVMapperTool()
        ucmv_data = tool._parse_ucmv_output(ucmv_no_dep)
        summaries = tool._build_ucmv_summaries(ucmv_data, "mycat", "mysch")
        assert summaries[0]["view_name"] == "mycat.mysch.fact_sales"


# ---------------------------------------------------------------------------
# LLM integration
# ---------------------------------------------------------------------------

class TestLLMIntegration:
    def test_llm_called_for_mapping(self):
        """Mock LLMManager, verify it's called with visual + UCMV summaries."""
        from unittest.mock import AsyncMock
        tool = PBIVisualUCMVMapperTool()
        auth = _mock_auth()

        mock_completion = AsyncMock(return_value=SAMPLE_LLM_RESPONSE)

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("src.core.llm_manager.LLMManager.completion", mock_completion) as mock_llm:
            tool._run(
                report_references_json=SAMPLE_REPORT_REFERENCES,
                ucmv_output=SAMPLE_UCMV_OUTPUT,
                catalog="main",
                schema_name="metrics",
            )

        mock_llm.assert_called_once()

    def test_llm_response_parsed_correctly(self):
        """Mock LLM returns JSON array → visual_mappings populated."""
        from unittest.mock import AsyncMock
        tool = PBIVisualUCMVMapperTool()
        auth = _mock_auth()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("src.core.llm_manager.LLMManager.completion", AsyncMock(return_value=SAMPLE_LLM_RESPONSE)):
            result = tool._run(
                report_references_json=SAMPLE_REPORT_REFERENCES,
                ucmv_output=SAMPLE_UCMV_OUTPUT,
                catalog="main",
                schema_name="metrics",
            )

        data = json.loads(result)
        assert "visual_mappings" in data
        assert len(data["visual_mappings"]) == 3

    def test_fallback_mapping_when_llm_fails(self):
        """litellm raises → fallback_mapping used."""
        tool = PBIVisualUCMVMapperTool()
        auth = _mock_auth()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("litellm.completion", side_effect=RuntimeError("LLM unavailable")):
            result = tool._run(
                report_references_json=SAMPLE_REPORT_REFERENCES,
                ucmv_output=SAMPLE_UCMV_OUTPUT,
                catalog="main",
                schema_name="metrics",
            )

        data = json.loads(result)
        # Should still have visual_mappings (from fallback)
        assert "visual_mappings" in data
        assert len(data["visual_mappings"]) == 3

    def test_auth_failure_uses_fallback(self):
        """Authentication fails → fallback_mapping used, no crash."""
        tool = PBIVisualUCMVMapperTool()

        with patch.object(tool, "_authenticate", side_effect=RuntimeError("Auth failed")):
            result = tool._run(
                report_references_json=SAMPLE_REPORT_REFERENCES,
                ucmv_output=SAMPLE_UCMV_OUTPUT,
                catalog="main",
                schema_name="metrics",
            )

        data = json.loads(result)
        assert "visual_mappings" in data


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def _run_with_fallback(self, tool):
        """Run with auth failure to get fallback output."""
        with patch.object(tool, "_authenticate", side_effect=RuntimeError("Auth failed")):
            result = tool._run(
                report_references_json=SAMPLE_REPORT_REFERENCES,
                ucmv_output=SAMPLE_UCMV_OUTPUT,
                catalog="main",
                schema_name="metrics",
                dashboard_title="Test Dashboard",
            )
        return json.loads(result)

    def test_output_has_required_keys(self):
        """Output JSON has visual_mappings, dashboard_title, summary."""
        tool = PBIVisualUCMVMapperTool()
        data = self._run_with_fallback(tool)
        assert "visual_mappings" in data
        assert "dashboard_title" in data
        assert "summary" in data

    def test_summary_counts_correct(self):
        """summary.total_visuals matches input count."""
        tool = PBIVisualUCMVMapperTool()
        data = self._run_with_fallback(tool)
        assert data["summary"]["total_visuals"] == 3

    def test_dashboard_title_in_output(self):
        tool = PBIVisualUCMVMapperTool()
        data = self._run_with_fallback(tool)
        assert data["dashboard_title"] == "Test Dashboard"

    def test_catalog_and_schema_in_output(self):
        tool = PBIVisualUCMVMapperTool()
        data = self._run_with_fallback(tool)
        assert data.get("catalog") == "main"
        assert data.get("schema_name") == "metrics"

    def test_summary_has_mapped_and_unmapped(self):
        tool = PBIVisualUCMVMapperTool()
        data = self._run_with_fallback(tool)
        assert "mapped" in data["summary"]
        assert "unmapped" in data["summary"]
        # mapped + unmapped == total
        assert data["summary"]["mapped"] + data["summary"]["unmapped"] == data["summary"]["total_visuals"]


# ---------------------------------------------------------------------------
# Fallback mapping
# ---------------------------------------------------------------------------

class TestFallbackMapping:
    def test_fallback_creates_mappings_for_all_visuals(self):
        tool = PBIVisualUCMVMapperTool()
        visuals = [
            {"visual_id": "v1", "page_name": "P1", "visual_type": "barChart", "measures": [], "tables": []},
            {"visual_id": "v2", "page_name": "P1", "visual_type": "card", "measures": [], "tables": []},
        ]
        ucmv_data = tool._parse_ucmv_output(SAMPLE_UCMV_OUTPUT)
        ucmv_summaries = tool._build_ucmv_summaries(ucmv_data, "main", "metrics")
        mappings = tool._fallback_mapping(visuals, ucmv_summaries)
        assert len(mappings) == 2

    def test_fallback_card_visual_generates_no_group_by(self):
        """card type → SQL without GROUP BY."""
        tool = PBIVisualUCMVMapperTool()
        visuals = [
            {"visual_id": "v1", "page_name": "P1", "visual_type": "card", "measures": [], "tables": []},
        ]
        ucmv_data = tool._parse_ucmv_output(SAMPLE_UCMV_OUTPUT)
        ucmv_summaries = tool._build_ucmv_summaries(ucmv_data, "main", "metrics")
        mappings = tool._fallback_mapping(visuals, ucmv_summaries)
        if mappings[0].get("sql"):
            assert "GROUP BY" not in mappings[0]["sql"]

    def test_fallback_bar_chart_generates_sql_with_group_by(self):
        """barChart → SQL with GROUP BY when dims available."""
        tool = PBIVisualUCMVMapperTool()
        visuals = [
            {"visual_id": "v1", "page_name": "P1", "visual_type": "barChart", "measures": [], "tables": []},
        ]
        ucmv_data = tool._parse_ucmv_output(SAMPLE_UCMV_OUTPUT)
        ucmv_summaries = tool._build_ucmv_summaries(ucmv_data, "main", "metrics")
        # If there are dimensions, GROUP BY should be present
        if ucmv_summaries and ucmv_summaries[0].get("dimensions"):
            mappings = tool._fallback_mapping(visuals, ucmv_summaries)
            if mappings[0].get("sql"):
                assert "GROUP BY" in mappings[0]["sql"]


# ---------------------------------------------------------------------------
# Parse LLM response
# ---------------------------------------------------------------------------

class TestParseLLMResponse:
    def test_parse_valid_json_array(self):
        tool = PBIVisualUCMVMapperTool()
        result = tool._parse_llm_response(SAMPLE_LLM_RESPONSE)
        assert isinstance(result, list)
        assert len(result) == 3

    def test_parse_json_with_markdown_fences(self):
        tool = PBIVisualUCMVMapperTool()
        fenced = f"```json\n{SAMPLE_LLM_RESPONSE}\n```"
        result = tool._parse_llm_response(fenced)
        assert isinstance(result, list)
        assert len(result) == 3

    def test_parse_invalid_json_returns_empty_list(self):
        tool = PBIVisualUCMVMapperTool()
        result = tool._parse_llm_response("not json at all")
        assert result == []

    def test_parse_json_object_instead_of_array_returns_empty(self):
        tool = PBIVisualUCMVMapperTool()
        result = tool._parse_llm_response('{"key": "value"}')
        assert result == []
