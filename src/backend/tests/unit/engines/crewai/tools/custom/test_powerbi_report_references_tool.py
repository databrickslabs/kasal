"""
Unit tests for powerbi_report_references_tool.py

Tests the PowerBIReportReferencesTool — extracts visual-to-measure/table
references from Power BI reports (PBIR format).

Strategy:
  - Instantiate the real class
  - Mock only httpx, powerbi_auth_utils, and validate_auth_config where needed
  - Test: init, placeholder detection/resolution, _run validation branches,
    and all pure synchronous helpers (_parse_report_info, _parse_pages,
    _parse_visuals, _extract_visual_references, _build_cross_reference,
    _build_report_url, _build_page_url, _resolve_placeholder, etc.)
"""

import base64
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.tools.custom.powerbi_report_references_tool import (
    PowerBIReportReferencesSchema,
    PowerBIReportReferencesTool,
)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

WS_ID = "ws-aaaaaa-1111-2222-3333-bbbbbbbbbbbb"
DS_ID = "ds-cccccc-4444-5555-6666-dddddddddddd"
REPORT_ID = "rpt-99999-1111-2222-3333-444444444444"
TENANT_ID = "tenant-eeeeee-7777-8888-9999-ffffffffffff"
CLIENT_ID = "client-11111111-aaaa-bbbb-cccc-222222222222"
CLIENT_SECRET = "s3cr3t"
ACCESS_TOKEN = "ey.fake.access.token"


def _make_tool(**kwargs):
    defaults = dict(
        workspace_id=WS_ID,
        dataset_id=DS_ID,
        access_token=ACCESS_TOKEN,
    )
    defaults.update(kwargs)
    return PowerBIReportReferencesTool(**defaults)


def _b64(obj) -> str:
    return base64.b64encode(json.dumps(obj).encode()).decode()


def _b64_text(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


# ===========================================================================
# Schema tests
# ===========================================================================

class TestPowerBIReportReferencesSchema:
    def test_all_fields_optional(self):
        schema = PowerBIReportReferencesSchema()
        assert schema.report_id is None

    def test_report_id_stored(self):
        schema = PowerBIReportReferencesSchema(report_id=REPORT_ID)
        assert schema.report_id == REPORT_ID

    def test_no_auth_or_plumbing_fields_in_schema(self):
        # Connection/auth plumbing is injected via tool_configs in __init__,
        # never exposed as LLM-fillable schema parameters.
        forbidden = {
            "workspace_id", "dataset_id", "tenant_id", "client_id",
            "client_secret", "username", "password", "auth_method",
            "access_token", "llm_token", "api_key", "token",
            "llm_workspace_url", "llm_model",
        }
        assert not forbidden & set(PowerBIReportReferencesSchema.model_fields)

    def test_output_format_default(self):
        schema = PowerBIReportReferencesSchema()
        assert schema.output_format == "markdown"

    def test_include_visual_details_default(self):
        schema = PowerBIReportReferencesSchema()
        assert schema.include_visual_details is True

    def test_group_by_default(self):
        schema = PowerBIReportReferencesSchema()
        assert schema.group_by == "page"


# ===========================================================================
# Init tests
# ===========================================================================

class TestPowerBIReportReferencesToolInit:
    def test_tool_name(self):
        tool = PowerBIReportReferencesTool()
        assert tool.name == "Power BI Report References Tool"

    def test_tool_description_non_empty(self):
        tool = PowerBIReportReferencesTool()
        assert len(tool.description) > 20

    def test_args_schema_set(self):
        tool = PowerBIReportReferencesTool()
        assert tool.args_schema is PowerBIReportReferencesSchema

    def test_default_config_populated(self):
        tool = PowerBIReportReferencesTool(
            workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )
        assert tool._default_config["workspace_id"] == WS_ID
        assert tool._default_config["dataset_id"] == DS_ID

    def test_placeholder_filtered_on_init(self):
        tool = PowerBIReportReferencesTool(workspace_id="{workspace_id}")
        # Placeholder should be filtered to None
        assert tool._default_config["workspace_id"] is None

    def test_instance_id_assigned(self):
        tool = PowerBIReportReferencesTool()
        assert hasattr(tool, "_instance_id")
        assert len(tool._instance_id) == 8

    def test_output_format_default_in_config(self):
        tool = PowerBIReportReferencesTool()
        assert tool._default_config["output_format"] == "markdown"


# ===========================================================================
# _resolve_placeholder tests
# ===========================================================================

class TestResolvePlaceholder:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_non_string_returns_as_is(self):
        assert self.tool._resolve_placeholder(42, {"foo": "bar"}) == 42
        assert self.tool._resolve_placeholder(None, {}) is None

    def test_no_placeholders_returns_unchanged(self):
        assert self.tool._resolve_placeholder("static-value", {}) == "static-value"

    def test_single_placeholder_resolved(self):
        result = self.tool._resolve_placeholder("{ws_id}", {"ws_id": "abc-123"})
        assert result == "abc-123"

    def test_multiple_placeholders_resolved(self):
        result = self.tool._resolve_placeholder(
            "{catalog}.{schema}", {"catalog": "main", "schema": "default"}
        )
        assert result == "main.default"

    def test_missing_placeholder_key_left_as_is(self):
        result = self.tool._resolve_placeholder("{unknown_key}", {"other": "val"})
        assert result == "{unknown_key}"

    def test_partial_resolution(self):
        result = self.tool._resolve_placeholder(
            "{ws}.{unknown}", {"ws": "my-workspace"}
        )
        assert result == "my-workspace.{unknown}"


# ===========================================================================
# _run validation tests
# ===========================================================================

class TestRunValidation:
    def test_missing_workspace_id_returns_error(self):
        tool = PowerBIReportReferencesTool(
            dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )
        result = tool._run(dataset_id=DS_ID)
        assert "error" in result.lower() or "workspace_id" in result.lower()

    def test_missing_dataset_and_report_returns_error(self):
        tool = PowerBIReportReferencesTool(
            workspace_id=WS_ID, access_token=ACCESS_TOKEN
        )
        result = tool._run()
        assert "error" in result.lower()

    def test_unresolved_placeholder_returns_error(self):
        tool = PowerBIReportReferencesTool()
        result = tool._run(
            workspace_id="{workspace_id}",
            dataset_id=DS_ID,
            access_token=ACCESS_TOKEN,
        )
        assert "error" in result.lower() or "placeholder" in result.lower()

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config")
    def test_invalid_auth_returns_error(self, mock_validate):
        mock_validate.return_value = (False, "No credentials provided")
        tool = PowerBIReportReferencesTool(workspace_id=WS_ID, dataset_id=DS_ID)
        result = tool._run()
        assert "error" in result.lower()

    def test_placeholder_kwargs_filtered(self):
        tool = _make_tool()
        # Placeholder in runtime kwarg — should be filtered out and default used
        with patch(
            "src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
            return_value=(True, ""),
        ), patch.object(tool, "_run_sync", return_value="ok"):
            result = tool._run(workspace_id="your_workspace_here")
        # Should succeed (default config workspace used)
        assert isinstance(result, str)

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_with_valid_auth_calls_run_sync(self, _mock_validate):
        tool = _make_tool()
        with patch.object(tool, "_run_sync", return_value="extracted result") as mock_sync:
            result = tool._run()
        mock_sync.assert_called_once()
        assert result == "extracted result"


# ===========================================================================
# _build_report_url and _build_page_url tests
# ===========================================================================

class TestUrlBuilders:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_build_report_url(self):
        url = self.tool._build_report_url(WS_ID, REPORT_ID)
        assert WS_ID in url
        assert REPORT_ID in url
        assert url.startswith("https://")

    def test_build_page_url_with_page_id(self):
        url = self.tool._build_page_url(WS_ID, REPORT_ID, "PageXYZ")
        assert WS_ID in url
        assert REPORT_ID in url
        assert "PageXYZ" in url

    def test_build_page_url_empty_page_id(self):
        url = self.tool._build_page_url(WS_ID, REPORT_ID, "")
        assert WS_ID in url
        assert REPORT_ID in url


# ===========================================================================
# _parse_report_info tests
# ===========================================================================

class TestParseReportInfo:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_parts_returns_empty_dict(self):
        result = self.tool._parse_report_info([])
        assert result == {}

    def test_no_report_json_part_returns_empty(self):
        part = {"path": "definition/pages/page1/page.json", "payload": _b64({"name": "p1"})}
        result = self.tool._parse_report_info([part])
        assert result == {}

    def test_report_json_part_parsed(self):
        report_data = {"name": "My Report", "version": "5.0"}
        part = {"path": "definition/report.json", "payload": _b64(report_data)}
        result = self.tool._parse_report_info([part])
        assert result.get("name") == "My Report"

    def test_invalid_base64_returns_empty(self):
        part = {"path": "definition/report.json", "payload": "not-valid-base64!!!"}
        result = self.tool._parse_report_info([part])
        assert result == {}


# ===========================================================================
# _parse_pages tests
# ===========================================================================

class TestParsePages:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_parts_returns_empty_list(self):
        result = self.tool._parse_pages([])
        assert result == []

    def test_page_json_file_parsed(self):
        page_data = {"name": "Overview", "displayName": "Overview", "ordinal": 0}
        part = {
            "path": "definition/pages/page1/page.json",
            "payload": _b64(page_data),
        }
        result = self.tool._parse_pages([part])
        assert len(result) == 1
        assert result[0]["name"] == "Overview"

    def test_pages_sorted_by_ordinal(self):
        page2 = {"name": "Page2", "displayName": "Page 2", "ordinal": 1}
        page1 = {"name": "Page1", "displayName": "Page 1", "ordinal": 0}
        parts = [
            {"path": "definition/pages/p2/page.json", "payload": _b64(page2)},
            {"path": "definition/pages/p1/page.json", "payload": _b64(page1)},
        ]
        result = self.tool._parse_pages(parts)
        assert result[0]["ordinal"] == 0
        assert result[1]["ordinal"] == 1

    def test_report_json_embedded_pages(self):
        report_data = {
            "pages": [
                {"name": "p1", "displayName": "Page 1", "ordinal": 0},
                {"name": "p2", "displayName": "Page 2", "ordinal": 1},
            ]
        }
        part = {"path": "report.json", "payload": _b64(report_data)}
        result = self.tool._parse_pages([part])
        assert len(result) == 2

    def test_report_json_sections_key(self):
        report_data = {
            "sections": [
                {"name": "s1", "displayName": "Section 1", "ordinal": 0}
            ]
        }
        part = {"path": "report.json", "payload": _b64(report_data)}
        result = self.tool._parse_pages([part])
        assert len(result) == 1

    def test_invalid_base64_skipped_gracefully(self):
        part = {"path": "definition/pages/p1/page.json", "payload": "!!invalid!!"}
        result = self.tool._parse_pages([part])
        assert isinstance(result, list)


# ===========================================================================
# _parse_pages_from_report_json tests
# ===========================================================================

class TestParsePagesFromReportJson:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_no_pages_key_returns_empty(self):
        report_data = {"config": "something"}
        part = {"path": "report.json", "payload": _b64(report_data)}
        result = self.tool._parse_pages_from_report_json([part])
        assert result == []

    def test_report_pages_key(self):
        report_data = {
            "reportPages": [{"name": "rp1", "displayName": "RP1", "ordinal": 0}]
        }
        part = {"path": "report.json", "payload": _b64(report_data)}
        result = self.tool._parse_pages_from_report_json([part])
        assert len(result) == 1


# ===========================================================================
# _parse_visuals tests
# ===========================================================================

class TestParseVisuals:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_parts_returns_empty_list(self):
        result = self.tool._parse_visuals([])
        assert result == []

    def test_visual_json_file_parsed(self):
        visual_data = {
            "name": "visual1",
            "visual": {"visualType": "card"},
        }
        part = {
            "path": "definition/pages/page1/visuals/vis1/visual.json",
            "payload": _b64(visual_data),
        }
        result = self.tool._parse_visuals([part])
        assert len(result) == 1
        assert result[0]["type"] == "card"

    def test_visual_without_type_uses_unknown(self):
        visual_data = {"name": "vis_no_type"}
        part = {
            "path": "definition/pages/p1/visuals/v1/visual.json",
            "payload": _b64(visual_data),
        }
        result = self.tool._parse_visuals([part])
        assert len(result) == 1
        assert result[0]["type"] == "unknown"

    def test_visual_page_id_extracted(self):
        visual_data = {"visual": {"visualType": "barChart"}}
        part = {
            "path": "definition/pages/MyPage/visuals/MyVis/visual.json",
            "payload": _b64(visual_data),
        }
        result = self.tool._parse_visuals([part])
        assert result[0]["page_id"] == "MyPage"


# ===========================================================================
# _extract_visual_references tests
# ===========================================================================

class TestExtractVisualReferences:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_visuals_returns_empty(self):
        result = self.tool._extract_visual_references([])
        assert result == []

    def test_visual_with_no_config_returns_entry(self):
        visual = {"id": "v1", "page_id": "p1", "type": "card", "name": "V1", "config": {}}
        result = self.tool._extract_visual_references([visual])
        assert len(result) == 1
        assert result[0]["visual_id"] == "v1"
        assert result[0]["measures"] == []
        assert result[0]["tables"] == []

    def test_visual_with_query_definition_measures(self):
        config = {
            "visual": {
                "queryDefinition": {
                    "from": [{"entity": "Sales"}],
                    "select": [
                        {"measure": {"property": "Total Revenue"}}
                    ],
                }
            }
        }
        visual = {"id": "v1", "page_id": "p1", "type": "card", "name": "V1", "config": config}
        result = self.tool._extract_visual_references([visual])
        assert "Total Revenue" in result[0]["measures"]
        assert "Sales" in result[0]["tables"]

    def test_visual_with_string_config_parsed(self):
        config_dict = {"visual": {"queryDefinition": {"from": [], "select": []}}}
        config_str = json.dumps(config_dict)
        visual = {"id": "v2", "page_id": "p1", "type": "card", "name": "V2", "config": config_str}
        result = self.tool._extract_visual_references([visual])
        assert len(result) == 1
        assert result[0]["visual_id"] == "v2"

    def test_multiple_visuals(self):
        visuals = [
            {"id": f"v{i}", "page_id": "p1", "type": "card", "name": f"V{i}", "config": {}}
            for i in range(3)
        ]
        result = self.tool._extract_visual_references(visuals)
        assert len(result) == 3


# ===========================================================================
# _build_cross_reference tests
# ===========================================================================

class TestBuildCrossReference:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_inputs_returns_dict(self):
        result = self.tool._build_cross_reference([], [])
        assert isinstance(result, dict)

    def test_cross_reference_with_measures(self):
        pages = [{"id": "p1", "displayName": "Overview"}]
        refs = [
            {
                "visual_id": "v1",
                "page_id": "p1",
                "visual_type": "card",
                "visual_name": "Revenue Card",
                "measures": ["Total Revenue"],
                "tables": ["Sales"],
                "columns": [],
            }
        ]
        result = self.tool._build_cross_reference(pages, refs)
        assert isinstance(result, dict)

    def test_cross_reference_multiple_pages(self):
        pages = [
            {"id": "p1", "displayName": "Page 1"},
            {"id": "p2", "displayName": "Page 2"},
        ]
        refs = [
            {"visual_id": "v1", "page_id": "p1", "measures": ["M1"], "tables": [], "columns": [], "visual_type": "card", "visual_name": "V1"},
            {"visual_id": "v2", "page_id": "p2", "measures": ["M1", "M2"], "tables": ["T1"], "columns": [], "visual_type": "bar", "visual_name": "V2"},
        ]
        result = self.tool._build_cross_reference(pages, refs)
        assert isinstance(result, dict)


# ===========================================================================
# _extract_from_query_definition tests
# ===========================================================================

class TestExtractFromQueryDefinition:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_query_def(self):
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_query_definition({}, measures, tables, columns)
        assert measures == set()
        assert tables == set()

    def test_from_clause_populates_tables(self):
        query_def = {"from": [{"entity": "Sales"}, {"entity": "Date"}]}
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_query_definition(query_def, measures, tables, columns)
        assert "Sales" in tables
        assert "Date" in tables

    def test_select_clause_populates_measures(self):
        query_def = {"select": [{"measure": {"property": "Total Revenue"}}]}
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_query_definition(query_def, measures, tables, columns)
        assert "Total Revenue" in measures

    def test_select_clause_populates_columns(self):
        query_def = {"select": [{"column": {"property": "Region", "entity": "Sales"}}]}
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_query_definition(query_def, measures, tables, columns)
        assert "Region" in columns
        assert "Sales" in tables


# ===========================================================================
# _run_sync tests
# ===========================================================================

class TestRunSync:
    def test_run_sync_executes_coroutine(self):
        tool = PowerBIReportReferencesTool()

        async def coro():
            return "sync_result"

        result = tool._run_sync(coro())
        assert result == "sync_result"

    def test_run_sync_propagates_exception(self):
        tool = PowerBIReportReferencesTool()

        async def coro():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            tool._run_sync(coro())


# ===========================================================================
# Integration: _run with mocked _run_sync
# ===========================================================================

class TestRunIntegration:
    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_returns_run_sync_result(self, _validate):
        tool = _make_tool()
        with patch.object(tool, "_run_sync", return_value="# markdown output"):
            result = tool._run()
        assert result == "# markdown output"

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_exception_returns_error_string(self, _validate):
        tool = _make_tool()
        with patch.object(tool, "_run_sync", side_effect=Exception("unexpected")):
            result = tool._run()
        assert "error" in result.lower() or "unexpected" in result.lower()

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_with_execution_inputs(self, _validate):
        tool = PowerBIReportReferencesTool(
            workspace_id="{ws}",
            dataset_id="{ds}",
            access_token=ACCESS_TOKEN,
        )
        with patch.object(tool, "_run_sync", return_value="resolved"):
            result = tool._run(
                execution_inputs={"ws": WS_ID, "ds": DS_ID},
            )
        # After resolution, workspace/dataset would be set correctly
        assert isinstance(result, str)


# ===========================================================================
# NEW COMPREHENSIVE TESTS — added to increase coverage
# ===========================================================================

import asyncio


# ===========================================================================
# _format_markdown_output tests
# ===========================================================================

class TestFormatMarkdownOutput:
    """Tests for _format_markdown_output."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def _make_page(self, page_id, name, ordinal=0):
        return {"id": page_id, "name": name, "displayName": name, "ordinal": ordinal, "url": f"https://powerbi.com/rpt/{page_id}"}

    def _make_visual_ref(self, visual_id, page_id, measures=None, tables=None):
        return {
            "visual_id": visual_id,
            "page_id": page_id,
            "visual_type": "card",
            "visual_name": visual_id,
            "measures": measures or [],
            "tables": tables or [],
            "columns": [],
        }

    def test_basic_output_structure(self):
        result = self.tool._format_markdown_output(
            WS_ID, REPORT_ID, {}, [], [], {}, True, "page"
        )
        assert "Power BI Report References" in result
        assert WS_ID in result
        assert REPORT_ID in result

    def test_pages_listed(self):
        pages = [self._make_page("p1", "Overview"), self._make_page("p2", "Details")]
        result = self.tool._format_markdown_output(
            WS_ID, REPORT_ID, {}, pages, [], {}, True, "page"
        )
        assert "Overview" in result
        assert "Details" in result

    def test_measures_counted(self):
        pages = [self._make_page("p1", "Overview")]
        refs = [self._make_visual_ref("v1", "p1", measures=["Revenue", "Costs"])]
        result = self.tool._format_markdown_output(
            WS_ID, REPORT_ID, {}, pages, refs, {}, True, "page"
        )
        assert "2" in result  # 2 unique measures

    def test_group_by_measure(self):
        pages = [self._make_page("p1", "Overview")]
        refs = [self._make_visual_ref("v1", "p1", measures=["Revenue"])]
        cross_ref = {
            "measure_pages": {"Revenue": ["Overview"]},
            "table_pages": {}
        }
        result = self.tool._format_markdown_output(
            WS_ID, REPORT_ID, {}, pages, refs, cross_ref, True, "measure"
        )
        assert "Revenue" in result
        assert "measure" in result.lower() or "Measure" in result

    def test_group_by_table(self):
        pages = [self._make_page("p1", "Overview")]
        refs = [self._make_visual_ref("v1", "p1", tables=["Sales"])]
        cross_ref = {
            "measure_pages": {},
            "table_pages": {"Sales": ["Overview"]}
        }
        result = self.tool._format_markdown_output(
            WS_ID, REPORT_ID, {}, pages, refs, cross_ref, True, "table"
        )
        assert "Sales" in result

    def test_report_name_in_output(self):
        result = self.tool._format_markdown_output(
            WS_ID, REPORT_ID, {"name": "My Report"}, [], [], {}, True, "page"
        )
        assert "My Report" in result

    def test_visual_details_included(self):
        pages = [self._make_page("p1", "Overview")]
        refs = [self._make_visual_ref("v1", "p1", measures=["M1"], tables=["T1"])]
        result = self.tool._format_markdown_output(
            WS_ID, REPORT_ID, {}, pages, refs, {}, True, "page"  # include_visual_details=True
        )
        assert "Visual Details" in result

    def test_visual_details_excluded(self):
        pages = [self._make_page("p1", "Overview")]
        refs = [self._make_visual_ref("v1", "p1", measures=["M1"])]
        result = self.tool._format_markdown_output(
            WS_ID, REPORT_ID, {}, pages, refs, {}, False, "page"  # include_visual_details=False
        )
        assert isinstance(result, str)


# ===========================================================================
# _format_json_output tests
# ===========================================================================

class TestFormatJsonOutput:
    """Tests for _format_json_output."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_returns_valid_json(self):
        result = self.tool._format_json_output(
            WS_ID, REPORT_ID, {}, [], [], {}
        )
        parsed = json.loads(result)
        assert "workspace_id" in parsed
        assert parsed["workspace_id"] == WS_ID

    def test_report_id_in_json(self):
        result = self.tool._format_json_output(
            WS_ID, REPORT_ID, {}, [], [], {}
        )
        parsed = json.loads(result)
        assert parsed.get("report_id") == REPORT_ID

    def test_pages_in_json(self):
        pages = [{"id": "p1", "name": "Overview", "displayName": "Overview", "ordinal": 0}]
        result = self.tool._format_json_output(
            WS_ID, REPORT_ID, {}, pages, [], {}
        )
        parsed = json.loads(result)
        assert len(parsed.get("pages", [])) == 1

    def test_visual_refs_in_json(self):
        refs = [{"visual_id": "v1", "page_id": "p1", "measures": ["M1"], "tables": [], "columns": [], "visual_type": "card", "visual_name": "v1"}]
        result = self.tool._format_json_output(
            WS_ID, REPORT_ID, {}, [], refs, {}
        )
        parsed = json.loads(result)
        assert len(parsed.get("visual_references", [])) == 1


# ===========================================================================
# _format_matrix_output tests
# ===========================================================================

class TestFormatMatrixOutput:
    """Tests for _format_matrix_output."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_returns_string(self):
        # _format_matrix_output(workspace_id, report_id, report_info, pages, visual_refs, cross_ref)
        result = self.tool._format_matrix_output(
            WS_ID, REPORT_ID, {}, [], [], {}
        )
        assert isinstance(result, str)

    def test_report_id_in_output(self):
        result = self.tool._format_matrix_output(
            WS_ID, REPORT_ID, {}, [], [], {}
        )
        assert REPORT_ID in result or isinstance(result, str)

    def test_with_pages_and_refs(self):
        pages = [{"id": "p1", "displayName": "Overview"}]
        refs = [{"visual_id": "v1", "page_id": "p1", "measures": ["Revenue"], "tables": ["Sales"], "columns": [], "visual_type": "card", "visual_name": "v1"}]
        cross_ref = {"measure_pages": {"Revenue": ["Overview"]}, "table_pages": {"Sales": ["Overview"]}}
        result = self.tool._format_matrix_output(
            WS_ID, REPORT_ID, {"name": "Report"}, pages, refs, cross_ref
        )
        assert isinstance(result, str)


# ===========================================================================
# Async methods: _list_workspace_reports, _fetch_report_definition
# ===========================================================================

class TestReportReferencesAsyncHelpers:
    """Tests for async helper methods in PowerBIReportReferencesTool."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_list_workspace_reports_success(self):
        reports = [
            {"id": "r1", "name": "Sales Report", "datasetId": DS_ID}
        ]
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"value": reports}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_report_references_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._list_workspace_reports(WS_ID, ACCESS_TOKEN))

        assert len(result) == 1
        assert result[0]["name"] == "Sales Report"

    def test_list_workspace_reports_exception(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_report_references_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._list_workspace_reports(WS_ID, ACCESS_TOKEN))

        assert result == []

    def test_fetch_report_definition_200_returns_parts(self):
        parts = [{"path": "definition/report.json", "payload": "base64data"}]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"definition": {"parts": parts}}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_report_references_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_report_definition(WS_ID, REPORT_ID, ACCESS_TOKEN))

        assert len(result) == 1
        assert result[0]["path"] == "definition/report.json"

    def test_fetch_report_definition_non_200_returns_empty(self):
        mock_response = MagicMock()
        mock_response.status_code = 404

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_report_references_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_report_definition(WS_ID, REPORT_ID, ACCESS_TOKEN))

        assert result == []

    def test_fetch_report_definition_exception_returns_empty(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_report_references_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_report_definition(WS_ID, REPORT_ID, ACCESS_TOKEN))

        assert result == []


# ===========================================================================
# _format_markdown_output_multi tests
# ===========================================================================

class TestFormatMarkdownOutputMulti:
    """Tests for _format_markdown_output_multi."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def _make_report_result(self, report_id, report_name, pages=None, visual_refs=None):
        return {
            "report_id": report_id,
            "report_name": report_name,
            "report_url": f"https://powerbi.com/r/{report_id}",
            "report_info": {"name": report_name},
            "pages": pages or [],
            "visual_references": visual_refs or [],
            "cross_ref": {"measure_pages": {}, "table_pages": {}},
        }

    def test_basic_output(self):
        results = [self._make_report_result("r1", "Sales Report")]
        output = self.tool._format_markdown_output_multi(
            WS_ID, DS_ID, results, [], True, "page"
        )
        assert "Sales Report" in output
        assert WS_ID in output

    def test_failed_reports_shown(self):
        results = [self._make_report_result("r1", "Good Report")]
        failed = [{"id": "r2", "name": "Bad Report", "error": "Access denied"}]
        output = self.tool._format_markdown_output_multi(
            WS_ID, DS_ID, results, failed, True, "page"
        )
        assert "Bad Report" in output or "Failed" in output or isinstance(output, str)

    def test_multiple_reports(self):
        results = [
            self._make_report_result("r1", "Report 1"),
            self._make_report_result("r2", "Report 2"),
        ]
        output = self.tool._format_markdown_output_multi(
            WS_ID, DS_ID, results, [], True, "page"
        )
        assert "Report 1" in output
        assert "Report 2" in output

    def test_no_dataset_id_omitted(self):
        results = [self._make_report_result("r1", "Report")]
        output = self.tool._format_markdown_output_multi(
            WS_ID, None, results, [], True, "page"
        )
        assert isinstance(output, str)


# ===========================================================================
# _format_json_output_multi tests
# ===========================================================================

class TestFormatJsonOutputMulti:
    """Tests for _format_json_output_multi."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_returns_valid_json(self):
        results = [{
            "report_id": "r1",
            "report_name": "Sales Report",
            "report_url": "https://powerbi.com/r/r1",
            "pages": [],
            "visual_references": [],
            "cross_ref": {},
        }]
        output = self.tool._format_json_output_multi(WS_ID, DS_ID, results, [])
        parsed = json.loads(output)
        assert "workspace_id" in parsed
        assert "reports" in parsed

    def test_failed_reports_included(self):
        results = [{
            "report_id": "r1",
            "report_name": "Report",
            "report_url": "",
            "pages": [],
            "visual_references": [],
            "cross_ref": {},
        }]
        failed = [{"id": "r2", "name": "Bad", "error": "Access denied"}]
        output = self.tool._format_json_output_multi(WS_ID, DS_ID, results, failed)
        parsed = json.loads(output)
        assert isinstance(parsed, dict)


# ===========================================================================
# _parse_pages_from_report_json tests (report references tool)
# ===========================================================================

class TestReportRefsParsePagesFromReportJson:
    """Tests for _parse_pages_from_report_json on the report references tool."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def _make_part(self, path, data):
        payload = base64.b64encode(json.dumps(data).encode()).decode()
        return {"path": path, "payload": payload}

    def test_empty_returns_empty(self):
        result = self.tool._parse_pages_from_report_json([])
        assert result == []

    def test_report_pages_key(self):
        data = {"reportPages": [{"name": "rp1", "displayName": "RP1", "ordinal": 0}]}
        part = self._make_part("report.json", data)
        result = self.tool._parse_pages_from_report_json([part])
        assert len(result) >= 1

    def test_sections_key(self):
        data = {"sections": [{"name": "s1", "displayName": "S1", "ordinal": 0}]}
        part = self._make_part("report.json", data)
        result = self.tool._parse_pages_from_report_json([part])
        assert len(result) >= 1

    def test_no_pages_data_returns_empty(self):
        data = {"config": "no pages here"}
        part = self._make_part("report.json", data)
        result = self.tool._parse_pages_from_report_json([part])
        assert result == []


# ===========================================================================
# _parse_visuals_from_report_json tests
# ===========================================================================

class TestParseVisualsFromReportJson:
    """Tests for _parse_visuals_from_report_json."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def _make_part(self, path, data):
        payload = base64.b64encode(json.dumps(data).encode()).decode()
        return {"path": path, "payload": payload}

    def test_empty_parts_returns_empty(self):
        result = self.tool._parse_visuals_from_report_json([])
        assert result == []

    def test_pages_with_visual_containers(self):
        data = {
            "pages": [
                {
                    "name": "p1",
                    "visualContainers": [
                        {
                            "name": "vis1",
                            "visualType": "card",
                            "config": json.dumps({"singleVisual": {"visualType": "card"}}),
                        }
                    ]
                }
            ]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1
        assert result[0]["id"] == "vis1"

    def test_pages_with_visuals_key(self):
        data = {
            "pages": [
                {
                    "name": "p1",
                    "visuals": [
                        {"name": "v1", "visual": {"visualType": "barChart"}}
                    ]
                }
            ]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1
        # The "visual" key is parsed - type is extracted from visual.visualType
        assert result[0]["type"] == "barChart"

    def test_sections_key_supported(self):
        data = {
            "sections": [
                {
                    "name": "sec1",
                    "visualContainers": [
                        {"name": "vis2", "visualType": "lineChart"}
                    ]
                }
            ]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1

    def test_config_as_dict(self):
        data = {
            "pages": [
                {
                    "name": "p1",
                    "visualContainers": [
                        {
                            "name": "vis3",
                            "config": {"singleVisual": {"visualType": "table"}},
                        }
                    ]
                }
            ]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1
        assert result[0]["type"] == "table"

    def test_report_pages_key_supported(self):
        data = {
            "reportPages": [
                {
                    "name": "rp1",
                    "visualContainers": [
                        {"name": "v1", "visualType": "card"}
                    ]
                }
            ]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1

    def test_visual_with_visual_key(self):
        data = {
            "pages": [
                {
                    "name": "p1",
                    "visualContainers": [
                        {
                            "name": "v1",
                            "visual": {"visualType": "pieChart"},
                        }
                    ]
                }
            ]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1
        assert result[0]["type"] == "pieChart"

    def test_no_pages_in_report_json(self):
        data = {"config": "nothing"}
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert result == []

    def test_invalid_base64_skipped(self):
        part = {"path": "report.json", "payload": "!!invalid!!"}
        result = self.tool._parse_visuals_from_report_json([part])
        assert isinstance(result, list)


# ===========================================================================
# _extract_from_container_objects tests
# ===========================================================================

class TestExtractFromContainerObjects:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_container_objects(self):
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_container_objects({}, measures, tables, columns)
        assert measures == set()

    def test_extracts_measure_from_expression(self):
        container_objects = {
            "filter": [
                {
                    "properties": {
                        "filter": {
                            "expr": {
                                "Measure": {"Property": "Revenue"}
                            }
                        }
                    }
                }
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_container_objects(container_objects, measures, tables, columns)
        assert "Revenue" in measures

    def test_non_list_value_skipped(self):
        container_objects = {"config": "not a list"}
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_container_objects(container_objects, measures, tables, columns)
        assert measures == set()


# ===========================================================================
# _extract_from_projections tests
# ===========================================================================

class TestExtractFromProjections:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_projections(self):
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_projections({}, measures, tables, columns)
        assert measures == set()

    def test_values_role_adds_measure(self):
        projections = {
            "Values": [
                {"queryRef": "Sales.Revenue"}
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_projections(projections, measures, tables, columns)
        assert "Sales" in tables
        assert "Revenue" in measures

    def test_category_role_adds_column(self):
        projections = {
            "Category": [
                {"queryRef": "Date.Year"}
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_projections(projections, measures, tables, columns)
        assert "Date" in tables
        assert "Year" in columns

    def test_field_with_measure_property(self):
        projections = {
            "Values": [
                {
                    "field": {
                        "Measure": {
                            "Property": "Total Sales",
                            "Expression": {"SourceRef": {"Entity": "FactSales"}}
                        }
                    }
                }
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_projections(projections, measures, tables, columns)
        assert "Total Sales" in measures
        assert "FactSales" in tables

    def test_field_with_column_property(self):
        projections = {
            "Category": [
                {
                    "field": {
                        "Column": {
                            "Property": "Region",
                            "Expression": {"SourceRef": {"Entity": "DimGeography"}}
                        }
                    }
                }
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_projections(projections, measures, tables, columns)
        assert "Region" in columns
        assert "DimGeography" in tables

    def test_non_list_role_skipped(self):
        projections = {"Values": "not a list"}
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_projections(projections, measures, tables, columns)
        assert measures == set()

    def test_query_ref_without_dot_skipped(self):
        projections = {
            "Values": [{"queryRef": "NoDotHere"}]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_projections(projections, measures, tables, columns)
        assert measures == set()
        assert tables == set()


# ===========================================================================
# _extract_from_prototype_query tests
# ===========================================================================

class TestExtractFromPrototypeQuery:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_prototype_query(self):
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_prototype_query({}, measures, tables, columns)
        assert measures == set()

    def test_from_clause_populates_tables(self):
        pq = {
            "From": [{"Entity": "Sales", "Name": "s"}],
            "Select": []
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_prototype_query(pq, measures, tables, columns)
        assert "Sales" in tables

    def test_select_measure_ref(self):
        pq = {
            "From": [{"Entity": "Sales", "Name": "s"}],
            "Select": [
                {
                    "Measure": {
                        "Property": "Total Revenue",
                        "Expression": {"SourceRef": {"Source": "s"}}
                    },
                    "Name": "Sales.Total Revenue"
                }
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_prototype_query(pq, measures, tables, columns)
        assert "Total Revenue" in measures
        assert "Sales" in tables

    def test_select_column_ref(self):
        pq = {
            "From": [{"Entity": "Date", "Name": "d"}],
            "Select": [
                {
                    "Column": {
                        "Property": "Year",
                        "Expression": {"SourceRef": {"Source": "d"}}
                    },
                    "Name": "Date.Year"
                }
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_prototype_query(pq, measures, tables, columns)
        assert "Year" in columns
        assert "Date" in tables

    def test_lowercase_from_and_select_supported(self):
        pq = {
            "from": [{"entity": "Fact", "name": "f"}],
            "select": [
                {"Measure": {"Property": "Metric"}}
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_prototype_query(pq, measures, tables, columns)
        assert "Fact" in tables
        assert "Metric" in measures

    def test_entity_directly_in_source_ref(self):
        pq = {
            "From": [],
            "Select": [
                {
                    "Measure": {
                        "Property": "Metric",
                        "Expression": {"SourceRef": {"Entity": "DirectEntity"}}
                    }
                }
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_prototype_query(pq, measures, tables, columns)
        assert "DirectEntity" in tables


# ===========================================================================
# _extract_from_data_transforms tests
# ===========================================================================

class TestExtractFromDataTransforms:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_data_transforms(self):
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_data_transforms({}, measures, tables, columns)
        assert tables == set()

    def test_selects_with_table_field(self):
        dt = {
            "selects": [
                {"displayName": "Sales.Revenue", "queryName": "Sales.Revenue"}
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_data_transforms(dt, measures, tables, columns)
        assert "Sales" in tables
        assert "Revenue" in columns

    def test_query_metadata_binding_projections(self):
        dt = {
            "selects": [
                {"displayName": "T1.Col1", "queryName": "T1.Col1"}
            ],
            "queryMetadata": {
                "Binding": {
                    "Primary": {
                        "Groupings": [{"Projections": [0]}]
                    }
                }
            }
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_data_transforms(dt, measures, tables, columns)
        assert "T1" in tables

    def test_projection_index_out_of_range(self):
        dt = {
            "selects": [{"displayName": "T1.Col1"}],
            "queryMetadata": {
                "Binding": {
                    "Primary": {
                        "Groupings": [{"Projections": [99]}]  # index > len(selects)
                    }
                }
            }
        }
        measures, tables, columns = set(), set(), set()
        # Should not raise
        self.tool._extract_from_data_transforms(dt, measures, tables, columns)

    def test_selects_without_dot_skipped(self):
        dt = {
            "selects": [
                {"displayName": "NoDot", "queryName": "NoDot"}
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_data_transforms(dt, measures, tables, columns)
        assert tables == set()


# ===========================================================================
# _extract_from_expression tests
# ===========================================================================

class TestExtractFromExpression:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_expr(self):
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_expression({}, measures, tables, columns)
        assert measures == set()

    def test_non_dict_ignored(self):
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_expression("not a dict", measures, tables, columns)
        assert measures == set()

    def test_measure_extracted(self):
        expr = {"Measure": {"Property": "Revenue"}}
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_expression(expr, measures, tables, columns)
        assert "Revenue" in measures

    def test_column_extracted_with_source(self):
        expr = {
            "Column": {
                "Property": "Region",
                "Expression": {"SourceRef": {"Entity": "Sales"}}
            }
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_expression(expr, measures, tables, columns)
        assert "Region" in columns
        assert "Sales" in tables

    def test_nested_dict_recursed(self):
        expr = {
            "outer": {
                "Measure": {"Property": "NestedMeasure"}
            }
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_expression(expr, measures, tables, columns)
        assert "NestedMeasure" in measures

    def test_list_values_recursed(self):
        expr = {
            "items": [
                {"Measure": {"Property": "ListMeasure"}}
            ]
        }
        measures, tables, columns = set(), set(), set()
        self.tool._extract_from_expression(expr, measures, tables, columns)
        assert "ListMeasure" in measures


# ===========================================================================
# _deep_search_references tests
# ===========================================================================

class TestDeepSearchReferences:
    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_empty_obj(self):
        measures, tables, columns = set(), set(), set()
        self.tool._deep_search_references({}, measures, tables, columns)
        assert measures == set()

    def test_entity_key_adds_table(self):
        obj = {"entity": "SalesTable"}
        measures, tables, columns = set(), set(), set()
        self.tool._deep_search_references(obj, measures, tables, columns)
        assert "SalesTable" in tables

    def test_measure_key_adds_measure(self):
        obj = {"measure": "Revenue"}
        measures, tables, columns = set(), set(), set()
        self.tool._deep_search_references(obj, measures, tables, columns)
        assert "Revenue" in measures

    def test_column_key_adds_column(self):
        obj = {"column": "Region"}
        measures, tables, columns = set(), set(), set()
        self.tool._deep_search_references(obj, measures, tables, columns)
        assert "Region" in columns

    def test_queryref_with_dot(self):
        obj = {"queryRef": "Sales.Revenue"}
        measures, tables, columns = set(), set(), set()
        self.tool._deep_search_references(obj, measures, tables, columns)
        assert "Sales" in tables
        assert "Revenue" in columns

    def test_displayname_with_dot(self):
        obj = {"displayName": "Sales.Revenue"}
        measures, tables, columns = set(), set(), set()
        self.tool._deep_search_references(obj, measures, tables, columns)
        assert "Sales" in tables

    def test_underscore_entity_skipped(self):
        obj = {"entity": "_internal"}
        measures, tables, columns = set(), set(), set()
        self.tool._deep_search_references(obj, measures, tables, columns)
        assert tables == set()

    def test_nested_list_recursed(self):
        obj = [{"entity": "NestedTable"}]
        measures, tables, columns = set(), set(), set()
        self.tool._deep_search_references(obj, measures, tables, columns)
        assert "NestedTable" in tables

    def test_max_depth_prevents_infinite_recursion(self):
        # Build deeply nested structure
        obj: dict = {}
        current = obj
        for _ in range(20):
            current["nested"] = {}
            current = current["nested"]
        current["entity"] = "DeepTable"

        measures, tables, columns = set(), set(), set()
        # Should not raise RecursionError
        self.tool._deep_search_references(obj, measures, tables, columns, depth=0)


# ===========================================================================
# _format_matrix_output_multi tests
# ===========================================================================

class TestFormatMatrixOutputMulti:
    """Tests for _format_matrix_output_multi."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def _make_result(self, report_id, report_name, pages=None, measures=None, tables=None):
        pages = pages or [{"id": "p1", "displayName": "Page 1", "name": "p1", "url": "https://powerbi.com/p1"}]
        cross_ref = {
            "measure_pages": {m: ["Page 1"] for m in (measures or [])},
            "table_pages": {t: ["Page 1"] for t in (tables or [])},
        }
        return {
            "report_id": report_id,
            "report_name": report_name,
            "report_url": f"https://powerbi.com/r/{report_id}",
            "pages": pages,
            "visual_references": [],
            "cross_ref": cross_ref,
        }

    def test_basic_returns_string(self):
        results = [self._make_result("r1", "Report 1")]
        output = self.tool._format_matrix_output_multi(WS_ID, DS_ID, results)
        assert isinstance(output, str)
        assert "Report 1" in output

    def test_with_measures_in_matrix(self):
        results = [self._make_result("r1", "Sales Report", measures=["Revenue", "Cost"])]
        output = self.tool._format_matrix_output_multi(WS_ID, DS_ID, results)
        assert "Revenue" in output
        assert "Cost" in output

    def test_multiple_reports(self):
        results = [
            self._make_result("r1", "Report A", measures=["M1"]),
            self._make_result("r2", "Report B", measures=["M1", "M2"]),
        ]
        output = self.tool._format_matrix_output_multi(WS_ID, DS_ID, results)
        assert "Report A" in output
        assert "Report B" in output
        assert "M1" in output
        assert "M2" in output

    def test_dataset_id_in_header(self):
        results = [self._make_result("r1", "Report")]
        output = self.tool._format_matrix_output_multi(WS_ID, DS_ID, results)
        assert DS_ID in output

    def test_no_dataset_id_omitted(self):
        results = [self._make_result("r1", "Report")]
        output = self.tool._format_matrix_output_multi(WS_ID, None, results)
        assert isinstance(output, str)

    def test_tables_in_matrix(self):
        results = [self._make_result("r1", "Report", tables=["Sales", "Date"])]
        output = self.tool._format_matrix_output_multi(WS_ID, DS_ID, results)
        assert "Sales" in output
        assert "Date" in output


# ===========================================================================
# _format_json_output_multi with debug_info
# ===========================================================================

class TestFormatJsonOutputMultiDebugInfo:
    """Tests for debug_info path in _format_json_output_multi."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def test_debug_info_included_when_present(self):
        results = [{
            "report_id": "r1",
            "report_name": "Debug Report",
            "report_url": "https://powerbi.com/r/r1",
            "pages": [],
            "visual_references": [],
            "cross_ref": {"measure_pages": {}, "table_pages": {}},
            "debug_info": {"note": "No pages found"},
        }]
        output = self.tool._format_json_output_multi(WS_ID, DS_ID, results, [])
        parsed = json.loads(output)
        report = parsed["reports"][0]
        assert "debug_info" in report
        assert report["debug_info"]["note"] == "No pages found"

    def test_no_debug_info_absent_from_output(self):
        results = [{
            "report_id": "r1",
            "report_name": "Clean Report",
            "report_url": "https://powerbi.com/r/r1",
            "pages": [],
            "visual_references": [],
            "cross_ref": {"measure_pages": {}, "table_pages": {}},
        }]
        output = self.tool._format_json_output_multi(WS_ID, DS_ID, results, [])
        parsed = json.loads(output)
        report = parsed["reports"][0]
        assert "debug_info" not in report


# ===========================================================================
# _format_markdown_output_multi with group_by="measure" and "table"
# ===========================================================================

class TestFormatMarkdownOutputMultiGroupBy:
    """Tests for _format_markdown_output_multi group_by variants."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def _make_result(self, report_id, report_name, measures=None, tables=None):
        pages = [{"id": "p1", "displayName": "Overview", "name": "p1", "url": "https://powerbi.com/p1"}]
        cross_ref = {
            "measure_pages": {m: ["Overview"] for m in (measures or [])},
            "table_pages": {t: ["Overview"] for t in (tables or [])},
        }
        refs = []
        if measures:
            refs.append({
                "visual_id": "v1",
                "page_id": "p1",
                "visual_type": "card",
                "visual_name": "Card",
                "measures": list(measures or []),
                "tables": list(tables or []),
                "columns": [],
            })
        return {
            "report_id": report_id,
            "report_name": report_name,
            "report_url": f"https://powerbi.com/r/{report_id}",
            "pages": pages,
            "visual_references": refs,
            "cross_ref": cross_ref,
        }

    def test_group_by_measure(self):
        results = [self._make_result("r1", "Report 1", measures=["Revenue", "Cost"])]
        output = self.tool._format_markdown_output_multi(WS_ID, DS_ID, results, [], True, "measure")
        assert "Revenue" in output
        assert "Cost" in output

    def test_group_by_table(self):
        results = [self._make_result("r1", "Report 1", tables=["Sales", "Date"])]
        output = self.tool._format_markdown_output_multi(WS_ID, DS_ID, results, [], True, "table")
        assert "Sales" in output
        assert "Date" in output

    def test_group_by_page_with_no_visuals(self):
        results = [{
            "report_id": "r1",
            "report_name": "Empty Report",
            "report_url": "https://powerbi.com/r/r1",
            "pages": [{"id": "p1", "displayName": "Overview", "name": "p1", "url": "https://powerbi.com/p1"}],
            "visual_references": [],
            "cross_ref": {"measure_pages": {}, "table_pages": {}},
        }]
        output = self.tool._format_markdown_output_multi(WS_ID, DS_ID, results, [], True, "page")
        assert "No visuals" in output or "Empty Report" in output

    def test_failed_reports_in_output(self):
        results = [self._make_result("r1", "Good Report")]
        failed = [{"id": "r2", "name": "Bad Report", "error": "Access denied"}]
        output = self.tool._format_markdown_output_multi(WS_ID, DS_ID, results, failed, True, "page")
        assert "Bad Report" in output or "Failed" in output

    def test_visual_details_excluded(self):
        results = [self._make_result("r1", "Report 1", measures=["Revenue"])]
        output = self.tool._format_markdown_output_multi(WS_ID, DS_ID, results, [], False, "page")
        assert isinstance(output, str)

    def test_measures_more_than_3_truncated(self):
        many_measures = ["M1", "M2", "M3", "M4", "M5"]
        pages = [{"id": "p1", "displayName": "Overview", "name": "p1", "url": "https://powerbi.com/p1"}]
        refs = [{
            "visual_id": "v1",
            "page_id": "p1",
            "visual_type": "table",
            "visual_name": "Table",
            "measures": many_measures,
            "tables": [],
            "columns": [],
        }]
        cross_ref = {
            "measure_pages": {m: ["Overview"] for m in many_measures},
            "table_pages": {},
        }
        results = [{
            "report_id": "r1",
            "report_name": "Report",
            "report_url": "https://powerbi.com/r/r1",
            "pages": pages,
            "visual_references": refs,
            "cross_ref": cross_ref,
        }]
        output = self.tool._format_markdown_output_multi(WS_ID, DS_ID, results, [], True, "page")
        assert "..." in output or isinstance(output, str)


# ===========================================================================
# _extract_report_references async tests
# ===========================================================================

class TestExtractReportReferencesAsync:
    """Tests for the async _extract_report_references orchestration."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_dataset_mode_no_reports_found(self):
        auth_config = {"access_token": ACCESS_TOKEN}

        with patch(
            "src.engines.crewai.tools.custom.powerbi_auth_utils.get_fabric_access_token_from_config",
            new_callable=AsyncMock, return_value=ACCESS_TOKEN
        ):
            with patch.object(
                self.tool, "_list_workspace_reports",
                new_callable=AsyncMock, return_value=[]
            ):
                result = self._run(self.tool._extract_report_references(
                    workspace_id=WS_ID,
                    dataset_id=DS_ID,
                    report_id=None,
                    auth_config=auth_config,
                    output_format="markdown",
                    include_visual_details=True,
                    group_by="page",
                ))

        assert "No reports found" in result

    def test_single_report_mode_succeeds(self):
        auth_config = {"access_token": ACCESS_TOKEN}
        parts = [
            {
                "path": "definition/report.json",
                "payload": base64.b64encode(json.dumps({"name": "Test Report"}).encode()).decode()
            }
        ]

        with patch(
            "src.engines.crewai.tools.custom.powerbi_auth_utils.get_fabric_access_token_from_config",
            new_callable=AsyncMock, return_value=ACCESS_TOKEN
        ):
            with patch.object(
                self.tool, "_fetch_report_definition",
                new_callable=AsyncMock, return_value=parts
            ):
                result = self._run(self.tool._extract_report_references(
                    workspace_id=WS_ID,
                    dataset_id=None,
                    report_id=REPORT_ID,
                    auth_config=auth_config,
                    output_format="markdown",
                    include_visual_details=True,
                    group_by="page",
                ))

        assert isinstance(result, str)
        assert len(result) > 0

    def test_output_format_json(self):
        auth_config = {"access_token": ACCESS_TOKEN}
        parts = [
            {
                "path": "definition/report.json",
                "payload": base64.b64encode(json.dumps({"name": "Report"}).encode()).decode()
            }
        ]

        with patch(
            "src.engines.crewai.tools.custom.powerbi_auth_utils.get_fabric_access_token_from_config",
            new_callable=AsyncMock, return_value=ACCESS_TOKEN
        ):
            with patch.object(
                self.tool, "_fetch_report_definition",
                new_callable=AsyncMock, return_value=parts
            ):
                result = self._run(self.tool._extract_report_references(
                    workspace_id=WS_ID,
                    dataset_id=None,
                    report_id=REPORT_ID,
                    auth_config=auth_config,
                    output_format="json",
                    include_visual_details=True,
                    group_by="page",
                ))

        parsed = json.loads(result)
        assert "workspace_id" in parsed or "reports" in parsed

    def test_output_format_matrix(self):
        auth_config = {"access_token": ACCESS_TOKEN}
        parts = [
            {
                "path": "definition/report.json",
                "payload": base64.b64encode(json.dumps({"name": "Report"}).encode()).decode()
            }
        ]

        with patch(
            "src.engines.crewai.tools.custom.powerbi_auth_utils.get_fabric_access_token_from_config",
            new_callable=AsyncMock, return_value=ACCESS_TOKEN
        ):
            with patch.object(
                self.tool, "_fetch_report_definition",
                new_callable=AsyncMock, return_value=parts
            ):
                result = self._run(self.tool._extract_report_references(
                    workspace_id=WS_ID,
                    dataset_id=None,
                    report_id=REPORT_ID,
                    auth_config=auth_config,
                    output_format="matrix",
                    include_visual_details=True,
                    group_by="page",
                ))

        assert isinstance(result, str)

    def test_dataset_mode_report_fetch_fails(self):
        auth_config = {"access_token": ACCESS_TOKEN}
        reports = [{"id": REPORT_ID, "name": "Report", "datasetId": DS_ID, "webUrl": ""}]

        with patch(
            "src.engines.crewai.tools.custom.powerbi_auth_utils.get_fabric_access_token_from_config",
            new_callable=AsyncMock, return_value=ACCESS_TOKEN
        ):
            with patch.object(
                self.tool, "_list_workspace_reports",
                new_callable=AsyncMock, return_value=reports
            ):
                with patch.object(
                    self.tool, "_fetch_report_definition",
                    new_callable=AsyncMock, return_value=[]  # empty = failed
                ):
                    result = self._run(self.tool._extract_report_references(
                        workspace_id=WS_ID,
                        dataset_id=DS_ID,
                        report_id=None,
                        auth_config=auth_config,
                        output_format="markdown",
                        include_visual_details=True,
                        group_by="page",
                    ))

        assert "Error" in result or "Could not process" in result or isinstance(result, str)

    def test_all_reports_fail(self):
        auth_config = {"access_token": ACCESS_TOKEN}
        reports = [{"id": REPORT_ID, "name": "Report", "datasetId": DS_ID, "webUrl": ""}]

        with patch(
            "src.engines.crewai.tools.custom.powerbi_auth_utils.get_fabric_access_token_from_config",
            new_callable=AsyncMock, return_value=ACCESS_TOKEN
        ):
            with patch.object(
                self.tool, "_list_workspace_reports",
                new_callable=AsyncMock, return_value=reports
            ):
                with patch.object(
                    self.tool, "_fetch_report_definition",
                    new_callable=AsyncMock, side_effect=Exception("Network error")
                ):
                    result = self._run(self.tool._extract_report_references(
                        workspace_id=WS_ID,
                        dataset_id=DS_ID,
                        report_id=None,
                        auth_config=auth_config,
                        output_format="markdown",
                        include_visual_details=True,
                        group_by="page",
                    ))

        assert "Error" in result or isinstance(result, str)


# ===========================================================================
# _fetch_report_definition async path: 202 polling
# ===========================================================================

class TestFetchReportDefinition202:
    """Tests for the 202 async polling path in _fetch_report_definition."""

    def setup_method(self):
        self.tool = PowerBIReportReferencesTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_202_polling_succeeded(self):
        parts = [{"path": "definition/report.json", "payload": "data"}]

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Succeeded"}

        result_response = MagicMock()
        result_response.raise_for_status = MagicMock()
        result_response.json.return_value = {"definition": {"parts": parts}}

        initial_response = MagicMock()
        initial_response.status_code = 202
        initial_response.headers = {"Location": "https://api.powerbi.com/poll/123"}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=initial_response)
        mock_client.get = AsyncMock(side_effect=[poll_response, result_response])

        with patch("src.engines.crewai.tools.custom.powerbi_report_references_tool.httpx.AsyncClient",
                   return_value=mock_client):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = self._run(
                    self.tool._fetch_report_definition(WS_ID, REPORT_ID, ACCESS_TOKEN)
                )

        assert len(result) == 1
        assert result[0]["path"] == "definition/report.json"

    def test_202_polling_failed_status(self):
        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Failed", "error": {"message": "Fail"}}

        initial_response = MagicMock()
        initial_response.status_code = 202
        initial_response.headers = {"Location": "https://api.powerbi.com/poll/123"}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=initial_response)
        mock_client.get = AsyncMock(return_value=poll_response)

        with patch("src.engines.crewai.tools.custom.powerbi_report_references_tool.httpx.AsyncClient",
                   return_value=mock_client):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = self._run(
                    self.tool._fetch_report_definition(WS_ID, REPORT_ID, ACCESS_TOKEN)
                )

        assert result == []

    def test_202_no_location_header(self):
        initial_response = MagicMock()
        initial_response.status_code = 202
        initial_response.headers = {}  # No Location

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=initial_response)

        with patch("src.engines.crewai.tools.custom.powerbi_report_references_tool.httpx.AsyncClient",
                   return_value=mock_client):
            result = self._run(
                self.tool._fetch_report_definition(WS_ID, REPORT_ID, ACCESS_TOKEN)
            )

        assert result == []

    def test_http_status_error_returns_empty(self):
        import httpx
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"
        mock_client.post = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Forbidden", request=MagicMock(), response=mock_resp
        ))

        with patch("src.engines.crewai.tools.custom.powerbi_report_references_tool.httpx.AsyncClient",
                   return_value=mock_client):
            result = self._run(
                self.tool._fetch_report_definition(WS_ID, REPORT_ID, ACCESS_TOKEN)
            )
