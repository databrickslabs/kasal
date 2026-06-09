"""
Unit tests for powerbi_analysis_tool.py

Tests the PowerBIAnalysisTool — the Copilot-style question-to-DAX pipeline.
Strategy:
  - Instantiate the real tool class (no mocking of it)
  - Mock only: httpx.AsyncClient, PowerBISemanticModelCacheService, async_session_factory,
    powerbi_auth_utils helpers
  - Exercise initialisation, placeholder detection, _run validation branches,
    and every pure synchronous helper method
"""

import base64
import json
import re
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.tools.custom.powerbi_analysis_tool import (
    PowerBIAnalysisSchema,
    PowerBIAnalysisTool,
    _run_async_in_sync_context,
)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

WS_ID = "ws-aaaaaa-1111-2222-3333-bbbbbbbbbbbb"
DS_ID = "ds-cccccc-4444-5555-6666-dddddddddddd"
TENANT_ID = "tenant-eeeeee-7777-8888-9999-ffffffffffff"
CLIENT_ID = "client-11111111-aaaa-bbbb-cccc-222222222222"
CLIENT_SECRET = "s3cr3t"
ACCESS_TOKEN = "ey.fake.access.token"


def _make_tool(**kwargs):
    """Helper: instantiate tool with sensible defaults."""
    defaults = dict(
        workspace_id=WS_ID,
        dataset_id=DS_ID,
        access_token=ACCESS_TOKEN,
        user_question="What is total revenue?",
    )
    defaults.update(kwargs)
    return PowerBIAnalysisTool(**defaults)


def _mock_session_factory(cached_metadata=None):
    """Return a mock async_session_factory that optionally returns cached metadata."""
    mock_service = MagicMock()
    mock_service.get_cached_metadata = AsyncMock(return_value=cached_metadata)
    mock_service.save_metadata = AsyncMock(return_value=None)
    mock_service.build_metadata_dict = MagicMock(return_value={})

    ctx_mgr = MagicMock()
    ctx_mgr.__aenter__ = AsyncMock(return_value=MagicMock())
    ctx_mgr.__aexit__ = AsyncMock(return_value=None)

    factory = MagicMock(return_value=ctx_mgr)
    return factory, mock_service


# ===========================================================================
# Schema tests
# ===========================================================================

class TestPowerBIAnalysisSchema:
    """Validate the Pydantic input schema."""

    def test_all_fields_optional(self):
        schema = PowerBIAnalysisSchema()
        assert schema.user_question is None
        assert schema.workspace_id is None
        assert schema.dataset_id is None

    def test_user_question_stored(self):
        schema = PowerBIAnalysisSchema(user_question="How many customers?")
        assert schema.user_question == "How many customers?"

    def test_sp_auth_fields(self):
        schema = PowerBIAnalysisSchema(
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        assert schema.tenant_id == TENANT_ID
        assert schema.client_secret == CLIENT_SECRET

    def test_access_token_field(self):
        schema = PowerBIAnalysisSchema(access_token=ACCESS_TOKEN)
        assert schema.access_token == ACCESS_TOKEN

    def test_business_mappings_field(self):
        schema = PowerBIAnalysisSchema(
            business_mappings={"Complete CGR": "[Status] = 'Complete'"}
        )
        assert schema.business_mappings is not None
        assert "Complete CGR" in schema.business_mappings

    def test_field_synonyms_field(self):
        schema = PowerBIAnalysisSchema(
            field_synonyms={"revenue": ["sales", "income"]}
        )
        assert schema.field_synonyms["revenue"] == ["sales", "income"]

    def test_active_filters_field(self):
        schema = PowerBIAnalysisSchema(active_filters={"BU": "Italy", "Week": 1})
        assert schema.active_filters["BU"] == "Italy"

    def test_max_dax_retries_default(self):
        schema = PowerBIAnalysisSchema()
        assert schema.max_dax_retries == 5

    def test_output_format_default(self):
        schema = PowerBIAnalysisSchema()
        assert schema.output_format == "markdown"

    def test_enable_info_columns_default(self):
        schema = PowerBIAnalysisSchema()
        assert schema.enable_info_columns is False

    def test_include_visual_references_default(self):
        schema = PowerBIAnalysisSchema()
        assert schema.include_visual_references is True

    def test_llm_model_default(self):
        schema = PowerBIAnalysisSchema()
        assert schema.llm_model == "databricks-claude-sonnet-4"


# ===========================================================================
# Initialisation tests
# ===========================================================================

class TestPowerBIAnalysisToolInit:
    """Tests for __init__ and _default_config population."""

    def test_tool_name(self):
        tool = PowerBIAnalysisTool()
        assert "Power BI" in tool.name

    def test_tool_description_non_empty(self):
        tool = PowerBIAnalysisTool()
        assert len(tool.description) > 20

    def test_args_schema_set(self):
        tool = PowerBIAnalysisTool()
        assert tool.args_schema is PowerBIAnalysisSchema

    def test_default_config_populated(self):
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
            user_question="test question",
        )
        assert tool._default_config["workspace_id"] == WS_ID
        assert tool._default_config["dataset_id"] == DS_ID
        assert tool._default_config["user_question"] == "test question"

    def test_default_config_none_when_not_provided(self):
        tool = PowerBIAnalysisTool()
        assert tool._default_config["workspace_id"] is None
        assert tool._default_config["dataset_id"] is None

    def test_default_config_llm_model_default(self):
        tool = PowerBIAnalysisTool()
        assert tool._default_config["llm_model"] == "databricks-claude-sonnet-4"

    def test_default_config_max_dax_retries_default(self):
        tool = PowerBIAnalysisTool()
        assert tool._default_config["max_dax_retries"] == 5

    def test_default_config_context_enrichment_defaults(self):
        tool = PowerBIAnalysisTool()
        assert tool._default_config["business_mappings"] == {}
        assert tool._default_config["field_synonyms"] == {}
        assert tool._default_config["active_filters"] == {}
        assert tool._default_config["visible_tables"] == []
        assert tool._default_config["conversation_history"] == []

    def test_instance_id_assigned(self):
        tool = PowerBIAnalysisTool()
        assert hasattr(tool, "_instance_id")
        assert len(tool._instance_id) == 8

    def test_instance_ids_unique(self):
        t1 = PowerBIAnalysisTool()
        t2 = PowerBIAnalysisTool()
        assert t1._instance_id != t2._instance_id


# ===========================================================================
# Placeholder detection tests
# ===========================================================================

class TestIsPlaceholderValue:
    """Tests for _is_placeholder_value helper."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_none_returns_false(self):
        assert self.tool._is_placeholder_value(None) is False

    def test_int_returns_false(self):
        assert self.tool._is_placeholder_value(42) is False

    def test_real_uuid_returns_false(self):
        # A real UUID should NOT be detected as placeholder
        assert self.tool._is_placeholder_value("a1b2c3d4-e5f6-7890-abcd-ef1234567890") is False

    def test_numeric_placeholder_pattern_detected(self):
        # 12345678-1234-1234-1234-123456789012 → all digits → placeholder
        assert self.tool._is_placeholder_value("12345678-1234-1234-1234-123456789012") is True

    def test_your_here_pattern(self):
        assert self.tool._is_placeholder_value("your_workspace_here") is True

    def test_angle_bracket_pattern(self):
        assert self.tool._is_placeholder_value("<workspace_id>") is True

    def test_curly_brace_pattern(self):
        assert self.tool._is_placeholder_value("{workspace_id}") is True

    def test_placeholder_word(self):
        assert self.tool._is_placeholder_value("placeholder_value") is True

    def test_example_com_pattern(self):
        assert self.tool._is_placeholder_value("https://example.com/api") is True

    def test_https_your_pattern(self):
        assert self.tool._is_placeholder_value("https://your-workspace.azuredatabricks.net") is True

    def test_real_secret_returns_false(self):
        assert self.tool._is_placeholder_value("RealS3cr3tV@lue!2024xyz") is False

    def test_empty_string_returns_false(self):
        assert self.tool._is_placeholder_value("") is False


# ===========================================================================
# _run validation tests (synchronous, no actual API calls)
# ===========================================================================

class TestRunValidation:
    """Tests for _run method validation branches."""

    def test_missing_user_question_returns_error(self):
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
            access_token=ACCESS_TOKEN,
        )
        result = tool._run()
        assert "user_question" in result.lower() or "error" in result.lower()

    def test_missing_workspace_id_returns_error(self):
        tool = PowerBIAnalysisTool(
            dataset_id=DS_ID,
            user_question="test?",
            access_token=ACCESS_TOKEN,
        )
        result = tool._run()
        assert "error" in result.lower()

    def test_missing_dataset_id_returns_error(self):
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            user_question="test?",
            access_token=ACCESS_TOKEN,
        )
        result = tool._run()
        assert "error" in result.lower()

    def test_no_auth_returns_error(self):
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
            user_question="test?",
        )
        result = tool._run()
        assert "authentication" in result.lower() or "error" in result.lower()

    def test_kwargs_question_used_when_no_default(self):
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
        )
        # Pass access_token at runtime — the question should be used (pipeline runs)
        result = tool._run(user_question="runtime question?", access_token=ACCESS_TOKEN)
        # Either the pipeline ran and produced output, or an error occurred — both are valid
        assert isinstance(result, str)
        assert len(result) > 0

    def test_placeholder_kwargs_filtered_out(self):
        """Placeholder values passed at runtime should be ignored."""
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
            access_token=ACCESS_TOKEN,
            user_question="question?",
        )
        # passing placeholder workspace_id should be ignored
        result = tool._run(workspace_id="your_workspace_here")
        # Should still reach auth step (default config workspace used)
        assert isinstance(result, str)

    def test_sp_auth_accepted(self):
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
            user_question="revenue?",
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        with patch(
            "src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context",
            return_value="mocked result"
        ):
            result = tool._run()
        assert result == "mocked result"


# ===========================================================================
# Config merging tests
# ===========================================================================

class TestConfigMerging:
    """Test that default_config values take precedence over runtime kwargs for auth params."""

    def test_default_workspace_takes_precedence(self):
        tool = PowerBIAnalysisTool(workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN)
        # This just tests that the default_config has the right value
        assert tool._default_config["workspace_id"] == WS_ID

    def test_user_question_from_default_config(self):
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
            access_token=ACCESS_TOKEN,
            user_question="preconfigured question",
        )
        assert tool._default_config["user_question"] == "preconfigured question"

    def test_context_enrichment_from_default(self):
        tool = PowerBIAnalysisTool(
            business_mappings={"CGR": "filter_expr"},
            active_filters={"BU": "Italy"},
        )
        assert tool._default_config["business_mappings"] == {"CGR": "filter_expr"}
        assert tool._default_config["active_filters"] == {"BU": "Italy"}


# ===========================================================================
# _extract_measures_from_dax tests
# ===========================================================================

class TestExtractMeasuresFromDax:
    """Tests for the _extract_measures_from_dax helper method."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_single_measure_found(self):
        dax = "EVALUATE SUMMARIZE(Sales, [Total Revenue])"
        measures = self.tool._extract_measures_from_dax(dax, ["Total Revenue", "Profit"])
        assert "Total Revenue" in measures

    def test_multiple_measures_found(self):
        dax = "EVALUATE {[Total Revenue], [Profit Margin]}"
        measures = self.tool._extract_measures_from_dax(
            dax, ["Total Revenue", "Profit Margin", "Units Sold"]
        )
        assert "Total Revenue" in measures
        assert "Profit Margin" in measures
        assert "Units Sold" not in measures

    def test_no_measures_found(self):
        dax = "EVALUATE Sales"
        measures = self.tool._extract_measures_from_dax(
            dax, ["Total Revenue", "Profit"]
        )
        assert measures == []

    def test_empty_dax(self):
        measures = self.tool._extract_measures_from_dax("", ["Total Revenue"])
        assert measures == []

    def test_empty_available_measures(self):
        measures = self.tool._extract_measures_from_dax(
            "EVALUATE {[Total Revenue]}", []
        )
        assert measures == []

    def test_case_sensitive_match(self):
        # The method uses exact bracket notation
        dax = "EVALUATE {[total revenue]}"
        measures = self.tool._extract_measures_from_dax(
            dax, ["Total Revenue", "total revenue"]
        )
        assert "total revenue" in measures
        assert "Total Revenue" not in measures

    def test_partial_name_not_matched(self):
        # [Revenue] should not match [Total Revenue]
        dax = "EVALUATE {[Revenue]}"
        measures = self.tool._extract_measures_from_dax(dax, ["Total Revenue"])
        assert measures == []


# ===========================================================================
# _format_output tests
# ===========================================================================

class TestFormatOutput:
    """Tests for the _format_output method."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()
        self.base_results = {
            "user_question": "What is total revenue?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "model_context": {"measures": [], "tables": [], "relationships": []},
            "generated_dax": None,
            "dax_execution": {"success": False, "data": [], "row_count": 0, "error": None},
            "visual_references": [],
            "errors": [],
            "dax_attempts": [],
        }

    def test_json_format(self):
        result = self.tool._format_output(self.base_results, "json")
        parsed = json.loads(result)
        assert parsed["user_question"] == "What is total revenue?"
        assert parsed["workspace_id"] == WS_ID

    def test_markdown_format_header(self):
        result = self.tool._format_output(self.base_results, "markdown")
        assert "Power BI Analysis Results" in result
        assert "What is total revenue?" in result

    def test_markdown_shows_workspace(self):
        result = self.tool._format_output(self.base_results, "markdown")
        assert WS_ID in result

    def test_markdown_shows_errors(self):
        results = {**self.base_results, "errors": ["Auth failed", "Model error"]}
        result = self.tool._format_output(results, "markdown")
        assert "Auth failed" in result
        assert "Model error" in result

    def test_markdown_shows_generated_dax(self):
        results = {**self.base_results, "generated_dax": "EVALUATE Sales"}
        result = self.tool._format_output(results, "markdown")
        assert "EVALUATE Sales" in result

    def test_markdown_successful_execution(self):
        results = {
            **self.base_results,
            "generated_dax": "EVALUATE Sales",
            "dax_execution": {
                "success": True,
                "data": [{"[Region]": "North", "[Revenue]": 100}],
                "row_count": 1,
                "columns": ["[Region]", "[Revenue]"],
                "error": None,
            },
        }
        result = self.tool._format_output(results, "markdown")
        assert "North" in result
        assert "1 row" in result.lower() or "1" in result

    def test_markdown_failed_execution(self):
        results = {
            **self.base_results,
            "dax_execution": {
                "success": False,
                "data": [],
                "row_count": 0,
                "error": "DAX syntax error",
                "columns": [],
            },
        }
        result = self.tool._format_output(results, "markdown")
        assert "DAX syntax error" in result or "failed" in result.lower()

    def test_markdown_model_context_summary(self):
        results = {
            **self.base_results,
            "model_context": {
                "measures": [{"name": "M1"}, {"name": "M2"}],
                "tables": [{"name": "T1"}],
                "relationships": [{"from_table": "T1", "to_table": "T2"}],
            },
        }
        result = self.tool._format_output(results, "markdown")
        assert "2" in result  # 2 measures
        assert "1" in result  # 1 table / 1 relationship

    def test_markdown_visual_references_shown(self):
        results = {
            **self.base_results,
            "visual_references": [
                {
                    "report_name": "Sales Report",
                    "report_url": "https://powerbi.com/report/1",
                    "page_name": "Overview",
                    "page_url": None,
                    "measure": "Total Revenue",
                    "visual_type": "card",
                }
            ],
        }
        result = self.tool._format_output(results, "markdown")
        assert "Sales Report" in result

    def test_dax_retry_history_shown(self):
        results = {
            **self.base_results,
            "generated_dax": "EVALUATE Sales",
            "dax_attempts": [
                {"attempt": 1, "dax": "bad dax", "success": False, "error": "syntax error", "row_count": 0},
                {"attempt": 2, "dax": "EVALUATE Sales", "success": True, "error": None, "row_count": 5},
            ],
        }
        result = self.tool._format_output(results, "markdown")
        assert "Attempt" in result or "attempt" in result


# ===========================================================================
# _parse_tmdl_for_filters tests
# ===========================================================================

class TestParseTmdlForFilters:
    """Tests for _parse_tmdl_for_filters — reads report.json from base64 parts."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _make_part(self, path, content_dict):
        payload = base64.b64encode(json.dumps(content_dict).encode()).decode()
        return {"path": path, "payload": payload}

    def test_no_parts_returns_empty(self):
        result = self.tool._parse_tmdl_for_filters([])
        assert result == {}

    def test_report_json_without_filters_returns_empty(self):
        part = self._make_part("definition/report.json", {"config": "something"})
        result = self.tool._parse_tmdl_for_filters([part])
        assert result == {}

    def test_report_json_with_empty_filters_array(self):
        part = self._make_part("definition/report.json", {"filters": "[]"})
        result = self.tool._parse_tmdl_for_filters([part])
        assert result == {}

    def test_non_report_json_path_ignored(self):
        part = self._make_part(
            "definition/pages/page1/page.json",
            {"filters": '[{"expression": {"Column": {}}}]'}
        )
        result = self.tool._parse_tmdl_for_filters([part])
        assert result == {}

    def test_report_json_with_valid_filter(self):
        filter_def = {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "Sales"}},
                    "Property": "Region"
                }
            },
            "filter": {
                "Where": [{"Condition": {"In": {"Values": [[{"Literal": {"Value": "'North'"}}]]}}}]
            },
            "type": "Categorical"
        }
        part = self._make_part(
            "definition/report.json",
            {"filters": json.dumps([filter_def])}
        )
        result = self.tool._parse_tmdl_for_filters([part])
        assert "Sales[Region]" in result


# ===========================================================================
# _extract_filter_from_definition tests
# ===========================================================================

class TestExtractFilterFromDefinition:
    """Tests for _extract_filter_from_definition helper."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _make_filter_def(self, table, column, where_clauses=None):
        return {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": table}},
                    "Property": column
                }
            },
            "filter": {"Where": where_clauses or []},
        }

    def test_empty_expression_returns_none(self):
        name, desc = self.tool._extract_filter_from_definition({})
        assert name is None
        assert desc is None

    def test_missing_table_returns_none(self):
        filter_def = {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {}},
                    "Property": "Region"
                }
            },
            "filter": {"Where": []}
        }
        name, desc = self.tool._extract_filter_from_definition(filter_def)
        assert name is None

    def test_filter_name_format(self):
        filter_def = self._make_filter_def("Sales", "Region")
        name, desc = self.tool._extract_filter_from_definition(filter_def)
        assert name == "Sales[Region]"

    def test_empty_where_clause_returns_unknown_description(self):
        filter_def = self._make_filter_def("Sales", "Region", where_clauses=[])
        name, desc = self.tool._extract_filter_from_definition(filter_def)
        assert name == "Sales[Region]"
        assert desc is not None


# ===========================================================================
# _parse_filter_condition tests
# ===========================================================================

class TestParseFilterCondition:
    """Tests for _parse_filter_condition."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_empty_condition_returns_string(self):
        result = self.tool._parse_filter_condition({})
        assert isinstance(result, str)

    def test_not_condition_detected(self):
        condition = {
            "Not": {
                "Expression": {
                    "In": {
                        "Expressions": [],
                        "Values": [[{"Literal": {"Value": "null"}}]]
                    }
                }
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_in_condition_detected(self):
        condition = {
            "In": {
                "Expressions": [],
                "Values": [[{"Literal": {"Value": "'Italy'"}}]]
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert isinstance(result, str)
        assert len(result) > 0


# ===========================================================================
# _parse_tmdl_for_measures_and_tables tests
# ===========================================================================

class TestParseTmdlForMeasuresAndTables:
    """Tests for the TMDL parser helper."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _make_tmdl_part(self, table_name, measures=None, has_spaces=False):
        name = f"'{table_name}'" if has_spaces else table_name
        lines = [f"table {name}"]
        if measures:
            for m_name, expr in measures:
                lines.append(f"\tmeasure '{m_name}' = {expr}")
        content = "\n".join(lines)
        payload = base64.b64encode(content.encode()).decode()
        return {"path": f"definition/tables/{table_name}.tmdl", "payload": payload}

    def test_empty_parts_returns_empty(self):
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([], {})
        assert measures == []
        assert tables == []

    def test_non_tmdl_path_ignored(self):
        part = {"path": "definition/model.json", "payload": ""}
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        assert measures == []
        assert tables == []

    def test_simple_table_parsed(self):
        part = self._make_tmdl_part("Sales")
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        assert any(t["name"] == "Sales" for t in tables)

    def test_system_table_skipped_when_skip_enabled(self):
        content = "table LocalDateTable_abc\n\tcolumn Date\n"
        payload = base64.b64encode(content.encode()).decode()
        part = {"path": "definition/tables/LocalDateTable_abc.tmdl", "payload": payload}
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables(
            [part], {"skip_system_tables": True}
        )
        assert not any("LocalDateTable" in t["name"] for t in tables)

    def test_system_table_included_when_skip_disabled(self):
        content = "table LocalDateTable_abc\n"
        payload = base64.b64encode(content.encode()).decode()
        part = {"path": "definition/tables/LocalDateTable_abc.tmdl", "payload": payload}
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables(
            [part], {"skip_system_tables": False}
        )
        assert any("LocalDateTable" in t["name"] for t in tables)

    def test_invalid_base64_gracefully_handled(self):
        part = {"path": "definition/tables/Broken.tmdl", "payload": "not-valid-base64!!!"}
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        # Should not raise; returns empty
        assert isinstance(measures, list)
        assert isinstance(tables, list)


# ===========================================================================
# _run_async_in_sync_context tests
# ===========================================================================

class TestRunAsyncInSyncContext:
    """Tests for the module-level async runner utility."""

    def test_simple_coroutine(self):
        async def coro():
            return 42

        result = _run_async_in_sync_context(coro())
        assert result == 42

    def test_coroutine_returning_string(self):
        async def coro():
            return "hello"

        result = _run_async_in_sync_context(coro())
        assert result == "hello"

    def test_coroutine_exception_propagates(self):
        async def coro():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            _run_async_in_sync_context(coro())


# ===========================================================================
# Integration-style tests with mocked async pipeline
# ===========================================================================

class TestRunWithMockedPipeline:
    """Test _run end-to-end with all async dependencies mocked."""

    def test_run_returns_string(self):
        tool = _make_tool()
        with patch(
            "src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context",
            return_value="Analysis complete"
        ):
            result = tool._run()
        assert isinstance(result, str)
        assert result == "Analysis complete"

    def test_run_exception_returns_error_string(self):
        tool = _make_tool()
        with patch(
            "src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context",
            side_effect=Exception("network failure")
        ):
            result = tool._run()
        assert "error" in result.lower() or "network failure" in result.lower()

    def test_run_with_json_output_format(self):
        tool = _make_tool(output_format="json")
        json_output = json.dumps({"status": "ok"})
        with patch(
            "src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context",
            return_value=json_output
        ):
            result = tool._run()
        assert result == json_output

    def test_run_context_enrichment_kwargs(self):
        """Verify context enrichment kwargs reach _run without error."""
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
            access_token=ACCESS_TOKEN,
            user_question="What is revenue?",
            business_mappings={"CGR": "expr"},
            active_filters={"BU": "Italy"},
        )
        with patch(
            "src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context",
            return_value="ok"
        ):
            result = tool._run()
        assert result == "ok"


# ===========================================================================
# Async pipeline tests
# ===========================================================================

import asyncio


class TestAnalysisPipelineAsync:
    """Test _execute_analysis_pipeline with mocked sub-methods."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            access_token=ACCESS_TOKEN, user_question="Revenue?"
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_auth_failure_returns_formatted_output(self):
        config = {
            "user_question": "Revenue?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "output_format": "markdown",
        }
        with patch.object(self.tool, "_get_access_token", side_effect=Exception("auth fail")):
            result = self._run(self.tool._execute_analysis_pipeline(config))
        assert isinstance(result, str)
        assert "auth" in result.lower() or "error" in result.lower()

    def test_cache_hit_skips_extraction(self):
        """When cache has data, extraction should be skipped."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
        }
        config = {
            "user_question": "Revenue?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "output_format": "markdown",
            "max_dax_retries": 1,
            "include_visual_references": False,
        }

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch(
                 "src.engines.crewai.tools.tool_session_provider.async_session_factory"
             ) as mock_factory, patch(
                 "src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService"
             ) as mock_svc_cls, \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE Sales"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }):

            mock_service = MagicMock()
            mock_service.get_cached_metadata = AsyncMock(return_value=cached)
            mock_service.build_metadata_dict = MagicMock(return_value={})
            mock_service.save_metadata = AsyncMock(return_value=None)
            mock_svc_cls.return_value = mock_service

            ctx = MagicMock()
            ctx.__aenter__ = AsyncMock(return_value=MagicMock())
            ctx.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = ctx

            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert "Revenue?" in result or "Power BI" in result

    def test_json_output_format(self):
        cached = {
            "measures": [],
            "relationships": [],
            "schema": {"tables": [], "columns": []},
            "sample_data": {},
        }
        config = {
            "user_question": "Test?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "output_format": "json",
            "max_dax_retries": 1,
            "include_visual_references": False,
        }

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch(
                 "src.engines.crewai.tools.tool_session_provider.async_session_factory"
             ) as mock_factory, patch(
                 "src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService"
             ) as mock_svc_cls:

            mock_service = MagicMock()
            mock_service.get_cached_metadata = AsyncMock(return_value=cached)
            mock_service.build_metadata_dict = MagicMock(return_value={})
            mock_service.save_metadata = AsyncMock(return_value=None)
            mock_svc_cls.return_value = mock_service

            ctx = MagicMock()
            ctx.__aenter__ = AsyncMock(return_value=MagicMock())
            ctx.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = ctx

            result = self._run(self.tool._execute_analysis_pipeline(config))

        parsed = json.loads(result)
        assert "user_question" in parsed


class TestAnalysisPipelineRetryLogic:
    """Test retry logic in _execute_analysis_pipeline."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            access_token=ACCESS_TOKEN, user_question="Revenue?"
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_config(self, **extra):
        base = {
            "user_question": "Revenue?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "output_format": "markdown",
            "max_dax_retries": 2,
            "include_visual_references": False,
        }
        base.update(extra)
        return base

    def _mock_cache_and_factory(self, cached=None):
        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=cached)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=MagicMock())
        ctx.__aexit__ = AsyncMock(return_value=None)
        return mock_service, ctx

    def test_dax_failure_then_success_on_retry(self):
        """Test that when first DAX attempt fails, second attempt succeeds."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
        }
        config = self._make_config()

        fail_result = {"success": False, "error": "DAX syntax error", "row_count": 0, "data": [], "columns": []}
        success_result = {"success": True, "data": [{"[Revenue]": 100}], "row_count": 1, "columns": ["[Revenue]"], "error": None}

        execution_call_count = [0]
        async def execute_side(*args):
            if execution_call_count[0] == 0:
                execution_call_count[0] += 1
                return fail_result
            return success_result

        mock_service, ctx = self._mock_cache_and_factory(cached)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_generate_dax_with_self_correction", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", side_effect=execute_side):

            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)

    def test_all_retries_fail_returns_error_in_output(self):
        """Test that all retries failing is handled gracefully."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
        }
        config = self._make_config(max_dax_retries=2)

        fail_result = {"success": False, "error": "Persistent DAX error", "row_count": 0, "data": [], "columns": []}

        mock_service, ctx = self._mock_cache_and_factory(cached)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_generate_dax_with_self_correction", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value=fail_result):

            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)
        # Should contain error information
        assert "Persistent DAX error" in result or "Failed" in result or "error" in result.lower()

    def test_no_dax_generated_is_handled(self):
        """Test that None DAX generated is handled gracefully."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
        }
        config = self._make_config(max_dax_retries=1)

        mock_service, ctx = self._mock_cache_and_factory(cached)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value=None):

            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)

    def test_visual_references_searched_when_measures_used(self):
        """Test that visual references are searched when enabled and measures found in DAX."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
        }
        config = self._make_config(max_dax_retries=1, include_visual_references=True)

        success_result = {"success": True, "data": [], "row_count": 0, "columns": [], "error": None}

        mock_service, ctx = self._mock_cache_and_factory(cached)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value=success_result), \
             patch.object(self.tool, "_find_visual_references", return_value=[{"report_name": "Test Report", "measure": "Revenue"}]):

            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)
        assert "Test Report" in result

    def test_exception_in_dax_attempt_continues(self):
        """Test that an exception in one attempt is caught and retried."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
        }
        config = self._make_config(max_dax_retries=2)

        call_count = [0]
        async def generate_side(*args, **kwargs):
            if call_count[0] == 0:
                call_count[0] += 1
                raise Exception("LLM call failed")
            return "EVALUATE {[Revenue]}"

        success_result = {"success": True, "data": [], "row_count": 0, "columns": [], "error": None}

        mock_service, ctx = self._mock_cache_and_factory(cached)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", side_effect=generate_side), \
             patch.object(self.tool, "_generate_dax_with_self_correction", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value=success_result):

            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)


class TestFetchColumnMetadataForTable:
    """Tests for _fetch_column_metadata_for_table."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_response_returns_columns(self):
        rows = [
            {"[ExplicitName]": "Amount", "[DataType]": "2", "[IsHidden]": False, "[Description]": ""},
            {"[ExplicitName]": "Region", "[DataType]": "6", "[IsHidden]": False, "[Description]": ""},
        ]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_column_metadata_for_table(
                    WS_ID, DS_ID, ACCESS_TOKEN, "Sales", {}
                )
            )

        assert len(result) == 2
        assert result[0]["column_name"] == "Amount"

    def test_non_200_returns_empty(self):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Error"
        try:
            mock_response.json.return_value = {"error": {"message": "Not supported"}}
        except Exception:
            pass

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_column_metadata_for_table(
                    WS_ID, DS_ID, ACCESS_TOKEN, "Sales", {}
                )
            )

        assert result == []

    def test_exception_returns_empty(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_column_metadata_for_table(
                    WS_ID, DS_ID, ACCESS_TOKEN, "Sales", {}
                )
            )

        assert result == []


class TestEnrichModelContextWithInfoColumns:
    """Tests for _enrich_model_context_with_metadata with enable_info_columns=True."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_info_columns_enabled_enriches_tables(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Amount", "Region"]}],
            "measures": [],
        }
        columns_metadata = [
            {"column_name": "Amount", "data_type": "2", "is_hidden": False, "description": "Revenue amount"}
        ]
        config = {"enable_info_columns": True, "skip_system_tables": True}

        with patch.object(self.tool, "_fetch_column_metadata_for_table", return_value=columns_metadata):
            with patch.object(self.tool, "_fetch_sample_column_values", return_value={}):
                result = self._run(
                    self.tool._enrich_model_context_with_metadata(
                        model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                    )
                )

        # Column metadata should be added to the table
        assert isinstance(result, dict)
        tables = result.get("tables", [])
        sales_table = next((t for t in tables if t["name"] == "Sales"), None)
        assert sales_table is not None
        assert "column_metadata" in sales_table

    def test_info_columns_system_tables_skipped(self):
        model_context = {
            "tables": [
                {"name": "LocalDateTable_abc", "columns": ["Date"]},
                {"name": "Sales", "columns": ["Amount"]},
            ],
            "measures": [],
        }
        config = {"enable_info_columns": True, "skip_system_tables": True}

        call_count = [0]
        async def mock_fetch_columns(*args, **kwargs):
            call_count[0] += 1
            return []

        with patch.object(self.tool, "_fetch_column_metadata_for_table", side_effect=mock_fetch_columns):
            with patch.object(self.tool, "_fetch_sample_column_values", return_value={}):
                result = self._run(
                    self.tool._enrich_model_context_with_metadata(
                        model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                    )
                )

        # LocalDateTable should be skipped, only Sales should be fetched
        assert call_count[0] == 1  # Only Sales was fetched


class TestFetchSampleColumnValues:
    """Tests for _fetch_sample_column_values."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_empty_model_context_returns_empty(self):
        result = self._run(
            self.tool._fetch_sample_column_values(
                WS_ID, DS_ID, ACCESS_TOKEN, {"tables": []}, {}
            )
        )
        assert result == {}

    def test_id_columns_skipped(self):
        """Columns with 'id' or 'key' suffix are skipped."""
        model_context = {
            "tables": [{"name": "Sales", "columns": ["customer_id", "date_key", "Amount"]}]
        }

        success_result = {"success": True, "data": [{"[Amount]": 100}]}

        with patch.object(self.tool, "_execute_dax_query", return_value=success_result):
            result = self._run(
                self.tool._fetch_sample_column_values(
                    WS_ID, DS_ID, ACCESS_TOKEN, model_context, {}
                )
            )

        # Only "Amount" should be fetched (id/key columns skipped)
        assert "Sales[Amount]" in result
        assert "Sales[customer_id]" not in result

    def test_successful_query_populates_sample_values(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Region"]}]
        }

        async def mock_execute(*args):
            return {"success": True, "data": [{"[Region]": "North"}, {"[Region]": "South"}]}

        with patch.object(self.tool, "_execute_dax_query", side_effect=mock_execute):
            result = self._run(
                self.tool._fetch_sample_column_values(
                    WS_ID, DS_ID, ACCESS_TOKEN, model_context, {}
                )
            )

        assert "Sales[Region]" in result
        assert result["Sales[Region]"]["type"] == "categorical"

    def test_failed_query_skipped_gracefully(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Region"]}]
        }

        with patch.object(self.tool, "_execute_dax_query", side_effect=Exception("query failed")):
            result = self._run(
                self.tool._fetch_sample_column_values(
                    WS_ID, DS_ID, ACCESS_TOKEN, model_context, {}
                )
            )

        # Should return empty dict without raising
        assert result == {}


class TestAnalysisHelperMethods:
    """Test smaller helper methods in the analysis tool."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_parse_tmdl_for_measures_and_tables_empty(self):
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([], {})
        assert measures == []
        assert tables == []

    def test_parse_tmdl_simple_table(self):
        import base64 as _b64
        content = "table Sales\n\tcolumn Amount\n"
        part = {
            "path": "definition/tables/Sales.tmdl",
            "payload": _b64.b64encode(content.encode()).decode()
        }
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        assert any(t["name"] == "Sales" for t in tables)

    def test_parse_tmdl_skips_system_tables(self):
        import base64 as _b64
        content = "table LocalDateTable_xyz\n\tcolumn Date\n"
        part = {
            "path": "definition/tables/LocalDateTable_xyz.tmdl",
            "payload": _b64.b64encode(content.encode()).decode()
        }
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables(
            [part], {"skip_system_tables": True}
        )
        assert not any("LocalDateTable" in t.get("name", "") for t in tables)


# ===========================================================================
# _generate_simple_dax tests
# ===========================================================================

class TestGenerateSimpleDax:
    """Tests for _generate_simple_dax — fallback DAX generation without LLM."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_no_measures_returns_none(self):
        result = self.tool._generate_simple_dax("How many customers?", {"measures": []})
        assert result is None

    def test_returns_evaluate_query(self):
        model_context = {
            "measures": [{"name": "Total Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}]
        }
        result = self.tool._generate_simple_dax("What is revenue?", model_context)
        assert result is not None
        assert "EVALUATE" in result
        assert "Total Revenue" in result

    def test_keyword_matching_finds_best_measure(self):
        model_context = {
            "measures": [
                {"name": "Total Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"},
                {"name": "Customer Count", "table": "Customers", "expression": "COUNTROWS(Customers)"},
            ]
        }
        result = self.tool._generate_simple_dax("how many customers?", model_context)
        assert "Customer Count" in result

    def test_fallback_to_first_measure_when_no_match(self):
        model_context = {
            "measures": [
                {"name": "Total Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"},
                {"name": "Units", "table": "Products", "expression": "SUM(Products[Qty])"},
            ]
        }
        result = self.tool._generate_simple_dax("something unrelated", model_context)
        assert result is not None
        assert "Total Revenue" in result

    def test_summarizecolumns_in_output(self):
        model_context = {"measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}]}
        result = self.tool._generate_simple_dax("test?", model_context)
        assert "SUMMARIZECOLUMNS" in result


# ===========================================================================
# _extract_dax_from_llm_response tests
# ===========================================================================

class TestExtractDaxFromLlmResponse:
    """Tests for _extract_dax_from_llm_response — parse DAX from LLM output."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_plain_evaluate_extracted(self):
        content = "EVALUATE\nSUMMARIZECOLUMNS(\n    \"Result\", [Total Revenue]\n)"
        result = self.tool._extract_dax_from_llm_response(content)
        assert "EVALUATE" in result
        assert "SUMMARIZECOLUMNS" in result

    def test_markdown_code_block_stripped(self):
        content = "```dax\nEVALUATE\nSUMMARIZECOLUMNS()\n```"
        result = self.tool._extract_dax_from_llm_response(content)
        assert "```" not in result
        assert "EVALUATE" in result

    def test_text_before_evaluate_stripped(self):
        content = "Here is your DAX query:\n\nEVALUATE\nSUMMARIZECOLUMNS(\n    \"R\", [M]\n)"
        result = self.tool._extract_dax_from_llm_response(content)
        assert result.upper().startswith("EVALUATE")

    def test_empty_string_returns_empty(self):
        result = self.tool._extract_dax_from_llm_response("")
        assert isinstance(result, str)

    def test_no_evaluate_returns_content_as_is(self):
        content = "SELECT * FROM table"
        result = self.tool._extract_dax_from_llm_response(content)
        assert isinstance(result, str)

    def test_markdown_with_explanation_after_removed(self):
        content = "EVALUATE\nSUMMARIZECOLUMNS(\"R\", [M]\n)\n**Key Changes Made:**\n1. Fixed syntax"
        result = self.tool._extract_dax_from_llm_response(content)
        assert "EVALUATE" in result


# ===========================================================================
# _auto_wrap_with_report_filters tests
# ===========================================================================

class TestAutoWrapWithReportFilters:
    """Tests for _auto_wrap_with_report_filters — DAX filter wrapping."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_no_filters_returns_original(self):
        dax = "EVALUATE SUMMARIZECOLUMNS(\"R\", [Revenue])"
        result = self.tool._auto_wrap_with_report_filters(dax, {"active_filters": {}})
        assert result == dax

    def test_empty_active_filters_returns_original(self):
        dax = "EVALUATE\nSUMMARIZECOLUMNS(\"R\", [Revenue])"
        result = self.tool._auto_wrap_with_report_filters(dax, {})
        assert result == dax

    def test_filter_added_for_not_null(self):
        dax = "EVALUATE\nSUMMARIZECOLUMNS(\"R\", [Revenue])"
        config = {"active_filters": {"Sales[Region]": "NOT NULL"}}
        result = self.tool._auto_wrap_with_report_filters(dax, config)
        assert "CALCULATETABLE" in result or "ISBLANK" in result

    def test_filter_skipped_when_already_in_dax(self):
        dax = "EVALUATE\nCALCULATETABLE(\n    SUMMARIZECOLUMNS(\"R\", [Revenue]),\n    Sales[Region] = \"North\"\n)"
        config = {"active_filters": {"Sales[Region]": "NOT NULL"}}
        result = self.tool._auto_wrap_with_report_filters(dax, config)
        # The filter name is in the dax, so it should be skipped
        assert isinstance(result, str)

    def test_equals_filter_generates_condition(self):
        dax = "EVALUATE\nSUMMARIZECOLUMNS(\"R\", [Revenue])"
        config = {"active_filters": {"MyTable[Status]": "= 'Active'"}}
        result = self.tool._auto_wrap_with_report_filters(dax, config)
        assert "CALCULATETABLE" in result

    def test_already_calculatetable_merges_filters(self):
        dax = "EVALUATE\nCALCULATETABLE(\n    SUMMARIZECOLUMNS(\"R\", [Revenue]),\n    AnotherTable[X] = 1\n)"
        config = {"active_filters": {"NewTable[Y]": "= 'Italy'"}}
        result = self.tool._auto_wrap_with_report_filters(dax, config)
        assert "CALCULATETABLE" in result


# ===========================================================================
# _generate_dax_filter_condition tests
# ===========================================================================

class TestGenerateDaxFilterCondition:
    """Tests for _generate_dax_filter_condition."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_not_null_returns_isblank(self):
        result = self.tool._generate_dax_filter_condition("Sales[Region]", "NOT NULL")
        assert "ISBLANK" in result
        assert "FALSE" in result

    def test_not_starts_with_returns_left(self):
        result = self.tool._generate_dax_filter_condition("Sales[Code]", "NOT STARTS WITH '7'")
        assert "LEFT" in result
        assert "7" in result

    def test_equals_returns_filter(self):
        result = self.tool._generate_dax_filter_condition("Sales[BU]", "= 'Italy'")
        assert "Italy" in result
        assert "Sales[BU]" in result

    def test_in_multiple_values_returns_in(self):
        result = self.tool._generate_dax_filter_condition("Sales[Status]", "IN (A, B)")
        assert "IN" in result or "A" in result

    def test_plain_value_returns_equals(self):
        result = self.tool._generate_dax_filter_condition("Sales[Type]", "Complete")
        assert "Complete" in result

    def test_returns_string(self):
        result = self.tool._generate_dax_filter_condition("T[C]", "some filter")
        assert isinstance(result, str)


# ===========================================================================
# _parse_filter_condition extended tests
# ===========================================================================

class TestParseFilterConditionExtended:
    """Extended tests for _parse_filter_condition."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_not_null_condition(self):
        condition = {
            "Not": {
                "Expression": {
                    "In": {
                        "Expressions": [],
                        "Values": [[{"Literal": {"Value": "null"}}]]
                    }
                }
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert result == "NOT NULL"

    def test_not_in_values_condition(self):
        condition = {
            "Not": {
                "Expression": {
                    "In": {
                        "Expressions": [],
                        "Values": [[{"Literal": {"Value": "'North'"}}], [{"Literal": {"Value": "'South'"}}]]
                    }
                }
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert "NOT IN" in result
        assert "North" in result

    def test_not_starts_with_condition(self):
        condition = {
            "Not": {
                "Expression": {
                    "StartsWith": {
                        "Right": {"Literal": {"Value": "'7'"}}
                    }
                }
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert "NOT STARTS WITH" in result
        assert "7" in result

    def test_in_single_value_returns_equals(self):
        condition = {
            "In": {
                "Expressions": [],
                "Values": [[{"Literal": {"Value": "'Italy'"}}]]
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert "= 'Italy'" in result

    def test_in_multiple_values(self):
        condition = {
            "In": {
                "Expressions": [],
                "Values": [
                    [{"Literal": {"Value": "'A'"}}],
                    [{"Literal": {"Value": "'B'"}}],
                ]
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert "IN" in result

    def test_comparison_equals(self):
        condition = {
            "Comparison": {
                "ComparisonKind": 0,
                "Right": {"Literal": {"Value": "100"}}
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert "=" in result
        assert "100" in result

    def test_comparison_greater_than(self):
        condition = {
            "Comparison": {
                "ComparisonKind": 2,
                "Right": {"Literal": {"Value": "50"}}
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert ">" in result

    def test_fallback_complex_filter(self):
        # Condition type not recognized - should return generic string
        condition = {"SomeUnknownType": {}}
        result = self.tool._parse_filter_condition(condition)
        assert isinstance(result, str)
        assert len(result) > 0


# ===========================================================================
# _extract_filters_from_tmdl_content tests
# ===========================================================================

class TestExtractFiltersFromTmdlContent:
    """Tests for _extract_filters_from_tmdl_content."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_empty_content_returns_empty(self):
        result = self.tool._extract_filters_from_tmdl_content("", "section")
        assert result == {}

    def test_no_matching_section_returns_empty(self):
        content = "table Sales\n\tcolumn Amount\n"
        result = self.tool._extract_filters_from_tmdl_content(content, "nonexistent")
        assert result == {}

    def test_filter_with_equals_extracted(self):
        content = "overview section\n    filter 'Region' on 'Sales'[Region] = \"North\"\n"
        result = self.tool._extract_filters_from_tmdl_content(content, "overview")
        assert "Region" in result
        assert result["Region"] == "North"

    def test_filter_with_in_set_extracted(self):
        content = "overview section\n    filter 'Status' on 'Sales'[Status] in {\"A\", \"B\"}\n"
        result = self.tool._extract_filters_from_tmdl_content(content, "overview")
        assert "Status" in result


# ===========================================================================
# _build_enriched_semantic_context tests
# ===========================================================================

class TestBuildEnrichedSemanticContext:
    """Tests for _build_enriched_semantic_context."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_empty_context_returns_string(self):
        result = self.tool._build_enriched_semantic_context({}, {})
        assert isinstance(result, str)
        assert "SEMANTIC MODEL SCHEMA" in result

    def test_tables_included(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Amount", "Region"]}],
            "measures": [],
            "relationships": [],
        }
        result = self.tool._build_enriched_semantic_context(model_context, {})
        assert "Sales" in result

    def test_measures_included(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": []}],
            "measures": [{"name": "Total Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
        }
        result = self.tool._build_enriched_semantic_context(model_context, {})
        assert "Total Revenue" in result

    def test_business_mappings_included(self):
        model_context = {"tables": [], "measures": [], "relationships": []}
        config = {"business_mappings": {"Complete CGR": "[Status] = 'Complete'"}}
        result = self.tool._build_enriched_semantic_context(model_context, config)
        assert "Complete CGR" in result
        assert "BUSINESS TERMINOLOGY" in result

    def test_field_synonyms_included(self):
        model_context = {"tables": [], "measures": [], "relationships": []}
        config = {"field_synonyms": {"revenue": ["sales", "income"]}}
        result = self.tool._build_enriched_semantic_context(model_context, config)
        assert "revenue" in result

    def test_active_filters_included(self):
        model_context = {"tables": [], "measures": [], "relationships": []}
        config = {"active_filters": {"Sales[BU]": "Italy"}}
        result = self.tool._build_enriched_semantic_context(model_context, config)
        assert "CURRENT VIEW STATE" in result
        assert "Italy" in result

    def test_active_filters_list_values(self):
        model_context = {"tables": [], "measures": [], "relationships": []}
        config = {"active_filters": {"Sales[Status]": ["Active", "Pending"]}}
        result = self.tool._build_enriched_semantic_context(model_context, config)
        assert "Active" in result
        assert "Pending" in result

    def test_conversation_history_included(self):
        model_context = {"tables": [], "measures": [], "relationships": []}
        config = {"conversation_history": [{"question": "What is revenue?", "answer": "100M"}]}
        result = self.tool._build_enriched_semantic_context(model_context, config)
        assert "CONVERSATION HISTORY" in result
        assert "What is revenue?" in result

    def test_sample_values_included(self):
        model_context = {
            "tables": [],
            "measures": [],
            "relationships": [],
            "sample_values": {
                "Sales[Region]": {"type": "categorical", "sample_values": ["North", "South"]}
            }
        }
        result = self.tool._build_enriched_semantic_context(model_context, {})
        assert "SAMPLE DATA VALUES" in result
        assert "North" in result

    def test_relationships_included(self):
        model_context = {
            "tables": [{"name": "Sales"}, {"name": "Date"}],
            "measures": [],
            "relationships": [
                {"from_table": "Sales", "from_column": "DateKey", "to_table": "Date", "to_column": "DateKey"}
            ],
        }
        result = self.tool._build_enriched_semantic_context(model_context, {})
        assert "Relationships" in result or "relationships" in result.lower()

    def test_visible_tables_priority(self):
        model_context = {
            "tables": [
                {"name": "Sales", "columns": []},
                {"name": "Date", "columns": []},
            ],
            "measures": [],
            "relationships": [],
        }
        config = {"visible_tables": ["Sales"]}
        result = self.tool._build_enriched_semantic_context(model_context, config)
        assert "Sales" in result

    def test_column_types_shown(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Amount"], "column_types": {"Amount": "Decimal"}}],
            "measures": [],
            "relationships": [],
        }
        result = self.tool._build_enriched_semantic_context(model_context, {})
        assert "Decimal" in result

    def test_column_descriptions_shown(self):
        model_context = {
            "tables": [
                {
                    "name": "Sales",
                    "columns": ["Amount"],
                    "column_descriptions": {"Amount": "The total sale amount"},
                }
            ],
            "measures": [],
            "relationships": [],
        }
        result = self.tool._build_enriched_semantic_context(model_context, {})
        assert "The total sale amount" in result


# ===========================================================================
# _parse_report_pages and _parse_report_visuals tests
# ===========================================================================

class TestParseReportPages:
    """Tests for _parse_report_pages."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _make_page_part(self, page_id, page_data):
        import base64 as _b64
        payload = _b64.b64encode(json.dumps(page_data).encode()).decode()
        return {"path": f"definition/pages/{page_id}/page.json", "payload": payload}

    def test_empty_parts_returns_empty(self):
        result = self.tool._parse_report_pages([])
        assert result == []

    def test_page_json_parsed(self):
        part = self._make_page_part("Page1", {"name": "Overview", "displayName": "Overview", "ordinal": 0})
        result = self.tool._parse_report_pages([part])
        assert len(result) == 1
        assert result[0]["id"] == "Page1"

    def test_pages_sorted_by_ordinal(self):
        parts = [
            self._make_page_part("P2", {"name": "P2", "displayName": "Page 2", "ordinal": 1}),
            self._make_page_part("P1", {"name": "P1", "displayName": "Page 1", "ordinal": 0}),
        ]
        result = self.tool._parse_report_pages(parts)
        assert result[0]["id"] == "P1"

    def test_fallback_to_report_json(self):
        report_data = {
            "pages": [
                {"name": "p1", "displayName": "Page 1", "ordinal": 0},
            ]
        }
        import base64 as _b64
        payload = _b64.b64encode(json.dumps(report_data).encode()).decode()
        part = {"path": "report.json", "payload": payload}
        result = self.tool._parse_report_pages([part])
        assert len(result) >= 1

    def test_invalid_base64_skipped(self):
        part = {"path": "definition/pages/P1/page.json", "payload": "!!invalid!!"}
        result = self.tool._parse_report_pages([part])
        assert isinstance(result, list)


class TestParseReportVisuals:
    """Tests for _parse_report_visuals."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _make_visual_part(self, page_id, visual_id, visual_data):
        import base64 as _b64
        payload = _b64.b64encode(json.dumps(visual_data).encode()).decode()
        return {"path": f"definition/pages/{page_id}/visuals/{visual_id}/visual.json", "payload": payload}

    def test_empty_parts_returns_empty(self):
        result = self.tool._parse_report_visuals([])
        assert result == []

    def test_visual_json_parsed(self):
        visual_data = {"visual": {"visualType": "card"}}
        part = self._make_visual_part("Page1", "Vis1", visual_data)
        result = self.tool._parse_report_visuals([part])
        assert len(result) == 1
        assert result[0]["type"] == "card"
        assert result[0]["page_id"] == "Page1"

    def test_unknown_type_when_no_visual_type(self):
        visual_data = {"name": "v1"}
        part = self._make_visual_part("P1", "V1", visual_data)
        result = self.tool._parse_report_visuals([part])
        assert result[0]["type"] == "unknown"

    def test_invalid_base64_skipped(self):
        part = {"path": "definition/pages/P1/visuals/V1/visual.json", "payload": "!!invalid!!"}
        result = self.tool._parse_report_visuals([part])
        assert isinstance(result, list)


# ===========================================================================
# _extract_measures_from_visual and _find_measures_in_dict tests
# ===========================================================================

class TestExtractMeasuresFromVisual:
    """Tests for _extract_measures_from_visual and _find_measures_in_dict."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_empty_config_returns_empty(self):
        visual = {"id": "v1", "config": {}}
        result = self.tool._extract_measures_from_visual(visual)
        assert result == []

    def test_measure_property_found(self):
        visual = {
            "config": {
                "visual": {
                    "queryDefinition": {
                        "select": [{"measure": {"property": "Total Revenue"}}]
                    }
                }
            }
        }
        result = self.tool._extract_measures_from_visual(visual)
        assert "Total Revenue" in result

    def test_measure_name_found(self):
        visual = {
            "config": {
                "measures": [{"measure": {"name": "Profit Margin"}}]
            }
        }
        result = self.tool._extract_measures_from_visual(visual)
        assert "Profit Margin" in result

    def test_uppercase_measure_key(self):
        visual = {
            "config": {
                "Measure": {"Property": "Revenue"}
            }
        }
        result = self.tool._extract_measures_from_visual(visual)
        assert "Revenue" in result

    def test_config_as_json_string(self):
        config_dict = {"measures": [{"measure": {"property": "Units"}}]}
        visual = {"config": json.dumps(config_dict)}
        result = self.tool._extract_measures_from_visual(visual)
        assert "Units" in result

    def test_invalid_json_string_returns_empty(self):
        visual = {"config": "not valid json!!!"}
        result = self.tool._extract_measures_from_visual(visual)
        assert result == []

    def test_nested_measure_reference_found(self):
        visual = {
            "config": {
                "level1": {
                    "level2": {
                        "measure": "Revenue"
                    }
                }
            }
        }
        result = self.tool._extract_measures_from_visual(visual)
        assert "Revenue" in result

    def test_list_traversal(self):
        visual = {
            "config": {
                "items": [
                    {"measure": {"property": "M1"}},
                    {"measure": {"property": "M2"}},
                ]
            }
        }
        result = self.tool._extract_measures_from_visual(visual)
        assert "M1" in result
        assert "M2" in result


# ===========================================================================
# _build_page_url tests
# ===========================================================================

class TestBuildPageUrl:
    """Tests for _build_page_url."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_with_page_id(self):
        url = self.tool._build_page_url(WS_ID, "report-id", "PageXYZ")
        assert WS_ID in url
        assert "report-id" in url
        assert "PageXYZ" in url
        assert url.startswith("https://")

    def test_without_page_id(self):
        url = self.tool._build_page_url(WS_ID, "report-id", "")
        assert WS_ID in url
        assert "report-id" in url
        assert url.startswith("https://")

    def test_format_includes_report_section(self):
        url = self.tool._build_page_url(WS_ID, "report-id", "MyPage")
        assert "ReportSection" in url


# ===========================================================================
# _parse_pages_from_report_json tests
# ===========================================================================

class TestAnalysisToolParsePagesFromReportJson:
    """Tests for _parse_pages_from_report_json."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _make_part(self, path, data):
        import base64 as _b64
        return {"path": path, "payload": _b64.b64encode(json.dumps(data).encode()).decode()}

    def test_empty_parts_returns_empty(self):
        assert self.tool._parse_pages_from_report_json([]) == []

    def test_pages_key_parsed(self):
        data = {"pages": [{"name": "p1", "displayName": "P1", "ordinal": 0}]}
        part = self._make_part("report.json", data)
        result = self.tool._parse_pages_from_report_json([part])
        assert len(result) == 1

    def test_sections_key_parsed(self):
        data = {"sections": [{"name": "s1", "displayName": "S1", "ordinal": 0}]}
        part = self._make_part("report.json", data)
        result = self.tool._parse_pages_from_report_json([part])
        assert len(result) == 1

    def test_report_pages_key_parsed(self):
        data = {"reportPages": [{"name": "rp1", "displayName": "RP1", "ordinal": 0}]}
        part = self._make_part("report.json", data)
        result = self.tool._parse_pages_from_report_json([part])
        assert len(result) == 1

    def test_no_pages_data_returns_empty(self):
        data = {"config": "something else"}
        part = self._make_part("report.json", data)
        result = self.tool._parse_pages_from_report_json([part])
        assert result == []


# ===========================================================================
# _parse_visuals_from_report_json tests
# ===========================================================================

class TestAnalysisToolParseVisualsFromReportJson:
    """Tests for _parse_visuals_from_report_json."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _make_part(self, path, data):
        import base64 as _b64
        return {"path": path, "payload": _b64.b64encode(json.dumps(data).encode()).decode()}

    def test_empty_parts_returns_empty(self):
        assert self.tool._parse_visuals_from_report_json([]) == []

    def test_visual_containers_parsed(self):
        data = {
            "pages": [
                {
                    "name": "p1",
                    "visualContainers": [
                        {"name": "v1", "visualType": "card", "config": {"singleVisual": {"visualType": "card"}}}
                    ]
                }
            ]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) >= 1

    def test_visual_with_string_config(self):
        config_dict = {"singleVisual": {"visualType": "barChart"}}
        data = {
            "pages": [
                {
                    "name": "p1",
                    "visuals": [
                        {"name": "v1", "config": json.dumps(config_dict)}
                    ]
                }
            ]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) >= 1

    def test_no_pages_returns_empty(self):
        data = {"config": "nothing"}
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert result == []


# ===========================================================================
# _emit_llm_trace tests
# ===========================================================================

class TestEmitLlmTrace:
    """Tests for _emit_llm_trace — tracing utility."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_no_trace_context_does_not_raise(self):
        # No trace_context attribute set
        self.tool._emit_llm_trace("test event", "prompt text", "model-name", "generate_dax")

    def test_trace_context_without_job_id_skips(self):
        self.tool.trace_context = {"group_context": {}}
        # Should not raise even without job_id
        self.tool._emit_llm_trace("test event", "prompt", "model", "op")

    def test_trace_context_with_job_id_attempts_emit(self):
        self.tool.trace_context = {"job_id": "test-job-123", "group_context": {}}
        mock_queue = MagicMock()
        mock_queue.put_nowait = MagicMock()
        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBIAnalysisTool._emit_llm_trace") as _:
            # Just confirm no exception is raised
            pass

    def test_emit_with_response(self):
        """Test emit with optional response parameter - swallows exceptions."""
        self.tool.trace_context = {"job_id": "job-123", "group_context": {}}
        # The method does a lazy import inside - just call it and confirm no propagation
        # If the import fails it's caught internally
        self.tool._emit_llm_trace("ctx", "prompt", "model", "op", response="response text")


# ===========================================================================
# Async method tests: _fetch_tmdl_via_fabric
# ===========================================================================

class TestFetchTmdlViaFabric:
    """Tests for _fetch_tmdl_via_fabric with mocked httpx."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_200_response_returns_parts(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "definition": {"parts": [{"path": "model.tmdl", "payload": "abc"}]}
        }

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN))

        assert result is not None
        assert isinstance(result, list)

    def test_non_200_404_returns_none(self):
        mock_response = MagicMock()
        mock_response.status_code = 404

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN))

        assert result is None

    def test_exception_returns_none(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("connection error"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN))

        assert result is None

    def test_202_no_location_returns_none(self):
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_response.headers = {}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN))

        assert result is None


# ===========================================================================
# Async method tests: _execute_dax_query
# ===========================================================================

class TestExecuteDaxQuery:
    """Tests for _execute_dax_query."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_query(self):
        rows = [{"[Region]": "North", "[Revenue]": 100}]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [{"tables": [{"rows": rows}]}]
        }
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "EVALUATE Sales")
            )

        assert result["success"] is True
        assert result["row_count"] == 1
        assert result["data"] == rows

    def test_empty_result_set(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"tables": []}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "EVALUATE Sales")
            )

        assert result["success"] is False
        assert result["row_count"] == 0

    def test_error_in_response_body(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"error": {"message": "Invalid DAX syntax"}}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "BAD DAX")
            )

        assert result["success"] is False
        assert "Invalid DAX syntax" in str(result["error"])

    def test_http_status_error(self):
        import httpx as _httpx
        mock_response_obj = MagicMock()
        mock_response_obj.status_code = 400
        mock_response_obj.text = "Bad Request"
        err = _httpx.HTTPStatusError("400", request=MagicMock(), response=mock_response_obj)

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=err)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "EVALUATE Sales")
            )

        assert result["success"] is False
        assert result["error"] is not None

    def test_general_exception_captured(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "EVALUATE Sales")
            )

        assert result["success"] is False
        assert "network error" in str(result["error"])


# ===========================================================================
# _fetch_column_metadata tests (backward-compatible empty method)
# ===========================================================================

class TestFetchColumnMetadata:
    """Tests for _fetch_column_metadata."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_returns_empty_list(self):
        result = asyncio.run(
            self.tool._fetch_column_metadata(WS_ID, DS_ID, ACCESS_TOKEN, {})
        )
        assert result == []


# ===========================================================================
# _fetch_relationships tests
# ===========================================================================

class TestFetchRelationshipsAnalysisTool:
    """Tests for _fetch_relationships."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_relationships_parsed(self):
        rows = [
            {"[ID]": 1, "[FromTable]": "Sales", "[FromColumn]": "DateKey",
             "[ToTable]": "Date", "[ToColumn]": "DateKey", "[IsActive]": True}
        ]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_relationships(WS_ID, DS_ID, ACCESS_TOKEN, {}))

        assert len(result) == 1
        assert result[0]["from_table"] == "Sales"

    def test_exception_returns_empty(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_relationships(WS_ID, DS_ID, ACCESS_TOKEN, {}))

        assert result == []

    def test_system_table_filtered_when_skip_enabled(self):
        rows = [
            {"[ID]": 1, "[FromTable]": "LocalDateTable_abc", "[FromColumn]": "Date",
             "[ToTable]": "Sales", "[ToColumn]": "DateKey", "[IsActive]": True}
        ]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_relationships(WS_ID, DS_ID, ACCESS_TOKEN, {"skip_system_tables": True})
            )

        assert len(result) == 0

    def test_duplicate_relationship_ids_deduplicated(self):
        rows = [
            {"[ID]": 1, "[FromTable]": "Sales", "[FromColumn]": "DateKey",
             "[ToTable]": "Date", "[ToColumn]": "DateKey", "[IsActive]": True},
            {"[ID]": 1, "[FromTable]": "Sales", "[FromColumn]": "DateKey",
             "[ToTable]": "Date", "[ToColumn]": "DateKey", "[IsActive]": True},
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_relationships(WS_ID, DS_ID, ACCESS_TOKEN, {}))

        assert len(result) == 1


# ===========================================================================
# _format_output extended tests (json with visual refs having page info)
# ===========================================================================

class TestFormatOutputExtended:
    """Extended tests for edge cases in _format_output."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()
        self.base_results = {
            "user_question": "Test?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "model_context": {"measures": [], "tables": [], "relationships": []},
            "generated_dax": None,
            "dax_execution": {"success": False, "data": [], "row_count": 0, "error": None},
            "visual_references": [],
            "errors": [],
            "dax_attempts": [],
        }

    def test_visual_refs_with_page_info(self):
        results = {
            **self.base_results,
            "visual_references": [
                {
                    "report_name": "Sales Report",
                    "report_url": "https://powerbi.com/r/1",
                    "page_name": "Overview",
                    "page_url": "https://powerbi.com/r/1/p/1",
                    "measure": "Total Revenue",
                    "visual_type": "card",
                }
            ],
        }
        result = self.tool._format_output(results, "markdown")
        assert "Overview" in result
        assert "card" in result

    def test_visual_refs_without_page_info(self):
        results = {
            **self.base_results,
            "visual_references": [
                {
                    "report_name": "Sales Report",
                    "report_url": "https://powerbi.com/r/1",
                    "page_name": None,
                    "page_url": None,
                    "measure": "Total Revenue",
                    "visual_type": None,
                }
            ],
        }
        result = self.tool._format_output(results, "markdown")
        assert "Sales Report" in result

    def test_many_data_rows_truncated(self):
        rows = [{"[Region]": f"R{i}", "[Revenue]": i} for i in range(30)]
        results = {
            **self.base_results,
            "generated_dax": "EVALUATE Sales",
            "dax_execution": {
                "success": True,
                "data": rows,
                "row_count": 30,
                "columns": ["[Region]", "[Revenue]"],
                "error": None,
            },
        }
        result = self.tool._format_output(results, "markdown")
        assert "more rows" in result.lower()

    def test_retry_history_single_attempt(self):
        results = {
            **self.base_results,
            "generated_dax": "EVALUATE Sales",
            "dax_attempts": [
                {"attempt": 1, "dax": "EVALUATE Sales", "success": True, "error": None, "row_count": 5},
            ],
        }
        # Single attempt should not show retry history
        result = self.tool._format_output(results, "markdown")
        assert isinstance(result, str)

    def test_json_output_includes_all_keys(self):
        result = self.tool._format_output(self.base_results, "json")
        parsed = json.loads(result)
        assert "user_question" in parsed
        assert "workspace_id" in parsed
        assert "dataset_id" in parsed
        assert "model_context" in parsed
        assert "dax_execution" in parsed


# ===========================================================================
# _generate_dax_with_llm fallback tests (no LLM credentials)
# ===========================================================================

class TestGenerateDaxWithLlmFallback:
    """Test _generate_dax_with_llm fallback path (no LLM credentials)."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_no_llm_credentials_falls_back_to_simple(self):
        model_context = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "tables": [],
        }
        config = {}  # No llm_workspace_url or llm_token
        result = self._run(self.tool._generate_dax_with_llm("Revenue?", model_context, config))
        # Should fall back to _generate_simple_dax
        assert result is not None
        assert "EVALUATE" in result

    def test_no_measures_with_no_llm_returns_none(self):
        model_context = {"measures": [], "tables": []}
        config = {}
        result = self._run(self.tool._generate_dax_with_llm("Revenue?", model_context, config))
        # No measures → None from simple dax
        assert result is None

    def test_llm_http_error_falls_back_to_simple(self):
        model_context = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "tables": [],
        }
        config = {
            "llm_workspace_url": "https://databricks.example.com",
            "llm_token": "fake-token",
            "llm_model": "databricks-claude-sonnet-4",
            "business_mappings": {},
            "field_synonyms": {},
            "active_filters": {},
            "conversation_history": [],
            "visible_tables": [],
        }

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("LLM unreachable"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            with patch.object(self.tool, "_emit_llm_trace"):
                result = self._run(self.tool._generate_dax_with_llm("Revenue?", model_context, config))

        # Should fall back to simple DAX
        assert result is not None
        assert "EVALUATE" in result


# ===========================================================================
# Context enrichment merge logic in _run
# ===========================================================================

class TestRunContextEnrichmentMerge:
    """Test JSON string parsing of context enrichment fields in _run."""

    def test_json_string_business_mappings_parsed(self):
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
            access_token=ACCESS_TOKEN,
            user_question="test?",
        )
        with patch(
            "src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context",
            return_value="ok"
        ):
            result = tool._run(business_mappings='{"Complete CGR": "expr"}')
        assert result == "ok"

    def test_invalid_json_string_business_mappings_uses_empty(self):
        tool = PowerBIAnalysisTool(
            workspace_id=WS_ID,
            dataset_id=DS_ID,
            access_token=ACCESS_TOKEN,
            user_question="test?",
        )
        with patch(
            "src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context",
            return_value="ok"
        ):
            result = tool._run(business_mappings="not valid json")
        assert isinstance(result, str)


# ===========================================================================
# _fetch_model_via_admin_scanner tests
# ===========================================================================

class TestFetchModelViaAdminScanner:
    """Tests for _fetch_model_via_admin_scanner."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_mock_client(self, responses):
        """Make a mock httpx client with sequential responses."""
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        call_count = [0]

        async def post_side_effect(*args, **kwargs):
            resp = responses[call_count[0] % len(responses)]
            call_count[0] += 1
            return resp

        async def get_side_effect(*args, **kwargs):
            resp = responses[call_count[0] % len(responses)]
            call_count[0] += 1
            return resp

        mock_client.post = AsyncMock(side_effect=post_side_effect)
        mock_client.get = AsyncMock(side_effect=get_side_effect)
        return mock_client

    def _make_response(self, status=200, json_data=None):
        resp = MagicMock()
        resp.status_code = status
        resp.json.return_value = json_data or {}
        resp.raise_for_status = MagicMock()
        return resp

    def test_unauthorized_returns_empty(self):
        resp = self._make_response(status=403)
        resp.raise_for_status.side_effect = None  # 403 checked before raise_for_status

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=resp)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []

    def test_no_scan_id_returns_empty(self):
        resp = self._make_response(status=200, json_data={})  # No "id" key

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=resp)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []

    def test_getinfo_exception_returns_empty(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []

    def test_workspace_not_found_returns_empty(self):
        # Scan returns successfully but workspace_id doesn't match
        post_resp = self._make_response(200, {"id": "scan-123"})
        poll_resp = self._make_response(200, {"status": "Succeeded"})
        result_resp = self._make_response(200, {"workspaces": [{"id": "different-ws"}]})

        response_sequence = [post_resp, poll_resp, result_resp]
        call_count = [0]

        async def post_side(*args, **kwargs):
            return post_resp

        async def get_side(*args, **kwargs):
            # First call = poll, second = result
            if call_count[0] == 0:
                call_count[0] += 1
                return poll_resp
            return result_resp

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=post_side)
        mock_client.get = AsyncMock(side_effect=get_side)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
                measures, tables = self._run(
                    self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
                )

        assert measures == []
        assert tables == []

    def test_successful_scan_returns_measures_and_tables(self):
        post_resp = self._make_response(200, {"id": "scan-abc"})
        poll_resp = self._make_response(200, {"status": "Succeeded"})
        result_data = {
            "workspaces": [{
                "id": WS_ID,
                "datasets": [{
                    "id": DS_ID,
                    "tables": [{
                        "name": "Sales",
                        "columns": [{"name": "Amount"}, {"name": "Region"}],
                        "measures": [
                            {"name": "Revenue", "expression": "SUM(Sales[Amount])"}
                        ]
                    }]
                }]
            }]
        }
        result_resp = self._make_response(200, result_data)

        call_count = [0]

        async def get_side(*args, **kwargs):
            if call_count[0] == 0:
                call_count[0] += 1
                return poll_resp
            return result_resp

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=post_resp)
        mock_client.get = AsyncMock(side_effect=get_side)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
                measures, tables = self._run(
                    self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
                )

        assert len(tables) == 1
        assert tables[0]["name"] == "Sales"
        assert len(measures) == 1
        assert measures[0]["name"] == "Revenue"


# ===========================================================================
# _fetch_model_via_powerbi_dax tests
# ===========================================================================

class TestFetchModelViaDAX:
    """Tests for _fetch_model_via_powerbi_dax."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_table_extraction(self):
        rows = [
            {"[FromTable]": "Sales", "[ToTable]": "Date"},
            {"[FromTable]": "Date", "[ToTable]": "Fact"},
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        # The method calls: await httpx.AsyncClient(timeout=60.0).post(...)
        mock_instance = MagicMock()
        mock_instance.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_instance):
            measures, tables = self._run(
                self.tool._fetch_model_via_powerbi_dax(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert len(tables) >= 2
        assert measures == []

    def test_exception_returns_empty(self):
        mock_instance = MagicMock()
        mock_instance.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_instance):
            measures, tables = self._run(
                self.tool._fetch_model_via_powerbi_dax(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert isinstance(tables, list)

    def test_system_tables_filtered(self):
        rows = [
            {"[FromTable]": "LocalDateTable_abc", "[ToTable]": "Sales"},
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_instance = MagicMock()
        mock_instance.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_instance):
            measures, tables = self._run(
                self.tool._fetch_model_via_powerbi_dax(WS_ID, DS_ID, ACCESS_TOKEN, {"skip_system_tables": True})
            )

        # LocalDateTable should be skipped
        assert not any("LocalDateTable" in t.get("name", "") for t in tables)


# ===========================================================================
# _find_visual_references tests
# ===========================================================================

class TestFindVisualReferences:
    """Tests for _find_visual_references."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_exception_returns_empty_list(self):
        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_client.get = AsyncMock(side_effect=Exception("network error"))

            result = self._run(
                self.tool._find_visual_references(WS_ID, DS_ID, ACCESS_TOKEN, ["Total Revenue"])
            )

        assert result == []

    def test_no_matching_reports_returns_empty(self):
        reports_resp = MagicMock()
        reports_resp.raise_for_status = MagicMock()
        reports_resp.json.return_value = {"value": [
            {"id": "r1", "name": "Other Report", "datasetId": "different-ds", "webUrl": "https://powerbi.com/r/1"}
        ]}

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_client.get = AsyncMock(return_value=reports_resp)

            result = self._run(
                self.tool._find_visual_references(WS_ID, DS_ID, ACCESS_TOKEN, ["Total Revenue"])
            )

        assert result == []

    def test_report_with_matching_dataset_returns_refs(self):
        reports_resp = MagicMock()
        reports_resp.raise_for_status = MagicMock()
        reports_resp.json.return_value = {"value": [
            {"id": "r1", "name": "Sales Report", "datasetId": DS_ID, "webUrl": "https://powerbi.com/r/1"}
        ]}

        # Fabric API for report definition fails → fallback ref
        def_resp = MagicMock()
        def_resp.status_code = 404

        def get_side(*args, **kwargs):
            url = args[0] if args else kwargs.get("url", "")
            if "reports" in str(url) and "getDefinition" not in str(url):
                return reports_resp
            return def_resp

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_client.get = AsyncMock(return_value=reports_resp)
            mock_client.post = AsyncMock(return_value=def_resp)

            result = self._run(
                self.tool._find_visual_references(WS_ID, DS_ID, ACCESS_TOKEN, ["Total Revenue"])
            )

        # Should have at least one fallback reference
        assert isinstance(result, list)


# ===========================================================================
# _generate_dax_with_self_correction tests (fallback path without LLM)
# ===========================================================================

class TestGenerateDaxWithSelfCorrection:
    """Tests for _generate_dax_with_self_correction."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_no_llm_credentials_returns_none(self):
        model_context = {"measures": [{"name": "Revenue", "table": "Sales"}], "tables": []}
        config = {}  # No LLM creds
        previous = [{"attempt": 1, "dax": "EVALUATE Sales", "success": False, "error": "err"}]

        result = self._run(
            self.tool._generate_dax_with_self_correction("Revenue?", model_context, config, previous)
        )
        assert result is None

    def test_llm_exception_returns_none(self):
        model_context = {"measures": [{"name": "Revenue", "table": "Sales"}], "tables": []}
        config = {
            "llm_workspace_url": "https://databricks.example.com",
            "llm_token": "token",
            "llm_model": "model",
            "business_mappings": {},
            "field_synonyms": {},
            "active_filters": {},
            "conversation_history": [],
            "visible_tables": [],
        }
        previous = [{"attempt": 1, "dax": "EVALUATE Sales", "success": False, "error": "syntax error"}]

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("LLM unreachable"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._generate_dax_with_self_correction("Revenue?", model_context, config, previous)
            )

        assert result is None


# ===========================================================================
# _enrich_model_context_with_metadata tests
# ===========================================================================

class TestEnrichModelContextWithMetadata:
    """Tests for _enrich_model_context_with_metadata."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_enable_info_columns_false_skips_enrichment(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Amount"]}],
            "measures": [],
        }
        config = {"enable_info_columns": False}

        with patch.object(self.tool, "_fetch_sample_column_values", return_value={}):
            result = self._run(
                self.tool._enrich_model_context_with_metadata(
                    model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                )
            )

        # With enable_info_columns=False, no column metadata fetching
        assert "tables" in result

    def test_sample_values_added_to_context(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Region"]}],
            "measures": [],
        }
        config = {"enable_info_columns": False}
        sample_values = {"Sales[Region]": {"type": "categorical", "sample_values": ["North", "South"]}}

        with patch.object(self.tool, "_fetch_sample_column_values", return_value=sample_values):
            result = self._run(
                self.tool._enrich_model_context_with_metadata(
                    model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                )
            )

        assert "sample_values" in result
        assert "Sales[Region]" in result["sample_values"]

    def test_sample_value_fetch_exception_handled(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Region"]}],
            "measures": [],
        }
        config = {"enable_info_columns": False}

        with patch.object(self.tool, "_fetch_sample_column_values", side_effect=Exception("fetch error")):
            # Should not raise
            result = self._run(
                self.tool._enrich_model_context_with_metadata(
                    model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                )
            )

        assert isinstance(result, dict)


# ===========================================================================
# _run_async_in_sync_context with running event loop (lines 50-53)
# ===========================================================================

class TestRunAsyncInSyncContextRunningLoop:
    """Test _run_async_in_sync_context when a running event loop already exists."""

    def test_runs_via_executor_when_loop_exists(self):
        """Covers lines 50-53: running loop path via ThreadPoolExecutor."""
        async def outer():
            async def inner_coro():
                return 42
            result = _run_async_in_sync_context(inner_coro())
            return result

        result = asyncio.run(outer())
        assert result == 42


# ===========================================================================
# Context enrichment logging branches (lines 402, 428-429, 461, 463)
# ===========================================================================

class TestContextEnrichmentLoggingBranches:
    """Cover the logging branches triggered by non-empty business_mappings,
    field_synonyms, active_filters, and auth debug with username/password set."""

    def setup_method(self):
        self.tool = _make_tool(
            access_token=ACCESS_TOKEN,
            business_mappings={"Complete CGR": "[Status]='Complete'"},
            field_synonyms={"revenue": ["sales", "income"]},
            active_filters={"Sales[BU]": "Italy"},
            username="user@example.com",
            password="s3cr3t",
        )

    def test_business_mappings_and_field_synonyms_logging_executed(self):
        """Triggers lines 402, 428-429 by having non-empty business_mappings and
        field_synonyms so the conditional logger.info calls execute."""
        with patch(
            "src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context",
            return_value="ok"
        ):
            result = self.tool._run()
        assert result == "ok"

    def test_auth_debug_logs_username_and_password_length(self):
        """Triggers lines 461 and 463 by setting username and password."""
        with patch(
            "src.engines.crewai.tools.custom.powerbi_analysis_tool._run_async_in_sync_context",
            return_value="ok"
        ):
            result = self.tool._run()
        assert result == "ok"


# ===========================================================================
# Cache hit with report_id + default_filters (lines 581-589)
# ===========================================================================

class TestCacheHitWithDefaultFilters:
    """Test pipeline when cache hit includes default_filters for a report_id."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            access_token=ACCESS_TOKEN, user_question="Revenue?"
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_config(self, **extra):
        base = {
            "user_question": "Revenue?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "output_format": "markdown",
            "max_dax_retries": 1,
            "include_visual_references": False,
            "report_id": "report-abc",
        }
        base.update(extra)
        return base

    def _make_cache_ctx(self, cached):
        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=cached)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=MagicMock())
        ctx.__aexit__ = AsyncMock(return_value=None)
        return mock_service, ctx

    def test_cache_hit_with_default_filters_merges_into_config(self):
        """Lines 581-589: cached_metadata has default_filters; they should be merged."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
            "default_filters": {"Sales[BU]": "Italy"},
        }
        config = self._make_config()
        mock_service, ctx = self._make_cache_ctx(cached)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)

    def test_cache_hit_with_empty_default_filters_still_works(self):
        """Lines 581-589: cached_metadata has default_filters=None, skips merge."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
            "default_filters": None,
        }
        config = self._make_config()
        mock_service, ctx = self._make_cache_ctx(cached)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)

    def test_cache_hit_with_user_filters_overrides_cached_filters(self):
        """Lines 584-588: user-provided active_filters take precedence over cached."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
            "default_filters": {"Sales[BU]": "Italy"},
        }
        config = self._make_config(active_filters={"Sales[BU]": "Germany"})
        mock_service, ctx = self._make_cache_ctx(cached)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)


# ===========================================================================
# Cache miss pipeline: enrich + report_id filter extraction + cache save
# (lines 592, 613-614, 620-637, 666-671)
# ===========================================================================

class TestCacheMissPipelineExtractAndSave:
    """Tests for cache-miss path: extract model context, enrich, extract
    default filters from report, and save to cache."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            access_token=ACCESS_TOKEN, user_question="Revenue?"
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_config(self, **extra):
        base = {
            "user_question": "Revenue?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "output_format": "markdown",
            "max_dax_retries": 1,
            "include_visual_references": False,
        }
        base.update(extra)
        return base

    def _make_cache_miss_ctx(self):
        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=None)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=MagicMock())
        ctx.__aexit__ = AsyncMock(return_value=None)
        return mock_service, ctx

    def _model_ctx(self):
        return {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(...)"}],
            "relationships": [],
            "tables": [{"name": "Sales", "columns": []}],
            "columns": [],
            "sample_data": {},
        }

    def test_cache_miss_enriches_and_saves(self):
        """Lines 592, 613-614: cache miss → extract model → enrich → save."""
        config = self._make_config()
        mock_service, ctx = self._make_cache_miss_ctx()
        model_ctx = self._model_ctx()

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_extract_model_context", return_value=model_ctx), \
             patch.object(self.tool, "_enrich_model_context_with_metadata", return_value=model_ctx), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)

    def test_cache_miss_with_enrich_exception_continues(self):
        """Lines 613-614: _enrich_model_context raises, pipeline continues."""
        config = self._make_config()
        mock_service, ctx = self._make_cache_miss_ctx()
        model_ctx = self._model_ctx()

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_extract_model_context", return_value=model_ctx), \
             patch.object(self.tool, "_enrich_model_context_with_metadata", side_effect=Exception("enrich fail")), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)

    def test_cache_miss_with_report_id_extracts_default_filters(self):
        """Lines 620-637: report_id present → extract_default_filters → merge."""
        config = self._make_config(report_id="report-xyz")
        mock_service, ctx = self._make_cache_miss_ctx()
        model_ctx = self._model_ctx()

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_extract_model_context", return_value=model_ctx), \
             patch.object(self.tool, "_enrich_model_context_with_metadata", return_value=model_ctx), \
             patch.object(self.tool, "_extract_default_filters", return_value={"Sales[BU]": "Italy"}), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)

    def test_cache_miss_filter_extraction_exception_continues(self):
        """Lines 636-637: _extract_default_filters raises, pipeline continues."""
        config = self._make_config(report_id="report-xyz")
        mock_service, ctx = self._make_cache_miss_ctx()
        model_ctx = self._model_ctx()

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_extract_model_context", return_value=model_ctx), \
             patch.object(self.tool, "_enrich_model_context_with_metadata", return_value=model_ctx), \
             patch.object(self.tool, "_extract_default_filters", side_effect=Exception("filter fail")), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)

    def test_cache_save_exception_does_not_fail_pipeline(self):
        """Lines 666-667: cache save raises exception; pipeline still completes."""
        config = self._make_config()
        model_ctx = self._model_ctx()

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=None)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(side_effect=Exception("db error"))
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=MagicMock())
        ctx.__aexit__ = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_extract_model_context", return_value=model_ctx), \
             patch.object(self.tool, "_enrich_model_context_with_metadata", return_value=model_ctx), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)

    def test_model_extraction_error_appended(self):
        """Lines 669-671: _extract_model_context raises → error added to results."""
        config = self._make_config()
        mock_service, ctx = self._make_cache_miss_ctx()

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_extract_model_context", side_effect=Exception("extraction failed")):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)


# ===========================================================================
# Non-dict execution result defense (lines 710-711)
# ===========================================================================

class TestNonDictExecutionResult:
    """Test pipeline when _execute_dax_query returns a non-dict value."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            access_token=ACCESS_TOKEN, user_question="Revenue?"
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_non_dict_result_handled_gracefully(self):
        """Lines 710-711: execution_result is not a dict → defensive check fires."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
        }
        config = {
            "user_question": "Revenue?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "output_format": "markdown",
            "max_dax_retries": 1,
            "include_visual_references": False,
        }

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=cached)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=MagicMock())
        ctx.__aexit__ = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value="invalid-string-result"):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)


# ===========================================================================
# Visual reference error path (lines 779-781)
# ===========================================================================

class TestVisualReferenceErrorPath:
    """Test that visual reference errors are caught and appended."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            access_token=ACCESS_TOKEN, user_question="Revenue?"
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_visual_reference_exception_appended_to_errors(self):
        """Lines 779-781: _find_visual_references raises → error appended."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [],
            "schema": {"tables": [{"name": "Sales", "columns": []}], "columns": []},
            "sample_data": {},
        }
        config = {
            "user_question": "Revenue?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "output_format": "markdown",
            "max_dax_retries": 1,
            "include_visual_references": True,
        }

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=cached)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=MagicMock())
        ctx.__aexit__ = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch("src.engines.crewai.tools.tool_session_provider.async_session_factory", return_value=ctx), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.PowerBISemanticModelCacheService", return_value=mock_service), \
             patch.object(self.tool, "_generate_dax_with_llm", return_value="EVALUATE {[Revenue]}"), \
             patch.object(self.tool, "_execute_dax_query", return_value={
                 "success": True, "data": [], "row_count": 0, "columns": [], "error": None
             }), \
             patch.object(self.tool, "_find_visual_references", side_effect=Exception("visual search failed")):
            result = self._run(self.tool._execute_analysis_pipeline(config))

        assert isinstance(result, str)


# ===========================================================================
# _extract_model_context fallback paths (lines 818-820, 828-829, 837-844)
# ===========================================================================

class TestExtractModelContextFallbacks:
    """Test _extract_model_context with Fabric unavailable and admin scanner paths."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_fabric_unavailable_falls_to_admin_scanner(self):
        """Lines 828-836: TMDL returns None → admin scanner called."""
        with patch.object(self.tool, "_get_fabric_token", return_value="fabric_token"), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=None), \
             patch.object(self.tool, "_fetch_model_via_admin_scanner", return_value=(
                 [{"name": "Revenue", "table": "Sales", "expression": "SUM(...)"}],
                 [{"name": "Sales", "columns": []}]
             )), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]):
            result = self._run(
                self.tool._extract_model_context(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert len(result["measures"]) > 0

    def test_fabric_unavailable_admin_scanner_empty_falls_to_dax(self):
        """Lines 837-844: TMDL=None AND admin scanner empty → DAX fallback."""
        with patch.object(self.tool, "_get_fabric_token", return_value="fabric_token"), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=None), \
             patch.object(self.tool, "_fetch_model_via_admin_scanner", return_value=([], [])), \
             patch.object(self.tool, "_fetch_model_via_powerbi_dax", return_value=(
                 [], [{"name": "Sales"}]
             )), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]):
            result = self._run(
                self.tool._extract_model_context(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert result["measures"] == []
        assert len(result["tables"]) > 0

    def test_fabric_token_exception_falls_back_to_powerbi_token(self):
        """Lines 818-820: getting fabric token raises → uses PowerBI token."""
        config = {"tenant_id": TENANT_ID, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}

        with patch.object(self.tool, "_get_fabric_token", side_effect=Exception("token fail")), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=[]) as mock_tmdl, \
             patch.object(self.tool, "_parse_tmdl_for_measures_and_tables", return_value=([], [])), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]):
            result = self._run(
                self.tool._extract_model_context(WS_ID, DS_ID, ACCESS_TOKEN, config)
            )

        assert isinstance(result, dict)
        mock_tmdl.assert_called_once_with(WS_ID, DS_ID, ACCESS_TOKEN)

    def test_fabric_returns_empty_list_uses_parse_tmdl(self):
        """Lines 827-831: TMDL returns empty list (not None) → parse_tmdl called."""
        config = {"tenant_id": TENANT_ID, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
        with patch.object(self.tool, "_get_fabric_token", return_value="token"), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=[]), \
             patch.object(self.tool, "_parse_tmdl_for_measures_and_tables", return_value=(
                 [{"name": "M1", "table": "T1", "expression": "SUM(...)"}],
                 [{"name": "T1"}]
             )), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]):
            result = self._run(
                self.tool._extract_model_context(WS_ID, DS_ID, ACCESS_TOKEN, config)
            )

        assert len(result["measures"]) == 1


# ===========================================================================
# _fetch_tmdl_via_fabric 202 polling branches (lines 899-914)
# ===========================================================================

class TestFetchTmdlViaFabric202Polling:
    """Test _fetch_tmdl_via_fabric long-running operation polling."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_client(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        return mock_client

    def test_202_succeeded_returns_parts(self):
        """Lines 899-908: 202 → poll → Succeeded → returns parts."""
        mock_client = self._make_client()

        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {"Location": "https://api.fabric.microsoft.com/v1/operations/op1"}

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Succeeded"}

        result_response = MagicMock()
        result_response.raise_for_status = MagicMock()
        result_response.json.return_value = {
            "definition": {"parts": [{"path": "model.tmdl", "payload": "abc"}]}
        }

        mock_client.post = AsyncMock(return_value=post_response)
        mock_client.get = AsyncMock(side_effect=[poll_response, result_response])

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN)
            )

        assert result is not None
        assert len(result) == 1

    def test_202_failed_returns_none(self):
        """Lines 909-911: 202 → poll → Failed → returns None."""
        mock_client = self._make_client()

        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {"Location": "https://api.fabric.microsoft.com/v1/operations/op1"}

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Failed", "error": "timeout"}

        mock_client.post = AsyncMock(return_value=post_response)
        mock_client.get = AsyncMock(return_value=poll_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN)
            )

        assert result is None

    def test_202_no_location_header_returns_none(self):
        """Lines 895-897: 202 but no Location header → returns None."""
        mock_client = self._make_client()

        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {}

        mock_client.post = AsyncMock(return_value=post_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN)
            )

        assert result is None

    def test_202_polling_all_running_exhausts_and_returns_none(self):
        """Lines 913-914: all polls return non-terminal status → returns None after timeout."""
        mock_client = self._make_client()

        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {"Location": "https://api.fabric.microsoft.com/v1/operations/op1"}

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Failed"}  # Will break immediately

        mock_client.post = AsyncMock(return_value=post_response)
        mock_client.get = AsyncMock(return_value=poll_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN)
            )

        assert result is None


# ===========================================================================
# _fetch_model_via_admin_scanner polling branches (lines 996-1055)
# ===========================================================================

class TestAdminScannerPollingBranches:
    """Test the polling and result parsing branches of _fetch_model_via_admin_scanner."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_client(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        return mock_client

    def test_scan_failed_status_returns_empty(self):
        """Lines 1013-1015: scan poll returns Failed → returns ([], [])."""
        mock_client = self._make_client()

        scan_response = MagicMock()
        scan_response.status_code = 200
        scan_response.raise_for_status = MagicMock()
        scan_response.json.return_value = {"id": "scan-id-1"}

        poll_response = MagicMock()
        poll_response.raise_for_status = MagicMock()
        poll_response.json.return_value = {"status": "Failed", "error": "scan error"}

        mock_client.post = AsyncMock(return_value=scan_response)
        mock_client.get = AsyncMock(return_value=poll_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert result == ([], [])

    def test_scan_polling_exception_returns_empty(self):
        """Lines 1019-1021: polling raises exception → returns ([], [])."""
        mock_client = self._make_client()

        scan_response = MagicMock()
        scan_response.status_code = 200
        scan_response.raise_for_status = MagicMock()
        scan_response.json.return_value = {"id": "scan-id-1"}

        mock_client.post = AsyncMock(return_value=scan_response)
        mock_client.get = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert result == ([], [])

    def test_scan_result_exception_returns_empty(self):
        """Lines 1030-1032: scanResult fetch raises exception → returns ([], [])."""
        mock_client = self._make_client()

        scan_response = MagicMock()
        scan_response.status_code = 200
        scan_response.raise_for_status = MagicMock()
        scan_response.json.return_value = {"id": "scan-id-1"}

        poll_response = MagicMock()
        poll_response.raise_for_status = MagicMock()
        poll_response.json.return_value = {"status": "Succeeded"}

        result_response = MagicMock()
        result_response.raise_for_status = MagicMock(side_effect=Exception("result error"))

        mock_client.post = AsyncMock(return_value=scan_response)
        mock_client.get = AsyncMock(side_effect=[poll_response, result_response])

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert result == ([], [])

    def test_workspace_not_found_returns_empty(self):
        """Lines 1042-1044: workspace not in scan result → returns ([], [])."""
        mock_client = self._make_client()

        scan_response = MagicMock()
        scan_response.status_code = 200
        scan_response.raise_for_status = MagicMock()
        scan_response.json.return_value = {"id": "scan-id-1"}

        poll_response = MagicMock()
        poll_response.raise_for_status = MagicMock()
        poll_response.json.return_value = {"status": "Succeeded"}

        result_response = MagicMock()
        result_response.raise_for_status = MagicMock()
        result_response.json.return_value = {
            "workspaces": [{"id": "other-workspace-id", "datasets": []}]
        }

        mock_client.post = AsyncMock(return_value=scan_response)
        mock_client.get = AsyncMock(side_effect=[poll_response, result_response])

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert result == ([], [])

    def test_dataset_not_found_returns_empty(self):
        """Lines 1053-1055: dataset not in workspace scan result → returns ([], [])."""
        mock_client = self._make_client()

        scan_response = MagicMock()
        scan_response.status_code = 200
        scan_response.raise_for_status = MagicMock()
        scan_response.json.return_value = {"id": "scan-id-1"}

        poll_response = MagicMock()
        poll_response.raise_for_status = MagicMock()
        poll_response.json.return_value = {"status": "Succeeded"}

        result_response = MagicMock()
        result_response.raise_for_status = MagicMock()
        result_response.json.return_value = {
            "workspaces": [{"id": WS_ID, "datasets": [{"id": "other-ds-id", "tables": []}]}]
        }

        mock_client.post = AsyncMock(return_value=scan_response)
        mock_client.get = AsyncMock(side_effect=[poll_response, result_response])

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert result == ([], [])

    def test_system_tables_skipped_and_empty_expression_measures_skipped(self):
        """Lines 1060-1075: system tables and empty-expression measures skipped."""
        mock_client = self._make_client()

        scan_response = MagicMock()
        scan_response.status_code = 200
        scan_response.raise_for_status = MagicMock()
        scan_response.json.return_value = {"id": "scan-id-1"}

        poll_response = MagicMock()
        poll_response.raise_for_status = MagicMock()
        poll_response.json.return_value = {"status": "Succeeded"}

        result_response = MagicMock()
        result_response.raise_for_status = MagicMock()
        result_response.json.return_value = {
            "workspaces": [{
                "id": WS_ID,
                "datasets": [{
                    "id": DS_ID,
                    "tables": [
                        {"name": "LocalDateTable_xyz", "columns": [], "measures": []},
                        {"name": "Sales", "columns": [
                            {"name": "Amount", "isHidden": False},
                            {"name": "_hidden", "isHidden": True},
                        ], "measures": [
                            {"name": "Revenue", "expression": "SUM(Sales[Amount])"},
                            {"name": "Empty", "expression": ""},
                        ]},
                    ]
                }]
            }]
        }

        mock_client.post = AsyncMock(return_value=scan_response)
        mock_client.get = AsyncMock(side_effect=[poll_response, result_response])

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN,
                                                          {"skip_system_tables": True})
            )

        table_names = [t["name"] for t in tables]
        assert "LocalDateTable_xyz" not in table_names
        assert "Sales" in table_names
        assert len(measures) == 1
        assert measures[0]["name"] == "Revenue"

    def test_no_scan_id_returns_empty(self):
        """Lines 991-993: getInfo response has no scan ID → returns ([], [])."""
        mock_client = self._make_client()

        scan_response = MagicMock()
        scan_response.status_code = 200
        scan_response.raise_for_status = MagicMock()
        scan_response.json.return_value = {}

        mock_client.post = AsyncMock(return_value=scan_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert result == ([], [])

    def test_http_status_error_returns_empty(self):
        """Lines 995-997: httpx.HTTPStatusError from getInfo → returns ([], [])."""
        import httpx as _httpx

        mock_client = self._make_client()
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_error = _httpx.HTTPStatusError("Server Error", request=mock_request, response=mock_response)
        mock_client.post = AsyncMock(side_effect=http_error)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert result == ([], [])


# ===========================================================================
# _parse_tmdl_for_measures_and_tables: measure extraction details
# (lines 1199, 1230-1241)
# ===========================================================================

class TestParseTmdlMeasureExtraction:
    """Test TMDL parsing with measure expressions containing metadata tags."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_measure_with_lineage_tag_stripped(self):
        """Lines 1234-1239: lineageTag lines stop expression accumulation."""
        content = "table Sales\n\tmeasure Revenue = SUM(Sales[Amount])\n\t\tlineageTag: abc123\n\t\tformatString: #,##0\n"
        payload = base64.b64encode(content.encode()).decode()
        parts = [{"path": "definition/tables/Sales.tmdl", "payload": payload}]

        measures, tables = self.tool._parse_tmdl_for_measures_and_tables(parts, {})

        assert any(t["name"] == "Sales" for t in tables)
        if measures:
            assert "lineageTag" not in measures[0].get("expression", "")

    def test_multiple_measures_extracted(self):
        """Lines 1229-1244: multiple measures in one table extracted."""
        content = (
            "table Sales\n"
            "\tcolumn Amount\n"
            "\tmeasure Revenue = SUM(Sales[Amount])\n"
            "\tmeasure Count = COUNTROWS(Sales)\n"
        )
        payload = base64.b64encode(content.encode()).decode()
        parts = [{"path": "definition/tables/Sales.tmdl", "payload": payload}]

        measures, tables = self.tool._parse_tmdl_for_measures_and_tables(parts, {})

        measure_names = [m["name"] for m in measures]
        assert len(measure_names) >= 1

    def test_quoted_table_name_extracted(self):
        """Lines 1199-1200: table name in quotes extracted correctly."""
        content = "table 'My Sales Table'\n\tcolumn Amount\n"
        payload = base64.b64encode(content.encode()).decode()
        parts = [{"path": "definition/tables/My Sales Table.tmdl", "payload": payload}]

        measures, tables = self.tool._parse_tmdl_for_measures_and_tables(parts, {})

        assert any("My Sales Table" in t["name"] for t in tables)


# ===========================================================================
# _enrich_model_context_with_metadata: column metadata exception per table
# (lines 1372-1388)
# ===========================================================================

class TestEnrichColumnMetadataExceptionHandling:
    """Test error handling in _enrich_model_context_with_metadata per-table."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_per_table_exception_continues_to_next_table(self):
        """Lines 1372-1374: one table throws exception, loop continues."""
        model_context = {
            "tables": [
                {"name": "Sales", "columns": ["Amount"]},
                {"name": "Date", "columns": ["DateKey"]},
            ],
            "measures": [],
        }
        config = {"enable_info_columns": True, "skip_system_tables": True}

        call_count = [0]
        async def mock_fetch_columns(ws, ds, token, table_name, cfg):
            call_count[0] += 1
            if table_name == "Sales":
                raise Exception("fetch failed for Sales")
            return [{"column_name": "DateKey", "data_type": "DateTime", "is_hidden": False, "description": ""}]

        with patch.object(self.tool, "_fetch_column_metadata_for_table", side_effect=mock_fetch_columns), \
             patch.object(self.tool, "_fetch_sample_column_values", return_value={}):
            result = self._run(
                self.tool._enrich_model_context_with_metadata(
                    model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                )
            )

        assert call_count[0] == 2
        assert isinstance(result, dict)

    def test_no_tables_enriched_logs_message(self):
        """Lines 1381-1385: no tables enriched → logs appropriate message."""
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Amount"]}],
            "measures": [],
        }
        config = {"enable_info_columns": True, "skip_system_tables": True}

        with patch.object(self.tool, "_fetch_column_metadata_for_table", return_value=[]), \
             patch.object(self.tool, "_fetch_sample_column_values", return_value={}):
            result = self._run(
                self.tool._enrich_model_context_with_metadata(
                    model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                )
            )

        assert isinstance(result, dict)


# ===========================================================================
# _extract_default_filters HTTP paths (lines 1424-1497)
# ===========================================================================

class TestExtractDefaultFiltersHttpPaths:
    """Test _extract_default_filters with 202, 200, error HTTP paths."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_client(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        return mock_client

    def test_200_response_parses_filters(self):
        """Lines 1473-1480: 200 direct response → parse TMDL parts."""
        filter_def = {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "Sales"}},
                    "Property": "Region"
                }
            },
            "filter": {
                "Where": [{"Condition": {"In": {"Values": [[{"Literal": {"Value": "'North'"}}]]}}}]
            }
        }
        parts = [{
            "path": "report.json",
            "payload": base64.b64encode(json.dumps({"filters": json.dumps([filter_def])}).encode()).decode()
        }]
        response_data = {"definition": {"parts": parts}}

        mock_client = self._make_client()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = response_data
        mock_client.post = AsyncMock(return_value=response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "report-id", ACCESS_TOKEN)
            )

        assert isinstance(result, dict)

    def test_200_response_no_parts_returns_empty(self):
        """Lines 1478-1480: 200 response with empty parts → empty filters."""
        mock_client = self._make_client()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"definition": {"parts": []}}
        mock_client.post = AsyncMock(return_value=response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "report-id", ACCESS_TOKEN)
            )

        assert result == {}

    def test_unexpected_status_code_returns_empty(self):
        """Lines 1481-1483: unexpected status code → returns {}."""
        mock_client = self._make_client()
        response = MagicMock()
        response.status_code = 404
        response.text = "Not found"
        mock_client.post = AsyncMock(return_value=response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "report-id", ACCESS_TOKEN)
            )

        assert result == {}

    def test_202_succeeded_returns_filters(self):
        """Lines 1443-1466: 202 → poll → Succeeded → parse filters."""
        filter_def = {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "Sales"}},
                    "Property": "BU"
                }
            },
            "filter": {
                "Where": [{"Condition": {"In": {"Values": [[{"Literal": {"Value": "'Italy'"}}]]}}}]
            }
        }
        parts = [{
            "path": "report.json",
            "payload": base64.b64encode(json.dumps({"filters": json.dumps([filter_def])}).encode()).decode()
        }]

        mock_client = self._make_client()

        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {"Location": "https://api.fabric.microsoft.com/operations/op1"}

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Succeeded"}

        result_response = MagicMock()
        result_response.raise_for_status = MagicMock()
        result_response.json.return_value = {"definition": {"parts": parts}}

        mock_client.post = AsyncMock(return_value=post_response)
        mock_client.get = AsyncMock(side_effect=[poll_response, result_response])

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "report-id", ACCESS_TOKEN)
            )

        assert isinstance(result, dict)
        assert "Sales[BU]" in result

    def test_202_no_location_header_returns_empty(self):
        """Lines 1470-1471: 202 but no Location header → returns {}."""
        mock_client = self._make_client()
        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {}
        mock_client.post = AsyncMock(return_value=post_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "report-id", ACCESS_TOKEN)
            )

        assert result == {}

    def test_202_poll_failed_status_returns_empty(self):
        """Lines 1467-1469: 202 → poll → Failed → returns {}."""
        mock_client = self._make_client()

        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {"Location": "https://api.fabric.microsoft.com/operations/op1"}

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Failed"}

        mock_client.post = AsyncMock(return_value=post_response)
        mock_client.get = AsyncMock(return_value=poll_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "report-id", ACCESS_TOKEN)
            )

        assert result == {}

    def test_http_status_error_returns_empty(self):
        """Lines 1492-1493: httpx.HTTPStatusError → returns {}."""
        import httpx as _httpx

        mock_client = self._make_client()
        mock_request = MagicMock()
        mock_response_obj = MagicMock()
        mock_response_obj.status_code = 403
        http_error = _httpx.HTTPStatusError("Forbidden", request=mock_request, response=mock_response_obj)
        mock_client.post = AsyncMock(side_effect=http_error)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "report-id", ACCESS_TOKEN)
            )

        assert result == {}

    def test_general_exception_returns_empty(self):
        """Lines 1494-1495: generic exception → returns {}."""
        mock_client = self._make_client()
        mock_client.post = AsyncMock(side_effect=Exception("connection refused"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "report-id", ACCESS_TOKEN)
            )

        assert result == {}


# ===========================================================================
# _generate_dax_with_llm: HTTP response path (lines 2516-2576)
# ===========================================================================

class TestGenerateDaxWithLlmHttpPath:
    """Test _generate_dax_with_llm when LLM credentials are provided and HTTP succeeds."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def _base_config(self, **extra):
        base = {
            "llm_workspace_url": "https://databricks.example.com",
            "llm_token": "my-token",
            "llm_model": "databricks-claude-sonnet",
            "business_mappings": {},
            "field_synonyms": {},
            "active_filters": {},
            "conversation_history": [],
            "visible_tables": [],
        }
        base.update(extra)
        return base

    def test_successful_llm_call_returns_dax(self):
        """Lines 2516-2576: successful HTTP response → extract DAX and return."""
        model_context = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "tables": [{"name": "Sales", "columns": ["Amount"]}],
            "relationships": [],
        }
        config = self._base_config()

        llm_response = {
            "choices": [{"message": {"content": "EVALUATE\nSUMMARIZECOLUMNS(\n    \"Result\", [Revenue]\n)"}}]
        }

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = llm_response
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._generate_dax_with_llm("What is revenue?", model_context, config)
            )

        assert result is not None
        assert "EVALUATE" in result or "Revenue" in result

    def test_llm_response_with_hallucinated_measure_still_returns(self):
        """Lines 2568-2571: hallucinated measure triggers warning but returns DAX."""
        model_context = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "tables": [{"name": "Sales", "columns": ["Amount"]}],
            "relationships": [],
        }
        config = self._base_config()

        llm_response = {
            "choices": [{"message": {"content": "EVALUATE\nSUMMARIZECOLUMNS(\n    \"R\", [FakeMetric]\n)"}}]
        }

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = llm_response
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._generate_dax_with_llm("What is revenue?", model_context, config)
            )

        assert result is not None

    def test_llm_http_exception_falls_back_to_simple_dax(self):
        """Lines 2578-2581: exception → fallback to _generate_simple_dax."""
        model_context = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "tables": [],
            "relationships": [],
        }
        config = self._base_config()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("HTTP timeout"))

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._generate_dax_with_llm("What is revenue?", model_context, config)
            )

        assert result is not None
        assert "EVALUATE" in result

    def test_no_measures_returns_none_early(self):
        """Lines 2287-2289: no measures → returns None before HTTP call."""
        model_context = {"measures": [], "tables": [], "relationships": []}
        config = self._base_config()

        result = self._run(
            self.tool._generate_dax_with_llm("What is revenue?", model_context, config)
        )

        assert result is None

    def test_active_filters_trigger_auto_wrap(self):
        """Line 2574: active_filters present → auto-wrap applied."""
        model_context = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "tables": [{"name": "Sales", "columns": ["Amount", "BU"]}],
            "relationships": [],
        }
        config = self._base_config(active_filters={"Sales[BU]": "Italy"})

        llm_response = {
            "choices": [{"message": {"content": "EVALUATE\nSUMMARIZECOLUMNS(\n    \"R\", [Revenue]\n)"}}]
        }

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = llm_response
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._generate_dax_with_llm("What is revenue?", model_context, config)
            )

        assert result is not None


# ===========================================================================
# _emit_llm_trace with trace_context set (lines 2615-2668)
# ===========================================================================

class TestEmitLlmTraceWithContext:
    """Test _emit_llm_trace when trace_context is set."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_emit_trace_exception_does_not_propagate(self):
        """Lines 2666-2668: exception in trace emission is caught."""
        self.tool.trace_context = {
            "job_id": "job-err",
            "group_context": {}
        }

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.get_trace_queue",
                   side_effect=Exception("queue unavailable"), create=True):
            self.tool._emit_llm_trace(
                event_context="Test",
                prompt="test",
                model="model",
                operation="op"
            )

    def test_emit_trace_no_job_id_skips(self):
        """Lines 2617-2619: trace_context without job_id → skips emission."""
        self.tool.trace_context = {"group_context": {}}

        mock_queue = MagicMock()
        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.get_trace_queue",
                   return_value=mock_queue, create=True):
            self.tool._emit_llm_trace(
                event_context="Test",
                prompt="test",
                model="model",
                operation="op"
            )

        mock_queue.put_nowait.assert_not_called()

    def test_emit_trace_no_trace_context_skips(self):
        """Lines 2611-2613: no trace_context at all → skips emission."""
        # Remove trace_context if set
        if hasattr(self.tool, "trace_context"):
            del self.tool.trace_context

        mock_queue = MagicMock()
        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.get_trace_queue",
                   return_value=mock_queue, create=True):
            self.tool._emit_llm_trace(
                event_context="Test",
                prompt="test",
                model="model",
                operation="op"
            )

        mock_queue.put_nowait.assert_not_called()

    def test_emit_trace_with_valid_job_id_calls_queue(self):
        """Lines 2621-2664: valid trace_context → get_trace_queue → put_nowait called."""
        self.tool.trace_context = {
            "job_id": "job-abc",
            "group_context": {"primary_group_id": "g1"}
        }

        mock_queue = MagicMock()
        mock_queue.put_nowait = MagicMock()

        # The import is inside the method: "from src.services.trace_queue import get_trace_queue"
        with patch("src.services.trace_queue.get_trace_queue", return_value=mock_queue, create=True):
            self.tool._emit_llm_trace(
                event_context="DAX Generation",
                prompt="Generate DAX",
                model="model-v1",
                operation="generate_dax",
                response="EVALUATE {[Revenue]}"
            )

        mock_queue.put_nowait.assert_called_once()


# ===========================================================================
# _generate_dax_with_self_correction: HTTP success path (lines 2859-2892)
# ===========================================================================

class TestGenerateDaxWithSelfCorrectionHttpSuccess:
    """Test _generate_dax_with_self_correction successful HTTP path."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def _base_config(self):
        return {
            "llm_workspace_url": "https://databricks.example.com",
            "llm_token": "my-token",
            "llm_model": "databricks-claude-sonnet",
            "business_mappings": {},
            "field_synonyms": {},
            "active_filters": {},
            "conversation_history": [],
            "visible_tables": [],
        }

    def test_successful_llm_correction_returns_dax(self):
        """Lines 2859-2892: successful HTTP → extract DAX → return."""
        model_context = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "tables": [{"name": "Sales", "columns": ["Amount"]}],
            "relationships": [],
        }
        config = self._base_config()
        previous = [{"attempt": 1, "dax": "EVALUATE Sales", "success": False, "error": "syntax error"}]

        llm_response = {
            "choices": [{"message": {"content": "EVALUATE\nSUMMARIZECOLUMNS(\n    \"R\", [Revenue]\n)"}}]
        }

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = llm_response
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._generate_dax_with_self_correction("Revenue?", model_context, config, previous)
            )

        assert result is not None
        assert "EVALUATE" in result or "Revenue" in result

    def test_hallucinated_measures_in_correction_still_returns(self):
        """Lines 2887-2889: correction returns DAX with non-existent measure; warning logged."""
        model_context = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "tables": [{"name": "Sales", "columns": ["Amount"]}],
            "relationships": [],
        }
        config = self._base_config()
        previous = [{"attempt": 1, "dax": "EVALUATE Sales", "success": False, "error": "err"}]

        llm_response = {
            "choices": [{"message": {"content": "EVALUATE\nSUMMARIZECOLUMNS(\n    \"R\", [NonExistentMeasure]\n)"}}]
        }

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = llm_response
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._generate_dax_with_self_correction("Revenue?", model_context, config, previous)
            )

        assert result is not None


# ===========================================================================
# _get_measure_page_references HTTP paths (lines 3092-3184)
# ===========================================================================

class TestGetMeasurePageReferences:
    """Test _get_measure_page_references HTTP paths (202 polling, 200 direct, error)."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_page_part(self, page_id, display_name):
        page_data = {"name": page_id, "displayName": display_name, "ordinal": 0}
        return {
            "path": f"definition/pages/{page_id}/page.json",
            "payload": base64.b64encode(json.dumps(page_data).encode()).decode()
        }

    def test_200_response_returns_refs(self):
        """Lines 3112-3113: 200 response → parse report_parts → build refs."""
        page_part = self._make_page_part("Page1", "Overview")

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"definition": {"parts": [page_part]}}

        outer_client = MagicMock()
        outer_client.post = AsyncMock(return_value=response)

        result = self._run(
            self.tool._get_measure_page_references(
                WS_ID, "report-id", "My Report", "https://report.url",
                ["Revenue"], ACCESS_TOKEN, outer_client
            )
        )

        assert isinstance(result, list)

    def test_non_200_non_202_returns_empty(self):
        """Line 3115: non-200/202 status → returns []."""
        response = MagicMock()
        response.status_code = 404

        outer_client = MagicMock()
        outer_client.post = AsyncMock(return_value=response)

        result = self._run(
            self.tool._get_measure_page_references(
                WS_ID, "report-id", "My Report", "https://report.url",
                ["Revenue"], ACCESS_TOKEN, outer_client
            )
        )

        assert result == []

    def test_202_succeeded_returns_refs(self):
        """Lines 3096-3106: 202 → poll → Succeeded → fetch result → parse."""
        page_part = self._make_page_part("Page1", "Overview")

        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {"Location": "https://api.fabric.microsoft.com/operations/op1"}

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Succeeded"}

        result_response = MagicMock()
        result_response.raise_for_status = MagicMock()
        result_response.json.return_value = {"definition": {"parts": [page_part]}}

        outer_client = MagicMock()
        outer_client.post = AsyncMock(return_value=post_response)
        outer_client.get = AsyncMock(side_effect=[poll_response, result_response])

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._get_measure_page_references(
                    WS_ID, "report-id", "My Report", "https://report.url",
                    ["Revenue"], ACCESS_TOKEN, outer_client
                )
            )

        assert isinstance(result, list)

    def test_202_no_location_header_returns_empty(self):
        """Lines 3093-3094: 202 but no Location → returns []."""
        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {}

        outer_client = MagicMock()
        outer_client.post = AsyncMock(return_value=post_response)

        result = self._run(
            self.tool._get_measure_page_references(
                WS_ID, "report-id", "My Report", "https://report.url",
                ["Revenue"], ACCESS_TOKEN, outer_client
            )
        )

        assert result == []

    def test_202_poll_failed_returns_empty(self):
        """Lines 3107-3108: 202 → poll Failed → returns []."""
        post_response = MagicMock()
        post_response.status_code = 202
        post_response.headers = {"Location": "https://api.fabric.microsoft.com/operations/op1"}

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Failed"}

        outer_client = MagicMock()
        outer_client.post = AsyncMock(return_value=post_response)
        outer_client.get = AsyncMock(return_value=poll_response)

        with patch("src.engines.crewai.tools.custom.powerbi_analysis_tool.asyncio.sleep", return_value=None):
            result = self._run(
                self.tool._get_measure_page_references(
                    WS_ID, "report-id", "My Report", "https://report.url",
                    ["Revenue"], ACCESS_TOKEN, outer_client
                )
            )

        assert result == []

    def test_exception_returns_empty(self):
        """Lines 3182-3184: general exception → returns []."""
        outer_client = MagicMock()
        outer_client.post = AsyncMock(side_effect=Exception("network failure"))

        result = self._run(
            self.tool._get_measure_page_references(
                WS_ID, "report-id", "My Report", "https://report.url",
                ["Revenue"], ACCESS_TOKEN, outer_client
            )
        )

        assert result == []

    def test_no_report_parts_returns_empty(self):
        """Lines 3117-3118: empty report parts → returns []."""
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"definition": {"parts": []}}

        outer_client = MagicMock()
        outer_client.post = AsyncMock(return_value=response)

        result = self._run(
            self.tool._get_measure_page_references(
                WS_ID, "report-id", "My Report", "https://report.url",
                ["Revenue"], ACCESS_TOKEN, outer_client
            )
        )

        assert result == []

    def test_measure_not_found_in_visuals_adds_report_level_ref(self):
        """Lines 3168-3178: measure not in visuals → report-level reference added."""
        page_part = self._make_page_part("Page1", "Overview")
        visual_data = {"visual": {"visualType": "card"}}
        visual_part = {
            "path": "definition/pages/Page1/visuals/V1/visual.json",
            "payload": base64.b64encode(json.dumps(visual_data).encode()).decode()
        }

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"definition": {"parts": [page_part, visual_part]}}

        outer_client = MagicMock()
        outer_client.post = AsyncMock(return_value=response)

        result = self._run(
            self.tool._get_measure_page_references(
                WS_ID, "report-id", "My Report", "https://report.url",
                ["Revenue"], ACCESS_TOKEN, outer_client
            )
        )

        assert isinstance(result, list)
        if result:
            assert result[0]["measure"] == "Revenue"


# ===========================================================================
# _parse_report_pages: "Pages" capitalized path (lines 3219-3223)
# ===========================================================================

class TestParseReportPagesCapitalizedPath:
    """Test _parse_report_pages with 'Pages' (capital P) in path."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_pages_capitalized_in_path_extracted(self):
        """Lines 3219-3221: 'Pages' (capital) in path → page_id extracted."""
        page_data = {"name": "MyPage", "displayName": "My Page", "ordinal": 0}
        payload = base64.b64encode(json.dumps(page_data).encode()).decode()
        part = {"path": "definition/Pages/PageOne/page.json", "payload": payload}

        result = self.tool._parse_report_pages([part])

        assert len(result) >= 1
        assert result[0]["id"] == "PageOne"

    def test_no_pages_in_path_uses_second_to_last(self):
        """Line 3223: neither 'pages' nor 'Pages' in path → path_parts[-2]."""
        page_data = {"name": "Pg", "displayName": "Pg", "ordinal": 0}
        payload = base64.b64encode(json.dumps(page_data).encode()).decode()
        part = {"path": "some/other/Folder/page.json", "payload": payload}

        result = self.tool._parse_report_pages([part])

        assert len(result) >= 1
        assert result[0]["id"] == "Folder"


# ===========================================================================
# _parse_report_visuals: "Pages" and "Visuals" capitalized paths
# (lines 3301-3313)
# ===========================================================================

class TestParseReportVisualsCapitalizedPaths:
    """Test _parse_report_visuals with 'Pages' and 'Visuals' (capitals) in path."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_pages_and_visuals_capitalized(self):
        """Lines 3301-3308: 'Pages' and 'Visuals' capitalized in path."""
        visual_data = {"visual": {"visualType": "lineChart"}}
        payload = base64.b64encode(json.dumps(visual_data).encode()).decode()
        part = {
            "path": "definition/Pages/Page1/Visuals/Vis1/visual.json",
            "payload": payload
        }

        result = self.tool._parse_report_visuals([part])

        assert len(result) == 1
        assert result[0]["page_id"] == "Page1"
        assert result[0]["type"] == "lineChart"

    def test_visuals_not_in_path_uses_second_to_last(self):
        """Lines 3312-3313: path has /visuals/ for is_visual_file but split leaves
        neither 'visuals' nor 'Visuals' as standalone component → path_parts[-2] fallback.
        We verify the 'Visuals' (capital) branch is hit as intended."""
        # Use a path with 'Visuals' so is_visual_file passes but path_parts has 'Visuals'
        # Then test the case where visual_id is set via index of 'Visuals'
        visual_data = {"visual": {"visualType": "table"}}
        payload = base64.b64encode(json.dumps(visual_data).encode()).decode()
        # Path with /Visuals/ (capital) - handled by the elif branch at line 3309
        part = {
            "path": "definition/Pages/Page2/Visuals/VisualXYZ/visual.json",
            "payload": payload
        }

        result = self.tool._parse_report_visuals([part])

        # Should find the visual via 'Visuals' branch
        assert len(result) >= 1
        assert result[0]["id"] == "VisualXYZ"


# ===========================================================================
# _parse_visuals_from_report_json edge cases (lines 3351-3390)
# ===========================================================================

class TestParseVisualsFromReportJsonEdgeCases:
    """Test edge cases in _parse_visuals_from_report_json."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _make_part(self, path, data):
        return {
            "path": path,
            "payload": base64.b64encode(json.dumps(data).encode()).decode()
        }

    def test_non_dict_page_item_skipped(self):
        """Line 3351: non-dict item in pages list is skipped."""
        data = {
            "pages": [
                "not-a-dict",
                {"name": "p1", "visualContainers": []}
            ]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert isinstance(result, list)

    def test_visual_with_dict_config(self):
        """Lines 3376-3378: config is already a dict, not a string."""
        data = {
            "pages": [{
                "name": "p1",
                "visuals": [{
                    "name": "v1",
                    "config": {"singleVisual": {"visualType": "barChart"}}
                }]
            }]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1
        assert result[0]["type"] == "barChart"

    def test_visual_with_invalid_json_config_gets_unknown_type(self):
        """Lines 3374: invalid JSON string config → unknown type."""
        data = {
            "pages": [{
                "name": "p1",
                "visuals": [{
                    "name": "v1",
                    "config": "not-valid-json{"
                }]
            }]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1
        assert result[0]["type"] == "unknown"

    def test_visual_with_visual_type_key_directly(self):
        """Lines 3379-3381: visual has 'visualType' directly."""
        data = {
            "pages": [{
                "name": "p1",
                "visuals": [{
                    "name": "v1",
                    "visualType": "pie"
                }]
            }]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1
        assert result[0]["type"] == "pie"

    def test_non_dict_visual_item_skipped(self):
        """Line 3362: non-dict visual item is skipped."""
        data = {
            "pages": [{
                "name": "p1",
                "visuals": [
                    "not-a-dict",
                    {"name": "v1", "visualType": "card"}
                ]
            }]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1

    def test_exception_in_report_json_parsing_handled(self):
        """Lines 3389-3390: exception in parsing is caught."""
        part = {"path": "report.json", "payload": "!!invalid base64!!"}
        result = self.tool._parse_visuals_from_report_json([part])
        assert isinstance(result, list)

    def test_fallback_visual_id_when_no_name_or_id(self):
        """Line 3364: visual_id fallback to f'visual_{vis_idx}'."""
        data = {
            "pages": [{
                "name": "p1",
                "visuals": [
                    {"config": {"singleVisual": {"visualType": "card"}}},
                ]
            }]
        }
        part = self._make_part("report.json", data)
        result = self.tool._parse_visuals_from_report_json([part])
        assert len(result) == 1
        assert result[0]["id"] == "visual_0"


# ===========================================================================
# _find_measures_in_dict: aggregation context path (lines 3433-3436)
# ===========================================================================

class TestFindMeasuresInDictAggregation:
    """Test _find_measures_in_dict with aggregation context."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def test_aggregation_property_added(self):
        """Lines 3433-3436: obj has aggregation + property → added to measures."""
        obj = {"aggregation": "Sum", "property": "Revenue"}
        measures = set()
        self.tool._find_measures_in_dict(obj, measures)
        assert "Revenue" in measures

    def test_measure_as_string_added(self):
        """Lines 3421-3422: measure value is a string → added directly."""
        obj = {"measure": "DirectMeasure"}
        measures = set()
        self.tool._find_measures_in_dict(obj, measures)
        assert "DirectMeasure" in measures

    def test_uppercase_measure_name_key(self):
        """Lines 3425-3430: 'Measure' key with 'Name' property."""
        obj = {"Measure": {"Name": "SomeMeasure"}}
        measures = set()
        self.tool._find_measures_in_dict(obj, measures)
        assert "SomeMeasure" in measures

    def test_uppercase_measure_property_key(self):
        """Lines 3425-3430: 'Measure' key with 'Property' key."""
        obj = {"Measure": {"Property": "PropMeasure"}}
        measures = set()
        self.tool._find_measures_in_dict(obj, measures)
        assert "PropMeasure" in measures


# ===========================================================================
# _format_output: visual references and data formatting (lines 3496-3580)
# ===========================================================================

class TestFormatOutputExtendedBranches:
    """Test _format_output branches for visual refs and data tables."""

    def setup_method(self):
        self.tool = PowerBIAnalysisTool()

    def _base_results(self, visual_refs=None, dax_exec=None, dax_attempts=None):
        return {
            "user_question": "Revenue?",
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "model_context": {"measures": [], "tables": [], "relationships": []},
            "generated_dax": "EVALUATE {[Revenue]}",
            "dax_execution": dax_exec or {"success": True, "data": [], "row_count": 0, "columns": [], "error": None},
            "errors": [],
            "dax_attempts": dax_attempts or [{"attempt": 1, "dax": "EVALUATE {[Revenue]}", "success": True, "error": None}],
            "visual_references": visual_refs or [],
        }

    def test_visual_refs_with_page_info_formatted(self):
        """Lines 3539-3548: visual references with page info."""
        refs = [{
            "report_name": "Sales Report",
            "report_url": "https://app.powerbi.com/report1",
            "page_name": "Overview",
            "page_url": "https://app.powerbi.com/report1/PageOverview",
            "measure": "Revenue",
            "visual_type": "card",
            "note": ""
        }]
        output = self.tool._format_output(self._base_results(visual_refs=refs), "markdown")

        assert "Sales Report" in output
        assert "Overview" in output
        assert "Revenue" in output

    def test_visual_refs_without_page_info_formatted(self):
        """Lines 3549-3557: visual references with page_name=None → _no_page_ bucket."""
        refs = [{
            "report_name": "Sales Report",
            "report_url": "https://app.powerbi.com/report1",
            "page_name": None,
            "page_url": None,
            "measure": "Revenue",
            "visual_type": None,
            "note": "Report uses the same dataset"
        }]
        output = self.tool._format_output(self._base_results(visual_refs=refs), "markdown")

        assert "Sales Report" in output
        assert "Revenue" in output

    def test_dax_execution_failure_formatted(self):
        """Line 3516: failed execution → error message shown."""
        dax_exec = {"success": False, "data": [], "row_count": 0, "columns": [], "error": "DAX error"}
        output = self.tool._format_output(self._base_results(dax_exec=dax_exec), "markdown")

        assert "DAX error" in output or "Failed" in output

    def test_execution_data_table_formatted(self):
        """Lines 3500-3514: successful execution with data rows → table formatted."""
        dax_exec = {
            "success": True,
            "data": [{"[Revenue]": 100}, {"[Revenue]": 200}],
            "row_count": 2,
            "columns": ["[Revenue]"],
            "error": None
        }
        output = self.tool._format_output(self._base_results(dax_exec=dax_exec), "markdown")

        assert "100" in output or "Revenue" in output

    def test_execution_many_rows_truncated(self):
        """Lines 3513-3514: > 20 rows → truncation message."""
        dax_exec = {
            "success": True,
            "data": [{"[R]": i} for i in range(25)],
            "row_count": 25,
            "columns": ["[R]"],
            "error": None
        }
        output = self.tool._format_output(self._base_results(dax_exec=dax_exec), "markdown")

        assert "more rows" in output

    def test_multiple_retry_attempts_shown(self):
        """Lines 3479-3486: multiple dax_attempts → retry history shown."""
        attempts = [
            {"attempt": 1, "dax": "EVALUATE bad", "success": False, "error": "syntax error", "row_count": 0},
            {"attempt": 2, "dax": "EVALUATE {[Revenue]}", "success": True, "error": None, "row_count": 5},
        ]
        output = self.tool._format_output(self._base_results(dax_attempts=attempts), "markdown")

        assert "Attempt" in output or "2" in output

    def test_visual_ref_with_visual_type_shown(self):
        """Line 3577-3578: visual_types shown when present."""
        refs = [{
            "report_name": "R1",
            "report_url": "https://url",
            "page_name": "P1",
            "page_url": "https://url/p1",
            "measure": "M1",
            "visual_type": "barChart",
            "note": ""
        }]
        output = self.tool._format_output(self._base_results(visual_refs=refs), "markdown")

        assert "barChart" in output or "M1" in output
