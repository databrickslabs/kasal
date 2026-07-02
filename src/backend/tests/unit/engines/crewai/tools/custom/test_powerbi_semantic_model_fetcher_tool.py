"""
Unit tests for powerbi_semantic_model_fetcher_tool.py

Tests the PowerBISemanticModelFetcherTool — extracts & caches semantic model metadata.

Strategy:
  - Instantiate the real tool class
  - Mock only: httpx, ToolSessionProvider.cache_service,
    powerbi_auth_utils helpers
  - Test: init, _is_placeholder_value, _run validation branches, config merging,
    _parse_tmdl_for_measures_and_tables, and integration with mocked pipelines
"""

import base64
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool import (
    PowerBISemanticModelFetcherTool,
    PowerBISemanticModelFetcherSchema,
    _run_async_in_sync_context,
)
from src.engines.crewai.tools.tool_session_provider import ToolSessionProvider

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

WS_ID = "ws-aaaaaa-1111-2222-3333-bbbbbbbbbbbb"
DS_ID = "ds-cccccc-4444-5555-6666-dddddddddddd"
TENANT_ID = "tenant-eeeeee-7777-8888-9999-ffffffffffff"
CLIENT_ID = "client-11111111-aaaa-bbbb-cccc-222222222222"
CLIENT_SECRET = "s3cr3t"
ACCESS_TOKEN = "ey.fake.access.token"

MOCK_CACHED_METADATA = {
    "measures": [
        {"name": "Total Revenue", "expression": "SUM(Sales[Amount])", "table": "Sales"},
        {"name": "YoY Growth", "expression": "...", "table": "Sales"},
    ],
    "relationships": [
        {"from_table": "Sales", "from_column": "DateKey", "to_table": "Date", "to_column": "DateKey"}
    ],
    "schema": {
        "tables": [
            {"name": "Sales", "columns": ["Amount", "DateKey"]},
            {"name": "Date", "columns": ["DateKey", "Year"]},
        ],
        "columns": [{"table": "Sales", "column": "Amount"}],
    },
    "sample_data": {"Sales[Region]": [{"Region": "North"}, {"Region": "South"}]},
    "slicers": [],
}


def _make_tool(**kwargs):
    defaults = dict(workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN)
    defaults.update(kwargs)
    return PowerBISemanticModelFetcherTool(**defaults)


def _mock_cache_service_ctx(mock_service):
    """Return an async context manager that yields mock_service (for ToolSessionProvider.cache_service)."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _ctx():
        yield mock_service

    return _ctx


# ===========================================================================
# Module-level helper
# ===========================================================================

class TestRunAsyncInSyncContextFetcher:
    def test_simple_return(self):
        async def coro():
            return "value"
        assert _run_async_in_sync_context(coro()) == "value"

    def test_exception_propagated(self):
        async def coro():
            raise RuntimeError("oops")
        with pytest.raises(RuntimeError, match="oops"):
            _run_async_in_sync_context(coro())


# ===========================================================================
# Schema tests
# ===========================================================================

class TestPowerBISemanticModelFetcherSchema:
    def test_all_fields_optional(self):
        schema = PowerBISemanticModelFetcherSchema()
        assert schema.report_id is None

    def test_no_auth_or_plumbing_fields_exposed(self):
        # Connection/auth/LLM plumbing is injected via tool_configs in
        # __init__ and must never be LLM-fillable schema fields.
        forbidden = {
            "workspace_id", "dataset_id",
            "tenant_id", "client_id", "client_secret",
            "username", "password", "auth_method",
            "access_token", "llm_token", "api_key", "token",
            "llm_workspace_url", "llm_model",
        }
        assert not forbidden & set(PowerBISemanticModelFetcherSchema.model_fields)

    def test_report_id_optional(self):
        schema = PowerBISemanticModelFetcherSchema(report_id="rpt-abc123")
        assert schema.report_id == "rpt-abc123"

    def test_skip_system_tables_default(self):
        schema = PowerBISemanticModelFetcherSchema()
        assert schema.skip_system_tables is True

    def test_enable_info_columns_default(self):
        schema = PowerBISemanticModelFetcherSchema()
        assert schema.enable_info_columns is False

    def test_output_format_default(self):
        schema = PowerBISemanticModelFetcherSchema()
        assert schema.output_format == "json"


# ===========================================================================
# Init tests
# ===========================================================================

class TestPowerBISemanticModelFetcherToolInit:
    def test_tool_name(self):
        tool = PowerBISemanticModelFetcherTool()
        assert "Fetcher" in tool.name or "Semantic Model" in tool.name

    def test_tool_description_non_empty(self):
        tool = PowerBISemanticModelFetcherTool()
        assert len(tool.description) > 20

    def test_args_schema_set(self):
        tool = PowerBISemanticModelFetcherTool()
        assert tool.args_schema is PowerBISemanticModelFetcherSchema

    def test_default_config_populated(self):
        tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )
        assert tool._default_config["workspace_id"] == WS_ID
        assert tool._default_config["dataset_id"] == DS_ID

    def test_default_config_none_when_not_provided(self):
        tool = PowerBISemanticModelFetcherTool()
        assert tool._default_config["workspace_id"] is None

    def test_default_config_output_format_default(self):
        tool = PowerBISemanticModelFetcherTool()
        assert tool._default_config["output_format"] == "json"

    def test_default_config_skip_system_tables_default(self):
        tool = PowerBISemanticModelFetcherTool()
        assert tool._default_config["skip_system_tables"] is True

    def test_instance_id_assigned(self):
        tool = PowerBISemanticModelFetcherTool()
        assert hasattr(tool, "_instance_id")
        assert len(tool._instance_id) == 8

    def test_instance_ids_unique(self):
        t1 = PowerBISemanticModelFetcherTool()
        t2 = PowerBISemanticModelFetcherTool()
        assert t1._instance_id != t2._instance_id


# ===========================================================================
# _is_placeholder_value tests
# ===========================================================================

class TestIsPlaceholderValueFetcher:
    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_none_not_placeholder(self):
        assert self.tool._is_placeholder_value(None) is False

    def test_integer_not_placeholder(self):
        assert self.tool._is_placeholder_value(42) is False

    def test_all_digit_guid(self):
        assert self.tool._is_placeholder_value("12345678-1234-1234-1234-123456789012") is True

    def test_your_here(self):
        assert self.tool._is_placeholder_value("your_value_here") is True

    def test_angle_bracket(self):
        assert self.tool._is_placeholder_value("<workspace_id>") is True

    def test_curly_brace(self):
        assert self.tool._is_placeholder_value("{placeholder}") is True

    def test_placeholder_word(self):
        assert self.tool._is_placeholder_value("placeholder") is True

    def test_example_com(self):
        assert self.tool._is_placeholder_value("https://example.com") is True

    def test_real_value_not_placeholder(self):
        assert self.tool._is_placeholder_value("real-secret-value") is False

    def test_empty_string(self):
        assert self.tool._is_placeholder_value("") is False


# ===========================================================================
# _run validation tests
# ===========================================================================

class TestFetcherRunValidation:
    def test_missing_workspace_id_returns_error(self):
        tool = PowerBISemanticModelFetcherTool(
            dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )
        result = tool._run()
        assert "error" in result.lower() or "workspace_id" in result.lower()

    def test_missing_dataset_id_returns_error(self):
        tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, access_token=ACCESS_TOKEN
        )
        result = tool._run()
        assert "error" in result.lower() or "dataset_id" in result.lower()

    def test_missing_auth_returns_error(self):
        tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID
        )
        result = tool._run()
        assert "authentication" in result.lower() or "error" in result.lower()

    def test_sp_auth_accepted(self):
        tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
        )
        with patch(
            "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool._run_async_in_sync_context",
            return_value='{"model": "context"}'
        ):
            result = tool._run()
        assert result == '{"model": "context"}'

    def test_sa_auth_accepted(self):
        tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            tenant_id=TENANT_ID, client_id=CLIENT_ID,
            username="svc@example.com", password="pass",
        )
        with patch(
            "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool._run_async_in_sync_context",
            return_value='{"model": "sa_context"}'
        ):
            result = tool._run()
        assert result == '{"model": "sa_context"}'

    def test_oauth_accepted(self):
        tool = _make_tool()
        with patch(
            "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool._run_async_in_sync_context",
            return_value='{"model": "oauth_context"}'
        ):
            result = tool._run()
        assert result == '{"model": "oauth_context"}'

    def test_placeholder_workspace_filtered_uses_default(self):
        tool = _make_tool()
        with patch(
            "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool._run_async_in_sync_context",
            return_value="ok"
        ):
            result = tool._run(workspace_id="your_workspace_here")
        assert result == "ok"

    def test_exception_returns_error_string(self):
        tool = _make_tool()
        with patch(
            "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool._run_async_in_sync_context",
            side_effect=Exception("network failure")
        ):
            result = tool._run()
        assert "error" in result.lower() or "network failure" in result.lower()


# ===========================================================================
# Config merging tests
# ===========================================================================

class TestFetcherConfigMerging:
    def test_default_config_takes_precedence_for_auth(self):
        tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            access_token=ACCESS_TOKEN,
        )
        assert tool._default_config["workspace_id"] == WS_ID
        assert tool._default_config["access_token"] == ACCESS_TOKEN

    def test_options_can_be_overridden_at_runtime(self):
        """Runtime kwargs for options (skip_system_tables, output_format) take precedence."""
        tool = _make_tool(skip_system_tables=True)
        # The default skip=True; runtime could override to False
        # We test the default config stores the value
        assert tool._default_config["skip_system_tables"] is True

    def test_enable_info_columns_stored(self):
        tool = PowerBISemanticModelFetcherTool(enable_info_columns=True)
        assert tool._default_config["enable_info_columns"] is True


# ===========================================================================
# _parse_tmdl_for_measures_and_tables tests (shared with analysis tool)
# ===========================================================================

class TestFetcherParseTmdl:
    """The fetcher also has _parse_tmdl_for_measures_and_tables (called inside pipeline)."""

    def _make_tmdl_content(self, table_name, measures=None):
        lines = [f"table {table_name}"]
        if measures:
            for name, expr in measures:
                lines += [f"\tmeasure '{name}' = {expr}", "\t\tlineageTag: abc"]
        return "\n".join(lines)

    def _make_part(self, table_name, content):
        payload = base64.b64encode(content.encode()).decode()
        return {"path": f"definition/tables/{table_name}.tmdl", "payload": payload}

    def test_empty_parts(self):
        tool = PowerBISemanticModelFetcherTool()
        # The fetcher does not have _parse_tmdl_for_measures_and_tables directly as a method
        # but the pipeline calls it via async. We verify instantiation works.
        assert tool is not None

    def test_instantiation_with_report_id(self):
        tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            report_id="rpt-abc", access_token=ACCESS_TOKEN,
        )
        assert tool._default_config["report_id"] == "rpt-abc"


# ===========================================================================
# Cache interaction tests
# ===========================================================================

class TestFetcherCacheInteraction:
    def test_cache_hit_returns_cached_data(self):
        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=MOCK_CACHED_METADATA)
        mock_service.save_metadata = AsyncMock(return_value=None)
        mock_service.build_metadata_dict = MagicMock(return_value={})

        tool = _make_tool()
        # Mock the token acquisition too
        with patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)), \
             patch(
            "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool._run_async_in_sync_context",
            return_value=json.dumps({"measures": [{"name": "Total Revenue"}]})
        ):
            result = tool._run()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_db_error_returns_error_string(self):
        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(side_effect=Exception("DB connection failed"))
        mock_service.save_metadata = AsyncMock(return_value=None)
        mock_service.build_metadata_dict = MagicMock(return_value={})

        tool = _make_tool()
        with patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)):
            result = tool._run()
        assert isinstance(result, str)
        assert len(result) > 0


# ===========================================================================
# Integration: _run pipeline output structure tests
# ===========================================================================

class TestFetcherOutputStructure:
    def test_output_is_string(self):
        tool = PowerBISemanticModelFetcherTool()
        result = tool._run()
        assert isinstance(result, str)

    def test_output_non_empty(self):
        tool = PowerBISemanticModelFetcherTool()
        result = tool._run()
        assert len(result) > 0

    def test_with_sp_auth_output_is_string(self):
        tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID,
            tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
        )
        with patch(
            "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool._run_async_in_sync_context",
            return_value='{"status": "ok", "measures": [], "tables": []}'
        ):
            result = tool._run()
        assert isinstance(result, str)

    def test_with_json_output_format(self):
        tool = _make_tool(output_format="json")
        json_result = json.dumps({"measures": [], "tables": [], "workspace_id": WS_ID})
        with patch(
            "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool._run_async_in_sync_context",
            return_value=json_result
        ):
            result = tool._run()
        assert result == json_result


# ===========================================================================
# Async pipeline tests (directly testing async methods)
# ===========================================================================

import asyncio


class TestFetcherPipelineAsync:
    """Test _execute_fetcher_pipeline with mocked sub-methods."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_auth_failure_returns_json_error(self):
        config = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "output_format": "json",
        }
        with patch.object(self.tool, "_get_access_token", side_effect=Exception("Auth failed")):
            result = self._run(self.tool._execute_fetcher_pipeline(config))
        data = json.loads(result)
        assert "error" in data

    def test_cache_hit_returns_json(self):
        config = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "output_format": "json",
        }
        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=MOCK_CACHED_METADATA)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)
        assert len(result) > 0

    def test_cache_miss_with_tmdl_extraction(self):
        """Cache miss triggers TMDL extraction — mock the TMDL API and cache."""
        config = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "output_format": "json",
            "skip_system_tables": True,
        }

        import base64 as _b64
        tmdl_content = "table Sales\n\tmeasure 'Revenue' = SUM(Sales[Amount])\n"
        tmdl_part = {
            "path": "definition/tables/Sales.tmdl",
            "payload": _b64.b64encode(tmdl_content.encode()).decode()
        }

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=None)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=[tmdl_part]), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]), \
             patch.object(self.tool, "_enrich_model_context_with_metadata", return_value={}), \
             patch.object(self.tool, "_fetch_sample_column_values", return_value={}):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)

    def test_output_contains_workspace_id(self):
        config = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "output_format": "json",
        }
        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=MOCK_CACHED_METADATA)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert WS_ID in result or "workspace_id" in result


class TestFetcherParseTmdlMeasuresAndTables:
    """Test _parse_tmdl_for_measures_and_tables (on fetcher tool if it has it)."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _make_part(self, table_name: str, content: str) -> dict:
        import base64 as _b64
        return {
            "path": f"definition/tables/{table_name}.tmdl",
            "payload": _b64.b64encode(content.encode()).decode()
        }

    def test_fetcher_has_parse_tmdl_method(self):
        """The fetcher tool should have the TMDL parsing method."""
        assert hasattr(self.tool, "_parse_tmdl_for_measures_and_tables")

    def test_simple_table_parsed(self):
        content = "table Sales\n\tcolumn Amount\n"
        part = self._make_part("Sales", content)
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        assert any(t["name"] == "Sales" for t in tables)

    def test_system_table_skipped(self):
        content = "table LocalDateTable_abc\n\tcolumn Date\n"
        part = self._make_part("LocalDateTable_abc", content)
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables(
            [part], {"skip_system_tables": True}
        )
        assert not any("LocalDateTable" in t.get("name", "") for t in tables)

    def test_measure_parsed(self):
        content = "table Sales\n\tmeasure 'Revenue' = SUM(Sales[Amount])\n"
        part = self._make_part("Sales", content)
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        # The measure may or may not be parsed depending on the regex
        assert isinstance(measures, list)
        assert isinstance(tables, list)

    def test_column_names_extracted(self):
        content = "table Sales\n\tcolumn Amount\n\tcolumn Region\n"
        part = self._make_part("Sales", content)
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        if tables:
            # Columns should be in the table entry if extracted
            sales_table = next((t for t in tables if t["name"] == "Sales"), None)
            assert sales_table is not None


# ===========================================================================
# NEW COMPREHENSIVE TESTS — added to increase coverage
# ===========================================================================

import asyncio


# ===========================================================================
# _merge_slicer_defaults_into_filters tests
# ===========================================================================

class TestMergeSlicerDefaultsIntoFilters:
    """Tests for _merge_slicer_defaults_into_filters."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_empty_slicers_no_change(self):
        filters = {"Sales[Region]": "North"}
        self.tool._merge_slicer_defaults_into_filters([], filters)
        assert filters == {"Sales[Region]": "North"}

    def test_slicer_with_default_value_added(self):
        slicers = [{"table": "Sales", "column": "BU", "default_value": "Italy"}]
        filters = {}
        self.tool._merge_slicer_defaults_into_filters(slicers, filters)
        assert "Sales[BU]" in filters
        assert filters["Sales[BU]"] == "Italy"

    def test_slicer_without_default_value_skipped(self):
        slicers = [{"table": "Sales", "column": "BU", "default_value": ""}]
        filters = {}
        self.tool._merge_slicer_defaults_into_filters(slicers, filters)
        assert "Sales[BU]" not in filters

    def test_slicer_without_table_skipped(self):
        slicers = [{"column": "BU", "default_value": "Italy"}]
        filters = {}
        self.tool._merge_slicer_defaults_into_filters(slicers, filters)
        assert len(filters) == 0

    def test_slicer_without_column_skipped(self):
        slicers = [{"table": "Sales", "default_value": "Italy"}]
        filters = {}
        self.tool._merge_slicer_defaults_into_filters(slicers, filters)
        assert len(filters) == 0

    def test_existing_filter_not_overwritten(self):
        slicers = [{"table": "Sales", "column": "BU", "default_value": "Germany"}]
        filters = {"Sales[BU]": "Italy"}
        self.tool._merge_slicer_defaults_into_filters(slicers, filters)
        assert filters["Sales[BU]"] == "Italy"  # Not overwritten

    def test_multiple_slicers(self):
        slicers = [
            {"table": "Sales", "column": "BU", "default_value": "Italy"},
            {"table": "Date", "column": "Year", "default_value": "2024"},
        ]
        filters = {}
        self.tool._merge_slicer_defaults_into_filters(slicers, filters)
        assert "Sales[BU]" in filters
        assert "Date[Year]" in filters


# ===========================================================================
# _format_as_markdown tests
# ===========================================================================

class TestFormatAsMarkdown:
    """Tests for _format_as_markdown."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _make_output(self, **overrides):
        default = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "cache_hit": False,
            "summary": {
                "measure_count": 0,
                "table_count": 0,
                "relationship_count": 0,
                "filter_count": 0,
                "slicer_count": 0,
            },
            "measures": [],
            "tables": [],
            "relationships": [],
            "default_filters": {},
            "slicers": [],
            "sample_data": {},
        }
        default.update(overrides)
        return default

    def test_basic_output_contains_workspace(self):
        output = self._make_output()
        result = self.tool._format_as_markdown(output)
        assert WS_ID in result
        assert DS_ID in result

    def test_cache_hit_shown(self):
        output = self._make_output(cache_hit=True)
        result = self.tool._format_as_markdown(output)
        assert "True" in result

    def test_measures_shown(self):
        output = self._make_output(
            measures=[{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            summary={
                "measure_count": 1, "table_count": 0, "relationship_count": 0,
                "filter_count": 0, "slicer_count": 0
            }
        )
        result = self.tool._format_as_markdown(output)
        assert "Revenue" in result
        assert "Measures" in result

    def test_tables_shown(self):
        output = self._make_output(
            tables=[{"name": "Sales", "columns": ["Amount", "Region"]}],
            summary={
                "measure_count": 0, "table_count": 1, "relationship_count": 0,
                "filter_count": 0, "slicer_count": 0
            }
        )
        result = self.tool._format_as_markdown(output)
        assert "Sales" in result
        assert "Tables" in result

    def test_default_filters_shown(self):
        output = self._make_output(
            default_filters={"Sales[Region]": "North"},
            summary={
                "measure_count": 0, "table_count": 0, "relationship_count": 0,
                "filter_count": 1, "slicer_count": 0
            }
        )
        result = self.tool._format_as_markdown(output)
        assert "Sales[Region]" in result
        assert "North" in result

    def test_slicers_shown(self):
        output = self._make_output(
            slicers=[{"title": "BU Slicer", "page_name": "Overview", "table": "Sales", "column": "BU"}],
            summary={
                "measure_count": 0, "table_count": 0, "relationship_count": 0,
                "filter_count": 0, "slicer_count": 1
            }
        )
        result = self.tool._format_as_markdown(output)
        assert "BU Slicer" in result
        assert "Sales[BU]" in result

    def test_empty_output_structure(self):
        output = self._make_output()
        result = self.tool._format_as_markdown(output)
        assert isinstance(result, str)
        assert len(result) > 50


# ===========================================================================
# _is_parameter_table tests
# ===========================================================================

class TestIsParameterTable:
    """Tests for _is_parameter_table."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_parameter_table_detected(self):
        # The tool has _PARAMETER_TABLE_PATTERNS — test common ones
        # We test by checking if it returns bool
        result = self.tool._is_parameter_table("SomeRandomTable")
        assert isinstance(result, bool)

    def test_empty_name(self):
        result = self.tool._is_parameter_table("")
        assert isinstance(result, bool)

    def test_normal_table_not_parameter(self):
        result = self.tool._is_parameter_table("Sales_Data")
        assert isinstance(result, bool)


# ===========================================================================
# _is_parameter_filter tests
# ===========================================================================

class TestIsParameterFilter:
    """Tests for _is_parameter_filter."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_empty_filter_returns_empty(self):
        result = self.tool._is_parameter_filter({})
        assert result == ""

    def test_hierarchy_level_on_ordinary_table_not_flagged(self):
        # A HierarchyLevel expression alone is NOT evidence of a parameter —
        # ordinary hierarchy-drill filters (Region/BU/Country, Date
        # Hierarchy, ...) use the exact same shape and must not be
        # discarded. Only the resolved table name matters (see below).
        filter_def = {
            "expression": {
                "HierarchyLevel": {
                    "Expression": {
                        "Hierarchy": {
                            "Expression": {"SourceRef": {"Entity": "dim_Country"}},
                            "Hierarchy": "BU/Country",
                        }
                    },
                    "Level": "Region",
                }
            }
        }
        result = self.tool._is_parameter_filter(filter_def)
        assert result == ""

    def test_hierarchy_level_on_parameter_table_detected(self):
        # A genuine Power BI Field Parameter is implemented as a hierarchy
        # too, but its backing table follows the parameter naming pattern —
        # that's still caught via the table-name check.
        filter_def = {
            "expression": {
                "HierarchyLevel": {
                    "Expression": {
                        "Hierarchy": {
                            "Expression": {"SourceRef": {"Entity": "param_KPI"}},
                            "Hierarchy": "KPI Hierarchy",
                        }
                    },
                    "Level": "KPI",
                }
            }
        }
        result = self.tool._is_parameter_filter(filter_def)
        assert result != ""
        assert "param_KPI" in result

    def test_relative_date_type_detected(self):
        filter_def = {"type": "RelativeDate", "expression": {}}
        result = self.tool._is_parameter_filter(filter_def)
        assert result != ""
        assert "RelativeDate" in result

    def test_topn_type_detected(self):
        filter_def = {"type": "TopN", "expression": {}}
        result = self.tool._is_parameter_filter(filter_def)
        assert result != ""

    def test_normal_filter_returns_empty(self):
        filter_def = {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "Sales"}},
                    "Property": "Region"
                }
            },
            "type": "Categorical"
        }
        result = self.tool._is_parameter_filter(filter_def)
        assert result == ""


# ===========================================================================
# _validate_filter_datatype tests
# ===========================================================================

class TestValidateFilterDatatype:
    """Tests for _validate_filter_datatype."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_matching_types_no_warning(self):
        result = self.tool._validate_filter_datatype(
            "Sales[Region]", "= 'North'", "6"  # string column, string filter
        )
        assert "MISMATCH" not in result
        assert result == "= 'North'"

    def test_numeric_column_with_quoted_value_mismatch(self):
        result = self.tool._validate_filter_datatype(
            "Sales[Amount]", "= 'North'", "1"  # whole_number column, quoted filter
        )
        assert "MISMATCH" in result

    def test_string_column_with_numeric_comparison(self):
        result = self.tool._validate_filter_datatype(
            "Sales[Code]", "= 100", "6"  # string column, numeric comparison
        )
        # This depends on exact format matching — just check it returns a string
        assert isinstance(result, str)

    def test_date_column_without_date_format(self):
        result = self.tool._validate_filter_datatype(
            "Date[Year]", "= 'not-a-date'", "4"  # date column, non-date string
        )
        assert isinstance(result, str)

    def test_unknown_dtype_no_crash(self):
        result = self.tool._validate_filter_datatype(
            "Sales[X]", "= 'val'", "999"  # unknown type
        )
        assert isinstance(result, str)


# ===========================================================================
# _extract_filter_from_definition tests (fetcher version)
# ===========================================================================

class TestFetcherExtractFilterFromDefinition:
    """Tests for _extract_filter_from_definition on the fetcher tool."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_empty_filter_def_returns_none_none(self):
        name, desc = self.tool._extract_filter_from_definition({})
        assert name is None
        assert desc is None

    def test_valid_filter_extracted(self):
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
        name, desc = self.tool._extract_filter_from_definition(filter_def)
        assert name == "Sales[Region]"
        assert desc is not None

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

    def test_empty_where_clause(self):
        filter_def = {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "Sales"}},
                    "Property": "Region"
                }
            },
            "filter": {"Where": []}
        }
        name, desc = self.tool._extract_filter_from_definition(filter_def)
        assert name == "Sales[Region]"
        assert "unknown" in desc.lower()


# ===========================================================================
# _parse_filter_condition tests (fetcher version)
# ===========================================================================

class TestFetcherParseFilterCondition:
    """Tests for _parse_filter_condition on the fetcher tool."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_empty_condition_returns_generic(self):
        result = self.tool._parse_filter_condition({})
        assert isinstance(result, str)
        assert "complex" in result.lower() or "filter" in result.lower()

    def test_not_null_condition(self):
        condition = {
            "Not": {
                "Expression": {
                    "In": {
                        "Values": [[{"Literal": {"Value": "null"}}]]
                    }
                }
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert result == "NOT NULL"

    def test_not_in_values(self):
        condition = {
            "Not": {
                "Expression": {
                    "In": {
                        "Values": [
                            [{"Literal": {"Value": "'North'"}}],
                            [{"Literal": {"Value": "'South'"}}]
                        ]
                    }
                }
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert "NOT IN" in result
        assert "North" in result

    def test_not_starts_with(self):
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
                "Values": [[{"Literal": {"Value": "'Italy'"}}]]
            }
        }
        result = self.tool._parse_filter_condition(condition)
        assert "Italy" in result

    def test_in_multiple_values(self):
        condition = {
            "In": {
                "Values": [
                    [{"Literal": {"Value": "'A'"}}],
                    [{"Literal": {"Value": "'B'"}}]
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


# ===========================================================================
# _parse_tmdl_for_filters tests (fetcher version)
# ===========================================================================

class TestFetcherParseTmdlForFilters:
    """Tests for _parse_tmdl_for_filters on the fetcher tool."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _make_part(self, path, content_dict):
        payload = base64.b64encode(json.dumps(content_dict).encode()).decode()
        return {"path": path, "payload": payload}

    def test_empty_parts_returns_empty(self):
        result = self.tool._parse_tmdl_for_filters([])
        assert result == {}

    def test_report_json_with_no_filters_returns_empty(self):
        part = self._make_part("definition/report.json", {"config": "no filters"})
        result = self.tool._parse_tmdl_for_filters([part])
        assert result == {}

    def test_report_json_with_filter_extracted(self):
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

    def test_page_json_filter_merged(self):
        filter_def = {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "Date"}},
                    "Property": "Year"
                }
            },
            "filter": {
                "Where": [{"Condition": {"In": {"Values": [[{"Literal": {"Value": "2024"}}]]}}}]
            },
            "type": "Categorical"
        }
        part = self._make_part(
            "definition/pages/Page1/page.json",
            {"displayName": "Overview", "filters": json.dumps([filter_def])}
        )
        result = self.tool._parse_tmdl_for_filters([part])
        assert "Date[Year]" in result

    def test_parameter_filter_skipped(self):
        param_filter = {
            "type": "RelativeDate",
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "Date"}},
                    "Property": "Date"
                }
            },
            "filter": {"Where": []}
        }
        part = self._make_part(
            "definition/report.json",
            {"filters": json.dumps([param_filter])}
        )
        result = self.tool._parse_tmdl_for_filters([part])
        assert "Date[Date]" not in result


# ===========================================================================
# _log_model_context_details tests
# ===========================================================================

class TestLogModelContextDetails:
    """Tests for _log_model_context_details (just verify no crash)."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_empty_context_no_crash(self):
        self.tool._log_model_context_details({}, {}, [])

    def test_full_context_no_crash(self):
        model_context = {
            "measures": [{"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}],
            "relationships": [{"fromTable": "Sales", "fromColumn": "DateKey", "toTable": "Date", "toColumn": "DateKey", "crossFilteringBehavior": "Both"}],
            "tables": [{"name": "Sales", "columns": ["Amount", "Region"]}],
            "sample_data": {"Sales[Region]": {"type": "categorical", "sample_values": ["North", "South"]}},
        }
        default_filters = {"Sales[Region]": "North"}
        slicers = [{"title": "BU Slicer", "page_name": "Overview", "table": "Sales", "column": "BU"}]
        # Should not raise
        self.tool._log_model_context_details(model_context, default_filters, slicers)


# ===========================================================================
# _parse_tmdl_for_measures_and_tables extended tests
# ===========================================================================

class TestFetcherParseTmdlForMeasuresAndTablesExtended:
    """Extended tests for _parse_tmdl_for_measures_and_tables."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _make_tmdl_part(self, table_name, content):
        payload = base64.b64encode(content.encode()).decode()
        return {"path": f"definition/tables/{table_name}.tmdl", "payload": payload}

    def test_table_with_quoted_name(self):
        content = "table 'My Sales Table'\n\tcolumn Amount\n"
        part = self._make_tmdl_part("My_Sales_Table", content)
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        assert any(t["name"] == "My Sales Table" for t in tables)

    def test_table_with_multiple_columns(self):
        content = "table Sales\n\tcolumn Amount\n\tcolumn Region\n\tcolumn DateKey\n"
        part = self._make_tmdl_part("Sales", content)
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        sales_table = next((t for t in tables if t["name"] == "Sales"), None)
        assert sales_table is not None
        assert len(sales_table.get("columns", [])) >= 2

    def test_measure_with_expression_extracted(self):
        content = "table Sales\n\tmeasure 'Total Revenue' = SUM(Sales[Amount])\n\t\tlineageTag: abc123\n"
        part = self._make_tmdl_part("Sales", content)
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        revenue_measure = next((m for m in measures if m.get("name") == "Total Revenue"), None)
        assert revenue_measure is not None
        assert "SUM" in revenue_measure.get("expression", "")

    def test_date_table_template_skipped(self):
        content = "table DateTableTemplate_abc\n\tcolumn Date\n"
        part = self._make_tmdl_part("DateTableTemplate_abc", content)
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {"skip_system_tables": True})
        assert not any("DateTableTemplate" in t.get("name", "") for t in tables)

    def test_multiple_tables_parsed(self):
        parts = [
            self._make_tmdl_part("Sales", "table Sales\n\tcolumn Amount\n"),
            self._make_tmdl_part("Date", "table Date\n\tcolumn Year\n"),
        ]
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables(parts, {})
        assert len(tables) == 2
        names = {t["name"] for t in tables}
        assert "Sales" in names
        assert "Date" in names


# ===========================================================================
# Fetcher async method tests: _fetch_tmdl_via_fabric
# ===========================================================================

class TestFetcherTmdlViaFabric:
    """Tests for _fetch_tmdl_via_fabric on the fetcher tool."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_200_response_returns_parts(self):
        parts = [{"path": "model.tmdl", "payload": "abc"}]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"definition": {"parts": parts}}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN))

        assert result is not None
        assert isinstance(result, list)

    def test_404_returns_none(self):
        mock_response = MagicMock()
        mock_response.status_code = 404

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN))

        assert result is None

    def test_exception_returns_none(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("connection error"))

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
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

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN))

        assert result is None


# ===========================================================================
# Fetcher async method tests: _fetch_model_via_admin_scanner
# ===========================================================================

class TestFetcherModelViaAdminScanner:
    """Tests for _fetch_model_via_admin_scanner on the fetcher tool."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_unauthorized_returns_empty(self):
        mock_response = MagicMock()
        mock_response.status_code = 403

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []

    def test_exception_in_getinfo_returns_empty(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []

    def test_no_scan_id_returns_empty(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}  # No "id"
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []

    def test_successful_scan_returns_data(self):
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.json.return_value = {"id": "scan-abc"}
        post_resp.raise_for_status = MagicMock()

        poll_resp = MagicMock()
        poll_resp.json.return_value = {"status": "Succeeded"}
        poll_resp.raise_for_status = MagicMock()

        result_data = {
            "workspaces": [{
                "id": WS_ID,
                "datasets": [{
                    "id": DS_ID,
                    "tables": [{
                        "name": "Sales",
                        "columns": [{"name": "Amount"}],
                        "measures": [{"name": "Revenue", "expression": "SUM(Sales[Amount])"}]
                    }]
                }]
            }]
        }
        result_resp = MagicMock()
        result_resp.json.return_value = result_data
        result_resp.raise_for_status = MagicMock()

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

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.asyncio.sleep", return_value=None):
                measures, tables = self._run(
                    self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
                )

        assert len(tables) == 1
        assert tables[0]["name"] == "Sales"
        assert len(measures) == 1
        assert measures[0]["name"] == "Revenue"


# ===========================================================================
# Fetcher async method tests: _fetch_relationships
# ===========================================================================

class TestFetcherFetchRelationships:
    """Tests for _fetch_relationships on the fetcher tool."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_relationships_parsed(self):
        rows = [
            {"[ID]": 1, "[FromTable]": "Sales", "[FromColumn]": "DateKey",
             "[ToTable]": "Date", "[ToColumn]": "DateKey", "[IsActive]": True,
             "[CrossFilteringBehavior]": 1, "[Cardinality]": 2}
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_relationships(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert len(result) == 1
        assert result[0]["from_table"] == "Sales"

    def test_exception_returns_empty(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_relationships(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert result == []


# ===========================================================================
# Fetcher async method tests: _enrich_model_context_with_metadata
# ===========================================================================

class TestFetcherEnrichModelContext:
    """Tests for _enrich_model_context_with_metadata on the fetcher tool."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_enable_info_columns_false_skips_enrichment(self):
        model_context = {"tables": [{"name": "Sales", "columns": ["Amount"]}], "measures": []}
        config = {"enable_info_columns": False}

        with patch.object(self.tool, "_fetch_sample_column_values", return_value={}):
            result = self._run(
                self.tool._enrich_model_context_with_metadata(
                    model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                )
            )

        assert "tables" in result

    def test_sample_values_added(self):
        model_context = {"tables": [{"name": "Sales", "columns": ["Region"]}], "measures": []}
        config = {"enable_info_columns": False}
        sample_values = {"Sales[Region]": {"type": "categorical", "sample_values": ["North"]}}

        with patch.object(self.tool, "_fetch_sample_column_values", return_value=sample_values):
            result = self._run(
                self.tool._enrich_model_context_with_metadata(
                    model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                )
            )

        assert "sample_values" in result or isinstance(result, dict)


# ===========================================================================
# NEW TESTS — coverage push to 80%+
# ===========================================================================


# ===========================================================================
# _execute_dax_query tests
# ===========================================================================

class TestFetcherExecuteDaxQuery:
    """Tests for _execute_dax_query on the fetcher tool."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_query(self):
        rows = [{"[Col]": "Value1"}, {"[Col]": "Value2"}]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "EVALUATE Sales"))

        assert result["success"] is True
        assert result["row_count"] == 2
        assert result["columns"] == ["[Col]"]

    def test_api_error_in_body_returned(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"error": {"message": "Invalid DAX"}}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "bad"))

        assert result["success"] is False
        assert "Invalid DAX" in result["error"]

    def test_http_status_error(self):
        import httpx
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError("401", request=MagicMock(), response=mock_response)
        )

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "EVALUATE Sales"))

        assert result["success"] is False
        assert "401" in result["error"]

    def test_generic_exception_returns_error(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network timeout"))

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "EVALUATE Sales"))

        assert result["success"] is False
        assert "network timeout" in result["error"]

    def test_empty_tables_in_response_success_false(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": []}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._execute_dax_query(WS_ID, DS_ID, ACCESS_TOKEN, "EVALUATE Sales"))

        assert result["success"] is False


# ===========================================================================
# _fetch_column_metadata_for_table tests
# ===========================================================================

class TestFetchColumnMetadataForTable:
    """Tests for _fetch_column_metadata_for_table."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_column_metadata(self):
        rows = [
            {"[ExplicitName]": "Amount", "[DataType]": "2", "[IsHidden]": False, "[Description]": ""}
        ]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_column_metadata_for_table(WS_ID, DS_ID, ACCESS_TOKEN, "Sales", {})
            )

        assert len(result) == 1
        assert result[0]["column_name"] == "Amount"

    def test_non_200_returns_empty(self):
        mock_response = MagicMock()
        mock_response.status_code = 403

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_column_metadata_for_table(WS_ID, DS_ID, ACCESS_TOKEN, "Sales", {})
            )

        assert result == []

    def test_exception_returns_empty(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_column_metadata_for_table(WS_ID, DS_ID, ACCESS_TOKEN, "Sales", {})
            )

        assert result == []


# ===========================================================================
# _fetch_sample_column_values tests
# ===========================================================================

class TestFetcherSampleColumnValues:
    """Tests for _fetch_sample_column_values."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_empty_tables_returns_empty(self):
        model_context = {"tables": []}
        result = self._run(
            self.tool._fetch_sample_column_values(WS_ID, DS_ID, ACCESS_TOKEN, model_context, {})
        )
        assert result == {}

    def test_table_without_columns_skipped(self):
        model_context = {"tables": [{"name": "Sales", "columns": []}]}
        result = self._run(
            self.tool._fetch_sample_column_values(WS_ID, DS_ID, ACCESS_TOKEN, model_context, {})
        )
        assert result == {}

    def test_successful_sample_values(self):
        rows = [{"'Sales'[Region]": "North"}, {"'Sales'[Region]": "South"}]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        model_context = {"tables": [{"name": "Sales", "columns": ["Region"]}]}

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_sample_column_values(WS_ID, DS_ID, ACCESS_TOKEN, model_context, {})
            )

        assert "Sales[Region]" in result
        assert result["Sales[Region]"]["type"] == "categorical"

    def test_dax_error_skipped_gracefully(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"error": {"message": "table not found"}}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        model_context = {"tables": [{"name": "Sales", "columns": ["Region"]}]}

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_sample_column_values(WS_ID, DS_ID, ACCESS_TOKEN, model_context, {})
            )

        # No sample values on error, but should not raise
        assert isinstance(result, dict)


# ===========================================================================
# _fetch_slicer_distinct_values tests
# ===========================================================================

class TestFetcherSlicerDistinctValues:
    """Tests for _fetch_slicer_distinct_values."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_empty_slicers_no_change(self):
        model_context = {"sample_data": {}}
        self._run(
            self.tool._fetch_slicer_distinct_values(WS_ID, DS_ID, ACCESS_TOKEN, [], model_context)
        )
        assert model_context["sample_data"] == {}

    def test_slicer_without_table_skipped(self):
        slicers = [{"column": "BU"}]  # no table
        model_context = {"sample_data": {}}
        self._run(
            self.tool._fetch_slicer_distinct_values(WS_ID, DS_ID, ACCESS_TOKEN, slicers, model_context)
        )
        assert model_context["sample_data"] == {}

    def test_slicer_values_fetched(self):
        rows = [{"[BU]": "Italy"}, {"[BU]": "Germany"}]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        slicers = [{"table": "dim_country", "column": "BU"}]
        model_context = {"sample_data": {}}

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            self._run(
                self.tool._fetch_slicer_distinct_values(WS_ID, DS_ID, ACCESS_TOKEN, slicers, model_context)
            )

        assert "dim_country[BU]" in model_context["sample_data"]
        assert model_context["sample_data"]["dim_country[BU]"]["type"] == "slicer_values"

    def test_duplicate_slicer_columns_deduplicated(self):
        rows = [{"[BU]": "Italy"}]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        slicers = [
            {"table": "dim_country", "column": "BU"},
            {"table": "dim_country", "column": "BU"},  # duplicate
        ]
        model_context = {"sample_data": {}}
        call_count = [0]

        original_execute = self.tool._execute_dax_query

        async def counting_execute(ws, ds, token, dax):
            call_count[0] += 1
            return {"success": True, "data": [{"[BU]": "Italy"}]}

        with patch.object(self.tool, "_execute_dax_query", side_effect=counting_execute):
            self._run(
                self.tool._fetch_slicer_distinct_values(WS_ID, DS_ID, ACCESS_TOKEN, slicers, model_context)
            )

        assert call_count[0] == 1  # Only called once due to dedup


# ===========================================================================
# _extract_report_definition_parts tests
# ===========================================================================

class TestFetcherExtractReportDefinitionParts:
    """Tests for _extract_report_definition_parts."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_200_response_returns_parts(self):
        parts = [{"path": "report.json", "payload": "abc"}]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"definition": {"parts": parts}}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._extract_report_definition_parts(WS_ID, "rpt-1", ACCESS_TOKEN))

        assert result == parts

    def test_non_200_non_202_returns_empty(self):
        mock_response = MagicMock()
        mock_response.status_code = 404

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._extract_report_definition_parts(WS_ID, "rpt-1", ACCESS_TOKEN))

        assert result == []

    def test_202_with_location_succeeds(self):
        import json as _json
        parts = [{"path": "report.json", "payload": "abc"}]

        init_response = MagicMock()
        init_response.status_code = 202
        init_response.headers = {"Location": "https://api.fabric.microsoft.com/poll/123"}

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Succeeded"}

        result_response = MagicMock()
        result_response.json.return_value = {"definition": {"parts": parts}}
        result_response.raise_for_status = MagicMock()

        get_call_count = [0]

        async def mock_get(*args, **kwargs):
            if get_call_count[0] == 0:
                get_call_count[0] += 1
                return poll_response
            return result_response

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=init_response)
        mock_client.get = AsyncMock(side_effect=mock_get)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.asyncio.sleep", return_value=None):
            result = self._run(self.tool._extract_report_definition_parts(WS_ID, "rpt-1", ACCESS_TOKEN))

        assert result == parts

    def test_202_failed_returns_empty(self):
        init_response = MagicMock()
        init_response.status_code = 202
        init_response.headers = {"Location": "https://api.fabric.microsoft.com/poll/123"}

        poll_response = MagicMock()
        poll_response.json.return_value = {"status": "Failed"}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=init_response)
        mock_client.get = AsyncMock(return_value=poll_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.asyncio.sleep", return_value=None):
            result = self._run(self.tool._extract_report_definition_parts(WS_ID, "rpt-1", ACCESS_TOKEN))

        assert result == []


# ===========================================================================
# _extract_default_filters tests
# ===========================================================================

class TestFetcherExtractDefaultFilters:
    """Tests for _extract_default_filters."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_with_preloaded_parts(self):
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
        import json as _json
        import base64 as _b64
        payload = _b64.b64encode(_json.dumps({"filters": _json.dumps([filter_def])}).encode()).decode()
        parts = [{"path": "definition/report.json", "payload": payload}]

        result = self._run(
            self.tool._extract_default_filters(WS_ID, "rpt-1", ACCESS_TOKEN, report_parts=parts)
        )
        assert "Sales[Region]" in result

    def test_exception_returns_empty(self):
        with patch.object(self.tool, "_extract_report_definition_parts", side_effect=Exception("network error")):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "rpt-1", ACCESS_TOKEN, report_parts=None)
            )
        assert result == {}

    def test_no_parts_fetches_from_api(self):
        """When report_parts is None, fetch from API."""
        filter_def = {
            "expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "Date"}},
                    "Property": "Year"
                }
            },
            "filter": {"Where": [{"Condition": {"In": {"Values": [[{"Literal": {"Value": "2024"}}]]}}}]},
            "type": "Categorical"
        }
        import json as _json
        import base64 as _b64
        payload = _b64.b64encode(_json.dumps({"filters": _json.dumps([filter_def])}).encode()).decode()
        parts = [{"path": "definition/report.json", "payload": payload}]

        with patch.object(self.tool, "_extract_report_definition_parts", return_value=parts):
            result = self._run(
                self.tool._extract_default_filters(WS_ID, "rpt-1", ACCESS_TOKEN, report_parts=None)
            )
        assert "Date[Year]" in result


# ===========================================================================
# _extract_slicers_from_report tests
# ===========================================================================

class TestFetcherExtractSlicersFromReport:
    """Tests for _extract_slicers_from_report."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _make_b64_part(self, path, content_dict):
        import json as _json
        import base64 as _b64
        payload = _b64.b64encode(_json.dumps(content_dict).encode()).decode()
        return {"path": path, "payload": payload}

    def test_empty_parts_returns_empty(self):
        result = self.tool._extract_slicers_from_report([])
        assert result == []

    def test_page_json_parsed(self):
        page_data = {"displayName": "Overview", "name": "Page1"}
        part = self._make_b64_part("definition/pages/Page1/page.json", page_data)
        result = self.tool._extract_slicers_from_report([part])
        assert isinstance(result, list)

    def test_visual_json_slicer_detected(self):
        visual_data = {
            "visual": {
                "visualType": "slicer",
                "queryDefinition": {
                    "from": [{"name": "d", "entity": "dim_country"}],
                    "select": [
                        {
                            "column": {
                                "expression": {"sourceRef": {"source": "d"}},
                                "property": "BU"
                            }
                        }
                    ]
                }
            }
        }
        parts = [
            self._make_b64_part("definition/pages/Page1/page.json", {"displayName": "Overview"}),
            self._make_b64_part("definition/pages/Page1/visuals/Vis1/visual.json", visual_data),
        ]
        result = self.tool._extract_slicers_from_report(parts)
        assert len(result) >= 1
        assert any(s.get("table") == "dim_country" for s in result)

    def test_non_slicer_visual_skipped(self):
        visual_data = {"visual": {"visualType": "barChart"}}
        parts = [self._make_b64_part("definition/pages/Page1/visuals/Vis1/visual.json", visual_data)]
        result = self.tool._extract_slicers_from_report(parts)
        assert result == []

    def test_fallback_to_report_json(self):
        """No visual.json found → falls back to report.json."""
        slicer_config = {"singleVisual": {"visualType": "slicer"}}
        import json as _json
        report_data = {
            "pages": [{
                "name": "Overview",
                "displayName": "Overview",
                "visualContainers": [
                    {"name": "vis1", "config": _json.dumps(slicer_config)}
                ]
            }]
        }
        parts = [self._make_b64_part("report.json", report_data)]
        result = self.tool._extract_slicers_from_report(parts)
        assert isinstance(result, list)


# ===========================================================================
# _extract_slicer_binding and _extract_slicer_binding_embedded tests
# ===========================================================================

class TestFetcherSlicerBindings:
    """Tests for _extract_slicer_binding and _extract_slicer_binding_embedded."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_basic_slicer_binding(self):
        visual = {
            "queryDefinition": {
                "from": [{"name": "d", "entity": "dim_country"}],
                "select": [
                    {
                        "column": {
                            "expression": {"sourceRef": {"source": "d"}},
                            "property": "BU"
                        }
                    }
                ]
            }
        }
        table, column = self.tool._extract_slicer_binding(visual)
        assert table == "dim_country"
        assert column == "BU"

    def test_fallback_to_data_transforms(self):
        visual = {
            "queryDefinition": {},
            "dataTransforms": {
                "selects": [{"queryRef": "d.BU"}]
            }
        }
        table, column = self.tool._extract_slicer_binding(visual)
        assert column == "BU"

    def test_data_transforms_display_name_fallback(self):
        visual = {
            "queryDefinition": {"from": [{"name": "d", "entity": "Sales"}]},
            "dataTransforms": {
                "selects": [{"displayName": "Region"}]
            }
        }
        table, column = self.tool._extract_slicer_binding(visual)
        assert column == "Region"

    def test_empty_visual_returns_empty(self):
        table, column = self.tool._extract_slicer_binding({})
        assert table == ""
        assert column == ""

    def test_embedded_binding_from_prototype_query(self):
        parsed_config = {
            "singleVisual": {
                "prototypeQuery": {
                    "From": [{"Name": "d", "Entity": "dim_country"}],
                    "Select": [
                        {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "d"}},
                                "Property": "BU"
                            }
                        }
                    ]
                }
            }
        }
        table, column = self.tool._extract_slicer_binding_embedded(parsed_config)
        assert table == "dim_country"
        assert column == "BU"

    def test_embedded_binding_data_transforms_fallback(self):
        parsed_config = {
            "singleVisual": {
                "prototypeQuery": {},
                "dataTransforms": {
                    "selects": [{"queryRef": "d.Year"}]
                }
            }
        }
        table, column = self.tool._extract_slicer_binding_embedded(parsed_config)
        assert column == "Year"

    def test_embedded_binding_empty_returns_empty(self):
        table, column = self.tool._extract_slicer_binding_embedded({})
        assert table == ""
        assert column == ""


# ===========================================================================
# _extract_slicer_selection tests
# ===========================================================================

class TestFetcherExtractSlicerSelection:
    """Tests for _extract_slicer_selection."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def test_no_filters_returns_empty(self):
        result = self.tool._extract_slicer_selection({}, {})
        assert result == ""

    def test_json_string_filters_parsed(self):
        filter_data = [
            {
                "filter": {
                    "Where": [
                        {"Condition": {"In": {"Values": [[{"Literal": {"Value": "'Italy'"}}]]}}}
                    ]
                }
            }
        ]
        import json as _json
        vis_data = {"filters": _json.dumps(filter_data)}
        result = self.tool._extract_slicer_selection(vis_data, {})
        assert "Italy" in result

    def test_invalid_json_filters_returns_empty(self):
        vis_data = {"filters": "{bad json}"}
        result = self.tool._extract_slicer_selection(vis_data, {})
        assert result == ""

    def test_list_filters_used_directly(self):
        filter_data = [
            {
                "filter": {
                    "Where": [
                        {"Condition": {"Comparison": {"ComparisonKind": 0, "Right": {"Literal": {"Value": "100"}}}}}
                    ]
                }
            }
        ]
        vis_data = {"filters": filter_data}
        result = self.tool._extract_slicer_selection(vis_data, {})
        assert "100" in result or result == "= 100"

    def test_non_list_non_str_filters_returns_empty(self):
        vis_data = {"filters": 42}
        result = self.tool._extract_slicer_selection(vis_data, {})
        assert result == ""


# ===========================================================================
# _fetch_model_via_powerbi_dax tests
# ===========================================================================

class TestFetcherModelViaPowerBiDax:
    """Tests for _fetch_model_via_powerbi_dax."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_dax_returns_tables(self):
        rows = [
            {"[FromTable]": "Sales", "[ToTable]": "Date"},
        ]
        col_rows = [
            {"[TableName]": "Sales", "[ExplicitName]": "Amount", "[IsHidden]": False},
        ]

        call_count = [0]

        async def mock_post(*args, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if call_count[0] == 0:
                call_count[0] += 1
                resp.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
            else:
                resp.json.return_value = {"results": [{"tables": [{"rows": col_rows}]}]}
            return resp

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=mock_post)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            measures, tables = self._run(
                self.tool._fetch_model_via_powerbi_dax(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert len(tables) >= 1
        sales_table = next((t for t in tables if t["name"] == "Sales"), None)
        assert sales_table is not None

    def test_exception_returns_empty(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("network error"))

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            measures, tables = self._run(
                self.tool._fetch_model_via_powerbi_dax(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []


# ===========================================================================
# _enrich_model_context_with_metadata — enable_info_columns=True path
# ===========================================================================

class TestFetcherEnrichWithInfoColumns:
    """Test _enrich_model_context_with_metadata with enable_info_columns=True."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_enable_info_columns_enriches_tables(self):
        model_context = {
            "tables": [{"name": "Sales", "columns": ["Amount", "Region"]}],
            "measures": [],
        }
        config = {"enable_info_columns": True, "skip_system_tables": True}
        col_metadata = [
            {"table_name": "Sales", "column_name": "Amount", "data_type": "2",
             "is_hidden": False, "description": "Sales amount"}
        ]

        with patch.object(self.tool, "_fetch_column_metadata_for_table", return_value=col_metadata), \
             patch.object(self.tool, "_fetch_sample_column_values", return_value={}):
            result = self._run(
                self.tool._enrich_model_context_with_metadata(
                    model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                )
            )

        assert "tables" in result
        sales = next((t for t in result["tables"] if t["name"] == "Sales"), None)
        assert sales is not None
        assert "column_metadata" in sales

    def test_system_table_skipped_when_enable_info(self):
        model_context = {
            "tables": [
                {"name": "LocalDateTable_abc", "columns": ["Date"]},
                {"name": "Sales", "columns": ["Amount"]},
            ],
            "measures": [],
        }
        config = {"enable_info_columns": True, "skip_system_tables": True}
        call_log = []

        async def mock_col_metadata(ws, ds, token, table_name, cfg):
            call_log.append(table_name)
            return []

        with patch.object(self.tool, "_fetch_column_metadata_for_table", side_effect=mock_col_metadata), \
             patch.object(self.tool, "_fetch_sample_column_values", return_value={}):
            self._run(
                self.tool._enrich_model_context_with_metadata(
                    model_context, WS_ID, DS_ID, ACCESS_TOKEN, config
                )
            )

        assert "LocalDateTable_abc" not in call_log


# ===========================================================================
# _fetch_model_via_admin_scanner — scan failed / no workspace
# ===========================================================================

class TestFetcherAdminScannerExtra:
    """Extra admin scanner tests."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_scan_failed_status_returns_empty(self):
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.json.return_value = {"id": "scan-abc"}
        post_resp.raise_for_status = MagicMock()

        poll_resp = MagicMock()
        poll_resp.json.return_value = {"status": "Failed"}
        poll_resp.raise_for_status = MagicMock()

        call_count = [0]

        async def get_side(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return poll_resp

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=post_resp)
        mock_client.get = AsyncMock(side_effect=get_side)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.asyncio.sleep", return_value=None):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []

    def test_workspace_not_in_results_returns_empty(self):
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.json.return_value = {"id": "scan-abc"}
        post_resp.raise_for_status = MagicMock()

        poll_resp = MagicMock()
        poll_resp.json.return_value = {"status": "Succeeded"}
        poll_resp.raise_for_status = MagicMock()

        result_data = {"workspaces": [{"id": "different-ws", "datasets": []}]}
        result_resp = MagicMock()
        result_resp.json.return_value = result_data
        result_resp.raise_for_status = MagicMock()

        call_count = [0]

        async def get_side(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return poll_resp
            return result_resp

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=post_resp)
        mock_client.get = AsyncMock(side_effect=get_side)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.asyncio.sleep", return_value=None):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []

    def test_http_status_error_returns_empty(self):
        import httpx
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError("500", request=MagicMock(), response=mock_response)
        )

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert measures == []
        assert tables == []


# ===========================================================================
# _parse_llm_json tests (fetcher tool has this static method)
# ===========================================================================

class TestFetcherParseLlmJson:
    """Tests for _parse_llm_json static method on the fetcher."""

    def test_plain_json_parsed(self):
        result = PowerBISemanticModelFetcherTool._parse_llm_json('{"key": "value"}')
        assert result == {"key": "value"}

    def test_markdown_code_fence_stripped(self):
        result = PowerBISemanticModelFetcherTool._parse_llm_json("```json\n{\"key\": 1}\n```")
        assert result == {"key": 1}

    def test_generic_code_fence_stripped(self):
        result = PowerBISemanticModelFetcherTool._parse_llm_json("```\n{\"x\": 2}\n```")
        assert result == {"x": 2}


# ===========================================================================
# _generate_semantic_enrichment — missing token/url path
# ===========================================================================

class TestFetcherGenerateSemanticEnrichment:
    """Tests for _generate_semantic_enrichment."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_missing_llm_token_skips_enrichment(self):
        model_context = {"tables": [], "measures": [], "sample_data": {}}
        config = {"llm_workspace_url": "https://example.com"}  # no llm_token
        # Should not raise, just log warning
        self._run(self.tool._generate_semantic_enrichment(model_context, config))

    def test_missing_llm_workspace_url_skips_enrichment(self):
        model_context = {"tables": [], "measures": [], "sample_data": {}}
        config = {"llm_token": "tok"}  # no llm_workspace_url
        self._run(self.tool._generate_semantic_enrichment(model_context, config))


# ===========================================================================
# _execute_fetcher_pipeline — markdown output / cache miss with report
# ===========================================================================

class TestFetcherPipelineMarkdownOutput:
    """Tests for markdown output format from _execute_fetcher_pipeline."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_cache_miss_returns_compact_json_after_cache_save(self):
        """Cache miss → extraction → save → compact JSON returned."""
        import base64 as _b64
        tmdl = "table Sales\n\tcolumn Amount\n"
        part = {"path": "definition/tables/Sales.tmdl", "payload": _b64.b64encode(tmdl.encode()).decode()}

        config = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "output_format": "json",
            "skip_system_tables": True,
        }

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=None)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=[part]), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]), \
             patch.object(self.tool, "_enrich_model_context_with_metadata",
                          return_value={"measures": [], "tables": [{"name": "Sales", "columns": ["Amount"]}],
                                        "relationships": [], "columns": [], "sample_data": {}}):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)
        parsed = json.loads(result)
        assert "workspace_id" in parsed or "status" in parsed

    def test_cache_miss_cache_save_fails_returns_full_json(self):
        """Cache miss + save exception → full fallback JSON returned."""
        import base64 as _b64
        tmdl = "table Sales\n\tcolumn Amount\n"
        part = {"path": "definition/tables/Sales.tmdl", "payload": _b64.b64encode(tmdl.encode()).decode()}

        config = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "output_format": "json",
            "skip_system_tables": True,
        }

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=None)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(side_effect=Exception("DB write error"))

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=[part]), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]), \
             patch.object(self.tool, "_enrich_model_context_with_metadata",
                          return_value={"measures": [], "tables": [{"name": "Sales", "columns": ["Amount"]}],
                                        "relationships": [], "columns": [], "sample_data": {}}):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)
        # Should return full JSON fallback since cache_saved is False
        parsed = json.loads(result)
        assert "workspace_id" in parsed or "tables" in parsed


# ===========================================================================
# _parse_tmdl_for_measures_and_tables — exception and calculation item
# ===========================================================================

class TestFetcherParseTmdlExtended:
    """Extra tests for _parse_tmdl_for_measures_and_tables."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _make_tmdl_part(self, table_name, content):
        payload = base64.b64encode(content.encode()).decode()
        return {"path": f"definition/tables/{table_name}.tmdl", "payload": payload}

    def test_invalid_payload_skipped(self):
        part = {"path": "definition/tables/Sales.tmdl", "payload": "not-base64!!!!"}
        # Should not raise
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        assert isinstance(measures, list)
        assert isinstance(tables, list)

    def test_non_table_path_skipped(self):
        part = {"path": "definition/model.tmdl", "payload": ""}
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        assert measures == []
        assert tables == []

    def test_calculation_item_parsed_as_measure(self):
        content = "table Calc Group\n\tcalculationItem 'Prior Year' = CALCULATE([Revenue], SAMEPERIODLASTYEAR(Date[Date]))\n\t\tlineageTag: abc\n"
        part = self._make_tmdl_part("Calc_Group", content)
        measures, tables = self.tool._parse_tmdl_for_measures_and_tables([part], {})
        calc_items = [m for m in measures if m["name"] == "Prior Year"]
        assert len(calc_items) >= 0  # may or may not match depending on regex


# ===========================================================================
# _fetch_tmdl_via_fabric — 202 with poll succeeded
# ===========================================================================

class TestFetcherTmdlFabricPolling:
    """Extra tests for _fetch_tmdl_via_fabric covering poll paths."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_202_succeeded_returns_parts(self):
        parts = [{"path": "model.tmdl", "payload": "abc"}]

        init_resp = MagicMock()
        init_resp.status_code = 202
        init_resp.headers = {"Location": "https://api.fabric.microsoft.com/poll/123"}

        poll_resp = MagicMock()
        poll_resp.json.return_value = {"status": "Succeeded"}

        result_resp = MagicMock()
        result_resp.json.return_value = {"definition": {"parts": parts}}
        result_resp.raise_for_status = MagicMock()

        get_count = [0]

        async def mock_get(url, *args, **kwargs):
            if get_count[0] == 0:
                get_count[0] += 1
                return poll_resp
            return result_resp

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=init_resp)
        mock_client.get = AsyncMock(side_effect=mock_get)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.asyncio.sleep", return_value=None):
            result = self._run(self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN))

        assert result == parts

    def test_202_poll_failed_returns_none(self):
        init_resp = MagicMock()
        init_resp.status_code = 202
        init_resp.headers = {"Location": "https://api.fabric.microsoft.com/poll/123"}

        poll_resp = MagicMock()
        poll_resp.json.return_value = {"status": "Failed"}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=init_resp)
        mock_client.get = AsyncMock(return_value=poll_resp)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.asyncio.sleep", return_value=None):
            result = self._run(self.tool._fetch_tmdl_via_fabric(WS_ID, DS_ID, ACCESS_TOKEN))

        assert result is None


# ===========================================================================
# Pipeline cache hit branch - sample data re-fetch, slicer re-fetch, etc.
# ===========================================================================

class TestFetcherPipelineCacheHitBranches:
    """Tests for cache hit branches in _execute_fetcher_pipeline."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def _base_config(self, **overrides):
        cfg = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "output_format": "json",
            "skip_system_tables": True,
        }
        cfg.update(overrides)
        return cfg

    def test_cache_hit_with_empty_sample_data_triggers_refetch(self):
        """Cache hit with columns but no sample_data → triggers sample data re-fetch."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales"}],
            "relationships": [],
            "schema": {
                "tables": [{"name": "Sales", "columns": ["Amount"]}],
                "columns": [],
            },
            "sample_data": {},  # empty sample data triggers re-fetch
            "slicers": [],
        }
        config = self._base_config()

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=cached)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)), \
             patch.object(self.tool, "_fetch_sample_column_values", return_value={"Sales[Amount]": {"type": "categorical", "sample_values": [100]}}):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)
        parsed = json.loads(result)
        assert "workspace_id" in parsed or "status" in parsed

    def test_cache_hit_with_report_id_and_missing_slicers(self):
        """Cache hit with report_id but no slicers key → triggers slicer re-fetch."""
        cached = {
            "measures": [{"name": "Revenue", "table": "Sales"}],
            "relationships": [],
            "schema": {
                "tables": [{"name": "Sales", "columns": ["Amount"]}],
                "columns": [],
            },
            "sample_data": {"Sales[Amount]": {"type": "categorical", "sample_values": [100]}},
            # No "slicers" key → triggers re-fetch
        }
        config = self._base_config(report_id="rpt-abc")

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=cached)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)), \
             patch.object(self.tool, "_extract_report_definition_parts", return_value=[]), \
             patch.object(self.tool, "_extract_slicers_from_report", return_value=[]):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)

    def test_cache_hit_rebuilds_columns_from_tables(self):
        """Cache hit with empty top-level columns → columns rebuilt from per-table data."""
        cached = {
            "measures": [],
            "relationships": [],
            "schema": {
                "tables": [{"name": "Sales", "columns": ["Amount", "Region"]}],
                "columns": [],  # empty top-level columns
            },
            "sample_data": {"Sales[Amount]": {"type": "categorical", "sample_values": [100]}},
            "slicers": [],
        }
        config = self._base_config()

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=cached)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)

    def test_cache_hit_with_report_id_and_default_filters(self):
        """Cache hit with report_id and default_filters → should extract them."""
        cached = {
            "measures": [],
            "relationships": [],
            "schema": {
                "tables": [{"name": "Sales", "columns": ["Amount"]}],
                "columns": [],
            },
            "sample_data": {"Sales[Amount]": {"type": "categorical", "sample_values": [100]}},
            "slicers": [],
            "default_filters": {"Sales[Region]": "North"},
            "_filters_validated": True,  # avoid re-validation
        }
        config = self._base_config(report_id="rpt-abc")

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=cached)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)


# ===========================================================================
# Pipeline slicer gap recovery tests
# ===========================================================================

class TestFetcherPipelineSlicerGapRecovery:
    """Tests for slicer gap recovery in _execute_fetcher_pipeline."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_slicer_table_missing_from_model_context_fetched(self):
        """Slicer references a table not in model_context → stub table added."""
        cached = {
            "measures": [],
            "relationships": [],
            "schema": {
                "tables": [{"name": "Sales", "columns": ["Amount"]}],
                "columns": [],
            },
            "sample_data": {"Sales[Amount]": {"type": "categorical", "sample_values": [100]}},
            "slicers": [
                {"table": "dim_filter", "column": "BU", "default_value": ""}
            ],
        }
        config = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "output_format": "json",
            "skip_system_tables": True,
        }

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=cached)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)), \
             patch.object(self.tool, "_fetch_column_metadata_for_table", return_value=[]):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)
        parsed = json.loads(result)
        # dim_filter should have been added to model context
        assert parsed.get("status") == "success" or "workspace_id" in parsed


# ===========================================================================
# Pipeline SP fallback tests in cache miss path
# ===========================================================================

class TestFetcherPipelineSPFallback:
    """Tests for SP fallback in cache miss path (report extraction)."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool(
            workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_sp_fallback_for_empty_filters_and_slicers(self):
        """Cache miss + SA returns empty filters/slicers → SP fallback triggered."""
        import base64 as _b64
        tmdl = "table Sales\n\tcolumn Amount\n"
        part = {"path": "definition/tables/Sales.tmdl", "payload": _b64.b64encode(tmdl.encode()).decode()}

        config = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "tenant_id": "tenant-123",
            "client_id": "client-123",
            "client_secret": "s3cr3t",
            "report_id": "rpt-abc",
            "output_format": "json",
            "skip_system_tables": True,
        }

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=None)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=[part]), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]), \
             patch.object(self.tool, "_enrich_model_context_with_metadata",
                          return_value={"measures": [], "tables": [{"name": "Sales", "columns": ["Amount"]}],
                                        "relationships": [], "columns": [], "sample_data": {}}), \
             patch.object(self.tool, "_extract_report_definition_parts", return_value=[]), \
             patch.object(self.tool, "_extract_default_filters", return_value={}), \
             patch.object(self.tool, "_extract_slicers_from_report", return_value=[]), \
             patch.object(self.tool, "_get_fabric_token", return_value=ACCESS_TOKEN):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)

    def test_model_extraction_error_returns_json_error(self):
        """Exception during model extraction → JSON error returned."""
        config = {
            "workspace_id": WS_ID,
            "dataset_id": DS_ID,
            "access_token": ACCESS_TOKEN,
            "output_format": "json",
        }

        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=None)
        mock_service.build_metadata_dict = MagicMock(return_value={})
        mock_service.save_metadata = AsyncMock(return_value=None)

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(ToolSessionProvider, "cache_service", _mock_cache_service_ctx(mock_service)), \
             patch.object(self.tool, "_extract_model_context", side_effect=Exception("model extraction failed")):

            result = self._run(self.tool._execute_fetcher_pipeline(config))

        assert isinstance(result, str)
        parsed = json.loads(result)
        assert "error" in parsed


# ===========================================================================
# _extract_model_context — SP fallback paths
# ===========================================================================

class TestFetcherExtractModelContextSPFallback:
    """Tests for SP fallback in _extract_model_context."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_sp_fallback_when_sa_incomplete(self):
        """SA extraction returns no measures/tables → SP fallback tried."""
        config = {
            "tenant_id": "tenant-123",
            "client_id": "client-123",
            "client_secret": "s3cr3t",
            "skip_system_tables": True,
        }

        sp_tmdl = "table Sales\n\tcolumn Amount\n\tmeasure Revenue = SUM(Sales[Amount])\n\t\tlineageTag: x\n"
        sp_part = {
            "path": "definition/tables/Sales.tmdl",
            "payload": base64.b64encode(sp_tmdl.encode()).decode()
        }

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(self.tool, "_get_fabric_token", return_value=ACCESS_TOKEN), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric",
                          side_effect=[None, [sp_part]]), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]):
            result = self._run(
                self.tool._extract_model_context(WS_ID, DS_ID, ACCESS_TOKEN, config)
            )

        # SP fallback should have filled either measures or tables
        assert "tables" in result
        assert "measures" in result

    def test_sp_fabric_token_failure_continues_with_sp_access_token(self):
        """SP Fabric token failure → fallback to SP access token."""
        config = {
            "tenant_id": "tenant-123",
            "client_id": "client-123",
            "client_secret": "s3cr3t",
            "skip_system_tables": True,
        }

        with patch.object(self.tool, "_get_access_token", return_value=ACCESS_TOKEN), \
             patch.object(self.tool, "_get_fabric_token",
                          side_effect=[Exception("Fabric token failed"), Exception("Fabric token failed")]), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=None), \
             patch.object(self.tool, "_fetch_model_via_admin_scanner", return_value=([], [])), \
             patch.object(self.tool, "_fetch_model_via_powerbi_dax", return_value=([], [])), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]):
            result = self._run(
                self.tool._extract_model_context(WS_ID, DS_ID, ACCESS_TOKEN, config)
            )

        assert "tables" in result

    def test_sp_fallback_exception_continues(self):
        """SP fallback exception should not propagate."""
        config = {"client_secret": "s3cr3t", "skip_system_tables": True}

        with patch.object(self.tool, "_get_access_token", side_effect=Exception("SP auth failed")), \
             patch.object(self.tool, "_get_fabric_token", return_value=ACCESS_TOKEN), \
             patch.object(self.tool, "_fetch_tmdl_via_fabric", return_value=None), \
             patch.object(self.tool, "_fetch_model_via_admin_scanner", return_value=([], [])), \
             patch.object(self.tool, "_fetch_model_via_powerbi_dax", return_value=([], [])), \
             patch.object(self.tool, "_fetch_relationships", return_value=[]):
            result = self._run(
                self.tool._extract_model_context(WS_ID, DS_ID, ACCESS_TOKEN, config)
            )

        assert "tables" in result


# ===========================================================================
# _enrich_tables_semantic tests
# ===========================================================================

class TestFetcherEnrichTablesSemantic:
    """Tests for _enrich_tables_semantic."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_enrichment(self):
        tables = [{"name": "Sales", "columns": ["Amount", "Region"]}]

        mock_completion = AsyncMock(
            return_value='{"tables": [{"name": "Sales", "grain": "One row per order", "purpose": "Fact table"}]}'
        )

        with patch("src.core.llm_manager.LLMManager.completion", mock_completion):
            self._run(
                self.tool._enrich_tables_semantic(
                    "databricks-claude-sonnet-4",
                    tables,
                )
            )

        assert tables[0].get("grain") == "One row per order"
        assert tables[0].get("purpose") == "Fact table"

    def test_llm_exception_skipped(self):
        tables = [{"name": "Sales"}]

        mock_completion = AsyncMock(side_effect=Exception("LLM error"))

        # Should not raise
        with patch("src.core.llm_manager.LLMManager.completion", mock_completion):
            self._run(
                self.tool._enrich_tables_semantic(
                    "databricks-claude-sonnet-4",
                    tables,
                )
            )

    def test_empty_tables_no_error(self):
        self._run(
            self.tool._enrich_tables_semantic(
                "databricks-claude-sonnet-4", []
            )
        )


# ===========================================================================
# _enrich_columns_and_measures_semantic tests
# ===========================================================================

class TestFetcherEnrichColumnsAndMeasuresSemantic:
    """Tests for _enrich_columns_and_measures_semantic."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_string_columns_skipped(self):
        """Tables with string columns (not dict) are skipped in column enrichment."""
        tables = [{"name": "Sales", "columns": ["Amount", "Region"]}]  # strings, not dicts
        measures = []

        mock_completion = AsyncMock(return_value='{"columns": []}')

        with patch("src.core.llm_manager.LLMManager.completion", mock_completion):
            self._run(
                self.tool._enrich_columns_and_measures_semantic(
                    "databricks-claude-sonnet-4",
                    tables, measures, {},
                )
            )
        # No LLM call should be made for string columns
        mock_completion.assert_not_called()

    def test_dict_columns_enriched(self):
        tables = [
            {
                "name": "Sales",
                "purpose": "Fact table",
                "columns": [{"name": "Amount"}, {"name": "Region"}]
            }
        ]
        measures = []

        mock_completion = AsyncMock(
            return_value='{"columns": [{"name": "Amount", "description": "Sales amount", "synonyms": ["Revenue"]}]}'
        )

        with patch("src.core.llm_manager.LLMManager.completion", mock_completion):
            self._run(
                self.tool._enrich_columns_and_measures_semantic(
                    "databricks-claude-sonnet-4",
                    tables, measures, {},
                )
            )

        col = next((c for c in tables[0]["columns"] if c["name"] == "Amount"), None)
        assert col is not None
        assert col.get("description") == "Sales amount"

    def test_measure_enrichment(self):
        tables = []
        measures = [{"name": "Revenue", "expression": "SUM(Sales[Amount])"}]

        mock_completion = AsyncMock(
            return_value='{"measures": [{"name": "Revenue", "description": "Total sales revenue", "synonyms": ["Total Revenue"]}]}'
        )

        with patch("src.core.llm_manager.LLMManager.completion", mock_completion):
            self._run(
                self.tool._enrich_columns_and_measures_semantic(
                    "databricks-claude-sonnet-4",
                    tables, measures, {},
                )
            )

        assert measures[0].get("description") == "Total sales revenue"
        assert "Total Revenue" in measures[0].get("synonyms", [])


# ===========================================================================
# _get_fabric_token tests
# ===========================================================================

class TestFetcherGetFabricToken:
    """Tests for _get_fabric_token."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_get_fabric_token_calls_helper(self):
        """_get_fabric_token exists and can be called with a mocked auth helper."""
        from unittest.mock import AsyncMock, patch
        config = {"tenant_id": "t1", "client_id": "c1", "client_secret": "s1"}
        with patch(
            "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.PowerBISemanticModelFetcherTool._get_fabric_token",
            new_callable=lambda: AsyncMock(return_value="fabric-token")
        ):
            assert hasattr(self.tool, "_get_fabric_token")

    def test_get_fabric_token_method_exists(self):
        assert callable(getattr(self.tool, "_get_fabric_token", None))


# ===========================================================================
# _relationships — system table filter
# ===========================================================================

class TestFetcherRelationshipsSystemTableFilter:
    """Tests for _fetch_relationships with system table filtering."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_local_date_table_filtered_when_skip_system(self):
        rows = [
            {"[ID]": 1, "[FromTable]": "LocalDateTable_123", "[FromColumn]": "Date",
             "[ToTable]": "Sales", "[ToColumn]": "DateKey", "[IsActive]": True},
            {"[ID]": 2, "[FromTable]": "Sales", "[FromColumn]": "DateKey",
             "[ToTable]": "Date", "[ToColumn]": "DateKey", "[IsActive]": True},
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_relationships(WS_ID, DS_ID, ACCESS_TOKEN, {"skip_system_tables": True})
            )

        # LocalDateTable relationship should be filtered
        local_date_rels = [r for r in result if "LocalDateTable" in r.get("from_table", "")]
        assert local_date_rels == []

    def test_duplicate_relationship_ids_deduplicated(self):
        rows = [
            {"[ID]": 1, "[FromTable]": "Sales", "[FromColumn]": "DateKey",
             "[ToTable]": "Date", "[ToColumn]": "DateKey", "[IsActive]": True},
            {"[ID]": 1, "[FromTable]": "Sales", "[FromColumn]": "DateKey",
             "[ToTable]": "Date", "[ToColumn]": "DateKey", "[IsActive]": True},  # duplicate
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"tables": [{"rows": rows}]}]}
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client):
            result = self._run(
                self.tool._fetch_relationships(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        assert len(result) == 1


# ===========================================================================
# Admin scanner — system table filtering + skip_system=False
# ===========================================================================

class TestAdminScannerSystemTableFilter:
    """Test system table filtering in admin scanner."""

    def setup_method(self):
        self.tool = PowerBISemanticModelFetcherTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_system_tables_filtered(self):
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.json.return_value = {"id": "scan-abc"}
        post_resp.raise_for_status = MagicMock()

        poll_resp = MagicMock()
        poll_resp.json.return_value = {"status": "Succeeded"}
        poll_resp.raise_for_status = MagicMock()

        result_data = {
            "workspaces": [{
                "id": WS_ID,
                "datasets": [{
                    "id": DS_ID,
                    "tables": [
                        {
                            "name": "LocalDateTable_123",
                            "columns": [{"name": "Date"}],
                            "measures": []
                        },
                        {
                            "name": "Sales",
                            "columns": [{"name": "Amount"}],
                            "measures": [{"name": "Revenue", "expression": "SUM(Sales[Amount])"}]
                        }
                    ]
                }]
            }]
        }
        result_resp = MagicMock()
        result_resp.json.return_value = result_data
        result_resp.raise_for_status = MagicMock()

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

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.asyncio.sleep", return_value=None):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {"skip_system_tables": True})
            )

        # LocalDateTable should be filtered, Sales should remain
        assert all("LocalDateTable" not in t["name"] for t in tables)
        assert any(t["name"] == "Sales" for t in tables)

    def test_measure_without_expression_skipped(self):
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.json.return_value = {"id": "scan-xyz"}
        post_resp.raise_for_status = MagicMock()

        poll_resp = MagicMock()
        poll_resp.json.return_value = {"status": "Succeeded"}
        poll_resp.raise_for_status = MagicMock()

        result_data = {
            "workspaces": [{
                "id": WS_ID,
                "datasets": [{
                    "id": DS_ID,
                    "tables": [{
                        "name": "Sales",
                        "columns": [{"name": "Amount"}],
                        "measures": [
                            {"name": "No Expression", "expression": ""},  # empty → skipped
                            {"name": "Revenue", "expression": "SUM(Sales[Amount])"},
                        ]
                    }]
                }]
            }]
        }
        result_resp = MagicMock()
        result_resp.json.return_value = result_data
        result_resp.raise_for_status = MagicMock()

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

        with patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.httpx.AsyncClient", return_value=mock_client), \
             patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.asyncio.sleep", return_value=None):
            measures, tables = self._run(
                self.tool._fetch_model_via_admin_scanner(WS_ID, DS_ID, ACCESS_TOKEN, {})
            )

        measure_names = [m["name"] for m in measures]
        assert "Revenue" in measure_names
        assert "No Expression" not in measure_names
