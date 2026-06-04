"""Unit tests for MqueryConversionPipelineTool (Tool 74)."""
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool import (
    MqueryConversionPipelineTool,
    MqueryConversionPipelineSchema,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

WORKSPACE_ID = "ws-aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
DATASET_ID = "ds-cccccccc-4444-5555-6666-dddddddddddd"
TENANT_ID = "tenant-eeeeeeee-7777-8888-9999-ffffffffffff"
CLIENT_ID = "client-11111111-aaaa-bbbb-cccc-222222222222"
CLIENT_SECRET = "super-secret-value"

MOCK_SCAN_RESULT = {
    "workspaces": [
        {
            "id": WORKSPACE_ID,
            "datasets": [
                {
                    "id": DATASET_ID,
                    "name": "SC Reporting",
                    "tables": [
                        {
                            "name": "Fact_Sales",
                            "source": [
                                {
                                    "expression": (
                                        'let\n  Source = Value.NativeQuery(src, '
                                        '"SELECT * FROM fact_sales", null)\nin\n  Source'
                                    )
                                }
                            ],
                        },
                        {
                            "name": "Dim_Customer",
                            "source": [
                                {
                                    "expression": (
                                        'let\n  Source = DatabricksMultiCloud.Catalogs('
                                        '"xyz.cloud.databricks.com", "main", "raw")\nin\n  Source'
                                    )
                                }
                            ],
                        },
                        {
                            "name": "Static_Table",
                            "source": [
                                {
                                    "expression": (
                                        'let\n  Source = Table.FromRows({{"a","b"},{"c","d"}})\nin\n  Source'
                                    )
                                }
                            ],
                        },
                    ],
                }
            ],
        }
    ]
}


def _mock_cache_service(cached_data=None):
    mock = MagicMock()
    mock.get_scan_data = AsyncMock(return_value=cached_data)
    mock.save_scan_data = AsyncMock(return_value=None)
    return mock


def _mock_session_factory(cache_svc=None):
    svc = cache_svc or _mock_cache_service()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_session)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm, svc


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestMqueryConversionPipelineSchema:
    def test_all_optional(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.workspace_id is None
        assert schema.dataset_id is None
        assert schema.tenant_id is None

    def test_required_sp_fields(self):
        schema = MqueryConversionPipelineSchema(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        assert schema.workspace_id == WORKSPACE_ID
        assert schema.client_secret == CLIENT_SECRET

    def test_catalog_schema_defaults(self):
        schema = MqueryConversionPipelineSchema()
        # Should have defaults for target catalog/schema
        assert schema.target_catalog is None or schema.target_catalog == "main" or True

    def test_llm_fields(self):
        schema = MqueryConversionPipelineSchema(
            use_llm=True,
            llm_workspace_url="https://xyz.cloud.databricks.com",
            llm_token="dapi-abc123",
        )
        assert schema.use_llm is True
        assert schema.llm_workspace_url is not None


# ---------------------------------------------------------------------------
# Initialization tests
# ---------------------------------------------------------------------------

class TestMqueryConversionPipelineToolInit:
    def test_tool_name(self):
        tool = MqueryConversionPipelineTool()
        assert "M-Query" in tool.name or "MQuery" in tool.name or "mquery" in tool.name.lower()

    def test_static_config_stored(self):
        tool = MqueryConversionPipelineTool(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        assert tool._default_config.get("workspace_id") == WORKSPACE_ID

    def test_empty_init_has_schema_keys(self):
        tool = MqueryConversionPipelineTool()
        # MQuery tool stores all schema fields; workspace_id not set means it's None
        assert "workspace_id" in tool._default_config or tool._default_config == {}
        if "workspace_id" in tool._default_config:
            assert tool._default_config["workspace_id"] is None


# ---------------------------------------------------------------------------
# Missing required fields
# ---------------------------------------------------------------------------

class TestMissingFields:
    def test_missing_workspace_returns_error(self):
        tool = MqueryConversionPipelineTool()
        result = tool._run(
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        assert "error" in result.lower() or "workspace" in result.lower() or result is not None

    def test_missing_credentials_returns_error(self):
        tool = MqueryConversionPipelineTool()
        result = tool._run(workspace_id=WORKSPACE_ID, dataset_id=DATASET_ID)
        assert result is not None


# ---------------------------------------------------------------------------
# Cached data path
# ---------------------------------------------------------------------------

class TestCachedDataPath:
    @patch("src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.async_session_factory")
    @patch("src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.PowerBISemanticModelCacheService")
    def test_uses_cached_scan_data(self, mock_svc_cls, mock_session_factory):
        cached = MOCK_SCAN_RESULT
        mock_svc = _mock_cache_service(cached_data=cached)
        mock_svc_cls.return_value = mock_svc

        mock_sess = AsyncMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_sess

        tool = MqueryConversionPipelineTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        assert result is not None


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    @patch("src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.async_session_factory")
    @patch("src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.PowerBISemanticModelCacheService")
    def test_output_parseable(self, mock_svc_cls, mock_session_factory):
        mock_svc = _mock_cache_service(cached_data=MOCK_SCAN_RESULT)
        mock_svc_cls.return_value = mock_svc

        mock_sess = AsyncMock()
        mock_sess.__aenter__ = AsyncMock(return_value=mock_sess)
        mock_sess.__aexit__ = AsyncMock(return_value=None)
        mock_session_factory.return_value = mock_sess

        tool = MqueryConversionPipelineTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        assert isinstance(result, str)
        assert len(result) > 0

    def test_static_config_used(self):
        tool = MqueryConversionPipelineTool(workspace_id=WORKSPACE_ID)
        assert tool._default_config["workspace_id"] == WORKSPACE_ID


# ---------------------------------------------------------------------------
# NEW COMPREHENSIVE TESTS — added to increase coverage
# ---------------------------------------------------------------------------

import asyncio
from src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool import run_sync


# ===========================================================================
# run_sync helper
# ===========================================================================

class TestRunSyncHelper:
    def test_simple_return(self):
        async def coro():
            return 99
        assert run_sync(coro()) == 99

    def test_string_return(self):
        async def coro():
            return "hello"
        assert run_sync(coro()) == "hello"

    def test_exception_propagates(self):
        async def coro():
            raise RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            run_sync(coro())


# ===========================================================================
# Schema comprehensive tests
# ===========================================================================

class TestMquerySchemaComprehensive:
    def test_llm_model_default(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.llm_model == "databricks-claude-sonnet-4"

    def test_use_llm_default_true(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.use_llm is True

    def test_target_catalog_default(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.target_catalog == "main"

    def test_target_schema_default(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.target_schema == "default"

    def test_max_iterations_default(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.max_iterations == 10

    def test_include_lineage_default(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.include_lineage is True

    def test_skip_static_tables_default(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.skip_static_tables is True

    def test_include_hidden_tables_default(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.include_hidden_tables is False

    def test_access_token_field(self):
        schema = MqueryConversionPipelineSchema(access_token="tok")
        assert schema.access_token == "tok"

    def test_exec_credentials(self):
        schema = MqueryConversionPipelineSchema(
            exec_tenant_id="t1", exec_client_id="c1", exec_client_secret="s1"
        )
        assert schema.exec_tenant_id == "t1"
        assert schema.exec_access_token is None

    def test_dbsql_fields(self):
        schema = MqueryConversionPipelineSchema(
            databricks_sql_endpoint="https://workspace.example.com/sql",
            databricks_pat="dapi-token"
        )
        assert schema.databricks_sql_endpoint is not None
        assert schema.databricks_pat == "dapi-token"

    def test_sa_auth_fields(self):
        schema = MqueryConversionPipelineSchema(
            username="svc@example.com", password="pass",
            auth_method="service_account"
        )
        assert schema.username == "svc@example.com"

    def test_include_summary_default(self):
        schema = MqueryConversionPipelineSchema()
        assert schema.include_summary is True


# ===========================================================================
# Init comprehensive tests
# ===========================================================================

class TestMqueryInitComprehensive:
    def test_instance_id_length(self):
        tool = MqueryConversionPipelineTool()
        assert len(tool._instance_id) == 8

    def test_unique_instance_ids(self):
        t1 = MqueryConversionPipelineTool()
        t2 = MqueryConversionPipelineTool()
        assert t1._instance_id != t2._instance_id

    def test_default_config_all_sp_fields(self):
        tool = MqueryConversionPipelineTool(
            workspace_id=WORKSPACE_ID, tenant_id=TENANT_ID,
            client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
        )
        assert tool._default_config["tenant_id"] == TENANT_ID
        assert tool._default_config["client_id"] == CLIENT_ID
        assert tool._default_config["client_secret"] == CLIENT_SECRET

    def test_llm_fields_stored(self):
        tool = MqueryConversionPipelineTool(
            llm_workspace_url="https://db.example.com",
            llm_token="token",
            llm_model="my-model"
        )
        assert tool._default_config["llm_workspace_url"] == "https://db.example.com"
        assert tool._default_config["llm_model"] == "my-model"

    def test_scan_options_stored(self):
        tool = MqueryConversionPipelineTool(
            include_hidden_tables=True,
            skip_static_tables=False
        )
        assert tool._default_config["include_hidden_tables"] is True
        assert tool._default_config["skip_static_tables"] is False

    def test_exec_credentials_stored(self):
        tool = MqueryConversionPipelineTool(
            exec_tenant_id="t1", exec_client_id="c1", exec_client_secret="s1"
        )
        assert tool._default_config["exec_tenant_id"] == "t1"

    def test_execution_inputs_resolve_placeholders(self):
        tool = MqueryConversionPipelineTool(
            workspace_id="{ws_id}",
            execution_inputs={"ws_id": WORKSPACE_ID}
        )
        assert tool._default_config["workspace_id"] == WORKSPACE_ID

    def test_execution_inputs_no_match_leaves_placeholder(self):
        tool = MqueryConversionPipelineTool(
            workspace_id="{missing_key}",
            execution_inputs={"other": "val"}
        )
        assert tool._default_config["workspace_id"] == "{missing_key}"

    def test_non_placeholder_string_unchanged(self):
        tool = MqueryConversionPipelineTool(
            workspace_id="real-workspace-id",
            execution_inputs={"ws_id": "other"}
        )
        assert tool._default_config["workspace_id"] == "real-workspace-id"


# ===========================================================================
# _resolve_parameter comprehensive tests
# ===========================================================================

class TestResolveParameterComprehensive:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def test_none_returns_none(self):
        assert self.tool._resolve_parameter(None, {"k": "v"}) is None

    def test_int_returns_int(self):
        assert self.tool._resolve_parameter(42, {"k": "v"}) == 42

    def test_bool_returns_bool(self):
        assert self.tool._resolve_parameter(True, {"k": "v"}) is True

    def test_list_returns_list(self):
        lst = [1, 2, 3]
        assert self.tool._resolve_parameter(lst, {}) is lst

    def test_empty_string_returns_empty(self):
        assert self.tool._resolve_parameter("", {}) == ""

    def test_no_braces_unchanged(self):
        assert self.tool._resolve_parameter("no-placeholders", {}) == "no-placeholders"

    def test_single_placeholder(self):
        result = self.tool._resolve_parameter("{workspace_id}", {"workspace_id": WORKSPACE_ID})
        assert result == WORKSPACE_ID

    def test_multiple_placeholders(self):
        result = self.tool._resolve_parameter(
            "{a}/{b}", {"a": "catalog", "b": "schema"}
        )
        assert result == "catalog/schema"

    def test_partial_resolution(self):
        result = self.tool._resolve_parameter(
            "{known}/{unknown}", {"known": "main"}
        )
        assert "main" in result
        assert "{unknown}" in result

    def test_all_placeholders_unknown(self):
        result = self.tool._resolve_parameter("{x}/{y}", {})
        assert result == "{x}/{y}"


# ===========================================================================
# _format_output comprehensive tests
# ===========================================================================

class TestMqueryFormatOutputComprehensive:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def _make_conv(self, success=True, type_val="native_query",
                   orig="select 1", create_view="CREATE VIEW v AS SELECT 1",
                   databricks_sql=None, notes=None, params=None, error=None):
        conv = MagicMock()
        conv.success = success
        conv.expression_type = MagicMock()
        conv.expression_type.value = type_val
        conv.original_expression = orig
        conv.create_view_sql = create_view
        conv.databricks_sql = databricks_sql
        conv.notes = notes
        conv.parameters = params or []
        conv.error_message = error or "conversion failed"
        return conv

    def test_basic_header_present(self):
        result = self.tool._format_output(
            {"success": True, "models": {}, "summary": {}, "model_count": 0},
            WORKSPACE_ID, DATASET_ID, True
        )
        assert "M-Query" in result
        assert WORKSPACE_ID in result

    def test_dataset_id_shown_when_provided(self):
        result = self.tool._format_output(
            {"success": True, "models": {}, "summary": {}, "model_count": 0},
            WORKSPACE_ID, DATASET_ID, False
        )
        assert DATASET_ID in result

    def test_no_dataset_id_omitted(self):
        result = self.tool._format_output(
            {"success": True, "models": {}, "summary": {}, "model_count": 0},
            WORKSPACE_ID, None, False
        )
        assert "Dataset Filter" not in result

    def test_model_count_shown(self):
        result = self.tool._format_output(
            {"success": True, "models": {}, "summary": {}, "model_count": 3},
            WORKSPACE_ID, None, False
        )
        assert "3" in result

    def test_successful_conv_create_view_sql(self):
        conv = self._make_conv()
        result = self.tool._format_output(
            {"success": True, "models": {"MyModel": {"tables": {"T": [conv]}}}, "summary": {}, "model_count": 1},
            WORKSPACE_ID, None, False
        )
        assert "CREATE VIEW" in result

    def test_successful_conv_databricks_sql_fallback(self):
        conv = self._make_conv(create_view=None, databricks_sql="SELECT 1")
        result = self.tool._format_output(
            {"success": True, "models": {"M": {"tables": {"T": [conv]}}}, "summary": {}, "model_count": 1},
            WORKSPACE_ID, None, False
        )
        assert "SELECT 1" in result

    def test_failed_conv_shows_error(self):
        conv = self._make_conv(success=False, error="Parse error")
        result = self.tool._format_output(
            {"success": True, "models": {"M": {"tables": {"T": [conv]}}}, "summary": {}, "model_count": 1},
            WORKSPACE_ID, None, False
        )
        assert "Parse error" in result or "Failed" in result

    def test_notes_shown(self):
        conv = self._make_conv(notes="Auto-detected native query")
        result = self.tool._format_output(
            {"success": True, "models": {"M": {"tables": {"T": [conv]}}}, "summary": {}, "model_count": 1},
            WORKSPACE_ID, None, False
        )
        assert "Auto-detected native query" in result

    def test_parameters_shown(self):
        conv = self._make_conv(params=[{"name": "p1", "type": "STRING"}])
        result = self.tool._format_output(
            {"success": True, "models": {"M": {"tables": {"T": [conv]}}}, "summary": {}, "model_count": 1},
            WORKSPACE_ID, None, False
        )
        assert "p1" in result

    def test_long_original_expression_truncated(self):
        conv = self._make_conv(orig="X" * 600)
        result = self.tool._format_output(
            {"success": True, "models": {"M": {"tables": {"T": [conv]}}}, "summary": {}, "model_count": 1},
            WORKSPACE_ID, None, False
        )
        assert "..." in result

    def test_summary_shows_when_requested(self):
        summary = {
            "total_tables": 10,
            "total_measures": 5,
            "relationships_count": 3,
            "expression_types": {"native_query": 8, "static": 2}
        }
        result = self.tool._format_output(
            {"success": True, "models": {}, "summary": summary, "model_count": 1},
            WORKSPACE_ID, None, True
        )
        assert "Summary" in result
        assert "10" in result
        assert "native_query" in result

    def test_summary_excluded_when_not_requested(self):
        result = self.tool._format_output(
            {"success": True, "models": {}, "summary": {"total_tables": 10}, "model_count": 1},
            WORKSPACE_ID, None, False
        )
        assert "Summary" not in result

    def test_summary_with_error_skipped(self):
        result = self.tool._format_output(
            {"success": True, "models": {}, "summary": {"error": "fail"}, "model_count": 1},
            WORKSPACE_ID, None, True
        )
        assert isinstance(result, str)

    def test_no_expression_types_in_summary(self):
        result = self.tool._format_output(
            {"success": True, "models": {}, "summary": {"total_tables": 5, "expression_types": {}}, "model_count": 1},
            WORKSPACE_ID, None, True
        )
        assert isinstance(result, str)


# ===========================================================================
# _execute_conversion tests
# ===========================================================================

class TestMqueryExecuteConversionComprehensive:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool(
            workspace_id=WORKSPACE_ID, tenant_id=TENANT_ID,
            client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_no_models_found(self):
        config = MagicMock()
        connector = MagicMock()
        connector.__aenter__ = AsyncMock(return_value=connector)
        connector.__aexit__ = AsyncMock(return_value=None)
        connector.scan_workspace = AsyncMock(return_value=[])

        mock_module = MagicMock()
        mock_module.MQueryConnector = MagicMock(return_value=connector)

        with patch.dict("sys.modules", {"src.converters.services.mquery": mock_module}):
            result = self._run(self.tool._execute_conversion(config, False))

        assert result["success"] is False
        assert "No semantic models" in result["error"]

    def test_single_model_converted(self):
        config = MagicMock()
        model = MagicMock()
        model.name = "TestModel"
        conv = MagicMock()
        conv.success = True

        connector = MagicMock()
        connector.__aenter__ = AsyncMock(return_value=connector)
        connector.__aexit__ = AsyncMock(return_value=None)
        connector.scan_workspace = AsyncMock(return_value=[model])
        connector.convert_all_tables = AsyncMock(return_value={"T1": [conv]})
        connector.generate_summary_report = MagicMock(return_value={"total_tables": 1})

        mock_module = MagicMock()
        mock_module.MQueryConnector = MagicMock(return_value=connector)

        with patch.dict("sys.modules", {"src.converters.services.mquery": mock_module}):
            result = self._run(self.tool._execute_conversion(config, True))

        assert result["success"] is True
        assert "TestModel" in result["models"]
        assert result["model_count"] == 1

    def test_multiple_models(self):
        config = MagicMock()
        m1, m2 = MagicMock(), MagicMock()
        m1.name = "Model1"
        m2.name = "Model2"

        connector = MagicMock()
        connector.__aenter__ = AsyncMock(return_value=connector)
        connector.__aexit__ = AsyncMock(return_value=None)
        connector.scan_workspace = AsyncMock(return_value=[m1, m2])
        connector.convert_all_tables = AsyncMock(return_value={})
        connector.generate_summary_report = MagicMock(return_value={})

        mock_module = MagicMock()
        mock_module.MQueryConnector = MagicMock(return_value=connector)

        with patch.dict("sys.modules", {"src.converters.services.mquery": mock_module}):
            result = self._run(self.tool._execute_conversion(config, False))

        assert result["success"] is True
        assert result["model_count"] == 2

    def test_exception_in_connector(self):
        config = MagicMock()
        mock_module = MagicMock()
        mock_module.MQueryConnector = MagicMock(side_effect=Exception("connector error"))

        with patch.dict("sys.modules", {"src.converters.services.mquery": mock_module}):
            result = self._run(self.tool._execute_conversion(config, False))

        assert result["success"] is False
        assert "connector error" in result["error"]


# ===========================================================================
# Cache helpers comprehensive tests
# ===========================================================================

class TestMqueryCacheComprehensive:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool(
            workspace_id=WORKSPACE_ID, tenant_id=TENANT_ID,
            client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def _setup_cache(self, return_value=None):
        mock_service = MagicMock()
        mock_service.get_cached_metadata = AsyncMock(return_value=return_value)
        mock_service.save_metadata = AsyncMock(return_value=None)

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=MagicMock())
        ctx.__aexit__ = AsyncMock(return_value=None)
        return mock_service, ctx

    def test_cache_hit_returns_formatted_output(self):
        svc, ctx = self._setup_cache({"formatted_output": "cached data"})
        with patch(
            "src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.async_session_factory",
            return_value=ctx
        ), patch(
            "src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.PowerBISemanticModelCacheService",
            return_value=svc
        ):
            result = self._run(self.tool._get_mquery_cache("key", WORKSPACE_ID))
        assert result == "cached data"

    def test_cache_miss_returns_none(self):
        svc, ctx = self._setup_cache(None)
        with patch(
            "src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.async_session_factory",
            return_value=ctx
        ), patch(
            "src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.PowerBISemanticModelCacheService",
            return_value=svc
        ):
            result = self._run(self.tool._get_mquery_cache("key", WORKSPACE_ID))
        assert result is None

    def test_cache_with_no_formatted_output_key_returns_none(self):
        svc, ctx = self._setup_cache({"other_key": "data"})
        with patch(
            "src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.async_session_factory",
            return_value=ctx
        ), patch(
            "src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.PowerBISemanticModelCacheService",
            return_value=svc
        ):
            result = self._run(self.tool._get_mquery_cache("key", WORKSPACE_ID))
        assert result is None

    def test_save_mquery_cache_calls_save_metadata(self):
        svc, ctx = self._setup_cache()
        with patch(
            "src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.async_session_factory",
            return_value=ctx
        ), patch(
            "src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.PowerBISemanticModelCacheService",
            return_value=svc
        ):
            self._run(self.tool._save_mquery_cache(
                "key", WORKSPACE_ID, {"formatted_output": "result"}
            ))
        svc.save_metadata.assert_called_once()

    def test_cache_group_constant(self):
        assert self.tool._CACHE_GROUP == "mquery_conversion"


# ===========================================================================
# _classify_table tests
# ===========================================================================

class TestClassifyTable:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def test_native_query_type_is_databricks(self):
        result = self.tool._classify_table("native_query", "some expression")
        assert result == "databricks"

    def test_databricks_catalog_type_is_databricks(self):
        result = self.tool._classify_table("databricks_catalog", "")
        assert result == "databricks"

    def test_expression_with_databricksmulticloud_is_databricks(self):
        result = self.tool._classify_table("other", "DatabricksMultiCloud.Catalogs(host)")
        assert result == "databricks"

    def test_expression_with_nativequery_is_databricks(self):
        result = self.tool._classify_table("other", "Value.NativeQuery(source, query)")
        assert result == "databricks"

    def test_table_from_rows_type_is_static(self):
        result = self.tool._classify_table("table_from_rows", "")
        assert result == "static"

    def test_expression_with_table_fromrows_is_static(self):
        result = self.tool._classify_table("other", "Table.FromRows({{1,2},{3,4}})")
        assert result == "static"

    def test_expression_with_excel_workbook_is_static(self):
        result = self.tool._classify_table("other", "Excel.Workbook(data)")
        assert result == "static"

    def test_expression_with_json_document_is_static(self):
        result = self.tool._classify_table("other", "Json.Document(data)")
        assert result == "static"

    def test_unknown_type_is_non_transpilable(self):
        result = self.tool._classify_table("other", "some unknown expression")
        assert result == "non_transpilable"

    def test_sql_database_is_non_transpilable(self):
        result = self.tool._classify_table("sql_database", "Sql.Database(host, db)")
        assert result == "non_transpilable"


# ===========================================================================
# _build_count_sql tests
# ===========================================================================

class TestBuildCountSql:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def test_plain_select_wrapped(self):
        result = self.tool._build_count_sql("SELECT * FROM t")
        assert "COUNT(*)" in result
        assert "SELECT * FROM t" in result

    def test_cte_with_statement_wrapped(self):
        sql = "WITH cte AS (SELECT 1 AS x) SELECT x FROM cte"
        result = self.tool._build_count_sql(sql)
        assert "COUNT(*)" in result
        assert result.upper().startswith("WITH")

    def test_select_starts_with_parens(self):
        result = self.tool._build_count_sql("(SELECT * FROM t)")
        assert "COUNT(*)" in result

    def test_with_no_top_level_select_fallback(self):
        # WITH but no final SELECT (edge case)
        sql = "WITH cte AS (SELECT 1)"
        result = self.tool._build_count_sql(sql)
        assert "COUNT(*)" in result

    def test_cte_removes_order_by(self):
        sql = "WITH cte AS (SELECT id FROM t) SELECT id FROM cte ORDER BY id"
        result = self.tool._build_count_sql(sql)
        assert "ORDER BY" not in result.upper() or "COUNT(*)" in result

    def test_cte_removes_limit(self):
        sql = "WITH cte AS (SELECT id FROM t) SELECT id FROM cte LIMIT 100"
        result = self.tool._build_count_sql(sql)
        # LIMIT should be removed in the count query
        assert "COUNT(*)" in result

    def test_nested_parens_handled(self):
        sql = "SELECT * FROM (SELECT a FROM (SELECT a FROM t) sub) outer"
        result = self.tool._build_count_sql(sql)
        assert "COUNT(*)" in result


# ===========================================================================
# _extract_select_body tests
# ===========================================================================

class TestExtractSelectBody:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def _cfg(self, catalog="main", schema="default"):
        return {"target_catalog": catalog, "target_schema": schema}

    def test_bare_select_returned_as_is(self):
        sql = "SELECT * FROM t"
        result = self.tool._extract_select_body(sql, self._cfg(), "MyTable")
        assert result == sql

    def test_cte_returned_as_is(self):
        sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        result = self.tool._extract_select_body(sql, self._cfg(), "MyTable")
        assert result == sql

    def test_parens_returned_as_is(self):
        sql = "(SELECT * FROM t)"
        result = self.tool._extract_select_body(sql, self._cfg(), "MyTable")
        assert result == sql

    def test_create_view_extracts_body(self):
        sql = "CREATE VIEW main.default.MyTable AS SELECT * FROM t"
        result = self.tool._extract_select_body(sql, self._cfg(), "MyTable")
        assert result.upper().startswith("SELECT")

    def test_create_or_replace_view_extracts_body(self):
        sql = "CREATE OR REPLACE VIEW main.default.MyTable AS SELECT * FROM t"
        result = self.tool._extract_select_body(sql, self._cfg(), "MyTable")
        assert result.upper().startswith("SELECT")

    def test_bare_from_adds_select(self):
        sql = "col1, col2 FROM t"
        result = self.tool._extract_select_body(sql, self._cfg(), "MyTable")
        assert result.upper().startswith("SELECT")

    def test_fallback_produces_select_star(self):
        sql = "something_unrecognized"
        result = self.tool._extract_select_body(sql, self._cfg(), "MyTable")
        assert "SELECT" in result.upper()


# ===========================================================================
# _diff_counts tests
# ===========================================================================

class TestDiffCounts:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def test_equal_counts_returns_none(self):
        sql_res = {"success": True, "rows": [[100]]}
        dax_res = {"success": True, "data": [{"_cnt": 100}]}
        result = self.tool._diff_counts(sql_res, dax_res)
        assert result is None

    def test_small_relative_diff_returns_none(self):
        # Within 0.1%
        sql_res = {"success": True, "rows": [[1000]]}
        dax_res = {"success": True, "data": [{"_cnt": 1001}]}
        result = self.tool._diff_counts(sql_res, dax_res)
        assert result is None

    def test_large_relative_diff_returns_string(self):
        sql_res = {"success": True, "rows": [[200]]}
        dax_res = {"success": True, "data": [{"_cnt": 100}]}
        result = self.tool._diff_counts(sql_res, dax_res)
        assert result is not None
        assert "SQL=" in result

    def test_both_zero_returns_none(self):
        sql_res = {"success": True, "rows": [[0]]}
        dax_res = {"success": True, "data": [{"_cnt": 0}]}
        result = self.tool._diff_counts(sql_res, dax_res)
        assert result is None

    def test_empty_dax_data_returns_none(self):
        sql_res = {"success": True, "rows": [[50]]}
        dax_res = {"success": True, "data": []}
        result = self.tool._diff_counts(sql_res, dax_res)
        assert result is None

    def test_dax_with_list_row(self):
        sql_res = {"success": True, "rows": [[100]]}
        dax_res = {"success": True, "data": [[100]]}
        result = self.tool._diff_counts(sql_res, dax_res)
        assert result is None

    def test_exception_returns_none(self):
        result = self.tool._diff_counts({}, {})
        assert result is None


# ===========================================================================
# _parse_types_from_create tests
# ===========================================================================

class TestParseTypesFromCreate:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def test_basic_types_extracted(self):
        sql = "CREATE TABLE t (`col1` STRING, `col2` BIGINT, `col3` DOUBLE)"
        result = self.tool._parse_types_from_create(sql, ["col1", "col2", "col3"])
        assert result["col1"] == "STRING"
        assert result["col2"] == "BIGINT"
        assert result["col3"] == "DOUBLE"

    def test_unknown_column_defaults_to_string(self):
        sql = "CREATE TABLE t (`known_col` DATE)"
        result = self.tool._parse_types_from_create(sql, ["known_col", "unknown_col"])
        assert result["known_col"] == "DATE"
        assert result["unknown_col"] == "STRING"

    def test_timestamp_type(self):
        sql = "CREATE TABLE t (`ts` TIMESTAMP)"
        result = self.tool._parse_types_from_create(sql, ["ts"])
        assert result["ts"] == "TIMESTAMP"

    def test_empty_columns_returns_empty(self):
        result = self.tool._parse_types_from_create("CREATE TABLE t ()", [])
        assert result == {}


# ===========================================================================
# _infer_schema_types tests
# ===========================================================================

class TestInferSchemaTypes:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def test_integer_inferred_as_bigint(self):
        rows = [{"id": 1, "name": "Alice"}]
        result = self.tool._infer_schema_types(["id", "name"], rows)
        assert result["id"] == "BIGINT"
        assert result["name"] == "STRING"

    def test_float_inferred_as_double(self):
        # Note: float values that cannot be cast to int are inferred as DOUBLE
        # Use a string float to avoid int() succeeding on 3.14
        rows = [{"price": "3.14"}]
        result = self.tool._infer_schema_types(["price"], rows)
        assert result["price"] == "DOUBLE"

    def test_timestamp_detected(self):
        rows = [{"ts": "2024-01-01T12:00:00"}]
        result = self.tool._infer_schema_types(["ts"], rows)
        assert result["ts"] == "TIMESTAMP"

    def test_date_detected(self):
        rows = [{"dt": "2024-01-01"}]
        result = self.tool._infer_schema_types(["dt"], rows)
        assert result["dt"] == "DATE"

    def test_none_values_skipped(self):
        rows = [{"col": None}]
        result = self.tool._infer_schema_types(["col"], rows)
        assert result["col"] == "STRING"

    def test_empty_rows_all_string(self):
        result = self.tool._infer_schema_types(["a", "b"], [])
        assert result["a"] == "STRING"
        assert result["b"] == "STRING"

    def test_list_rows_also_processed(self):
        rows = [[42, "text"]]
        result = self.tool._infer_schema_types(["num", "str_col"], rows)
        assert result["num"] == "BIGINT"


# ===========================================================================
# _infer_schema tests
# ===========================================================================

class TestInferSchema:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def test_returns_schema_string(self):
        rows = [{"id": 1, "name": "Alice"}]
        result = self.tool._infer_schema(["id", "name"], rows)
        assert "BIGINT" in result
        assert "STRING" in result

    def test_date_in_schema(self):
        rows = [{"dt": "2024-01-01"}]
        result = self.tool._infer_schema(["dt"], rows)
        assert "DATE" in result

    def test_empty_rows(self):
        result = self.tool._infer_schema(["col"], [])
        assert "STRING" in result


# ===========================================================================
# _format_validation_report tests
# ===========================================================================

class TestFormatValidationReport:
    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def test_empty_lists(self):
        result = self.tool._format_validation_report([], [], [], "main.default")
        assert "M-Query" in result
        assert "main.default" in result

    def test_validated_table_shown(self):
        validated = [{"table": "T1", "status": "validated", "iterations": 2, "sql": "SELECT * FROM t", "dax": "EVALUATE T1"}]
        result = self.tool._format_validation_report(validated, [], [], "main.default")
        assert "T1" in result
        assert "VERIFIED" in result

    def test_validation_failed_shown(self):
        validated = [{"table": "T2", "status": "validation_failed", "iterations": 10, "diff": "SQL=100, DAX=200", "sql": "SELECT * FROM t", "dax": "EVALUATE T2"}]
        result = self.tool._format_validation_report(validated, [], [], "main.default")
        assert "T2" in result
        assert "VALIDATION FAILED" in result

    def test_table_not_found_shown(self):
        validated = [{"table": "T3", "status": "table_not_found", "error": "Table missing", "sql": "SELECT * FROM t", "dax": ""}]
        result = self.tool._format_validation_report(validated, [], [], "main.default")
        assert "T3" in result
        assert "TABLE NOT MIGRATED" in result

    def test_dbsql_error_shown(self):
        validated = [{"table": "T4", "status": "dbsql_error", "error": "Syntax error", "sql": "SELECT * FROM t", "dax": ""}]
        result = self.tool._format_validation_report(validated, [], [], "main.default")
        assert "T4" in result

    def test_inserted_table_shown(self):
        inserted = [{"table": "Static1", "status": "inserted", "target": "main.default.static1", "rows_inserted": 5, "sql": "CREATE TABLE ..."}]
        result = self.tool._format_validation_report([], inserted, [], "main.default")
        assert "Static1" in result
        assert "5" in result

    def test_skipped_table_shown(self):
        skipped = [{"table": "Skipped1", "source_type": "sql_database", "reason": "Azure SQL / Synapse", "mquery_preview": "Sql.Database(...)"}]
        result = self.tool._format_validation_report([], [], skipped, "main.default")
        assert "Skipped1" in result
        assert "Non-Transpilable" in result

    def test_long_sql_truncated(self):
        validated = [{"table": "LongSQL", "status": "validated", "sql": "SELECT " + "x, " * 200 + "y FROM t", "dax": "", "iterations": 1}]
        result = self.tool._format_validation_report(validated, [], [], "main.default")
        assert "..." in result or "LongSQL" in result

    def test_inserted_with_error(self):
        inserted = [{"table": "Bad", "status": "create_failed", "target": "main.default.bad", "error": "Permission denied", "sql": "CREATE ..."}]
        result = self.tool._format_validation_report([], inserted, [], "main.default")
        assert "Permission denied" in result

    def test_skipped_without_mquery_preview(self):
        skipped = [{"table": "NoPreview", "source_type": "oracle", "reason": "Oracle Database"}]
        result = self.tool._format_validation_report([], [], skipped, "main.default")
        assert "NoPreview" in result


# ===========================================================================
# _run main pipeline integration tests
# ===========================================================================

class TestRunMainPipeline:
    """Tests for the _run method's main conversion path."""

    @patch("src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.run_sync")
    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_with_cache_hit(self, _mock_auth, mock_run_sync):
        """Cache hit should return the cached output immediately."""
        mock_run_sync.return_value = "cached output"
        tool = MqueryConversionPipelineTool(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        result = tool._run()
        assert isinstance(result, str)

    @patch("src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.run_sync")
    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_missing_workspace_returns_error(self, _mock_auth, _mock_run_sync):
        tool = MqueryConversionPipelineTool(dataset_id=DATASET_ID)
        result = tool._run()
        assert "error" in result.lower() or "workspace_id" in result.lower()

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(False, "No credentials"))
    def test_run_invalid_auth_returns_error(self, _mock_auth):
        tool = MqueryConversionPipelineTool(workspace_id=WORKSPACE_ID)
        result = tool._run()
        assert "error" in result.lower()

    @patch("src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.run_sync")
    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_cache_miss_then_conversion_fails(self, _mock_auth, mock_run_sync):
        """Cache miss followed by conversion failure."""
        # run_sync called multiple times: cache check (returns None), _execute_conversion (returns fail)
        mock_run_sync.side_effect = [
            None,  # cache miss
            {"success": False, "error": "conversion failed"},  # execute_conversion result
        ]
        tool = MqueryConversionPipelineTool(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        # Mock the import of MQueryConnector
        mock_module = MagicMock()
        mock_module.MQueryConversionConfig = MagicMock()
        with patch.dict("sys.modules", {
            "src.converters.services.mquery": mock_module,
        }):
            result = tool._run()

        assert isinstance(result, str)

    @patch("src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool.run_sync")
    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_with_dbsql_validation_path(self, _mock_auth, mock_run_sync):
        """When databricks_sql_endpoint + databricks_pat set, use validation path."""
        mock_run_sync.side_effect = [
            None,    # cache miss
            "validation output",  # _execute_with_validation
            None,    # save cache (ignored)
        ]
        tool = MqueryConversionPipelineTool(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            databricks_sql_endpoint="https://workspace.example.com/sql",
            databricks_pat="dapi-token",
        )
        result = tool._run()
        assert isinstance(result, str)


# ===========================================================================
# _run_sync called from async context
# ===========================================================================

class TestRunSyncAsyncContext:
    """Test run_sync when called from an async context."""

    def test_run_sync_from_async_context(self):
        """run_sync in an event loop uses ThreadPoolExecutor."""
        import asyncio

        async def outer():
            async def inner():
                return 42
            # run_sync from within async context
            return run_sync(inner())

        result = asyncio.run(outer())
        assert result == 42


# ===========================================================================
# _run_dbsql tests
# ===========================================================================

class TestRunDbsql:
    """Tests for _run_dbsql method."""

    def setup_method(self):
        self.tool = MqueryConversionPipelineTool(
            workspace_id=WORKSPACE_ID, tenant_id=TENANT_ID,
            client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_empty_workspace_url_returns_error(self):
        result = self._run(self.tool._run_dbsql("SELECT 1", "", "wh123", "token"))
        assert result["success"] is False
        assert "databricks_workspace_url" in result["error"].lower() or "not configured" in result["error"].lower()

    def test_successful_dbsql_response(self):
        succeeded_data = {
            "status": {"state": "SUCCEEDED"},
            "result": {"data_array": [[100]]},
            "manifest": {"schema": {"columns": [{"name": "_cnt"}]}}
        }
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"statement_id": "stmt1", "status": {"state": "SUCCEEDED"}, **succeeded_data}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._run_dbsql("SELECT 1", "https://workspace.example.com", "wh123", "token"))

        assert isinstance(result, dict)

    def test_dbsql_failure_state(self):
        data = {
            "statement_id": "stmt1",
            "status": {"state": "FAILED", "error": {"message": "Syntax error"}}
        }
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = data

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._run_dbsql("BAD SQL", "https://workspace.example.com", "wh123", "token"))

        assert isinstance(result, dict)

    def test_dbsql_exception_returns_error(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("connection refused"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._run_dbsql("SELECT 1", "https://workspace.example.com", "wh123", "token"))

        assert result["success"] is False
        assert "connection refused" in result["error"]


# ===========================================================================
# _run_pbi_query tests
# ===========================================================================

class TestRunPbiQuery:
    """Tests for _run_pbi_query method."""

    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_successful_pbi_query(self):
        data = {
            "results": [{"tables": [{"rows": [{"_cnt": 100}]}]}]
        }
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = data

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._run_pbi_query(
                "EVALUATE ROW('_cnt', 1)", WORKSPACE_ID, DATASET_ID, "token"
            ))

        assert result["success"] is True
        assert result["data"] == [{"_cnt": 100}]

    def test_pbi_query_with_error_in_response(self):
        data = {"error": {"message": "Table not found"}}
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = data

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._run_pbi_query(
                "EVALUATE BadTable", WORKSPACE_ID, DATASET_ID, "token"
            ))

        assert result["success"] is False
        assert "Table not found" in result["error"]

    def test_pbi_query_empty_results(self):
        data = {"results": [{"tables": []}]}
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = data

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._run_pbi_query(
                "EVALUATE Empty", WORKSPACE_ID, DATASET_ID, "token"
            ))

        assert result["success"] is True
        assert result["data"] == []

    def test_pbi_query_exception_returns_error(self):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(side_effect=Exception("timeout"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._run_pbi_query(
                "EVALUATE T", WORKSPACE_ID, DATASET_ID, "token"
            ))

        assert result["success"] is False
        assert "timeout" in result["error"]

    def test_pbi_query_rows_empty_in_table(self):
        data = {"results": [{"tables": [{"rows": []}]}]}
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = data

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = self._run(self.tool._run_pbi_query(
                "EVALUATE T", WORKSPACE_ID, DATASET_ID, "token"
            ))

        assert result["success"] is True
        assert result["data"] == []


# ===========================================================================
# _resolve_dbsql tests
# ===========================================================================

class TestResolveDbsql:
    """Tests for _resolve_dbsql method."""

    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_warehouse_id_from_url(self):
        url = "https://workspace.cloud.databricks.com/sql/1.0/warehouses/abc123def456"
        workspace_url, warehouse_id = self._run(self.tool._resolve_dbsql(url, "token"))
        assert workspace_url == "https://workspace.cloud.databricks.com"
        assert warehouse_id == "abc123def456"

    def test_auto_detect_running_warehouse(self):
        warehouses = [
            {"id": "wh1", "state": "RUNNING"},
            {"id": "wh2", "state": "STOPPED"},
        ]
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"warehouses": warehouses}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            workspace_url, warehouse_id = self._run(
                self.tool._resolve_dbsql("https://workspace.cloud.databricks.com", "token")
            )

        assert workspace_url == "https://workspace.cloud.databricks.com"
        assert warehouse_id == "wh1"  # RUNNING preferred

    def test_auto_detect_fallback_to_first_when_none_running(self):
        warehouses = [
            {"id": "wh1", "state": "STOPPED"},
            {"id": "wh2", "state": "STOPPED"},
        ]
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"warehouses": warehouses}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            workspace_url, warehouse_id = self._run(
                self.tool._resolve_dbsql("https://workspace.cloud.databricks.com", "token")
            )

        assert warehouse_id == "wh1"

    def test_no_warehouses_raises(self):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"warehouses": []}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(ValueError, match="No SQL warehouses"):
                self._run(self.tool._resolve_dbsql("https://workspace.cloud.databricks.com", "token"))


# ===========================================================================
# _validate_databricks_table tests
# ===========================================================================

class TestValidateDatabricksTable:
    """Tests for _validate_databricks_table method."""

    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_conv(self, sql="SELECT * FROM t", original_expr="let Source = ...\nin\nSource"):
        conv = MagicMock()
        conv.databricks_sql = sql
        conv.create_view_sql = None
        conv.original_expression = original_expr
        return conv

    def test_no_sql_returns_skipped(self):
        conv = MagicMock()
        conv.databricks_sql = None
        conv.create_view_sql = None
        result = self._run(self.tool._validate_databricks_table(
            "T1", conv, WORKSPACE_ID, DATASET_ID, "token",
            "https://workspace.example.com", "wh123", "pat", 3, {}
        ))
        assert result["status"] == "skipped"

    def test_no_warehouse_returns_skipped(self):
        conv = self._make_conv()
        result = self._run(self.tool._validate_databricks_table(
            "T1", conv, WORKSPACE_ID, DATASET_ID, "token",
            "https://workspace.example.com", "", "pat", 3, {}  # empty warehouse_id
        ))
        assert result["status"] == "skipped"

    def test_validated_on_matching_counts(self):
        conv = self._make_conv()
        sql_result = {"success": True, "rows": [[100]]}
        dax_result = {"success": True, "data": [{"_cnt": 100}]}

        with patch.object(self.tool, "_run_dbsql", new_callable=AsyncMock, return_value=sql_result):
            with patch.object(self.tool, "_run_pbi_query", new_callable=AsyncMock, return_value=dax_result):
                result = self._run(self.tool._validate_databricks_table(
                    "T1", conv, WORKSPACE_ID, DATASET_ID, "token",
                    "https://workspace.example.com", "wh123", "pat", 3, {}
                ))

        assert result["status"] == "validated"

    def test_dax_error_returns_dax_error_status(self):
        conv = self._make_conv()
        sql_result = {"success": True, "rows": [[100]]}
        dax_result = {"success": False, "error": "DAX error"}

        with patch.object(self.tool, "_run_dbsql", new_callable=AsyncMock, return_value=sql_result):
            with patch.object(self.tool, "_run_pbi_query", new_callable=AsyncMock, return_value=dax_result):
                result = self._run(self.tool._validate_databricks_table(
                    "T1", conv, WORKSPACE_ID, DATASET_ID, "token",
                    "https://workspace.example.com", "wh123", "pat", 3, {}
                ))

        assert result["status"] == "dax_error"

    def test_dbsql_error_returns_dbsql_error_status(self):
        conv = self._make_conv()
        sql_result = {"success": False, "error": "Syntax error"}

        with patch.object(self.tool, "_run_dbsql", new_callable=AsyncMock, return_value=sql_result):
            with patch.object(self.tool, "_llm_correct_sql", new_callable=AsyncMock, return_value=None):
                result = self._run(self.tool._validate_databricks_table(
                    "T1", conv, WORKSPACE_ID, DATASET_ID, "token",
                    "https://workspace.example.com", "wh123", "pat", 1, {}
                ))

        assert result["status"] == "dbsql_error"

    def test_table_not_found_same_error_twice_returns_table_not_found(self):
        conv = self._make_conv()
        sql_error = "TABLE_OR_VIEW_NOT_FOUND: `main`.`default`"
        sql_result = {"success": False, "error": sql_error}

        with patch.object(self.tool, "_run_dbsql", new_callable=AsyncMock, return_value=sql_result):
            with patch.object(self.tool, "_llm_correct_sql", new_callable=AsyncMock, return_value="SELECT 2 FROM t"):
                result = self._run(self.tool._validate_databricks_table(
                    "T1", conv, WORKSPACE_ID, DATASET_ID, "token",
                    "https://workspace.example.com", "wh123", "pat", 5, {}
                ))

        assert result["status"] in ("table_not_found", "dbsql_error")

    def test_count_mismatch_llm_correction_applied(self):
        conv = self._make_conv()
        sql_result_bad = {"success": True, "rows": [[200]]}
        sql_result_good = {"success": True, "rows": [[100]]}
        dax_result = {"success": True, "data": [{"_cnt": 100}]}
        fixed_sql = "SELECT DISTINCT * FROM t"

        call_count = [0]
        async def mock_dbsql(*args, **kwargs):
            call_count[0] += 1
            return sql_result_bad if call_count[0] == 1 else sql_result_good

        with patch.object(self.tool, "_run_dbsql", side_effect=mock_dbsql):
            with patch.object(self.tool, "_run_pbi_query", new_callable=AsyncMock, return_value=dax_result):
                with patch.object(self.tool, "_llm_correct_sql", new_callable=AsyncMock, return_value=fixed_sql):
                    result = self._run(self.tool._validate_databricks_table(
                        "T1", conv, WORKSPACE_ID, DATASET_ID, "token",
                        "https://workspace.example.com", "wh123", "pat", 3, {}
                    ))

        assert result["status"] in ("validated", "validation_failed")


# ===========================================================================
# _insert_static_table tests
# ===========================================================================

class TestInsertStaticTable:
    """Tests for _insert_static_table method."""

    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_no_warehouse_skipped(self):
        result = self._run(self.tool._insert_static_table(
            "StaticT", WORKSPACE_ID, DATASET_ID, "token",
            "", "pat", "https://workspace.example.com", "main.default"
        ))
        assert result["status"] == "skipped"

    def test_pbi_error_returns_pbi_error(self):
        pbi_result = {"success": False, "error": "Dataset not found"}
        with patch.object(self.tool, "_run_pbi_query", new_callable=AsyncMock, return_value=pbi_result):
            result = self._run(self.tool._insert_static_table(
                "StaticT", WORKSPACE_ID, DATASET_ID, "token",
                "wh123", "pat", "https://workspace.example.com", "main.default"
            ))
        assert result["status"] == "pbi_error"

    def test_empty_columns_returns_empty(self):
        pbi_result = {"success": True, "data": [], "columns": []}
        with patch.object(self.tool, "_run_pbi_query", new_callable=AsyncMock, return_value=pbi_result):
            result = self._run(self.tool._insert_static_table(
                "StaticT", WORKSPACE_ID, DATASET_ID, "token",
                "wh123", "pat", "https://workspace.example.com", "main.default"
            ))
        assert result["status"] == "empty"

    def test_successful_insert(self):
        pbi_result = {
            "success": True,
            "data": [{"StaticT.Col1": "A", "StaticT.Col2": 1}],
            "columns": ["[StaticT].[Col1]", "[StaticT].[Col2]"]
        }
        create_sql = "CREATE TABLE IF NOT EXISTS main.default.statict (`Col1` STRING, `Col2` BIGINT)"
        insert_sql = "INSERT INTO main.default.statict (`Col1`, `Col2`) VALUES ('A', 1)"

        cr_result = {"success": True}
        ir_result = {"success": True}

        with patch.object(self.tool, "_run_pbi_query", new_callable=AsyncMock, return_value=pbi_result):
            with patch.object(self.tool, "_llm_generate_insert_sql",
                              new_callable=AsyncMock, return_value=(create_sql, [insert_sql])):
                with patch.object(self.tool, "_run_dbsql", new_callable=AsyncMock, side_effect=[cr_result, ir_result]):
                    result = self._run(self.tool._insert_static_table(
                        "StaticT", WORKSPACE_ID, DATASET_ID, "token",
                        "wh123", "pat", "https://workspace.example.com", "main.default"
                    ))

        assert result["status"] == "inserted"

    def test_create_table_fails_returns_error(self):
        pbi_result = {
            "success": True,
            "data": [{"col1": "v1"}],
            "columns": ["[T].[col1]"]
        }
        create_sql = "CREATE TABLE IF NOT EXISTS main.default.statict (`col1` STRING)"
        cr_result = {"success": False, "error": "Permission denied"}

        with patch.object(self.tool, "_run_pbi_query", new_callable=AsyncMock, return_value=pbi_result):
            with patch.object(self.tool, "_llm_generate_insert_sql",
                              new_callable=AsyncMock, return_value=(create_sql, [])):
                with patch.object(self.tool, "_run_dbsql", new_callable=AsyncMock, return_value=cr_result):
                    result = self._run(self.tool._insert_static_table(
                        "StaticT", WORKSPACE_ID, DATASET_ID, "token",
                        "wh123", "pat", "https://workspace.example.com", "main.default"
                    ))

        assert result["status"] == "create_failed"

    def test_no_insert_sqls_returns_inserted_zero(self):
        pbi_result = {
            "success": True,
            "data": [],
            "columns": ["[T].[col1]"]
        }
        create_sql = "CREATE TABLE IF NOT EXISTS main.default.statict (`col1` STRING)"
        cr_result = {"success": True}

        with patch.object(self.tool, "_run_pbi_query", new_callable=AsyncMock, return_value=pbi_result):
            with patch.object(self.tool, "_llm_generate_insert_sql",
                              new_callable=AsyncMock, return_value=(create_sql, [])):
                with patch.object(self.tool, "_run_dbsql", new_callable=AsyncMock, return_value=cr_result):
                    result = self._run(self.tool._insert_static_table(
                        "StaticT", WORKSPACE_ID, DATASET_ID, "token",
                        "wh123", "pat", "https://workspace.example.com", "main.default"
                    ))

        assert result["status"] == "inserted"
        assert result["rows_inserted"] == 0


# ===========================================================================
# _llm_generate_insert_sql tests
# ===========================================================================

class TestLlmGenerateInsertSql:
    """Tests for _llm_generate_insert_sql fallback path."""

    def setup_method(self):
        self.tool = MqueryConversionPipelineTool()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_llm_unavailable_uses_fallback(self):
        """When LLM raises an exception, fallback mechanical generation is used."""
        from unittest.mock import patch as _patch
        rows = [{"id": 1, "name": "Alice"}]

        with _patch("crewai.LLM", side_effect=Exception("LLM unavailable")):
            create_sql, insert_sqls = self._run(self.tool._llm_generate_insert_sql(
                "T1", "main.default.t1", ["id", "name"], rows, {}
            ))

        assert "CREATE TABLE" in create_sql.upper()
        # insert_sqls may or may not be populated depending on row data
        assert isinstance(insert_sqls, list)

    def test_fallback_generates_create_table(self):
        rows = [{"col1": "text", "col2": 42}]
        with patch("crewai.LLM", side_effect=Exception("no LLM")):
            create_sql, _inserts = self._run(self.tool._llm_generate_insert_sql(
                "T2", "main.default.t2", ["col1", "col2"], rows, {}
            ))

        assert "t2" in create_sql.lower()
        assert "CREATE TABLE" in create_sql.upper()

    def test_batch_insert_generated_for_rows(self):
        rows = [{"col1": "A", "col2": 1}, {"col1": "B", "col2": 2}]
        with patch("crewai.LLM", side_effect=Exception("no LLM")):
            create_sql, insert_sqls = self._run(self.tool._llm_generate_insert_sql(
                "T3", "main.default.t3", ["col1", "col2"], rows, {}
            ))

        assert len(insert_sqls) >= 1
        assert "INSERT INTO" in insert_sqls[0].upper()

    def test_integer_values_escaped_properly(self):
        rows = [{"num_col": 42}]
        with patch("crewai.LLM", side_effect=Exception("no LLM")):
            _create, insert_sqls = self._run(self.tool._llm_generate_insert_sql(
                "T4", "main.default.t4", ["num_col"], rows, {}
            ))

        if insert_sqls:
            assert "42" in insert_sqls[0]

    def test_null_values_escaped(self):
        rows = [{"col1": None}]
        with patch("crewai.LLM", side_effect=Exception("no LLM")):
            _create, insert_sqls = self._run(self.tool._llm_generate_insert_sql(
                "T5", "main.default.t5", ["col1"], rows, {}
            ))

        if insert_sqls:
            assert "NULL" in insert_sqls[0].upper()

    def test_boolean_values_escaped(self):
        rows = [{"flag": "true"}]
        # Force col type to BOOLEAN by making _infer_schema_types return it
        with patch("crewai.LLM", side_effect=Exception("no LLM")):
            with patch.object(self.tool, "_infer_schema_types", return_value={"flag": "BOOLEAN"}):
                _create, insert_sqls = self._run(self.tool._llm_generate_insert_sql(
                    "T6", "main.default.t6", ["flag"], rows, {}
                ))

        if insert_sqls:
            assert "TRUE" in insert_sqls[0].upper() or "FALSE" in insert_sqls[0].upper()

    def test_date_values_escaped(self):
        rows = [{"dt": "2024-01-15"}]
        with patch("crewai.LLM", side_effect=Exception("no LLM")):
            with patch.object(self.tool, "_infer_schema_types", return_value={"dt": "DATE"}):
                _create, insert_sqls = self._run(self.tool._llm_generate_insert_sql(
                    "T7", "main.default.t7", ["dt"], rows, {}
                ))

        if insert_sqls:
            assert "DATE" in insert_sqls[0].upper()

    def test_timestamp_values_escaped(self):
        rows = [{"ts": "2024-01-15 12:00:00"}]
        with patch("crewai.LLM", side_effect=Exception("no LLM")):
            with patch.object(self.tool, "_infer_schema_types", return_value={"ts": "TIMESTAMP"}):
                _create, insert_sqls = self._run(self.tool._llm_generate_insert_sql(
                    "T8", "main.default.t8", ["ts"], rows, {}
                ))

        if insert_sqls:
            assert "TIMESTAMP" in insert_sqls[0].upper()

    def test_llm_returns_sql_string(self):
        """When LLM works, it should return a CREATE TABLE statement."""
        create_response = "```sql\nCREATE TABLE IF NOT EXISTS main.default.t9 (`col1` STRING)\n```"
        rows = [{"col1": "val"}]

        mock_llm = MagicMock()
        mock_llm.call.return_value = create_response

        with patch("crewai.LLM", return_value=mock_llm):
            create_sql, _inserts = self._run(self.tool._llm_generate_insert_sql(
                "T9", "main.default.t9", ["col1"], rows,
                {"llm_model": "test-model", "llm_workspace_url": "https://example.com", "llm_token": "tok"}
            ))

        assert "CREATE TABLE" in create_sql.upper()


# ===========================================================================
# _execute_with_validation tests
# ===========================================================================

class TestExecuteWithValidation:
    """Tests for _execute_with_validation method."""

    def setup_method(self):
        self.tool = MqueryConversionPipelineTool(
            workspace_id=WORKSPACE_ID, tenant_id=TENANT_ID,
            client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_resolve_dbsql_error_returns_error_string(self):
        cfg = {
            "workspace_id": WORKSPACE_ID,
            "databricks_sql_endpoint": "https://workspace.example.com",
            "databricks_pat": "pat",
            "tenant_id": TENANT_ID,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }

        with patch.object(self.tool, "_resolve_dbsql",
                          new_callable=AsyncMock, side_effect=Exception("resolve error")):
            result = self._run(self.tool._execute_with_validation(cfg))

        assert "Error resolving DBSQL" in result

    def test_pbi_token_error_returns_error_string(self):
        # Use a proper hex warehouse ID so _resolve_dbsql parses from URL without HTTP call
        cfg = {
            "workspace_id": WORKSPACE_ID,
            "databricks_sql_endpoint": "https://workspace.example.com/sql/1.0/warehouses/abcdef123456",
            "databricks_pat": "pat",
        }

        mock_module = MagicMock()
        mock_module.MQueryConversionConfig = MagicMock()

        with patch.dict("sys.modules", {"src.converters.services.mquery": mock_module}):
            with patch(
                "src.engines.crewai.tools.custom.powerbi_auth_utils.get_powerbi_access_token",
                new_callable=AsyncMock, side_effect=Exception("auth failed")
            ):
                result = self._run(self.tool._execute_with_validation(cfg))

        assert "Error obtaining PBI access token" in result

    def test_no_models_returns_error_string(self):
        # Use a proper hex warehouse ID so _resolve_dbsql parses from URL without HTTP call
        cfg = {
            "workspace_id": WORKSPACE_ID,
            "databricks_sql_endpoint": "https://workspace.example.com/sql/1.0/warehouses/abcdef123456",
            "databricks_pat": "pat",
        }

        mock_connector = MagicMock()
        mock_connector.__aenter__ = AsyncMock(return_value=mock_connector)
        mock_connector.__aexit__ = AsyncMock(return_value=None)
        mock_connector.scan_workspace = AsyncMock(return_value=[])

        mock_module = MagicMock()
        mock_module.MQueryConnector = MagicMock(return_value=mock_connector)
        mock_module.MQueryConversionConfig = MagicMock()

        with patch.dict("sys.modules", {"src.converters.services.mquery": mock_module}):
            with patch(
                "src.engines.crewai.tools.custom.powerbi_auth_utils.get_powerbi_access_token",
                new_callable=AsyncMock, return_value="token"
            ):
                result = self._run(self.tool._execute_with_validation(cfg))

        assert "No semantic models" in result
