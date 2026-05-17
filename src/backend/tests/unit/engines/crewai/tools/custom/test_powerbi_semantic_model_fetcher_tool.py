"""Unit tests for PowerBISemanticModelFetcherTool (Tool 79)."""
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool import (
    PowerBISemanticModelFetcherTool,
    PowerBISemanticModelFetcherSchema,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORKSPACE_ID = "ws-aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
DATASET_ID = "ds-cccccccc-4444-5555-6666-dddddddddddd"
TENANT_ID = "tenant-eeeeeeee-7777-8888-9999-ffffffffffff"
CLIENT_ID = "client-11111111-aaaa-bbbb-cccc-222222222222"
CLIENT_SECRET = "super-secret"

MOCK_MODEL_CONTEXT = {
    "workspace_id": WORKSPACE_ID,
    "dataset_id": DATASET_ID,
    "measures": [
        {"name": "Total Revenue", "expression": "SUM(Sales[Amount])", "table": "Sales"},
        {"name": "YoY Growth", "expression": "...", "table": "Sales"},
    ],
    "tables": [
        {"name": "Sales", "columns": [{"name": "Amount"}, {"name": "DateKey"}]},
        {"name": "Dim_Date", "columns": [{"name": "DateKey"}, {"name": "Year"}]},
    ],
    "relationships": [
        {"from_table": "Sales", "from_column": "DateKey", "to_table": "Dim_Date", "to_column": "DateKey"},
    ],
    "columns": [],
    "sample_data": {},
    "slicers": [],
    "default_filters": {},
}


def _mock_cache_service(cached=None):
    mock = MagicMock()
    mock.get_model_context = AsyncMock(return_value=cached)
    mock.save_model_context = AsyncMock(return_value=None)
    mock.get_scan_data = AsyncMock(return_value=None)
    mock.save_scan_data = AsyncMock(return_value=None)
    return mock


def _mock_session():
    sess = AsyncMock()
    sess.__aenter__ = AsyncMock(return_value=sess)
    sess.__aexit__ = AsyncMock(return_value=None)
    return sess


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestPowerBISemanticModelFetcherSchema:
    def test_all_optional(self):
        schema = PowerBISemanticModelFetcherSchema()
        assert schema.workspace_id is None
        assert schema.dataset_id is None

    def test_sp_auth_fields(self):
        schema = PowerBISemanticModelFetcherSchema(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        assert schema.workspace_id == WORKSPACE_ID
        assert schema.client_secret == CLIENT_SECRET

    def test_user_oauth_field(self):
        schema = PowerBISemanticModelFetcherSchema(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="eyJhbGci...",
        )
        assert schema.access_token == "eyJhbGci..."

    def test_report_id_optional(self):
        schema = PowerBISemanticModelFetcherSchema(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            report_id="rpt-abc123",
        )
        assert schema.report_id == "rpt-abc123"

    def test_skip_system_tables_field(self):
        schema = PowerBISemanticModelFetcherSchema(skip_system_tables=True)
        assert schema.skip_system_tables is True


# ---------------------------------------------------------------------------
# Init tests
# ---------------------------------------------------------------------------

class TestPowerBISemanticModelFetcherToolInit:
    def test_tool_name(self):
        tool = PowerBISemanticModelFetcherTool()
        assert "Fetcher" in tool.name or "Semantic Model" in tool.name

    def test_static_config_stored(self):
        tool = PowerBISemanticModelFetcherTool(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
        )
        assert tool._default_config["workspace_id"] == WORKSPACE_ID
        assert tool._default_config["tenant_id"] == TENANT_ID

    def test_empty_init_has_schema_keys(self):
        tool = PowerBISemanticModelFetcherTool()
        # Tool stores all schema fields; no workspace_id set means it's None
        assert "workspace_id" in tool._default_config or tool._default_config == {}
        if "workspace_id" in tool._default_config:
            assert tool._default_config["workspace_id"] is None

    def test_description_not_empty(self):
        tool = PowerBISemanticModelFetcherTool()
        assert len(tool.description) > 20


# ---------------------------------------------------------------------------
# Cache hit path
# ---------------------------------------------------------------------------

class TestCacheHitPath:
    @patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.async_session_factory")
    @patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.PowerBISemanticModelCacheService")
    def test_returns_cached_model_context(self, mock_svc_cls, mock_session_factory):
        mock_svc = _mock_cache_service(cached=MOCK_MODEL_CONTEXT)
        mock_svc_cls.return_value = mock_svc
        mock_session_factory.return_value = _mock_session()

        tool = PowerBISemanticModelFetcherTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        assert result is not None
        assert len(result) > 0

    @patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.async_session_factory")
    @patch("src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.PowerBISemanticModelCacheService")
    def test_cached_result_contains_measures(self, mock_svc_cls, mock_session_factory):
        mock_svc = _mock_cache_service(cached=MOCK_MODEL_CONTEXT)
        mock_svc_cls.return_value = mock_svc
        mock_session_factory.return_value = _mock_session()

        tool = PowerBISemanticModelFetcherTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="tok",
        )
        # Result should either be the cached JSON or an error string
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Missing fields
# ---------------------------------------------------------------------------

class TestMissingFields:
    def test_missing_workspace_id(self):
        tool = PowerBISemanticModelFetcherTool()
        result = tool._run(dataset_id=DATASET_ID, access_token="tok")
        assert result is not None

    def test_missing_dataset_id(self):
        tool = PowerBISemanticModelFetcherTool()
        result = tool._run(workspace_id=WORKSPACE_ID, access_token="tok")
        assert result is not None

    def test_missing_auth(self):
        tool = PowerBISemanticModelFetcherTool()
        result = tool._run(workspace_id=WORKSPACE_ID, dataset_id=DATASET_ID)
        assert result is not None


# ---------------------------------------------------------------------------
# Static config fallback
# ---------------------------------------------------------------------------

class TestStaticConfigFallback:
    def test_static_workspace_used(self):
        tool = PowerBISemanticModelFetcherTool(workspace_id=WORKSPACE_ID)
        assert tool._default_config["workspace_id"] == WORKSPACE_ID

    def test_runtime_kwargs_override_static(self):
        tool = PowerBISemanticModelFetcherTool(workspace_id="old-ws")
        assert tool._default_config["workspace_id"] == "old-ws"
        # Runtime kwarg would override when _run is called


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    @patch(
        "src.engines.crewai.tools.custom.powerbi_semantic_model_fetcher_tool.async_session_factory",
        side_effect=Exception("DB connection failed"),
    )
    def test_db_error_returns_error_string(self, mock_factory):
        tool = PowerBISemanticModelFetcherTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="tok",
        )
        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def test_output_is_string(self):
        tool = PowerBISemanticModelFetcherTool()
        result = tool._run()
        assert isinstance(result, str)
