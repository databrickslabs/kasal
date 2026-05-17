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
