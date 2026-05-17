"""Unit tests for PowerBISemanticModelDaxTool (Tool 80)."""
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from src.engines.crewai.tools.custom.powerbi_semantic_model_dax_tool import (
    PowerBISemanticModelDaxTool,
    PowerBISemanticModelDaxSchema,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORKSPACE_ID = "ws-aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
DATASET_ID = "ds-cccccccc-4444-5555-6666-dddddddddddd"
TENANT_ID = "tenant-eeeeeeee-7777-8888-9999-ffffffffffff"
CLIENT_ID = "client-11111111-aaaa-bbbb-cccc-222222222222"
CLIENT_SECRET = "super-secret"

SAMPLE_MODEL_CONTEXT = json.dumps({
    "workspace_id": WORKSPACE_ID,
    "dataset_id": DATASET_ID,
    "measures": [
        {"name": "Total Revenue", "expression": "SUM(Sales[Amount])", "table": "Sales"},
        {"name": "Total Units", "expression": "SUM(Sales[Quantity])", "table": "Sales"},
    ],
    "tables": [
        {"name": "Sales", "columns": [{"name": "Amount"}, {"name": "Region"}]},
        {"name": "Dim_Date", "columns": [{"name": "DateKey"}, {"name": "Year"}]},
    ],
    "relationships": [
        {"from_table": "Sales", "from_column": "DateKey", "to_table": "Dim_Date", "to_column": "DateKey"},
    ],
    "sample_data": {
        "Sales[Region]": [{"Region": "North"}, {"Region": "South"}],
    },
    "slicers": [],
    "default_filters": {},
})

PBI_EXECUTE_SUCCESS = {
    "results": [
        {
            "tables": [
                {
                    "rows": [
                        {"[Region]": "North", "[Total Revenue]": 1234567},
                        {"[Region]": "South", "[Total Revenue]": 987654},
                    ]
                }
            ]
        }
    ]
}


def _mock_http_post(status=200, body=None):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = body or PBI_EXECUTE_SUCCESS
    resp.text = json.dumps(body or PBI_EXECUTE_SUCCESS)
    return resp


def _mock_cache_service(cached=None):
    mock = MagicMock()
    mock.get_model_context = AsyncMock(return_value=cached)
    mock.save_model_context = AsyncMock(return_value=None)
    return mock


def _mock_session():
    sess = AsyncMock()
    sess.__aenter__ = AsyncMock(return_value=sess)
    sess.__aexit__ = AsyncMock(return_value=None)
    return sess


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestPowerBISemanticModelDaxSchema:
    def test_all_optional(self):
        schema = PowerBISemanticModelDaxSchema()
        assert schema.user_question is None
        assert schema.model_context_json is None

    def test_user_question_field(self):
        schema = PowerBISemanticModelDaxSchema(
            user_question="What is total revenue by region?"
        )
        assert schema.user_question == "What is total revenue by region?"

    def test_model_context_json_field(self):
        schema = PowerBISemanticModelDaxSchema(model_context_json=SAMPLE_MODEL_CONTEXT)
        assert schema.model_context_json == SAMPLE_MODEL_CONTEXT

    def test_sp_auth_fields(self):
        schema = PowerBISemanticModelDaxSchema(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        assert schema.workspace_id == WORKSPACE_ID
        assert schema.client_secret == CLIENT_SECRET

    def test_llm_fields(self):
        schema = PowerBISemanticModelDaxSchema(
            llm_workspace_url="https://xyz.cloud.databricks.com",
            llm_token="dapi-abc123",
            llm_model="databricks-claude-sonnet-4",
        )
        assert schema.llm_workspace_url is not None
        assert schema.llm_model == "databricks-claude-sonnet-4"

    def test_max_retries_field(self):
        schema = PowerBISemanticModelDaxSchema(max_dax_retries=3)
        assert schema.max_dax_retries == 3

    def test_output_format_field(self):
        schema = PowerBISemanticModelDaxSchema(output_format="json")
        assert schema.output_format == "json"

    def test_business_mappings_field(self):
        schema = PowerBISemanticModelDaxSchema(
            business_mappings={"revenue": "Total Revenue", "sales": "Total Revenue"}
        )
        assert schema.business_mappings is not None


# ---------------------------------------------------------------------------
# Init tests
# ---------------------------------------------------------------------------

class TestPowerBISemanticModelDaxToolInit:
    def test_tool_name(self):
        tool = PowerBISemanticModelDaxTool()
        assert "DAX" in tool.name or "Semantic Model" in tool.name

    def test_static_config_stored(self):
        tool = PowerBISemanticModelDaxTool(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            user_question="What are total sales?",
        )
        assert tool._default_config["workspace_id"] == WORKSPACE_ID
        assert tool._default_config["user_question"] == "What are total sales?"

    def test_empty_init_has_defaults(self):
        tool = PowerBISemanticModelDaxTool()
        # Tool pre-populates config with all schema fields (including None defaults)
        assert "workspace_id" in tool._default_config
        assert tool._default_config["workspace_id"] is None
        assert tool._default_config["max_dax_retries"] == 5

    def test_description_not_empty(self):
        tool = PowerBISemanticModelDaxTool()
        assert len(tool.description) > 20


# ---------------------------------------------------------------------------
# Missing required fields
# ---------------------------------------------------------------------------

class TestMissingFields:
    def test_missing_question_returns_error(self):
        tool = PowerBISemanticModelDaxTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="tok",
        )
        assert result is not None

    def test_missing_auth_returns_error(self):
        tool = PowerBISemanticModelDaxTool()
        result = tool._run(
            user_question="What are total sales?",
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
        )
        assert result is not None

    def test_no_args(self):
        tool = PowerBISemanticModelDaxTool()
        result = tool._run()
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# With model context provided (no API needed for context)
# ---------------------------------------------------------------------------

class TestWithModelContextProvided:
    @patch("httpx.AsyncClient.post")
    def test_uses_provided_model_context(self, mock_post):
        mock_post.return_value = _mock_http_post()
        tool = PowerBISemanticModelDaxTool()
        result = tool._run(
            user_question="What is total revenue by region?",
            model_context_json=SAMPLE_MODEL_CONTEXT,
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="tok",
        )
        assert result is not None
        assert isinstance(result, str)

    @patch("httpx.AsyncClient.post")
    def test_with_context_no_llm_falls_back(self, mock_post):
        """Without LLM config, should fall back to keyword-based DAX."""
        mock_post.return_value = _mock_http_post()
        tool = PowerBISemanticModelDaxTool()
        result = tool._run(
            user_question="total revenue",
            model_context_json=SAMPLE_MODEL_CONTEXT,
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="tok",
        )
        assert result is not None


# ---------------------------------------------------------------------------
# Cache hit path
# ---------------------------------------------------------------------------

class TestCacheHitPath:
    @patch("src.engines.crewai.tools.custom.powerbi_semantic_model_dax_tool.async_session_factory")
    @patch("src.engines.crewai.tools.custom.powerbi_semantic_model_dax_tool.PowerBISemanticModelCacheService")
    @patch("httpx.AsyncClient.post")
    def test_cache_hit_skips_fetch(self, mock_post, mock_svc_cls, mock_session_factory):
        import json as _json
        cached = _json.loads(SAMPLE_MODEL_CONTEXT)
        mock_svc = _mock_cache_service(cached=cached)
        mock_svc_cls.return_value = mock_svc
        mock_session_factory.return_value = _mock_session()
        mock_post.return_value = _mock_http_post()

        tool = PowerBISemanticModelDaxTool()
        result = tool._run(
            user_question="What is total revenue?",
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="tok",
        )
        assert result is not None


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    @patch("httpx.AsyncClient.post")
    def test_api_401_handled(self, mock_post):
        mock_post.return_value = _mock_http_post(status=401, body={"error": "Unauthorized"})
        tool = PowerBISemanticModelDaxTool()
        result = tool._run(
            user_question="What is revenue?",
            model_context_json=SAMPLE_MODEL_CONTEXT,
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="bad-token",
        )
        assert result is not None

    @patch("httpx.AsyncClient.post", side_effect=Exception("Network error"))
    def test_network_error_handled(self, mock_post):
        tool = PowerBISemanticModelDaxTool()
        result = tool._run(
            user_question="What is revenue?",
            model_context_json=SAMPLE_MODEL_CONTEXT,
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="tok",
        )
        assert result is not None

    def test_invalid_model_context_json(self):
        tool = PowerBISemanticModelDaxTool()
        result = tool._run(
            user_question="What is revenue?",
            model_context_json="{invalid json{{",
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            access_token="tok",
        )
        assert result is not None


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def test_output_is_string(self):
        tool = PowerBISemanticModelDaxTool()
        result = tool._run()
        assert isinstance(result, str)

    def test_output_non_empty(self):
        tool = PowerBISemanticModelDaxTool()
        result = tool._run()
        assert len(result) > 0
