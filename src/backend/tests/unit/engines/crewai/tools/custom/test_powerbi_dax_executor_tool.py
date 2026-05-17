"""Unit tests for PowerBIDaxExecutorTool (Tool 82)."""
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from src.engines.crewai.tools.custom.powerbi_dax_executor_tool import (
    PowerBIDaxExecutorTool,
    PowerBIDaxExecutorSchema,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

DAX_QUERY = "EVALUATE SUMMARIZECOLUMNS('Geography'[Region], \"Revenue\", [Total Revenue])"
WORKSPACE_ID = "ws-aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
DATASET_ID = "ds-cccccccc-4444-5555-6666-dddddddddddd"

PBI_SUCCESS_RESPONSE = {
    "results": [
        {
            "tables": [
                {
                    "rows": [
                        {"[Region]": "North", "[Revenue]": 1234567},
                        {"[Region]": "South", "[Revenue]": 987654},
                    ]
                }
            ]
        }
    ]
}


def _mock_token():
    return "Bearer fake-token-xyz"


def _mock_http_post(status=200, body=None):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = body or PBI_SUCCESS_RESPONSE
    resp.text = json.dumps(body or PBI_SUCCESS_RESPONSE)
    return resp


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestPowerBIDaxExecutorSchema:
    def test_all_optional_fields(self):
        schema = PowerBIDaxExecutorSchema()
        assert schema.workspace_id is None
        assert schema.dataset_id is None
        assert schema.dax_query is None
        assert schema.output_format == "markdown"

    def test_output_format_default(self):
        schema = PowerBIDaxExecutorSchema(dax_query=DAX_QUERY)
        assert schema.output_format == "markdown"

    def test_service_principal_fields(self):
        schema = PowerBIDaxExecutorSchema(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
            auth_method="service_principal",
            tenant_id="tenant-123",
            client_id="client-123",
            client_secret="secret-abc",
        )
        assert schema.auth_method == "service_principal"
        assert schema.client_secret == "secret-abc"

    def test_user_oauth_fields(self):
        schema = PowerBIDaxExecutorSchema(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
            auth_method="user_oauth",
            access_token="eyJhbGci...",
        )
        assert schema.auth_method == "user_oauth"
        assert schema.access_token == "eyJhbGci..."

    def test_output_format_json(self):
        schema = PowerBIDaxExecutorSchema(output_format="json")
        assert schema.output_format == "json"


# ---------------------------------------------------------------------------
# Initialization tests
# ---------------------------------------------------------------------------

class TestPowerBIDaxExecutorToolInit:
    def test_tool_name(self):
        tool = PowerBIDaxExecutorTool()
        assert tool.name == "Power BI DAX Executor"

    def test_static_config_stored(self):
        tool = PowerBIDaxExecutorTool(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
        )
        assert tool._default_config["workspace_id"] == WORKSPACE_ID
        assert tool._default_config["dax_query"] == DAX_QUERY

    def test_empty_init(self):
        tool = PowerBIDaxExecutorTool()
        assert tool._default_config == {}


# ---------------------------------------------------------------------------
# Missing required fields
# ---------------------------------------------------------------------------

class TestMissingFields:
    def test_missing_workspace_id(self):
        tool = PowerBIDaxExecutorTool()
        result = tool._run(dataset_id=DATASET_ID, dax_query=DAX_QUERY, access_token="tok")
        assert "error" in result.lower() or "workspace" in result.lower()

    def test_missing_dataset_id(self):
        tool = PowerBIDaxExecutorTool()
        result = tool._run(workspace_id=WORKSPACE_ID, dax_query=DAX_QUERY, access_token="tok")
        assert "error" in result.lower() or "dataset" in result.lower()

    def test_missing_dax_query(self):
        tool = PowerBIDaxExecutorTool()
        result = tool._run(workspace_id=WORKSPACE_ID, dataset_id=DATASET_ID, access_token="tok")
        assert "error" in result.lower() or "query" in result.lower()

    def test_missing_auth(self):
        tool = PowerBIDaxExecutorTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
        )
        assert "error" in result.lower() or "auth" in result.lower() or "token" in result.lower()


# ---------------------------------------------------------------------------
# Successful execution
# ---------------------------------------------------------------------------

class TestSuccessfulExecution:
    @patch(
        "src.engines.crewai.tools.custom.powerbi_dax_executor_tool.get_powerbi_access_token",
        return_value="fake-token",
    )
    @patch("httpx.AsyncClient.post")
    def test_markdown_output(self, mock_post, mock_token):
        mock_post.return_value = _mock_http_post()
        tool = PowerBIDaxExecutorTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
            auth_method="user_oauth",
            access_token="tok",
            output_format="markdown",
        )
        assert "North" in result or "Region" in result or "|" in result

    @patch(
        "src.engines.crewai.tools.custom.powerbi_dax_executor_tool.get_powerbi_access_token",
        return_value="fake-token",
    )
    @patch("httpx.AsyncClient.post")
    def test_json_output(self, mock_post, mock_token):
        mock_post.return_value = _mock_http_post()
        tool = PowerBIDaxExecutorTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
            auth_method="user_oauth",
            access_token="tok",
            output_format="json",
        )
        # JSON output is a list of row dicts or a dict with result keys
        data = json.loads(result)
        assert isinstance(data, (list, dict))

    @patch(
        "src.engines.crewai.tools.custom.powerbi_dax_executor_tool.get_powerbi_access_token",
        return_value="fake-token",
    )
    @patch("httpx.AsyncClient.post")
    def test_static_config_used_when_no_kwargs(self, mock_post, mock_token):
        mock_post.return_value = _mock_http_post()
        tool = PowerBIDaxExecutorTool(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
            auth_method="user_oauth",
            access_token="tok",
        )
        result = tool._run()
        assert "error" not in result.lower() or "North" in result or "|" in result


# ---------------------------------------------------------------------------
# API error handling
# ---------------------------------------------------------------------------

class TestApiErrorHandling:
    @patch(
        "src.engines.crewai.tools.custom.powerbi_dax_executor_tool.get_powerbi_access_token",
        return_value="fake-token",
    )
    @patch("httpx.AsyncClient.post")
    def test_api_401_returns_error(self, mock_post, mock_token):
        mock_post.return_value = _mock_http_post(status=401, body={"error": {"code": "Unauthorized"}})
        tool = PowerBIDaxExecutorTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
            auth_method="user_oauth",
            access_token="bad-tok",
        )
        assert "error" in result.lower() or "401" in result or "unauthorized" in result.lower()

    @patch(
        "src.engines.crewai.tools.custom.powerbi_dax_executor_tool.get_powerbi_access_token",
        side_effect=Exception("Token acquisition failed"),
    )
    def test_auth_failure_returns_error(self, mock_token):
        tool = PowerBIDaxExecutorTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
            auth_method="service_principal",
            tenant_id="t",
            client_id="c",
            client_secret="s",
        )
        assert "error" in result.lower()

    @patch(
        "src.engines.crewai.tools.custom.powerbi_dax_executor_tool.get_powerbi_access_token",
        return_value="fake-token",
    )
    @patch("httpx.AsyncClient.post", side_effect=Exception("Connection timeout"))
    def test_network_error_returns_error(self, mock_post, mock_token):
        tool = PowerBIDaxExecutorTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
            auth_method="user_oauth",
            access_token="tok",
        )
        result_lower = result.lower()
        assert "error" in result_lower or "failed" in result_lower or "timeout" in result_lower


# ---------------------------------------------------------------------------
# Output format tests
# ---------------------------------------------------------------------------

class TestOutputFormats:
    @patch(
        "src.engines.crewai.tools.custom.powerbi_dax_executor_tool.get_powerbi_access_token",
        return_value="fake-token",
    )
    @patch("httpx.AsyncClient.post")
    def test_empty_result_set(self, mock_post, mock_token):
        mock_post.return_value = _mock_http_post(
            body={"results": [{"tables": [{"rows": []}]}]}
        )
        tool = PowerBIDaxExecutorTool()
        result = tool._run(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dax_query=DAX_QUERY,
            auth_method="user_oauth",
            access_token="tok",
        )
        assert result is not None
        assert len(result) > 0
