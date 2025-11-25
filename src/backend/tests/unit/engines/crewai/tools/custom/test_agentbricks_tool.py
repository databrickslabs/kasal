"""Unit tests for AgentBricksTool."""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
import aiohttp

from src.engines.crewai.tools.custom.agentbricks_tool import (
    AgentBricksTool,
    AgentBricksInput
)


class TestAgentBricksInput:
    """Test cases for AgentBricksInput schema."""

    def test_input_with_string(self):
        """Test input with a simple string question."""
        input_data = AgentBricksInput(question="What is the weather?")
        assert input_data.question == "What is the weather?"

    def test_input_with_dict_description(self):
        """Test input parsing from dict with description field."""
        input_data = AgentBricksInput(question={"description": "Test question"})
        assert input_data.question == "Test question"

    def test_input_with_dict_text(self):
        """Test input parsing from dict with text field."""
        input_data = AgentBricksInput(question={"text": "Test question"})
        assert input_data.question == "Test question"

    def test_input_with_dict_query(self):
        """Test input parsing from dict with query field."""
        input_data = AgentBricksInput(question={"query": "Test question"})
        assert input_data.question == "Test question"

    def test_input_with_dict_question(self):
        """Test input parsing from dict with question field."""
        input_data = AgentBricksInput(question={"question": "Test question"})
        assert input_data.question == "Test question"

    def test_input_with_dict_fallback(self):
        """Test input parsing falls back to string conversion for unknown dict."""
        input_data = AgentBricksInput(question={"unknown": "value"})
        assert "unknown" in input_data.question
        assert "value" in input_data.question

    def test_input_with_other_type(self):
        """Test input converts other types to string."""
        input_data = AgentBricksInput(question=12345)
        assert input_data.question == "12345"


class TestAgentBricksToolInitialization:
    """Test cases for AgentBricksTool initialization."""

    def test_initialization_default_values(self):
        """Test that the tool initializes with correct default values."""
        tool = AgentBricksTool()

        assert tool.name == "AgentBricksTool"
        assert "AgentBricks" in tool.description
        assert "Mosaic AI Agent Bricks" in tool.description
        assert tool.args_schema == AgentBricksInput
        assert tool._timeout == 120
        assert tool._endpoint_name is None
        assert tool._user_token is None
        assert tool._group_id is None
        assert tool._return_trace is False
        assert tool._custom_inputs is None

    def test_initialization_with_endpoint_name(self):
        """Test initialization with endpointName in config."""
        tool = AgentBricksTool(tool_config={"endpointName": "test-endpoint"})
        assert tool._endpoint_name == "test-endpoint"

    def test_initialization_with_endpoint_name_list(self):
        """Test initialization with endpointName as list."""
        tool = AgentBricksTool(tool_config={"endpointName": ["endpoint-1", "endpoint-2"]})
        assert tool._endpoint_name == "endpoint-1"

    def test_initialization_with_endpoint_key(self):
        """Test initialization with endpoint key in config."""
        tool = AgentBricksTool(tool_config={"endpoint": "alt-endpoint"})
        assert tool._endpoint_name == "alt-endpoint"

    def test_initialization_with_endpoint_name_key(self):
        """Test initialization with endpoint_name key in config."""
        tool = AgentBricksTool(tool_config={"endpoint_name": "snake-endpoint"})
        assert tool._endpoint_name == "snake-endpoint"

    def test_initialization_with_timeout(self):
        """Test initialization with custom timeout."""
        tool = AgentBricksTool(tool_config={"timeout": 60})
        assert tool._timeout == 60

    def test_initialization_with_return_trace(self):
        """Test initialization with return_trace flag."""
        tool = AgentBricksTool(tool_config={"return_trace": True})
        assert tool._return_trace is True

    def test_initialization_with_custom_inputs(self):
        """Test initialization with custom inputs."""
        custom = {"context": "test context", "mode": "fast"}
        tool = AgentBricksTool(tool_config={"custom_inputs": custom})
        assert tool._custom_inputs == custom

    def test_initialization_with_user_token(self):
        """Test initialization with user token."""
        tool = AgentBricksTool(user_token="obo-token-123")
        assert tool._user_token == "obo-token-123"

    def test_initialization_with_group_id(self):
        """Test initialization with group_id."""
        tool = AgentBricksTool(group_id="group-abc")
        assert tool._group_id == "group-abc"

    def test_initialization_with_tool_id(self):
        """Test initialization with tool_id."""
        tool = AgentBricksTool(tool_id=42)
        assert tool._tool_id == 42

    def test_initialization_with_result_as_answer(self):
        """Test initialization with result_as_answer flag."""
        tool = AgentBricksTool(result_as_answer=True)
        # This is passed to the base class
        assert tool.result_as_answer is True

    def test_aliases_configured(self):
        """Test that tool aliases are configured."""
        tool = AgentBricksTool()
        assert "AgentBricks" in tool.aliases
        assert "DatabricksAgent" in tool.aliases
        assert "MosaicAgent" in tool.aliases


class TestAgentBricksToolMethods:
    """Test cases for AgentBricksTool methods."""

    def test_set_user_token(self):
        """Test set_user_token method."""
        tool = AgentBricksTool()
        assert tool._user_token is None

        tool.set_user_token("new-token-123")
        assert tool._user_token == "new-token-123"

    def test_make_url(self):
        """Test _make_url helper method."""
        tool = AgentBricksTool()

        # Test with trailing slash on workspace URL
        url = tool._make_url("https://workspace.databricks.com/", "/api/endpoint")
        assert url == "https://workspace.databricks.com/api/endpoint"

        # Test without trailing slash
        url = tool._make_url("https://workspace.databricks.com", "/api/endpoint")
        assert url == "https://workspace.databricks.com/api/endpoint"

        # Test path without leading slash
        url = tool._make_url("https://workspace.databricks.com", "api/endpoint")
        assert url == "https://workspace.databricks.com/api/endpoint"


class TestAgentBricksToolAsyncMethods:
    """Test cases for AgentBricksTool async methods."""

    @pytest.fixture
    def tool_with_endpoint(self):
        """Create a tool with endpoint configured."""
        return AgentBricksTool(
            tool_config={"endpointName": "test-agent"},
            user_token="test-token"
        )

    @pytest.mark.asyncio
    async def test_get_workspace_url_success(self, tool_with_endpoint):
        """Test _get_workspace_url returns workspace URL."""
        # Patch at the import location inside the method
        with patch('src.utils.databricks_auth._databricks_auth') as mock_auth:
            mock_auth.get_workspace_url = AsyncMock(return_value="https://workspace.databricks.com")

            url = await tool_with_endpoint._get_workspace_url()

            assert url == "https://workspace.databricks.com"

    @pytest.mark.asyncio
    async def test_get_workspace_url_empty_raises(self, tool_with_endpoint):
        """Test _get_workspace_url raises when URL is empty."""
        with patch('src.utils.databricks_auth._databricks_auth') as mock_auth:
            mock_auth.get_workspace_url = AsyncMock(return_value=None)

            with pytest.raises(ValueError, match="Could not obtain workspace URL"):
                await tool_with_endpoint._get_workspace_url()

    @pytest.mark.asyncio
    async def test_get_auth_headers_success(self, tool_with_endpoint):
        """Test _get_auth_headers returns headers from auth context."""
        mock_auth_context = MagicMock()
        mock_auth_context.get_headers.return_value = {"Authorization": "Bearer test-token"}

        # Patch where the function is actually called
        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_context

            with patch('src.utils.user_context.UserContext'):
                headers = await tool_with_endpoint._get_auth_headers()

            assert headers == {"Authorization": "Bearer test-token"}

    @pytest.mark.asyncio
    async def test_get_auth_headers_no_auth(self, tool_with_endpoint):
        """Test _get_auth_headers returns None when no auth available."""
        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = None

            with patch('src.utils.user_context.UserContext'):
                headers = await tool_with_endpoint._get_auth_headers()

            assert headers is None

    @pytest.mark.asyncio
    async def test_get_auth_headers_with_group_id(self):
        """Test _get_auth_headers sets UserContext with group_id."""
        tool = AgentBricksTool(
            tool_config={"endpointName": "test-agent"},
            group_id="test-group-123"
        )

        mock_auth_context = MagicMock()
        mock_auth_context.get_headers.return_value = {"Authorization": "Bearer token"}

        with patch('src.utils.databricks_auth.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth_context

            with patch('src.utils.user_context.UserContext') as mock_user_context:
                with patch('src.utils.user_context.GroupContext') as mock_group_context:
                    await tool._get_auth_headers()

                    # Verify GroupContext was created with group_id
                    mock_group_context.assert_called_once()
                    call_kwargs = mock_group_context.call_args[1]
                    assert "test-group-123" in call_kwargs['group_ids']


class TestAgentBricksToolQueryEndpoint:
    """Test cases for _query_agentbricks_endpoint method."""

    @pytest.fixture
    def tool_with_endpoint(self):
        """Create a tool with endpoint configured."""
        return AgentBricksTool(
            tool_config={"endpointName": "test-agent"},
            user_token="test-token"
        )

    @pytest.mark.asyncio
    async def test_query_endpoint_success(self, tool_with_endpoint):
        """Test successful query to AgentBricks endpoint."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "choices": [{
                "message": {"content": "This is the response"}
            }]
        })

        with patch.object(tool_with_endpoint, '_get_workspace_url', return_value="https://workspace.databricks.com"):
            with patch.object(tool_with_endpoint, '_get_auth_headers', return_value={"Authorization": "Bearer test"}):
                with patch('aiohttp.ClientSession') as mock_session_class:
                    mock_session = MagicMock()
                    mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                    mock_session_class.return_value.__aexit__ = AsyncMock(return_value=None)

                    mock_post_context = MagicMock()
                    mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
                    mock_post_context.__aexit__ = AsyncMock(return_value=None)
                    mock_session.post.return_value = mock_post_context

                    result = await tool_with_endpoint._query_agentbricks_endpoint("Test question")

        assert result == "This is the response"

    @pytest.mark.asyncio
    async def test_query_endpoint_no_endpoint_configured(self):
        """Test query returns error when endpoint not configured."""
        tool = AgentBricksTool()  # No endpoint

        # The method returns an error string rather than raising an exception
        result = await tool._query_agentbricks_endpoint("Test question")
        assert "Error" in result or "endpoint name is required" in result

    @pytest.mark.asyncio
    async def test_query_endpoint_no_auth_headers(self, tool_with_endpoint):
        """Test query returns error when no auth headers available."""
        with patch.object(tool_with_endpoint, '_get_workspace_url', new_callable=AsyncMock, return_value="https://workspace.databricks.com"):
            with patch.object(tool_with_endpoint, '_get_auth_headers', new_callable=AsyncMock, return_value=None):
                # The method returns an error string rather than raising an exception
                result = await tool_with_endpoint._query_agentbricks_endpoint("Test question")
                assert "Error" in result or "No authentication headers" in result

    @pytest.mark.asyncio
    async def test_query_endpoint_response_format(self, tool_with_endpoint):
        """Test query handles different response formats."""
        # Test direct response field
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"response": "Direct response"})

        with patch.object(tool_with_endpoint, '_get_workspace_url', return_value="https://workspace.databricks.com"):
            with patch.object(tool_with_endpoint, '_get_auth_headers', return_value={"Authorization": "Bearer test"}):
                with patch('aiohttp.ClientSession') as mock_session_class:
                    mock_session = MagicMock()
                    mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                    mock_session_class.return_value.__aexit__ = AsyncMock(return_value=None)

                    mock_post_context = MagicMock()
                    mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
                    mock_post_context.__aexit__ = AsyncMock(return_value=None)
                    mock_session.post.return_value = mock_post_context

                    result = await tool_with_endpoint._query_agentbricks_endpoint("Test question")

        assert result == "Direct response"

    @pytest.mark.asyncio
    async def test_query_endpoint_predictions_format(self, tool_with_endpoint):
        """Test query handles predictions format."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"predictions": ["Prediction result"]})

        with patch.object(tool_with_endpoint, '_get_workspace_url', return_value="https://workspace.databricks.com"):
            with patch.object(tool_with_endpoint, '_get_auth_headers', return_value={"Authorization": "Bearer test"}):
                with patch('aiohttp.ClientSession') as mock_session_class:
                    mock_session = MagicMock()
                    mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                    mock_session_class.return_value.__aexit__ = AsyncMock(return_value=None)

                    mock_post_context = MagicMock()
                    mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
                    mock_post_context.__aexit__ = AsyncMock(return_value=None)
                    mock_session.post.return_value = mock_post_context

                    result = await tool_with_endpoint._query_agentbricks_endpoint("Test question")

        assert result == "Prediction result"

    @pytest.mark.asyncio
    async def test_query_endpoint_timeout(self, tool_with_endpoint):
        """Test query handles timeout error."""
        with patch.object(tool_with_endpoint, '_get_workspace_url', return_value="https://workspace.databricks.com"):
            with patch.object(tool_with_endpoint, '_get_auth_headers', return_value={"Authorization": "Bearer test"}):
                with patch('aiohttp.ClientSession') as mock_session_class:
                    mock_session = MagicMock()
                    mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                    mock_session_class.return_value.__aexit__ = AsyncMock(return_value=None)

                    mock_post_context = MagicMock()
                    mock_post_context.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError())
                    mock_session.post.return_value = mock_post_context

                    result = await tool_with_endpoint._query_agentbricks_endpoint("Test question")

        assert "timed out" in result.lower()

    @pytest.mark.asyncio
    async def test_query_endpoint_connection_error(self, tool_with_endpoint):
        """Test query handles connection error."""
        with patch.object(tool_with_endpoint, '_get_workspace_url', return_value="https://workspace.databricks.com"):
            with patch.object(tool_with_endpoint, '_get_auth_headers', return_value={"Authorization": "Bearer test"}):
                with patch('aiohttp.ClientSession') as mock_session_class:
                    mock_session = MagicMock()
                    mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                    mock_session_class.return_value.__aexit__ = AsyncMock(return_value=None)

                    mock_post_context = MagicMock()
                    mock_post_context.__aenter__ = AsyncMock(side_effect=aiohttp.ClientConnectionError("Connection failed"))
                    mock_session.post.return_value = mock_post_context

                    result = await tool_with_endpoint._query_agentbricks_endpoint("Test question")

        assert "Error connecting" in result

    @pytest.mark.asyncio
    async def test_query_endpoint_http_error(self, tool_with_endpoint):
        """Test query handles HTTP error."""
        mock_response = MagicMock()
        mock_response.status = 401
        mock_response.text = AsyncMock(return_value="Unauthorized")

        with patch.object(tool_with_endpoint, '_get_workspace_url', return_value="https://workspace.databricks.com"):
            with patch.object(tool_with_endpoint, '_get_auth_headers', return_value={"Authorization": "Bearer test"}):
                with patch('aiohttp.ClientSession') as mock_session_class:
                    mock_session = MagicMock()
                    mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                    mock_session_class.return_value.__aexit__ = AsyncMock(return_value=None)

                    mock_post_context = MagicMock()
                    mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
                    mock_post_context.__aexit__ = AsyncMock(return_value=None)
                    mock_session.post.return_value = mock_post_context

                    result = await tool_with_endpoint._query_agentbricks_endpoint("Test question")

        assert "HTTP 401" in result


class TestAgentBricksToolRun:
    """Test cases for _run and _run_async methods."""

    @pytest.fixture
    def tool_with_endpoint(self):
        """Create a tool with endpoint configured."""
        return AgentBricksTool(
            tool_config={"endpointName": "test-agent"},
            user_token="test-token"
        )

    @pytest.mark.asyncio
    async def test_run_async_success(self, tool_with_endpoint):
        """Test successful async run."""
        with patch.object(tool_with_endpoint, '_query_agentbricks_endpoint', return_value="Success response"):
            result = await tool_with_endpoint._run_async("Test question")

        assert result == "Success response"

    @pytest.mark.asyncio
    async def test_run_async_no_endpoint(self):
        """Test async run without endpoint configured."""
        tool = AgentBricksTool()  # No endpoint

        result = await tool._run_async("Test question")

        assert "ERROR" in result
        assert "endpoint name is not configured" in result

    @pytest.mark.asyncio
    async def test_run_async_empty_question(self, tool_with_endpoint):
        """Test async run with empty question."""
        result = await tool_with_endpoint._run_async("")

        assert "provide a specific question" in result.lower()

    @pytest.mark.asyncio
    async def test_run_async_none_question(self, tool_with_endpoint):
        """Test async run with 'None' as question."""
        result = await tool_with_endpoint._run_async("None")

        assert "provide a specific question" in result.lower()

    @pytest.mark.asyncio
    async def test_run_async_exception_handling(self, tool_with_endpoint):
        """Test async run handles exceptions."""
        with patch.object(tool_with_endpoint, '_query_agentbricks_endpoint', side_effect=Exception("Unexpected error")):
            result = await tool_with_endpoint._run_async("Test question")

        assert "Error using AgentBricks" in result
        assert "Unexpected error" in result

    def test_run_sync_wrapper(self, tool_with_endpoint):
        """Test synchronous _run wrapper."""
        with patch.object(tool_with_endpoint, '_run_async', new_callable=AsyncMock) as mock_async:
            mock_async.return_value = "Sync result"

            result = tool_with_endpoint._run("Test question")

        assert result == "Sync result"


class TestAgentBricksToolWithTrace:
    """Test cases for trace functionality."""

    @pytest.mark.asyncio
    async def test_query_with_trace_enabled(self):
        """Test query includes trace when enabled."""
        tool = AgentBricksTool(
            tool_config={
                "endpointName": "test-agent",
                "return_trace": True
            },
            user_token="test-token"
        )

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "choices": [{"message": {"content": "Response"}}],
            "trace": {"steps": ["step1", "step2"]}
        })

        with patch.object(tool, '_get_workspace_url', return_value="https://workspace.databricks.com"):
            with patch.object(tool, '_get_auth_headers', return_value={"Authorization": "Bearer test"}):
                with patch('aiohttp.ClientSession') as mock_session_class:
                    mock_session = MagicMock()
                    mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                    mock_session_class.return_value.__aexit__ = AsyncMock(return_value=None)

                    mock_post_context = MagicMock()
                    mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
                    mock_post_context.__aexit__ = AsyncMock(return_value=None)
                    mock_session.post.return_value = mock_post_context

                    result = await tool._query_agentbricks_endpoint("Test question")

        assert "Response" in result
        assert "Trace:" in result


class TestAgentBricksToolWithCustomInputs:
    """Test cases for custom inputs functionality."""

    @pytest.mark.asyncio
    async def test_query_with_custom_inputs(self):
        """Test query includes custom inputs in payload."""
        tool = AgentBricksTool(
            tool_config={
                "endpointName": "test-agent",
                "custom_inputs": {"context": "test context", "mode": "fast"}
            },
            user_token="test-token"
        )

        captured_payload = None

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "choices": [{"message": {"content": "Response"}}]
        })

        with patch.object(tool, '_get_workspace_url', return_value="https://workspace.databricks.com"):
            with patch.object(tool, '_get_auth_headers', return_value={"Authorization": "Bearer test"}):
                with patch('aiohttp.ClientSession') as mock_session_class:
                    mock_session = MagicMock()
                    mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                    mock_session_class.return_value.__aexit__ = AsyncMock(return_value=None)

                    def capture_post(*args, **kwargs):
                        nonlocal captured_payload
                        captured_payload = kwargs.get('json')
                        mock_post_context = MagicMock()
                        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
                        mock_post_context.__aexit__ = AsyncMock(return_value=None)
                        return mock_post_context

                    mock_session.post.side_effect = capture_post

                    await tool._query_agentbricks_endpoint("Test question")

        assert captured_payload is not None
        assert captured_payload.get("context") == "test context"
        assert captured_payload.get("mode") == "fast"
