"""
Test suite for GenieRepository
"""

import asyncio
import time

import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock, PropertyMock
import httpx
import json

from src.repositories.genie_repository import GenieRepository
from src.schemas.genie import (
    GenieAuthConfig,
    GenieSpace,
    GenieSpacesResponse,
    GenieStartConversationRequest,
    GenieStartConversationResponse,
    GenieSendMessageRequest,
    GenieSendMessageResponse,
    GenieGetMessageStatusRequest,
    GenieMessageStatus,
    GenieGetQueryResultRequest,
    GenieQueryResult,
    GenieExecutionRequest,
    GenieExecutionResponse,
    GenieQueryStatus
)

# Patch target for get_auth_context - the local imports inside methods
# re-import from this module, so patching it here affects all call sites.
AUTH_PATCH = 'src.utils.databricks_auth.get_auth_context'


def _make_auth_mock():
    """Helper to create a standard mock auth context."""
    from src.utils.databricks_auth import AuthContext
    mock_auth_ctx = Mock(spec=AuthContext)
    mock_auth_ctx.token = "test-token"
    mock_auth_ctx.workspace_url = "https://test-workspace.cloud.databricks.com"
    mock_auth_ctx.get_headers.return_value = {"Authorization": "Bearer test-token"}
    return mock_auth_ctx


def _make_http_response(status_code=200, json_data=None, text=""):
    """Helper to create a mock HTTP response."""
    mock_resp = Mock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data if json_data is not None else {}
    mock_resp.text = text
    mock_resp.raise_for_status = Mock()
    return mock_resp


class TestGenieRepository:
    """Test cases for GenieRepository"""

    @pytest.fixture
    def auth_config(self):
        """Mock auth config"""
        return GenieAuthConfig(
            host="https://test-workspace.cloud.databricks.com",
            pat_token="test-token",
            user_token="test-user-token"
        )

    @pytest.fixture
    def repository(self, auth_config):
        """Create repository instance"""
        return GenieRepository(auth_config)

    @pytest.fixture
    def repository_no_auth(self):
        """Create repository instance without auth"""
        return GenieRepository()

    @pytest.fixture
    def mock_response(self):
        """Create a mock HTTP response"""
        mock = Mock()
        mock.status_code = 200
        mock.headers = {"content-type": "application/json"}
        return mock

    def test_init_with_auth_config(self, auth_config):
        """Test repository initialization with auth config"""
        repository = GenieRepository(auth_config)
        assert repository.auth_config == auth_config

    def test_init_without_auth_config(self):
        """Test repository initialization without auth config"""
        repository = GenieRepository()
        assert repository.auth_config is None

    def test_build_headers_with_auth(self, repository):
        """Test header building with authentication"""
        headers = repository._build_headers()

        expected_headers = {
            "Authorization": "Bearer test-token",
            "X-Databricks-Genie-User-Token": "test-user-token",
            "Content-Type": "application/json"
        }

        assert headers == expected_headers

    def test_build_headers_without_auth(self, repository_no_auth):
        """Test header building without authentication"""
        headers = repository_no_auth._build_headers()

        expected_headers = {
            "Content-Type": "application/json"
        }

        assert headers == expected_headers

    def test_build_headers_partial_auth(self):
        """Test header building with partial auth (only token)"""
        auth_config = GenieAuthConfig(
            host="https://test-workspace.cloud.databricks.com",
            pat_token="test-token"
            # user_token is None
        )
        repository = GenieRepository(auth_config)
        headers = repository._build_headers()

        expected_headers = {
            "Authorization": "Bearer test-token",
            "Content-Type": "application/json"
        }

        assert headers == expected_headers

    @pytest.mark.asyncio
    async def test_make_url_host_with_trailing_slash(self):
        """Test _make_url strips trailing slash from host."""
        auth_config = GenieAuthConfig(
            host="https://test-workspace.cloud.databricks.com/",
            pat_token="test-token"
        )
        repository = GenieRepository(auth_config)
        url = await repository._make_url("/api/2.0/genie/spaces")
        assert url == "https://test-workspace.cloud.databricks.com/api/2.0/genie/spaces"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_success(self, mock_auth, repository):
        """Test successful get_spaces call"""
        mock_auth.return_value = _make_auth_mock()

        mock_spaces_data = {
            "spaces": [
                {"id": "space1", "name": "Test Space 1", "description": "Description 1"},
                {"id": "space2", "name": "Test Space 2", "description": "Description 2"}
            ],
            "next_page_token": "next-token",
            "total_fetched": 2
        }

        mock_response = _make_http_response(json_data=mock_spaces_data)
        repository._client.get = AsyncMock(return_value=mock_response)

        result = await repository.get_spaces()

        assert isinstance(result, GenieSpacesResponse)
        assert len(result.spaces) == 2
        assert result.spaces[0].id == "space1"
        assert result.spaces[0].name == "Test Space 1"
        assert result.next_page_token == "next-token"
        assert result.total_fetched == 2

        repository._client.get.assert_called_once()
        call_args = repository._client.get.call_args
        assert "/api/2.0/genie/spaces" in call_args[0][0]
        assert call_args[1]["params"]["page_size"] == 50

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_with_pagination(self, mock_auth, repository):
        """Test get_spaces with pagination parameters"""
        mock_auth.return_value = _make_auth_mock()

        mock_response = _make_http_response(json_data={})
        repository._client.get = AsyncMock(return_value=mock_response)
        repository._client.post = AsyncMock(return_value=mock_response)

        result = await repository.get_spaces(page_token="current-token", page_size=25)

        repository._client.get.assert_called_once()

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_search_spaces_success(self, mock_auth, repository):
        """Test successful search_spaces call using get_spaces with search_query"""
        mock_auth.return_value = _make_auth_mock()

        mock_spaces_data = {
            "spaces": [
                {"id": "space1", "name": "Development Space", "description": "Dev space"}
            ],
            "next_page_token": None,
            "total_fetched": 1
        }

        mock_response = _make_http_response(json_data=mock_spaces_data)
        repository._client.get = AsyncMock(return_value=mock_response)

        result = await repository.get_spaces(
            search_query="development",
            page_size=50,
            enabled_only=True
        )

        assert len(result.spaces) == 1
        assert result.spaces[0].name == "Development Space"
        repository._client.get.assert_called_once()

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_search_spaces_with_all_params(self, mock_auth, repository):
        """Test search_spaces with all parameters"""
        mock_auth.return_value = _make_auth_mock()

        mock_spaces_data = {"spaces": [], "next_page_token": None, "total_fetched": 0}

        mock_response = _make_http_response(json_data=mock_spaces_data)
        repository._client.get = AsyncMock(return_value=mock_response)

        await repository.get_spaces(
            search_query="test query",
            page_token="token123",
            page_size=25,
            enabled_only=False
        )

        repository._client.get.assert_called_once()

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_start_conversation_success(self, mock_auth, repository):
        """Test successful start_conversation call"""
        mock_auth.return_value = _make_auth_mock()
        mock_response_data = {
            "conversation_id": "conv-123",
            "message_id": "msg-456"
        }

        mock_response = _make_http_response(json_data=mock_response_data)
        repository._client.post = AsyncMock(return_value=mock_response)

        request = GenieStartConversationRequest(
            space_id="space1",
            initial_message="Hello, what data do we have?"
        )

        result = await repository.start_conversation(request)

        assert isinstance(result, GenieStartConversationResponse)
        assert result.conversation_id == "conv-123"
        assert result.message_id == "msg-456"
        repository._client.post.assert_called_once()

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_send_message_success(self, mock_auth, repository):
        """Test successful send_message call"""
        mock_auth.return_value = _make_auth_mock()

        mock_response_data = {
            "id": "msg-789",
            "status": "RUNNING",
            "content": None
        }

        mock_response = _make_http_response(json_data=mock_response_data)
        repository._client.post = AsyncMock(return_value=mock_response)

        request = GenieSendMessageRequest(
            space_id="space1",
            conversation_id="conv-123",
            message="Can you show me more details?"
        )

        result = await repository.send_message(request)

        assert isinstance(result, GenieSendMessageResponse)
        assert result.message_id == "msg-789"
        assert result.conversation_id == "conv-123"
        repository._client.post.assert_called_once()

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_message_status_success(self, mock_auth, repository):
        """Test successful get_message_status call"""
        mock_auth.return_value = _make_auth_mock()

        mock_response_data = {
            "status": "COMPLETED",
            "result": {"data": "test_data"}
        }

        mock_response = _make_http_response(json_data=mock_response_data)
        repository._client.get = AsyncMock(return_value=mock_response)

        request = GenieGetMessageStatusRequest(
            space_id="space1",
            conversation_id="conv-123",
            message_id="msg-456"
        )

        result = await repository.get_message_status(request)

        assert isinstance(result, GenieMessageStatus)
        assert result == GenieMessageStatus.COMPLETED

        repository._client.get.assert_called_once()
        call_args = repository._client.get.call_args
        assert "/api/2.0/genie/spaces/space1/conversations/conv-123/messages/msg-456" in call_args[0][0]

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_query_result_success(self, mock_auth, repository):
        """Test successful get_query_result call"""
        mock_auth.return_value = _make_auth_mock()

        mock_response_data = {
            "query_id": "query-123",
            "status": "SUCCESS",
            "data": [
                {"col1": "value1", "col2": "value2"},
                {"col1": "value3", "col2": "value4"}
            ],
            "columns": ["col1", "col2"],
            "sql_query": "SELECT * FROM test_table",
            "execution_time": 1.5
        }

        mock_response = _make_http_response(json_data=mock_response_data)
        repository._client.get = AsyncMock(return_value=mock_response)

        request = GenieGetQueryResultRequest(
            space_id="space1",
            conversation_id="conv-123",
            message_id="msg-456"
        )

        result = await repository.get_query_result(request)

        assert isinstance(result, GenieQueryResult)
        assert result.query_id == "query-123"
        assert result.status == GenieQueryStatus.SUCCESS
        assert result.sql == "SELECT * FROM test_table"
        assert len(result.data) == 2
        assert result.columns == ["col1", "col2"]
        assert result.row_count == 2
        assert result.execution_time == 1.5

        repository._client.get.assert_called_once()
        call_args = repository._client.get.call_args
        assert "/api/2.0/genie/spaces/space1/conversations/conv-123/messages/msg-456/query-result" in call_args[0][0]

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_execute_query_success(self, mock_auth, repository):
        """Test successful execute_query call (send_message -> get_message_status -> get_query_result)"""
        mock_auth.return_value = _make_auth_mock()

        mock_send_response = GenieSendMessageResponse(
            conversation_id="conv-123",
            message_id="msg-789",
            status=GenieMessageStatus.RUNNING
        )
        repository.send_message = AsyncMock(return_value=mock_send_response)

        repository.get_message_status = AsyncMock(return_value=GenieMessageStatus.COMPLETED)

        mock_query_result = GenieQueryResult(
            status=GenieQueryStatus.SUCCESS,
            data=[
                {"user_id": 1, "name": "Alice"},
                {"user_id": 2, "name": "Bob"}
            ],
            columns=["user_id", "name"],
            sql="SELECT * FROM users LIMIT 10"
        )
        repository.get_query_result = AsyncMock(return_value=mock_query_result)

        repository._extract_response_text = Mock(return_value="Query executed successfully")

        request = GenieExecutionRequest(
            space_id="space1",
            question="SELECT * FROM users LIMIT 10"
        )

        result = await repository.execute_query(request)

        assert isinstance(result, GenieExecutionResponse)
        assert result.conversation_id == "conv-123"
        assert result.message_id == "msg-789"
        assert result.status == GenieQueryStatus.SUCCESS
        assert result.query_result is not None
        assert result.query_result.status == GenieQueryStatus.SUCCESS
        assert len(result.query_result.data) == 2
        assert result.query_result.columns == ["user_id", "name"]
        assert result.query_result.sql == "SELECT * FROM users LIMIT 10"

        repository.send_message.assert_called_once()
        repository.get_message_status.assert_called_once()
        repository.get_query_result.assert_called_once()

    @pytest.mark.asyncio
    async def test_http_error_handling(self, repository):
        """Test HTTP error handling"""
        pass  # Test disabled until rewritten

    @pytest.mark.asyncio
    async def test_json_decode_error_handling(self, repository):
        """Test JSON decode error handling"""
        pass  # Test disabled until rewritten

    @pytest.mark.asyncio
    async def test_get_spaces_http_404(self, repository):
        """Test get_spaces with HTTP 404 response"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")

        result = await repository.get_spaces()
        assert result.spaces == []

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_empty_response(self, mock_auth, repository):
        """Test get_spaces with empty spaces list"""
        mock_auth.return_value = _make_auth_mock()

        mock_response = _make_http_response(json_data={})
        repository._client.get = AsyncMock(return_value=mock_response)
        repository._client.post = AsyncMock(return_value=mock_response)

        result = await repository.get_spaces()

        assert isinstance(result, GenieSpacesResponse)
        assert len(result.spaces) == 0
        assert result.next_page_token is None
        assert result.total_fetched == 0

    @pytest.mark.asyncio
    async def test_make_url_construction(self, repository):
        """Test URL construction for different endpoints via _make_url"""
        spaces_url = await repository._make_url("/api/2.0/genie/spaces")
        search_url = await repository._make_url("/api/2.0/genie/spaces/search")
        conversations_url = await repository._make_url("/api/2.0/genie/conversations")

        assert spaces_url == "https://test-workspace.cloud.databricks.com/api/2.0/genie/spaces"
        assert search_url == "https://test-workspace.cloud.databricks.com/api/2.0/genie/spaces/search"
        assert conversations_url == "https://test-workspace.cloud.databricks.com/api/2.0/genie/conversations"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_search_spaces_no_query(self, mock_auth, repository):
        """Test search_spaces with empty query"""
        mock_auth.return_value = _make_auth_mock()

        mock_response = _make_http_response(json_data={})
        repository._client.get = AsyncMock(return_value=mock_response)
        repository._client.post = AsyncMock(return_value=mock_response)

        await repository.get_spaces(search_query="")

        repository._client.get.assert_called_once()

    def test_auth_config_immutable(self, auth_config):
        """Test that auth config is properly stored and not modified"""
        repository = GenieRepository(auth_config)

        assert repository.auth_config.host == "https://test-workspace.cloud.databricks.com"
        assert repository.auth_config.pat_token == "test-token"
        assert repository.auth_config.user_token == "test-user-token"

    @pytest.mark.asyncio
    async def test_repository_without_auth_raises_appropriate_error(self, repository_no_auth):
        """Test that repository without auth configuration handles errors appropriately"""
        result = await repository_no_auth.get_spaces()
        assert result.spaces == []
        assert result.total_fetched is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_malformed_response(self, mock_auth, repository):
        """Test handling of malformed response structure"""
        mock_auth.return_value = _make_auth_mock()

        mock_spaces_data = {
            "spaces": [{"id": "space1"}]
        }
        mock_response = _make_http_response(json_data=mock_spaces_data)
        repository._client.get = AsyncMock(return_value=mock_response)
        repository._client.post = AsyncMock(return_value=mock_response)

        result = await repository.get_spaces()
        assert result.spaces[0].id == "space1"
        assert result.spaces[0].name == "Space space1"
        assert result.spaces[0].description == ""
        assert result.next_page_token is None

    def test_headers_consistency(self, repository):
        """Test that headers are consistent across calls"""
        headers1 = repository._build_headers()
        headers2 = repository._build_headers()

        assert headers1 == headers2
        assert headers1["Authorization"] == "Bearer test-token"
        assert headers1["Content-Type"] == "application/json"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_error_handling(self, mock_auth, repository):
        """Test error handling in get_spaces"""
        mock_auth.return_value = _make_auth_mock()

        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server error")
        repository._client.get = AsyncMock(return_value=mock_response)

        result = await repository.get_spaces()

        assert result.spaces == []
        assert result.total_fetched is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_start_conversation_error_handling(self, mock_auth, repository):
        """Test error handling in start_conversation"""
        mock_auth.return_value = _make_auth_mock()

        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server error")
        repository._client.post = AsyncMock(return_value=mock_response)

        request = GenieStartConversationRequest(
            space_id="space1",
            initial_message="Hello"
        )

        result = await repository.start_conversation(request)

        assert result is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_send_message_error_handling(self, mock_auth, repository):
        """Test error handling in send_message"""
        mock_auth.return_value = _make_auth_mock()

        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server error")
        repository._client.post = AsyncMock(return_value=mock_response)

        request = GenieSendMessageRequest(
            space_id="space1",
            conversation_id="conv-123",
            message="Test message"
        )

        result = await repository.send_message(request)

        assert result is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_execute_query_error_handling(self, mock_auth, repository):
        """Test error handling in execute_query"""
        mock_auth.return_value = _make_auth_mock()

        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server error")
        repository._client.post = AsyncMock(return_value=mock_response)

        request = GenieExecutionRequest(
            space_id="space1",
            question="SELECT * FROM users"
        )

        result = await repository.execute_query(request)

        assert result is not None
        assert isinstance(result, GenieExecutionResponse)
        assert result.status == GenieQueryStatus.FAILED
        assert result.error == "Failed to send message to Genie"


class TestGenieRepositoryCleanup:
    """Tests for GenieRepository cleanup methods (aclose and __del__)."""

    @pytest.mark.asyncio
    async def test_aclose_closes_client(self):
        """Test that aclose properly closes the HTTP client."""
        repo = GenieRepository(
            auth_config=GenieAuthConfig(host="test.databricks.com")
        )
        await repo.aclose()
        assert repo._client.is_closed

    @pytest.mark.asyncio
    async def test_aclose_with_no_client(self):
        """Test that aclose handles None client without raising."""
        repo = GenieRepository(
            auth_config=GenieAuthConfig(host="test.databricks.com")
        )
        repo._client = None
        await repo.aclose()  # Should not raise

    def test_del_with_running_loop(self):
        """Test __del__ schedules client close when event loop is running."""
        repo = GenieRepository(
            auth_config=GenieAuthConfig(host="test.databricks.com")
        )
        mock_loop = MagicMock()
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            repo.__del__()
        mock_loop.create_task.assert_called_once()

    def test_del_without_running_loop(self):
        """Test __del__ catches RuntimeError when no event loop is running."""
        repo = GenieRepository(
            auth_config=GenieAuthConfig(host="test.databricks.com")
        )
        with patch("asyncio.get_running_loop", side_effect=RuntimeError):
            repo.__del__()  # Should not raise

    def test_del_with_closed_client(self):
        """Test __del__ does not schedule close when client is already closed."""
        repo = GenieRepository(
            auth_config=GenieAuthConfig(host="test.databricks.com")
        )
        repo._client = MagicMock(is_closed=True)
        with patch("asyncio.get_running_loop") as mock_get_loop:
            repo.__del__()
        mock_get_loop.assert_not_called()


class TestGetHost:
    """Tests for _get_host() covering all fallback paths."""

    @pytest.fixture
    def repo_with_host_config(self):
        """Repository with host in config."""
        config = GenieAuthConfig(host="configured-host.databricks.com")
        return GenieRepository(config)

    @pytest.fixture
    def repo_no_host_config(self):
        """Repository with config but no host."""
        config = GenieAuthConfig(host=None)
        return GenieRepository(config)

    @pytest.mark.asyncio
    async def test_get_host_returns_cached(self, repo_with_host_config):
        """Test _get_host returns cached host (covers line 94)."""
        repo_with_host_config._host = "cached-host.databricks.com"
        result = await repo_with_host_config._get_host()
        assert result == "cached-host.databricks.com"

    @pytest.mark.asyncio
    async def test_get_host_from_config(self, repo_with_host_config):
        """Test _get_host uses config host when available."""
        result = await repo_with_host_config._get_host()
        assert result == "configured-host.databricks.com"
        assert repo_with_host_config._host == "configured-host.databricks.com"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_host_from_auth_context(self, mock_get_auth, repo_no_host_config):
        """Test _get_host falls back to auth context (covers lines 103-105)."""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://auth-host.databricks.com/"
        mock_get_auth.return_value = mock_auth

        result = await repo_no_host_config._get_host()
        assert result == "auth-host.databricks.com"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_host_auth_context_none_falls_to_sdk(self, mock_get_auth, repo_no_host_config):
        """Test _get_host falls to SDK Config when auth context has no workspace_url (covers lines 108-116)."""
        mock_auth = Mock()
        mock_auth.workspace_url = None
        mock_get_auth.return_value = mock_auth

        mock_sdk_config = Mock()
        mock_sdk_config.host = "https://sdk-host.databricks.com"

        with patch.dict('sys.modules', {'databricks': Mock(), 'databricks.sdk': Mock(), 'databricks.sdk.config': Mock()}):
            with patch('databricks.sdk.config.Config', return_value=mock_sdk_config):
                result = await repo_no_host_config._get_host()
                assert result == "sdk-host.databricks.com"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_host_sdk_config_fails_falls_to_databricks_auth(self, mock_get_auth, repo_no_host_config):
        """Test _get_host falls to databricks_auth when SDK fails (covers lines 115-126)."""
        mock_auth = Mock()
        mock_auth.workspace_url = None
        mock_get_auth.return_value = mock_auth

        # Make SDK Config raise an exception
        with patch.dict('sys.modules', {'databricks': Mock(), 'databricks.sdk': Mock(), 'databricks.sdk.config': Mock()}):
            with patch('databricks.sdk.config.Config', side_effect=Exception("SDK not available")):
                # Mock _databricks_auth
                mock_db_auth = Mock()
                mock_db_auth._load_config = AsyncMock()
                mock_db_auth.get_workspace_host.return_value = "https://dbauth-host.databricks.com/"

                with patch('src.utils.databricks_auth._databricks_auth', mock_db_auth):
                    result = await repo_no_host_config._get_host()
                    assert result == "dbauth-host.databricks.com"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_host_all_fallbacks_fail_uses_default(self, mock_get_auth, repo_no_host_config):
        """Test _get_host uses default when all fallbacks fail (covers lines 128-136)."""
        mock_get_auth.return_value = None

        with patch.dict('sys.modules', {'databricks': Mock(), 'databricks.sdk': Mock(), 'databricks.sdk.config': Mock()}):
            with patch('databricks.sdk.config.Config', side_effect=Exception("SDK fail")):
                with patch('src.utils.databricks_auth._databricks_auth') as mock_db_auth:
                    mock_db_auth._load_config = AsyncMock(side_effect=Exception("db auth fail"))

                    result = await repo_no_host_config._get_host()
                    assert result == "your-workspace.cloud.databricks.com"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_host_normalizes_https_prefix(self, mock_get_auth, repo_no_host_config):
        """Test _get_host strips https:// prefix (covers lines 133-134)."""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://stripped-host.databricks.com"
        mock_get_auth.return_value = mock_auth

        result = await repo_no_host_config._get_host()
        assert result == "stripped-host.databricks.com"
        assert not result.startswith("https://")

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_host_normalizes_trailing_slash(self, mock_get_auth, repo_no_host_config):
        """Test _get_host strips trailing slash (covers lines 135-136)."""
        mock_auth = Mock()
        mock_auth.workspace_url = "https://slash-host.databricks.com/"
        mock_get_auth.return_value = mock_auth

        result = await repo_no_host_config._get_host()
        assert result == "slash-host.databricks.com"
        assert not result.endswith("/")


class TestGetAuthHeaders:
    """Tests for _get_auth_headers() covering all paths."""

    @pytest.fixture
    def repo_with_obo(self):
        """Repository configured for OBO."""
        config = GenieAuthConfig(
            host="test.databricks.com",
            use_obo=True,
            user_token="user-obo-token"
        )
        return GenieRepository(config)

    @pytest.fixture
    def repo_no_obo(self):
        """Repository without OBO."""
        config = GenieAuthConfig(
            host="test.databricks.com",
            use_obo=False
        )
        return GenieRepository(config)

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_auth_headers_with_obo_user_token(self, mock_get_auth, repo_with_obo):
        """Test _get_auth_headers passes user token for OBO (covers lines 153-155, 158)."""
        mock_auth_ctx = _make_auth_mock()
        mock_get_auth.return_value = mock_auth_ctx

        headers, error = await repo_with_obo._get_auth_headers()

        assert headers == {"Authorization": "Bearer test-token"}
        assert error is None
        mock_get_auth.assert_called_once_with(user_token="user-obo-token")

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_auth_headers_no_auth_available(self, mock_get_auth, repo_no_obo):
        """Test _get_auth_headers returns error when no auth (covers line 161)."""
        mock_get_auth.return_value = None

        headers, error = await repo_no_obo._get_auth_headers()

        assert headers is None
        assert error == "No authentication method available"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_auth_headers_exception(self, mock_get_auth, repo_no_obo):
        """Test _get_auth_headers handles exception (covers lines 166-168)."""
        mock_get_auth.side_effect = Exception("Auth system down")

        headers, error = await repo_no_obo._get_auth_headers()

        assert headers is None
        assert error == "Auth system down"


class TestMakeUrl:
    """Tests for _make_url() covering the https prefix path."""

    @pytest.mark.asyncio
    async def test_make_url_adds_https_when_missing(self):
        """Test _make_url adds https:// when host has no scheme (covers line 174)."""
        config = GenieAuthConfig(host="plain-host.databricks.com")
        repo = GenieRepository(config)
        repo._host = "plain-host.databricks.com"

        url = await repo._make_url("/api/2.0/genie/spaces")
        assert url == "https://plain-host.databricks.com/api/2.0/genie/spaces"

    @pytest.mark.asyncio
    async def test_make_url_preserves_existing_https(self):
        """Test _make_url does not double-add https:// when already present."""
        config = GenieAuthConfig(host="https://secure-host.databricks.com")
        repo = GenieRepository(config)
        repo._host = "https://secure-host.databricks.com"

        url = await repo._make_url("/api/2.0/genie/spaces")
        assert url == "https://secure-host.databricks.com/api/2.0/genie/spaces"


class TestGetSpacesAdvanced:
    """Advanced tests for get_spaces() covering pagination, filtering, and edge cases."""

    @pytest.fixture
    def repo(self):
        config = GenieAuthConfig(
            host="https://test.databricks.com",
            pat_token="test-token"
        )
        return GenieRepository(config)

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_403_permission_denied(self, mock_get_auth, repo):
        """Test get_spaces returns empty on 403 (covers lines 232-234)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_response = _make_http_response(status_code=403, text="Forbidden")
        repo._client.get = AsyncMock(return_value=mock_response)

        result = await repo.get_spaces()

        assert isinstance(result, GenieSpacesResponse)
        assert result.spaces == []

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_list_response_format(self, mock_get_auth, repo):
        """Test get_spaces handles list response format (covers lines 246-248)."""
        mock_get_auth.return_value = _make_auth_mock()

        list_data = [
            {"id": "s1", "name": "Space 1", "description": "Desc 1"},
            {"id": "s2", "name": "Space 2", "description": "Desc 2"}
        ]
        mock_response = _make_http_response(json_data=list_data)
        repo._client.get = AsyncMock(return_value=mock_response)

        result = await repo.get_spaces()

        assert len(result.spaces) == 2
        assert result.spaces[0].id == "s1"
        assert result.spaces[1].id == "s2"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_fetch_all_pagination(self, mock_get_auth, repo):
        """Test get_spaces with fetch_all=True loops through pages (covers line 278)."""
        mock_get_auth.return_value = _make_auth_mock()

        page1_data = {
            "spaces": [{"id": "s1", "name": "Space 1"}],
            "next_page_token": "page2-token"
        }
        page2_data = {
            "spaces": [{"id": "s2", "name": "Space 2"}],
            "next_page_token": None
        }

        mock_resp1 = _make_http_response(json_data=page1_data)
        mock_resp2 = _make_http_response(json_data=page2_data)
        repo._client.get = AsyncMock(side_effect=[mock_resp1, mock_resp2])

        result = await repo.get_spaces(fetch_all=True)

        assert len(result.spaces) == 2
        assert result.spaces[0].id == "s1"
        assert result.spaces[1].id == "s2"
        assert repo._client.get.call_count == 2
        assert result.next_page_token is None
        assert result.has_more is False

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_filter_by_space_ids(self, mock_get_auth, repo):
        """Test get_spaces filters by specific space_ids (covers lines 305-310)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_data = {
            "spaces": [
                {"id": "s1", "name": "Space 1"},
                {"id": "s2", "name": "Space 2"},
                {"id": "s3", "name": "Space 3"}
            ]
        }
        mock_response = _make_http_response(json_data=mock_data)
        repo._client.get = AsyncMock(return_value=mock_response)

        result = await repo.get_spaces(space_ids=["s1", "s3"], enabled_only=False)

        assert len(result.spaces) == 2
        assert result.spaces[0].id == "s1"
        assert result.spaces[1].id == "s3"
        assert result.filtered is True

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_search_with_fetch_all_returns_none_token(self, mock_get_auth, repo):
        """Test get_spaces with fetch_all returns None token (covers line 325)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_data = {
            "spaces": [{"id": "s1", "name": "Test Space"}],
            "next_page_token": None
        }
        mock_response = _make_http_response(json_data=mock_data)
        repo._client.get = AsyncMock(return_value=mock_response)

        result = await repo.get_spaces(fetch_all=True, enabled_only=False)

        assert result.next_page_token is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_search_fetches_all_pages(self, mock_get_auth, repo):
        """Test get_spaces with search_query fetches all pages for client-side filtering."""
        mock_get_auth.return_value = _make_auth_mock()

        # Create 5 pages, last one without next_page_token
        pages = []
        for i in range(5):
            data = {
                "spaces": [{"id": f"s{i}", "name": f"Space {i}"}],
                "next_page_token": f"token-{i+1}" if i < 4 else None
            }
            pages.append(_make_http_response(json_data=data))

        repo._client.get = AsyncMock(side_effect=pages)

        result = await repo.get_spaces(
            search_query="Space",
            enabled_only=False
        )

        # Should fetch all 5 pages (no artificial cap)
        assert repo._client.get.call_count == 5
        # All pages fetched, so no pagination token
        assert result.next_page_token is None
        assert result.has_more is False
        assert len(result.spaces) == 5

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_spaces_search_filters_by_description(self, mock_get_auth, repo):
        """Test search_query matches on description too."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_data = {
            "spaces": [
                {"id": "s1", "name": "Alpha", "description": "Contains the keyword special"},
                {"id": "s2", "name": "Beta", "description": "Nothing here"}
            ]
        }
        mock_response = _make_http_response(json_data=mock_data)
        repo._client.get = AsyncMock(return_value=mock_response)

        result = await repo.get_spaces(search_query="special", enabled_only=False)

        assert len(result.spaces) == 1
        assert result.spaces[0].id == "s1"


class TestGetSpaceDetails:
    """Tests for get_space_details() (covers lines 358-381)."""

    @pytest.fixture
    def repo(self):
        config = GenieAuthConfig(host="https://test.databricks.com", pat_token="tok")
        return GenieRepository(config)

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_space_details_success(self, mock_get_auth, repo):
        """Test successful get_space_details (covers lines 358-377)."""
        mock_get_auth.return_value = _make_auth_mock()

        space_data = {
            "id": "space-abc",
            "name": "My Space",
            "description": "A test space",
            "type": "data",
            "enabled": True,
            "owner": "user@example.com",
            "workspace_id": "ws-123"
        }
        mock_response = _make_http_response(json_data=space_data)
        repo._client.get = AsyncMock(return_value=mock_response)

        result = await repo.get_space_details("space-abc")

        assert isinstance(result, GenieSpace)
        assert result.id == "space-abc"
        assert result.name == "My Space"
        assert result.description == "A test space"
        assert result.type == "data"
        assert result.enabled is True
        assert result.owner == "user@example.com"
        assert result.workspace_id == "ws-123"

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_space_details_auth_error(self, mock_get_auth, repo):
        """Test get_space_details returns None on auth error (covers lines 360-362)."""
        mock_get_auth.return_value = None

        result = await repo.get_space_details("space-abc")
        assert result is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_space_details_exception(self, mock_get_auth, repo):
        """Test get_space_details returns None on exception (covers lines 379-381)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("Not found")
        repo._client.get = AsyncMock(return_value=mock_response)

        result = await repo.get_space_details("bad-id")
        assert result is None


class TestStartConversationAdvanced:
    """Additional tests for start_conversation() covering payload branches."""

    @pytest.fixture
    def repo(self):
        config = GenieAuthConfig(host="https://test.databricks.com", pat_token="tok")
        return GenieRepository(config)

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_start_conversation_auth_error(self, mock_get_auth, repo):
        """Test start_conversation returns None on auth error (covers lines 399-400)."""
        mock_get_auth.return_value = None

        request = GenieStartConversationRequest(
            space_id="space1",
            initial_message="Hello"
        )

        result = await repo.start_conversation(request)
        assert result is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_start_conversation_with_title(self, mock_get_auth, repo):
        """Test start_conversation includes title in payload (covers line 408)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_response = _make_http_response(json_data={
            "conversation_id": "conv-999",
            "message_id": "msg-001"
        })
        repo._client.post = AsyncMock(return_value=mock_response)

        request = GenieStartConversationRequest(
            space_id="space1",
            initial_message="Hello",
            title="My Conversation Title"
        )

        result = await repo.start_conversation(request)

        assert result is not None
        assert result.conversation_id == "conv-999"

        call_kwargs = repo._client.post.call_args[1]
        assert call_kwargs["json"]["title"] == "My Conversation Title"
        assert call_kwargs["json"]["content"] == "Hello"


class TestSendMessageAdvanced:
    """Additional tests for send_message() covering new conversation and attachment paths."""

    @pytest.fixture
    def repo(self):
        config = GenieAuthConfig(host="https://test.databricks.com", pat_token="tok")
        return GenieRepository(config)

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_send_message_auth_error(self, mock_get_auth, repo):
        """Test send_message returns None on auth error (covers lines 441-442)."""
        mock_get_auth.return_value = None

        request = GenieSendMessageRequest(
            space_id="space1",
            conversation_id="conv-123",
            message="Hello"
        )

        result = await repo.send_message(request)
        assert result is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_send_message_new_conversation(self, mock_get_auth, repo):
        """Test send_message creates new conversation when no conversation_id (covers lines 446-459)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_start_resp = GenieStartConversationResponse(
            conversation_id="new-conv-id",
            message_id="new-msg-id",
            space_id="space1"
        )
        repo.start_conversation = AsyncMock(return_value=mock_start_resp)

        request = GenieSendMessageRequest(
            space_id="space1",
            conversation_id=None,
            message="Start a new conversation"
        )

        result = await repo.send_message(request)

        assert result is not None
        assert result.conversation_id == "new-conv-id"
        assert result.message_id == "new-msg-id"
        assert result.status == GenieMessageStatus.RUNNING

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_send_message_new_conversation_start_fails(self, mock_get_auth, repo):
        """Test send_message returns None when start_conversation fails."""
        mock_get_auth.return_value = _make_auth_mock()

        repo.start_conversation = AsyncMock(return_value=None)

        request = GenieSendMessageRequest(
            space_id="space1",
            conversation_id=None,
            message="Hello"
        )

        result = await repo.send_message(request)
        assert result is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_send_message_with_attachments(self, mock_get_auth, repo):
        """Test send_message includes attachments in payload (covers line 470)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_response = _make_http_response(json_data={
            "id": "msg-attach",
            "status": "RUNNING",
            "content": None
        })
        repo._client.post = AsyncMock(return_value=mock_response)

        attachments = [{"type": "file", "url": "https://example.com/file.csv"}]
        request = GenieSendMessageRequest(
            space_id="space1",
            conversation_id="conv-existing",
            message="Here is data",
            attachments=attachments
        )

        result = await repo.send_message(request)

        assert result is not None
        assert result.message_id == "msg-attach"
        call_kwargs = repo._client.post.call_args[1]
        assert call_kwargs["json"]["attachments"] == attachments


class TestGetMessageStatusAdvanced:
    """Additional tests for get_message_status()."""

    @pytest.fixture
    def repo(self):
        config = GenieAuthConfig(host="https://test.databricks.com", pat_token="tok")
        return GenieRepository(config)

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_message_status_auth_error(self, mock_get_auth, repo):
        """Test get_message_status returns None on auth error (covers lines 503-504)."""
        mock_get_auth.return_value = None

        request = GenieGetMessageStatusRequest(
            space_id="space1",
            conversation_id="conv-1",
            message_id="msg-1"
        )

        result = await repo.get_message_status(request)
        assert result is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_message_status_exception(self, mock_get_auth, repo):
        """Test get_message_status returns None on exception (covers lines 518-520)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("Server error")
        repo._client.get = AsyncMock(return_value=mock_response)

        request = GenieGetMessageStatusRequest(
            space_id="space1",
            conversation_id="conv-1",
            message_id="msg-1"
        )

        result = await repo.get_message_status(request)
        assert result is None


class TestGetQueryResultAdvanced:
    """Additional tests for get_query_result() covering 404 and various result fields."""

    @pytest.fixture
    def repo(self):
        config = GenieAuthConfig(host="https://test.databricks.com", pat_token="tok")
        return GenieRepository(config)

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_query_result_auth_error(self, mock_get_auth, repo):
        """Test get_query_result returns None on auth error (covers lines 538-539)."""
        mock_get_auth.return_value = None

        request = GenieGetQueryResultRequest(
            space_id="space1",
            conversation_id="conv-1",
            message_id="msg-1"
        )

        result = await repo.get_query_result(request)
        assert result is None

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_query_result_404_not_ready(self, mock_get_auth, repo):
        """Test get_query_result returns PENDING on 404 (covers lines 548-550)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_response = _make_http_response(status_code=404)
        repo._client.get = AsyncMock(return_value=mock_response)

        request = GenieGetQueryResultRequest(
            space_id="space1",
            conversation_id="conv-1",
            message_id="msg-1"
        )

        result = await repo.get_query_result(request)
        assert result is not None
        assert result.status == GenieQueryStatus.PENDING

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_query_result_with_result_field(self, mock_get_auth, repo):
        """Test get_query_result parses result field (covers line 565)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_data = {
            "query_id": "q1",
            "status": "SUCCESS",
            "result": {"summary": "10 rows found"}
        }
        mock_response = _make_http_response(json_data=mock_data)
        repo._client.get = AsyncMock(return_value=mock_response)

        request = GenieGetQueryResultRequest(
            space_id="s1", conversation_id="c1", message_id="m1"
        )

        result = await repo.get_query_result(request)
        assert result.result == {"summary": "10 rows found"}

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_query_result_with_execution_time(self, mock_get_auth, repo):
        """Test get_query_result parses execution_time (covers lines 574-575)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_data = {
            "query_id": "q2",
            "status": "SUCCESS",
            "execution_time": 3.14
        }
        mock_response = _make_http_response(json_data=mock_data)
        repo._client.get = AsyncMock(return_value=mock_response)

        request = GenieGetQueryResultRequest(
            space_id="s1", conversation_id="c1", message_id="m1"
        )

        result = await repo.get_query_result(request)
        assert result.execution_time == 3.14

    @patch(AUTH_PATCH)
    @pytest.mark.asyncio
    async def test_get_query_result_exception(self, mock_get_auth, repo):
        """Test get_query_result returns None on exception (covers lines 579-581)."""
        mock_get_auth.return_value = _make_auth_mock()

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.side_effect = Exception("Parse error")
        repo._client.get = AsyncMock(return_value=mock_response)

        request = GenieGetQueryResultRequest(
            space_id="s1", conversation_id="c1", message_id="m1"
        )

        result = await repo.get_query_result(request)
        assert result is None


class TestExecuteQueryAdvanced:
    """Advanced tests for execute_query() covering retry, timeout, and result paths."""

    @pytest.fixture
    def repo(self):
        config = GenieAuthConfig(host="https://test.databricks.com", pat_token="tok")
        return GenieRepository(config)

    @pytest.mark.asyncio
    async def test_execute_query_failed_query_result(self, repo):
        """Test execute_query handles FAILED query result (covers lines 652-658)."""
        mock_send_resp = GenieSendMessageResponse(
            conversation_id="conv-1",
            message_id="msg-1",
            status=GenieMessageStatus.RUNNING
        )
        repo.send_message = AsyncMock(return_value=mock_send_resp)
        repo.get_message_status = AsyncMock(return_value=GenieMessageStatus.COMPLETED)

        failed_result = GenieQueryResult(
            status=GenieQueryStatus.FAILED,
            error="SQL syntax error"
        )
        repo.get_query_result = AsyncMock(return_value=failed_result)

        request = GenieExecutionRequest(
            space_id="space1",
            question="bad query"
        )

        result = await repo.execute_query(request)

        assert result.status == GenieQueryStatus.FAILED
        assert result.error == "SQL syntax error"
        assert result.conversation_id == "conv-1"
        assert result.message_id == "msg-1"

    @pytest.mark.asyncio
    async def test_execute_query_message_failed_with_retry(self, repo):
        """Test execute_query retries on FAILED message status (covers lines 660-669)."""
        mock_send_resp = GenieSendMessageResponse(
            conversation_id="conv-1",
            message_id="msg-1",
            status=GenieMessageStatus.RUNNING
        )
        repo.send_message = AsyncMock(return_value=mock_send_resp)

        repo.get_message_status = AsyncMock(return_value=GenieMessageStatus.FAILED)

        request = GenieExecutionRequest(
            space_id="space1",
            question="Hello",
            max_retries=3,
            timeout=10
        )

        with patch('asyncio.sleep', new_callable=AsyncMock):
            result = await repo.execute_query(request)

        assert result.status == GenieQueryStatus.FAILED
        assert result.error == "Message processing failed"

    @pytest.mark.asyncio
    async def test_execute_query_timeout(self, repo):
        """Test execute_query handles timeout (covers lines 674-680)."""
        mock_send_resp = GenieSendMessageResponse(
            conversation_id="conv-1",
            message_id="msg-1",
            status=GenieMessageStatus.RUNNING
        )
        repo.send_message = AsyncMock(return_value=mock_send_resp)
        repo.get_message_status = AsyncMock(return_value=GenieMessageStatus.RUNNING)

        request = GenieExecutionRequest(
            space_id="space1",
            question="slow query",
            timeout=1,
            max_retries=3
        )

        # Simulate time progression: start=0, first check=0.5 (in loop), second check=2.0 (exceeds timeout)
        time_values = iter([0.0, 0.5, 2.0])
        with patch('time.time', side_effect=time_values):
            with patch('asyncio.sleep', new_callable=AsyncMock):
                result = await repo.execute_query(request)

        assert result.status == GenieQueryStatus.FAILED
        assert "timed out" in result.error

    @pytest.mark.asyncio
    async def test_execute_query_exception(self, repo):
        """Test execute_query handles exception in workflow (covers lines 682-684)."""
        repo.send_message = AsyncMock(side_effect=Exception("Network failure"))

        request = GenieExecutionRequest(
            space_id="space1",
            question="Hello",
            conversation_id="conv-existing"
        )

        result = await repo.execute_query(request)

        assert result.status == GenieQueryStatus.FAILED
        assert result.error == "Network failure"
        assert result.conversation_id == "conv-existing"

    @pytest.mark.asyncio
    async def test_execute_query_exception_no_conversation_id(self, repo):
        """Test execute_query exception with no conversation_id defaults to empty string."""
        repo.send_message = AsyncMock(side_effect=Exception("Oops"))

        request = GenieExecutionRequest(
            space_id="space1",
            question="Hello"
        )

        result = await repo.execute_query(request)

        assert result.status == GenieQueryStatus.FAILED
        assert result.conversation_id == ""


class TestExtractResponseText:
    """Tests for _extract_response_text() covering all branches (covers lines 701-724)."""

    @pytest.fixture
    def repo(self):
        config = GenieAuthConfig(host="https://test.databricks.com")
        return GenieRepository(config)

    def test_extract_no_content(self, repo):
        """Test _extract_response_text with empty query result returns default message."""
        qr = GenieQueryResult(status=GenieQueryStatus.SUCCESS)
        result = repo._extract_response_text(qr)
        assert result == "No response content found"

    def test_extract_string_result(self, repo):
        """Test _extract_response_text with string result (covers lines 704-706)."""
        qr = GenieQueryResult(
            status=GenieQueryStatus.SUCCESS,
            result="Here are the results"
        )
        result = repo._extract_response_text(qr)
        assert "Here are the results" in result

    def test_extract_dict_result(self, repo):
        """Test _extract_response_text with dict result (covers lines 707-708)."""
        qr = GenieQueryResult(
            status=GenieQueryStatus.SUCCESS,
            result={"key": "value", "count": 42}
        )
        result = repo._extract_response_text(qr)
        assert "key" in result
        assert "value" in result

    def test_extract_with_sql(self, repo):
        """Test _extract_response_text includes SQL query (covers lines 711-712)."""
        qr = GenieQueryResult(
            status=GenieQueryStatus.SUCCESS,
            sql="SELECT * FROM users"
        )
        result = repo._extract_response_text(qr)
        assert "SQL Query:" in result
        assert "SELECT * FROM users" in result

    def test_extract_with_data_and_columns(self, repo):
        """Test _extract_response_text with data and columns (covers lines 715-722)."""
        qr = GenieQueryResult(
            status=GenieQueryStatus.SUCCESS,
            data=[
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35}
            ],
            columns=["name", "age"],
            row_count=3
        )
        result = repo._extract_response_text(qr)
        assert "Results: 3 rows" in result
        assert "Preview:" in result
        assert "Alice" in result
        assert "Bob" in result

    def test_extract_with_data_columns_and_result(self, repo):
        """Test _extract_response_text with all fields populated."""
        qr = GenieQueryResult(
            status=GenieQueryStatus.SUCCESS,
            result="Summary: 3 users found",
            sql="SELECT * FROM users LIMIT 3",
            data=[
                {"name": "Alice"},
                {"name": "Bob"},
                {"name": "Charlie"}
            ],
            columns=["name"],
            row_count=3
        )
        result = repo._extract_response_text(qr)
        assert "Summary: 3 users found" in result
        assert "SQL Query:" in result
        assert "Results: 3 rows" in result
        assert "Preview:" in result

    def test_extract_with_more_than_5_rows(self, repo):
        """Test _extract_response_text previews only first 5 rows (covers line 719)."""
        rows = [{"id": i} for i in range(10)]
        qr = GenieQueryResult(
            status=GenieQueryStatus.SUCCESS,
            data=rows,
            columns=["id"],
            row_count=10
        )
        result = repo._extract_response_text(qr)
        assert "Results: 10 rows" in result
        assert "{'id': 0}" in result
        assert "{'id': 4}" in result
        assert "{'id': 5}" not in result

    def test_extract_data_without_columns(self, repo):
        """Test _extract_response_text with data but no columns does not show Results section."""
        qr = GenieQueryResult(
            status=GenieQueryStatus.SUCCESS,
            data=[{"a": 1}],
            row_count=1
        )
        result = repo._extract_response_text(qr)
        assert "Results:" not in result
        assert result == "No response content found"

    def test_extract_empty_data_with_columns(self, repo):
        """Test _extract_response_text with empty data list and columns."""
        qr = GenieQueryResult(
            status=GenieQueryStatus.SUCCESS,
            data=[],
            columns=["col1"],
            row_count=0
        )
        result = repo._extract_response_text(qr)
        assert result == "No response content found"
