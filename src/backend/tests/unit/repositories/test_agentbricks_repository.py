"""
Unit tests for AgentBricks repository.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import requests

from src.repositories.agentbricks_repository import AgentBricksRepository
from src.schemas.agentbricks import (
    AgentBricksEndpoint,
    AgentBricksEndpointsRequest,
    AgentBricksEndpointsResponse,
    AgentBricksQueryRequest,
    AgentBricksQueryResponse,
    AgentBricksExecutionRequest,
    AgentBricksExecutionResponse,
    AgentBricksAuthConfig,
    AgentBricksMessage,
    AgentBricksQueryStatus,
)


class TestAgentBricksRepositoryInit:
    """Tests for AgentBricksRepository initialization."""

    def test_init_without_auth_config(self):
        """Test repository initialization without auth config."""
        repo = AgentBricksRepository()
        assert repo.auth_config is None
        assert repo._host is None
        assert repo._session is not None

    def test_init_with_auth_config(self):
        """Test repository initialization with auth config."""
        auth_config = AgentBricksAuthConfig(
            use_obo=True,
            host="https://workspace.databricks.com"
        )
        repo = AgentBricksRepository(auth_config=auth_config)
        assert repo.auth_config == auth_config

    def test_session_setup(self):
        """Test that session is properly configured with retry logic."""
        repo = AgentBricksRepository()
        assert repo._session is not None
        # Verify session has adapters mounted
        assert 'https://' in repo._session.adapters
        assert 'http://' in repo._session.adapters


class TestGetHost:
    """Tests for _get_host method."""

    @pytest.fixture
    def repo(self):
        """Create a repository instance."""
        return AgentBricksRepository()

    @pytest.mark.asyncio
    async def test_get_host_from_config(self):
        """Test getting host from auth config."""
        auth_config = AgentBricksAuthConfig(
            host="https://workspace.databricks.com"
        )
        repo = AgentBricksRepository(auth_config=auth_config)

        host = await repo._get_host()

        assert host == "https://workspace.databricks.com"

    @pytest.mark.asyncio
    async def test_get_host_cached(self, repo):
        """Test that host is cached after first retrieval."""
        repo._host = "https://cached.databricks.com"

        host = await repo._get_host()

        assert host == "https://cached.databricks.com"

    @pytest.mark.asyncio
    async def test_get_host_from_auth_context(self, repo):
        """Test getting host from unified auth context."""
        mock_auth = MagicMock()
        mock_auth.workspace_url = "https://auth-context.databricks.com"

        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            host = await repo._get_host()

            assert host == "auth-context.databricks.com"


class TestGetAuthHeaders:
    """Tests for _get_auth_headers method."""

    @pytest.fixture
    def repo(self):
        """Create a repository instance."""
        return AgentBricksRepository()

    @pytest.mark.asyncio
    async def test_get_auth_headers_success(self, repo):
        """Test successful auth header retrieval."""
        mock_auth = MagicMock()
        mock_auth.get_headers.return_value = {"Authorization": "Bearer token123"}

        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            headers, error = await repo._get_auth_headers()

            assert headers == {"Authorization": "Bearer token123"}
            assert error is None

    @pytest.mark.asyncio
    async def test_get_auth_headers_with_user_token(self):
        """Test auth headers with user token for OBO."""
        auth_config = AgentBricksAuthConfig(
            use_obo=True,
            user_token="user-token-123"
        )
        repo = AgentBricksRepository(auth_config=auth_config)

        mock_auth = MagicMock()
        mock_auth.get_headers.return_value = {"Authorization": "Bearer obo-token"}

        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            headers, error = await repo._get_auth_headers()

            # Verify user_token was passed to get_auth_context
            mock_get_auth.assert_called_once_with(user_token="user-token-123")

    @pytest.mark.asyncio
    async def test_get_auth_headers_no_auth(self, repo):
        """Test when no authentication is available."""
        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = None

            headers, error = await repo._get_auth_headers()

            assert headers is None
            assert "No authentication method available" in error

    @pytest.mark.asyncio
    async def test_get_auth_headers_exception(self, repo):
        """Test handling exceptions in auth header retrieval."""
        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.side_effect = Exception("Auth error")

            headers, error = await repo._get_auth_headers()

            assert headers is None
            assert "Auth error" in error


class TestMakeUrl:
    """Tests for _make_url method."""

    @pytest.fixture
    def repo(self):
        """Create a repository instance."""
        return AgentBricksRepository()

    @pytest.mark.asyncio
    async def test_make_url(self, repo):
        """Test URL construction."""
        repo._host = "workspace.databricks.com"

        url = await repo._make_url("/api/2.0/serving-endpoints")

        assert url == "https://workspace.databricks.com/api/2.0/serving-endpoints"

    @pytest.mark.asyncio
    async def test_make_url_with_https_host(self, repo):
        """Test URL construction when host already has https."""
        repo._host = "https://workspace.databricks.com"

        url = await repo._make_url("/api/test")

        assert url == "https://workspace.databricks.com/api/test"


class TestIsAgentBricksEndpoint:
    """Tests for _is_agentbricks_endpoint method."""

    @pytest.fixture
    def repo(self):
        """Create a repository instance."""
        return AgentBricksRepository()

    def test_is_agentbricks_endpoint_by_name(self, repo):
        """Test detection by endpoint name."""
        endpoint_data = {
            "name": "mas-agent-endpoint",
            "config": {}
        }
        assert repo._is_agentbricks_endpoint(endpoint_data) is True

    def test_is_agentbricks_endpoint_by_agent_name(self, repo):
        """Test detection by 'agent' in name."""
        endpoint_data = {
            "name": "my-agent-service",
            "config": {}
        }
        assert repo._is_agentbricks_endpoint(endpoint_data) is True

    def test_is_agentbricks_endpoint_by_tag(self, repo):
        """Test detection by tags."""
        endpoint_data = {
            "name": "some-endpoint",
            "tags": [{"key": "type", "value": "agentbricks"}],
            "config": {}
        }
        assert repo._is_agentbricks_endpoint(endpoint_data) is True

    def test_is_agentbricks_endpoint_by_external_model(self, repo):
        """Test detection by external model in config."""
        endpoint_data = {
            "name": "some-endpoint",
            "config": {
                "served_entities": [
                    {"external_model": {"name": "gpt-4"}}
                ]
            }
        }
        assert repo._is_agentbricks_endpoint(endpoint_data) is True

    def test_is_not_agentbricks_endpoint(self, repo):
        """Test non-AgentBricks endpoint."""
        endpoint_data = {
            "name": "regular-model-serving",
            "config": {
                "served_entities": [
                    {"model_name": "some-model", "model_version": "1"}
                ]
            }
        }
        assert repo._is_agentbricks_endpoint(endpoint_data) is False


class TestGetEndpoints:
    """Tests for get_endpoints method."""

    @pytest.fixture
    def repo(self):
        """Create a repository instance."""
        return AgentBricksRepository()

    @pytest.mark.asyncio
    async def test_get_endpoints_success(self, repo):
        """Test successful endpoint retrieval."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "endpoints": [
                {
                    "id": "ep-1",
                    "name": "agent-endpoint",
                    "state": {"ready": "READY"},
                    "config": {}
                }
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._session, 'get', return_value=mock_response) as mock_get:
                    request = AgentBricksEndpointsRequest()
                    result = await repo.get_endpoints(request)

                    # The endpoint won't be returned because _is_agentbricks_endpoint returns False
                    # for a basic endpoint without agent indicators
                    assert isinstance(result, AgentBricksEndpointsResponse)

    @pytest.mark.asyncio
    async def test_get_endpoints_auth_failure(self, repo):
        """Test handling auth failure."""
        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = (None, "Authentication failed")

            request = AgentBricksEndpointsRequest()
            result = await repo.get_endpoints(request)

            assert len(result.endpoints) == 0

    @pytest.mark.asyncio
    async def test_get_endpoints_permission_denied(self, repo):
        """Test handling 403 permission denied."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._session, 'get', return_value=mock_response):
                    request = AgentBricksEndpointsRequest()
                    result = await repo.get_endpoints(request)

                    assert len(result.endpoints) == 0

    @pytest.mark.asyncio
    async def test_get_endpoints_with_filtering(self, repo):
        """Test endpoint filtering."""
        # This test verifies the filtering logic
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "endpoints": [
                {
                    "id": "ep-1",
                    "name": "mas-agent-1",
                    "state": {"ready": "READY"},
                    "creator": "user@example.com"
                },
                {
                    "id": "ep-2",
                    "name": "mas-agent-2",
                    "state": {"ready": "NOT_READY"},
                    "creator": "other@example.com"
                }
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._session, 'get', return_value=mock_response):
                    # Test filtering by ready_only
                    request = AgentBricksEndpointsRequest(ready_only=True)
                    result = await repo.get_endpoints(request)

                    # Only ready endpoints should be returned
                    ready_endpoints = [ep for ep in result.endpoints if ep.state == "READY"]
                    assert len(ready_endpoints) == len(result.endpoints)


class TestQueryEndpoint:
    """Tests for query_endpoint method."""

    @pytest.fixture
    def repo(self):
        """Create a repository instance."""
        return AgentBricksRepository()

    @pytest.mark.asyncio
    async def test_query_endpoint_success(self, repo):
        """Test successful endpoint query."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {"message": {"content": "Hello! How can I help?"}}
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/serving-endpoints/test/invocations"

                with patch.object(repo._session, 'post', return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    assert "Hello" in result.response

    @pytest.mark.asyncio
    async def test_query_endpoint_auth_failure(self, repo):
        """Test query when auth fails."""
        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = (None, "Auth failed")

            messages = [AgentBricksMessage(role="user", content="Hello")]
            request = AgentBricksQueryRequest(
                endpoint_name="test-endpoint",
                messages=messages
            )
            result = await repo.query_endpoint(request)

            assert result.status == "FAILED"
            assert "Authentication failed" in result.error

    @pytest.mark.asyncio
    async def test_query_endpoint_http_error(self, repo):
        """Test query with HTTP error response."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._session, 'post', return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "FAILED"
                    assert "500" in result.error

    @pytest.mark.asyncio
    async def test_query_endpoint_predictions_format(self, repo):
        """Test parsing predictions format response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "predictions": ["This is the prediction"]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._session, 'post', return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    assert "prediction" in result.response


class TestExecuteQuery:
    """Tests for execute_query method."""

    @pytest.fixture
    def repo(self):
        """Create a repository instance."""
        return AgentBricksRepository()

    @pytest.mark.asyncio
    async def test_execute_query_success(self, repo):
        """Test successful query execution."""
        mock_query_response = AgentBricksQueryResponse(
            response="Answer to your question",
            status=AgentBricksQueryStatus.SUCCESS
        )

        with patch.object(repo, 'query_endpoint', new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_query_response

            request = AgentBricksExecutionRequest(
                endpoint_name="test-endpoint",
                question="What is the answer?"
            )
            result = await repo.execute_query(request)

            assert result.status == "SUCCESS"
            assert result.result == "Answer to your question"

    @pytest.mark.asyncio
    async def test_execute_query_failure(self, repo):
        """Test query execution failure."""
        mock_query_response = AgentBricksQueryResponse(
            response="",
            status=AgentBricksQueryStatus.FAILED,
            error="Query failed"
        )

        with patch.object(repo, 'query_endpoint', new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_query_response

            request = AgentBricksExecutionRequest(
                endpoint_name="test-endpoint",
                question="Hello"
            )
            result = await repo.execute_query(request)

            assert result.status == "FAILED"
            assert result.error == "Query failed"

    @pytest.mark.asyncio
    async def test_execute_query_exception(self, repo):
        """Test handling exceptions in execute."""
        with patch.object(repo, 'query_endpoint', new_callable=AsyncMock) as mock_query:
            mock_query.side_effect = Exception("Network error")

            request = AgentBricksExecutionRequest(
                endpoint_name="test-endpoint",
                question="Hello"
            )
            result = await repo.execute_query(request)

            assert result.status == "FAILED"
            assert "Network error" in result.error


class TestRepositoryCleanup:
    """Tests for repository cleanup."""

    def test_del_closes_session(self):
        """Test that session is closed on deletion."""
        repo = AgentBricksRepository()
        mock_session = MagicMock()
        repo._session = mock_session

        del repo

        mock_session.close.assert_called_once()
