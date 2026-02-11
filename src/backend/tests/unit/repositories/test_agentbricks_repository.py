"""
Unit tests for AgentBricks repository.
"""

import asyncio

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx

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
        assert repo._client is not None

    def test_init_with_auth_config(self):
        """Test repository initialization with auth config."""
        auth_config = AgentBricksAuthConfig(
            use_obo=True,
            host="https://workspace.databricks.com"
        )
        repo = AgentBricksRepository(auth_config=auth_config)
        assert repo.auth_config == auth_config

    def test_client_setup(self):
        """Test that client is properly configured."""
        repo = AgentBricksRepository()
        assert repo._client is not None
        assert isinstance(repo._client, httpx.AsyncClient)


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

    @pytest.mark.asyncio
    async def test_get_host_sdk_config_fallback(self, repo):
        """Test SDK Config fallback when auth context has no workspace_url."""
        mock_auth = MagicMock()
        mock_auth.workspace_url = None

        mock_sdk_config = MagicMock()
        mock_sdk_config.host = "https://sdk-host.databricks.com"

        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            with patch.dict('sys.modules', {'databricks.sdk.config': MagicMock()}):
                with patch('databricks.sdk.config.Config', return_value=mock_sdk_config):
                    host = await repo._get_host()

                    assert host == "sdk-host.databricks.com"

    @pytest.mark.asyncio
    async def test_get_host_sdk_config_exception(self, repo):
        """Test SDK Config fallback raises an exception, falls through to databricks_auth."""
        mock_auth = MagicMock()
        mock_auth.workspace_url = None

        mock_db_auth = MagicMock()
        mock_db_auth._load_config = AsyncMock()
        mock_db_auth.get_workspace_host.return_value = "https://dbauth-host.databricks.com"

        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            with patch.dict('sys.modules', {'databricks': MagicMock(), 'databricks.sdk': MagicMock(), 'databricks.sdk.config': MagicMock()}):
                import sys
                sdk_config_mod = sys.modules['databricks.sdk.config']
                sdk_config_mod.Config = MagicMock(side_effect=Exception("SDK not available"))

                with patch('src.utils.databricks_auth._databricks_auth', mock_db_auth):
                    host = await repo._get_host()

                    assert host == "dbauth-host.databricks.com"

    @pytest.mark.asyncio
    async def test_get_host_databricks_auth_fallback(self, repo):
        """Test _databricks_auth fallback when SDK Config returns no host."""
        mock_auth = MagicMock()
        mock_auth.workspace_url = None

        mock_sdk_config = MagicMock()
        mock_sdk_config.host = None

        mock_db_auth = MagicMock()
        mock_db_auth._load_config = AsyncMock()
        mock_db_auth.get_workspace_host.return_value = "https://dbauth-host.databricks.com"

        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            with patch.dict('sys.modules', {'databricks': MagicMock(), 'databricks.sdk': MagicMock(), 'databricks.sdk.config': MagicMock()}):
                import sys
                sdk_config_mod = sys.modules['databricks.sdk.config']
                sdk_config_mod.Config = MagicMock(return_value=mock_sdk_config)

                with patch('src.utils.databricks_auth._databricks_auth', mock_db_auth):
                    host = await repo._get_host()

                    assert host == "dbauth-host.databricks.com"

    @pytest.mark.asyncio
    async def test_get_host_databricks_auth_exception(self, repo):
        """Test _databricks_auth fallback that also raises an exception, falls to default."""
        mock_auth = MagicMock()
        mock_auth.workspace_url = None

        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            with patch.dict('sys.modules', {'databricks': MagicMock(), 'databricks.sdk': MagicMock(), 'databricks.sdk.config': MagicMock()}):
                import sys
                sdk_config_mod = sys.modules['databricks.sdk.config']
                sdk_config_mod.Config = MagicMock(side_effect=Exception("SDK not available"))

                mock_db_auth = MagicMock()
                mock_db_auth._load_config = AsyncMock(side_effect=Exception("databricks_auth not available"))

                with patch('src.utils.databricks_auth._databricks_auth', mock_db_auth):
                    host = await repo._get_host()

                    # Falls back to the default host
                    assert host == "your-workspace.cloud.databricks.com"

    @pytest.mark.asyncio
    async def test_get_host_default_fallback(self, repo):
        """Test default host when all detection methods fail."""
        mock_auth = MagicMock()
        mock_auth.workspace_url = None

        mock_sdk_config = MagicMock()
        mock_sdk_config.host = None

        mock_db_auth = MagicMock()
        mock_db_auth._load_config = AsyncMock()
        mock_db_auth.get_workspace_host.return_value = None

        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            with patch.dict('sys.modules', {'databricks': MagicMock(), 'databricks.sdk': MagicMock(), 'databricks.sdk.config': MagicMock()}):
                import sys
                sdk_config_mod = sys.modules['databricks.sdk.config']
                sdk_config_mod.Config = MagicMock(return_value=mock_sdk_config)

                with patch('src.utils.databricks_auth._databricks_auth', mock_db_auth):
                    host = await repo._get_host()

                    assert host == "your-workspace.cloud.databricks.com"

    @pytest.mark.asyncio
    async def test_get_host_trailing_slash_removed(self, repo):
        """Test that trailing slash is removed from host."""
        mock_auth = MagicMock()
        mock_auth.workspace_url = "https://workspace.databricks.com/"

        with patch('src.repositories.agentbricks_repository.get_auth_context', new_callable=AsyncMock) as mock_get_auth:
            mock_get_auth.return_value = mock_auth

            host = await repo._get_host()

            assert not host.endswith("/")
            assert host == "workspace.databricks.com"


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

    def test_is_agentbricks_endpoint_by_foundation_model_with_workload_size(self, repo):
        """Test detection by foundation_model_name with workload_size (lines 169-171)."""
        endpoint_data = {
            "name": "some-endpoint",
            "config": {
                "served_entities": [
                    {
                        "foundation_model_name": "llama-2-70b",
                        "workload_size": "Small"
                    }
                ]
            }
        }
        assert repo._is_agentbricks_endpoint(endpoint_data) is True

    def test_is_agentbricks_endpoint_foundation_model_without_workload_size(self, repo):
        """Test that foundation_model_name without workload_size does not match."""
        endpoint_data = {
            "name": "some-endpoint",
            "config": {
                "served_entities": [
                    {
                        "foundation_model_name": "llama-2-70b"
                    }
                ]
            }
        }
        # No workload_size, no external_model, name does not match patterns
        assert repo._is_agentbricks_endpoint(endpoint_data) is False

    def test_is_agentbricks_endpoint_by_agent_tag_key(self, repo):
        """Test detection by 'agent' in tag key (line 181)."""
        endpoint_data = {
            "name": "some-endpoint",
            "tags": [{"key": "agent-type", "value": "custom"}],
            "config": {}
        }
        assert repo._is_agentbricks_endpoint(endpoint_data) is True

    def test_is_agentbricks_endpoint_by_mosaic_tag_key(self, repo):
        """Test detection by 'mosaic' in tag key (line 181-182)."""
        endpoint_data = {
            "name": "some-endpoint",
            "tags": [{"key": "mosaic-service", "value": "something"}],
            "config": {}
        }
        assert repo._is_agentbricks_endpoint(endpoint_data) is True


def _make_agentbricks_endpoint_data(
    ep_id, name, state_ready=None, config_update=None, creator=None
):
    """Helper to create endpoint data recognized as AgentBricks (name starts with mas-)."""
    state = {}
    if state_ready is not None:
        state["ready"] = state_ready
    if config_update is not None:
        state["config_update"] = config_update
    data = {
        "id": ep_id,
        "name": name,
        "state": state,
        "config": {},
    }
    if creator is not None:
        data["creator"] = creator
    return data


def _mock_get_endpoints_response(endpoints_data):
    """Helper to create a mock HTTP response for get_endpoints."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"endpoints": endpoints_data}
    mock_response.raise_for_status = MagicMock()
    return mock_response


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

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response) as mock_get:
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

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
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

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
                    # Test filtering by ready_only
                    request = AgentBricksEndpointsRequest(ready_only=True)
                    result = await repo.get_endpoints(request)

                    # Only ready endpoints should be returned
                    ready_endpoints = [ep for ep in result.endpoints if ep.state == "READY"]
                    assert len(ready_endpoints) == len(result.endpoints)

    @pytest.mark.asyncio
    async def test_get_endpoints_non_agentbricks_filtered_out(self, repo):
        """Test that non-agentbricks endpoints are skipped (line 230 continue)."""
        endpoints_data = [
            # This one IS an agentbricks endpoint (name starts with "mas-")
            _make_agentbricks_endpoint_data("ep-1", "mas-my-agent", state_ready="READY"),
            # This one is NOT an agentbricks endpoint
            {
                "id": "ep-2",
                "name": "regular-serving-endpoint",
                "state": {"ready": "READY"},
                "config": {
                    "served_entities": [
                        {"model_name": "some-model", "model_version": "1"}
                    ]
                },
            },
        ]
        mock_response = _mock_get_endpoints_response(endpoints_data)

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
                    request = AgentBricksEndpointsRequest(ready_only=False)
                    result = await repo.get_endpoints(request)

                    # Only the agentbricks endpoint should be present
                    assert len(result.endpoints) == 1
                    assert result.endpoints[0].id == "ep-1"

    @pytest.mark.asyncio
    async def test_get_endpoints_state_parsing_unknown(self, repo):
        """Test state parsing for non-READY/NOT_READY states (line 244)."""
        endpoints_data = [
            _make_agentbricks_endpoint_data(
                "ep-1", "mas-updating-agent", config_update="UPDATING"
            ),
        ]
        mock_response = _mock_get_endpoints_response(endpoints_data)

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
                    request = AgentBricksEndpointsRequest(ready_only=False)
                    result = await repo.get_endpoints(request)

                    assert len(result.endpoints) == 1
                    # state_str falls to else branch: state_data.get("config_update") or "NOT_UPDATING"
                    assert result.endpoints[0].state == "UPDATING"

    @pytest.mark.asyncio
    async def test_get_endpoints_state_parsing_fallback_not_updating(self, repo):
        """Test state parsing defaults to NOT_UPDATING when no config_update present."""
        endpoints_data = [
            _make_agentbricks_endpoint_data("ep-1", "mas-agent-unknown"),
        ]
        mock_response = _mock_get_endpoints_response(endpoints_data)

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
                    request = AgentBricksEndpointsRequest(ready_only=False)
                    result = await repo.get_endpoints(request)

                    assert len(result.endpoints) == 1
                    assert result.endpoints[0].state == "NOT_UPDATING"

    @pytest.mark.asyncio
    async def test_get_endpoints_filter_by_endpoint_ids(self, repo):
        """Test filtering by specific endpoint IDs (lines 274-278)."""
        endpoints_data = [
            _make_agentbricks_endpoint_data("ep-1", "mas-agent-1", state_ready="READY"),
            _make_agentbricks_endpoint_data("ep-2", "mas-agent-2", state_ready="READY"),
            _make_agentbricks_endpoint_data("ep-3", "mas-agent-3", state_ready="READY"),
        ]
        mock_response = _mock_get_endpoints_response(endpoints_data)

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
                    request = AgentBricksEndpointsRequest(
                        ready_only=False,
                        endpoint_ids=["ep-1", "ep-3"]
                    )
                    result = await repo.get_endpoints(request)

                    assert len(result.endpoints) == 2
                    ids = {ep.id for ep in result.endpoints}
                    assert ids == {"ep-1", "ep-3"}
                    assert result.filtered is True

    @pytest.mark.asyncio
    async def test_get_endpoints_filter_by_search_query(self, repo):
        """Test filtering by search query matching name or creator (lines 282-288)."""
        endpoints_data = [
            _make_agentbricks_endpoint_data("ep-1", "mas-agent-alpha", state_ready="READY", creator="alice@example.com"),
            _make_agentbricks_endpoint_data("ep-2", "mas-agent-beta", state_ready="READY", creator="bob@example.com"),
            _make_agentbricks_endpoint_data("ep-3", "mas-agent-gamma", state_ready="READY", creator="alice@example.com"),
        ]
        mock_response = _mock_get_endpoints_response(endpoints_data)

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
                    # Search by name
                    request = AgentBricksEndpointsRequest(
                        ready_only=False,
                        search_query="alpha"
                    )
                    result = await repo.get_endpoints(request)

                    assert len(result.endpoints) == 1
                    assert result.endpoints[0].name == "mas-agent-alpha"
                    assert result.filtered is True

    @pytest.mark.asyncio
    async def test_get_endpoints_filter_by_search_query_creator_match(self, repo):
        """Test search query matching on creator field."""
        endpoints_data = [
            _make_agentbricks_endpoint_data("ep-1", "mas-agent-one", state_ready="READY", creator="alice@example.com"),
            _make_agentbricks_endpoint_data("ep-2", "mas-agent-two", state_ready="READY", creator="bob@example.com"),
        ]
        mock_response = _mock_get_endpoints_response(endpoints_data)

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
                    # Search by creator
                    request = AgentBricksEndpointsRequest(
                        ready_only=False,
                        search_query="bob"
                    )
                    result = await repo.get_endpoints(request)

                    assert len(result.endpoints) == 1
                    assert result.endpoints[0].creator == "bob@example.com"

    @pytest.mark.asyncio
    async def test_get_endpoints_filter_by_creator_filter(self, repo):
        """Test filtering by creator_filter (lines 292-296)."""
        endpoints_data = [
            _make_agentbricks_endpoint_data("ep-1", "mas-agent-1", state_ready="READY", creator="alice@example.com"),
            _make_agentbricks_endpoint_data("ep-2", "mas-agent-2", state_ready="READY", creator="bob@example.com"),
            _make_agentbricks_endpoint_data("ep-3", "mas-agent-3", state_ready="READY", creator="alice@example.com"),
        ]
        mock_response = _mock_get_endpoints_response(endpoints_data)

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
                    request = AgentBricksEndpointsRequest(
                        ready_only=False,
                        creator_filter="alice"
                    )
                    result = await repo.get_endpoints(request)

                    assert len(result.endpoints) == 2
                    for ep in result.endpoints:
                        assert "alice" in ep.creator.lower()
                    assert result.filtered is True

    @pytest.mark.asyncio
    async def test_get_endpoints_exception_handling(self, repo):
        """Test exception handling in get_endpoints (lines 306-308)."""
        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.side_effect = Exception("Unexpected error")

            request = AgentBricksEndpointsRequest()
            result = await repo.get_endpoints(request)

            assert isinstance(result, AgentBricksEndpointsResponse)
            assert len(result.endpoints) == 0

    @pytest.mark.asyncio
    async def test_get_endpoints_ready_only_filters_not_ready(self, repo):
        """Test that ready_only=True filters out non-READY endpoints (lines 274-278)."""
        endpoints_data = [
            _make_agentbricks_endpoint_data("ep-1", "mas-agent-ready", state_ready="READY"),
            _make_agentbricks_endpoint_data("ep-2", "mas-agent-not-ready", state_ready="NOT_READY"),
        ]
        mock_response = _mock_get_endpoints_response(endpoints_data)

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_get_host', new_callable=AsyncMock) as mock_host:
                mock_host.return_value = "workspace.databricks.com"

                with patch.object(repo._client, 'get', new_callable=AsyncMock, return_value=mock_response):
                    request = AgentBricksEndpointsRequest(ready_only=True)
                    result = await repo.get_endpoints(request)

                    assert len(result.endpoints) == 1
                    assert result.endpoints[0].state == "READY"
                    assert result.filtered is True


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

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response):
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

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response):
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

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    assert "prediction" in result.response

    @pytest.mark.asyncio
    async def test_query_endpoint_with_custom_inputs(self, repo):
        """Test query with custom_inputs in payload (line 354)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Custom response"}}]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response) as mock_post:
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages,
                        custom_inputs={"temperature": 0.5, "max_tokens": 100}
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    # Verify the payload included custom_inputs
                    call_kwargs = mock_post.call_args
                    payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
                    assert "temperature" in payload
                    assert payload["temperature"] == 0.5

    @pytest.mark.asyncio
    async def test_query_endpoint_with_stream_flag(self, repo):
        """Test query with stream=True adds stream to payload (line 358)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Streamed response"}}]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response) as mock_post:
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages,
                        stream=True
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    # Verify the payload included stream flag
                    call_kwargs = mock_post.call_args
                    payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
                    assert payload.get("stream") is True

    @pytest.mark.asyncio
    async def test_query_endpoint_direct_response_format(self, repo):
        """Test parsing direct 'response' field format (line 393)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "response": "Direct response content"
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    assert result.response == "Direct response content"

    @pytest.mark.asyncio
    async def test_query_endpoint_string_result(self, repo):
        """Test parsing when result is a plain string (line 405-406)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = "Plain string response"
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    assert result.response == "Plain string response"

    @pytest.mark.asyncio
    async def test_query_endpoint_with_usage_info(self, repo):
        """Test extraction of usage information from response (line 403)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Response with usage"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    assert result.usage is not None
                    assert result.usage["total_tokens"] == 30

    @pytest.mark.asyncio
    async def test_query_endpoint_with_trace_info(self, repo):
        """Test extraction of trace info when return_trace is True (line 411)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Response with trace"}}],
            "trace": {"steps": ["step1", "step2"]}
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages,
                        return_trace=True
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    assert result.trace is not None
                    assert result.trace["steps"] == ["step1", "step2"]

    @pytest.mark.asyncio
    async def test_query_endpoint_with_metadata_as_trace(self, repo):
        """Test extraction of metadata as trace fallback (line 411)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Response with metadata"}}],
            "metadata": {"run_id": "abc123"}
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages,
                        return_trace=True
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    assert result.trace is not None
                    assert result.trace["run_id"] == "abc123"

    @pytest.mark.asyncio
    async def test_query_endpoint_exception_handling(self, repo):
        """Test exception handling in query_endpoint (lines 422-424)."""
        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.side_effect = Exception("Unexpected connection error")

            messages = [AgentBricksMessage(role="user", content="Hello")]
            request = AgentBricksQueryRequest(
                endpoint_name="test-endpoint",
                messages=messages
            )
            result = await repo.query_endpoint(request)

            assert result.status == "FAILED"
            assert "Unexpected connection error" in result.error

    @pytest.mark.asyncio
    async def test_query_endpoint_trace_not_returned_when_not_requested(self, repo):
        """Test that trace is None when return_trace is False even if result has trace."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Response"}}],
            "trace": {"steps": ["step1"]},
            "usage": {"total_tokens": 5}
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(repo, '_get_auth_headers', new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = ({"Authorization": "Bearer token"}, None)

            with patch.object(repo, '_make_url', new_callable=AsyncMock) as mock_url:
                mock_url.return_value = "https://workspace.databricks.com/test"

                with patch.object(repo._client, 'post', new_callable=AsyncMock, return_value=mock_response):
                    messages = [AgentBricksMessage(role="user", content="Hello")]
                    request = AgentBricksQueryRequest(
                        endpoint_name="test-endpoint",
                        messages=messages,
                        return_trace=False
                    )
                    result = await repo.query_endpoint(request)

                    assert result.status == "SUCCESS"
                    # trace should not be populated when return_trace=False
                    assert result.trace is None
                    # but usage should still be there
                    assert result.usage is not None


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

    @pytest.mark.asyncio
    async def test_aclose_closes_client(self):
        """Test that client is closed via aclose."""
        repo = AgentBricksRepository(auth_config=AgentBricksAuthConfig(host="test.databricks.com"))
        await repo.aclose()
        assert repo._client.is_closed

    @pytest.mark.asyncio
    async def test_aclose_with_no_client(self):
        """Test that aclose handles None client without raising."""
        repo = AgentBricksRepository(auth_config=AgentBricksAuthConfig(host="test.databricks.com"))
        repo._client = None
        await repo.aclose()  # Should not raise

    def test_del_with_running_loop(self):
        """Test __del__ schedules client close when event loop is running."""
        repo = AgentBricksRepository(auth_config=AgentBricksAuthConfig(host="test.databricks.com"))
        mock_loop = MagicMock()
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            repo.__del__()
        mock_loop.create_task.assert_called_once()

    def test_del_without_running_loop(self):
        """Test __del__ catches RuntimeError when no event loop is running."""
        repo = AgentBricksRepository(auth_config=AgentBricksAuthConfig(host="test.databricks.com"))
        with patch("asyncio.get_running_loop", side_effect=RuntimeError):
            repo.__del__()  # Should not raise

    def test_del_with_closed_client(self):
        """Test __del__ does not schedule close when client is already closed."""
        repo = AgentBricksRepository(auth_config=AgentBricksAuthConfig(host="test.databricks.com"))
        repo._client = MagicMock(is_closed=True)
        with patch("asyncio.get_running_loop") as mock_get_loop:
            repo.__del__()
        mock_get_loop.assert_not_called()
