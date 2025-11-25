"""
Unit tests for AgentBricks service.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.agentbricks_service import AgentBricksService
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
    AgentBricksEndpointState,
)


class TestAgentBricksServiceInit:
    """Tests for AgentBricksService initialization."""

    def test_init_without_auth_config(self):
        """Test service initialization without auth config."""
        service = AgentBricksService()
        assert service.auth_config is None
        assert service.repository is not None

    def test_init_with_auth_config(self):
        """Test service initialization with auth config."""
        auth_config = AgentBricksAuthConfig(
            use_obo=True,
            host="https://workspace.databricks.com"
        )
        service = AgentBricksService(auth_config=auth_config)
        assert service.auth_config == auth_config


class TestGetEndpoints:
    """Tests for get_endpoints method."""

    @pytest.fixture
    def service(self):
        """Create a service instance."""
        return AgentBricksService()

    @pytest.fixture
    def mock_endpoints(self):
        """Create mock endpoints."""
        return [
            AgentBricksEndpoint(
                id="ep-1",
                name="agent-endpoint-1",
                state=AgentBricksEndpointState.READY
            ),
            AgentBricksEndpoint(
                id="ep-2",
                name="agent-endpoint-2",
                state=AgentBricksEndpointState.READY
            )
        ]

    @pytest.mark.asyncio
    async def test_get_endpoints_success(self, service, mock_endpoints):
        """Test successful endpoint retrieval."""
        mock_response = AgentBricksEndpointsResponse(
            endpoints=mock_endpoints,
            total_count=2
        )

        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            result = await service.get_endpoints()

            assert len(result.endpoints) == 2
            assert result.total_count == 2
            mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_endpoints_with_request(self, service, mock_endpoints):
        """Test endpoint retrieval with request parameters."""
        request = AgentBricksEndpointsRequest(
            search_query="agent",
            ready_only=True
        )
        mock_response = AgentBricksEndpointsResponse(
            endpoints=mock_endpoints,
            total_count=2,
            filtered=True
        )

        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            result = await service.get_endpoints(request)

            assert result.filtered is True
            mock_get.assert_called_once_with(request)

    @pytest.mark.asyncio
    async def test_get_endpoints_default_request(self, service):
        """Test that default request is created when none provided."""
        mock_response = AgentBricksEndpointsResponse(endpoints=[])

        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            await service.get_endpoints(None)

            # Verify that a default request was created
            call_args = mock_get.call_args[0][0]
            assert isinstance(call_args, AgentBricksEndpointsRequest)

    @pytest.mark.asyncio
    async def test_get_endpoints_error_handling(self, service):
        """Test error handling in get_endpoints."""
        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = Exception("Network error")

            result = await service.get_endpoints()

            assert len(result.endpoints) == 0


class TestSearchEndpoints:
    """Tests for search_endpoints method."""

    @pytest.fixture
    def service(self):
        """Create a service instance."""
        return AgentBricksService()

    @pytest.mark.asyncio
    async def test_search_endpoints_with_query(self, service):
        """Test searching endpoints with a query."""
        mock_response = AgentBricksEndpointsResponse(
            endpoints=[
                AgentBricksEndpoint(id="ep-1", name="agent-search")
            ],
            total_count=1,
            filtered=True
        )

        with patch.object(service, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            result = await service.search_endpoints(query="search")

            assert len(result.endpoints) == 1
            # Verify the request passed to get_endpoints
            call_args = mock_get.call_args[0][0]
            assert call_args.search_query == "search"
            assert call_args.ready_only is True

    @pytest.mark.asyncio
    async def test_search_endpoints_include_not_ready(self, service):
        """Test searching including non-ready endpoints."""
        mock_response = AgentBricksEndpointsResponse(endpoints=[])

        with patch.object(service, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            await service.search_endpoints(query="test", ready_only=False)

            call_args = mock_get.call_args[0][0]
            assert call_args.ready_only is False

    @pytest.mark.asyncio
    async def test_search_endpoints_error_handling(self, service):
        """Test error handling in search_endpoints."""
        with patch.object(service, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = Exception("Search error")

            result = await service.search_endpoints(query="test")

            assert len(result.endpoints) == 0


class TestGetEndpointByName:
    """Tests for get_endpoint_by_name method."""

    @pytest.fixture
    def service(self):
        """Create a service instance."""
        return AgentBricksService()

    @pytest.mark.asyncio
    async def test_get_endpoint_by_name_found(self, service):
        """Test finding endpoint by exact name."""
        target_endpoint = AgentBricksEndpoint(
            id="ep-1",
            name="target-endpoint"
        )
        mock_response = AgentBricksEndpointsResponse(
            endpoints=[target_endpoint]
        )

        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            result = await service.get_endpoint_by_name("target-endpoint")

            assert result is not None
            assert result.name == "target-endpoint"

    @pytest.mark.asyncio
    async def test_get_endpoint_by_name_not_found(self, service):
        """Test when endpoint is not found."""
        mock_response = AgentBricksEndpointsResponse(endpoints=[])

        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            result = await service.get_endpoint_by_name("nonexistent")

            assert result is None

    @pytest.mark.asyncio
    async def test_get_endpoint_by_name_multiple_results(self, service):
        """Test finding exact match when multiple results returned."""
        endpoints = [
            AgentBricksEndpoint(id="ep-1", name="agent-endpoint-1"),
            AgentBricksEndpoint(id="ep-2", name="agent-endpoint-2")
        ]
        mock_response = AgentBricksEndpointsResponse(endpoints=endpoints)

        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            result = await service.get_endpoint_by_name("agent-endpoint-2")

            assert result is not None
            assert result.id == "ep-2"

    @pytest.mark.asyncio
    async def test_get_endpoint_by_name_error_handling(self, service):
        """Test error handling in get_endpoint_by_name."""
        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = Exception("Error")

            result = await service.get_endpoint_by_name("test")

            assert result is None


class TestQueryEndpoint:
    """Tests for query_endpoint method."""

    @pytest.fixture
    def service(self):
        """Create a service instance."""
        return AgentBricksService()

    @pytest.fixture
    def sample_messages(self):
        """Create sample messages."""
        return [AgentBricksMessage(role="user", content="Hello")]

    @pytest.mark.asyncio
    async def test_query_endpoint_success(self, service, sample_messages):
        """Test successful endpoint query."""
        mock_response = AgentBricksQueryResponse(
            response="Hello! How can I help?",
            status=AgentBricksQueryStatus.SUCCESS
        )

        with patch.object(service.repository, 'query_endpoint', new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            result = await service.query_endpoint(
                endpoint_name="test-endpoint",
                messages=sample_messages
            )

            assert result.status == "SUCCESS"
            assert "Hello" in result.response

    @pytest.mark.asyncio
    async def test_query_endpoint_with_options(self, service, sample_messages):
        """Test query with optional parameters."""
        mock_response = AgentBricksQueryResponse(
            response="Answer",
            status=AgentBricksQueryStatus.SUCCESS,
            trace={"steps": []}
        )

        with patch.object(service.repository, 'query_endpoint', new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            result = await service.query_endpoint(
                endpoint_name="test-endpoint",
                messages=sample_messages,
                custom_inputs={"context": "test"},
                return_trace=True
            )

            assert result.trace is not None
            # Verify the request passed to repository
            call_args = mock_query.call_args[0][0]
            assert call_args.custom_inputs == {"context": "test"}
            assert call_args.return_trace is True

    @pytest.mark.asyncio
    async def test_query_endpoint_failure(self, service, sample_messages):
        """Test handling query failure."""
        mock_response = AgentBricksQueryResponse(
            response="",
            status=AgentBricksQueryStatus.FAILED,
            error="Authentication failed"
        )

        with patch.object(service.repository, 'query_endpoint', new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            result = await service.query_endpoint(
                endpoint_name="test-endpoint",
                messages=sample_messages
            )

            assert result.status == "FAILED"
            assert result.error is not None

    @pytest.mark.asyncio
    async def test_query_endpoint_exception(self, service, sample_messages):
        """Test handling exceptions in query."""
        with patch.object(service.repository, 'query_endpoint', new_callable=AsyncMock) as mock_query:
            mock_query.side_effect = Exception("Connection error")

            result = await service.query_endpoint(
                endpoint_name="test-endpoint",
                messages=sample_messages
            )

            assert result.status == "FAILED"
            assert "Connection error" in result.error


class TestExecuteQuery:
    """Tests for execute_query method."""

    @pytest.fixture
    def service(self):
        """Create a service instance."""
        return AgentBricksService()

    @pytest.mark.asyncio
    async def test_execute_query_success(self, service):
        """Test successful query execution."""
        mock_response = AgentBricksExecutionResponse(
            endpoint_name="test-endpoint",
            status=AgentBricksQueryStatus.SUCCESS,
            result="Here is your answer"
        )

        with patch.object(service.repository, 'execute_query', new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_response

            result = await service.execute_query(
                endpoint_name="test-endpoint",
                question="What is the answer?"
            )

            assert result.status == "SUCCESS"
            assert result.result == "Here is your answer"

    @pytest.mark.asyncio
    async def test_execute_query_with_options(self, service):
        """Test execute query with all options."""
        mock_response = AgentBricksExecutionResponse(
            endpoint_name="test-endpoint",
            status=AgentBricksQueryStatus.SUCCESS,
            result="Answer",
            trace={"time": 1.5}
        )

        with patch.object(service.repository, 'execute_query', new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_response

            result = await service.execute_query(
                endpoint_name="test-endpoint",
                question="Hello",
                custom_inputs={"key": "value"},
                return_trace=True,
                timeout=60
            )

            # Verify the request
            call_args = mock_exec.call_args[0][0]
            assert call_args.custom_inputs == {"key": "value"}
            assert call_args.return_trace is True
            assert call_args.timeout == 60

    @pytest.mark.asyncio
    async def test_execute_query_failure(self, service):
        """Test handling execution failure."""
        mock_response = AgentBricksExecutionResponse(
            endpoint_name="test-endpoint",
            status=AgentBricksQueryStatus.FAILED,
            error="Endpoint not available"
        )

        with patch.object(service.repository, 'execute_query', new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_response

            result = await service.execute_query(
                endpoint_name="test-endpoint",
                question="Hello"
            )

            assert result.status == "FAILED"
            assert result.error is not None

    @pytest.mark.asyncio
    async def test_execute_query_exception(self, service):
        """Test handling exceptions in execute."""
        with patch.object(service.repository, 'execute_query', new_callable=AsyncMock) as mock_exec:
            mock_exec.side_effect = Exception("Network error")

            result = await service.execute_query(
                endpoint_name="test-endpoint",
                question="Hello"
            )

            assert result.status == "FAILED"
            assert "Network error" in result.error


class TestValidateEndpointAccess:
    """Tests for validate_endpoint_access method."""

    @pytest.fixture
    def service(self):
        """Create a service instance."""
        return AgentBricksService()

    @pytest.mark.asyncio
    async def test_validate_access_success(self, service):
        """Test successful access validation."""
        with patch.object(service, 'get_endpoint_by_name', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = AgentBricksEndpoint(id="ep-1", name="test")

            result = await service.validate_endpoint_access("test")

            assert result is True

    @pytest.mark.asyncio
    async def test_validate_access_no_access(self, service):
        """Test when no access to endpoint."""
        with patch.object(service, 'get_endpoint_by_name', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None

            result = await service.validate_endpoint_access("test")

            assert result is False

    @pytest.mark.asyncio
    async def test_validate_access_with_auth_config(self, service):
        """Test validation with custom auth config."""
        auth_config = AgentBricksAuthConfig(use_obo=False)

        with patch.object(service, 'get_endpoint_by_name', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = AgentBricksEndpoint(id="ep-1", name="test")

            result = await service.validate_endpoint_access("test", auth_config)

            assert result is True
            # Verify auth config was set on repository
            assert service.repository.auth_config == auth_config

    @pytest.mark.asyncio
    async def test_validate_access_error(self, service):
        """Test error handling in validation."""
        with patch.object(service, 'get_endpoint_by_name', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = Exception("Error")

            result = await service.validate_endpoint_access("test")

            assert result is False


class TestListReadyEndpoints:
    """Tests for list_ready_endpoints method."""

    @pytest.fixture
    def service(self):
        """Create a service instance."""
        return AgentBricksService()

    @pytest.mark.asyncio
    async def test_list_ready_endpoints(self, service):
        """Test listing ready endpoints."""
        endpoints = [
            AgentBricksEndpoint(id="ep-1", name="ready-1", state=AgentBricksEndpointState.READY),
            AgentBricksEndpoint(id="ep-2", name="ready-2", state=AgentBricksEndpointState.READY)
        ]
        mock_response = AgentBricksEndpointsResponse(endpoints=endpoints)

        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            result = await service.list_ready_endpoints()

            assert len(result) == 2
            # Verify ready_only=True was passed
            call_args = mock_get.call_args[0][0]
            assert call_args.ready_only is True

    @pytest.mark.asyncio
    async def test_list_ready_endpoints_empty(self, service):
        """Test when no ready endpoints available."""
        mock_response = AgentBricksEndpointsResponse(endpoints=[])

        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response

            result = await service.list_ready_endpoints()

            assert len(result) == 0

    @pytest.mark.asyncio
    async def test_list_ready_endpoints_error(self, service):
        """Test error handling in list ready endpoints."""
        with patch.object(service.repository, 'get_endpoints', new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = Exception("Error")

            result = await service.list_ready_endpoints()

            assert len(result) == 0
