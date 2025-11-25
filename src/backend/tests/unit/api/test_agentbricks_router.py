"""
Unit tests for AgentBricks API router.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.api.agentbricks_router import (
    router,
    get_agentbricks_endpoints,
    search_agentbricks_endpoints,
    get_agentbricks_endpoint_details,
    query_agentbricks_endpoint,
    execute_agentbricks_query,
)
from src.schemas.agentbricks import (
    AgentBricksEndpoint,
    AgentBricksEndpointsRequest,
    AgentBricksEndpointsResponse,
    AgentBricksQueryRequest,
    AgentBricksQueryResponse,
    AgentBricksQueryStatus,
    AgentBricksExecutionRequest,
    AgentBricksExecutionResponse,
    AgentBricksAuthConfig,
    AgentBricksMessage,
)


class TestRouterConfiguration:
    """Tests for router configuration."""

    def test_router_prefix(self):
        """Test that router has correct prefix."""
        assert router.prefix == "/api/agentbricks"

    def test_router_tags(self):
        """Test that router has correct tags."""
        assert "agentbricks" in router.tags


class TestGetAgentBricksEndpoints:
    """Tests for get_agentbricks_endpoints endpoint."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock FastAPI request."""
        request = MagicMock()
        request.headers = {}
        return request

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.group_ids = ["group-1"]
        return context

    @pytest.mark.asyncio
    async def test_get_endpoints_success(self, mock_request, mock_group_context):
        """Test successful retrieval of endpoints."""
        mock_endpoints = [
            AgentBricksEndpoint(id="ep-1", name="endpoint-1"),
            AgentBricksEndpoint(id="ep-2", name="endpoint-2"),
        ]
        mock_response = AgentBricksEndpointsResponse(
            endpoints=mock_endpoints,
            total_count=2,
            filtered=False
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await get_agentbricks_endpoints(
                        request=mock_request,
                        ready_only=True,
                        search_query=None,
                        group_context=mock_group_context
                    )

        assert len(result.endpoints) == 2
        assert result.total_count == 2

    @pytest.mark.asyncio
    async def test_get_endpoints_with_search_query(self, mock_request, mock_group_context):
        """Test get endpoints with search query filter."""
        mock_response = AgentBricksEndpointsResponse(
            endpoints=[AgentBricksEndpoint(id="ep-1", name="matching-endpoint")],
            total_count=1,
            filtered=True
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await get_agentbricks_endpoints(
                        request=mock_request,
                        ready_only=True,
                        search_query="matching",
                        group_context=mock_group_context
                    )

        assert result.filtered is True

    @pytest.mark.asyncio
    async def test_get_endpoints_empty_list(self, mock_request, mock_group_context):
        """Test get endpoints returns empty list."""
        mock_response = AgentBricksEndpointsResponse(
            endpoints=[],
            total_count=0,
            filtered=False
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await get_agentbricks_endpoints(
                        request=mock_request,
                        ready_only=True,
                        search_query=None,
                        group_context=mock_group_context
                    )

        assert len(result.endpoints) == 0

    @pytest.mark.asyncio
    async def test_get_endpoints_without_group_context(self, mock_request):
        """Test get endpoints without group context."""
        mock_response = AgentBricksEndpointsResponse(
            endpoints=[],
            total_count=0,
            filtered=False
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.return_value = mock_response
                mock_service_class.return_value = mock_service

                result = await get_agentbricks_endpoints(
                    request=mock_request,
                    ready_only=True,
                    search_query=None,
                    group_context=None
                )

        assert result is not None

    @pytest.mark.asyncio
    async def test_get_endpoints_service_error(self, mock_request, mock_group_context):
        """Test get endpoints handles service errors."""
        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.side_effect = Exception("Service error")
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    with pytest.raises(HTTPException) as exc_info:
                        await get_agentbricks_endpoints(
                            request=mock_request,
                            ready_only=True,
                            search_query=None,
                            group_context=mock_group_context
                        )

        assert exc_info.value.status_code == 500
        assert "Failed to fetch AgentBricks endpoints" in exc_info.value.detail


class TestSearchAgentBricksEndpoints:
    """Tests for search_agentbricks_endpoints endpoint."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock FastAPI request."""
        request = MagicMock()
        request.headers = {}
        return request

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.group_ids = ["group-1"]
        return context

    @pytest.mark.asyncio
    async def test_search_endpoints_success(self, mock_request, mock_group_context):
        """Test successful search of endpoints."""
        search_request = AgentBricksEndpointsRequest(
            search_query="agent",
            ready_only=True
        )
        mock_response = AgentBricksEndpointsResponse(
            endpoints=[AgentBricksEndpoint(id="ep-1", name="agent-endpoint")],
            total_count=1,
            filtered=True
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await search_agentbricks_endpoints(
                        request=mock_request,
                        endpoints_request=search_request,
                        group_context=mock_group_context
                    )

        assert len(result.endpoints) == 1
        assert result.filtered is True

    @pytest.mark.asyncio
    async def test_search_endpoints_with_multiple_filters(self, mock_request, mock_group_context):
        """Test search with multiple filters."""
        search_request = AgentBricksEndpointsRequest(
            search_query="test",
            endpoint_ids=["ep-1", "ep-2"],
            ready_only=False,
            creator_filter="user@example.com"
        )
        mock_response = AgentBricksEndpointsResponse(
            endpoints=[],
            total_count=0,
            filtered=True
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await search_agentbricks_endpoints(
                        request=mock_request,
                        endpoints_request=search_request,
                        group_context=mock_group_context
                    )

        assert result.filtered is True

    @pytest.mark.asyncio
    async def test_search_endpoints_error(self, mock_request, mock_group_context):
        """Test search endpoints handles errors."""
        search_request = AgentBricksEndpointsRequest(search_query="test")

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.side_effect = Exception("Search error")
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    with pytest.raises(HTTPException) as exc_info:
                        await search_agentbricks_endpoints(
                            request=mock_request,
                            endpoints_request=search_request,
                            group_context=mock_group_context
                        )

        assert exc_info.value.status_code == 500
        assert "Failed to search AgentBricks endpoints" in exc_info.value.detail


class TestGetAgentBricksEndpointDetails:
    """Tests for get_agentbricks_endpoint_details endpoint."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock FastAPI request."""
        request = MagicMock()
        request.headers = {}
        return request

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.group_ids = ["group-1"]
        return context

    @pytest.mark.asyncio
    async def test_get_endpoint_details_success(self, mock_request, mock_group_context):
        """Test successful retrieval of endpoint details."""
        endpoint_name = "test-endpoint"
        mock_endpoint = AgentBricksEndpoint(
            id="ep-123",
            name=endpoint_name,
            creator="user@example.com",
            task="llm/v1/chat"
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoint_by_name.return_value = mock_endpoint
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await get_agentbricks_endpoint_details(
                        endpoint_name=endpoint_name,
                        request=mock_request,
                        group_context=mock_group_context
                    )

        assert result.name == endpoint_name
        assert result.id == "ep-123"

    @pytest.mark.asyncio
    async def test_get_endpoint_details_not_found(self, mock_request, mock_group_context):
        """Test get endpoint details when endpoint not found."""
        endpoint_name = "non-existent"

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoint_by_name.return_value = None
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    with pytest.raises(HTTPException) as exc_info:
                        await get_agentbricks_endpoint_details(
                            endpoint_name=endpoint_name,
                            request=mock_request,
                            group_context=mock_group_context
                        )

        assert exc_info.value.status_code == 404
        assert endpoint_name in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_get_endpoint_details_service_error(self, mock_request, mock_group_context):
        """Test get endpoint details handles service errors."""
        endpoint_name = "test-endpoint"

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoint_by_name.side_effect = Exception("Service error")
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    with pytest.raises(HTTPException) as exc_info:
                        await get_agentbricks_endpoint_details(
                            endpoint_name=endpoint_name,
                            request=mock_request,
                            group_context=mock_group_context
                        )

        assert exc_info.value.status_code == 500
        assert "Failed to fetch endpoint details" in exc_info.value.detail


class TestQueryAgentBricksEndpoint:
    """Tests for query_agentbricks_endpoint endpoint."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock FastAPI request."""
        request = MagicMock()
        request.headers = {}
        return request

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.group_ids = ["group-1"]
        return context

    @pytest.mark.asyncio
    async def test_query_endpoint_success(self, mock_request, mock_group_context):
        """Test successful query to endpoint."""
        query_request = AgentBricksQueryRequest(
            endpoint_name="test-endpoint",
            messages=[AgentBricksMessage(role="user", content="Hello")]
        )
        mock_response = AgentBricksQueryResponse(
            response="Hi there!",
            status=AgentBricksQueryStatus.SUCCESS
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.query_endpoint.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await query_agentbricks_endpoint(
                        request=mock_request,
                        query_request=query_request,
                        group_context=mock_group_context
                    )

        assert result.response == "Hi there!"
        assert result.status == "SUCCESS"

    @pytest.mark.asyncio
    async def test_query_endpoint_with_custom_inputs(self, mock_request, mock_group_context):
        """Test query with custom inputs."""
        query_request = AgentBricksQueryRequest(
            endpoint_name="test-endpoint",
            messages=[AgentBricksMessage(role="user", content="Hello")],
            custom_inputs={"context": "test context"},
            return_trace=True
        )
        mock_response = AgentBricksQueryResponse(
            response="Response with context",
            status=AgentBricksQueryStatus.SUCCESS,
            trace={"steps": ["step1"]}
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.query_endpoint.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await query_agentbricks_endpoint(
                        request=mock_request,
                        query_request=query_request,
                        group_context=mock_group_context
                    )

        assert result.trace is not None

    @pytest.mark.asyncio
    async def test_query_endpoint_error(self, mock_request, mock_group_context):
        """Test query endpoint handles errors."""
        query_request = AgentBricksQueryRequest(
            endpoint_name="test-endpoint",
            messages=[AgentBricksMessage(role="user", content="Hello")]
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.query_endpoint.side_effect = Exception("Query error")
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    with pytest.raises(HTTPException) as exc_info:
                        await query_agentbricks_endpoint(
                            request=mock_request,
                            query_request=query_request,
                            group_context=mock_group_context
                        )

        assert exc_info.value.status_code == 500
        assert "Failed to query endpoint" in exc_info.value.detail


class TestExecuteAgentBricksQuery:
    """Tests for execute_agentbricks_query endpoint."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock FastAPI request."""
        request = MagicMock()
        request.headers = {}
        return request

    @pytest.fixture
    def mock_group_context(self):
        """Create a mock group context."""
        context = MagicMock()
        context.group_ids = ["group-1"]
        return context

    @pytest.mark.asyncio
    async def test_execute_query_success(self, mock_request, mock_group_context):
        """Test successful query execution."""
        execution_request = AgentBricksExecutionRequest(
            endpoint_name="test-endpoint",
            question="What is the weather?"
        )
        mock_response = AgentBricksExecutionResponse(
            endpoint_name="test-endpoint",
            status=AgentBricksQueryStatus.SUCCESS,
            result="The weather is sunny"
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.execute_query.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await execute_agentbricks_query(
                        request=mock_request,
                        execution_request=execution_request,
                        group_context=mock_group_context
                    )

        assert result.result == "The weather is sunny"
        assert result.status == "SUCCESS"

    @pytest.mark.asyncio
    async def test_execute_query_with_options(self, mock_request, mock_group_context):
        """Test execute query with all options."""
        execution_request = AgentBricksExecutionRequest(
            endpoint_name="test-endpoint",
            question="What is the weather?",
            custom_inputs={"location": "NYC"},
            return_trace=True,
            timeout=60
        )
        mock_response = AgentBricksExecutionResponse(
            endpoint_name="test-endpoint",
            status=AgentBricksQueryStatus.SUCCESS,
            result="The weather in NYC is sunny",
            trace={"execution_time": 1.5}
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.execute_query.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    result = await execute_agentbricks_query(
                        request=mock_request,
                        execution_request=execution_request,
                        group_context=mock_group_context
                    )

        assert result.trace is not None
        mock_service.execute_query.assert_called_once_with(
            endpoint_name="test-endpoint",
            question="What is the weather?",
            custom_inputs={"location": "NYC"},
            return_trace=True,
            timeout=60
        )

    @pytest.mark.asyncio
    async def test_execute_query_default_timeout(self, mock_request, mock_group_context):
        """Test execute query uses default timeout."""
        execution_request = AgentBricksExecutionRequest(
            endpoint_name="test-endpoint",
            question="Hello"
        )
        mock_response = AgentBricksExecutionResponse(
            endpoint_name="test-endpoint",
            status=AgentBricksQueryStatus.SUCCESS,
            result="Hi"
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.execute_query.return_value = mock_response
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    await execute_agentbricks_query(
                        request=mock_request,
                        execution_request=execution_request,
                        group_context=mock_group_context
                    )

        # Default timeout should be 120
        mock_service.execute_query.assert_called_once()
        call_kwargs = mock_service.execute_query.call_args[1]
        assert call_kwargs['timeout'] == 120

    @pytest.mark.asyncio
    async def test_execute_query_error(self, mock_request, mock_group_context):
        """Test execute query handles errors."""
        execution_request = AgentBricksExecutionRequest(
            endpoint_name="test-endpoint",
            question="Hello"
        )

        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "test-token"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.execute_query.side_effect = Exception("Execution error")
                mock_service_class.return_value = mock_service

                with patch('src.api.agentbricks_router.UserContext.set_group_context'):
                    with pytest.raises(HTTPException) as exc_info:
                        await execute_agentbricks_query(
                            request=mock_request,
                            execution_request=execution_request,
                            group_context=mock_group_context
                        )

        assert exc_info.value.status_code == 500
        assert "Failed to execute query" in exc_info.value.detail


class TestAuthConfigCreation:
    """Tests for authentication configuration in router."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock FastAPI request."""
        request = MagicMock()
        request.headers = {}
        return request

    @pytest.mark.asyncio
    async def test_auth_config_with_user_token(self, mock_request):
        """Test auth config is created with user token."""
        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = "obo-token-123"
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.return_value = AgentBricksEndpointsResponse()
                mock_service_class.return_value = mock_service

                await get_agentbricks_endpoints(
                    request=mock_request,
                    ready_only=True,
                    search_query=None,
                    group_context=None
                )

        # Check that service was created with auth config containing user token
        call_args = mock_service_class.call_args[0][0]
        assert isinstance(call_args, AgentBricksAuthConfig)
        assert call_args.use_obo is True

    @pytest.mark.asyncio
    async def test_auth_config_without_user_token(self, mock_request):
        """Test auth config when no user token available."""
        with patch('src.api.agentbricks_router.extract_user_token_from_request') as mock_extract:
            mock_extract.return_value = None
            with patch('src.api.agentbricks_router.AgentBricksService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_endpoints.return_value = AgentBricksEndpointsResponse()
                mock_service_class.return_value = mock_service

                await get_agentbricks_endpoints(
                    request=mock_request,
                    ready_only=True,
                    search_query=None,
                    group_context=None
                )

        # Service should still be created
        mock_service_class.assert_called_once()
