"""
Test suite for GenieService
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock

from src.services.genie_service import GenieService
from src.repositories.genie_repository import GenieRepository
from src.schemas.genie import (
    GenieAuthConfig,
    GenieSpace,
    GenieSpacesRequest,
    GenieSpacesResponse,
    GenieConversation,
    GenieMessage,
    GenieMessageStatus,
    GenieQueryResult,
    GenieQueryStatus,
    GenieStartConversationRequest,
    GenieStartConversationResponse,
    GenieSendMessageRequest,
    GenieSendMessageResponse,
    GenieGetMessageStatusRequest,
    GenieGetQueryResultRequest,
    GenieExecutionRequest,
    GenieExecutionResponse
)


class TestGenieService:
    """Test cases for GenieService"""

    @pytest.fixture
    def auth_config(self):
        """Mock auth config"""
        return GenieAuthConfig(
            workspace_url="https://test-workspace.cloud.databricks.com",
            token="test-token",
            user_token="test-user-token"
        )

    @pytest.fixture
    def mock_repository(self):
        """Mock repository"""
        return Mock(spec=GenieRepository)

    @pytest.fixture
    def service(self, auth_config):
        """Create service instance"""
        return GenieService(auth_config)

    @pytest.fixture
    def service_with_mock_repo(self, auth_config, mock_repository):
        """Create service with mocked repository"""
        service = GenieService(auth_config)
        service.repository = mock_repository
        return service

    def test_init_with_auth_config(self, auth_config):
        """Test service initialization with auth config"""
        service = GenieService(auth_config)
        assert service.auth_config == auth_config
        assert isinstance(service.repository, GenieRepository)

    def test_init_without_auth_config(self):
        """Test service initialization without auth config"""
        service = GenieService()
        assert service.auth_config is None
        assert isinstance(service.repository, GenieRepository)

    @pytest.mark.asyncio
    async def test_get_spaces_success(self, service_with_mock_repo, mock_repository):
        """Test successful get_spaces call"""
        # Setup mock data
        mock_spaces = [
            GenieSpace(id="space1", name="Test Space 1", description="Description 1"),
            GenieSpace(id="space2", name="Test Space 2", description="Description 2")
        ]
        mock_response = GenieSpacesResponse(
            spaces=mock_spaces,
            next_page_token=None,
            total_fetched=2
        )
        mock_repository.get_spaces = AsyncMock(return_value=mock_response)

        # Create request
        request = GenieSpacesRequest(page_size=50)

        # Call service method
        result = await service_with_mock_repo.get_spaces(request)

        # Assertions
        assert isinstance(result, GenieSpacesResponse)
        assert len(result.spaces) == 2
        assert result.spaces[0].id == "space1"
        assert result.total_fetched == 2

        # Verify repository was called correctly with all parameters
        mock_repository.get_spaces.assert_called_once_with(
            search_query=None,
            space_ids=None,
            enabled_only=True,
            page_token=None,
            page_size=50,
        )

    @pytest.mark.asyncio
    async def test_get_spaces_with_pagination(self, service_with_mock_repo, mock_repository):
        """Test get_spaces with pagination"""
        mock_response = GenieSpacesResponse(
            spaces=[],
            next_page_token="next-token",
            total_fetched=100
        )
        mock_repository.get_spaces = AsyncMock(return_value=mock_response)

        # Create request with pagination
        request = GenieSpacesRequest(
            page_token="current-token",
            page_size=25
        )

        result = await service_with_mock_repo.get_spaces(request)

        # Verify pagination parameters were passed with all parameters
        mock_repository.get_spaces.assert_called_once_with(
            search_query=None,
            space_ids=None,
            enabled_only=True,
            page_token="current-token",
            page_size=25,
        )
        assert result.next_page_token == "next-token"

    @pytest.mark.asyncio
    async def test_search_spaces_success(self, service_with_mock_repo, mock_repository):
        """Test successful search_spaces call"""
        mock_spaces = [
            GenieSpace(id="space1", name="Development Space", description="Dev space")
        ]
        mock_response = GenieSpacesResponse(
            spaces=mock_spaces,
            next_page_token=None,
            total_fetched=1
        )
        mock_repository.get_spaces = AsyncMock(return_value=mock_response)

        result = await service_with_mock_repo.search_spaces("development", page_size=50)

        # Assertions
        assert len(result.spaces) == 1
        assert result.spaces[0].name == "Development Space"

        # Verify repository was called with correct parameters
        mock_repository.get_spaces.assert_called_once_with(
            search_query="development",
            space_ids=None,
            enabled_only=True,
            page_token=None,
            page_size=50,
        )

    @pytest.mark.asyncio
    async def test_start_conversation_success(self, service_with_mock_repo, mock_repository):
        """Test successful start_conversation call"""
        mock_response = GenieStartConversationResponse(
            conversation_id="conv-123",
            message_id="msg-456",
            space_id="space1"
        )
        mock_repository.start_conversation = AsyncMock(return_value=mock_response)

        # Call service method with individual parameters
        result = await service_with_mock_repo.start_conversation(
            space_id="space1",
            initial_message="Hello, what data do we have?"
        )

        # Assertions
        assert isinstance(result, GenieStartConversationResponse)
        assert result.conversation_id == "conv-123"
        assert result.message_id == "msg-456"

        # Verify repository was called with a request object
        # The service internally creates a GenieStartConversationRequest
        called_request = mock_repository.start_conversation.call_args[0][0]
        assert isinstance(called_request, GenieStartConversationRequest)
        assert called_request.space_id == "space1"
        assert called_request.initial_message == "Hello, what data do we have?"

    @pytest.mark.asyncio
    async def test_send_message_success(self, service_with_mock_repo, mock_repository):
        """Test successful send_message call"""
        mock_response = GenieSendMessageResponse(
            message_id="msg-789",
            conversation_id="conv-123",
            status=GenieMessageStatus.RUNNING
        )
        mock_repository.send_message = AsyncMock(return_value=mock_response)

        # Call service method with individual parameters
        result = await service_with_mock_repo.send_message(
            space_id="space1",
            message="Can you show me more details?",
            conversation_id="conv-123"
        )

        # Assertions
        assert isinstance(result, GenieSendMessageResponse)
        assert result.message_id == "msg-789"
        assert result.conversation_id == "conv-123"

        # Verify repository was called with a request object
        # The service internally creates a GenieSendMessageRequest
        called_request = mock_repository.send_message.call_args[0][0]
        assert isinstance(called_request, GenieSendMessageRequest)
        assert called_request.space_id == "space1"
        assert called_request.message == "Can you show me more details?"
        assert called_request.conversation_id == "conv-123"

    @pytest.mark.asyncio
    async def test_get_message_status_success(self, service_with_mock_repo, mock_repository):
        """Test successful get_message_status call"""
        mock_response = Mock()
        mock_response.status = "COMPLETED"
        mock_response.result = {"data": "test_data"}
        mock_repository.get_message_status = AsyncMock(return_value=mock_response)

        # Create request
        # Call service method directly with parameters
        result = await service_with_mock_repo.get_message_status(
            space_id="space1",
            conversation_id="conv-123", 
            message_id="msg-456"
        )

        # Assertions
        assert result is not None
        assert result.status == "COMPLETED"
        assert result.result == {"data": "test_data"}

        # Verify repository was called with correct parameters
        mock_repository.get_message_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_query_result_success(self, service_with_mock_repo, mock_repository):
        """Test successful get_query_result call"""
        mock_result = GenieQueryResult(
            status=GenieQueryStatus.COMPLETED,
            data=[{"col1": "value1", "col2": "value2"}],
            columns=["col1", "col2"],
            row_count=1
        )
        mock_repository.get_query_result = AsyncMock(return_value=mock_result)

        # Call service method with individual parameters
        result = await service_with_mock_repo.get_query_result(
            space_id="space1",
            conversation_id="conv-123",
            message_id="msg-456"
        )

        # Assertions
        assert isinstance(result, GenieQueryResult)
        assert len(result.data) == 1
        assert result.columns == ["col1", "col2"]
        assert result.row_count == 1

        # Verify repository was called with a request object
        # The service internally creates a GenieGetQueryResultRequest
        called_request = mock_repository.get_query_result.call_args[0][0]
        assert isinstance(called_request, GenieGetQueryResultRequest)
        assert called_request.space_id == "space1"
        assert called_request.conversation_id == "conv-123"
        assert called_request.message_id == "msg-456"

    @pytest.mark.asyncio
    async def test_execute_query_success(self, service_with_mock_repo, mock_repository):
        """Test successful execute_query call"""
        mock_query_result = GenieQueryResult(
            status=GenieQueryStatus.COMPLETED,
            data=[
                {"column1": "result1", "column2": "result2"},
                {"column1": "result3", "column2": "result4"}
            ],
            columns=["column1", "column2"],
            row_count=2
        )
        mock_response = GenieExecutionResponse(
            conversation_id="conv-123",
            message_id="msg-456",
            status=GenieQueryStatus.COMPLETED,
            query_result=mock_query_result
        )
        mock_repository.execute_query = AsyncMock(return_value=mock_response)

        # Call service method with individual parameters
        result = await service_with_mock_repo.execute_query(
            space_id="space1",
            question="SELECT * FROM users LIMIT 10"
        )

        # Assertions
        assert isinstance(result, GenieExecutionResponse)
        assert result.conversation_id == "conv-123"
        assert result.message_id == "msg-456"
        assert result.query_result is not None
        assert len(result.query_result.data) == 2
        assert result.query_result.row_count == 2

        # Verify repository was called with a request object
        # The service internally creates a GenieExecutionRequest
        called_request = mock_repository.execute_query.call_args[0][0]
        assert isinstance(called_request, GenieExecutionRequest)
        assert called_request.space_id == "space1"
        assert called_request.question == "SELECT * FROM users LIMIT 10"

    @pytest.mark.asyncio
    async def test_get_spaces_repository_error(self, service_with_mock_repo, mock_repository):
        """Test get_spaces when repository raises an error"""
        mock_repository.get_spaces = AsyncMock(side_effect=Exception("Repository error"))

        request = GenieSpacesRequest(page_size=50)

        # Repository errors return empty response, not exception
        result = await service_with_mock_repo.get_spaces(request)
        assert result.spaces == []

    @pytest.mark.asyncio
    async def test_search_spaces_repository_error(self, service_with_mock_repo, mock_repository):
        """Test search_spaces when repository raises an error"""
        mock_repository.get_spaces = AsyncMock(side_effect=Exception("Search failed"))

        # search_spaces catches exceptions and returns empty response
        result = await service_with_mock_repo.search_spaces("test", page_size=50)
        assert result.spaces == []

    @pytest.mark.asyncio
    async def test_start_conversation_repository_error(self, service_with_mock_repo, mock_repository):
        """Test start_conversation when repository raises an error"""
        mock_repository.start_conversation = AsyncMock(side_effect=Exception("Conversation start failed"))

        # start_conversation returns None on error
        result = await service_with_mock_repo.start_conversation(
            space_id="space1",
            initial_message="Test message"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_send_message_repository_error(self, service_with_mock_repo, mock_repository):
        """Test send_message when repository raises an error"""
        mock_repository.send_message = AsyncMock(side_effect=Exception("Message send failed"))

        # send_message returns None on error
        result = await service_with_mock_repo.send_message(
            space_id="space1",
            conversation_id="conv-123",
            message="Test message"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_execute_query_repository_error(self, service_with_mock_repo, mock_repository):
        """Test execute_query when repository raises an error"""
        mock_repository.execute_query = AsyncMock(side_effect=Exception("Query execution failed"))

        # execute_query returns a failed response on error
        result = await service_with_mock_repo.execute_query(
            space_id="space1",
            question="SELECT * FROM table"
        )
        assert isinstance(result, GenieExecutionResponse)
        assert result.status == "FAILED"
        assert "Query execution failed" in result.error

    def test_service_provides_consistent_interface(self, service):
        """Test that service provides all expected methods"""
        # Check that all expected async methods exist
        assert hasattr(service, 'get_spaces')
        assert hasattr(service, 'search_spaces')
        assert hasattr(service, 'start_conversation')
        assert hasattr(service, 'send_message')
        assert hasattr(service, 'get_message_status')
        assert hasattr(service, 'get_query_result')
        assert hasattr(service, 'execute_query')

        # Check that methods are callable
        assert callable(service.get_spaces)
        assert callable(service.search_spaces)
        assert callable(service.start_conversation)
        assert callable(service.send_message)
        assert callable(service.get_message_status)
        assert callable(service.get_query_result)
        assert callable(service.execute_query)

    @pytest.mark.asyncio
    async def test_service_request_validation(self, service_with_mock_repo, mock_repository):
        """Test that service validates requests properly"""
        mock_repository.get_spaces = AsyncMock()

        # Test with invalid request type returns empty response (error is logged)
        result = await service_with_mock_repo.get_spaces("invalid_request")
        assert result.spaces == []

    @pytest.mark.asyncio
    async def test_search_spaces_empty_query(self, service_with_mock_repo, mock_repository):
        """Test search_spaces with empty query falls back to get_spaces"""
        mock_response = GenieSpacesResponse(spaces=[], next_page_token=None, total_fetched=0)
        mock_repository.get_spaces = AsyncMock(return_value=mock_response)

        result = await service_with_mock_repo.search_spaces("", page_size=50)

        # Should call get_spaces with empty query and specified page_size
        mock_repository.get_spaces.assert_called_once_with(
            search_query="",
            space_ids=None,
            enabled_only=True,
            page_token=None,
            page_size=50,
        )

    @pytest.mark.asyncio
    async def test_service_with_default_repository_creation(self, auth_config):
        """Test that service creates repository correctly"""
        service = GenieService(auth_config)
        
        # Repository should be created with same auth config
        assert service.repository.auth_config == auth_config

    def test_service_logging_integration(self, service_with_mock_repo, caplog):
        """Test that service logs appropriately"""
        import logging
        
        # Set logger level to capture logs
        logging.getLogger("src.services.genie_service").setLevel(logging.INFO)

        # Create service (this should log something during initialization)
        service = GenieService()
        
        # For now, just verify that service can be created without logging errors
        assert service is not None

    @pytest.mark.asyncio
    async def test_concurrent_requests(self, service_with_mock_repo, mock_repository):
        """Test that service handles concurrent requests properly"""
        import asyncio

        mock_response = GenieSpacesResponse(spaces=[], next_page_token=None, total_fetched=0)
        mock_repository.get_spaces = AsyncMock(return_value=mock_response)

        # Create multiple concurrent requests
        requests = [
            GenieSpacesRequest(page_size=10),
            GenieSpacesRequest(page_size=20),
            GenieSpacesRequest(page_size=30)
        ]

        # Execute concurrently
        results = await asyncio.gather(*[
            service_with_mock_repo.get_spaces(req) for req in requests
        ])

        # All should complete successfully
        assert len(results) == 3
        assert all(isinstance(result, GenieSpacesResponse) for result in results)

        # Repository should have been called 3 times
        assert mock_repository.get_spaces.call_count == 3

    @pytest.mark.asyncio
    async def test_get_spaces_none_request_uses_default(self, service_with_mock_repo, mock_repository):
        """Test get_spaces with None request creates default GenieSpacesRequest"""
        mock_response = GenieSpacesResponse(spaces=[], total_fetched=0)
        mock_repository.get_spaces = AsyncMock(return_value=mock_response)

        result = await service_with_mock_repo.get_spaces(None)

        assert isinstance(result, GenieSpacesResponse)
        # Default request has page_size=100, enabled_only=True, no search/pagination
        mock_repository.get_spaces.assert_called_once_with(
            search_query=None,
            space_ids=None,
            enabled_only=True,
            page_token=None,
            page_size=100,
        )

    @pytest.mark.asyncio
    async def test_search_spaces_with_page_token(self, service_with_mock_repo, mock_repository):
        """Test search_spaces passes page_token correctly"""
        mock_response = GenieSpacesResponse(spaces=[], total_fetched=0)
        mock_repository.get_spaces = AsyncMock(return_value=mock_response)

        result = await service_with_mock_repo.search_spaces(
            "query", page_size=25, page_token="token-abc"
        )

        assert isinstance(result, GenieSpacesResponse)
        mock_repository.get_spaces.assert_called_once_with(
            search_query="query",
            space_ids=None,
            enabled_only=True,
            page_token="token-abc",
            page_size=25,
        )

    @pytest.mark.asyncio
    async def test_search_spaces_no_args_uses_defaults(self, service_with_mock_repo, mock_repository):
        """Test search_spaces with no args uses default page_size from schema"""
        mock_response = GenieSpacesResponse(spaces=[], total_fetched=0)
        mock_repository.get_spaces = AsyncMock(return_value=mock_response)

        result = await service_with_mock_repo.search_spaces()

        assert isinstance(result, GenieSpacesResponse)
        mock_repository.get_spaces.assert_called_once_with(
            search_query=None,
            space_ids=None,
            enabled_only=True,
            page_token=None,
            page_size=100,
        )

    @pytest.mark.asyncio
    async def test_get_space_details_success(self, service_with_mock_repo, mock_repository):
        """Test successful get_space_details call"""
        mock_space = GenieSpace(id="space1", name="Test Space", description="Desc")
        mock_repository.get_space_details = AsyncMock(return_value=mock_space)

        result = await service_with_mock_repo.get_space_details("space1")

        assert isinstance(result, GenieSpace)
        assert result.id == "space1"
        assert result.name == "Test Space"
        mock_repository.get_space_details.assert_called_once_with("space1")

    @pytest.mark.asyncio
    async def test_get_space_details_not_found(self, service_with_mock_repo, mock_repository):
        """Test get_space_details when space is not found"""
        mock_repository.get_space_details = AsyncMock(return_value=None)

        result = await service_with_mock_repo.get_space_details("nonexistent")

        assert result is None
        mock_repository.get_space_details.assert_called_once_with("nonexistent")

    @pytest.mark.asyncio
    async def test_get_space_details_repository_error(self, service_with_mock_repo, mock_repository):
        """Test get_space_details when repository raises an error"""
        mock_repository.get_space_details = AsyncMock(
            side_effect=Exception("Details fetch failed")
        )

        result = await service_with_mock_repo.get_space_details("space1")

        assert result is None

    @pytest.mark.asyncio
    async def test_start_conversation_returns_none(self, service_with_mock_repo, mock_repository):
        """Test start_conversation when repository returns None"""
        mock_repository.start_conversation = AsyncMock(return_value=None)

        result = await service_with_mock_repo.start_conversation(
            space_id="space1",
            initial_message="Hello"
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_send_message_returns_none(self, service_with_mock_repo, mock_repository):
        """Test send_message when repository returns None"""
        mock_repository.send_message = AsyncMock(return_value=None)

        result = await service_with_mock_repo.send_message(
            space_id="space1",
            message="Test message",
            conversation_id="conv-123"
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_get_message_status_repository_error(self, service_with_mock_repo, mock_repository):
        """Test get_message_status when repository raises an error"""
        mock_repository.get_message_status = AsyncMock(
            side_effect=Exception("Status fetch failed")
        )

        result = await service_with_mock_repo.get_message_status(
            space_id="space1",
            conversation_id="conv-123",
            message_id="msg-456"
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_get_query_result_repository_error(self, service_with_mock_repo, mock_repository):
        """Test get_query_result when repository raises an error"""
        mock_repository.get_query_result = AsyncMock(
            side_effect=Exception("Query result fetch failed")
        )

        result = await service_with_mock_repo.get_query_result(
            space_id="space1",
            conversation_id="conv-123",
            message_id="msg-456"
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_execute_query_success_status(self, service_with_mock_repo, mock_repository):
        """Test execute_query when response has SUCCESS status"""
        mock_response = GenieExecutionResponse(
            conversation_id="conv-123",
            message_id="msg-456",
            status=GenieQueryStatus.SUCCESS,
            result="Query completed"
        )
        mock_repository.execute_query = AsyncMock(return_value=mock_response)

        result = await service_with_mock_repo.execute_query(
            space_id="space1",
            question="SELECT count(*) FROM users"
        )

        assert result.status == "SUCCESS"
        assert result.conversation_id == "conv-123"

    @pytest.mark.asyncio
    async def test_execute_query_failed_status(self, service_with_mock_repo, mock_repository):
        """Test execute_query when response has non-SUCCESS status"""
        mock_response = GenieExecutionResponse(
            conversation_id="conv-123",
            message_id="msg-456",
            status=GenieQueryStatus.FAILED,
            error="Query timed out"
        )
        mock_repository.execute_query = AsyncMock(return_value=mock_response)

        result = await service_with_mock_repo.execute_query(
            space_id="space1",
            question="SELECT * FROM huge_table"
        )

        assert result.status == "FAILED"
        assert result.error == "Query timed out"

    @pytest.mark.asyncio
    async def test_validate_space_access_success(self, service_with_mock_repo, mock_repository):
        """Test validate_space_access when space exists"""
        mock_space = GenieSpace(id="space1", name="Test Space")
        mock_repository.get_space_details = AsyncMock(return_value=mock_space)

        result = await service_with_mock_repo.validate_space_access("space1")

        assert result is True
        mock_repository.get_space_details.assert_called_once_with("space1")

    @pytest.mark.asyncio
    async def test_validate_space_access_not_found(self, service_with_mock_repo, mock_repository):
        """Test validate_space_access when space not found"""
        mock_repository.get_space_details = AsyncMock(return_value=None)

        result = await service_with_mock_repo.validate_space_access("nonexistent")

        assert result is False

    @pytest.mark.asyncio
    async def test_validate_space_access_with_auth_config(self, service_with_mock_repo, mock_repository):
        """Test validate_space_access with custom auth config"""
        mock_space = GenieSpace(id="space1", name="Test Space")
        mock_repository.get_space_details = AsyncMock(return_value=mock_space)

        custom_auth = GenieAuthConfig(host="https://custom.databricks.com")
        result = await service_with_mock_repo.validate_space_access(
            "space1", auth_config=custom_auth
        )

        assert result is True
        assert mock_repository.auth_config == custom_auth

    @pytest.mark.asyncio
    async def test_validate_space_access_repository_error(self, service_with_mock_repo, mock_repository):
        """Test validate_space_access when repository raises an error"""
        mock_repository.get_space_details = AsyncMock(
            side_effect=Exception("Access check failed")
        )

        result = await service_with_mock_repo.validate_space_access("space1")

        assert result is False

    @pytest.mark.asyncio
    async def test_search_spaces_exception_in_get_spaces(self, service_with_mock_repo):
        """Test search_spaces exception handler when get_spaces raises unexpectedly"""
        # Patch get_spaces to raise directly (bypassing its own try/except)
        service_with_mock_repo.get_spaces = AsyncMock(
            side_effect=Exception("Unexpected error in get_spaces")
        )

        result = await service_with_mock_repo.search_spaces("test", page_size=50)

        assert isinstance(result, GenieSpacesResponse)
        assert result.spaces == []