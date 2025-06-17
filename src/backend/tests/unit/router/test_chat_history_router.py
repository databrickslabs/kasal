"""
Unit tests for ChatHistory Router.

Tests the API layer of chat history functionality including
endpoint behavior, error handling, and request/response validation.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.api.chat_history_router import router
from src.models.chat_history import ChatHistory
from src.schemas.chat_history import (
    ChatHistoryResponse, 
    ChatHistoryListResponse,
    ChatSessionListResponse,
    SaveMessageRequest
)
from src.services.chat_history_service import ChatHistoryService
from src.utils.user_context import GroupContext


@pytest.fixture
def app():
    """Create FastAPI app with router."""
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def mock_service():
    """Mock chat history service."""
    service = AsyncMock(spec=ChatHistoryService)
    service.save_message = AsyncMock()
    service.get_chat_session = AsyncMock()
    service.get_user_sessions = AsyncMock()
    service.get_group_sessions = AsyncMock()
    service.delete_session = AsyncMock()
    service.count_session_messages = AsyncMock()
    service.generate_session_id = MagicMock()
    return service


@pytest.fixture
def mock_group_context():
    """Mock group context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="test@company.com",
        email_domain="company.com",
        user_id="test@company.com"
    )


@pytest.fixture
def invalid_group_context():
    """Mock invalid group context."""
    return GroupContext()


@pytest.fixture
def sample_chat_message():
    """Sample chat history message."""
    return ChatHistory(
        id="msg-123",
        session_id="session-456",
        user_id="test@company.com",
        message_type="user",
        content="Test message content",
        timestamp=datetime.now(timezone.utc),
        group_id="group-123",
        group_email="test@company.com"
    )


@pytest.fixture
def sample_save_request():
    """Sample save message request."""
    return {
        "session_id": "session-456",
        "message_type": "user",
        "content": "Test message content",
        "intent": "generate_agent",
        "confidence": 0.95,
        "generation_result": {"agent_id": "agent-123"}
    }


def setup_dependencies(app, mock_service, group_context):
    """Helper to setup app dependencies."""
    from src.core.dependencies import get_group_context
    from src.api.chat_history_router import get_chat_history_service
    
    app.dependency_overrides[get_group_context] = lambda: group_context
    app.dependency_overrides[get_chat_history_service] = lambda: mock_service


class TestChatHistoryRouter:
    """Unit tests for chat history router endpoints."""

    def test_save_chat_message_success(self, app, mock_service, mock_group_context, sample_chat_message, sample_save_request):
        """Test successful message saving."""
        # Arrange
        mock_service.save_message.return_value = sample_chat_message
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post(
            "/chat-history/messages",
            json=sample_save_request
        )

        # Assert
        assert response.status_code == 201
        mock_service.save_message.assert_called_once()
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_save_chat_message_invalid_group_context(self, app, mock_service, invalid_group_context, sample_save_request):
        """Test message saving with invalid group context."""
        # Arrange
        setup_dependencies(app, mock_service, invalid_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post(
            "/chat-history/messages",
            json=sample_save_request
        )

        # Assert
        assert response.status_code == 400
        assert "No valid group context provided" in response.json()["detail"]
        mock_service.save_message.assert_not_called()
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_save_chat_message_service_exception(self, app, mock_service, mock_group_context, sample_save_request):
        """Test message saving when service raises exception."""
        # Arrange
        mock_service.save_message.side_effect = Exception("Database error")
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post(
            "/chat-history/messages",
            json=sample_save_request
        )

        # Assert
        assert response.status_code == 500
        assert "Failed to save chat message" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_chat_session_messages_success(self, app, mock_service, mock_group_context, sample_chat_message):
        """Test successful session message retrieval."""
        # Arrange
        mock_service.get_chat_session.return_value = [sample_chat_message]
        mock_service.count_session_messages.return_value = 1
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get(
            "/chat-history/sessions/session-456/messages",
            params={"page": 0, "per_page": 50}
        )

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["session_id"] == "session-456"
        assert data["total_messages"] == 1
        assert data["page"] == 0
        assert data["per_page"] == 50
        assert len(data["messages"]) == 1
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_chat_session_messages_invalid_group_context(self, app, mock_service, invalid_group_context):
        """Test session message retrieval with invalid group context."""
        # Arrange
        setup_dependencies(app, mock_service, invalid_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions/session-456/messages")

        # Assert
        assert response.status_code == 400
        assert "No valid group context provided" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_chat_session_messages_pagination(self, app, mock_service, mock_group_context):
        """Test session message retrieval with pagination parameters."""
        # Arrange
        mock_service.get_chat_session.return_value = []
        mock_service.count_session_messages.return_value = 0
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get(
            "/chat-history/sessions/session-456/messages",
            params={"page": 2, "per_page": 25}
        )

        # Assert
        assert response.status_code == 200
        mock_service.get_chat_session.assert_called_once_with(
            session_id="session-456",
            page=2,
            per_page=25,
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_user_chat_sessions_success(self, app, mock_service, mock_group_context, sample_chat_message):
        """Test successful user session retrieval."""
        # Arrange
        mock_service.get_user_sessions.return_value = [sample_chat_message]
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get(
            "/chat-history/users/sessions",
            params={"page": 0, "per_page": 20}
        )

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        mock_service.get_user_sessions.assert_called_once_with(
            user_id="test@company.com",
            page=0,
            per_page=20,
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_user_chat_sessions_invalid_group_context(self, app, mock_service, invalid_group_context):
        """Test user session retrieval with invalid group context."""
        # Arrange
        setup_dependencies(app, mock_service, invalid_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/users/sessions")

        # Assert
        assert response.status_code == 400
        assert "No valid group context provided" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_group_chat_sessions_success(self, app, mock_service, mock_group_context):
        """Test successful group session retrieval."""
        # Arrange
        mock_sessions = [
            {"session_id": "session-1", "user_id": "user1@company.com", "latest_timestamp": datetime.now(timezone.utc), "message_count": 5},
            {"session_id": "session-2", "user_id": "user2@company.com", "latest_timestamp": datetime.now(timezone.utc), "message_count": 3}
        ]
        mock_service.get_group_sessions.return_value = mock_sessions
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get(
            "/chat-history/sessions",
            params={"page": 0, "per_page": 20}
        )

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["total_sessions"] == 2
        assert data["page"] == 0
        assert data["per_page"] == 20
        assert len(data["sessions"]) == 2
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_group_chat_sessions_with_user_filter(self, app, mock_service, mock_group_context):
        """Test group session retrieval with user filter."""
        # Arrange
        mock_sessions = [
            {"session_id": "session-1", "user_id": "user1@company.com", "latest_timestamp": datetime.now(timezone.utc), "message_count": 5}
        ]
        mock_service.get_group_sessions.return_value = mock_sessions
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get(
            "/chat-history/sessions",
            params={"page": 0, "per_page": 20, "user_id": "user1@company.com"}
        )

        # Assert
        assert response.status_code == 200
        mock_service.get_group_sessions.assert_called_once_with(
            page=0,
            per_page=20,
            user_id="user1@company.com",
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_delete_chat_session_success(self, app, mock_service, mock_group_context):
        """Test successful session deletion."""
        # Arrange
        mock_service.delete_session.return_value = True
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.delete("/chat-history/sessions/session-456")

        # Assert
        assert response.status_code == 204
        assert response.text == ""  # Should have empty response body
        mock_service.delete_session.assert_called_once_with(
            session_id="session-456",
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_delete_chat_session_not_found(self, app, mock_service, mock_group_context):
        """Test session deletion when session not found."""
        # Arrange
        mock_service.delete_session.return_value = False
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.delete("/chat-history/sessions/nonexistent-session")

        # Assert
        assert response.status_code == 404
        assert "Chat session not found" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_delete_chat_session_invalid_group_context(self, app, mock_service, invalid_group_context):
        """Test session deletion with invalid group context."""
        # Arrange
        setup_dependencies(app, mock_service, invalid_group_context)
        
        client = TestClient(app)

        # Act
        response = client.delete("/chat-history/sessions/session-456")

        # Assert
        assert response.status_code == 400
        assert "No valid group context provided" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_create_new_chat_session_success(self, app, mock_service, mock_group_context):
        """Test successful new session creation."""
        # Arrange
        mock_service.generate_session_id.return_value = "new-session-123"
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post("/chat-history/sessions/new")

        # Assert
        assert response.status_code == 201
        data = response.json()
        assert data["session_id"] == "new-session-123"
        mock_service.generate_session_id.assert_called_once()
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_create_new_chat_session_invalid_group_context(self, app, mock_service, invalid_group_context):
        """Test new session creation with invalid group context."""
        # Arrange
        setup_dependencies(app, mock_service, invalid_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post("/chat-history/sessions/new")

        # Assert
        assert response.status_code == 400
        assert "No valid group context provided" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_save_message_validation_error(self, app, mock_service, mock_group_context):
        """Test message saving with validation errors."""
        # Arrange
        invalid_request = {
            "session_id": "",  # Invalid - empty string
            "message_type": "invalid_type",  # Invalid message type
            "content": "",  # Invalid - empty content
        }
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post(
            "/chat-history/messages",
            json=invalid_request
        )

        # Assert
        assert response.status_code == 422  # Validation error
        mock_service.save_message.assert_not_called()
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_session_messages_pagination_validation(self, app, mock_service, mock_group_context):
        """Test session message retrieval with invalid pagination parameters."""
        # Arrange
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act - Test negative page number
        response = client.get(
            "/chat-history/sessions/session-456/messages",
            params={"page": -1, "per_page": 50}
        )

        # Assert
        assert response.status_code == 422  # Validation error

        # Act - Test per_page too large
        response = client.get(
            "/chat-history/sessions/session-456/messages",
            params={"page": 0, "per_page": 200}  # Max is 100
        )

        # Assert
        assert response.status_code == 422  # Validation error
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_save_message_http_exception_passthrough(self, app, mock_service, mock_group_context, sample_save_request):
        """Test that HTTPExceptions from service are passed through."""
        # Arrange
        mock_service.save_message.side_effect = HTTPException(status_code=403, detail="Forbidden access")
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post(
            "/chat-history/messages",
            json=sample_save_request
        )

        # Assert
        assert response.status_code == 403
        assert "Forbidden access" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_router_configuration(self):
        """Test router configuration."""
        # Assert
        assert router.prefix == "/chat-history"
        assert "chat-history" in router.tags
        assert 404 in router.responses
    
    def test_logger_initialization(self):
        """Test logger is properly initialized."""
        from src.api.chat_history_router import logger
        assert logger is not None
        assert logger.name == "src.api.chat_history_router"
    
    def test_imports_available(self):
        """Test that all necessary imports are available."""
        from src.api.chat_history_router import (
            router, 
            logger,
            get_chat_history_service
        )
        # Test imports don't raise errors and modules are available
        assert router is not None
        assert logger is not None
        assert get_chat_history_service is not None
    
    def test_service_dependency_creation(self):
        """Test that the service dependency is properly configured."""
        from src.api.chat_history_router import get_chat_history_service
        from src.core.dependencies import get_service
        from src.services.chat_history_service import ChatHistoryService
        from src.repositories.chat_history_repository import ChatHistoryRepository
        from src.models.chat_history import ChatHistory
        
        assert get_chat_history_service is not None
        # Verify it's the correct dependency function
        expected_dependency = get_service(ChatHistoryService, ChatHistoryRepository, ChatHistory)
        assert callable(get_chat_history_service)
        assert callable(expected_dependency)

    def test_router_endpoint_paths(self):
        """Test that all router endpoints are properly configured."""
        # Arrange
        endpoint_paths = [route.path for route in router.routes]
        
        # Assert
        expected_paths = [
            "/chat-history/messages",
            "/chat-history/sessions/{session_id}/messages", 
            "/chat-history/users/sessions",
            "/chat-history/sessions",
            "/chat-history/sessions/{session_id}",
            "/chat-history/sessions/new"
        ]
        
        for expected_path in expected_paths:
            assert expected_path in endpoint_paths

    def test_router_http_methods(self):
        """Test that router endpoints have correct HTTP methods."""
        # Arrange
        routes_methods = {route.path: route.methods for route in router.routes}
        
        # Assert
        assert "POST" in routes_methods["/chat-history/messages"]
        assert "GET" in routes_methods["/chat-history/sessions/{session_id}/messages"]
        assert "GET" in routes_methods["/chat-history/users/sessions"]
        assert "GET" in routes_methods["/chat-history/sessions"]
        assert "DELETE" in routes_methods["/chat-history/sessions/{session_id}"]
        assert "POST" in routes_methods["/chat-history/sessions/new"]

    def test_save_message_with_unknown_user_fallback(self, app, mock_service, sample_save_request, sample_chat_message):
        """Test save message handles unknown user fallback."""
        # Arrange
        mock_service.save_message.return_value = sample_chat_message  # Return valid message instead of None
        
        # Create a group context with no group_email
        mock_group_context = GroupContext(
            group_ids=["group-123"],
            group_email=None,  # This should trigger unknown_user fallback
            email_domain="company.com",
            user_id=None
        )
        
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post(
            "/chat-history/messages",
            json=sample_save_request
        )

        # Assert
        assert response.status_code == 201
        # Verify the service was called with "unknown_user" as user_id
        mock_service.save_message.assert_called_once()
        call_args = mock_service.save_message.call_args
        assert call_args.kwargs['user_id'] == "unknown_user"
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_user_sessions_with_unknown_user_fallback(self, app, mock_service):
        """Test get user sessions handles unknown user fallback."""
        # Arrange
        mock_service.get_user_sessions.return_value = []
        
        # Create a group context with no group_email
        mock_group_context = GroupContext(
            group_ids=["group-123"],
            group_email=None,  # This should trigger unknown_user fallback
            email_domain="company.com",
            user_id=None
        )
        
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/users/sessions")

        # Assert
        assert response.status_code == 200
        # Verify the service was called with "unknown_user" as user_id
        mock_service.get_user_sessions.assert_called_once()
        call_args = mock_service.get_user_sessions.call_args
        assert call_args.kwargs['user_id'] == "unknown_user"
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_group_sessions_total_calculation(self, app, mock_service, mock_group_context):
        """Test that total_sessions is calculated correctly from returned sessions."""
        # Arrange
        mock_sessions = [
            {"session_id": "session-1", "user_id": "user1@company.com", "latest_timestamp": datetime.now(timezone.utc), "message_count": 5},
            {"session_id": "session-2", "user_id": "user2@company.com", "latest_timestamp": datetime.now(timezone.utc), "message_count": 3},
            {"session_id": "session-3", "user_id": "user3@company.com", "latest_timestamp": datetime.now(timezone.utc), "message_count": 8}
        ]
        mock_service.get_group_sessions.return_value = mock_sessions
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["total_sessions"] == 3  # This tests the len(sessions) calculation on line 201
        assert len(data["sessions"]) == 3
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_group_sessions_empty_result(self, app, mock_service, mock_group_context):
        """Test group sessions endpoint with empty result."""
        # Arrange
        mock_service.get_group_sessions.return_value = []
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["total_sessions"] == 0  # Tests empty list case
        assert len(data["sessions"]) == 0
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_session_messages_http_exception_passthrough(self, app, mock_service, mock_group_context):
        """Test that HTTPExceptions from session messages service are passed through."""
        # Arrange
        mock_service.get_chat_session.side_effect = HTTPException(status_code=403, detail="Forbidden access")
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions/session-456/messages")

        # Assert
        assert response.status_code == 403
        assert "Forbidden access" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_user_sessions_http_exception_passthrough(self, app, mock_service, mock_group_context):
        """Test that HTTPExceptions from user sessions service are passed through."""
        # Arrange
        mock_service.get_user_sessions.side_effect = HTTPException(status_code=403, detail="Forbidden access")
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/users/sessions")

        # Assert
        assert response.status_code == 403
        assert "Forbidden access" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_group_sessions_http_exception_passthrough(self, app, mock_service, mock_group_context):
        """Test that HTTPExceptions from group sessions service are passed through."""
        # Arrange
        mock_service.get_group_sessions.side_effect = HTTPException(status_code=403, detail="Forbidden access")
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions")

        # Assert
        assert response.status_code == 403
        assert "Forbidden access" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_delete_session_http_exception_passthrough(self, app, mock_service, mock_group_context):
        """Test that HTTPExceptions from delete session service are passed through."""
        # Arrange
        mock_service.delete_session.side_effect = HTTPException(status_code=403, detail="Forbidden access")
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.delete("/chat-history/sessions/session-456")

        # Assert
        assert response.status_code == 403
        assert "Forbidden access" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_create_session_http_exception_passthrough(self, app, mock_service, mock_group_context):
        """Test that HTTPExceptions from create session service are passed through."""
        # Arrange
        mock_service.generate_session_id.side_effect = HTTPException(status_code=403, detail="Forbidden access")
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post("/chat-history/sessions/new")

        # Assert
        assert response.status_code == 403
        assert "Forbidden access" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_session_messages_count_service_error(self, app, mock_service, mock_group_context, sample_chat_message):
        """Test session messages when count service fails."""
        # Arrange
        mock_service.get_chat_session.return_value = [sample_chat_message]
        mock_service.count_session_messages.side_effect = Exception("Count service error")
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions/session-456/messages")

        # Assert
        assert response.status_code == 500
        assert "Failed to get chat session messages" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_get_session_messages_count_service_http_exception(self, app, mock_service, mock_group_context, sample_chat_message):
        """Test session messages when count service raises HTTPException."""
        # Arrange
        mock_service.get_chat_session.return_value = [sample_chat_message]
        mock_service.count_session_messages.side_effect = HTTPException(status_code=403, detail="Count access denied")
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions/session-456/messages")

        # Assert
        assert response.status_code == 403
        assert "Count access denied" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    @patch('src.api.chat_history_router.logger')
    def test_save_message_error_logging(self, mock_logger, app, mock_service, mock_group_context, sample_save_request):
        """Test that errors are properly logged when saving messages."""
        # Arrange
        test_error = Exception("Test database error")
        mock_service.save_message.side_effect = test_error
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post(
            "/chat-history/messages",
            json=sample_save_request
        )

        # Assert
        assert response.status_code == 500
        mock_logger.error.assert_called_once_with(f"Error saving chat message: {test_error}")
        
        # Cleanup
        app.dependency_overrides.clear()

    @patch('src.api.chat_history_router.logger')
    def test_get_session_messages_error_logging(self, mock_logger, app, mock_service, mock_group_context):
        """Test that errors are properly logged when getting session messages."""
        # Arrange
        test_error = Exception("Test session error")
        mock_service.get_chat_session.side_effect = test_error
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions/session-456/messages")

        # Assert
        assert response.status_code == 500
        mock_logger.error.assert_called_once_with(f"Error getting chat session messages: {test_error}")
        
        # Cleanup
        app.dependency_overrides.clear()

    @patch('src.api.chat_history_router.logger')
    def test_get_user_sessions_error_logging(self, mock_logger, app, mock_service, mock_group_context):
        """Test that errors are properly logged when getting user sessions."""
        # Arrange
        test_error = Exception("Test user sessions error")
        mock_service.get_user_sessions.side_effect = test_error
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/users/sessions")

        # Assert
        assert response.status_code == 500
        mock_logger.error.assert_called_once_with(f"Error getting user chat sessions: {test_error}")
        
        # Cleanup
        app.dependency_overrides.clear()

    @patch('src.api.chat_history_router.logger')
    def test_get_group_sessions_error_logging(self, mock_logger, app, mock_service, mock_group_context):
        """Test that errors are properly logged when getting group sessions."""
        # Arrange
        test_error = Exception("Test group sessions error")
        mock_service.get_group_sessions.side_effect = test_error
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions")

        # Assert
        assert response.status_code == 500
        mock_logger.error.assert_called_once_with(f"Error getting group chat sessions: {test_error}")
        
        # Cleanup
        app.dependency_overrides.clear()

    @patch('src.api.chat_history_router.logger')
    def test_delete_session_error_logging(self, mock_logger, app, mock_service, mock_group_context):
        """Test that errors are properly logged when deleting session."""
        # Arrange
        test_error = Exception("Test delete error")
        mock_service.delete_session.side_effect = test_error
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.delete("/chat-history/sessions/session-456")

        # Assert
        assert response.status_code == 500
        mock_logger.error.assert_called_once_with(f"Error deleting chat session: {test_error}")
        
        # Cleanup
        app.dependency_overrides.clear()

    @patch('src.api.chat_history_router.logger')
    def test_create_session_error_logging(self, mock_logger, app, mock_service, mock_group_context):
        """Test that errors are properly logged when creating new session."""
        # Arrange
        test_error = Exception("Test create session error")
        mock_service.generate_session_id.side_effect = test_error
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act
        response = client.post("/chat-history/sessions/new")

        # Assert
        assert response.status_code == 500
        mock_logger.error.assert_called_once_with(f"Error creating new chat session: {test_error}")
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_session_messages_with_default_pagination(self, app, mock_service, mock_group_context):
        """Test session messages endpoint with default pagination parameters."""
        # Arrange
        mock_service.get_chat_session.return_value = []
        mock_service.count_session_messages.return_value = 0
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act - No pagination parameters provided, should use defaults
        response = client.get("/chat-history/sessions/session-456/messages")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 0  # Default page
        assert data["per_page"] == 50  # Default per_page
        mock_service.get_chat_session.assert_called_once_with(
            session_id="session-456",
            page=0,
            per_page=50,
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_user_sessions_with_default_pagination(self, app, mock_service, mock_group_context):
        """Test user sessions endpoint with default pagination parameters."""
        # Arrange
        mock_service.get_user_sessions.return_value = []
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act - No pagination parameters provided, should use defaults
        response = client.get("/chat-history/users/sessions")

        # Assert
        assert response.status_code == 200
        mock_service.get_user_sessions.assert_called_once_with(
            user_id="test@company.com",
            page=0,
            per_page=20,
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_group_sessions_with_default_pagination(self, app, mock_service, mock_group_context):
        """Test group sessions endpoint with default pagination parameters."""
        # Arrange
        mock_service.get_group_sessions.return_value = []
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act - No pagination parameters provided, should use defaults
        response = client.get("/chat-history/sessions")

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 0  # Default page
        assert data["per_page"] == 20  # Default per_page
        mock_service.get_group_sessions.assert_called_once_with(
            page=0,
            per_page=20,
            user_id=None,
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_group_sessions_invalid_group_context(self, app, mock_service, invalid_group_context):
        """Test group sessions endpoint with invalid group context."""
        # Arrange
        setup_dependencies(app, mock_service, invalid_group_context)
        
        client = TestClient(app)

        # Act
        response = client.get("/chat-history/sessions")

        # Assert
        assert response.status_code == 400
        assert "No valid group context provided" in response.json()["detail"]
        mock_service.get_group_sessions.assert_not_called()
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_save_message_with_all_optional_fields(self, app, mock_service, mock_group_context, sample_chat_message):
        """Test save message with all optional fields provided."""
        # Arrange
        mock_service.save_message.return_value = sample_chat_message
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)
        
        request_with_all_fields = {
            "session_id": "session-456",
            "message_type": "user",
            "content": "Test message content",
            "intent": "generate_agent",
            "confidence": 0.95,
            "generation_result": {"agent_id": "agent-123", "extra_data": "test"}
        }

        # Act
        response = client.post(
            "/chat-history/messages",
            json=request_with_all_fields
        )

        # Assert
        assert response.status_code == 201
        mock_service.save_message.assert_called_once_with(
            session_id="session-456",
            user_id="test@company.com",
            message_type="user",
            content="Test message content",
            intent="generate_agent",
            confidence=0.95,
            generation_result={"agent_id": "agent-123", "extra_data": "test"},
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_save_message_with_minimal_fields(self, app, mock_service, mock_group_context, sample_chat_message):
        """Test save message with only required fields provided."""
        # Arrange
        mock_service.save_message.return_value = sample_chat_message
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)
        
        minimal_request = {
            "session_id": "session-456",
            "message_type": "user",
            "content": "Test message"
        }

        # Act
        response = client.post(
            "/chat-history/messages",
            json=minimal_request
        )

        # Assert
        assert response.status_code == 201
        mock_service.save_message.assert_called_once_with(
            session_id="session-456",
            user_id="test@company.com",
            message_type="user",
            content="Test message",
            intent=None,
            confidence=None,
            generation_result=None,
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()


class TestChatHistoryRouterIntegration:
    """Integration-style tests for router workflow."""

    def test_complete_chat_workflow(self, app, mock_service, mock_group_context, sample_chat_message):
        """Test complete workflow: create session, save message, retrieve, delete."""
        # Arrange
        mock_service.generate_session_id.return_value = "workflow-session-123"
        mock_service.save_message.return_value = sample_chat_message
        mock_service.get_chat_session.return_value = [sample_chat_message]
        mock_service.count_session_messages.return_value = 1
        mock_service.delete_session.return_value = True
        
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Act & Assert - Create new session
        response = client.post("/chat-history/sessions/new")
        assert response.status_code == 201
        session_id = response.json()["session_id"]

        # Act & Assert - Save message
        save_request = {
            "session_id": session_id,
            "message_type": "user",
            "content": "Test workflow message"
        }
        response = client.post(
            "/chat-history/messages",
            json=save_request
        )
        assert response.status_code == 201

        # Act & Assert - Get session messages
        response = client.get(f"/chat-history/sessions/{session_id}/messages")
        assert response.status_code == 200
        assert response.json()["total_messages"] == 1

        # Act & Assert - Delete session
        response = client.delete(f"/chat-history/sessions/{session_id}")
        assert response.status_code == 204
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_error_handling_consistency(self, app, mock_service, mock_group_context):
        """Test that all endpoints handle service errors consistently."""
        # Arrange
        service_error = Exception("Service unavailable")
        
        # Configure all service methods to raise exceptions
        mock_service.save_message.side_effect = service_error
        mock_service.get_chat_session.side_effect = service_error
        mock_service.get_user_sessions.side_effect = service_error
        mock_service.get_group_sessions.side_effect = service_error
        mock_service.delete_session.side_effect = service_error
        mock_service.generate_session_id.side_effect = service_error
        
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Test all endpoints return 500 for service errors
        endpoints_to_test = [
            ("POST", "/chat-history/messages", {"session_id": "test", "message_type": "user", "content": "test"}),
            ("GET", "/chat-history/sessions/test/messages", None),
            ("GET", "/chat-history/users/sessions", None),
            ("GET", "/chat-history/sessions", None),
            ("DELETE", "/chat-history/sessions/test", None),
            ("POST", "/chat-history/sessions/new", None),
        ]

        for method, endpoint, json_data in endpoints_to_test:
            if method == "POST" and json_data:
                response = client.post(endpoint, json=json_data)
            elif method == "POST":
                response = client.post(endpoint)
            elif method == "GET":
                response = client.get(endpoint)
            elif method == "DELETE":
                response = client.delete(endpoint)

            assert response.status_code == 500
            assert "Failed to" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_group_context_validation_consistency(self, app, mock_service, invalid_group_context):
        """Test that all endpoints validate group context consistently."""
        # Arrange
        setup_dependencies(app, mock_service, invalid_group_context)
        
        client = TestClient(app)

        # Test all endpoints return 400 for invalid group context
        endpoints_to_test = [
            ("POST", "/chat-history/messages", {"session_id": "test", "message_type": "user", "content": "test"}),
            ("GET", "/chat-history/sessions/test/messages", None),
            ("GET", "/chat-history/users/sessions", None),
            ("GET", "/chat-history/sessions", None),
            ("DELETE", "/chat-history/sessions/test", None),
            ("POST", "/chat-history/sessions/new", None),
        ]

        for method, endpoint, json_data in endpoints_to_test:
            if method == "POST" and json_data:
                response = client.post(endpoint, json=json_data)
            elif method == "POST":
                response = client.post(endpoint)
            elif method == "GET":
                response = client.get(endpoint)
            elif method == "DELETE":
                response = client.delete(endpoint)

            assert response.status_code == 400
            assert "No valid group context provided" in response.json()["detail"]
        
        # Cleanup
        app.dependency_overrides.clear()

    def test_pagination_parameter_handling(self, app, mock_service, mock_group_context):
        """Test pagination parameter handling across endpoints."""
        # Arrange
        mock_service.get_chat_session.return_value = []
        mock_service.count_session_messages.return_value = 0
        mock_service.get_user_sessions.return_value = []
        mock_service.get_group_sessions.return_value = []
        setup_dependencies(app, mock_service, mock_group_context)
        
        client = TestClient(app)

        # Test session messages pagination
        response = client.get(
            "/chat-history/sessions/test/messages",
            params={"page": 1, "per_page": 25}
        )
        assert response.status_code == 200
        mock_service.get_chat_session.assert_called_with(
            session_id="test",
            page=1,
            per_page=25,
            group_context=mock_group_context
        )

        # Test user sessions pagination
        response = client.get(
            "/chat-history/users/sessions",
            params={"page": 2, "per_page": 10}
        )
        assert response.status_code == 200
        mock_service.get_user_sessions.assert_called_with(
            user_id="test@company.com",
            page=2,
            per_page=10,
            group_context=mock_group_context
        )

        # Test group sessions pagination
        response = client.get(
            "/chat-history/sessions",
            params={"page": 3, "per_page": 15}
        )
        assert response.status_code == 200
        mock_service.get_group_sessions.assert_called_with(
            page=3,
            per_page=15,
            user_id=None,
            group_context=mock_group_context
        )
        
        # Cleanup
        app.dependency_overrides.clear()