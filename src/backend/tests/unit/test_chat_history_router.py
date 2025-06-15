"""
Unit tests for ChatHistoryRouter.

Tests the functionality of the chat history router including
message CRUD operations, session management, group context handling, and error scenarios.
"""
import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.api.chat_history_router import router
from src.schemas.chat_history import (
    SaveMessageRequest, 
    ChatHistoryResponse,
    ChatHistoryListResponse,
    ChatSessionListResponse
)


@pytest.fixture
def mock_chat_history_service():
    """Create a mock chat history service."""
    service = AsyncMock()
    
    # Create mock chat history objects
    mock_chat = MagicMock()
    mock_chat.id = "chat-123"
    mock_chat.session_id = "session-456"
    mock_chat.user_id = "user-789"
    mock_chat.message_type = "user"
    mock_chat.content = "Test message"
    mock_chat.timestamp = datetime.now(UTC)
    mock_chat.intent = None
    mock_chat.confidence = None
    mock_chat.generation_result = None
    mock_chat.group_id = "group-123"
    mock_chat.group_email = "test@example.com"
    
    # Setup service method returns
    service.save_message.return_value = mock_chat
    service.get_chat_session.return_value = [mock_chat]
    service.get_user_sessions.return_value = [mock_chat]
    service.get_group_sessions.return_value = [
        {"session_id": "session-1", "user_id": "user-1", "latest_timestamp": datetime.now(UTC)}
    ]
    service.delete_session.return_value = True
    service.count_session_messages.return_value = 5
    # generate_session_id is not async, so use regular MagicMock
    service.generate_session_id = MagicMock(return_value="new-session-123")
    
    return service


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.group_id = "group-123"
    context.group_email = "admin@company.com"
    context.group_ids = ["group-123", "group-456"]
    context.is_valid.return_value = True
    return context


@pytest.fixture
def mock_invalid_group_context():
    """Create a mock invalid group context."""
    context = MagicMock()
    context.is_valid.return_value = False
    return context


class TestChatHistoryRouter:
    """Test cases for ChatHistory router endpoints."""

    @pytest.mark.asyncio
    async def test_save_chat_message_success(self, mock_chat_history_service, mock_group_context):
        """Test successful chat message saving."""
        # Arrange
        message_request = SaveMessageRequest(
            session_id="session-123",
            message_type="user",
            content="Test message",
            intent="generate_agent",
            confidence=0.95,
            generation_result={"agent_name": "Test Agent"}
        )
        
        with patch('src.api.chat_history_router.get_chat_history_service', return_value=mock_chat_history_service):
            # Import and test the router function
            from src.api.chat_history_router import save_chat_message
            
            # Act
            result = await save_chat_message(
                message_request=message_request,
                service=mock_chat_history_service,
                group_context=mock_group_context
            )
            
            # Assert
            assert result is not None
            mock_chat_history_service.save_message.assert_called_once()
            call_args = mock_chat_history_service.save_message.call_args
            assert call_args.kwargs['session_id'] == "session-123"
            assert call_args.kwargs['message_type'] == "user"
            assert call_args.kwargs['content'] == "Test message"
            assert call_args.kwargs['user_id'] == mock_group_context.group_email
            assert call_args.kwargs['group_context'] == mock_group_context

    @pytest.mark.asyncio
    async def test_save_chat_message_invalid_group_context(self, mock_chat_history_service, mock_invalid_group_context):
        """Test chat message saving with invalid group context."""
        # Arrange
        message_request = SaveMessageRequest(
            session_id="session-123",
            message_type="user",
            content="Test message"
        )
        
        from src.api.chat_history_router import save_chat_message
        
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await save_chat_message(
                message_request=message_request,
                service=mock_chat_history_service,
                group_context=mock_invalid_group_context
            )
        
        assert exc_info.value.status_code == 400
        assert "No valid group context provided" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_save_chat_message_service_error(self, mock_chat_history_service, mock_group_context):
        """Test chat message saving with service error."""
        # Arrange
        message_request = SaveMessageRequest(
            session_id="session-123",
            message_type="user",
            content="Test message"
        )
        
        mock_chat_history_service.save_message.side_effect = Exception("Database error")
        
        from src.api.chat_history_router import save_chat_message
        
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await save_chat_message(
                message_request=message_request,
                service=mock_chat_history_service,
                group_context=mock_group_context
            )
        
        assert exc_info.value.status_code == 500
        assert "Failed to save chat message" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_chat_session_messages_success(self, mock_chat_history_service, mock_group_context):
        """Test successful chat session messages retrieval."""
        # Arrange
        session_id = "session-123"
        page = 0
        per_page = 50
        
        from src.api.chat_history_router import get_chat_session_messages
        
        # Act
        result = await get_chat_session_messages(
            session_id=session_id,
            page=page,
            per_page=per_page,
            service=mock_chat_history_service,
            group_context=mock_group_context
        )
        
        # Assert
        assert isinstance(result, ChatHistoryListResponse)
        assert result.session_id == session_id
        assert result.page == page
        assert result.per_page == per_page
        mock_chat_history_service.get_chat_session.assert_called_once_with(
            session_id=session_id,
            page=page,
            per_page=per_page,
            group_context=mock_group_context
        )
        mock_chat_history_service.count_session_messages.assert_called_once_with(
            session_id=session_id,
            group_context=mock_group_context
        )

    @pytest.mark.asyncio
    async def test_get_chat_session_messages_invalid_group_context(self, mock_chat_history_service, mock_invalid_group_context):
        """Test chat session messages retrieval with invalid group context."""
        # Arrange
        session_id = "session-123"
        
        from src.api.chat_history_router import get_chat_session_messages
        
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await get_chat_session_messages(
                session_id=session_id,
                service=mock_chat_history_service,
                group_context=mock_invalid_group_context
            )
        
        assert exc_info.value.status_code == 400
        assert "No valid group context provided" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_user_chat_sessions_success(self, mock_chat_history_service, mock_group_context):
        """Test successful user chat sessions retrieval."""
        # Arrange
        page = 0
        per_page = 20
        
        from src.api.chat_history_router import get_user_chat_sessions
        
        # Act
        result = await get_user_chat_sessions(
            page=page,
            per_page=per_page,
            service=mock_chat_history_service,
            group_context=mock_group_context
        )
        
        # Assert
        assert isinstance(result, list)
        mock_chat_history_service.get_user_sessions.assert_called_once_with(
            user_id=mock_group_context.group_email,
            page=page,
            per_page=per_page,
            group_context=mock_group_context
        )

    @pytest.mark.asyncio
    async def test_get_group_chat_sessions_success(self, mock_chat_history_service, mock_group_context):
        """Test successful group chat sessions retrieval."""
        # Arrange
        page = 0
        per_page = 20
        user_id = "specific-user"
        
        from src.api.chat_history_router import get_group_chat_sessions
        
        # Act
        result = await get_group_chat_sessions(
            page=page,
            per_page=per_page,
            user_id=user_id,
            service=mock_chat_history_service,
            group_context=mock_group_context
        )
        
        # Assert
        assert isinstance(result, ChatSessionListResponse)
        assert result.page == page
        assert result.per_page == per_page
        mock_chat_history_service.get_group_sessions.assert_called_once_with(
            page=page,
            per_page=per_page,
            user_id=user_id,
            group_context=mock_group_context
        )

    @pytest.mark.asyncio
    async def test_get_group_chat_sessions_without_user_filter(self, mock_chat_history_service, mock_group_context):
        """Test group chat sessions retrieval without user filter."""
        # Arrange
        from src.api.chat_history_router import get_group_chat_sessions
        
        # Act
        result = await get_group_chat_sessions(
            service=mock_chat_history_service,
            group_context=mock_group_context,
            page=0,
            per_page=20,
            user_id=None
        )
        
        # Assert
        mock_chat_history_service.get_group_sessions.assert_called_once_with(
            page=0,
            per_page=20,
            user_id=None,
            group_context=mock_group_context
        )

    @pytest.mark.asyncio
    async def test_delete_chat_session_success(self, mock_chat_history_service, mock_group_context):
        """Test successful chat session deletion."""
        # Arrange
        session_id = "session-123"
        
        from src.api.chat_history_router import delete_chat_session
        
        # Act
        result = await delete_chat_session(
            session_id=session_id,
            service=mock_chat_history_service,
            group_context=mock_group_context
        )
        
        # Assert
        assert result is None  # 204 No Content
        mock_chat_history_service.delete_session.assert_called_once_with(
            session_id=session_id,
            group_context=mock_group_context
        )

    @pytest.mark.asyncio
    async def test_delete_chat_session_not_found(self, mock_chat_history_service, mock_group_context):
        """Test chat session deletion when session not found."""
        # Arrange
        session_id = "session-123"
        mock_chat_history_service.delete_session.return_value = False
        
        from src.api.chat_history_router import delete_chat_session
        
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await delete_chat_session(
                session_id=session_id,
                service=mock_chat_history_service,
                group_context=mock_group_context
            )
        
        assert exc_info.value.status_code == 404
        assert "Chat session not found" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_delete_chat_session_invalid_group_context(self, mock_chat_history_service, mock_invalid_group_context):
        """Test chat session deletion with invalid group context."""
        # Arrange
        session_id = "session-123"
        
        from src.api.chat_history_router import delete_chat_session
        
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await delete_chat_session(
                session_id=session_id,
                service=mock_chat_history_service,
                group_context=mock_invalid_group_context
            )
        
        assert exc_info.value.status_code == 400
        assert "No valid group context provided" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_create_new_chat_session_success(self, mock_chat_history_service, mock_group_context):
        """Test successful new chat session creation."""
        # Arrange
        from src.api.chat_history_router import create_new_chat_session
        
        # Act
        result = await create_new_chat_session(
            service=mock_chat_history_service,
            group_context=mock_group_context
        )
        
        # Assert
        assert result == {"session_id": "new-session-123"}
        mock_chat_history_service.generate_session_id.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_new_chat_session_invalid_group_context(self, mock_chat_history_service, mock_invalid_group_context):
        """Test new chat session creation with invalid group context."""
        # Arrange
        from src.api.chat_history_router import create_new_chat_session
        
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await create_new_chat_session(
                service=mock_chat_history_service,
                group_context=mock_invalid_group_context
            )
        
        assert exc_info.value.status_code == 400
        assert "No valid group context provided" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_router_error_handling(self, mock_chat_history_service, mock_group_context):
        """Test general error handling in router endpoints."""
        # Arrange
        mock_chat_history_service.get_chat_session.side_effect = Exception("Unexpected error")
        
        from src.api.chat_history_router import get_chat_session_messages
        
        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await get_chat_session_messages(
                session_id="session-123",
                service=mock_chat_history_service,
                group_context=mock_group_context
            )
        
        assert exc_info.value.status_code == 500
        assert "Failed to get chat session messages" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_pagination_parameters(self, mock_chat_history_service, mock_group_context):
        """Test that pagination parameters are properly handled."""
        # Arrange
        session_id = "session-123"
        page = 5
        per_page = 25
        
        from src.api.chat_history_router import get_chat_session_messages
        
        # Act
        result = await get_chat_session_messages(
            session_id=session_id,
            page=page,
            per_page=per_page,
            service=mock_chat_history_service,
            group_context=mock_group_context
        )
        
        # Assert
        assert result.page == page
        assert result.per_page == per_page
        mock_chat_history_service.get_chat_session.assert_called_once_with(
            session_id=session_id,
            page=page,
            per_page=per_page,
            group_context=mock_group_context
        )

    @pytest.mark.asyncio
    async def test_user_id_extraction_from_group_context(self, mock_chat_history_service, mock_group_context):
        """Test that user ID is correctly extracted from group context."""
        # Arrange
        message_request = SaveMessageRequest(
            session_id="session-123",
            message_type="user",
            content="Test message"
        )
        
        from src.api.chat_history_router import save_chat_message
        
        # Act
        await save_chat_message(
            message_request=message_request,
            service=mock_chat_history_service,
            group_context=mock_group_context
        )
        
        # Assert
        call_args = mock_chat_history_service.save_message.call_args
        assert call_args.kwargs['user_id'] == mock_group_context.group_email

    @pytest.mark.asyncio
    async def test_user_id_fallback_when_no_email(self, mock_chat_history_service):
        """Test user ID fallback when group context has no email."""
        # Arrange
        context_no_email = MagicMock()
        context_no_email.group_email = None
        context_no_email.is_valid.return_value = True
        
        message_request = SaveMessageRequest(
            session_id="session-123",
            message_type="user",
            content="Test message"
        )
        
        from src.api.chat_history_router import save_chat_message
        
        # Act
        await save_chat_message(
            message_request=message_request,
            service=mock_chat_history_service,
            group_context=context_no_email
        )
        
        # Assert
        call_args = mock_chat_history_service.save_message.call_args
        assert call_args.kwargs['user_id'] == "unknown_user"