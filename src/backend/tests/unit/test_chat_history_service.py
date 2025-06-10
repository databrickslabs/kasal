"""
Unit tests for ChatHistoryService.

Tests the functionality of the chat history service including
business logic, group context handling, and repository interactions.
"""
import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List, Optional

from src.services.chat_history_service import ChatHistoryService
from src.repositories.chat_history_repository import ChatHistoryRepository
from src.models.chat_history import ChatHistory
from src.schemas.chat_history import ChatHistoryCreate
from src.utils.user_context import GroupContext


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    return AsyncMock()


@pytest.fixture
def mock_repository():
    """Create a mock ChatHistoryRepository."""
    return AsyncMock(spec=ChatHistoryRepository)


@pytest.fixture
def mock_group_context():
    """Create a mock GroupContext."""
    context = MagicMock(spec=GroupContext)
    context.group_id = "group-123"
    context.group_email = "admin@company.com"
    context.group_ids = ["group-123", "group-456"]
    return context


@pytest.fixture
def mock_chat_history():
    """Create a mock ChatHistory object."""
    chat = MagicMock(spec=ChatHistory)
    chat.id = "chat-123"
    chat.session_id = "session-456"
    chat.user_id = "user-789"
    chat.message_type = "user"
    chat.content = "Test message"
    chat.timestamp = datetime.now(UTC)
    chat.group_id = "group-123"
    chat.group_email = "admin@company.com"
    return chat


@pytest.fixture
def chat_history_service(mock_session, mock_repository):
    """Create a ChatHistoryService instance with mocked dependencies."""
    service = ChatHistoryService(mock_session)
    service.repository = mock_repository
    return service


class TestChatHistoryService:
    """Test cases for ChatHistoryService."""

    def test_init(self, mock_session):
        """Test service initialization."""
        # Act
        service = ChatHistoryService(mock_session)
        
        # Assert
        assert service.session == mock_session
        assert isinstance(service.repository, ChatHistoryRepository)

    def test_create_factory_method(self, mock_session):
        """Test the create factory method."""
        # Act
        service = ChatHistoryService.create(mock_session)
        
        # Assert
        assert isinstance(service, ChatHistoryService)
        assert service.session == mock_session

    @pytest.mark.asyncio
    async def test_save_message_success(self, chat_history_service, mock_repository, mock_group_context, mock_chat_history):
        """Test successful message saving."""
        # Arrange
        session_id = "session-123"
        user_id = "user-456"
        message_type = "user"
        content = "Test message"
        intent = "generate_agent"
        confidence = 0.95
        generation_result = {"agent_name": "Test Agent"}
        
        mock_repository.create.return_value = mock_chat_history
        
        # Act
        result = await chat_history_service.save_message(
            session_id=session_id,
            user_id=user_id,
            message_type=message_type,
            content=content,
            intent=intent,
            confidence=confidence,
            generation_result=generation_result,
            group_context=mock_group_context
        )
        
        # Assert
        assert result == mock_chat_history
        mock_repository.create.assert_called_once()
        
        # Verify the data passed to repository.create
        call_args = mock_repository.create.call_args[0][0]
        assert call_args['session_id'] == session_id
        assert call_args['user_id'] == user_id
        assert call_args['message_type'] == message_type
        assert call_args['content'] == content
        assert call_args['intent'] == intent
        assert call_args['confidence'] == "0.95"
        assert call_args['generation_result'] == generation_result
        assert call_args['group_id'] == mock_group_context.group_id
        assert call_args['group_email'] == mock_group_context.group_email

    @pytest.mark.asyncio
    async def test_save_message_without_group_context(self, chat_history_service, mock_repository, mock_chat_history):
        """Test saving message without group context."""
        # Arrange
        session_id = "session-123"
        user_id = "user-456"
        message_type = "user"
        content = "Test message"
        
        mock_repository.create.return_value = mock_chat_history
        
        # Act
        result = await chat_history_service.save_message(
            session_id=session_id,
            user_id=user_id,
            message_type=message_type,
            content=content,
            group_context=None
        )
        
        # Assert
        assert result == mock_chat_history
        
        # Verify group fields are not set
        call_args = mock_repository.create.call_args[0][0]
        assert 'group_id' not in call_args
        assert 'group_email' not in call_args

    @pytest.mark.asyncio
    async def test_save_message_with_none_confidence(self, chat_history_service, mock_repository, mock_chat_history):
        """Test saving message with None confidence."""
        # Arrange
        mock_repository.create.return_value = mock_chat_history
        
        # Act
        result = await chat_history_service.save_message(
            session_id="session-123",
            user_id="user-456",
            message_type="user",
            content="Test message",
            confidence=None
        )
        
        # Assert
        call_args = mock_repository.create.call_args[0][0]
        assert call_args['confidence'] is None

    @pytest.mark.asyncio
    async def test_get_chat_session_success(self, chat_history_service, mock_repository, mock_group_context, mock_chat_history):
        """Test successful chat session retrieval."""
        # Arrange
        session_id = "session-123"
        page = 0
        per_page = 50
        
        mock_repository.get_by_session_and_group.return_value = [mock_chat_history]
        
        # Act
        result = await chat_history_service.get_chat_session(
            session_id=session_id,
            page=page,
            per_page=per_page,
            group_context=mock_group_context
        )
        
        # Assert
        assert result == [mock_chat_history]
        mock_repository.get_by_session_and_group.assert_called_once_with(
            session_id=session_id,
            group_ids=mock_group_context.group_ids,
            page=page,
            per_page=per_page
        )

    @pytest.mark.asyncio
    async def test_get_chat_session_no_group_context(self, chat_history_service):
        """Test chat session retrieval without group context."""
        # Act
        result = await chat_history_service.get_chat_session(
            session_id="session-123",
            group_context=None
        )
        
        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_chat_session_empty_group_ids(self, chat_history_service):
        """Test chat session retrieval with empty group IDs."""
        # Arrange
        mock_context = MagicMock()
        mock_context.group_ids = []
        
        # Act
        result = await chat_history_service.get_chat_session(
            session_id="session-123",
            group_context=mock_context
        )
        
        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_user_sessions_success(self, chat_history_service, mock_repository, mock_group_context, mock_chat_history):
        """Test successful user sessions retrieval."""
        # Arrange
        user_id = "user-123"
        page = 0
        per_page = 20
        
        mock_repository.get_user_sessions.return_value = [mock_chat_history]
        
        # Act
        result = await chat_history_service.get_user_sessions(
            user_id=user_id,
            page=page,
            per_page=per_page,
            group_context=mock_group_context
        )
        
        # Assert
        assert result == [mock_chat_history]
        mock_repository.get_user_sessions.assert_called_once_with(
            user_id=user_id,
            group_ids=mock_group_context.group_ids,
            page=page,
            per_page=per_page
        )

    @pytest.mark.asyncio
    async def test_get_user_sessions_no_group_context(self, chat_history_service):
        """Test user sessions retrieval without group context."""
        # Act
        result = await chat_history_service.get_user_sessions(
            user_id="user-123",
            group_context=None
        )
        
        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_group_sessions_success(self, chat_history_service, mock_repository, mock_group_context):
        """Test successful group sessions retrieval."""
        # Arrange
        page = 0
        per_page = 20
        user_id = "user-456"
        
        mock_sessions = [
            {"session_id": "session-1", "user_id": "user-456", "latest_timestamp": datetime.now(UTC)},
            {"session_id": "session-2", "user_id": "user-789", "latest_timestamp": datetime.now(UTC)}
        ]
        mock_repository.get_sessions_by_group.return_value = mock_sessions
        
        # Act
        result = await chat_history_service.get_group_sessions(
            page=page,
            per_page=per_page,
            user_id=user_id,
            group_context=mock_group_context
        )
        
        # Assert
        assert result == mock_sessions
        mock_repository.get_sessions_by_group.assert_called_once_with(
            group_ids=mock_group_context.group_ids,
            user_id=user_id,
            page=page,
            per_page=per_page
        )

    @pytest.mark.asyncio
    async def test_get_group_sessions_no_group_context(self, chat_history_service):
        """Test group sessions retrieval without group context."""
        # Act
        result = await chat_history_service.get_group_sessions(
            group_context=None
        )
        
        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_delete_session_success(self, chat_history_service, mock_repository, mock_group_context):
        """Test successful session deletion."""
        # Arrange
        session_id = "session-123"
        mock_repository.delete_session.return_value = True
        
        # Act
        result = await chat_history_service.delete_session(
            session_id=session_id,
            group_context=mock_group_context
        )
        
        # Assert
        assert result is True
        mock_repository.delete_session.assert_called_once_with(
            session_id=session_id,
            group_ids=mock_group_context.group_ids
        )

    @pytest.mark.asyncio
    async def test_delete_session_not_found(self, chat_history_service, mock_repository, mock_group_context):
        """Test session deletion when session not found."""
        # Arrange
        session_id = "session-123"
        mock_repository.delete_session.return_value = False
        
        # Act
        result = await chat_history_service.delete_session(
            session_id=session_id,
            group_context=mock_group_context
        )
        
        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_session_no_group_context(self, chat_history_service):
        """Test session deletion without group context."""
        # Act
        result = await chat_history_service.delete_session(
            session_id="session-123",
            group_context=None
        )
        
        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_count_session_messages_success(self, chat_history_service, mock_repository, mock_group_context):
        """Test successful message counting."""
        # Arrange
        session_id = "session-123"
        mock_repository.count_messages_by_session.return_value = 25
        
        # Act
        result = await chat_history_service.count_session_messages(
            session_id=session_id,
            group_context=mock_group_context
        )
        
        # Assert
        assert result == 25
        mock_repository.count_messages_by_session.assert_called_once_with(
            session_id=session_id,
            group_ids=mock_group_context.group_ids
        )

    @pytest.mark.asyncio
    async def test_count_session_messages_no_group_context(self, chat_history_service):
        """Test message counting without group context."""
        # Act
        result = await chat_history_service.count_session_messages(
            session_id="session-123",
            group_context=None
        )
        
        # Assert
        assert result == 0

    def test_generate_session_id(self, chat_history_service):
        """Test session ID generation."""
        # Act
        session_id1 = chat_history_service.generate_session_id()
        session_id2 = chat_history_service.generate_session_id()
        
        # Assert
        assert session_id1 is not None
        assert session_id2 is not None
        assert session_id1 != session_id2
        assert isinstance(session_id1, str)
        assert isinstance(session_id2, str)
        assert len(session_id1) == 36  # Standard UUID length
        assert len(session_id2) == 36

    @pytest.mark.asyncio
    async def test_save_message_with_legacy_fields(self, chat_history_service, mock_repository, mock_group_context, mock_chat_history):
        """Test that legacy tenant fields are set for backward compatibility."""
        # Arrange
        mock_repository.create.return_value = mock_chat_history
        
        # Act
        await chat_history_service.save_message(
            session_id="session-123",
            user_id="user-456",
            message_type="user",
            content="Test message",
            group_context=mock_group_context
        )
        
        # Assert
        call_args = mock_repository.create.call_args[0][0]
        assert call_args['tenant_id'] == mock_group_context.group_id
        assert call_args['tenant_email'] == mock_group_context.group_email

    @pytest.mark.asyncio
    async def test_save_message_timestamp_auto_generated(self, chat_history_service, mock_repository, mock_chat_history):
        """Test that timestamp is automatically generated."""
        # Arrange
        mock_repository.create.return_value = mock_chat_history
        
        # Act
        await chat_history_service.save_message(
            session_id="session-123",
            user_id="user-456",
            message_type="user",
            content="Test message"
        )
        
        # Assert
        call_args = mock_repository.create.call_args[0][0]
        assert 'timestamp' in call_args
        assert isinstance(call_args['timestamp'], datetime)

    @pytest.mark.asyncio
    async def test_service_with_default_pagination(self, chat_history_service, mock_repository, mock_group_context):
        """Test service methods with default pagination values."""
        # Arrange
        mock_repository.get_by_session_and_group.return_value = []
        mock_repository.get_user_sessions.return_value = []
        mock_repository.get_sessions_by_group.return_value = []
        
        # Act
        await chat_history_service.get_chat_session("session-123", group_context=mock_group_context)
        await chat_history_service.get_user_sessions("user-123", group_context=mock_group_context)
        await chat_history_service.get_group_sessions(group_context=mock_group_context)
        
        # Assert - verify default pagination values were used
        mock_repository.get_by_session_and_group.assert_called_with(
            session_id="session-123",
            group_ids=mock_group_context.group_ids,
            page=0,
            per_page=50
        )
        mock_repository.get_user_sessions.assert_called_with(
            user_id="user-123",
            group_ids=mock_group_context.group_ids,
            page=0,
            per_page=20
        )
        mock_repository.get_sessions_by_group.assert_called_with(
            group_ids=mock_group_context.group_ids,
            user_id=None,
            page=0,
            per_page=20
        )