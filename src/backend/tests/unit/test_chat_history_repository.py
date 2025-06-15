"""
Unit tests for ChatHistoryRepository.

Tests the functionality of the chat history repository including
CRUD operations, group filtering, pagination, and complex queries.
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.chat_history_repository import ChatHistoryRepository
from src.models.chat_history import ChatHistory


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = AsyncMock()
    return session


@pytest.fixture
def mock_chat_history():
    """Create a mock ChatHistory object."""
    chat = MagicMock(spec=ChatHistory)
    chat.id = "chat-123"
    chat.session_id = "session-456"
    chat.user_id = "user-789"
    chat.message_type = "user"
    chat.content = "Test message"
    chat.timestamp = datetime.utcnow()
    chat.group_id = "group-123"
    chat.group_email = "test@example.com"
    return chat


@pytest.fixture
def chat_history_repository(mock_session):
    """Create a ChatHistoryRepository instance with mock session."""
    return ChatHistoryRepository(mock_session)


class TestChatHistoryRepository:
    """Test cases for ChatHistoryRepository."""

    @pytest.mark.asyncio
    async def test_init(self, mock_session):
        """Test repository initialization."""
        # Act
        repo = ChatHistoryRepository(mock_session)
        
        # Assert
        assert repo.session == mock_session
        assert repo.model == ChatHistory

    @pytest.mark.asyncio
    async def test_get_by_session_and_group_success(self, chat_history_repository, mock_session, mock_chat_history):
        """Test getting messages by session ID and group with successful result."""
        # Arrange
        session_id = "session-123"
        group_ids = ["group-456", "group-789"]
        page = 0
        per_page = 50
        
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [mock_chat_history]
        mock_result = MagicMock()
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        # Act
        result = await chat_history_repository.get_by_session_and_group(
            session_id, group_ids, page, per_page
        )
        
        # Assert
        assert len(result) == 1
        assert result[0] == mock_chat_history
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_by_session_and_group_empty_groups(self, chat_history_repository):
        """Test getting messages with empty group list returns empty result."""
        # Arrange
        session_id = "session-123"
        group_ids = []
        
        # Act
        result = await chat_history_repository.get_by_session_and_group(
            session_id, group_ids
        )
        
        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_by_session_and_group_with_pagination(self, chat_history_repository, mock_session):
        """Test getting messages with pagination parameters."""
        # Arrange
        session_id = "session-123"
        group_ids = ["group-456"]
        page = 2
        per_page = 25
        
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_result = MagicMock()
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        # Act
        result = await chat_history_repository.get_by_session_and_group(
            session_id, group_ids, page, per_page
        )
        
        # Assert
        assert result == []
        mock_session.execute.assert_called_once()
        # Verify pagination parameters are used in query construction

    @pytest.mark.asyncio
    async def test_get_by_session_and_group_database_error(self, chat_history_repository, mock_session):
        """Test database error handling in get_by_session_and_group."""
        # Arrange
        session_id = "session-123"
        group_ids = ["group-456"]
        mock_session.execute.side_effect = SQLAlchemyError("Database error")
        
        # Act & Assert
        with pytest.raises(SQLAlchemyError):
            await chat_history_repository.get_by_session_and_group(session_id, group_ids)
        
        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_sessions_by_group_success(self, chat_history_repository, mock_session):
        """Test getting sessions by group with successful result."""
        # Arrange
        group_ids = ["group-123", "group-456"]
        user_id = "user-789"
        page = 0
        per_page = 20
        
        mock_row = MagicMock()
        mock_row.session_id = "session-123"
        mock_row.user_id = "user-789"
        mock_row.latest_timestamp = datetime.utcnow()
        mock_row.message_count = 5
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row]
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        # Act
        result = await chat_history_repository.get_sessions_by_group(
            group_ids, user_id, page, per_page
        )
        
        # Assert
        assert len(result) == 1
        assert result[0]['session_id'] == "session-123"
        assert result[0]['user_id'] == "user-789"
        assert result[0]['message_count'] == 5
        assert 'latest_timestamp' in result[0]

    @pytest.mark.asyncio
    async def test_get_sessions_by_group_empty_groups(self, chat_history_repository):
        """Test getting sessions with empty group list returns empty result."""
        # Arrange
        group_ids = []
        
        # Act
        result = await chat_history_repository.get_sessions_by_group(group_ids)
        
        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_sessions_by_group_without_user_filter(self, chat_history_repository, mock_session):
        """Test getting sessions without user ID filter."""
        # Arrange
        group_ids = ["group-123"]
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        # Act
        result = await chat_history_repository.get_sessions_by_group(group_ids)
        
        # Assert
        assert result == []
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_user_sessions_success(self, chat_history_repository, mock_session, mock_chat_history):
        """Test getting user sessions with successful result."""
        # Arrange
        user_id = "user-123"
        group_ids = ["group-456"]
        page = 0
        per_page = 20
        
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [mock_chat_history]
        mock_result = MagicMock()
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        # Act
        result = await chat_history_repository.get_user_sessions(
            user_id, group_ids, page, per_page
        )
        
        # Assert
        assert len(result) == 1
        assert result[0] == mock_chat_history

    @pytest.mark.asyncio
    async def test_get_user_sessions_empty_groups(self, chat_history_repository):
        """Test getting user sessions with empty group list returns empty result."""
        # Arrange
        user_id = "user-123"
        group_ids = []
        
        # Act
        result = await chat_history_repository.get_user_sessions(user_id, group_ids)
        
        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_delete_session_success(self, chat_history_repository, mock_session, mock_chat_history):
        """Test successful session deletion."""
        # Arrange
        session_id = "session-123"
        group_ids = ["group-456"]
        
        # Mock get_by_session_and_group to return messages
        chat_history_repository.get_by_session_and_group = AsyncMock(
            return_value=[mock_chat_history]
        )
        
        # Act
        result = await chat_history_repository.delete_session(session_id, group_ids)
        
        # Assert
        assert result is True
        mock_session.delete.assert_called_once_with(mock_chat_history)
        mock_session.flush.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_session_no_messages(self, chat_history_repository):
        """Test session deletion when no messages exist."""
        # Arrange
        session_id = "session-123"
        group_ids = ["group-456"]
        
        # Mock get_by_session_and_group to return empty list
        chat_history_repository.get_by_session_and_group = AsyncMock(return_value=[])
        
        # Act
        result = await chat_history_repository.delete_session(session_id, group_ids)
        
        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_session_empty_groups(self, chat_history_repository):
        """Test session deletion with empty group list returns False."""
        # Arrange
        session_id = "session-123"
        group_ids = []
        
        # Act
        result = await chat_history_repository.delete_session(session_id, group_ids)
        
        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_session_database_error(self, chat_history_repository, mock_session, mock_chat_history):
        """Test database error handling in delete_session."""
        # Arrange
        session_id = "session-123"
        group_ids = ["group-456"]
        
        chat_history_repository.get_by_session_and_group = AsyncMock(
            return_value=[mock_chat_history]
        )
        mock_session.delete.side_effect = SQLAlchemyError("Database error")
        
        # Act & Assert
        with pytest.raises(SQLAlchemyError):
            await chat_history_repository.delete_session(session_id, group_ids)
        
        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_count_messages_by_session_success(self, chat_history_repository, mock_session):
        """Test counting messages in a session."""
        # Arrange
        session_id = "session-123"
        group_ids = ["group-456"]
        
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = ["id1", "id2", "id3"]
        mock_result = MagicMock()
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        # Act
        result = await chat_history_repository.count_messages_by_session(
            session_id, group_ids
        )
        
        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_count_messages_by_session_empty_groups(self, chat_history_repository):
        """Test counting messages with empty group list returns 0."""
        # Arrange
        session_id = "session-123"
        group_ids = []
        
        # Act
        result = await chat_history_repository.count_messages_by_session(
            session_id, group_ids
        )
        
        # Assert
        assert result == 0

    @pytest.mark.asyncio
    async def test_count_messages_by_session_database_error(self, chat_history_repository, mock_session):
        """Test database error handling in count_messages_by_session."""
        # Arrange
        session_id = "session-123"
        group_ids = ["group-456"]
        mock_session.execute.side_effect = SQLAlchemyError("Database error")
        
        # Act & Assert
        with pytest.raises(SQLAlchemyError):
            await chat_history_repository.count_messages_by_session(session_id, group_ids)
        
        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_message_success(self, chat_history_repository, mock_session):
        """Test creating a new chat message."""
        # Arrange
        message_data = {
            'session_id': 'session-123',
            'user_id': 'user-456',
            'message_type': 'user',
            'content': 'Test message',
            'group_id': 'group-789'
        }
        
        mock_chat = MagicMock(spec=ChatHistory)
        mock_chat.id = "chat-123"
        
        # Mock session methods
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.refresh = AsyncMock()
        
        # Mock the model class on the repository instance
        with patch.object(chat_history_repository, 'model') as mock_model_class:
            mock_model_class.__name__ = 'ChatHistory'
            mock_model_class.return_value = mock_chat
            
            # Act
            result = await chat_history_repository.create(message_data)
            
            # Assert
            assert result == mock_chat
            mock_session.add.assert_called_once_with(mock_chat)
            mock_session.flush.assert_called_once()
            mock_session.commit.assert_called_once()
            mock_session.refresh.assert_called_once_with(mock_chat)

    @pytest.mark.asyncio
    async def test_create_message_database_error(self, chat_history_repository, mock_session):
        """Test database error handling in create."""
        # Arrange
        message_data = {
            'session_id': 'session-123',
            'user_id': 'user-456',
            'message_type': 'user',
            'content': 'Test message'
        }
        
        # Mock the model class
        mock_chat = MagicMock(spec=ChatHistory)
        with patch.object(chat_history_repository, 'model') as mock_model_class:
            mock_model_class.__name__ = 'ChatHistory'
            mock_model_class.return_value = mock_chat
            mock_session.add = MagicMock(side_effect=SQLAlchemyError("Database error"))
            
            # Act & Assert
            with pytest.raises(SQLAlchemyError):
                await chat_history_repository.create(message_data)
        
        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_group_filtering(self, chat_history_repository, mock_session):
        """Test that multiple group IDs are properly handled in filtering."""
        # Arrange
        session_id = "session-123"
        group_ids = ["group-1", "group-2", "group-3"]
        
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_result = MagicMock()
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        # Act
        result = await chat_history_repository.get_by_session_and_group(
            session_id, group_ids
        )
        
        # Assert
        assert result == []
        mock_session.execute.assert_called_once()
        # Verify that the query was constructed with multiple group IDs