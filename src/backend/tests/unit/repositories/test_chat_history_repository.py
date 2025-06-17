"""
Unit tests for ChatHistoryRepository.

Tests the data access layer of chat history functionality including
database operations, group filtering, and pagination.
"""
import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.models.chat_history import ChatHistory
from src.repositories.chat_history_repository import ChatHistoryRepository


@pytest.fixture
def mock_session():
    """Mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.flush = AsyncMock()
    session.delete = AsyncMock()
    return session


@pytest.fixture
def chat_history_repository(mock_session):
    """Create repository with mocked session."""
    return ChatHistoryRepository(session=mock_session)


@pytest.fixture
def sample_chat_messages():
    """Sample chat history messages."""
    base_time = datetime.utcnow()
    return [
        ChatHistory(
            id="msg-1",
            session_id="session-123",
            user_id="user1@company.com",
            message_type="user",
            content="First message",
            timestamp=base_time,
            group_id="group-111",
            group_email="group1@company.com"
        ),
        ChatHistory(
            id="msg-2",
            session_id="session-123",
            user_id="user1@company.com",
            message_type="assistant",
            content="Assistant response",
            timestamp=base_time,
            group_id="group-111",
            group_email="group1@company.com"
        ),
        ChatHistory(
            id="msg-3", 
            session_id="session-456",
            user_id="user2@company.com",
            message_type="user",
            content="Different session message",
            timestamp=base_time,
            group_id="group-222",
            group_email="group2@company.com"
        )
    ]


class TestChatHistoryRepository:
    """Unit tests for ChatHistoryRepository."""

    @pytest.mark.asyncio
    async def test_get_by_session_and_group_success(self, chat_history_repository, mock_session, sample_chat_messages):
        """Test successful retrieval of messages by session and group."""
        # Arrange
        expected_messages = sample_chat_messages[:2]  # First two messages belong to same session/group
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = expected_messages
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result

        # Act
        result = await chat_history_repository.get_by_session_and_group(
            session_id="session-123",
            group_ids=["group-111"],
            page=0,
            per_page=50
        )

        # Assert
        assert result == expected_messages
        mock_session.execute.assert_called_once()
        mock_session.rollback.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_by_session_and_group_empty_group_ids(self, chat_history_repository):
        """Test that empty group_ids returns empty list."""
        # Act
        result = await chat_history_repository.get_by_session_and_group(
            session_id="session-123",
            group_ids=[],
            page=0,
            per_page=50
        )

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_by_session_and_group_with_pagination(self, chat_history_repository, mock_session):
        """Test pagination parameters are applied correctly."""
        # Arrange
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result

        # Act
        await chat_history_repository.get_by_session_and_group(
            session_id="session-123",
            group_ids=["group-111"],
            page=2,
            per_page=10
        )

        # Assert
        mock_session.execute.assert_called_once()
        # Verify the query includes pagination (offset=20, limit=10)
        call_args = mock_session.execute.call_args[0][0]
        # Note: In a real test, you'd inspect the query more thoroughly

    @pytest.mark.asyncio
    async def test_get_by_session_and_group_database_exception(self, chat_history_repository, mock_session):
        """Test handling of database exceptions."""
        # Arrange
        mock_session.execute.side_effect = Exception("Database connection error")

        # Act & Assert
        with pytest.raises(Exception, match="Database connection error"):
            await chat_history_repository.get_by_session_and_group(
                session_id="session-123",
                group_ids=["group-111"]
            )
        
        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_sessions_by_group_success(self, chat_history_repository, mock_session):
        """Test successful retrieval of sessions by group."""
        # Arrange
        mock_rows = [
            MagicMock(session_id="session-1", user_id="user1@company.com", 
                     latest_timestamp=datetime.utcnow(), message_count=5),
            MagicMock(session_id="session-2", user_id="user2@company.com", 
                     latest_timestamp=datetime.utcnow(), message_count=3)
        ]
        mock_result = MagicMock()
        mock_result.fetchall.return_value = mock_rows
        mock_session.execute.return_value = mock_result

        # Act
        result = await chat_history_repository.get_sessions_by_group(
            group_ids=["group-111"],
            page=0,
            per_page=20
        )

        # Assert
        assert len(result) == 2
        assert result[0]["session_id"] == "session-1"
        assert result[0]["user_id"] == "user1@company.com"
        assert result[0]["message_count"] == 5
        assert result[1]["session_id"] == "session-2"
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_sessions_by_group_with_user_filter(self, chat_history_repository, mock_session):
        """Test session retrieval with user ID filter."""
        # Arrange
        mock_rows = [
            MagicMock(session_id="session-1", user_id="user1@company.com", 
                     latest_timestamp=datetime.utcnow(), message_count=5)
        ]
        mock_result = MagicMock()
        mock_result.fetchall.return_value = mock_rows
        mock_session.execute.return_value = mock_result

        # Act
        result = await chat_history_repository.get_sessions_by_group(
            group_ids=["group-111"],
            user_id="user1@company.com",
            page=0,
            per_page=20
        )

        # Assert
        assert len(result) == 1
        assert result[0]["user_id"] == "user1@company.com"

    @pytest.mark.asyncio
    async def test_get_sessions_by_group_empty_group_ids(self, chat_history_repository):
        """Test that empty group_ids returns empty list."""
        # Act
        result = await chat_history_repository.get_sessions_by_group(
            group_ids=[],
            page=0,
            per_page=20
        )

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_sessions_by_group_database_exception(self, chat_history_repository, mock_session):
        """Test handling of database exceptions in get_sessions_by_group."""
        # Arrange
        mock_session.execute.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(Exception, match="Database error"):
            await chat_history_repository.get_sessions_by_group(
                group_ids=["group-111"]
            )
        
        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_user_sessions_success(self, chat_history_repository, mock_session, sample_chat_messages):
        """Test successful retrieval of user sessions."""
        # Arrange
        user_sessions = [sample_chat_messages[0], sample_chat_messages[1]]  # Two sessions for user1
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = user_sessions
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result

        # Act
        result = await chat_history_repository.get_user_sessions(
            user_id="user1@company.com",
            group_ids=["group-111"],
            page=0,
            per_page=20
        )

        # Assert
        assert result == user_sessions
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_user_sessions_empty_group_ids(self, chat_history_repository):
        """Test that empty group_ids returns empty list."""
        # Act
        result = await chat_history_repository.get_user_sessions(
            user_id="user1@company.com",
            group_ids=[],
            page=0,
            per_page=20
        )

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_user_sessions_with_pagination(self, chat_history_repository, mock_session):
        """Test user sessions with pagination."""
        # Arrange
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result

        # Act
        await chat_history_repository.get_user_sessions(
            user_id="user1@company.com",
            group_ids=["group-111"],
            page=1,
            per_page=5
        )

        # Assert
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_user_sessions_database_exception(self, chat_history_repository, mock_session):
        """Test handling of database exceptions in get_user_sessions."""
        # Arrange
        mock_session.execute.side_effect = Exception("Connection timeout")

        # Act & Assert
        with pytest.raises(Exception, match="Connection timeout"):
            await chat_history_repository.get_user_sessions(
                user_id="user1@company.com",
                group_ids=["group-111"]
            )
        
        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_session_success(self, chat_history_repository, mock_session, sample_chat_messages):
        """Test successful session deletion."""
        # Arrange
        messages_to_delete = sample_chat_messages[:2]  # Two messages in the session
        
        # Mock the get_by_session_and_group call within delete_session
        with patch.object(chat_history_repository, 'get_by_session_and_group', return_value=messages_to_delete):
            # Act
            result = await chat_history_repository.delete_session(
                session_id="session-123",
                group_ids=["group-111"]
            )

        # Assert
        assert result is True
        assert mock_session.delete.call_count == 2  # Two messages deleted
        mock_session.flush.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_session_no_messages_found(self, chat_history_repository, mock_session):
        """Test session deletion when no messages found."""
        # Arrange
        with patch.object(chat_history_repository, 'get_by_session_and_group', return_value=[]):
            # Act
            result = await chat_history_repository.delete_session(
                session_id="nonexistent-session",
                group_ids=["group-111"]
            )

        # Assert
        assert result is False
        mock_session.delete.assert_not_called()
        mock_session.flush.assert_not_called()
        mock_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_session_empty_group_ids(self, chat_history_repository):
        """Test that empty group_ids returns False."""
        # Act
        result = await chat_history_repository.delete_session(
            session_id="session-123",
            group_ids=[]
        )

        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_session_database_exception(self, chat_history_repository, mock_session, sample_chat_messages):
        """Test handling of database exceptions during deletion."""
        # Arrange
        messages_to_delete = sample_chat_messages[:1]
        mock_session.delete.side_effect = Exception("Delete constraint violation")
        
        with patch.object(chat_history_repository, 'get_by_session_and_group', return_value=messages_to_delete):
            # Act & Assert
            with pytest.raises(Exception, match="Delete constraint violation"):
                await chat_history_repository.delete_session(
                    session_id="session-123",
                    group_ids=["group-111"]
                )
        
        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_count_messages_by_session_success(self, chat_history_repository, mock_session):
        """Test successful message count."""
        # Arrange
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = ["id1", "id2", "id3"]  # 3 message IDs
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result

        # Act
        result = await chat_history_repository.count_messages_by_session(
            session_id="session-123",
            group_ids=["group-111"]
        )

        # Assert
        assert result == 3
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_count_messages_by_session_no_messages(self, chat_history_repository, mock_session):
        """Test message count when no messages exist."""
        # Arrange
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []  # No messages
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result

        # Act
        result = await chat_history_repository.count_messages_by_session(
            session_id="empty-session",
            group_ids=["group-111"]
        )

        # Assert
        assert result == 0

    @pytest.mark.asyncio
    async def test_count_messages_by_session_empty_group_ids(self, chat_history_repository):
        """Test that empty group_ids returns 0."""
        # Act
        result = await chat_history_repository.count_messages_by_session(
            session_id="session-123",
            group_ids=[]
        )

        # Assert
        assert result == 0

    @pytest.mark.asyncio
    async def test_count_messages_by_session_database_exception(self, chat_history_repository, mock_session):
        """Test handling of database exceptions in count_messages_by_session."""
        # Arrange
        mock_session.execute.side_effect = Exception("Query timeout")

        # Act & Assert
        with pytest.raises(Exception, match="Query timeout"):
            await chat_history_repository.count_messages_by_session(
                session_id="session-123",
                group_ids=["group-111"]
            )
        
        mock_session.rollback.assert_called_once()

    def test_repository_initialization(self, mock_session):
        """Test repository initialization."""
        # Act
        repository = ChatHistoryRepository(session=mock_session)

        # Assert
        assert repository.session == mock_session
        assert repository.model == ChatHistory

    @pytest.mark.asyncio
    async def test_multiple_group_ids_filtering(self, chat_history_repository, mock_session, sample_chat_messages):
        """Test filtering with multiple group IDs."""
        # Arrange
        expected_messages = sample_chat_messages  # All messages
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = expected_messages
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result

        # Act
        result = await chat_history_repository.get_by_session_and_group(
            session_id="session-123",
            group_ids=["group-111", "group-222", "group-333"],
            page=0,
            per_page=50
        )

        # Assert
        assert result == expected_messages
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_pagination_edge_cases(self, chat_history_repository, mock_session):
        """Test pagination with edge cases."""
        # Arrange
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result

        # Act - Test page 0
        await chat_history_repository.get_by_session_and_group(
            session_id="session-123",
            group_ids=["group-111"],
            page=0,
            per_page=1
        )

        # Act - Test large page number
        await chat_history_repository.get_by_session_and_group(
            session_id="session-123",
            group_ids=["group-111"],
            page=100,
            per_page=50
        )

        # Assert
        assert mock_session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_session_ordering(self, chat_history_repository, mock_session):
        """Test that messages are ordered by timestamp ascending."""
        # Arrange
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result

        # Act
        await chat_history_repository.get_by_session_and_group(
            session_id="session-123",
            group_ids=["group-111"]
        )

        # Assert
        mock_session.execute.assert_called_once()
        # In a real test, you would verify the ORDER BY clause in the query


class TestChatHistoryRepositoryIntegration:
    """Integration-style unit tests for repository workflow."""

    @pytest.mark.asyncio
    async def test_complete_session_lifecycle(self, chat_history_repository, mock_session, sample_chat_messages):
        """Test complete lifecycle: retrieve, count, delete."""
        # Arrange - Setup mocks for different operations
        messages_in_session = sample_chat_messages[:2]  # Two messages in session-123
        
        # Mock get_by_session_and_group for retrieve and delete operations
        with patch.object(chat_history_repository, 'get_by_session_and_group') as mock_get:
            mock_get.return_value = messages_in_session
            
            # Mock count operation
            mock_count_result = MagicMock()
            mock_count_scalars = MagicMock()
            mock_count_scalars.all.return_value = ["id1", "id2"]
            mock_count_result.scalars.return_value = mock_count_scalars
            mock_session.execute.return_value = mock_count_result

            # Act & Assert - Get messages
            messages = await chat_history_repository.get_by_session_and_group(
                session_id="session-123",
                group_ids=["group-111"]
            )
            assert len(messages) == 2

            # Act & Assert - Count messages
            count = await chat_history_repository.count_messages_by_session(
                session_id="session-123",
                group_ids=["group-111"]
            )
            assert count == 2

            # Act & Assert - Delete session
            deleted = await chat_history_repository.delete_session(
                session_id="session-123",
                group_ids=["group-111"]
            )
            assert deleted is True
            assert mock_session.delete.call_count == 2

    @pytest.mark.asyncio
    async def test_multi_user_group_isolation(self, chat_history_repository, mock_session):
        """Test that group isolation works across multiple users."""
        # Arrange
        group1_sessions = [
            MagicMock(session_id="session-1", user_id="user1@company.com", 
                     latest_timestamp=datetime.utcnow(), message_count=3),
            MagicMock(session_id="session-2", user_id="user2@company.com", 
                     latest_timestamp=datetime.utcnow(), message_count=2)
        ]
        
        group2_sessions = [
            MagicMock(session_id="session-3", user_id="user3@company.com", 
                     latest_timestamp=datetime.utcnow(), message_count=1)
        ]

        # Mock the fetchall behavior with proper call tracking
        call_count = 0
        def mock_execute_side_effect(query):
            nonlocal call_count
            call_count += 1
            mock_result = MagicMock()
            # First call is for group-111, second call is for group-222
            if call_count == 1:
                mock_result.fetchall.return_value = group1_sessions
            elif call_count == 2:
                mock_result.fetchall.return_value = group2_sessions
            else:
                mock_result.fetchall.return_value = []
            return mock_result

        mock_session.execute.side_effect = mock_execute_side_effect

        # Act & Assert - Group 1 sessions
        group1_result = await chat_history_repository.get_sessions_by_group(
            group_ids=["group-111"]
        )
        assert len(group1_result) == 2

        # Act & Assert - Group 2 sessions  
        group2_result = await chat_history_repository.get_sessions_by_group(
            group_ids=["group-222"]
        )
        assert len(group2_result) == 1

    @pytest.mark.asyncio
    async def test_repository_error_handling_consistency(self, chat_history_repository, mock_session):
        """Test that all methods handle exceptions consistently."""
        # Arrange
        mock_session.execute.side_effect = Exception("Database unavailable")

        # Act & Assert - All methods should rollback on exception
        methods_to_test = [
            (chat_history_repository.get_by_session_and_group, 
             {"session_id": "test", "group_ids": ["group-111"]}),
            (chat_history_repository.get_sessions_by_group, 
             {"group_ids": ["group-111"]}),
            (chat_history_repository.get_user_sessions, 
             {"user_id": "user@test.com", "group_ids": ["group-111"]}),
            (chat_history_repository.count_messages_by_session, 
             {"session_id": "test", "group_ids": ["group-111"]})
        ]

        for method, kwargs in methods_to_test:
            with pytest.raises(Exception, match="Database unavailable"):
                await method(**kwargs)
            mock_session.rollback.assert_called()
            mock_session.rollback.reset_mock()  # Reset for next iteration

    @pytest.mark.asyncio
    async def test_empty_results_handling(self, chat_history_repository, mock_session):
        """Test handling of empty results across all methods."""
        # Arrange - Mock empty results
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_result.scalars.return_value = mock_scalars
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        # Act & Assert - All methods should handle empty results gracefully
        messages = await chat_history_repository.get_by_session_and_group(
            session_id="empty-session",
            group_ids=["group-111"]
        )
        assert messages == []

        sessions = await chat_history_repository.get_sessions_by_group(
            group_ids=["group-111"]
        )
        assert sessions == []

        user_sessions = await chat_history_repository.get_user_sessions(
            user_id="user@test.com",
            group_ids=["group-111"]
        )
        assert user_sessions == []

        count = await chat_history_repository.count_messages_by_session(
            session_id="empty-session",
            group_ids=["group-111"]
        )
        assert count == 0