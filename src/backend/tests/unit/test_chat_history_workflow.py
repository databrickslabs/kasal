"""
Unit tests for the chat history workflow.

Tests the business logic of chat history functionality including
message persistence, session management, and group isolation.
"""
import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from src.models.chat_history import ChatHistory
from src.schemas.chat_history import ChatHistoryCreate, ChatHistoryResponse
from src.services.chat_history_service import ChatHistoryService
from src.repositories.chat_history_repository import ChatHistoryRepository
from src.utils.user_context import GroupContext


@pytest.fixture
def mock_session():
    """Mock database session."""
    session = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_repository():
    """Mock chat history repository."""
    repository = AsyncMock(spec=ChatHistoryRepository)
    repository.create = AsyncMock()
    repository.get_by_session_and_group = AsyncMock()
    repository.get_user_sessions = AsyncMock()
    repository.get_sessions_by_group = AsyncMock()
    repository.delete_session = AsyncMock()
    repository.count_messages_by_session = AsyncMock()
    return repository


@pytest.fixture
def group_context():
    """Mock group context."""
    context = MagicMock(spec=GroupContext)
    context.primary_group_id = "group-123"
    context.group_email = "test@company.com"
    context.group_ids = ["group-123"]
    context.is_valid.return_value = True
    return context


@pytest.fixture
def chat_history_service(mock_session, mock_repository):
    """Create chat history service with mocked dependencies."""
    service = ChatHistoryService(session=mock_session)
    service.repository = mock_repository
    return service


@pytest.fixture
def sample_chat_message():
    """Sample chat message data."""
    return {
        "id": "msg-123",
        "session_id": "session-456",
        "user_id": "user@company.com",
        "message_type": "user",
        "content": "Create an agent that can analyze financial data",
        "intent": "generate_agent",
        "confidence": "0.95",
        "generation_result": {
            "agent_name": "Financial Analyst",
            "role": "Data Analyst",
            "tools": ["python", "pandas", "matplotlib"]
        },
        "timestamp": datetime.utcnow(),
        "group_id": "group-123",
        "group_email": "test@company.com"
    }


class TestChatHistoryServiceUnit:
    """Unit tests for ChatHistoryService."""

    @pytest.mark.asyncio
    async def test_save_message_success(self, chat_history_service, mock_repository, group_context, sample_chat_message):
        """Test successful message saving."""
        # Arrange
        expected_message = ChatHistory(**sample_chat_message)
        mock_repository.create.return_value = expected_message

        # Act
        result = await chat_history_service.save_message(
            session_id="session-456",
            user_id="user@company.com",
            message_type="user",
            content="Create an agent that can analyze financial data",
            intent="generate_agent",
            confidence=0.95,
            generation_result={"agent_name": "Financial Analyst"},
            group_context=group_context
        )

        # Assert
        assert result == expected_message
        mock_repository.create.assert_called_once()
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["session_id"] == "session-456"
        assert call_args["user_id"] == "user@company.com"
        assert call_args["message_type"] == "user"
        assert call_args["content"] == "Create an agent that can analyze financial data"
        assert call_args["intent"] == "generate_agent"
        assert call_args["confidence"] == "0.95"
        assert call_args["group_id"] == "group-123"
        assert call_args["group_email"] == "test@company.com"

    @pytest.mark.asyncio
    async def test_save_message_without_group_context(self, chat_history_service, mock_repository):
        """Test saving message without group context."""
        # Arrange
        expected_message = ChatHistory(
            id="msg-123",
            session_id="session-456",
            user_id="user@company.com",
            message_type="user",
            content="Test message",
            timestamp=datetime.utcnow()
        )
        mock_repository.create.return_value = expected_message

        # Act
        result = await chat_history_service.save_message(
            session_id="session-456",
            user_id="user@company.com",
            message_type="user",
            content="Test message"
        )

        # Assert
        assert result == expected_message
        mock_repository.create.assert_called_once()
        call_args = mock_repository.create.call_args[0][0]
        assert "group_id" not in call_args or call_args["group_id"] is None
        assert "group_email" not in call_args or call_args["group_email"] is None

    @pytest.mark.asyncio
    async def test_save_message_with_confidence_none(self, chat_history_service, mock_repository, group_context):
        """Test saving message with confidence as None."""
        # Arrange
        expected_message = ChatHistory(
            id="msg-123",
            session_id="session-456",
            user_id="user@company.com",
            message_type="user",
            content="Test message",
            confidence=None,
            timestamp=datetime.utcnow()
        )
        mock_repository.create.return_value = expected_message

        # Act
        result = await chat_history_service.save_message(
            session_id="session-456",
            user_id="user@company.com",
            message_type="user",
            content="Test message",
            confidence=None,
            group_context=group_context
        )

        # Assert
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["confidence"] is None

    @pytest.mark.asyncio
    async def test_get_chat_session_success(self, chat_history_service, mock_repository, group_context, sample_chat_message):
        """Test successful retrieval of chat session messages."""
        # Arrange
        expected_messages = [ChatHistory(**sample_chat_message)]
        mock_repository.get_by_session_and_group.return_value = expected_messages

        # Act
        result = await chat_history_service.get_chat_session(
            session_id="session-456",
            page=0,
            per_page=50,
            group_context=group_context
        )

        # Assert
        assert result == expected_messages
        mock_repository.get_by_session_and_group.assert_called_once_with(
            session_id="session-456",
            group_ids=["group-123"],
            page=0,
            per_page=50
        )

    @pytest.mark.asyncio
    async def test_get_chat_session_no_group_context(self, chat_history_service, mock_repository):
        """Test chat session retrieval without group context returns empty list."""
        # Act
        result = await chat_history_service.get_chat_session(
            session_id="session-456",
            group_context=None
        )

        # Assert
        assert result == []
        mock_repository.get_by_session_and_group.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_chat_session_invalid_group_context(self, chat_history_service, mock_repository):
        """Test chat session retrieval with invalid group context."""
        # Arrange
        invalid_context = MagicMock()
        invalid_context.group_ids = []

        # Act
        result = await chat_history_service.get_chat_session(
            session_id="session-456",
            group_context=invalid_context
        )

        # Assert
        assert result == []
        mock_repository.get_by_session_and_group.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_user_sessions_success(self, chat_history_service, mock_repository, group_context, sample_chat_message):
        """Test successful retrieval of user sessions."""
        # Arrange
        expected_sessions = [ChatHistory(**sample_chat_message)]
        mock_repository.get_user_sessions.return_value = expected_sessions

        # Act
        result = await chat_history_service.get_user_sessions(
            user_id="user@company.com",
            page=0,
            per_page=20,
            group_context=group_context
        )

        # Assert
        assert result == expected_sessions
        mock_repository.get_user_sessions.assert_called_once_with(
            user_id="user@company.com",
            group_ids=["group-123"],
            page=0,
            per_page=20
        )

    @pytest.mark.asyncio
    async def test_get_user_sessions_no_group_context(self, chat_history_service, mock_repository):
        """Test user sessions retrieval without group context."""
        # Act
        result = await chat_history_service.get_user_sessions(
            user_id="user@company.com"
        )

        # Assert
        assert result == []
        mock_repository.get_user_sessions.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_group_sessions_success(self, chat_history_service, mock_repository, group_context):
        """Test successful retrieval of group sessions."""
        # Arrange
        expected_sessions = [
            {"session_id": "session-1", "user_id": "user1@company.com", "latest_timestamp": datetime.utcnow()},
            {"session_id": "session-2", "user_id": "user2@company.com", "latest_timestamp": datetime.utcnow()}
        ]
        mock_repository.get_sessions_by_group.return_value = expected_sessions

        # Act
        result = await chat_history_service.get_group_sessions(
            page=0,
            per_page=20,
            user_id="user@company.com",
            group_context=group_context
        )

        # Assert
        assert result == expected_sessions
        mock_repository.get_sessions_by_group.assert_called_once_with(
            group_ids=["group-123"],
            user_id="user@company.com",
            page=0,
            per_page=20
        )

    @pytest.mark.asyncio
    async def test_get_group_sessions_no_user_filter(self, chat_history_service, mock_repository, group_context):
        """Test group sessions retrieval without user filter."""
        # Arrange
        expected_sessions = []
        mock_repository.get_sessions_by_group.return_value = expected_sessions

        # Act
        result = await chat_history_service.get_group_sessions(
            page=0,
            per_page=20,
            group_context=group_context
        )

        # Assert
        mock_repository.get_sessions_by_group.assert_called_once_with(
            group_ids=["group-123"],
            user_id=None,
            page=0,
            per_page=20
        )

    @pytest.mark.asyncio
    async def test_delete_session_success(self, chat_history_service, mock_repository, group_context):
        """Test successful session deletion."""
        # Arrange
        mock_repository.delete_session.return_value = True

        # Act
        result = await chat_history_service.delete_session(
            session_id="session-456",
            group_context=group_context
        )

        # Assert
        assert result is True
        mock_repository.delete_session.assert_called_once_with(
            session_id="session-456",
            group_ids=["group-123"]
        )

    @pytest.mark.asyncio
    async def test_delete_session_not_found(self, chat_history_service, mock_repository, group_context):
        """Test session deletion when session not found."""
        # Arrange
        mock_repository.delete_session.return_value = False

        # Act
        result = await chat_history_service.delete_session(
            session_id="nonexistent-session",
            group_context=group_context
        )

        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_session_no_group_context(self, chat_history_service, mock_repository):
        """Test session deletion without group context."""
        # Act
        result = await chat_history_service.delete_session(
            session_id="session-456"
        )

        # Assert
        assert result is False
        mock_repository.delete_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_count_session_messages_success(self, chat_history_service, mock_repository, group_context):
        """Test successful message count."""
        # Arrange
        mock_repository.count_messages_by_session.return_value = 5

        # Act
        result = await chat_history_service.count_session_messages(
            session_id="session-456",
            group_context=group_context
        )

        # Assert
        assert result == 5
        mock_repository.count_messages_by_session.assert_called_once_with(
            session_id="session-456",
            group_ids=["group-123"]
        )

    @pytest.mark.asyncio
    async def test_count_session_messages_no_group_context(self, chat_history_service, mock_repository):
        """Test message count without group context."""
        # Act
        result = await chat_history_service.count_session_messages(
            session_id="session-456"
        )

        # Assert
        assert result == 0
        mock_repository.count_messages_by_session.assert_not_called()

    def test_generate_session_id(self, chat_history_service):
        """Test session ID generation."""
        # Act
        session_id = chat_history_service.generate_session_id()

        # Assert
        assert isinstance(session_id, str)
        assert len(session_id) == 36  # UUID format
        assert session_id.count('-') == 4  # UUID has 4 hyphens

    def test_generate_session_id_uniqueness(self, chat_history_service):
        """Test that generated session IDs are unique."""
        # Act
        session_id1 = chat_history_service.generate_session_id()
        session_id2 = chat_history_service.generate_session_id()

        # Assert
        assert session_id1 != session_id2

    @pytest.mark.asyncio
    async def test_service_factory_method(self, mock_session):
        """Test service factory method."""
        # Act
        service = ChatHistoryService.create(mock_session)

        # Assert
        assert isinstance(service, ChatHistoryService)
        assert service.session == mock_session
        assert isinstance(service.repository, ChatHistoryRepository)

    @pytest.mark.asyncio
    async def test_save_message_repository_exception(self, chat_history_service, mock_repository, group_context):
        """Test handling of repository exceptions during save."""
        # Arrange
        mock_repository.create.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(Exception, match="Database error"):
            await chat_history_service.save_message(
                session_id="session-456",
                user_id="user@company.com",
                message_type="user",
                content="Test message",
                group_context=group_context
            )

    @pytest.mark.asyncio
    async def test_get_chat_session_repository_exception(self, chat_history_service, mock_repository, group_context):
        """Test handling of repository exceptions during retrieval."""
        # Arrange
        mock_repository.get_by_session_and_group.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(Exception, match="Database error"):
            await chat_history_service.get_chat_session(
                session_id="session-456",
                group_context=group_context
            )

    @pytest.mark.asyncio
    async def test_save_message_with_complex_generation_result(self, chat_history_service, mock_repository, group_context):
        """Test saving message with complex generation result."""
        # Arrange
        complex_result = {
            "crew_id": "crew-456",
            "agents": [
                {
                    "id": "agent-1",
                    "name": "Research Agent",
                    "tools": ["web_search", "document_analysis"],
                    "config": {"max_iter": 25, "verbose": True}
                }
            ],
            "tasks": [
                {
                    "id": "task-1",
                    "name": "Research Task",
                    "dependencies": []
                }
            ],
            "metadata": {
                "created_at": "2023-01-01T00:00:00Z",
                "processing_time": 2.5
            }
        }
        
        expected_message = ChatHistory(
            id="msg-123",
            session_id="session-456",
            user_id="user@company.com",
            message_type="assistant",
            content="I've created a complete crew for you",
            generation_result=complex_result,
            timestamp=datetime.utcnow()
        )
        mock_repository.create.return_value = expected_message

        # Act
        result = await chat_history_service.save_message(
            session_id="session-456",
            user_id="user@company.com",
            message_type="assistant",
            content="I've created a complete crew for you",
            generation_result=complex_result,
            group_context=group_context
        )

        # Assert
        assert result == expected_message
        call_args = mock_repository.create.call_args[0][0]
        assert call_args["generation_result"] == complex_result
        assert call_args["generation_result"]["crew_id"] == "crew-456"
        assert len(call_args["generation_result"]["agents"]) == 1
        assert call_args["generation_result"]["metadata"]["processing_time"] == 2.5


class TestChatHistoryWorkflowIntegration:
    """Integration-style unit tests for chat history workflow."""

    @pytest.mark.asyncio
    async def test_complete_message_workflow(self, chat_history_service, mock_repository, group_context):
        """Test complete workflow: save message, retrieve, count, delete."""
        # Arrange
        session_id = "workflow-session-123"
        message_data = {
            "id": "msg-123",
            "session_id": session_id,
            "user_id": "user@company.com",
            "message_type": "user",
            "content": "Test workflow message",
            "timestamp": datetime.utcnow(),
            "group_id": "group-123",
            "group_email": "test@company.com"
        }
        
        saved_message = ChatHistory(**message_data)
        mock_repository.create.return_value = saved_message
        mock_repository.get_by_session_and_group.return_value = [saved_message]
        mock_repository.count_messages_by_session.return_value = 1
        mock_repository.delete_session.return_value = True

        # Act & Assert - Save message
        result = await chat_history_service.save_message(
            session_id=session_id,
            user_id="user@company.com",
            message_type="user",
            content="Test workflow message",
            group_context=group_context
        )
        assert result.session_id == session_id

        # Act & Assert - Retrieve messages
        messages = await chat_history_service.get_chat_session(
            session_id=session_id,
            group_context=group_context
        )
        assert len(messages) == 1
        assert messages[0].content == "Test workflow message"

        # Act & Assert - Count messages
        count = await chat_history_service.count_session_messages(
            session_id=session_id,
            group_context=group_context
        )
        assert count == 1

        # Act & Assert - Delete session
        deleted = await chat_history_service.delete_session(
            session_id=session_id,
            group_context=group_context
        )
        assert deleted is True

    @pytest.mark.asyncio
    async def test_multi_user_session_workflow(self, chat_history_service, mock_repository, group_context):
        """Test workflow with multiple users in same group."""
        # Arrange
        user1_sessions = [
            {"session_id": "session-1", "user_id": "user1@company.com", "latest_timestamp": datetime.utcnow()},
            {"session_id": "session-2", "user_id": "user1@company.com", "latest_timestamp": datetime.utcnow()}
        ]
        user2_sessions = [
            {"session_id": "session-3", "user_id": "user2@company.com", "latest_timestamp": datetime.utcnow()}
        ]
        all_sessions = user1_sessions + user2_sessions

        mock_repository.get_user_sessions.side_effect = [user1_sessions, user2_sessions]
        mock_repository.get_sessions_by_group.return_value = all_sessions

        # Act & Assert - Get user1 sessions
        user1_result = await chat_history_service.get_user_sessions(
            user_id="user1@company.com",
            group_context=group_context
        )
        assert len(user1_result) == 2

        # Act & Assert - Get user2 sessions
        user2_result = await chat_history_service.get_user_sessions(
            user_id="user2@company.com",
            group_context=group_context
        )
        assert len(user2_result) == 1

        # Act & Assert - Get all group sessions
        group_sessions = await chat_history_service.get_group_sessions(
            group_context=group_context
        )
        assert len(group_sessions) == 3

    @pytest.mark.asyncio
    async def test_pagination_workflow(self, chat_history_service, mock_repository, group_context):
        """Test pagination across multiple requests."""
        # Arrange
        session_id = "paginated-session"
        page1_messages = [
            ChatHistory(id=f"msg-{i}", session_id=session_id, user_id="user@company.com", 
                       message_type="user", content=f"Message {i}", timestamp=datetime.utcnow())
            for i in range(10)
        ]
        page2_messages = [
            ChatHistory(id=f"msg-{i}", session_id=session_id, user_id="user@company.com", 
                       message_type="user", content=f"Message {i}", timestamp=datetime.utcnow())
            for i in range(10, 15)
        ]

        def mock_paginated_response(session_id, group_ids, page, per_page):
            if page == 0:
                return page1_messages
            elif page == 1:
                return page2_messages
            return []

        mock_repository.get_by_session_and_group.side_effect = mock_paginated_response

        # Act & Assert - Get first page
        page1_result = await chat_history_service.get_chat_session(
            session_id=session_id,
            page=0,
            per_page=10,
            group_context=group_context
        )
        assert len(page1_result) == 10

        # Act & Assert - Get second page
        page2_result = await chat_history_service.get_chat_session(
            session_id=session_id,
            page=1,
            per_page=10,
            group_context=group_context
        )
        assert len(page2_result) == 5

    @pytest.mark.asyncio
    async def test_group_isolation_workflow(self, chat_history_service, mock_repository):
        """Test that group isolation works properly."""
        # Arrange
        group1_context = MagicMock()
        group1_context.primary_group_id = "group-111"
        group1_context.group_email = "group1@company.com" 
        group1_context.group_ids = ["group-111"]

        group2_context = MagicMock()
        group2_context.primary_group_id = "group-222"
        group2_context.group_email = "group2@company.com"
        group2_context.group_ids = ["group-222"]

        # Mock repository responses for different groups
        def mock_group_response(session_id, group_ids, page=0, per_page=50):
            if "group-111" in group_ids:
                return [ChatHistory(id="msg-1", session_id=session_id, content="Group 1 message", 
                                  user_id="user1@company.com", message_type="user", timestamp=datetime.utcnow())]
            elif "group-222" in group_ids:
                return [ChatHistory(id="msg-2", session_id=session_id, content="Group 2 message",
                                  user_id="user2@company.com", message_type="user", timestamp=datetime.utcnow())]
            return []

        mock_repository.get_by_session_and_group.side_effect = mock_group_response

        # Act & Assert - Group 1 sees only their messages
        group1_messages = await chat_history_service.get_chat_session(
            session_id="shared-session-id",
            group_context=group1_context
        )
        assert len(group1_messages) == 1
        assert group1_messages[0].content == "Group 1 message"

        # Act & Assert - Group 2 sees only their messages
        group2_messages = await chat_history_service.get_chat_session(
            session_id="shared-session-id",
            group_context=group2_context
        )
        assert len(group2_messages) == 1
        assert group2_messages[0].content == "Group 2 message"