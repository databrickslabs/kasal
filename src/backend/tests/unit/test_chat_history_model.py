"""
Unit tests for ChatHistory model.

Tests the functionality of the ChatHistory database model including
field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime
from unittest.mock import MagicMock

from src.models.chat_history import ChatHistory, generate_uuid


class TestChatHistory:
    """Test cases for ChatHistory model."""

    def test_chat_history_creation(self):
        """Test basic ChatHistory model creation."""
        # Arrange
        session_id = "test-session-123"
        user_id = "user-456"
        message_type = "user"
        content = "Hello, this is a test message"
        timestamp = datetime.utcnow()
        group_id = "group-789"
        group_email = "test@example.com"
        
        # Act
        chat_history = ChatHistory(
            session_id=session_id,
            user_id=user_id,
            message_type=message_type,
            content=content,
            timestamp=timestamp,
            group_id=group_id,
            group_email=group_email
        )
        
        # Assert
        assert chat_history.session_id == session_id
        assert chat_history.user_id == user_id
        assert chat_history.message_type == message_type
        assert chat_history.content == content
        assert chat_history.timestamp == timestamp
        assert chat_history.group_id == group_id
        assert chat_history.group_email == group_email
        assert chat_history.intent is None
        assert chat_history.confidence is None
        assert chat_history.generation_result is None

    def test_chat_history_with_optional_fields(self):
        """Test ChatHistory model creation with optional fields."""
        # Arrange
        session_id = "test-session-123"
        user_id = "user-456"
        message_type = "assistant"
        content = "I've generated an agent for you"
        intent = "generate_agent"
        confidence = "0.95"
        generation_result = {"agent_name": "Research Agent", "tools": ["web_search"]}
        
        # Act
        chat_history = ChatHistory(
            session_id=session_id,
            user_id=user_id,
            message_type=message_type,
            content=content,
            intent=intent,
            confidence=confidence,
            generation_result=generation_result
        )
        
        # Assert
        assert chat_history.intent == intent
        assert chat_history.confidence == confidence
        assert chat_history.generation_result == generation_result
        assert chat_history.generation_result["agent_name"] == "Research Agent"

    def test_chat_history_defaults(self):
        """Test ChatHistory model with default values."""
        # Arrange & Act
        chat_history = ChatHistory(
            session_id="test-session",
            user_id="test-user",
            message_type="user",
            content="Test message"
        )
        
        # Assert
        # Note: id and timestamp are only generated when saved to database
        assert chat_history.intent is None
        assert chat_history.confidence is None
        assert chat_history.generation_result is None
        assert chat_history.group_id is None
        assert chat_history.group_email is None

    def test_chat_history_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert ChatHistory.__tablename__ == "chat_history"

    def test_chat_history_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        indexes = ChatHistory.__table_args__
        
        # Assert
        assert len(indexes) == 3
        
        # Check index names
        index_names = [index.name for index in indexes if hasattr(index, 'name')]
        expected_indexes = [
            'idx_chat_history_session_timestamp',
            'idx_chat_history_user_timestamp', 
            'idx_chat_history_group_timestamp'
        ]
        
        for expected_index in expected_indexes:
            assert expected_index in index_names

    def test_generate_uuid_function(self):
        """Test the generate_uuid function."""
        # Act
        uuid1 = generate_uuid()
        uuid2 = generate_uuid()
        
        # Assert
        assert uuid1 is not None
        assert uuid2 is not None
        assert uuid1 != uuid2
        assert isinstance(uuid1, str)
        assert isinstance(uuid2, str)
        assert len(uuid1) == 36  # Standard UUID length
        assert len(uuid2) == 36

    def test_chat_history_repr(self):
        """Test string representation of ChatHistory model."""
        # Arrange
        chat_history = ChatHistory(
            session_id="test-session",
            user_id="test-user",
            message_type="user",
            content="Test message"
        )
        
        # Act
        repr_str = repr(chat_history)
        
        # Assert
        assert "ChatHistory" in repr_str

    def test_chat_history_multi_tenant_fields(self):
        """Test multi-tenant fields for group isolation."""
        # Arrange
        group_id = "group-123"
        group_email = "admin@company.com"
        
        # Act
        chat_history = ChatHistory(
            session_id="test-session",
            user_id="test-user",
            message_type="user",
            content="Test message",
            group_id=group_id,
            group_email=group_email
        )
        
        # Assert
        assert chat_history.group_id == group_id
        assert chat_history.group_email == group_email

    def test_chat_history_json_generation_result(self):
        """Test ChatHistory with complex JSON generation result."""
        # Arrange
        complex_result = {
            "type": "agent",
            "data": {
                "name": "Data Analyst",
                "role": "Senior Data Analyst",
                "tools": ["python", "sql", "tableau"],
                "config": {
                    "max_iter": 50,
                    "verbose": True
                }
            },
            "metadata": {
                "created_at": "2023-01-01T00:00:00Z",
                "confidence": 0.98
            }
        }
        
        # Act
        chat_history = ChatHistory(
            session_id="test-session",
            user_id="test-user",
            message_type="assistant",
            content="Created a data analyst agent",
            generation_result=complex_result
        )
        
        # Assert
        assert chat_history.generation_result == complex_result
        assert chat_history.generation_result["type"] == "agent"
        assert chat_history.generation_result["data"]["name"] == "Data Analyst"
        assert len(chat_history.generation_result["data"]["tools"]) == 3
        assert chat_history.generation_result["metadata"]["confidence"] == 0.98

    def test_chat_history_message_types(self):
        """Test that both valid message types work correctly."""
        # Test user message
        user_message = ChatHistory(
            session_id="test-session",
            user_id="test-user",
            message_type="user",
            content="Create an agent for me"
        )
        assert user_message.message_type == "user"
        
        # Test assistant message  
        assistant_message = ChatHistory(
            session_id="test-session",
            user_id="test-user",
            message_type="assistant",
            content="I've created an agent for you"
        )
        assert assistant_message.message_type == "assistant"

    def test_chat_history_long_content(self):
        """Test ChatHistory with long text content."""
        # Arrange
        long_content = "This is a very long message. " * 100  # 2900 characters
        
        # Act
        chat_history = ChatHistory(
            session_id="test-session",
            user_id="test-user",
            message_type="user",
            content=long_content
        )
        
        # Assert
        assert chat_history.content == long_content
        assert len(chat_history.content) == 2900