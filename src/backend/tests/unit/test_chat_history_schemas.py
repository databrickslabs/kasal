"""
Unit tests for ChatHistory schemas.

Tests the functionality of the chat history Pydantic schemas including
validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Any

from src.schemas.chat_history import (
    ChatHistoryBase,
    ChatHistoryCreate,
    ChatHistoryUpdate,
    ChatHistoryResponse,
    ChatHistoryInDB,
    ChatSessionInfo,
    ChatSessionListResponse,
    ChatHistoryListResponse,
    SaveMessageRequest,
    GetSessionRequest,
    GetUserSessionsRequest
)


class TestChatHistoryBase:
    """Test cases for ChatHistoryBase schema."""

    def test_valid_chat_history_base(self):
        """Test valid ChatHistoryBase creation."""
        # Arrange & Act
        schema = ChatHistoryBase(
            session_id="session-123",
            user_id="user-456",
            message_type="user",
            content="Test message"
        )
        
        # Assert
        assert schema.session_id == "session-123"
        assert schema.user_id == "user-456"
        assert schema.message_type == "user"
        assert schema.content == "Test message"
        assert schema.intent is None
        assert schema.confidence is None
        assert schema.generation_result is None

    def test_chat_history_base_with_optional_fields(self):
        """Test ChatHistoryBase with optional fields."""
        # Arrange
        generation_result = {"agent_name": "Test Agent", "tools": ["web_search"]}
        
        # Act
        schema = ChatHistoryBase(
            session_id="session-123",
            user_id="user-456",
            message_type="assistant",
            content="I've created an agent for you",
            intent="generate_agent",
            confidence="0.95",
            generation_result=generation_result
        )
        
        # Assert
        assert schema.intent == "generate_agent"
        assert schema.confidence == "0.95"
        assert schema.generation_result == generation_result

    def test_invalid_message_type(self):
        """Test ChatHistoryBase with invalid message type."""
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            ChatHistoryBase(
                session_id="session-123",
                user_id="user-456",
                message_type="invalid",
                content="Test message"
            )
        
        # Check for pattern matching error (Pydantic v2 format)
        error_str = str(exc_info.value)
        assert "String should match pattern" in error_str or "string_pattern_mismatch" in error_str

    def test_empty_content(self):
        """Test ChatHistoryBase with empty content."""
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            ChatHistoryBase(
                session_id="session-123",
                user_id="user-456",
                message_type="user",
                content=""
            )
        
        # Check for the error about string length (Pydantic v2 format)
        error_str = str(exc_info.value).lower()
        assert "at least 1 character" in error_str or "string_too_short" in error_str

    def test_missing_required_fields(self):
        """Test ChatHistoryBase with missing required fields."""
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            ChatHistoryBase(
                session_id="session-123",
                user_id="user-456"
                # Missing message_type and content
            )
        
        # Check for field required error (Pydantic v2 format)
        error_str = str(exc_info.value).lower()
        assert "field required" in error_str or "missing" in error_str


class TestChatHistoryCreate:
    """Test cases for ChatHistoryCreate schema."""

    def test_valid_create_schema(self):
        """Test valid ChatHistoryCreate."""
        # Act
        schema = ChatHistoryCreate(
            session_id="session-123",
            user_id="user-456",
            message_type="user",
            content="Create an agent for me"
        )
        
        # Assert
        assert schema.session_id == "session-123"
        assert schema.user_id == "user-456"
        assert schema.message_type == "user"
        assert schema.content == "Create an agent for me"

    def test_create_schema_inheritance(self):
        """Test that ChatHistoryCreate inherits from ChatHistoryBase."""
        # Act
        schema = ChatHistoryCreate(
            session_id="session-123",
            user_id="user-456",
            message_type="assistant",
            content="Agent created successfully",
            intent="generate_agent",
            confidence="0.95"
        )
        
        # Assert
        assert hasattr(schema, 'session_id')
        assert hasattr(schema, 'user_id')
        assert hasattr(schema, 'message_type')
        assert hasattr(schema, 'content')
        assert hasattr(schema, 'intent')
        assert hasattr(schema, 'confidence')
        assert hasattr(schema, 'generation_result')


class TestChatHistoryUpdate:
    """Test cases for ChatHistoryUpdate schema."""

    def test_valid_update_schema(self):
        """Test valid ChatHistoryUpdate."""
        # Act
        schema = ChatHistoryUpdate(
            content="Updated message content",
            intent="generate_task",
            confidence="0.85"
        )
        
        # Assert
        assert schema.content == "Updated message content"
        assert schema.intent == "generate_task"
        assert schema.confidence == "0.85"

    def test_update_schema_all_optional(self):
        """Test that all fields in ChatHistoryUpdate are optional."""
        # Act
        schema = ChatHistoryUpdate()
        
        # Assert
        assert schema.content is None
        assert schema.intent is None
        assert schema.confidence is None
        assert schema.generation_result is None

    def test_update_schema_empty_content_validation(self):
        """Test ChatHistoryUpdate with empty content validation."""
        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            ChatHistoryUpdate(content="")
        
        # Check for the error about string length (Pydantic v2 format)
        error_str = str(exc_info.value).lower()
        assert "at least 1 character" in error_str or "string_too_short" in error_str


class TestChatHistoryResponse:
    """Test cases for ChatHistoryResponse schema."""

    def test_valid_response_schema(self):
        """Test valid ChatHistoryResponse."""
        # Act
        schema = ChatHistoryResponse(
            id="chat-123",
            session_id="session-456",
            user_id="user-789",
            message_type="user",
            content="Test message",
            timestamp=datetime.utcnow(),
            group_id="group-123",
            group_email="admin@company.com"
        )
        
        # Assert
        assert schema.id == "chat-123"
        assert schema.session_id == "session-456"
        assert schema.user_id == "user-789"
        assert schema.message_type == "user"
        assert schema.content == "Test message"
        assert isinstance(schema.timestamp, datetime)
        assert schema.group_id == "group-123"
        assert schema.group_email == "admin@company.com"

    def test_response_schema_config(self):
        """Test ChatHistoryResponse Config settings."""
        # Act
        config = ChatHistoryResponse.Config
        
        # Assert
        assert config.from_attributes is True


class TestChatSessionInfo:
    """Test cases for ChatSessionInfo schema."""

    def test_valid_session_info(self):
        """Test valid ChatSessionInfo."""
        # Arrange
        timestamp = datetime.utcnow()
        
        # Act
        schema = ChatSessionInfo(
            session_id="session-123",
            user_id="user-456",
            latest_timestamp=timestamp,
            message_count=25
        )
        
        # Assert
        assert schema.session_id == "session-123"
        assert schema.user_id == "user-456"
        assert schema.latest_timestamp == timestamp
        assert schema.message_count == 25

    def test_session_info_optional_message_count(self):
        """Test ChatSessionInfo with optional message_count."""
        # Act
        schema = ChatSessionInfo(
            session_id="session-123",
            user_id="user-456",
            latest_timestamp=datetime.utcnow()
        )
        
        # Assert
        assert schema.message_count is None


class TestChatSessionListResponse:
    """Test cases for ChatSessionListResponse schema."""

    def test_valid_session_list_response(self):
        """Test valid ChatSessionListResponse."""
        # Arrange
        sessions = [
            ChatSessionInfo(
                session_id="session-1",
                user_id="user-1",
                latest_timestamp=datetime.utcnow()
            ),
            ChatSessionInfo(
                session_id="session-2",
                user_id="user-2",
                latest_timestamp=datetime.utcnow()
            )
        ]
        
        # Act
        schema = ChatSessionListResponse(
            sessions=sessions,
            total_sessions=2,
            page=0,
            per_page=20
        )
        
        # Assert
        assert len(schema.sessions) == 2
        assert schema.total_sessions == 2
        assert schema.page == 0
        assert schema.per_page == 20

    def test_empty_session_list_response(self):
        """Test ChatSessionListResponse with empty sessions."""
        # Act
        schema = ChatSessionListResponse(
            sessions=[],
            total_sessions=0,
            page=0,
            per_page=20
        )
        
        # Assert
        assert len(schema.sessions) == 0
        assert schema.total_sessions == 0


class TestChatHistoryListResponse:
    """Test cases for ChatHistoryListResponse schema."""

    def test_valid_history_list_response(self):
        """Test valid ChatHistoryListResponse."""
        # Arrange
        messages = [
            ChatHistoryResponse(
                id="msg-1",
                session_id="session-123",
                user_id="user-456",
                message_type="user",
                content="Hello",
                timestamp=datetime.utcnow()
            ),
            ChatHistoryResponse(
                id="msg-2",
                session_id="session-123",
                user_id="assistant",
                message_type="assistant",
                content="Hi there!",
                timestamp=datetime.utcnow()
            )
        ]
        
        # Act
        schema = ChatHistoryListResponse(
            messages=messages,
            total_messages=2,
            page=0,
            per_page=50,
            session_id="session-123"
        )
        
        # Assert
        assert len(schema.messages) == 2
        assert schema.total_messages == 2
        assert schema.page == 0
        assert schema.per_page == 50
        assert schema.session_id == "session-123"


class TestSaveMessageRequest:
    """Test cases for SaveMessageRequest schema."""

    def test_valid_save_message_request(self):
        """Test valid SaveMessageRequest."""
        # Act
        schema = SaveMessageRequest(
            session_id="session-123",
            message_type="user",
            content="Create an agent for me",
            intent="generate_agent",
            confidence=0.95,
            generation_result={"agent_name": "Test Agent"}
        )
        
        # Assert
        assert schema.session_id == "session-123"
        assert schema.message_type == "user"
        assert schema.content == "Create an agent for me"
        assert schema.intent == "generate_agent"
        assert schema.confidence == 0.95
        assert schema.generation_result == {"agent_name": "Test Agent"}

    def test_save_message_request_confidence_validation(self):
        """Test SaveMessageRequest confidence validation."""
        # Test valid confidence range
        schema = SaveMessageRequest(
            session_id="session-123",
            message_type="user",
            content="Test",
            confidence=0.5
        )
        assert schema.confidence == 0.5
        
        # Test invalid confidence (too low)
        with pytest.raises(ValidationError) as exc_info:
            SaveMessageRequest(
                session_id="session-123",
                message_type="user",
                content="Test",
                confidence=-0.1
            )
        # Check for minimum value error (Pydantic v2 format)
        error_str = str(exc_info.value).lower()
        assert "greater than or equal to 0" in error_str or "greater_than_equal" in error_str
        
        # Test invalid confidence (too high)
        with pytest.raises(ValidationError) as exc_info:
            SaveMessageRequest(
                session_id="session-123",
                message_type="user",
                content="Test",
                confidence=1.1
            )
        # Check for maximum value error (Pydantic v2 format)
        error_str = str(exc_info.value).lower()
        assert "less than or equal to 1" in error_str or "less_than_equal" in error_str

    def test_save_message_request_message_type_validation(self):
        """Test SaveMessageRequest message type validation."""
        # Valid message types
        for msg_type in ["user", "assistant"]:
            schema = SaveMessageRequest(
                session_id="session-123",
                message_type=msg_type,
                content="Test message"
            )
            assert schema.message_type == msg_type
        
        # Invalid message type
        with pytest.raises(ValidationError) as exc_info:
            SaveMessageRequest(
                session_id="session-123",
                message_type="invalid",
                content="Test message"
            )
        # Check for pattern matching error (Pydantic v2 format)
        error_str = str(exc_info.value)
        assert "string does not match expected pattern" in error_str or "string_pattern_mismatch" in error_str or "String should match pattern" in error_str


class TestGetSessionRequest:
    """Test cases for GetSessionRequest schema."""

    def test_valid_get_session_request(self):
        """Test valid GetSessionRequest."""
        # Act
        schema = GetSessionRequest(page=2, per_page=25)
        
        # Assert
        assert schema.page == 2
        assert schema.per_page == 25

    def test_get_session_request_defaults(self):
        """Test GetSessionRequest default values."""
        # Act
        schema = GetSessionRequest()
        
        # Assert
        assert schema.page == 0
        assert schema.per_page == 50

    def test_get_session_request_validation(self):
        """Test GetSessionRequest validation."""
        # Test negative page
        with pytest.raises(ValidationError) as exc_info:
            GetSessionRequest(page=-1)
        # Check for minimum value error (Pydantic v2 format)
        error_str = str(exc_info.value).lower()
        assert "greater than or equal to 0" in error_str or "greater_than_equal" in error_str
        
        # Test per_page too high
        with pytest.raises(ValidationError) as exc_info:
            GetSessionRequest(per_page=101)
        # Check for maximum value error (Pydantic v2 format)
        error_str = str(exc_info.value).lower()
        assert "less than or equal to 100" in error_str or "less_than_equal" in error_str
        
        # Test per_page too low
        with pytest.raises(ValidationError) as exc_info:
            GetSessionRequest(per_page=0)
        # Check for minimum value error (Pydantic v2 format)
        error_str = str(exc_info.value).lower()
        assert "greater than or equal to 1" in error_str or "greater_than_equal" in error_str


class TestGetUserSessionsRequest:
    """Test cases for GetUserSessionsRequest schema."""

    def test_valid_get_user_sessions_request(self):
        """Test valid GetUserSessionsRequest."""
        # Act
        schema = GetUserSessionsRequest(page=1, per_page=15)
        
        # Assert
        assert schema.page == 1
        assert schema.per_page == 15

    def test_get_user_sessions_request_defaults(self):
        """Test GetUserSessionsRequest default values."""
        # Act
        schema = GetUserSessionsRequest()
        
        # Assert
        assert schema.page == 0
        assert schema.per_page == 20

    def test_get_user_sessions_request_validation(self):
        """Test GetUserSessionsRequest validation."""
        # Test per_page too high
        with pytest.raises(ValidationError) as exc_info:
            GetUserSessionsRequest(per_page=51)
        # Check for maximum value error (Pydantic v2 format)
        error_str = str(exc_info.value).lower()
        assert "less than or equal to 50" in error_str or "less_than_equal" in error_str


class TestSchemaInteroperability:
    """Test cases for schema interoperability and conversions."""

    def test_create_to_response_conversion(self):
        """Test converting ChatHistoryCreate to ChatHistoryResponse."""
        # Arrange
        create_data = {
            "session_id": "session-123",
            "user_id": "user-456",
            "message_type": "user",
            "content": "Test message",
            "intent": "generate_agent",
            "confidence": "0.95"
        }
        
        response_data = {
            **create_data,
            "id": "chat-123",
            "timestamp": datetime.utcnow(),
            "group_id": "group-123",
            "group_email": "admin@company.com"
        }
        
        # Act
        create_schema = ChatHistoryCreate(**create_data)
        response_schema = ChatHistoryResponse(**response_data)
        
        # Assert
        assert create_schema.session_id == response_schema.session_id
        assert create_schema.user_id == response_schema.user_id
        assert create_schema.message_type == response_schema.message_type
        assert create_schema.content == response_schema.content
        assert create_schema.intent == response_schema.intent
        assert create_schema.confidence == response_schema.confidence

    def test_complex_generation_result_serialization(self):
        """Test complex generation result serialization."""
        # Arrange
        complex_result = {
            "type": "crew",
            "agents": [
                {
                    "name": "Research Agent",
                    "role": "Researcher",
                    "tools": ["web_search", "document_analysis"]
                },
                {
                    "name": "Writer Agent",
                    "role": "Content Writer",
                    "tools": ["text_generation"]
                }
            ],
            "tasks": [
                {
                    "name": "Research Task",
                    "description": "Research the topic",
                    "agent_id": "agent-1"
                }
            ],
            "metadata": {
                "created_at": "2023-01-01T00:00:00Z",
                "confidence": 0.98,
                "processing_time": 1.5
            }
        }
        
        # Act
        schema = SaveMessageRequest(
            session_id="session-123",
            message_type="assistant",
            content="Created a crew with 2 agents and 1 task",
            intent="generate_crew",
            generation_result=complex_result
        )
        
        # Assert
        assert schema.generation_result == complex_result
        assert schema.generation_result["type"] == "crew"
        assert len(schema.generation_result["agents"]) == 2
        assert len(schema.generation_result["tasks"]) == 1
        assert schema.generation_result["metadata"]["confidence"] == 0.98