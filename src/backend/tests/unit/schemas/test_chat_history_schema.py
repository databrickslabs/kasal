"""
Unit tests for chat history schemas.

Tests the functionality of Pydantic schemas for chat history operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Any, List

from src.schemas.chat_history import (
    ChatHistoryBase, ChatHistoryCreate, ChatHistoryUpdate,
    ChatHistoryInDBBase, ChatHistoryResponse, ChatHistoryInDB,
    ChatSessionInfo, ChatSessionListResponse, ChatHistoryListResponse,
    SaveMessageRequest, GetSessionRequest, GetUserSessionsRequest
)


class TestChatHistoryBase:
    """Test cases for ChatHistoryBase schema."""
    
    def test_valid_chat_history_base_minimal(self):
        """Test ChatHistoryBase with minimal required fields."""
        data = {
            "session_id": "session-123",
            "user_id": "user-456",
            "message_type": "user",
            "content": "Hello world"
        }
        chat = ChatHistoryBase(**data)
        assert chat.session_id == "session-123"
        assert chat.user_id == "user-456"
        assert chat.message_type == "user"
        assert chat.content == "Hello world"
        assert chat.intent is None
        assert chat.confidence is None
        assert chat.generation_result is None

    def test_valid_chat_history_base_full(self):
        """Test ChatHistoryBase with all fields."""
        data = {
            "session_id": "session-789",
            "user_id": "user-101",
            "message_type": "assistant",
            "content": "I can help you with that",
            "intent": "generate_agent",
            "confidence": "0.95",
            "generation_result": {"agent_id": "agent-123", "status": "success"}
        }
        chat = ChatHistoryBase(**data)
        assert chat.session_id == "session-789"
        assert chat.user_id == "user-101"
        assert chat.message_type == "assistant"
        assert chat.content == "I can help you with that"
        assert chat.intent == "generate_agent"
        assert chat.confidence == "0.95"
        assert chat.generation_result == {"agent_id": "agent-123", "status": "success"}

    def test_message_type_validation(self):
        """Test message_type field validation."""
        valid_types = ["user", "assistant", "execution", "trace"]
        
        for msg_type in valid_types:
            data = {
                "session_id": "session-test",
                "user_id": "user-test",
                "message_type": msg_type,
                "content": "Test content"
            }
            chat = ChatHistoryBase(**data)
            assert chat.message_type == msg_type

    def test_invalid_message_type(self):
        """Test invalid message_type validation."""
        data = {
            "session_id": "session-test",
            "user_id": "user-test",
            "message_type": "invalid_type",
            "content": "Test content"
        }
        with pytest.raises(ValidationError) as exc_info:
            ChatHistoryBase(**data)
        
        errors = exc_info.value.errors()
        assert any(error["loc"] == ("message_type",) for error in errors)

    def test_missing_required_fields(self):
        """Test validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            ChatHistoryBase(session_id="test")
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "user_id" in missing_fields
        assert "message_type" in missing_fields
        assert "content" in missing_fields

    def test_empty_content_validation(self):
        """Test content field minimum length validation."""
        data = {
            "session_id": "session-test",
            "user_id": "user-test",
            "message_type": "user",
            "content": ""
        }
        with pytest.raises(ValidationError) as exc_info:
            ChatHistoryBase(**data)
        
        errors = exc_info.value.errors()
        assert any(error["loc"] == ("content",) for error in errors)


class TestChatHistoryCreate:
    """Test cases for ChatHistoryCreate schema."""
    
    def test_chat_history_create_inheritance(self):
        """Test that ChatHistoryCreate inherits from ChatHistoryBase."""
        data = {
            "session_id": "create-session",
            "user_id": "create-user",
            "message_type": "user",
            "content": "Create test message"
        }
        create_chat = ChatHistoryCreate(**data)
        
        assert hasattr(create_chat, 'session_id')
        assert hasattr(create_chat, 'user_id')
        assert hasattr(create_chat, 'message_type')
        assert hasattr(create_chat, 'content')
        assert hasattr(create_chat, 'intent')
        assert hasattr(create_chat, 'confidence')
        assert hasattr(create_chat, 'generation_result')
        
        assert create_chat.session_id == "create-session"
        assert create_chat.user_id == "create-user"
        assert create_chat.message_type == "user"
        assert create_chat.content == "Create test message"


class TestChatHistoryUpdate:
    """Test cases for ChatHistoryUpdate schema."""
    
    def test_chat_history_update_all_optional(self):
        """Test that all ChatHistoryUpdate fields are optional."""
        update = ChatHistoryUpdate()
        assert update.content is None
        assert update.intent is None
        assert update.confidence is None
        assert update.generation_result is None

    def test_chat_history_update_partial(self):
        """Test ChatHistoryUpdate with partial fields."""
        update_data = {
            "content": "Updated content",
            "intent": "generate_task"
        }
        update = ChatHistoryUpdate(**update_data)
        assert update.content == "Updated content"
        assert update.intent == "generate_task"
        assert update.confidence is None
        assert update.generation_result is None

    def test_chat_history_update_full(self):
        """Test ChatHistoryUpdate with all fields."""
        update_data = {
            "content": "Fully updated content",
            "intent": "generate_crew",
            "confidence": "0.87",
            "generation_result": {"crew_id": "crew-456", "status": "updated"}
        }
        update = ChatHistoryUpdate(**update_data)
        assert update.content == "Fully updated content"
        assert update.intent == "generate_crew"
        assert update.confidence == "0.87"
        assert update.generation_result == {"crew_id": "crew-456", "status": "updated"}

    def test_update_content_validation(self):
        """Test content field validation in update."""
        update_data = {"content": ""}
        with pytest.raises(ValidationError) as exc_info:
            ChatHistoryUpdate(**update_data)
        
        errors = exc_info.value.errors()
        assert any(error["loc"] == ("content",) for error in errors)


class TestChatHistoryInDBBase:
    """Test cases for ChatHistoryInDBBase schema."""
    
    def test_valid_chat_history_in_db_base(self):
        """Test ChatHistoryInDBBase with all required fields."""
        now = datetime.now()
        data = {
            "id": "msg-123",
            "session_id": "session-db",
            "user_id": "user-db",
            "message_type": "assistant",
            "content": "Database message",
            "timestamp": now,
            "group_id": "group-123",
            "group_email": "group@example.com"
        }
        db_chat = ChatHistoryInDBBase(**data)
        assert db_chat.id == "msg-123"
        assert db_chat.session_id == "session-db"
        assert db_chat.user_id == "user-db"
        assert db_chat.message_type == "assistant"
        assert db_chat.content == "Database message"
        assert db_chat.timestamp == now
        assert db_chat.group_id == "group-123"
        assert db_chat.group_email == "group@example.com"

    def test_chat_history_in_db_base_config(self):
        """Test ChatHistoryInDBBase Config class."""
        assert hasattr(ChatHistoryInDBBase, 'model_config')
        assert ChatHistoryInDBBase.model_config.get('from_attributes') is True

    def test_missing_db_fields(self):
        """Test validation with missing database fields."""
        data = {
            "session_id": "session-test",
            "user_id": "user-test", 
            "message_type": "user",
            "content": "Test content"
        }
        with pytest.raises(ValidationError) as exc_info:
            ChatHistoryInDBBase(**data)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        assert "timestamp" in missing_fields


class TestChatHistoryResponse:
    """Test cases for ChatHistoryResponse schema."""
    
    def test_chat_history_response_inheritance(self):
        """Test that ChatHistoryResponse inherits from ChatHistoryInDBBase."""
        now = datetime.now()
        data = {
            "id": "response-123",
            "session_id": "response-session",
            "user_id": "response-user",
            "message_type": "execution",
            "content": "Response message",
            "timestamp": now
        }
        response_chat = ChatHistoryResponse(**data)
        
        assert hasattr(response_chat, 'id')
        assert hasattr(response_chat, 'timestamp')
        assert hasattr(response_chat, 'group_id')
        assert hasattr(response_chat, 'group_email')
        
        assert response_chat.id == "response-123"
        assert response_chat.session_id == "response-session"
        assert response_chat.timestamp == now


class TestChatSessionInfo:
    """Test cases for ChatSessionInfo schema."""
    
    def test_valid_chat_session_info(self):
        """Test ChatSessionInfo with all fields."""
        now = datetime.now()
        data = {
            "session_id": "session-info-123",
            "user_id": "user-info",
            "latest_timestamp": now,
            "message_count": 15
        }
        session_info = ChatSessionInfo(**data)
        assert session_info.session_id == "session-info-123"
        assert session_info.user_id == "user-info"
        assert session_info.latest_timestamp == now
        assert session_info.message_count == 15

    def test_session_info_without_message_count(self):
        """Test ChatSessionInfo without optional message_count."""
        now = datetime.now()
        data = {
            "session_id": "session-minimal",
            "user_id": "user-minimal",
            "latest_timestamp": now
        }
        session_info = ChatSessionInfo(**data)
        assert session_info.session_id == "session-minimal"
        assert session_info.user_id == "user-minimal"
        assert session_info.latest_timestamp == now
        assert session_info.message_count is None

    def test_session_info_config(self):
        """Test ChatSessionInfo Config class."""
        assert hasattr(ChatSessionInfo, 'model_config')
        assert ChatSessionInfo.model_config.get('from_attributes') is True


class TestChatSessionListResponse:
    """Test cases for ChatSessionListResponse schema."""
    
    def test_valid_chat_session_list_response(self):
        """Test ChatSessionListResponse with all fields."""
        now = datetime.now()
        sessions = [
            ChatSessionInfo(
                session_id="session-1",
                user_id="user-1",
                latest_timestamp=now,
                message_count=10
            ),
            ChatSessionInfo(
                session_id="session-2",
                user_id="user-1",
                latest_timestamp=now,
                message_count=5
            )
        ]
        
        data = {
            "sessions": sessions,
            "total_sessions": 2,
            "page": 0,
            "per_page": 20
        }
        list_response = ChatSessionListResponse(**data)
        
        assert len(list_response.sessions) == 2
        assert list_response.total_sessions == 2
        assert list_response.page == 0
        assert list_response.per_page == 20
        assert list_response.sessions[0].session_id == "session-1"
        assert list_response.sessions[1].session_id == "session-2"

    def test_empty_session_list(self):
        """Test ChatSessionListResponse with empty session list."""
        data = {
            "sessions": [],
            "total_sessions": 0,
            "page": 0,
            "per_page": 20
        }
        list_response = ChatSessionListResponse(**data)
        assert len(list_response.sessions) == 0
        assert list_response.total_sessions == 0


class TestChatHistoryListResponse:
    """Test cases for ChatHistoryListResponse schema."""
    
    def test_valid_chat_history_list_response(self):
        """Test ChatHistoryListResponse with all fields."""
        now = datetime.now()
        messages = [
            ChatHistoryResponse(
                id="msg-1",
                session_id="list-session",
                user_id="list-user",
                message_type="user",
                content="First message",
                timestamp=now
            ),
            ChatHistoryResponse(
                id="msg-2",
                session_id="list-session",
                user_id="list-user",
                message_type="assistant",
                content="Second message",
                timestamp=now
            )
        ]
        
        data = {
            "messages": messages,
            "total_messages": 2,
            "page": 0,
            "per_page": 50,
            "session_id": "list-session"
        }
        list_response = ChatHistoryListResponse(**data)
        
        assert len(list_response.messages) == 2
        assert list_response.total_messages == 2
        assert list_response.page == 0
        assert list_response.per_page == 50
        assert list_response.session_id == "list-session"
        assert list_response.messages[0].content == "First message"
        assert list_response.messages[1].content == "Second message"


class TestSaveMessageRequest:
    """Test cases for SaveMessageRequest schema."""
    
    def test_valid_save_message_request(self):
        """Test SaveMessageRequest with all fields."""
        data = {
            "session_id": "save-session",
            "message_type": "user",
            "content": "Save this message",
            "intent": "generate_agent",
            "confidence": 0.92,
            "generation_result": {"result": "success"}
        }
        request = SaveMessageRequest(**data)
        assert request.session_id == "save-session"
        assert request.message_type == "user"
        assert request.content == "Save this message"
        assert request.intent == "generate_agent"
        assert request.confidence == 0.92
        assert request.generation_result == {"result": "success"}

    def test_save_message_request_minimal(self):
        """Test SaveMessageRequest with minimal required fields."""
        data = {
            "session_id": "minimal-session",
            "message_type": "assistant",
            "content": "Minimal message"
        }
        request = SaveMessageRequest(**data)
        assert request.session_id == "minimal-session"
        assert request.message_type == "assistant"
        assert request.content == "Minimal message"
        assert request.intent is None
        assert request.confidence is None
        assert request.generation_result is None

    def test_confidence_range_validation(self):
        """Test confidence field range validation."""
        # Valid confidence values
        for confidence in [0.0, 0.5, 1.0]:
            data = {
                "session_id": "conf-session",
                "message_type": "user",
                "content": "Test confidence",
                "confidence": confidence
            }
            request = SaveMessageRequest(**data)
            assert request.confidence == confidence

        # Invalid confidence values
        for invalid_confidence in [-0.1, 1.1, 2.0]:
            data = {
                "session_id": "conf-session",
                "message_type": "user",
                "content": "Test confidence",
                "confidence": invalid_confidence
            }
            with pytest.raises(ValidationError):
                SaveMessageRequest(**data)


class TestGetSessionRequest:
    """Test cases for GetSessionRequest schema."""
    
    def test_valid_get_session_request(self):
        """Test GetSessionRequest with all fields."""
        data = {
            "page": 2,
            "per_page": 25
        }
        request = GetSessionRequest(**data)
        assert request.page == 2
        assert request.per_page == 25

    def test_get_session_request_defaults(self):
        """Test GetSessionRequest with default values."""
        request = GetSessionRequest()
        assert request.page == 0
        assert request.per_page == 50

    def test_get_session_request_validation(self):
        """Test GetSessionRequest field validation."""
        # Valid values
        request = GetSessionRequest(page=0, per_page=1)
        assert request.page == 0
        assert request.per_page == 1
        
        request = GetSessionRequest(page=10, per_page=100)
        assert request.page == 10
        assert request.per_page == 100

        # Invalid values
        with pytest.raises(ValidationError):
            GetSessionRequest(page=-1)  # page must be >= 0
            
        with pytest.raises(ValidationError):
            GetSessionRequest(per_page=0)  # per_page must be >= 1
            
        with pytest.raises(ValidationError):
            GetSessionRequest(per_page=101)  # per_page must be <= 100


class TestGetUserSessionsRequest:
    """Test cases for GetUserSessionsRequest schema."""
    
    def test_valid_get_user_sessions_request(self):
        """Test GetUserSessionsRequest with all fields."""
        data = {
            "page": 1,
            "per_page": 10
        }
        request = GetUserSessionsRequest(**data)
        assert request.page == 1
        assert request.per_page == 10

    def test_get_user_sessions_request_defaults(self):
        """Test GetUserSessionsRequest with default values."""
        request = GetUserSessionsRequest()
        assert request.page == 0
        assert request.per_page == 20

    def test_get_user_sessions_request_validation(self):
        """Test GetUserSessionsRequest field validation."""
        # Valid values
        request = GetUserSessionsRequest(page=0, per_page=1)
        assert request.page == 0
        assert request.per_page == 1
        
        request = GetUserSessionsRequest(page=5, per_page=50)
        assert request.page == 5
        assert request.per_page == 50

        # Invalid values
        with pytest.raises(ValidationError):
            GetUserSessionsRequest(page=-1)  # page must be >= 0
            
        with pytest.raises(ValidationError):
            GetUserSessionsRequest(per_page=0)  # per_page must be >= 1
            
        with pytest.raises(ValidationError):
            GetUserSessionsRequest(per_page=51)  # per_page must be <= 50


class TestSchemaIntegration:
    """Integration tests for chat history schema interactions."""
    
    def test_chat_message_workflow(self):
        """Test complete chat message workflow."""
        # Create message
        create_data = {
            "session_id": "workflow-session",
            "user_id": "workflow-user",
            "message_type": "user",
            "content": "Help me create an agent",
            "intent": "generate_agent"
        }
        create_schema = ChatHistoryCreate(**create_data)
        
        # Update message
        update_data = {
            "confidence": "0.95",
            "generation_result": {"agent_id": "agent-123", "status": "created"}
        }
        update_schema = ChatHistoryUpdate(**update_data)
        
        # Simulate database entity
        now = datetime.now()
        db_data = {
            "id": "msg-workflow-1",
            "session_id": create_schema.session_id,
            "user_id": create_schema.user_id,
            "message_type": create_schema.message_type,
            "content": create_schema.content,
            "intent": create_schema.intent,
            "confidence": update_data["confidence"],
            "generation_result": update_data["generation_result"],
            "timestamp": now,
            "group_id": "group-123"
        }
        response = ChatHistoryResponse(**db_data)
        
        # Verify the complete workflow
        assert create_schema.session_id == "workflow-session"
        assert create_schema.intent == "generate_agent"
        assert update_schema.confidence == "0.95"
        assert response.id == "msg-workflow-1"
        assert response.session_id == "workflow-session"
        assert response.confidence == "0.95"
        assert response.generation_result == {"agent_id": "agent-123", "status": "created"}
        assert response.timestamp == now

    def test_session_management_workflow(self):
        """Test session management workflow."""
        # Create session info
        now = datetime.now()
        session_info = ChatSessionInfo(
            session_id="mgmt-session",
            user_id="mgmt-user",
            latest_timestamp=now,
            message_count=25
        )
        
        # Create session list
        session_list = ChatSessionListResponse(
            sessions=[session_info],
            total_sessions=1,
            page=0,
            per_page=20
        )
        
        # Create message history
        message = ChatHistoryResponse(
            id="mgmt-msg-1",
            session_id="mgmt-session",
            user_id="mgmt-user",
            message_type="user",
            content="Session management test",
            timestamp=now
        )
        
        message_list = ChatHistoryListResponse(
            messages=[message],
            total_messages=1,
            page=0,
            per_page=50,
            session_id="mgmt-session"
        )
        
        # Verify workflow
        assert session_list.sessions[0].session_id == "mgmt-session"
        assert session_list.sessions[0].message_count == 25
        assert message_list.messages[0].session_id == "mgmt-session"
        assert message_list.session_id == "mgmt-session"
        assert message_list.total_messages == 1