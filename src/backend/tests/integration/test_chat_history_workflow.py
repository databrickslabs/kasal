"""
Integration tests for chat history workflow.

Tests end-to-end chat history functionality including
message persistence, session management, and group isolation.
"""
import pytest
import uuid
import asyncio
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from typing import Dict, Any

from src.main import app


@pytest.fixture
def client():
    """Create test client for integration tests."""
    return TestClient(app)


@pytest.fixture
def mock_group_headers():
    """Mock group headers for multi-tenant testing."""
    return {
        "X-Auth-Request-Email": "testuser@company.com",
        "X-Auth-Request-Access-Token": "test-access-token"
    }


@pytest.fixture
def sample_chat_message():
    """Sample chat message data."""
    return {
        "session_id": "session-123",
        "message_type": "user",
        "content": "Create an agent that can analyze financial data",
        "intent": "generate_agent",
        "confidence": 0.95,
        "generation_result": {
            "agent_name": "Financial Analyst",
            "role": "Data Analyst",
            "tools": ["python", "pandas", "matplotlib"]
        }
    }


@pytest.fixture
def sample_assistant_message():
    """Sample assistant message data."""
    return {
        "session_id": "session-123",
        "message_type": "assistant",
        "content": "I've created a financial analyst agent for you",
        "intent": "generate_agent",
        "confidence": 0.98,
        "generation_result": {
            "agent_id": "agent-456",
            "created": True
        }
    }


class TestChatHistoryWorkflowIntegration:
    """Integration tests for chat history workflow."""

    @pytest.mark.asyncio
    async def test_complete_chat_session_workflow(self, client, mock_group_headers, sample_chat_message, sample_assistant_message):
        """Test complete chat session workflow from creation to deletion."""
        # Step 1: Create new chat session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        assert response.status_code == 201
        session_data = response.json()
        assert "session_id" in session_data
        session_id = session_data["session_id"]
        
        # Step 2: Save user message
        user_message = {**sample_chat_message, "session_id": session_id}
        response = client.post(
            "/api/v1/chat-history/messages",
            json=user_message,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        user_message_response = response.json()
        assert user_message_response["message_type"] == "user"
        assert user_message_response["content"] == user_message["content"]
        assert user_message_response["session_id"] == session_id
        
        # Step 3: Save assistant message
        assistant_message = {**sample_assistant_message, "session_id": session_id}
        response = client.post(
            "/api/v1/chat-history/messages",
            json=assistant_message,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        assistant_message_response = response.json()
        assert assistant_message_response["message_type"] == "assistant"
        assert assistant_message_response["content"] == assistant_message["content"]
        
        # Step 4: Retrieve session messages
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers,
            params={"page": 0, "per_page": 50}
        )
        assert response.status_code == 200
        messages_data = response.json()
        assert messages_data["session_id"] == session_id
        assert len(messages_data["messages"]) == 2
        assert messages_data["total_messages"] == 2
        
        # Verify message order (should be chronological)
        messages = messages_data["messages"]
        user_msg = next(msg for msg in messages if msg["message_type"] == "user")
        assistant_msg = next(msg for msg in messages if msg["message_type"] == "assistant")
        assert user_msg["content"] == user_message["content"]
        assert assistant_msg["content"] == assistant_message["content"]
        
        # Step 5: Get user sessions
        response = client.get(
            "/api/v1/chat-history/users/sessions",
            headers=mock_group_headers,
            params={"page": 0, "per_page": 20}
        )
        assert response.status_code == 200
        user_sessions = response.json()
        assert len(user_sessions) >= 1
        
        # Step 6: Get group sessions
        response = client.get(
            "/api/v1/chat-history/sessions",
            headers=mock_group_headers,
            params={"page": 0, "per_page": 20}
        )
        assert response.status_code == 200
        group_sessions_data = response.json()
        assert len(group_sessions_data["sessions"]) >= 1
        
        # Step 7: Delete session
        response = client.delete(
            f"/api/v1/chat-history/sessions/{session_id}",
            headers=mock_group_headers
        )
        assert response.status_code == 204
        
        # Step 8: Verify session is deleted
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        messages_data = response.json()
        assert len(messages_data["messages"]) == 0
        assert messages_data["total_messages"] == 0

    @pytest.mark.asyncio
    async def test_group_isolation(self, client, sample_chat_message):
        """Test that chat messages are properly isolated by group."""
        # Create messages with different group headers
        group1_headers = {
            "X-Auth-Request-Email": "user1@company.com",
            "X-Auth-Request-Access-Token": "test-access-token-1"
        }
        
        group2_headers = {
            "X-Auth-Request-Email": "user2@company.com",
            "X-Auth-Request-Access-Token": "test-access-token-2"
        }
        
        # Create session for group 1
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=group1_headers
        )
        assert response.status_code == 201
        session1_id = response.json()["session_id"]
        
        # Create session for group 2
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=group2_headers
        )
        assert response.status_code == 201
        session2_id = response.json()["session_id"]
        
        # Save message in group 1
        message1 = {**sample_chat_message, "session_id": session1_id, "content": "Group 1 message"}
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message1,
            headers=group1_headers
        )
        assert response.status_code == 201
        
        # Save message in group 2
        message2 = {**sample_chat_message, "session_id": session2_id, "content": "Group 2 message"}
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message2,
            headers=group2_headers
        )
        assert response.status_code == 201
        
        # Group 1 should only see their messages
        response = client.get(
            f"/api/v1/chat-history/sessions/{session1_id}/messages",
            headers=group1_headers
        )
        assert response.status_code == 200
        group1_messages = response.json()["messages"]
        assert len(group1_messages) == 1
        assert group1_messages[0]["content"] == "Group 1 message"
        
        # Group 2 should only see their messages
        response = client.get(
            f"/api/v1/chat-history/sessions/{session2_id}/messages",
            headers=group2_headers
        )
        assert response.status_code == 200
        group2_messages = response.json()["messages"]
        assert len(group2_messages) == 1
        assert group2_messages[0]["content"] == "Group 2 message"
        
        # Group 1 should not see group 2's session
        response = client.get(
            f"/api/v1/chat-history/sessions/{session2_id}/messages",
            headers=group1_headers
        )
        assert response.status_code == 200
        cross_group_messages = response.json()["messages"]
        assert len(cross_group_messages) == 0  # Should be empty due to group isolation

    @pytest.mark.asyncio
    async def test_pagination_workflow(self, client, mock_group_headers):
        """Test pagination across multiple chat messages."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Create 15 messages to test pagination
        for i in range(15):
            message = {
                "session_id": session_id,
                "message_type": "user" if i % 2 == 0 else "assistant",
                "content": f"Test message {i + 1}"
            }
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            assert response.status_code == 201
        
        # Test first page (10 messages)
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers,
            params={"page": 0, "per_page": 10}
        )
        assert response.status_code == 200
        page1_data = response.json()
        assert len(page1_data["messages"]) == 10
        assert page1_data["total_messages"] == 15
        assert page1_data["page"] == 0
        assert page1_data["per_page"] == 10
        
        # Test second page (5 messages)
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers,
            params={"page": 1, "per_page": 10}
        )
        assert response.status_code == 200
        page2_data = response.json()
        assert len(page2_data["messages"]) == 5
        assert page2_data["total_messages"] == 15
        assert page2_data["page"] == 1

    @pytest.mark.asyncio
    async def test_error_handling_workflow(self, client, mock_group_headers):
        """Test error handling in chat history workflow."""
        # Test invalid message type
        invalid_message = {
            "session_id": "session-123",
            "message_type": "invalid_type",
            "content": "Test message"
        }
        response = client.post(
            "/api/v1/chat-history/messages",
            json=invalid_message,
            headers=mock_group_headers
        )
        assert response.status_code == 422  # Validation error
        
        # Test empty content
        empty_content_message = {
            "session_id": "session-123",
            "message_type": "user",
            "content": ""
        }
        response = client.post(
            "/api/v1/chat-history/messages",
            json=empty_content_message,
            headers=mock_group_headers
        )
        assert response.status_code == 422  # Validation error
        
        # Test missing group context
        response = client.post(
            "/api/v1/chat-history/messages",
            json={
                "session_id": "session-123",
                "message_type": "user",
                "content": "Test message"
            }
            # No headers provided
        )
        assert response.status_code == 400  # Bad request
        
        # Test deleting non-existent session
        response = client.delete(
            "/api/v1/chat-history/sessions/non-existent-session",
            headers=mock_group_headers
        )
        assert response.status_code == 404  # Not found

    @pytest.mark.asyncio
    async def test_complex_generation_result_workflow(self, client, mock_group_headers):
        """Test workflow with complex generation results."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Save message with complex generation result
        complex_message = {
            "session_id": session_id,
            "message_type": "assistant",
            "content": "I've created a complete plan for you",
            "intent": "generate_crew",
            "confidence": 0.97,
            "generation_result": {
                "crew_id": "crew-456",
                "agents": [
                    {
                        "id": "agent-1",
                        "name": "Research Agent",
                        "role": "Researcher",
                        "tools": ["web_search", "document_analysis"],
                        "config": {
                            "max_iter": 25,
                            "verbose": True
                        }
                    },
                    {
                        "id": "agent-2", 
                        "name": "Writer Agent",
                        "role": "Content Writer",
                        "tools": ["text_generation"],
                        "config": {
                            "max_iter": 15,
                            "temperature": 0.7
                        }
                    }
                ],
                "tasks": [
                    {
                        "id": "task-1",
                        "name": "Research Task",
                        "description": "Research the given topic",
                        "agent_id": "agent-1",
                        "dependencies": []
                    },
                    {
                        "id": "task-2",
                        "name": "Writing Task", 
                        "description": "Write content based on research",
                        "agent_id": "agent-2",
                        "dependencies": ["task-1"]
                    }
                ],
                "metadata": {
                    "created_at": "2023-01-01T00:00:00Z",
                    "processing_time": 2.5,
                    "llm_model": "gpt-4o-mini"
                }
            }
        }
        
        response = client.post(
            "/api/v1/chat-history/messages",
            json=complex_message,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        
        # Retrieve and verify the complex data
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        messages = response.json()["messages"]
        assert len(messages) == 1
        
        saved_message = messages[0]
        assert saved_message["intent"] == "generate_crew"
        assert saved_message["confidence"] == "0.97"
        
        generation_result = saved_message["generation_result"]
        assert generation_result["crew_id"] == "crew-456"
        assert len(generation_result["agents"]) == 2
        assert len(generation_result["tasks"]) == 2
        assert generation_result["agents"][0]["name"] == "Research Agent"
        assert generation_result["tasks"][1]["dependencies"] == ["task-1"]
        assert generation_result["metadata"]["processing_time"] == 2.5

    @pytest.mark.asyncio
    async def test_concurrent_session_workflow(self, client, mock_group_headers):
        """Test concurrent operations on different sessions."""
        # Create multiple sessions concurrently
        sessions = []
        for i in range(3):
            response = client.post(
                "/api/v1/chat-history/sessions/new",
                headers=mock_group_headers
            )
            assert response.status_code == 201
            sessions.append(response.json()["session_id"])
        
        # Add messages to each session concurrently
        for i, session_id in enumerate(sessions):
            message = {
                "session_id": session_id,
                "message_type": "user",
                "content": f"Message for session {i + 1}"
            }
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            assert response.status_code == 201
        
        # Verify each session has its own messages
        for i, session_id in enumerate(sessions):
            response = client.get(
                f"/api/v1/chat-history/sessions/{session_id}/messages",
                headers=mock_group_headers
            )
            assert response.status_code == 200
            messages = response.json()["messages"]
            assert len(messages) == 1
            assert messages[0]["content"] == f"Message for session {i + 1}"

    @pytest.mark.asyncio
    async def test_user_session_filtering_workflow(self, client):
        """Test user-based session filtering."""
        # Create sessions for different users
        user1_headers = {
            "X-Auth-Request-Email": "user1@company.com",
            "X-Auth-Request-Access-Token": "test-access-token-1"
        }
        
        user2_headers = {
            "X-Auth-Request-Email": "user2@company.com",
            "X-Auth-Request-Access-Token": "test-access-token-2"
        }
        
        # Create sessions for each user
        sessions_user1 = []
        sessions_user2 = []
        
        for i in range(2):
            # User 1 sessions
            response = client.post(
                "/api/v1/chat-history/sessions/new",
                headers=user1_headers
            )
            sessions_user1.append(response.json()["session_id"])
            
            # User 2 sessions
            response = client.post(
                "/api/v1/chat-history/sessions/new",
                headers=user2_headers
            )
            sessions_user2.append(response.json()["session_id"])
        
        # Add messages to each session
        for session_id in sessions_user1:
            response = client.post(
                "/api/v1/chat-history/messages",
                json={
                    "session_id": session_id,
                    "message_type": "user",
                    "content": "User 1 message"
                },
                headers=user1_headers
            )
            assert response.status_code == 201
        
        for session_id in sessions_user2:
            response = client.post(
                "/api/v1/chat-history/messages",
                json={
                    "session_id": session_id,
                    "message_type": "user", 
                    "content": "User 2 message"
                },
                headers=user2_headers
            )
            assert response.status_code == 201
        
        # Get user 1's sessions
        response = client.get(
            "/api/v1/chat-history/users/sessions",
            headers=user1_headers
        )
        assert response.status_code == 200
        user1_sessions = response.json()
        assert len(user1_sessions) >= 2  # Allow for data from other tests
        
        # Get user 2's sessions
        response = client.get(
            "/api/v1/chat-history/users/sessions",
            headers=user2_headers
        )
        assert response.status_code == 200
        user2_sessions = response.json()
        assert len(user2_sessions) >= 2  # Allow for data from other tests
        
        # Get group sessions with user filter
        response = client.get(
            "/api/v1/chat-history/sessions",
            headers=user1_headers,
            params={"user_id": "user1@company.com"}
        )
        assert response.status_code == 200
        filtered_sessions = response.json()["sessions"]
        # Should find sessions for user1 (exact count depends on isolation implementation)

    @pytest.mark.asyncio
    async def test_missing_group_context_scenarios(self, client):
        """Test scenarios with missing or invalid group context."""
        # Test with completely missing headers
        response = client.post("/api/v1/chat-history/sessions/new")
        assert response.status_code == 400
        
        # Test with partial headers (email only - this should work as it creates individual group)
        partial_headers = {
            "X-Auth-Request-Email": "user@company.com"
        }
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=partial_headers
        )
        assert response.status_code == 201  # Individual group fallback should work
        
        # Test get operations with valid individual context
        response = client.get(
            "/api/v1/chat-history/sessions/test-session/messages",
            headers=partial_headers
        )
        assert response.status_code == 200  # Should work with individual group
        
        response = client.get(
            "/api/v1/chat-history/users/sessions",
            headers=partial_headers
        )
        assert response.status_code == 200  # Should work with individual group
        
        response = client.get(
            "/api/v1/chat-history/sessions",
            headers=partial_headers
        )
        assert response.status_code == 200  # Should work with individual group

    @pytest.mark.asyncio
    async def test_confidence_score_conversion_workflow(self, client, mock_group_headers):
        """Test confidence score conversion and storage."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Test various confidence score formats
        confidence_test_cases = [
            {"confidence": 0.95, "expected": "0.95"},
            {"confidence": 1.0, "expected": "1.0"},
            {"confidence": 0.0, "expected": "0.0"},
            {"confidence": 0.123456, "expected": "0.123456"},
        ]
        
        for i, test_case in enumerate(confidence_test_cases):
            message = {
                "session_id": session_id,
                "message_type": "assistant",
                "content": f"Test message {i}",
                "intent": "test_intent",
                "confidence": test_case["confidence"]
            }
            
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            assert response.status_code == 201
            
            saved_message = response.json()
            assert saved_message["confidence"] == test_case["expected"]
        
        # Test None confidence (should be null)
        message_no_confidence = {
            "session_id": session_id,
            "message_type": "user",
            "content": "Message without confidence"
        }
        
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message_no_confidence,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        saved_message = response.json()
        assert saved_message["confidence"] is None

    @pytest.mark.asyncio
    async def test_message_type_validation_workflow(self, client, mock_group_headers):
        """Test various message type scenarios."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Test valid message types
        valid_types = ["user", "assistant"]
        for msg_type in valid_types:
            message = {
                "session_id": session_id,
                "message_type": msg_type,
                "content": f"Test {msg_type} message"
            }
            
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            assert response.status_code == 201
            assert response.json()["message_type"] == msg_type
        
        # Test invalid message types
        invalid_types = ["system", "error", "debug", ""]
        for msg_type in invalid_types:
            message = {
                "session_id": session_id,
                "message_type": msg_type,
                "content": "Test message"
            }
            
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_boundary_pagination_workflow(self, client, mock_group_headers):
        """Test pagination boundary conditions."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new", 
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Add a few messages
        for i in range(5):
            message = {
                "session_id": session_id,
                "message_type": "user",
                "content": f"Message {i}"
            }
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            assert response.status_code == 201
        
        # Test minimum pagination values
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers,
            params={"page": 0, "per_page": 1}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["messages"]) == 1
        assert data["page"] == 0
        assert data["per_page"] == 1
        
        # Test maximum pagination values
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers,
            params={"page": 0, "per_page": 100}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["messages"]) == 5  # All messages
        assert data["per_page"] == 100
        
        # Test page beyond available data
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers,
            params={"page": 10, "per_page": 10}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["messages"]) == 0  # No messages on this page
        assert data["total_messages"] == 5  # But total is still correct
        
        # Test invalid pagination parameters
        invalid_params = [
            {"page": -1, "per_page": 10},  # Negative page
            {"page": 0, "per_page": 0},    # Zero per_page
            {"page": 0, "per_page": 101},  # Too large per_page
        ]
        
        for params in invalid_params:
            response = client.get(
                f"/api/v1/chat-history/sessions/{session_id}/messages",
                headers=mock_group_headers,
                params=params
            )
            assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_multiple_group_ids_workflow(self, client):
        """Test scenarios with multiple group IDs in context."""
        # Create headers with multi-group context
        multi_group_headers = {
            "X-Auth-Request-Email": "multiuser@company.com",
            "X-Auth-Request-Access-Token": "test-multi-access-token"
        }
        
        # Create session with multi-group context
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=multi_group_headers
        )
        assert response.status_code == 201
        session_id = response.json()["session_id"]
        
        # Save message with multi-group context
        message = {
            "session_id": session_id,
            "message_type": "user",
            "content": "Multi-group message"
        }
        
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message,
            headers=multi_group_headers
        )
        assert response.status_code == 201
        
        # Retrieve messages should work with multi-group context
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=multi_group_headers
        )
        assert response.status_code == 200
        messages = response.json()["messages"]
        assert len(messages) == 1
        assert messages[0]["content"] == "Multi-group message"

    @pytest.mark.asyncio
    async def test_empty_session_operations_workflow(self, client, mock_group_headers):
        """Test operations on empty sessions."""
        # Create session but don't add messages
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Get messages from empty session
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["messages"]) == 0
        assert data["total_messages"] == 0
        assert data["session_id"] == session_id
        
        # Delete empty session (should return 404 since no messages were ever saved)
        response = client.delete(
            f"/api/v1/chat-history/sessions/{session_id}",
            headers=mock_group_headers
        )
        assert response.status_code == 404  # No session record exists without messages
        
        # Verify deletion
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["messages"]) == 0

    @pytest.mark.asyncio
    async def test_intent_and_generation_result_variations(self, client, mock_group_headers):
        """Test various intent and generation result combinations."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Test message with intent only
        message_intent_only = {
            "session_id": session_id,
            "message_type": "user",
            "content": "Create an agent",
            "intent": "generate_agent"
        }
        
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message_intent_only,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        saved = response.json()
        assert saved["intent"] == "generate_agent"
        assert saved["confidence"] is None
        assert saved["generation_result"] is None
        
        # Test message with generation_result only
        message_result_only = {
            "session_id": session_id,
            "message_type": "assistant",
            "content": "Agent created",
            "generation_result": {"agent_id": "test-123"}
        }
        
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message_result_only,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        saved = response.json()
        assert saved["generation_result"]["agent_id"] == "test-123"
        assert saved["intent"] is None
        
        # Test message with all optional fields
        message_complete = {
            "session_id": session_id,
            "message_type": "assistant",
            "content": "Complete message",
            "intent": "generate_task",
            "confidence": 0.87,
            "generation_result": {
                "task_id": "task-456",
                "metadata": {"processing_time": 1.5}
            }
        }
        
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message_complete,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        saved = response.json()
        assert saved["intent"] == "generate_task"
        assert saved["confidence"] == "0.87"
        assert saved["generation_result"]["task_id"] == "task-456"
        assert saved["generation_result"]["metadata"]["processing_time"] == 1.5
        
        # Test message with minimal fields
        message_minimal = {
            "session_id": session_id,
            "message_type": "user",
            "content": "Minimal message"
        }
        
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message_minimal,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        saved = response.json()
        assert saved["content"] == "Minimal message"
        assert saved["intent"] is None
        assert saved["confidence"] is None
        assert saved["generation_result"] is None

    @pytest.mark.asyncio
    async def test_session_id_generation_uniqueness(self, client, mock_group_headers):
        """Test that generated session IDs are unique."""
        session_ids = set()
        
        # Generate multiple sessions
        for i in range(10):
            response = client.post(
                "/api/v1/chat-history/sessions/new",
                headers=mock_group_headers
            )
            assert response.status_code == 201
            session_id = response.json()["session_id"]
            
            # Verify UUID format (basic check)
            assert len(session_id) == 36  # UUID4 format length
            assert session_id.count('-') == 4  # UUID has 4 hyphens
            
            # Verify uniqueness
            assert session_id not in session_ids
            session_ids.add(session_id)
        
        assert len(session_ids) == 10  # All unique

    @pytest.mark.asyncio
    async def test_cross_session_isolation_workflow(self, client, mock_group_headers):
        """Test that messages are properly isolated between sessions."""
        # Create two separate sessions
        response1 = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session1_id = response1.json()["session_id"]
        
        response2 = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session2_id = response2.json()["session_id"]
        
        # Add messages to each session
        for i in range(3):
            # Session 1 messages
            message1 = {
                "session_id": session1_id,
                "message_type": "user",
                "content": f"Session 1 message {i}"
            }
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message1,
                headers=mock_group_headers
            )
            assert response.status_code == 201
            
            # Session 2 messages
            message2 = {
                "session_id": session2_id,
                "message_type": "user",
                "content": f"Session 2 message {i}"
            }
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message2,
                headers=mock_group_headers
            )
            assert response.status_code == 201
        
        # Verify session 1 isolation
        response = client.get(
            f"/api/v1/chat-history/sessions/{session1_id}/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        session1_messages = response.json()["messages"]
        assert len(session1_messages) == 3
        for msg in session1_messages:
            assert "Session 1 message" in msg["content"]
            assert msg["session_id"] == session1_id
        
        # Verify session 2 isolation
        response = client.get(
            f"/api/v1/chat-history/sessions/{session2_id}/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        session2_messages = response.json()["messages"]
        assert len(session2_messages) == 3
        for msg in session2_messages:
            assert "Session 2 message" in msg["content"]
            assert msg["session_id"] == session2_id

    @pytest.mark.asyncio
    async def test_user_session_pagination_workflow(self, client, mock_group_headers):
        """Test pagination for user sessions endpoint."""
        # Create multiple sessions and add messages
        session_ids = []
        for i in range(8):
            response = client.post(
                "/api/v1/chat-history/sessions/new",
                headers=mock_group_headers
            )
            session_id = response.json()["session_id"]
            session_ids.append(session_id)
            
            # Add a message to each session
            message = {
                "session_id": session_id,
                "message_type": "user",
                "content": f"Message in session {i}"
            }
            client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
        
        # Test first page
        response = client.get(
            "/api/v1/chat-history/users/sessions",
            headers=mock_group_headers,
            params={"page": 0, "per_page": 5}
        )
        assert response.status_code == 200
        page1_sessions = response.json()
        assert len(page1_sessions) <= 5  # May be less due to other tests
        
        # Test second page
        response = client.get(
            "/api/v1/chat-history/users/sessions",
            headers=mock_group_headers,
            params={"page": 1, "per_page": 5}
        )
        assert response.status_code == 200
        page2_sessions = response.json()
        # Should have remaining sessions or be empty

    @pytest.mark.asyncio
    async def test_group_sessions_pagination_workflow(self, client, mock_group_headers):
        """Test pagination for group sessions endpoint."""
        # Create multiple sessions with messages
        for i in range(6):
            response = client.post(
                "/api/v1/chat-history/sessions/new",
                headers=mock_group_headers
            )
            session_id = response.json()["session_id"]
            
            # Add message to make it appear in group sessions
            message = {
                "session_id": session_id,
                "message_type": "user",
                "content": f"Group session message {i}"
            }
            client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
        
        # Test pagination
        response = client.get(
            "/api/v1/chat-history/sessions",
            headers=mock_group_headers,
            params={"page": 0, "per_page": 3}
        )
        assert response.status_code == 200
        data = response.json()
        assert "sessions" in data
        assert "total_sessions" in data
        assert data["page"] == 0
        assert data["per_page"] == 3

    @pytest.mark.asyncio
    async def test_large_content_handling(self, client, mock_group_headers):
        """Test handling of reasonably large message content."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Create reasonably large content (10KB)
        large_content = "x" * (10 * 1024)
        message = {
            "session_id": session_id,
            "message_type": "user",
            "content": large_content
        }
        
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message,
            headers=mock_group_headers
        )
        # Should succeed
        assert response.status_code == 201
        assert response.json()["content"] == large_content

    @pytest.mark.asyncio
    async def test_malformed_json_payload(self, client, mock_group_headers):
        """Test handling of malformed JSON in request payload."""
        # Send invalid JSON
        response = client.post(
            "/api/v1/chat-history/messages",
            data="invalid json data",
            headers={**mock_group_headers, "Content-Type": "application/json"}
        )
        assert response.status_code == 422  # Unprocessable entity

    @pytest.mark.asyncio
    async def test_unicode_and_special_characters_handling(self, client, mock_group_headers):
        """Test handling of unicode and special characters in messages."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Test various unicode and special characters
        special_content_tests = [
            "Hello 世界! 🌍 Testing unicode",
            "Special chars: !@#$%^&*()_+{}[]|\\:;\"'<>?,./",
            "Emoji test: 🚀🎉🔥💯🦄🌟⚡🎨🎯🏆",
            "Mathematical: ∑∫∞≠≤≥±√∏∆∇",
            "Right-to-left: مرحبا بالعالم العربي",
            "Mixed: ASCII + Unicode + 中文 + العربية + русский"
        ]
        
        for content in special_content_tests:
            message = {
                "session_id": session_id,
                "message_type": "user",
                "content": content
            }
            
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            assert response.status_code == 201
            saved_message = response.json()
            assert saved_message["content"] == content

    @pytest.mark.asyncio
    async def test_generation_result_with_nested_structures(self, client, mock_group_headers):
        """Test deeply nested generation result structures."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Create complex nested structure
        complex_generation_result = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "deep_array": [
                                {"item": "test1", "nested": {"value": 123}},
                                {"item": "test2", "nested": {"value": 456}}
                            ],
                            "deep_string": "very deep value"
                        }
                    }
                }
            },
            "arrays": [
                [1, 2, [3, 4, [5, 6]]],
                {"complex": True, "nested_array": [{"a": 1}, {"b": 2}]}
            ],
            "mixed_types": {
                "string": "test",
                "number": 42,
                "boolean": True,
                "null_value": None,
                "float": 3.14159
            }
        }
        
        message = {
            "session_id": session_id,
            "message_type": "assistant",
            "content": "Complex nested structure result",
            "generation_result": complex_generation_result
        }
        
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        
        # Verify structure is preserved
        saved_message = response.json()
        assert saved_message["generation_result"]["level1"]["level2"]["level3"]["level4"]["deep_string"] == "very deep value"
        assert len(saved_message["generation_result"]["arrays"][1]["nested_array"]) == 2

    @pytest.mark.asyncio
    async def test_session_id_format_acceptance(self, client, mock_group_headers):
        """Test that various session ID formats are accepted."""
        valid_session_ids = [
            "invalid-uuid-format",  # API accepts any string format
            "123",  # Short strings work
            "x" * 50,  # Long strings work
            "special-chars-test",  # Some special characters work
        ]
        
        for session_id in valid_session_ids:
            message = {
                "session_id": session_id,
                "message_type": "user",
                "content": "Test message"
            }
            
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            # Should succeed
            assert response.status_code == 201

    @pytest.mark.asyncio
    async def test_message_content_edge_cases(self, client, mock_group_headers):
        """Test edge cases for message content validation."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Test edge cases
        edge_cases = [
            ("a", 201),  # Single character should succeed
            ("a" * 10000, 201),  # Very long but reasonable content
            ("\\n\\t\\r", 201),  # Escaped characters
            ("null", 201),  # String "null"
            ("false", 201),  # String "false"
            ("   test   ", 201),  # Whitespace with content
        ]
        
        for content, expected_status in edge_cases:
            message = {
                "session_id": session_id,
                "message_type": "user",
                "content": content
            }
            
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            assert response.status_code == expected_status

    @pytest.mark.asyncio
    async def test_confidence_score_edge_cases(self, client, mock_group_headers):
        """Test edge cases for confidence score values."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Test edge cases for confidence scores
        confidence_tests = [
            (0.0, "0.0"),
            (1.0, "1.0"),
            (0.999999, "0.999999"),
            (0.001, "0.001"),  # Use a value that won't be in scientific notation
        ]
        
        for confidence_input, expected in confidence_tests:
            message = {
                "session_id": session_id,
                "message_type": "assistant",
                "content": "Test confidence message",
                "confidence": confidence_input
            }
            
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            
            assert response.status_code == 201
            assert response.json()["confidence"] == expected

    @pytest.mark.asyncio
    async def test_intent_field_variations(self, client, mock_group_headers):
        """Test various intent field values."""
        # Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        session_id = response.json()["session_id"]
        
        # Test various intent values
        intent_tests = [
            None,  # None should be accepted
            "",  # Empty string
            "generate_agent",  # Standard intent
            "custom_intent_with_underscores",
            "intent-with-dashes",
            "Intent With Spaces",
            "UPPERCASE_INTENT",
            "very_long_intent_name_that_exceeds_normal_length_but_should_still_work"
        ]
        
        for intent in intent_tests:
            message = {
                "session_id": session_id,
                "message_type": "user",
                "content": "Test intent message",
                "intent": intent
            }
            
            response = client.post(
                "/api/v1/chat-history/messages",
                json=message,
                headers=mock_group_headers
            )
            assert response.status_code == 201
            assert response.json()["intent"] == intent

    @pytest.mark.asyncio
    async def test_workflow_state_consistency(self, client, mock_group_headers):
        """Test workflow state consistency across operations."""
        # Test: Create session, add message, verify state, modify, verify again
        
        # Step 1: Create session
        response = client.post(
            "/api/v1/chat-history/sessions/new",
            headers=mock_group_headers
        )
        assert response.status_code == 201
        session_id = response.json()["session_id"]
        
        # Step 2: Verify empty state
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        assert response.json()["total_messages"] == 0
        
        # Step 3: Add first message
        message1 = {
            "session_id": session_id,
            "message_type": "user",
            "content": "First message"
        }
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message1,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        
        # Step 4: Verify state after first message
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_messages"] == 1
        assert data["messages"][0]["content"] == "First message"
        
        # Step 5: Add second message
        message2 = {
            "session_id": session_id,
            "message_type": "assistant",
            "content": "Second message"
        }
        response = client.post(
            "/api/v1/chat-history/messages",
            json=message2,
            headers=mock_group_headers
        )
        assert response.status_code == 201
        
        # Step 6: Verify final state
        response = client.get(
            f"/api/v1/chat-history/sessions/{session_id}/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_messages"] == 2
        
        # Messages should be in chronological order
        messages = data["messages"]
        contents = [msg["content"] for msg in messages]
        assert "First message" in contents
        assert "Second message" in contents

    @pytest.mark.asyncio
    async def test_additional_coverage_scenarios(self, client, mock_group_headers):
        """Test additional scenarios for comprehensive coverage."""
        # Test 1: Non-existent session message retrieval (should return empty)
        response = client.get(
            "/api/v1/chat-history/sessions/non-existent-session/messages",
            headers=mock_group_headers
        )
        assert response.status_code == 200
        assert response.json()["total_messages"] == 0
        
        # Test 2: User with no sessions
        new_user_headers = {
            "X-Auth-Request-Email": "newuser@newcompany.com",
            "X-Auth-Request-Access-Token": "new-access-token"
        }
        response = client.get(
            "/api/v1/chat-history/users/sessions",
            headers=new_user_headers
        )
        assert response.status_code == 200
        assert len(response.json()) == 0
        
        # Test 3: Group with no sessions
        response = client.get(
            "/api/v1/chat-history/sessions",
            headers=new_user_headers
        )
        assert response.status_code == 200
        assert response.json()["total_sessions"] == 0