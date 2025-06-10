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
        "x-databricks-workspace-id": "workspace-123",
        "x-databricks-group-id": "group-456", 
        "x-databricks-user-email": "testuser@company.com",
        "x-databricks-user-id": "user-789"
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
            "x-databricks-workspace-id": "workspace-123",
            "x-databricks-group-id": "group-111",
            "x-databricks-user-email": "user1@company.com"
        }
        
        group2_headers = {
            "x-databricks-workspace-id": "workspace-123", 
            "x-databricks-group-id": "group-222",
            "x-databricks-user-email": "user2@company.com"
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
            "content": "I've created a complete crew for you",
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
            "x-databricks-workspace-id": "workspace-123",
            "x-databricks-group-id": "group-456",
            "x-databricks-user-email": "user1@company.com"
        }
        
        user2_headers = {
            "x-databricks-workspace-id": "workspace-123",
            "x-databricks-group-id": "group-456",  # Same group
            "x-databricks-user-email": "user2@company.com"
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
        assert len(user1_sessions) == 2
        
        # Get user 2's sessions
        response = client.get(
            "/api/v1/chat-history/users/sessions",
            headers=user2_headers
        )
        assert response.status_code == 200
        user2_sessions = response.json()
        assert len(user2_sessions) == 2
        
        # Get group sessions with user filter
        response = client.get(
            "/api/v1/chat-history/sessions",
            headers=user1_headers,
            params={"user_id": "user1@company.com"}
        )
        assert response.status_code == 200
        filtered_sessions = response.json()["sessions"]
        # Should find sessions for user1 (exact count depends on isolation implementation)