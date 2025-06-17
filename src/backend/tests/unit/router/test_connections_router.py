"""
Unit tests for ConnectionsRouter.

Tests the functionality of connections endpoints including
generating connections between agents/tasks and testing API keys.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import logging
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.schemas.connection import (
    ConnectionRequest, ConnectionResponse, ApiKeyTestResponse,
    Agent, Task, TaskContext, AgentAssignment, TaskAssignment, Dependency,
    ApiKeyTestResult, PythonInfo
)


@pytest.fixture
def mock_connection_service():
    """Create a mock connection service."""
    service = AsyncMock()
    return service



@pytest.fixture
def mock_current_user():
    """Create a mock authenticated user."""
    from src.models.enums import UserRole, UserStatus
    from datetime import datetime
    
    class MockUser:
        def __init__(self):
            self.id = "current-user-123"
            self.username = "testuser"
            self.email = "test@example.com"
            self.role = UserRole.REGULAR
            self.status = UserStatus.ACTIVE
            self.created_at = datetime.utcnow()
            self.updated_at = datetime.utcnow()
    
    return MockUser()


@pytest.fixture
def client():
    """Create a test client."""
    from fastapi import FastAPI
    from src.api.connections_router import router
    
    app = FastAPI()
    app.include_router(router)
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user


    
    return TestClient(app)


@pytest.fixture
def sample_connection_request():
    """Create a sample connection request."""
    agents = [
        Agent(
            name="Agent 1",
            role="Developer",
            goal="Write code",
            backstory="Experienced developer",
            tools=["code_editor"]
        ),
        Agent(
            name="Agent 2",
            role="Tester",
            goal="Test code",
            backstory="QA specialist",
            tools=["test_runner"]
        )
    ]
    
    tasks = [
        Task(
            name="Task 1",
            description="Write feature code",
            expected_output="Completed code",
            tools=["code_editor"],
            context=TaskContext(
                type="development",
                priority="high",
                complexity="medium",
                required_skills=["python", "javascript"]
            )
        ),
        Task(
            name="Task 2",
            description="Test the feature",
            expected_output="Test report",
            tools=["test_runner"],
            context=TaskContext(
                type="testing",
                priority="high",
                complexity="low",
                required_skills=["testing", "automation"]
            )
        )
    ]
    
    return ConnectionRequest(agents=agents, tasks=tasks)


class TestGenerateConnections:
    """Test cases for generate connections endpoint."""
    
    def test_generate_connections_success(self, client, mock_connection_service, sample_connection_request):
        """Test successful connection generation."""
        # Mock response
        mock_response = ConnectionResponse(
            assignments=[
                AgentAssignment(
                    agent_name="Agent 1",
                    tasks=[
                        TaskAssignment(task_name="Task 1", reasoning="Developer agent best suited for coding")
                    ]
                ),
                AgentAssignment(
                    agent_name="Agent 2",
                    tasks=[
                        TaskAssignment(task_name="Task 2", reasoning="Tester agent best suited for testing")
                    ]
                )
            ],
            dependencies=[
                Dependency(
                    task_name="Task 2",
                    depends_on=["Task 1"],
                    reasoning="Testing must happen after code is written"
                )
            ]
        )
        
        mock_connection_service.generate_connections.return_value = mock_response
        
        with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
            response = client.post("/connections/generate-connections", json=sample_connection_request.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["assignments"]) == 2
        assert len(data["dependencies"]) == 1
        assert data["assignments"][0]["agent_name"] == "Agent 1"
        assert data["assignments"][0]["tasks"][0]["task_name"] == "Task 1"
    
    def test_generate_connections_validation_error(self, client, mock_connection_service, sample_connection_request):
        """Test connection generation with validation error."""
        mock_connection_service.generate_connections.side_effect = ValueError("Invalid agent configuration")
        
        with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
            response = client.post("/connections/generate-connections", json=sample_connection_request.model_dump())
        
        assert response.status_code == 400
        assert "Invalid agent configuration" in response.json()["detail"]
    
    def test_generate_connections_server_error(self, client, mock_connection_service, sample_connection_request):
        """Test connection generation with server error."""
        mock_connection_service.generate_connections.side_effect = Exception("Internal server error")
        
        with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
            response = client.post("/connections/generate-connections", json=sample_connection_request.model_dump())
        
        assert response.status_code == 500
        assert "Error generating connections" in response.json()["detail"]
    
    def test_generate_connections_empty_agents(self, client, mock_connection_service):
        """Test connection generation with empty agents list."""
        request_data = {
            "agents": [],
            "tasks": [
                {
                    "name": "Task 1",
                    "description": "Test task",
                    "expected_output": "Output",
                    "tools": [],
                    "context": {
                        "type": "general",
                        "priority": "medium",
                        "complexity": "medium",
                        "required_skills": []
                    }
                }
            ]
        }
        
        mock_response = ConnectionResponse(assignments=[], dependencies=[])
        mock_connection_service.generate_connections.return_value = mock_response
        
        with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
            response = client.post("/connections/generate-connections", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["assignments"]) == 0
        assert len(data["dependencies"]) == 0
    
    def test_generate_connections_empty_tasks(self, client, mock_connection_service):
        """Test connection generation with empty tasks list."""
        request_data = {
            "agents": [
                {
                    "name": "Agent 1",
                    "role": "Developer",
                    "goal": "Write code",
                    "backstory": "Developer",
                    "tools": []
                }
            ],
            "tasks": []
        }
        
        mock_response = ConnectionResponse(assignments=[], dependencies=[])
        mock_connection_service.generate_connections.return_value = mock_response
        
        with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
            response = client.post("/connections/generate-connections", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["assignments"]) == 0
        assert len(data["dependencies"]) == 0
    
    def test_generate_connections_logging(self, client, mock_connection_service, sample_connection_request, caplog):
        """Test that connection generation logs properly."""
        mock_response = ConnectionResponse(assignments=[], dependencies=[])
        mock_connection_service.generate_connections.return_value = mock_response
        
        with caplog.at_level(logging.INFO):
            with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
                response = client.post("/connections/generate-connections", json=sample_connection_request.model_dump())
        
        assert response.status_code == 200
        assert "Generating connections for 2 agents and 2 tasks" in caplog.text
        assert "Generated 0 assignments and 0 dependencies" in caplog.text


class TestApiKeyEndpoint:
    """Test cases for API key testing endpoint."""
    
    def test_test_api_key_success(self, client, mock_connection_service):
        """Test successful API key testing."""
        mock_response = ApiKeyTestResponse(
            openai=ApiKeyTestResult(has_key=True, valid=True, message="Valid", key_prefix="sk-"),
            anthropic=ApiKeyTestResult(has_key=True, valid=True, message="Valid", key_prefix="sk-ant-"),
            deepseek=ApiKeyTestResult(has_key=False, valid=False, message="API key not configured"),
            python_info=PythonInfo(version="3.9.0", executable="/usr/bin/python", platform="linux")
        )
        
        mock_connection_service.test_api_keys.return_value = mock_response
        
        with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
            response = client.get("/connections/test-api-key")
        
        assert response.status_code == 200
        data = response.json()
        assert data["openai"]["has_key"] is True
        assert data["openai"]["valid"] is True
        assert data["anthropic"]["has_key"] is True
        assert data["anthropic"]["valid"] is True
        assert data["deepseek"]["has_key"] is False
        assert data["python_info"]["version"] == "3.9.0"
    
    def test_test_api_key_all_invalid(self, client, mock_connection_service):
        """Test API key testing when all keys are invalid."""
        mock_response = ApiKeyTestResponse(
            openai=ApiKeyTestResult(has_key=True, valid=False, message="Invalid API key"),
            anthropic=ApiKeyTestResult(has_key=True, valid=False, message="Invalid API key"),
            deepseek=ApiKeyTestResult(has_key=False, valid=False, message="API key not configured"),
            python_info=PythonInfo(version="3.9.0", executable="/usr/bin/python", platform="linux")
        )
        
        mock_connection_service.test_api_keys.return_value = mock_response
        
        with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
            response = client.get("/connections/test-api-key")
        
        assert response.status_code == 200
        data = response.json()
        assert data["openai"]["has_key"] is True
        assert data["openai"]["valid"] is False
        assert data["anthropic"]["has_key"] is True
        assert data["anthropic"]["valid"] is False
        assert data["deepseek"]["has_key"] is False
    
    def test_test_api_key_server_error(self, client, mock_connection_service):
        """Test API key testing with server error."""
        mock_connection_service.test_api_keys.side_effect = Exception("Service unavailable")
        
        with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
            response = client.get("/connections/test-api-key")
        
        assert response.status_code == 500
        assert "Error testing API keys" in response.json()["detail"]
    
    def test_test_api_key_logging(self, client, mock_connection_service, caplog):
        """Test that API key testing logs properly."""
        mock_response = ApiKeyTestResponse(
            openai=ApiKeyTestResult(has_key=True, valid=True, message="Valid"),
            anthropic=ApiKeyTestResult(has_key=True, valid=False, message="Invalid"),
            deepseek=ApiKeyTestResult(has_key=False, valid=False, message="Not configured"),
            python_info=PythonInfo(version="3.9.0", executable="/usr/bin/python", platform="linux")
        )
        
        mock_connection_service.test_api_keys.return_value = mock_response
        
        with caplog.at_level(logging.INFO):
            with patch('src.api.connections_router.ConnectionService', return_value=mock_connection_service):
                response = client.get("/connections/test-api-key")
        
        assert response.status_code == 200
        assert "Testing API keys" in caplog.text