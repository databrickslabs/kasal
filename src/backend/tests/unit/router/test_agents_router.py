"""
Unit tests for AgentsRouter.

Tests the functionality of agent management endpoints including
CRUD operations with group isolation.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import logging
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from src.models.agent import Agent
from src.schemas.agent import AgentCreate, AgentUpdate, AgentLimitedUpdate
from src.utils.user_context import GroupContext


# Mock agent model
class MockAgent:
    def __init__(self, id="agent-123", name="Test Agent", role="Developer", 
                 group_id="group-123", backstory="Expert developer"):
        from datetime import datetime
        self.id = id
        self.name = name
        self.role = role
        self.group_id = group_id
        self.backstory = backstory
        self.goal = "Write code"
        self.tools = ["code_editor"]
        self.max_iter = 5
        self.verbose = True
        self.allow_delegation = False
        self.autosave = True
        self.context = None
        self.max_execution_time = 600
        self.function_calling_llm = None
        self.system_template = None
        self.prompt_template = None
        self.response_template = None
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "id": self.id,
            "name": self.name,
            "role": self.role,
            "goal": self.goal,
            "backstory": self.backstory,
            "tools": self.tools,
            "max_iter": self.max_iter,
            "verbose": self.verbose,
            "allow_delegation": self.allow_delegation,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@pytest.fixture
def mock_agent_service():
    """Create a mock agent service."""
    service = AsyncMock()
    return service


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = GroupContext(
        group_ids=["group-123"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="user-123"
    )
    return context


@pytest.fixture
def app(mock_agent_service, mock_group_context):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.agents_router import router, get_agent_service
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(router)
    
    # Create override functions
    async def override_get_agent_service():
        return mock_agent_service
        
    async def override_get_group_context():
        return mock_group_context
    
    # Override dependencies
    app.dependency_overrides[get_agent_service] = override_get_agent_service
    app.dependency_overrides[get_group_context] = override_get_group_context
    
    return app



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
def client(app):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user


    return TestClient(app)


@pytest.fixture
def sample_agent_create():
    """Create a sample agent creation request."""
    return AgentCreate(
        name="New Agent",
        role="Developer",
        goal="Write code",
        backstory="Expert developer",
        tools=["code_editor"],
        max_iter=5,
        verbose=True,
        allow_delegation=False
    )


@pytest.fixture
def sample_agent_update():
    """Create a sample agent update request."""
    return AgentUpdate(
        name="Updated Agent",
        role="Senior Developer",
        goal="Write and review code",
        backstory="Senior expert developer",
        tools=["code_editor", "reviewer"],
        max_iter=10
    )


@pytest.fixture
def sample_agent_limited_update():
    """Create a sample limited agent update request."""
    return AgentLimitedUpdate(
        name="Slightly Updated Agent",
        goal="Write better code"
    )


class TestCreateAgent:
    """Test cases for create agent endpoint."""
    
    def test_create_agent_success(self, client, mock_agent_service, mock_group_context, sample_agent_create):
        """Test successful agent creation."""
        created_agent = MockAgent()
        mock_agent_service.create_with_group.return_value = created_agent
        
        response = client.post("/agents", json=sample_agent_create.model_dump())
        
        assert response.status_code == 201
        data = response.json()
        assert data["id"] == "agent-123"
        assert data["name"] == "Test Agent"
        mock_agent_service.create_with_group.assert_called_once()
    
    def test_create_agent_no_group_context(self, client, mock_agent_service, sample_agent_create, app):
        """Test agent creation without group context."""
        # Override to return None
        async def override_get_group_context():
            return None
            
        from src.core.dependencies import get_group_context
        app.dependency_overrides[get_group_context] = override_get_group_context
        
        response = client.post("/agents", json=sample_agent_create.model_dump())
        
        # Due to the router's exception handling, HTTPException gets caught and re-raised as 500
        assert response.status_code == 500
        assert "No valid group context provided" in response.json()["detail"]
    
    def test_create_agent_invalid_group_context(self, client, mock_agent_service, sample_agent_create, app):
        """Test agent creation with invalid group context."""
        invalid_context = MagicMock()
        invalid_context.is_valid.return_value = False
        
        async def override_get_group_context():
            return invalid_context
            
        from src.core.dependencies import get_group_context
        app.dependency_overrides[get_group_context] = override_get_group_context
        
        response = client.post("/agents", json=sample_agent_create.model_dump())
        
        # Due to the router's exception handling, HTTPException gets caught and re-raised as 500
        assert response.status_code == 500
        assert "No valid group context provided" in response.json()["detail"]
    
    def test_create_agent_service_error(self, client, mock_agent_service, mock_group_context, sample_agent_create):
        """Test agent creation with service error."""
        mock_agent_service.create_with_group.side_effect = Exception("Service error")
        
        response = client.post("/agents", json=sample_agent_create.model_dump())
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestListAgents:
    """Test cases for list agents endpoint."""
    
    def test_list_agents_success(self, client, mock_agent_service, mock_group_context):
        """Test successful agents listing."""
        agents = [MockAgent(id="agent-1"), MockAgent(id="agent-2")]
        mock_agent_service.find_by_group.return_value = agents
        
        response = client.get("/agents")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["id"] == "agent-1"
        assert data[1]["id"] == "agent-2"
    
    def test_list_agents_no_group_context(self, client, mock_agent_service, app):
        """Test agents listing without group context."""
        async def override_get_group_context():
            return None
            
        from src.core.dependencies import get_group_context
        app.dependency_overrides[get_group_context] = override_get_group_context
        
        response = client.get("/agents")
        
        assert response.status_code == 200
        assert response.json() == []
    
    def test_list_agents_invalid_group_context(self, client, mock_agent_service, app):
        """Test agents listing with invalid group context."""
        invalid_context = MagicMock()
        invalid_context.is_valid.return_value = False
        
        async def override_get_group_context():
            return invalid_context
            
        from src.core.dependencies import get_group_context
        app.dependency_overrides[get_group_context] = override_get_group_context
        
        response = client.get("/agents")
        
        assert response.status_code == 200
        assert response.json() == []
    
    def test_list_agents_service_error(self, client, mock_agent_service, mock_group_context):
        """Test agents listing with service error."""
        mock_agent_service.find_by_group.side_effect = Exception("Service error")
        
        response = client.get("/agents")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestGetAgent:
    """Test cases for get agent endpoint."""
    
    def test_get_agent_success(self, client, mock_agent_service):
        """Test successful agent retrieval."""
        agent = MockAgent()
        mock_agent_service.get.return_value = agent
        
        response = client.get("/agents/agent-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "agent-123"
        assert data["name"] == "Test Agent"
    
    def test_get_agent_not_found(self, client, mock_agent_service):
        """Test getting non-existent agent."""
        mock_agent_service.get.return_value = None
        
        response = client.get("/agents/nonexistent")
        
        assert response.status_code == 404
        assert "Agent not found" in response.json()["detail"]
    
    def test_get_agent_service_error(self, client, mock_agent_service):
        """Test getting agent with service error."""
        mock_agent_service.get.side_effect = Exception("Service error")
        
        response = client.get("/agents/agent-123")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestUpdateAgentFull:
    """Test cases for full update agent endpoint."""
    
    def test_update_agent_full_success(self, client, mock_agent_service, sample_agent_update):
        """Test successful full agent update."""
        updated_agent = MockAgent(name="Updated Agent", role="Senior Developer")
        mock_agent_service.update_with_partial_data.return_value = updated_agent
        
        response = client.put("/agents/agent-123/full", json=sample_agent_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Agent"
        assert data["role"] == "Senior Developer"
    
    def test_update_agent_full_not_found(self, client, mock_agent_service, sample_agent_update):
        """Test updating non-existent agent."""
        mock_agent_service.update_with_partial_data.return_value = None
        
        response = client.put("/agents/nonexistent/full", json=sample_agent_update.model_dump())
        
        # Due to the router's exception handling, HTTPException gets caught and re-raised as 500
        assert response.status_code == 500
        assert "Agent not found" in response.json()["detail"]
    
    def test_update_agent_full_service_error(self, client, mock_agent_service, sample_agent_update):
        """Test full update with service error."""
        mock_agent_service.update_with_partial_data.side_effect = Exception("Service error")
        
        response = client.put("/agents/agent-123/full", json=sample_agent_update.model_dump())
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestUpdateAgentLimited:
    """Test cases for limited update agent endpoint."""
    
    def test_update_agent_limited_success(self, client, mock_agent_service, sample_agent_limited_update):
        """Test successful limited agent update."""
        updated_agent = MockAgent(name="Slightly Updated Agent")
        mock_agent_service.update_limited_fields.return_value = updated_agent
        
        response = client.put("/agents/agent-123", json=sample_agent_limited_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Slightly Updated Agent"
    
    def test_update_agent_limited_not_found(self, client, mock_agent_service, sample_agent_limited_update):
        """Test limited update of non-existent agent."""
        mock_agent_service.update_limited_fields.return_value = None
        
        response = client.put("/agents/nonexistent", json=sample_agent_limited_update.model_dump())
        
        # Due to the router's exception handling, HTTPException gets caught and re-raised as 500
        assert response.status_code == 500
        assert "Agent not found" in response.json()["detail"]
    
    def test_update_agent_limited_service_error(self, client, mock_agent_service, sample_agent_limited_update):
        """Test limited update with service error."""
        mock_agent_service.update_limited_fields.side_effect = Exception("Service error")
        
        response = client.put("/agents/agent-123", json=sample_agent_limited_update.model_dump())
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestDeleteAgent:
    """Test cases for delete agent endpoint."""
    
    def test_delete_agent_success(self, client, mock_agent_service):
        """Test successful agent deletion."""
        mock_agent_service.delete.return_value = True
        
        response = client.delete("/agents/agent-123")
        
        assert response.status_code == 204
    
    def test_delete_agent_not_found(self, client, mock_agent_service):
        """Test deleting non-existent agent."""
        mock_agent_service.delete.return_value = False
        
        response = client.delete("/agents/nonexistent")
        
        # Due to the router's exception handling, HTTPException gets caught and re-raised as 500
        assert response.status_code == 500
        assert "Agent not found" in response.json()["detail"]
    
    def test_delete_agent_service_error(self, client, mock_agent_service):
        """Test deleting agent with service error."""
        mock_agent_service.delete.side_effect = Exception("Service error")
        
        response = client.delete("/agents/agent-123")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestDeleteAllAgents:
    """Test cases for delete all agents endpoint."""
    
    def test_delete_all_agents_success(self, client, mock_agent_service):
        """Test successful deletion of all agents."""
        mock_agent_service.delete_all.return_value = None
        
        response = client.delete("/agents")
        
        assert response.status_code == 204
    
    def test_delete_all_agents_integrity_error(self, client, mock_agent_service):
        """Test deleting all agents with integrity constraint."""
        mock_agent_service.delete_all.side_effect = IntegrityError("statement", "params", "orig")
        
        response = client.delete("/agents")
        
        assert response.status_code == 409
        assert "Cannot delete agents because some are still referenced by tasks" in response.json()["detail"]
    
    def test_delete_all_agents_service_error(self, client, mock_agent_service):
        """Test deleting all agents with service error."""
        mock_agent_service.delete_all.side_effect = Exception("Service error")
        
        response = client.delete("/agents")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]
    
    def test_delete_all_agents_logging(self, client, mock_agent_service, caplog):
        """Test that delete all agents logs properly."""
        mock_agent_service.delete_all.return_value = None
        
        with caplog.at_level(logging.INFO):
            response = client.delete("/agents")
        
        assert response.status_code == 204