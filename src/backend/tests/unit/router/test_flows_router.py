"""
Unit tests for FlowsRouter.

Tests the functionality of flow management endpoints including
CRUD operations, validation, and force deletion with executions.
"""
import pytest
from unittest.mock import AsyncMock
from datetime import datetime
import uuid
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.flow import FlowCreate, FlowUpdate


# Mock flow model
class MockFlow:
    def __init__(self, id=None, name="Test Flow", crew_id=None, 
                 nodes=None, edges=None, flow_config=None):
        self.id = id or uuid.uuid4()
        self.name = name
        self.crew_id = crew_id
        self.nodes = nodes or []
        self.edges = edges or []
        self.flow_config = flow_config or {}
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "id": str(self.id),
            "name": self.name,
            "crew_id": self.crew_id,
            "nodes": self.nodes,
            "edges": self.edges,
            "flow_config": self.flow_config,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@pytest.fixture
def mock_flow_service():
    """Create a mock flow service."""
    service = AsyncMock()
    return service


@pytest.fixture
def mock_db_session():
    """Create a mock database session."""
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def app(mock_flow_service, mock_db_session):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.flows_router import router, get_flow_service
    from src.core.dependencies import get_db
    
    app = FastAPI()
    app.include_router(router)
    
    # Create override functions - get_flow_service doesn't need session parameter when overridden
    def override_get_flow_service():
        return mock_flow_service
        
    async def override_get_db():
        return mock_db_session
    
    # Override dependencies
    app.dependency_overrides[get_flow_service] = override_get_flow_service
    app.dependency_overrides[get_db] = override_get_db
    
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
def sample_flow_create():
    """Create a sample flow creation request."""
    return FlowCreate(
        name="Test Flow",
        crew_id=uuid.uuid4(),
        nodes=[
            {
                "id": "node1",
                "type": "crew",
                "position": {"x": 100, "y": 100},
                "data": {"label": "Test Node"}
            }
        ],
        edges=[
            {
                "id": "edge1",
                "source": "node1",
                "target": "node2"
            }
        ],
        flow_config={"setting1": "value1"}
    )


@pytest.fixture
def sample_flow_update():
    """Create a sample flow update request."""
    return FlowUpdate(
        name="Updated Flow",
        flow_config={"setting2": "value2"}
    )


class TestGetAllFlows:
    """Test cases for get all flows endpoint."""
    
    def test_get_all_flows_success(self, client, mock_flow_service):
        """Test successful flows listing."""
        flows = [
            MockFlow(name="Flow 1"),
            MockFlow(name="Flow 2")
        ]
        mock_flow_service.get_all_flows.return_value = flows
        
        response = client.get("/flows")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["name"] == "Flow 1"
        assert data[1]["name"] == "Flow 2"
        mock_flow_service.get_all_flows.assert_called_once()
    
    def test_get_all_flows_empty(self, client, mock_flow_service):
        """Test getting flows when none exist."""
        mock_flow_service.get_all_flows.return_value = []
        
        response = client.get("/flows")
        
        assert response.status_code == 200
        assert response.json() == []
    
    def test_get_all_flows_service_error(self, client, mock_flow_service):
        """Test getting flows with service error."""
        mock_flow_service.get_all_flows.side_effect = Exception("Database error")
        
        response = client.get("/flows")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestGetFlow:
    """Test cases for get flow endpoint."""
    
    def test_get_flow_success(self, client, mock_flow_service):
        """Test successful flow retrieval."""
        flow_id = uuid.uuid4()
        flow = MockFlow(id=flow_id, name="Test Flow")
        mock_flow_service.get_flow.return_value = flow
        
        response = client.get(f"/flows/{flow_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == str(flow_id)
        assert data["name"] == "Test Flow"
        mock_flow_service.get_flow.assert_called_once_with(flow_id)
    
    def test_get_flow_not_found(self, client, mock_flow_service):
        """Test getting non-existent flow."""
        flow_id = uuid.uuid4()
        mock_flow_service.get_flow.side_effect = HTTPException(status_code=404, detail="Flow not found")
        
        response = client.get(f"/flows/{flow_id}")
        
        assert response.status_code == 404
        assert "Flow not found" in response.json()["detail"]
    
    def test_get_flow_invalid_uuid(self, client, mock_flow_service):
        """Test getting flow with invalid UUID."""
        response = client.get("/flows/invalid-uuid")
        
        assert response.status_code == 422  # Validation error
    
    def test_get_flow_service_error(self, client, mock_flow_service):
        """Test getting flow with service error."""
        flow_id = uuid.uuid4()
        mock_flow_service.get_flow.side_effect = Exception("Database error")
        
        response = client.get(f"/flows/{flow_id}")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestCreateFlow:
    """Test cases for create flow endpoint."""
    
    def test_create_flow_success(self, client, mock_flow_service, sample_flow_create):
        """Test successful flow creation."""
        created_flow = MockFlow(name="Test Flow")
        mock_flow_service.create_flow.return_value = created_flow
        
        response = client.post("/flows", json=sample_flow_create.model_dump(mode='json'))
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Test Flow"
        mock_flow_service.create_flow.assert_called_once_with(sample_flow_create)
    
    def test_create_flow_service_error(self, client, mock_flow_service, sample_flow_create):
        """Test flow creation with service error."""
        mock_flow_service.create_flow.side_effect = Exception("Creation error")
        
        response = client.post("/flows", json=sample_flow_create.model_dump(mode='json'))
        
        assert response.status_code == 500
        assert "Creation error" in response.json()["detail"]


class TestDebugFlowData:
    """Test cases for debug flow data endpoint."""
    
    def test_debug_flow_data_success(self, client, mock_flow_service, sample_flow_create):
        """Test successful flow data validation."""
        validation_result = {"valid": True, "issues": []}
        mock_flow_service.validate_flow_data.return_value = validation_result
        
        response = client.post("/flows/debug", json=sample_flow_create.model_dump(mode='json'))
        
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        mock_flow_service.validate_flow_data.assert_called_once_with(sample_flow_create)
    
    def test_debug_flow_data_with_issues(self, client, mock_flow_service, sample_flow_create):
        """Test flow data validation with issues."""
        validation_result = {"valid": False, "issues": ["Missing required field"]}
        mock_flow_service.validate_flow_data.return_value = validation_result
        
        response = client.post("/flows/debug", json=sample_flow_create.model_dump(mode='json'))
        
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is False
        assert len(data["issues"]) == 1


class TestUpdateFlow:
    """Test cases for update flow endpoint."""
    
    def test_update_flow_success(self, client, mock_flow_service, sample_flow_update):
        """Test successful flow update."""
        flow_id = uuid.uuid4()
        updated_flow = MockFlow(id=flow_id, name="Updated Flow")
        mock_flow_service.update_flow.return_value = updated_flow
        
        response = client.put(f"/flows/{flow_id}", json=sample_flow_update.model_dump(mode='json'))
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Flow"
        mock_flow_service.update_flow.assert_called_once_with(flow_id, sample_flow_update)
    
    def test_update_flow_not_found(self, client, mock_flow_service, sample_flow_update):
        """Test updating non-existent flow."""
        flow_id = uuid.uuid4()
        mock_flow_service.update_flow.side_effect = HTTPException(status_code=404, detail="Flow not found")
        
        response = client.put(f"/flows/{flow_id}", json=sample_flow_update.model_dump(mode='json'))
        
        assert response.status_code == 404
        assert "Flow not found" in response.json()["detail"]
    
    def test_update_flow_invalid_uuid(self, client, mock_flow_service, sample_flow_update):
        """Test updating flow with invalid UUID."""
        response = client.put("/flows/invalid-uuid", json=sample_flow_update.model_dump(mode='json'))
        
        assert response.status_code == 422  # Validation error
    
    def test_update_flow_service_error(self, client, mock_flow_service, sample_flow_update):
        """Test updating flow with service error."""
        flow_id = uuid.uuid4()
        mock_flow_service.update_flow.side_effect = Exception("Update error")
        
        response = client.put(f"/flows/{flow_id}", json=sample_flow_update.model_dump(mode='json'))
        
        assert response.status_code == 500
        assert "Update error" in response.json()["detail"]


class TestDeleteFlow:
    """Test cases for delete flow endpoint."""
    
    def test_delete_flow_success(self, client, mock_flow_service):
        """Test successful flow deletion."""
        flow_id = uuid.uuid4()
        mock_flow_service.force_delete_flow_with_executions.return_value = True
        
        response = client.delete(f"/flows/{flow_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "deleted successfully" in data["message"]
        mock_flow_service.force_delete_flow_with_executions.assert_called_once_with(flow_id)
    
    def test_delete_flow_with_force_parameter(self, client, mock_flow_service):
        """Test flow deletion with force parameter (for backward compatibility)."""
        flow_id = uuid.uuid4()
        mock_flow_service.force_delete_flow_with_executions.return_value = True
        
        response = client.delete(f"/flows/{flow_id}?force=true")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        # Force parameter is ignored, but endpoint should still work
        mock_flow_service.force_delete_flow_with_executions.assert_called_once_with(flow_id)
    
    def test_delete_flow_not_found(self, client, mock_flow_service):
        """Test deleting non-existent flow."""
        flow_id = uuid.uuid4()
        mock_flow_service.force_delete_flow_with_executions.side_effect = HTTPException(
            status_code=404, detail="Flow not found"
        )
        
        response = client.delete(f"/flows/{flow_id}")
        
        assert response.status_code == 404
        assert "Flow not found" in response.json()["detail"]
    
    def test_delete_flow_invalid_uuid(self, client, mock_flow_service):
        """Test deleting flow with invalid UUID."""
        response = client.delete("/flows/invalid-uuid")
        
        assert response.status_code == 422  # Validation error
    
    def test_delete_flow_service_error(self, client, mock_flow_service):
        """Test deleting flow with service error."""
        flow_id = uuid.uuid4()
        mock_flow_service.force_delete_flow_with_executions.side_effect = Exception("Deletion error")
        
        response = client.delete(f"/flows/{flow_id}")
        
        assert response.status_code == 500
        assert "Unexpected error deleting flow" in response.json()["detail"]


class TestDeleteAllFlows:
    """Test cases for delete all flows endpoint."""
    
    def test_delete_all_flows_success(self, client, mock_flow_service):
        """Test successful deletion of all flows."""
        mock_flow_service.delete_all_flows.return_value = None
        
        response = client.delete("/flows")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "All flows deleted successfully" in data["message"]
        mock_flow_service.delete_all_flows.assert_called_once()
    
    def test_delete_all_flows_service_error(self, client, mock_flow_service):
        """Test deleting all flows with service error."""
        mock_flow_service.delete_all_flows.side_effect = Exception("Deletion error")
        
        response = client.delete("/flows")
        
        assert response.status_code == 500
        assert "Deletion error" in response.json()["detail"]


class TestGetFlowServiceDependency:
    """Test cases for get_flow_service dependency function."""
    
    def test_get_flow_service_dependency(self, mock_db_session):
        """Test the get_flow_service dependency function directly to achieve 100% coverage."""
        from src.api.flows_router import get_flow_service
        from src.services.flow_service import FlowService
        
        # Call the dependency function directly
        service = get_flow_service(mock_db_session)
        
        # Verify it returns a FlowService instance
        assert isinstance(service, FlowService)
        assert service.session == mock_db_session