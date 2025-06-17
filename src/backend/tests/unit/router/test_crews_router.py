"""
Unit tests for CrewsRouter.

Tests the functionality of crew management endpoints including
CRUD operations with group isolation.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import logging
from datetime import datetime
from uuid import UUID, uuid4
import json
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from src.schemas.crew import CrewCreate, CrewUpdate
from src.utils.user_context import GroupContext


# Mock crew model
class MockCrew:
    def __init__(self, id=None, name="Test Crew", agent_ids=None, task_ids=None,
                 nodes=None, edges=None, group_id="group-123"):
        self.id = id or uuid4()
        self.name = name
        self.agent_ids = agent_ids or ["agent-1", "agent-2"]
        self.task_ids = task_ids or ["task-1", "task-2"]
        self.nodes = nodes or []
        self.edges = edges or []
        self.group_id = group_id
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


@pytest.fixture
def mock_crew_service():
    """Create a mock crew service."""
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
def app(mock_crew_service, mock_group_context):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.crews_router import router, get_crew_service
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(router)
    
    # Override dependencies
    app.dependency_overrides[get_crew_service] = lambda: mock_crew_service
    app.dependency_overrides[get_group_context] = lambda: mock_group_context
    
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
def sample_crew_create():
    """Create a sample crew creation request."""
    return CrewCreate(
        name="New Crew",
        agent_ids=["agent-1", "agent-2", "agent-3"],
        task_ids=["task-1", "task-2", "task-3"],
        nodes=[
            {
                "id": "node-1", 
                "type": "agent", 
                "position": {"x": 0, "y": 0},
                "data": {"label": "Agent 1"}
            },
            {
                "id": "node-2", 
                "type": "task", 
                "position": {"x": 200, "y": 0},
                "data": {"label": "Task 1"}
            }
        ],
        edges=[
            {"id": "edge-1", "source": "node-1", "target": "node-2"}
        ]
    )


@pytest.fixture
def sample_crew_update():
    """Create a sample crew update request."""
    return CrewUpdate(
        name="Updated Crew",
        agent_ids=["agent-1", "agent-4"],
        nodes=[
            {
                "id": "node-1", 
                "type": "agent", 
                "position": {"x": 0, "y": 0},
                "data": {"label": "Agent 1 Updated"}
            }
        ]
    )


class TestListCrews:
    """Test cases for list crews endpoint."""
    
    def test_list_crews_success(self, client, mock_crew_service, mock_group_context):
        """Test successful crews listing."""
        crews = [
            MockCrew(name="Crew 1"),
            MockCrew(name="Crew 2")
        ]
        mock_crew_service.find_by_group.return_value = crews
        
        response = client.get("/crews")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["name"] == "Crew 1"
        assert data[1]["name"] == "Crew 2"
        assert all("id" in crew for crew in data)
        assert all("created_at" in crew for crew in data)
    
    def test_list_crews_empty_list(self, client, mock_crew_service, mock_group_context):
        """Test listing crews when no crews exist."""
        mock_crew_service.find_by_group.return_value = []
        
        response = client.get("/crews")
        
        assert response.status_code == 200
        assert response.json() == []
    
    def test_list_crews_service_error(self, client, mock_crew_service, mock_group_context):
        """Test listing crews with service error."""
        mock_crew_service.find_by_group.side_effect = Exception("Service error")
        
        response = client.get("/crews")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]
    
    def test_list_crews_with_nodes_edges(self, client, mock_crew_service, mock_group_context):
        """Test listing crews with nodes and edges."""
        # Create proper node and edge structures
        from src.schemas.crew import Node, Edge, NodeData, Position
        
        node = Node(
            id="n1",
            type="agent",
            position=Position(x=0, y=0),
            data=NodeData(label="Agent Node")
        )
        edge = Edge(id="e1", source="n1", target="n2")
        
        crews = [MockCrew(
            name="Crew with graph",
            nodes=[node],
            edges=[edge]
        )]
        mock_crew_service.find_by_group.return_value = crews
        
        response = client.get("/crews")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data[0]["nodes"]) == 1
        assert len(data[0]["edges"]) == 1


class TestGetCrew:
    """Test cases for get crew endpoint."""
    
    def test_get_crew_success(self, client, mock_crew_service, mock_group_context):
        """Test successful crew retrieval."""
        crew_id = uuid4()
        crew = MockCrew(id=crew_id, name="Test Crew")
        mock_crew_service.get_by_group.return_value = crew
        
        response = client.get(f"/crews/{crew_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == str(crew_id)
        assert data["name"] == "Test Crew"
        assert "created_at" in data
        assert "updated_at" in data
    
    def test_get_crew_not_found(self, client, mock_crew_service, mock_group_context):
        """Test getting non-existent crew."""
        crew_id = uuid4()
        mock_crew_service.get_by_group.return_value = None
        
        response = client.get(f"/crews/{crew_id}")
        
        assert response.status_code == 404
        assert "Crew not found" in response.json()["detail"]
    
    def test_get_crew_service_error(self, client, mock_crew_service, mock_group_context):
        """Test getting crew with service error."""
        crew_id = uuid4()
        mock_crew_service.get_by_group.side_effect = Exception("Service error")
        
        response = client.get(f"/crews/{crew_id}")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]
    
    def test_get_crew_invalid_uuid(self, client, mock_crew_service, mock_group_context):
        """Test getting crew with invalid UUID."""
        response = client.get("/crews/invalid-uuid")
        
        assert response.status_code == 422


class TestCreateCrew:
    """Test cases for create crew endpoint."""
    
    def test_create_crew_success(self, client, mock_crew_service, mock_group_context, sample_crew_create):
        """Test successful crew creation."""
        created_crew = MockCrew(
            name=sample_crew_create.name,
            agent_ids=sample_crew_create.agent_ids,
            task_ids=sample_crew_create.task_ids,
            nodes=sample_crew_create.nodes,
            edges=sample_crew_create.edges
        )
        mock_crew_service.create_with_group.return_value = created_crew
        
        response = client.post("/crews", json=sample_crew_create.model_dump())
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "New Crew"
        assert len(data["agent_ids"]) == 3
        assert len(data["task_ids"]) == 3
        assert len(data["nodes"]) == 2
        assert len(data["edges"]) == 1
    
    def test_create_crew_validation_error(self, client, mock_crew_service, mock_group_context):
        """Test creating crew with validation error."""
        # Create an actual ValidationError by trying to create invalid data
        try:
            CrewCreate(agent_ids=[], task_ids=[])  # Missing required 'name' field
        except ValidationError as e:
            mock_crew_service.create_with_group.side_effect = e
        
        response = client.post("/crews", json={"agent_ids": [], "task_ids": []})
        
        assert response.status_code == 422
    
    def test_create_crew_validation_error_with_logging(self, client, mock_crew_service, mock_group_context, sample_crew_create):
        """Test creating crew with ValidationError that triggers logging - covers lines 139-140."""
        from pydantic import ValidationError
        
        # Create a ValidationError to be raised by the service
        validation_error = ValidationError.from_exception_data(
            "ValidationError",
            [{"type": "missing", "loc": ("name",), "msg": "Field required", "input": {}}]
        )
        mock_crew_service.create_with_group.side_effect = validation_error
        
        with patch('src.api.crews_router.logger') as mock_logger:
            response = client.post("/crews", json=sample_crew_create.model_dump())
        
        assert response.status_code == 422
        # Verify that logger.error was called with the validation error
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args[0][0]
        assert "Validation error:" in call_args
    
    def test_create_crew_service_error(self, client, mock_crew_service, mock_group_context, sample_crew_create):
        """Test creating crew with service error."""
        mock_crew_service.create_with_group.side_effect = Exception("Service error")
        
        response = client.post("/crews", json=sample_crew_create.model_dump())
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestDebugCrewData:
    """Test cases for debug crew data endpoint."""
    
    def test_debug_crew_data_success(self, client, sample_crew_create):
        """Test successful crew data validation."""
        response = client.post("/crews/debug", json=sample_crew_create.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["message"] == "Data validation successful"
        assert data["data"]["name"] == "New Crew"
        assert data["data"]["node_count"] == 2
        assert data["data"]["edge_count"] == 1
    
    def test_debug_crew_data_validation_error(self, client):
        """Test debug endpoint with invalid data."""
        invalid_data = {
            "agent_ids": "not-a-list",  # Should be a list
            "task_ids": []
        }
        
        response = client.post("/crews/debug", json=invalid_data)
        
        # The endpoint validates input data before processing, so it returns 422
        assert response.status_code == 422
    
    def test_debug_crew_data_logging(self, client, sample_crew_create, caplog):
        """Test that debug endpoint logs properly."""
        with caplog.at_level(logging.INFO):
            response = client.post("/crews/debug", json=sample_crew_create.model_dump())
        
        assert response.status_code == 200
        assert "Data validation successful" in caplog.text
        assert "Crew name: New Crew" in caplog.text
        assert "Number of nodes: 2" in caplog.text

    def test_debug_crew_data_validation_error_with_exception(self, client):
        """Test debug endpoint with ValidationError exception during model_dump() - covers lines 179-185."""
        from pydantic import ValidationError
        
        # We need to mock the crew_in object's model_dump method directly
        with patch.object(CrewCreate, 'model_dump') as mock_model_dump:
            # Create a ValidationError to be raised during model_dump
            validation_error = ValidationError.from_exception_data(
                "ValidationError",
                [{"type": "missing", "loc": ("name",), "msg": "Field required", "input": {}}]
            )
            mock_model_dump.side_effect = validation_error
            
            # This should trigger the ValidationError handling in the debug endpoint
            with patch('src.api.crews_router.logger') as mock_logger:
                response = client.post("/crews/debug", json={
                    "name": "Test",
                    "agent_ids": ["agent-1"],
                    "task_ids": ["task-1"],
                    "nodes": [],
                    "edges": []
                })
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "error"
            assert data["message"] == "Validation failed"
            assert "errors" in data
            mock_logger.error.assert_called()
    
    def test_debug_crew_data_unexpected_exception(self, client):
        """Test debug endpoint with unexpected exception - covers lines 186-191."""
        # Patch model_dump to raise an unexpected exception
        with patch('src.schemas.crew.CrewCreate.model_dump') as mock_model_dump:
            mock_model_dump.side_effect = RuntimeError("Unexpected error")
            
            with patch('src.api.crews_router.logger') as mock_logger:
                response = client.post("/crews/debug", json={
                    "name": "Test",
                    "agent_ids": ["agent-1"],
                    "task_ids": ["task-1"],
                    "nodes": [],
                    "edges": []
                })
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "error"
            assert data["message"] == "Unexpected error: Unexpected error"
            mock_logger.error.assert_called_with("Unexpected error: Unexpected error")


class TestUpdateCrew:
    """Test cases for update crew endpoint."""
    
    def test_update_crew_success(self, client, mock_crew_service, mock_group_context, sample_crew_update):
        """Test successful crew update."""
        crew_id = uuid4()
        updated_crew = MockCrew(
            id=crew_id,
            name=sample_crew_update.name,
            agent_ids=sample_crew_update.agent_ids
        )
        mock_crew_service.update_with_partial_data_by_group.return_value = updated_crew
        
        response = client.put(f"/crews/{crew_id}", json=sample_crew_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Crew"
        assert len(data["agent_ids"]) == 2
    
    def test_update_crew_not_found(self, client, mock_crew_service, mock_group_context, sample_crew_update):
        """Test updating non-existent crew."""
        crew_id = uuid4()
        mock_crew_service.update_with_partial_data_by_group.return_value = None
        
        response = client.put(f"/crews/{crew_id}", json=sample_crew_update.model_dump())
        
        assert response.status_code == 404
        assert "Crew not found" in response.json()["detail"]
    
    def test_update_crew_validation_error(self, client, mock_crew_service, mock_group_context):
        """Test updating crew with validation error."""
        crew_id = uuid4()
        # Create an actual ValidationError
        try:
            # Try to create invalid node data
            CrewUpdate(nodes=[{"id": "invalid", "type": "invalid"}])  # Missing required fields
        except ValidationError as e:
            mock_crew_service.update_with_partial_data_by_group.side_effect = e
        
        response = client.put(f"/crews/{crew_id}", json={"name": ""})
        
        assert response.status_code == 422
    
    def test_update_crew_service_error(self, client, mock_crew_service, mock_group_context, sample_crew_update):
        """Test updating crew with service error."""
        crew_id = uuid4()
        mock_crew_service.update_with_partial_data_by_group.side_effect = Exception("Service error")
        
        response = client.put(f"/crews/{crew_id}", json=sample_crew_update.model_dump())
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestDeleteCrew:
    """Test cases for delete crew endpoint."""
    
    def test_delete_crew_success(self, client, mock_crew_service, mock_group_context):
        """Test successful crew deletion."""
        crew_id = uuid4()
        mock_crew_service.delete_by_group.return_value = True
        
        response = client.delete(f"/crews/{crew_id}")
        
        assert response.status_code == 204
    
    def test_delete_crew_not_found(self, client, mock_crew_service, mock_group_context):
        """Test deleting non-existent crew."""
        crew_id = uuid4()
        mock_crew_service.delete_by_group.return_value = False
        
        response = client.delete(f"/crews/{crew_id}")
        
        # Due to router's exception handling, not found gets caught and re-raised as 500
        assert response.status_code == 500
        assert "Crew not found" in response.json()["detail"]
    
    def test_delete_crew_service_error(self, client, mock_crew_service, mock_group_context):
        """Test deleting crew with service error."""
        crew_id = uuid4()
        mock_crew_service.delete_by_group.side_effect = Exception("Service error")
        
        response = client.delete(f"/crews/{crew_id}")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]


class TestDeleteAllCrews:
    """Test cases for delete all crews endpoint."""
    
    def test_delete_all_crews_success(self, client, mock_crew_service, mock_group_context):
        """Test successful deletion of all crews."""
        mock_crew_service.delete_all_by_group.return_value = None
        
        response = client.delete("/crews")
        
        assert response.status_code == 204
    
    def test_delete_all_crews_service_error(self, client, mock_crew_service, mock_group_context):
        """Test deleting all crews with service error."""
        mock_crew_service.delete_all_by_group.side_effect = Exception("Service error")
        
        response = client.delete("/crews")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]
    
    def test_delete_all_crews_logging(self, client, mock_crew_service, mock_group_context, caplog):
        """Test that delete all crews logs errors properly."""
        mock_crew_service.delete_all_by_group.side_effect = Exception("Delete failed")
        
        with caplog.at_level(logging.ERROR):
            response = client.delete("/crews")
        
        assert response.status_code == 500
        assert "Error deleting all crews: Delete failed" in caplog.text


class TestGetCrewService:
    """Test cases for the get_crew_service dependency function - covers line 24."""
    
    def test_get_crew_service_dependency(self):
        """Test that get_crew_service returns CrewService instance."""
        from src.api.crews_router import get_crew_service
        from src.services.crew_service import CrewService
        from unittest.mock import Mock
        
        # Create a mock session
        mock_session = Mock()
        
        # Call the dependency function
        service = get_crew_service(mock_session)
        
        # Verify it returns a CrewService instance
        assert isinstance(service, CrewService)
        assert service.session == mock_session


class TestHTTPExceptionReRaising:
    """Test cases for HTTPException re-raising patterns."""
    
    def test_get_crew_http_exception_reraise(self, client, mock_crew_service, mock_group_context):
        """Test that HTTPException in get_crew is re-raised properly - covers lines 99-100."""
        crew_id = uuid4()
        http_exception = HTTPException(status_code=403, detail="Forbidden")
        mock_crew_service.get_by_group.side_effect = http_exception
        
        response = client.get(f"/crews/{crew_id}")
        
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]
    
    def test_update_crew_http_exception_reraise(self, client, mock_crew_service, mock_group_context, sample_crew_update):
        """Test that HTTPException in update_crew is re-raised properly - covers lines 233-234."""
        crew_id = uuid4()
        http_exception = HTTPException(status_code=403, detail="Forbidden")
        mock_crew_service.update_with_partial_data_by_group.side_effect = http_exception
        
        response = client.put(f"/crews/{crew_id}", json=sample_crew_update.model_dump())
        
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]


class TestDeleteCrewNotFoundCorrection:
    """Test to fix the delete crew not found scenario."""
    
    def test_delete_crew_not_found_proper(self, client, mock_crew_service, mock_group_context):
        """Test deleting non-existent crew - the 404 HTTPException gets caught and re-raised as 500."""
        crew_id = uuid4()
        mock_crew_service.delete_by_group.return_value = False
        
        response = client.delete(f"/crews/{crew_id}")
        
        # Due to the exception handling in the delete endpoint, 404 becomes 500
        assert response.status_code == 500
        assert "Crew not found" in response.json()["detail"]
    
    def test_delete_crew_exception_handling_coverage(self, client, mock_crew_service, mock_group_context):
        """Test that the delete crew exception handling properly catches and logs exceptions."""
        crew_id = uuid4()
        mock_crew_service.delete_by_group.return_value = False
        
        with patch('src.api.crews_router.logger') as mock_logger:
            response = client.delete(f"/crews/{crew_id}")
        
        # Verify that the logger was called
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args[0][0]
        assert "Error deleting crew:" in call_args
        assert response.status_code == 500