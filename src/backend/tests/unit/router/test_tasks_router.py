"""
Unit tests for TasksRouter.

Tests the functionality of task management endpoints including
CRUD operations with group isolation.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi.testclient import TestClient

from src.schemas.task import TaskCreate, TaskUpdate, TaskConfig
from src.utils.user_context import GroupContext


# Mock task model
class MockTask:
    def __init__(self, id="task-123", name="Test Task", description="Test task description",
                 expected_output="Test output", agent_id="agent-123", group_id="group-123"):
        self.id = id
        self.name = name
        self.description = description
        self.expected_output = expected_output
        self.agent_id = agent_id
        self.group_id = group_id
        self.tools = ["tool1", "tool2"]
        self.async_execution = False
        self.context = []
        self.config = {"cache_response": True}
        self.output_json = None
        self.output_pydantic = None
        self.output_file = None
        self.output = None
        self.markdown = False
        self.callback = None
        self.human_input = False
        self.converter_cls = None
        self.guardrail = None
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "expected_output": self.expected_output,
            "agent_id": self.agent_id,
            "tools": self.tools,
            "async_execution": self.async_execution,
            "context": self.context,
            "config": self.config,
            "output_json": self.output_json,
            "output_pydantic": self.output_pydantic,
            "output_file": self.output_file,
            "output": self.output,
            "markdown": self.markdown,
            "callback": self.callback,
            "human_input": self.human_input,
            "converter_cls": self.converter_cls,
            "guardrail": self.guardrail,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@pytest.fixture
def mock_task_service():
    """Create a mock task service."""
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
def app(mock_task_service, mock_group_context):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.tasks_router import router, get_task_service
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(router)
    
    # Create override functions
    async def override_get_task_service():
        return mock_task_service
        
    async def override_get_group_context():
        return mock_group_context
    
    # Override dependencies
    app.dependency_overrides[get_task_service] = override_get_task_service
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
def sample_task_create():
    """Create a sample task creation request."""
    return TaskCreate(
        name="Test Task",
        description="Test task description",
        expected_output="Expected test output",
        agent_id="agent-123",
        tools=["tool1", "tool2"],
        async_execution=False,
        context=[],
        config=TaskConfig(cache_response=True),
        markdown=False,
        human_input=False
    )


@pytest.fixture
def sample_task_update():
    """Create a sample task update request."""
    return TaskUpdate(
        name="Updated Task",
        description="Updated task description",
        expected_output="Updated expected output",
        tools=["tool1", "tool2", "tool3"],
        async_execution=True,
        markdown=True
    )


class TestCreateTask:
    """Test cases for create task endpoint."""
    
    def test_create_task_success(self, client, mock_task_service, mock_group_context, sample_task_create):
        """Test successful task creation."""
        created_task = MockTask()
        mock_task_service.create_with_group.return_value = created_task
        
        response = client.post("/tasks", json=sample_task_create.model_dump())
        
        assert response.status_code == 201
        data = response.json()
        assert data["id"] == "task-123"
        assert data["name"] == "Test Task"
        mock_task_service.create_with_group.assert_called_once_with(sample_task_create, mock_group_context)
    
    def test_create_task_service_error(self, client, mock_task_service, mock_group_context, sample_task_create):
        """Test task creation with service error."""
        mock_task_service.create_with_group.side_effect = Exception("Database error")
        
        response = client.post("/tasks", json=sample_task_create.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestListTasks:
    """Test cases for list tasks endpoint."""
    
    def test_list_tasks_success(self, client, mock_task_service, mock_group_context):
        """Test successful tasks listing."""
        tasks = [MockTask(id="task-1"), MockTask(id="task-2")]
        mock_task_service.find_by_group.return_value = tasks
        
        response = client.get("/tasks")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["id"] == "task-1"
        assert data[1]["id"] == "task-2"
        mock_task_service.find_by_group.assert_called_once_with(mock_group_context)
    
    def test_list_tasks_empty(self, client, mock_task_service, mock_group_context):
        """Test listing tasks when none exist."""
        mock_task_service.find_by_group.return_value = []
        
        response = client.get("/tasks")
        
        assert response.status_code == 200
        assert response.json() == []
    
    def test_list_tasks_service_error(self, client, mock_task_service, mock_group_context):
        """Test tasks listing with service error."""
        mock_task_service.find_by_group.side_effect = Exception("Database error")
        
        response = client.get("/tasks")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestGetTask:
    """Test cases for get task endpoint."""
    
    def test_get_task_success(self, client, mock_task_service):
        """Test successful task retrieval."""
        task = MockTask()
        mock_task_service.get.return_value = task
        
        response = client.get("/tasks/task-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "task-123"
        assert data["name"] == "Test Task"
        mock_task_service.get.assert_called_once_with("task-123")
    
    def test_get_task_not_found(self, client, mock_task_service):
        """Test getting non-existent task."""
        mock_task_service.get.return_value = None
        
        response = client.get("/tasks/nonexistent")
        
        assert response.status_code == 404
        assert "Task not found" in response.json()["detail"]
    
    def test_get_task_service_error(self, client, mock_task_service):
        """Test getting task with service error."""
        mock_task_service.get.side_effect = Exception("Database error")
        
        response = client.get("/tasks/task-123")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestUpdateTaskFull:
    """Test cases for full update task endpoint."""
    
    def test_update_task_full_success(self, client, mock_task_service):
        """Test successful full task update."""
        updated_task = MockTask(name="Fully Updated Task")
        mock_task_service.update_full.return_value = updated_task
        
        task_data = {
            "name": "Fully Updated Task",
            "description": "Updated description",
            "expected_output": "Updated output",
            "tools": ["new_tool"]
        }
        
        response = client.put("/tasks/task-123/full", json=task_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Fully Updated Task"
        mock_task_service.update_full.assert_called_once_with("task-123", task_data)
    
    def test_update_task_full_not_found(self, client, mock_task_service):
        """Test full update of non-existent task."""
        mock_task_service.update_full.return_value = None
        
        task_data = {"name": "Updated Task"}
        
        response = client.put("/tasks/nonexistent/full", json=task_data)
        
        assert response.status_code == 404
        assert "Task not found" in response.json()["detail"]
    
    def test_update_task_full_service_error(self, client, mock_task_service):
        """Test full update with service error."""
        mock_task_service.update_full.side_effect = Exception("Database error")
        
        task_data = {"name": "Updated Task"}
        
        response = client.put("/tasks/task-123/full", json=task_data)
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestUpdateTask:
    """Test cases for partial update task endpoint."""
    
    def test_update_task_success(self, client, mock_task_service, sample_task_update):
        """Test successful partial task update."""
        updated_task = MockTask(name="Updated Task")
        mock_task_service.update_with_partial_data.return_value = updated_task
        
        response = client.put("/tasks/task-123", json=sample_task_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Task"
        mock_task_service.update_with_partial_data.assert_called_once_with("task-123", sample_task_update)
    
    def test_update_task_not_found(self, client, mock_task_service, sample_task_update):
        """Test partial update of non-existent task."""
        mock_task_service.update_with_partial_data.return_value = None
        
        response = client.put("/tasks/nonexistent", json=sample_task_update.model_dump())
        
        assert response.status_code == 404
        assert "Task not found" in response.json()["detail"]
    
    def test_update_task_service_error(self, client, mock_task_service, sample_task_update):
        """Test partial update with service error."""
        mock_task_service.update_with_partial_data.side_effect = Exception("Database error")
        
        response = client.put("/tasks/task-123", json=sample_task_update.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestDeleteTask:
    """Test cases for delete task endpoint."""
    
    def test_delete_task_success(self, client, mock_task_service):
        """Test successful task deletion."""
        mock_task_service.delete.return_value = True
        
        response = client.delete("/tasks/task-123")
        
        assert response.status_code == 204
        mock_task_service.delete.assert_called_once_with("task-123")
    
    def test_delete_task_not_found(self, client, mock_task_service):
        """Test deleting non-existent task."""
        mock_task_service.delete.return_value = False
        
        response = client.delete("/tasks/nonexistent")
        
        assert response.status_code == 404
        assert "Task not found" in response.json()["detail"]
    
    def test_delete_task_service_error(self, client, mock_task_service):
        """Test deleting task with service error."""
        mock_task_service.delete.side_effect = Exception("Database error")
        
        response = client.delete("/tasks/task-123")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestDeleteAllTasks:
    """Test cases for delete all tasks endpoint."""
    
    def test_delete_all_tasks_success(self, client, mock_task_service):
        """Test successful deletion of all tasks."""
        mock_task_service.delete_all.return_value = None
        
        response = client.delete("/tasks")
        
        assert response.status_code == 204
        mock_task_service.delete_all.assert_called_once()
    
    def test_delete_all_tasks_service_error(self, client, mock_task_service):
        """Test deleting all tasks with service error."""
        mock_task_service.delete_all.side_effect = Exception("Database error")
        
        response = client.delete("/tasks")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]