"""
Unit tests for TaskTrackingRouter.

Tests the functionality of task progress tracking endpoints with comprehensive coverage.
"""
import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime
from fastapi import HTTPException

from fastapi.testclient import TestClient
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)


@pytest.fixture
def mock_service():
    """Create a mock service."""
    return AsyncMock()


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
def app(mock_service):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.task_tracking_router import router
    from src.services.task_tracking_service import get_task_tracking_service
    
    app = FastAPI()
    app.include_router(router)
    
    async def override_get_task_tracking_service():
        return mock_service
    
    app.dependency_overrides[get_task_tracking_service] = override_get_task_tracking_service
    
    return app


@pytest.fixture
def client(app, mock_current_user):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user

    return TestClient(app)


class TestTaskTrackingRouter:
    """Test cases for task tracking endpoints."""
    
    def test_get_job_status_success(self, client, mock_service):
        """Test successful job status retrieval."""
        from src.schemas.task_tracking import JobExecutionStatusResponse, TaskStatusSchema
        
        mock_response = JobExecutionStatusResponse(
            job_id="job-123",
            status="running",
            tasks=[
                TaskStatusSchema(
                    id=1,
                    task_id="task-1",
                    status="running",
                    agent_name="Agent 1",
                    started_at=datetime(2023, 1, 1),
                    completed_at=None
                )
            ]
        )
        
        mock_service.get_job_status.return_value = mock_response
        
        response = client.get("/task-tracking/status/job-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["job_id"] == "job-123"
        assert data["status"] == "running"
        assert len(data["tasks"]) == 1
        assert data["tasks"][0]["task_id"] == "task-1"
        mock_service.get_job_status.assert_called_once_with("job-123")
    
    def test_get_job_status_service_exception(self, client, mock_service):
        """Test job status retrieval with service exception."""
        mock_service.get_job_status.side_effect = Exception("Database error")
        
        response = client.get("/task-tracking/status/job-456")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
        mock_service.get_job_status.assert_called_once_with("job-456")

    def test_get_job_status_http_exception_from_service(self, client, mock_service):
        """Test job status retrieval when service raises HTTPException."""
        mock_service.get_job_status.side_effect = HTTPException(
            status_code=404, detail="Job not found"
        )
        
        response = client.get("/task-tracking/status/job-not-found")
        
        # Router catches HTTPException and converts to 500 with the original message
        assert response.status_code == 500
        assert "Job not found" in response.json()["detail"]
        mock_service.get_job_status.assert_called_once_with("job-not-found")
    
    def test_get_all_tasks_success(self, client, mock_service):
        """Test successful tasks retrieval."""
        from src.schemas.task_tracking import TaskStatusResponse
        
        mock_response = [
            TaskStatusResponse(
                id=1,
                job_id="job-1",
                task_id="task-1",
                status="completed",
                agent_name="Agent 1",
                started_at=datetime(2023, 1, 1),
                completed_at=datetime(2023, 1, 1, 1)
            ),
            TaskStatusResponse(
                id=2,
                job_id="job-2",
                task_id="task-2",
                status="running",
                agent_name="Agent 2",
                started_at=datetime(2023, 1, 1),
                completed_at=None
            )
        ]
        
        mock_service.get_all_tasks.return_value = mock_response
        
        response = client.get("/task-tracking/tasks")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["task_id"] == "task-1"
        assert data[1]["status"] == "running"
        mock_service.get_all_tasks.assert_called_once_with()
    
    def test_get_all_tasks_empty_list(self, client, mock_service):
        """Test tasks retrieval with empty result."""
        mock_service.get_all_tasks.return_value = []
        
        response = client.get("/task-tracking/tasks")
        
        assert response.status_code == 200
        data = response.json()
        assert data == []
        mock_service.get_all_tasks.assert_called_once_with()
    
    def test_get_all_tasks_service_exception(self, client, mock_service):
        """Test tasks retrieval with service exception."""
        mock_service.get_all_tasks.side_effect = Exception("Connection timeout")
        
        response = client.get("/task-tracking/tasks")
        
        assert response.status_code == 500
        assert "Connection timeout" in response.json()["detail"]
        mock_service.get_all_tasks.assert_called_once_with()

    def test_get_all_tasks_http_exception_from_service(self, client, mock_service):
        """Test tasks retrieval when service raises HTTPException."""
        mock_service.get_all_tasks.side_effect = HTTPException(
            status_code=403, detail="Access denied"
        )
        
        response = client.get("/task-tracking/tasks")
        
        # Router catches HTTPException and converts to 500 with the original message
        assert response.status_code == 500
        assert "Access denied" in response.json()["detail"]
        mock_service.get_all_tasks.assert_called_once_with()
    
    def test_create_task_success(self, client, mock_service):
        """Test successful task creation."""
        from src.schemas.task_tracking import TaskStatusResponse
        
        mock_response = TaskStatusResponse(
            id=3,
            job_id="job-new",
            task_id="task-new",
            status="running",
            agent_name="New Agent",
            started_at=datetime(2023, 1, 1),
            completed_at=None
        )
        
        mock_service.create_task.return_value = mock_response
        
        task_data = {
            "job_id": "job-new",
            "task_id": "task-new",
            "status": "running",
            "agent_name": "New Agent"
        }
        
        response = client.post("/task-tracking/tasks", json=task_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == "task-new"
        assert data["status"] == "running"
        assert data["agent_name"] == "New Agent"
        
        # Verify the service was called with correct data
        mock_service.create_task.assert_called_once()
        call_args = mock_service.create_task.call_args[0][0]
        assert call_args.job_id == "job-new"
        assert call_args.task_id == "task-new"
        assert call_args.status == "running"
    
    def test_create_task_without_agent_name(self, client, mock_service):
        """Test task creation without optional agent_name."""
        from src.schemas.task_tracking import TaskStatusResponse
        
        mock_response = TaskStatusResponse(
            id=4,
            job_id="job-no-agent",
            task_id="task-no-agent",
            status="running",
            agent_name=None,
            started_at=datetime(2023, 1, 1),
            completed_at=None
        )
        
        mock_service.create_task.return_value = mock_response
        
        task_data = {
            "job_id": "job-no-agent",
            "task_id": "task-no-agent",
            "status": "running"
        }
        
        response = client.post("/task-tracking/tasks", json=task_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == "task-no-agent"
        assert data["agent_name"] is None
    
    def test_create_task_invalid_data(self, client, mock_service):
        """Test task creation with invalid data."""
        task_data = {
            "job_id": "job-invalid",
            # Missing required fields
        }
        
        response = client.post("/task-tracking/tasks", json=task_data)
        
        assert response.status_code == 422  # Validation error
        # Service should not be called for invalid input
        mock_service.create_task.assert_not_called()
    
    def test_create_task_service_exception(self, client, mock_service):
        """Test task creation with service exception."""
        mock_service.create_task.side_effect = Exception("Database constraint violation")
        
        task_data = {
            "job_id": "job-error",
            "task_id": "task-error",
            "status": "running"
        }
        
        response = client.post("/task-tracking/tasks", json=task_data)
        
        assert response.status_code == 500
        assert "Database constraint violation" in response.json()["detail"]
    
    def test_create_task_http_exception_from_service(self, client, mock_service):
        """Test task creation when service raises HTTPException."""
        mock_service.create_task.side_effect = HTTPException(
            status_code=409, detail="Task already exists"
        )
        
        task_data = {
            "job_id": "job-duplicate",
            "task_id": "task-duplicate",
            "status": "running"
        }
        
        response = client.post("/task-tracking/tasks", json=task_data)
        
        # Router catches HTTPException and converts to 500 with the original message
        assert response.status_code == 500
        assert "Task already exists" in response.json()["detail"]
    
    def test_update_task_success(self, client, mock_service):
        """Test successful task update."""
        from src.schemas.task_tracking import TaskStatusResponse
        
        mock_response = TaskStatusResponse(
            id=1,
            job_id="job-1",
            task_id="task-1",
            status="completed",
            agent_name="Agent 1",
            started_at=datetime(2023, 1, 1),
            completed_at=datetime(2023, 1, 1, 1)
        )
        
        mock_service.update_task.return_value = mock_response
        
        update_data = {
            "status": "completed"
        }
        
        response = client.put("/task-tracking/tasks/1", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == "task-1"
        assert data["status"] == "completed"
        
        # Verify the service was called with correct parameters
        mock_service.update_task.assert_called_once()
        call_args = mock_service.update_task.call_args
        assert call_args[0][0] == 1  # task_id
        assert call_args[0][1].status == "completed"  # update data
    
    def test_update_task_invalid_id(self, client, mock_service):
        """Test task update with invalid task ID."""
        update_data = {
            "status": "completed"
        }
        
        # Test with non-integer ID
        response = client.put("/task-tracking/tasks/invalid-id", json=update_data)
        
        assert response.status_code == 422  # Validation error
        mock_service.update_task.assert_not_called()
    
    def test_update_task_invalid_data(self, client, mock_service):
        """Test task update with invalid data."""
        update_data = {
            "invalid_field": "value"
        }
        
        response = client.put("/task-tracking/tasks/1", json=update_data)
        
        assert response.status_code == 422  # Validation error
        mock_service.update_task.assert_not_called()
    
    def test_update_task_service_exception(self, client, mock_service):
        """Test task update with service exception."""
        mock_service.update_task.side_effect = Exception("Update failed")
        
        update_data = {
            "status": "completed"
        }
        
        response = client.put("/task-tracking/tasks/1", json=update_data)
        
        assert response.status_code == 500
        assert "Update failed" in response.json()["detail"]
    
    def test_update_task_http_exception_from_service(self, client, mock_service):
        """Test task update when service raises HTTPException."""
        mock_service.update_task.side_effect = HTTPException(
            status_code=404, detail="Task not found"
        )
        
        update_data = {
            "status": "completed"
        }
        
        response = client.put("/task-tracking/tasks/999", json=update_data)
        
        # Router catches HTTPException and converts to 500 with the original message
        assert response.status_code == 500
        assert "Task not found" in response.json()["detail"]
    
    def test_update_task_with_different_statuses(self, client, mock_service):
        """Test task update with different status values."""
        from src.schemas.task_tracking import TaskStatusResponse
        
        # Test different status values
        status_tests = ["running", "completed", "failed"]
        
        for status in status_tests:
            mock_response = TaskStatusResponse(
                id=1,
                job_id="job-1",
                task_id="task-1",
                status=status,
                agent_name="Agent 1",
                started_at=datetime(2023, 1, 1),
                completed_at=datetime(2023, 1, 1, 1) if status == "completed" else None
            )
            
            mock_service.update_task.return_value = mock_response
            
            update_data = {"status": status}
            
            response = client.put("/task-tracking/tasks/1", json=update_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == status
    
    def test_router_prefix_and_tags(self):
        """Test router configuration."""
        from src.api.task_tracking_router import router
        
        assert router.prefix == "/task-tracking"
        assert "task tracking" in router.tags
    
    def test_logger_configuration(self):
        """Test logger is properly configured."""
        from src.api.task_tracking_router import logger
        
        assert logger.name == "src.api.task_tracking_router"
    
    def test_all_endpoint_paths(self, client):
        """Test that all expected endpoints exist and return appropriate responses."""
        # Test endpoints exist by checking for method not allowed vs not found
        # This ensures all paths are registered
        
        # GET /task-tracking/status/{job_id} - should work with mock
        response = client.options("/task-tracking/status/test-job")
        assert response.status_code in [200, 405]  # 405 means endpoint exists but method not allowed
        
        # GET /task-tracking/tasks - should work with mock  
        response = client.options("/task-tracking/tasks")
        assert response.status_code in [200, 405]
        
        # POST /task-tracking/tasks - should work with mock
        response = client.options("/task-tracking/tasks")
        assert response.status_code in [200, 405]
        
        # PUT /task-tracking/tasks/{task_id} - should work with mock
        response = client.options("/task-tracking/tasks/1")
        assert response.status_code in [200, 405]
    
    def test_response_model_validation(self, client, mock_service):
        """Test that response models are properly validated."""
        from src.schemas.task_tracking import JobExecutionStatusResponse, TaskStatusSchema
        
        # Test with malformed response data (should still work if service returns valid Pydantic models)
        mock_response = JobExecutionStatusResponse(
            job_id="test-job",
            status="running",
            tasks=[
                TaskStatusSchema(
                    id=1,
                    task_id="task-1",
                    status="running",
                    agent_name="Test Agent",
                    started_at=datetime(2023, 1, 1),
                    completed_at=None
                )
            ]
        )
        
        mock_service.get_job_status.return_value = mock_response
        
        response = client.get("/task-tracking/status/test-job")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify required fields are present
        assert "job_id" in data
        assert "status" in data
        assert "tasks" in data
        assert isinstance(data["tasks"], list)
        
        if data["tasks"]:
            task = data["tasks"][0]
            assert "id" in task
            assert "task_id" in task
            assert "status" in task
            assert "started_at" in task
    
    @patch('src.api.task_tracking_router.logger')
    def test_logging_on_exceptions(self, mock_logger, client, mock_service):
        """Test that exceptions are properly logged."""
        # Test logging for get_job_status
        mock_service.get_job_status.side_effect = Exception("Test error")
        
        response = client.get("/task-tracking/status/test-job")
        
        assert response.status_code == 500
        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert "Error getting job status" in error_message
        assert "Test error" in error_message
        
        # Reset mock and test get_all_tasks
        mock_logger.reset_mock()
        mock_service.get_all_tasks.side_effect = Exception("Another test error")
        
        response = client.get("/task-tracking/tasks")
        
        assert response.status_code == 500
        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert "Error getting tasks" in error_message
        assert "Another test error" in error_message
        
        # Reset mock and test create_task
        mock_logger.reset_mock()
        mock_service.create_task.side_effect = Exception("Create error")
        
        task_data = {
            "job_id": "job-error",
            "task_id": "task-error", 
            "status": "running"
        }
        
        response = client.post("/task-tracking/tasks", json=task_data)
        
        assert response.status_code == 500
        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert "Error creating task" in error_message
        assert "Create error" in error_message
        
        # Reset mock and test update_task
        mock_logger.reset_mock()
        mock_service.update_task.side_effect = Exception("Update error")
        
        update_data = {"status": "completed"}
        
        response = client.put("/task-tracking/tasks/1", json=update_data)
        
        assert response.status_code == 500
        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert "Error updating task" in error_message
        assert "Update error" in error_message


class TestRouterIntegration:
    """Integration tests for router configuration and setup."""
    
    def test_router_includes_all_routes(self):
        """Test that router includes all expected routes."""
        from src.api.task_tracking_router import router
        
        # Check that router has routes
        assert len(router.routes) > 0
        
        # Get all route paths
        route_paths = []
        for route in router.routes:
            if hasattr(route, 'path'):
                route_paths.append(route.path)
        
        # Expected routes based on the router definition
        expected_routes = [
            "/status/{job_id}",
            "/tasks", 
            "/tasks/{task_id}"
        ]
        
        for expected_route in expected_routes:
            # Check if any route matches the expected pattern
            found = any(expected_route.replace("{job_id}", "").replace("{task_id}", "") 
                       in route_path for route_path in route_paths)
            assert found, f"Expected route pattern {expected_route} not found in {route_paths}"
    
    def test_dependency_injection_setup(self):
        """Test that dependency injection is properly configured."""
        from src.services.task_tracking_service import get_task_tracking_service
        
        # Test that dependency function exists and is callable
        assert callable(get_task_tracking_service)
        
        # Test that it's an async generator (dependency)
        import inspect
        assert inspect.isasyncgenfunction(get_task_tracking_service)