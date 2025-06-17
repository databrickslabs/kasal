"""
Unit tests for FlowExecutionRouter.

Tests the functionality of flow execution management endpoints.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from src.api.flow_execution_router import router


@pytest.fixture
def app():
    """Create a FastAPI app for testing."""
    app = FastAPI()
    app.include_router(router)
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
def client(app, mock_current_user):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user

    return TestClient(app)


class TestFlowExecutionRouter:
    """Test cases for flow execution endpoints."""
    
    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_execute_flow_success(self, mock_service_class, client):
        """Test successful flow execution."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        flow_id = str(uuid.uuid4())
        
        mock_service.run_flow.return_value = {
            "execution_id": "exec-123",
            "flow_id": flow_id,
            "status": "running",
            "message": "Flow execution started"
        }
        
        execution_data = {
            "flow_id": flow_id,
            "job_id": "job-123",
            "config": {"param1": "value1"}
        }
        
        response = client.post("/flow-executions", json=execution_data)
        
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "running"
        assert data["flow_id"] == flow_id
        mock_service.run_flow.assert_called_once_with(
            flow_id=flow_id,
            job_id="job-123",
            config={"param1": "value1"}
        )

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_execute_flow_success_false_returned(self, mock_service_class, client):
        """Test flow execution when success is False."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        flow_id = str(uuid.uuid4())
        
        mock_service.run_flow.return_value = {
            "success": False,
            "error": "Flow execution failed"
        }
        
        execution_data = {
            "flow_id": flow_id,
            "job_id": "job-123",
            "config": {"param1": "value1"}
        }
        
        response = client.post("/flow-executions", json=execution_data)
        
        assert response.status_code == 400
        data = response.json()
        assert data["detail"] == "Flow execution failed"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_execute_flow_success_false_no_error_message(self, mock_service_class, client):
        """Test flow execution when success is False and no error message."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        flow_id = str(uuid.uuid4())
        
        mock_service.run_flow.return_value = {
            "success": False
        }
        
        execution_data = {
            "flow_id": flow_id,
            "job_id": "job-123"
        }
        
        response = client.post("/flow-executions", json=execution_data)
        
        assert response.status_code == 400
        data = response.json()
        assert data["detail"] == "Flow execution failed"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_execute_flow_service_exception(self, mock_service_class, client):
        """Test flow execution with service exception."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        flow_id = str(uuid.uuid4())
        
        mock_service.run_flow.side_effect = Exception("Service error")
        
        execution_data = {
            "flow_id": flow_id,
            "job_id": "job-123"
        }
        
        response = client.post("/flow-executions", json=execution_data)
        
        assert response.status_code == 500
        data = response.json()
        assert data["detail"] == "Service error"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_execute_flow_http_exception_passthrough(self, mock_service_class, client):
        """Test that HTTPExceptions are passed through unchanged."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        flow_id = str(uuid.uuid4())
        
        mock_service.run_flow.side_effect = HTTPException(status_code=404, detail="Not found")
        
        execution_data = {
            "flow_id": flow_id,
            "job_id": "job-123"
        }
        
        response = client.post("/flow-executions", json=execution_data)
        
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Not found"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_execution_success(self, mock_service_class, client):
        """Test getting flow execution status."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_execution.return_value = {
            "execution_id": 123,
            "status": "completed",
            "result": {"output": "success"}
        }
        
        response = client.get("/flow-executions/123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "completed"
        assert data["execution_id"] == 123
        mock_service.get_flow_execution.assert_called_once_with(123)

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_execution_with_execution_key(self, mock_service_class, client):
        """Test getting flow execution when result has execution key."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_execution.return_value = {
            "success": True,
            "execution": {
                "execution_id": 123,
                "status": "completed",
                "result": {"output": "success"}
            }
        }
        
        response = client.get("/flow-executions/123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "completed"
        assert data["execution_id"] == 123

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_execution_success_false(self, mock_service_class, client):
        """Test getting flow execution when success is False."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_execution.return_value = {
            "success": False,
            "error": "Execution not found"
        }
        
        response = client.get("/flow-executions/123")
        
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Execution not found"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_execution_success_false_no_error(self, mock_service_class, client):
        """Test getting flow execution when success is False and no error message."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_execution.return_value = {
            "success": False
        }
        
        response = client.get("/flow-executions/123")
        
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Flow execution not found"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_execution_service_exception(self, mock_service_class, client):
        """Test getting flow execution with service exception."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_execution.side_effect = Exception("Service error")
        
        response = client.get("/flow-executions/123")
        
        assert response.status_code == 500
        data = response.json()
        assert data["detail"] == "Service error"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_execution_http_exception_passthrough(self, mock_service_class, client):
        """Test that HTTPExceptions are passed through unchanged in get_flow_execution."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_execution.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        response = client.get("/flow-executions/123")
        
        assert response.status_code == 403
        data = response.json()
        assert data["detail"] == "Forbidden"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_executions_by_flow_success(self, mock_service_class, client):
        """Test getting flow executions by flow ID."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        # Return a dict structure that router expects - it should be a dict-like object
        mock_service.get_flow_executions_by_flow.return_value = {
            "success": True,
            "data": [
                {"execution_id": 1, "status": "completed"},
                {"execution_id": 2, "status": "running"}
            ]
        }
        
        response = client.get("/flow-executions/by-flow/flow-123")
        
        assert response.status_code == 200
        data = response.json()
        # The router returns the whole result since there's no 'executions' key
        assert data["success"] == True
        assert len(data["data"]) == 2
        assert data["data"][0]["execution_id"] == 1
        assert data["data"][1]["execution_id"] == 2
        mock_service.get_flow_executions_by_flow.assert_called_once_with("flow-123")

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_executions_by_flow_with_executions_key(self, mock_service_class, client):
        """Test getting flow executions when result has executions key."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_executions_by_flow.return_value = {
            "success": True,
            "executions": [
                {"execution_id": 1, "status": "completed"},
                {"execution_id": 2, "status": "running"}
            ]
        }
        
        response = client.get("/flow-executions/by-flow/flow-123")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["execution_id"] == 1

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_executions_by_flow_success_false(self, mock_service_class, client):
        """Test getting flow executions when success is False."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_executions_by_flow.return_value = {
            "success": False,
            "error": "Flow not found"
        }
        
        response = client.get("/flow-executions/by-flow/flow-123")
        
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Flow not found"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_executions_by_flow_success_false_no_error(self, mock_service_class, client):
        """Test getting flow executions when success is False and no error message."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_executions_by_flow.return_value = {
            "success": False
        }
        
        response = client.get("/flow-executions/by-flow/flow-123")
        
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Flow not found"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_executions_by_flow_service_exception(self, mock_service_class, client):
        """Test getting flow executions with service exception."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_executions_by_flow.side_effect = Exception("Service error")
        
        response = client.get("/flow-executions/by-flow/flow-123")
        
        assert response.status_code == 500
        data = response.json()
        assert data["detail"] == "Service error"

    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_executions_by_flow_http_exception_passthrough(self, mock_service_class, client):
        """Test that HTTPExceptions are passed through unchanged in get_flow_executions_by_flow."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        mock_service.get_flow_executions_by_flow.side_effect = HTTPException(status_code=401, detail="Unauthorized")
        
        response = client.get("/flow-executions/by-flow/flow-123")
        
        assert response.status_code == 401
        data = response.json()
        assert data["detail"] == "Unauthorized"

    def test_flow_execution_request_model_validation(self, client):
        """Test FlowExecutionRequest validation."""
        # Test missing required fields
        response = client.post("/flow-executions", json={})
        assert response.status_code == 422
        
        # Test valid minimal request
        execution_data = {
            "flow_id": "test-flow",
            "job_id": "test-job"
        }
        
        # This will fail at the service level, but should pass validation
        with patch('src.api.flow_execution_router.CrewAIFlowService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service_class.return_value = mock_service
            mock_service.run_flow.return_value = {"status": "ok"}
            
            response = client.post("/flow-executions", json=execution_data)
            assert response.status_code == 202

    @patch('src.api.flow_execution_router.logger')
    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_execute_flow_logging_on_exception(self, mock_service_class, mock_logger, client):
        """Test that exceptions are logged properly in execute_flow."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        error_message = "Database connection failed"
        mock_service.run_flow.side_effect = Exception(error_message)
        
        execution_data = {
            "flow_id": "test-flow",
            "job_id": "test-job"
        }
        
        response = client.post("/flow-executions", json=execution_data)
        
        assert response.status_code == 500
        mock_logger.error.assert_called_once_with(f"Error executing flow: {error_message}")

    @patch('src.api.flow_execution_router.logger')
    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_execution_logging_on_exception(self, mock_service_class, mock_logger, client):
        """Test that exceptions are logged properly in get_flow_execution."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        error_message = "Database query failed"
        mock_service.get_flow_execution.side_effect = Exception(error_message)
        
        response = client.get("/flow-executions/123")
        
        assert response.status_code == 500
        mock_logger.error.assert_called_once_with(f"Error getting flow execution: {error_message}")

    @patch('src.api.flow_execution_router.logger')
    @patch('src.api.flow_execution_router.CrewAIFlowService')
    def test_get_flow_executions_by_flow_logging_on_exception(self, mock_service_class, mock_logger, client):
        """Test that exceptions are logged properly in get_flow_executions_by_flow."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        error_message = "Network timeout"
        mock_service.get_flow_executions_by_flow.side_effect = Exception(error_message)
        
        response = client.get("/flow-executions/by-flow/flow-123")
        
        assert response.status_code == 500
        mock_logger.error.assert_called_once_with(f"Error getting flow executions: {error_message}")