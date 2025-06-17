"""
Unit tests for ExecutionHistoryRouter.

Tests the functionality of execution history management endpoints including
retrieval, pagination, debug outputs, and bulk deletion operations.
"""
import pytest
from unittest.mock import AsyncMock
from datetime import datetime
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.utils.user_context import GroupContext


# Mock execution history models
class MockExecutionHistoryItem:
    def __init__(self, id=1, job_id="exec-123", status="completed"):
        self.id = id
        self.job_id = job_id
        self.status = status
        self.created_at = datetime.utcnow()
        self.name = None
        self.agents_yaml = None
        self.tasks_yaml = None
        self.model = None
        self.error = None
        self.input = None
        self.execution_type = None
        self.result = None
        self.group_email = None
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "id": self.id,
            "job_id": self.job_id,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "name": self.name,
            "agents_yaml": self.agents_yaml,
            "tasks_yaml": self.tasks_yaml,
            "model": self.model,
            "error": self.error,
            "input": self.input,
            "execution_type": self.execution_type,
            "result": self.result,
            "group_email": self.group_email
        }


class MockExecutionHistoryList:
    def __init__(self, executions, total, limit, offset):
        self.executions = executions
        self.total = total
        self.limit = limit
        self.offset = offset
        
    def model_dump(self):
        return {
            "executions": [exec.model_dump() for exec in self.executions],
            "total": self.total,
            "limit": self.limit,
            "offset": self.offset
        }


class MockExecutionOutput:
    def __init__(self, id=1, job_id="exec-123", output="test output"):
        self.id = id
        self.job_id = job_id
        self.output = output
        self.timestamp = datetime.utcnow()
        self.task_name = None
        self.agent_name = None
        
    def model_dump(self):
        return {
            "id": self.id,
            "job_id": self.job_id,
            "output": self.output,
            "timestamp": self.timestamp.isoformat(),
            "task_name": self.task_name,
            "agent_name": self.agent_name
        }

class MockExecutionOutputList:
    def __init__(self, execution_id, outputs, total, limit, offset):
        self.execution_id = execution_id
        self.outputs = outputs
        self.total = total
        self.limit = limit
        self.offset = offset
        
    def model_dump(self):
        return {
            "execution_id": self.execution_id,
            "outputs": [output.model_dump() if hasattr(output, 'model_dump') else output for output in self.outputs],
            "total": self.total,
            "limit": self.limit,
            "offset": self.offset
        }


class MockExecutionOutputDebug:
    def __init__(self, id=1, timestamp=None, task_name=None, agent_name=None, output_preview=None):
        self.id = id
        self.timestamp = timestamp or datetime.utcnow()
        self.task_name = task_name
        self.agent_name = agent_name
        self.output_preview = output_preview
        
    def model_dump(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "task_name": self.task_name,
            "agent_name": self.agent_name,
            "output_preview": self.output_preview
        }

class MockExecutionOutputDebugList:
    def __init__(self, run_id=1, execution_id="exec-123", total_outputs=0, outputs=None):
        self.run_id = run_id
        self.execution_id = execution_id
        self.total_outputs = total_outputs
        self.outputs = outputs or []
        
    def model_dump(self):
        return {
            "run_id": self.run_id,
            "execution_id": self.execution_id,
            "total_outputs": self.total_outputs,
            "outputs": [output.model_dump() if hasattr(output, 'model_dump') else output for output in self.outputs]
        }


class MockDeleteResponse:
    def __init__(self, message="Deleted successfully", deleted_run_id=None, deleted_job_id=None, deleted_runs=None, deleted_outputs=None):
        self.message = message
        self.deleted_run_id = deleted_run_id
        self.deleted_job_id = deleted_job_id
        self.deleted_runs = deleted_runs
        self.deleted_outputs = deleted_outputs
        
    def model_dump(self):
        return {
            "message": self.message,
            "deleted_run_id": self.deleted_run_id,
            "deleted_job_id": self.deleted_job_id,
            "deleted_runs": self.deleted_runs,
            "deleted_outputs": self.deleted_outputs
        }


@pytest.fixture
def mock_execution_history_service():
    """Create a mock execution history service."""
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
def app(mock_execution_history_service, mock_group_context):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.execution_history_router import router
    from src.services.execution_history_service import get_execution_history_service
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(router)
    
    # Create override functions
    async def override_get_execution_history_service():
        return mock_execution_history_service
        
    async def override_get_group_context():
        return mock_group_context
    
    # Override dependencies
    app.dependency_overrides[get_execution_history_service] = override_get_execution_history_service
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
def client(app, mock_current_user):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user

    return TestClient(app)


class TestGetExecutionHistory:
    """Test cases for get execution history endpoint."""
    
    def test_get_execution_history_success(self, client, mock_execution_history_service, mock_group_context):
        """Test successful execution history retrieval."""
        executions = [
            MockExecutionHistoryItem(id=1, job_id="exec-1"),
            MockExecutionHistoryItem(id=2, job_id="exec-2")
        ]
        history_list = MockExecutionHistoryList(executions=executions, total=2, limit=50, offset=0)
        mock_execution_history_service.get_execution_history.return_value = history_list
        
        response = client.get("/executions/history")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert len(data["executions"]) == 2
        mock_execution_history_service.get_execution_history.assert_called_once_with(
            50, 0, group_ids=mock_group_context.group_ids
        )
    
    def test_get_execution_history_with_pagination(self, client, mock_execution_history_service, mock_group_context):
        """Test execution history retrieval with pagination parameters."""
        executions = [MockExecutionHistoryItem(id=3, job_id="exec-3")]
        history_list = MockExecutionHistoryList(executions=executions, total=1, limit=10, offset=20)
        mock_execution_history_service.get_execution_history.return_value = history_list
        
        response = client.get("/executions/history?limit=10&offset=20")
        
        assert response.status_code == 200
        data = response.json()
        assert data["limit"] == 10
        assert data["offset"] == 20
        mock_execution_history_service.get_execution_history.assert_called_once_with(
            10, 20, group_ids=mock_group_context.group_ids
        )
    
    def test_get_execution_history_invalid_params(self, client, mock_execution_history_service):
        """Test execution history with invalid pagination parameters."""
        response = client.get("/executions/history?limit=0")
        assert response.status_code == 422  # Validation error
        
        response = client.get("/executions/history?limit=101")
        assert response.status_code == 422  # Validation error
        
        response = client.get("/executions/history?offset=-1")
        assert response.status_code == 422  # Validation error
    
    def test_get_execution_history_service_error(self, client, mock_execution_history_service, mock_group_context):
        """Test execution history retrieval with service error."""
        mock_execution_history_service.get_execution_history.side_effect = Exception("Database error")
        
        response = client.get("/executions/history")
        
        assert response.status_code == 500
        assert "Failed to retrieve execution history" in response.json()["detail"]


class TestCheckExecutionExists:
    """Test cases for check execution exists endpoint."""
    
    def test_check_execution_exists_success(self, client, mock_execution_history_service, mock_group_context):
        """Test successful execution existence check."""
        mock_execution_history_service.check_execution_exists.return_value = True
        
        response = client.head("/executions/history/1")
        
        assert response.status_code == 200
        assert response.content == b""  # HEAD request should have empty body
        mock_execution_history_service.check_execution_exists.assert_called_once_with(1)
    
    def test_check_execution_exists_not_found(self, client, mock_execution_history_service, mock_group_context):
        """Test execution existence check for non-existent execution."""
        mock_execution_history_service.check_execution_exists.return_value = False
        
        response = client.head("/executions/history/999")
        
        assert response.status_code == 404
    
    def test_check_execution_exists_service_error(self, client, mock_execution_history_service, mock_group_context):
        """Test execution existence check with service error."""
        mock_execution_history_service.check_execution_exists.side_effect = Exception("Database error")
        
        response = client.head("/executions/history/1")
        
        assert response.status_code == 500
    
    def test_check_execution_exists_http_exception_reraise(self, client, mock_execution_history_service, mock_group_context):
        """Test execution existence check re-raising HTTPException."""
        from fastapi import HTTPException
        mock_execution_history_service.check_execution_exists.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        response = client.head("/executions/history/1")
        
        assert response.status_code == 403


class TestGetExecutionById:
    """Test cases for get execution by ID endpoint."""
    
    def test_get_execution_by_id_success(self, client, mock_execution_history_service, mock_group_context):
        """Test successful execution retrieval by ID."""
        execution = MockExecutionHistoryItem(id=1, job_id="exec-123")
        mock_execution_history_service.get_execution_by_id.return_value = execution
        
        response = client.get("/executions/history/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["job_id"] == "exec-123"
        mock_execution_history_service.get_execution_by_id.assert_called_once_with(
            1, group_ids=mock_group_context.group_ids
        )
    
    def test_get_execution_by_id_not_found(self, client, mock_execution_history_service, mock_group_context):
        """Test getting non-existent execution by ID."""
        mock_execution_history_service.get_execution_by_id.return_value = None
        
        response = client.get("/executions/history/999")
        
        assert response.status_code == 404
        assert "Execution with ID 999 not found" in response.json()["detail"]
    
    def test_get_execution_by_id_service_error(self, client, mock_execution_history_service, mock_group_context):
        """Test getting execution by ID with service error."""
        mock_execution_history_service.get_execution_by_id.side_effect = Exception("Database error")
        
        response = client.get("/executions/history/1")
        
        assert response.status_code == 500
        assert "Failed to retrieve execution" in response.json()["detail"]
    
    def test_get_execution_by_id_http_exception_reraise(self, client, mock_execution_history_service, mock_group_context):
        """Test getting execution by ID re-raising HTTPException."""
        from fastapi import HTTPException
        mock_execution_history_service.get_execution_by_id.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        response = client.get("/executions/history/1")
        
        assert response.status_code == 403


class TestGetExecutionOutputs:
    """Test cases for get execution outputs endpoint."""
    
    def test_get_execution_outputs_success(self, client, mock_execution_history_service, mock_group_context):
        """Test successful execution outputs retrieval."""
        outputs = [
            MockExecutionOutput(id=1, job_id="exec-123", output="output1"),
            MockExecutionOutput(id=2, job_id="exec-123", output="output2")
        ]
        output_list = MockExecutionOutputList(execution_id="exec-123", outputs=outputs, total=2, limit=1000, offset=0)
        mock_execution_history_service.get_execution_outputs.return_value = output_list
        
        response = client.get("/executions/exec-123/outputs")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert len(data["outputs"]) == 2
        assert data["execution_id"] == "exec-123"
        mock_execution_history_service.get_execution_outputs.assert_called_once_with(
            "exec-123", 1000, 0, group_ids=mock_group_context.group_ids
        )
    
    def test_get_execution_outputs_with_pagination(self, client, mock_execution_history_service, mock_group_context):
        """Test execution outputs retrieval with pagination."""
        outputs = [MockExecutionOutput(id=3, job_id="exec-123", output="output3")]
        output_list = MockExecutionOutputList(execution_id="exec-123", outputs=outputs, total=1, limit=100, offset=50)
        mock_execution_history_service.get_execution_outputs.return_value = output_list
        
        response = client.get("/executions/exec-123/outputs?limit=100&offset=50")
        
        assert response.status_code == 200
        data = response.json()
        assert data["limit"] == 100
        assert data["offset"] == 50
        assert data["execution_id"] == "exec-123"
        mock_execution_history_service.get_execution_outputs.assert_called_once_with(
            "exec-123", 100, 50, group_ids=mock_group_context.group_ids
        )
    
    def test_get_execution_outputs_invalid_params(self, client, mock_execution_history_service):
        """Test execution outputs with invalid pagination parameters."""
        response = client.get("/executions/exec-123/outputs?limit=0")
        assert response.status_code == 422  # Validation error
        
        response = client.get("/executions/exec-123/outputs?limit=5001")
        assert response.status_code == 422  # Validation error
    
    def test_get_execution_outputs_service_error(self, client, mock_execution_history_service, mock_group_context):
        """Test execution outputs retrieval with service error."""
        mock_execution_history_service.get_execution_outputs.side_effect = Exception("Database error")
        
        response = client.get("/executions/exec-123/outputs")
        
        assert response.status_code == 500
        assert "Failed to retrieve execution outputs" in response.json()["detail"]


class TestGetExecutionDebugOutputs:
    """Test cases for get execution debug outputs endpoint."""
    
    def test_get_execution_debug_outputs_success(self, client, mock_execution_history_service, mock_group_context):
        """Test successful execution debug outputs retrieval."""
        debug_outputs = [
            MockExecutionOutputDebug(id=1, task_name="task1", output_preview="preview1"),
            MockExecutionOutputDebug(id=2, task_name="task2", output_preview="preview2")
        ]
        debug_list = MockExecutionOutputDebugList(run_id=1, execution_id="exec-123", total_outputs=2, outputs=debug_outputs)
        mock_execution_history_service.get_debug_outputs.return_value = debug_list
        
        response = client.get("/executions/exec-123/outputs/debug")
        
        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == 1
        assert data["execution_id"] == "exec-123"
        assert data["total_outputs"] == 2
        assert len(data["outputs"]) == 2
        mock_execution_history_service.get_debug_outputs.assert_called_once_with(
            "exec-123", group_ids=mock_group_context.group_ids
        )
    
    def test_get_execution_debug_outputs_not_found(self, client, mock_execution_history_service, mock_group_context):
        """Test getting debug outputs for non-existent execution."""
        mock_execution_history_service.get_debug_outputs.return_value = None
        
        response = client.get("/executions/nonexistent/outputs/debug")
        
        assert response.status_code == 404
        assert "Execution with ID nonexistent not found" in response.json()["detail"]
    
    def test_get_execution_debug_outputs_service_error(self, client, mock_execution_history_service, mock_group_context):
        """Test getting debug outputs with service error."""
        mock_execution_history_service.get_debug_outputs.side_effect = Exception("Database error")
        
        response = client.get("/executions/exec-123/outputs/debug")
        
        assert response.status_code == 500
        assert "Failed to retrieve debug outputs" in response.json()["detail"]
    
    def test_get_execution_debug_outputs_http_exception_reraise(self, client, mock_execution_history_service, mock_group_context):
        """Test getting debug outputs re-raising HTTPException."""
        from fastapi import HTTPException
        mock_execution_history_service.get_debug_outputs.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        response = client.get("/executions/exec-123/outputs/debug")
        
        assert response.status_code == 403


class TestDeleteAllExecutions:
    """Test cases for delete all executions endpoint."""
    
    def test_delete_all_executions_success(self, client, mock_execution_history_service, mock_group_context):
        """Test successful deletion of all executions."""
        delete_response = MockDeleteResponse(message="All executions deleted", deleted_runs=5, deleted_outputs=10)
        mock_execution_history_service.delete_all_executions.return_value = delete_response
        
        response = client.delete("/executions/history")
        
        assert response.status_code == 200
        data = response.json()
        assert data["deleted_runs"] == 5
        assert data["deleted_outputs"] == 10
        assert "All executions deleted" in data["message"]
        mock_execution_history_service.delete_all_executions.assert_called_once()
    
    def test_delete_all_executions_service_error(self, client, mock_execution_history_service, mock_group_context):
        """Test deleting all executions with service error."""
        mock_execution_history_service.delete_all_executions.side_effect = Exception("Database error")
        
        response = client.delete("/executions/history")
        
        assert response.status_code == 500
        assert "Failed to delete executions" in response.json()["detail"]


class TestDeleteExecution:
    """Test cases for delete execution by ID endpoint."""
    
    def test_delete_execution_success(self, client, mock_execution_history_service, mock_group_context):
        """Test successful deletion of specific execution."""
        delete_response = MockDeleteResponse(message="Execution deleted", deleted_run_id=1, deleted_outputs=3)
        mock_execution_history_service.delete_execution.return_value = delete_response
        
        response = client.delete("/executions/history/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["deleted_run_id"] == 1
        assert data["deleted_outputs"] == 3
        assert "Execution deleted" in data["message"]
        mock_execution_history_service.delete_execution.assert_called_once_with(1)
    
    def test_delete_execution_not_found(self, client, mock_execution_history_service, mock_group_context):
        """Test deleting non-existent execution."""
        mock_execution_history_service.delete_execution.return_value = None
        
        response = client.delete("/executions/history/999")
        
        assert response.status_code == 404
        assert "Execution with ID 999 not found" in response.json()["detail"]
    
    def test_delete_execution_service_error(self, client, mock_execution_history_service, mock_group_context):
        """Test deleting execution with service error."""
        mock_execution_history_service.delete_execution.side_effect = Exception("Database error")
        
        response = client.delete("/executions/history/1")
        
        assert response.status_code == 500
        assert "Failed to delete execution" in response.json()["detail"]
    
    def test_delete_execution_http_exception_reraise(self, client, mock_execution_history_service, mock_group_context):
        """Test deleting execution re-raising HTTPException."""
        from fastapi import HTTPException
        mock_execution_history_service.delete_execution.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        response = client.delete("/executions/history/1")
        
        assert response.status_code == 403


class TestDeleteExecutionByJobId:
    """Test cases for delete execution by job ID endpoint."""
    
    def test_delete_execution_by_job_id_success(self, client, mock_execution_history_service, mock_group_context):
        """Test successful deletion of execution by job ID."""
        delete_response = MockDeleteResponse(message="Execution deleted by job_id", deleted_job_id="job-uuid-123", deleted_outputs=2)
        mock_execution_history_service.delete_execution_by_job_id.return_value = delete_response
        
        response = client.delete("/executions/job-uuid-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["deleted_job_id"] == "job-uuid-123"
        assert data["deleted_outputs"] == 2
        assert "Execution deleted by job_id" in data["message"]
        mock_execution_history_service.delete_execution_by_job_id.assert_called_once_with("job-uuid-123")
    
    def test_delete_execution_by_job_id_not_found(self, client, mock_execution_history_service, mock_group_context):
        """Test deleting execution by non-existent job ID."""
        mock_execution_history_service.delete_execution_by_job_id.return_value = None
        
        response = client.delete("/executions/nonexistent-job-id")
        
        assert response.status_code == 404
        assert "Execution with job_id nonexistent-job-id not found" in response.json()["detail"]
    
    def test_delete_execution_by_job_id_service_error(self, client, mock_execution_history_service, mock_group_context):
        """Test deleting execution by job ID with service error."""
        mock_execution_history_service.delete_execution_by_job_id.side_effect = Exception("Database error")
        
        response = client.delete("/executions/job-uuid-123")
        
        assert response.status_code == 500
        assert "Failed to delete execution" in response.json()["detail"]
    
    def test_delete_execution_by_job_id_http_exception_reraise(self, client, mock_execution_history_service, mock_group_context):
        """Test deleting execution by job ID re-raising HTTPException."""
        from fastapi import HTTPException
        mock_execution_history_service.delete_execution_by_job_id.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        response = client.delete("/executions/job-uuid-123")
        
        assert response.status_code == 403