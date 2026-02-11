"""
Unit tests for ExecutionTraceRouter.

Tests the functionality of execution trace/debugging endpoints.
"""
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import HTTPException
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi.testclient import TestClient

from src.utils.user_context import GroupContext


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="test@example.com",
        user_id="user-123"
    )


@pytest.fixture
def app(mock_group_context):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.execution_trace_router import router
    from src.core.dependencies import get_group_context
    from tests.unit.router.conftest import register_exception_handlers

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    async def override_get_group_context():
        return mock_group_context

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


class TestExecutionTraceRouter:
    """Test cases for execution trace endpoints."""
    
    # Test get_all_traces endpoint
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_all_traces_for_group')
    def test_get_all_traces_success(self, mock_get_all_traces, client, mock_group_context):
        """Test successful retrieval of all traces."""
        mock_get_all_traces.return_value = {
            "traces": [
                {
                    "id": 1,
                    "run_id": 456,
                    "job_id": "job-123",
                    "timestamp": "2024-01-01T00:00:00",
                    "event_source": "agent",
                    "event_type": "execution"
                }
            ],
            "total": 1,
            "limit": 100,
            "offset": 0
        }
        
        response = client.get("/traces/")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["traces"]) == 1
        assert data["total"] == 1
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_all_traces')
    def test_get_all_traces_exception(self, mock_get_all_traces, client, mock_group_context):
        """Test exception handling in get_all_traces."""
        mock_get_all_traces.side_effect = Exception("Database error")
        
        response = client.get("/traces/")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    # Test get_traces_by_run_id endpoint
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_run_id')
    def test_get_traces_by_run_id_success(self, mock_get_traces, client, mock_group_context):
        """Test successful retrieval of traces by run_id."""
        mock_get_traces.return_value = {
            "run_id": 456,
            "traces": [
                {
                    "id": 1,
                    "run_id": 456,
                    "job_id": "job-123",
                    "timestamp": "2024-01-01T00:00:00",
                    "event_source": "agent",
                    "event_type": "execution"
                }
            ]
        }
        
        response = client.get("/traces/execution/456")
        
        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == 456
        assert len(data["traces"]) == 1
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_run_id')
    def test_get_traces_by_run_id_not_found(self, mock_get_traces, client, mock_group_context):
        """Test traces by run_id not found."""
        mock_get_traces.return_value = None
        
        response = client.get("/traces/execution/999")
        
        assert response.status_code == 404
        assert "Execution with ID 999 not found" in response.json()["detail"]
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_run_id')
    def test_get_traces_by_run_id_http_exception_reraise(self, mock_get_traces, client, mock_group_context):
        """Test HTTP exception re-raising in get_traces_by_run_id."""
        mock_get_traces.side_effect = HTTPException(status_code=404, detail="Not found")
        
        response = client.get("/traces/execution/456")
        
        assert response.status_code == 404
        assert "Not found" in response.json()["detail"]
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_run_id')
    def test_get_traces_by_run_id_exception(self, mock_get_traces, client, mock_group_context):
        """Test exception handling in get_traces_by_run_id."""
        mock_get_traces.side_effect = Exception("Database error")
        
        response = client.get("/traces/execution/456")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    # Test get_traces_by_job_id endpoint
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id')
    def test_get_traces_by_job_id_success(self, mock_get_traces, client, mock_group_context):
        """Test successful retrieval of traces by job_id."""
        mock_get_traces.return_value = {
            "job_id": "job-123",
            "traces": [
                {
                    "id": 1,
                    "run_id": 456,
                    "job_id": "job-123",
                    "timestamp": "2024-01-01T00:00:00",
                    "event_source": "agent",
                    "event_type": "execution"
                }
            ]
        }
        
        response = client.get("/traces/job/job-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["job_id"] == "job-123"
        assert len(data["traces"]) == 1
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id')
    def test_get_traces_by_job_id_not_found(self, mock_get_traces, client, mock_group_context):
        """Test traces by job_id not found."""
        mock_get_traces.return_value = None
        
        response = client.get("/traces/job/nonexistent-job")
        
        assert response.status_code == 404
        assert "Execution with job_id nonexistent-job not found" in response.json()["detail"]
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id')
    def test_get_traces_by_job_id_http_exception_reraise(self, mock_get_traces, client, mock_group_context):
        """Test HTTP exception re-raising in get_traces_by_job_id."""
        mock_get_traces.side_effect = HTTPException(status_code=404, detail="Not found")
        
        response = client.get("/traces/job/job-123")
        
        assert response.status_code == 404
        assert "Not found" in response.json()["detail"]
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id')
    def test_get_traces_by_job_id_exception(self, mock_get_traces, client, mock_group_context):
        """Test exception handling in get_traces_by_job_id."""
        mock_get_traces.side_effect = Exception("Database error")
        
        response = client.get("/traces/job/job-123")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    # Test get_trace_by_id endpoint
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_trace_by_id_with_group_check')
    def test_get_trace_by_id_success(self, mock_get_trace, client, mock_group_context):
        """Test successful execution trace retrieval by ID."""
        mock_get_trace.return_value = {
            "id": 123,
            "run_id": 456,
            "job_id": "job-123",
            "timestamp": "2024-01-01T00:00:00",
            "event_source": "agent",
            "event_type": "execution",
            "input_data": {"step": 1, "action": "start"},
            "output_data": {"result": "success"}
        }
        
        response = client.get("/traces/123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 123
        assert data["run_id"] == 456
        assert data["job_id"] == "job-123"
        assert data["event_source"] == "agent"
    
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_trace_by_id_with_group_check')
    def test_get_trace_by_id_not_found(self, mock_get_trace, client, mock_group_context):
        """Test execution trace retrieval for non-existent trace."""
        mock_get_trace.return_value = None
        
        response = client.get("/traces/999")
        
        assert response.status_code == 404
        assert "Trace with ID 999 not found" in response.json()["detail"]
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_trace_by_id_with_group_check')
    def test_get_trace_by_id_http_exception_reraise(self, mock_get_trace, client, mock_group_context):
        """Test HTTP exception re-raising in get_trace_by_id."""
        mock_get_trace.side_effect = HTTPException(status_code=404, detail="Not found")
        
        response = client.get("/traces/123")
        
        assert response.status_code == 404
        assert "Not found" in response.json()["detail"]
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_trace_by_id_with_group_check')
    def test_get_trace_by_id_exception(self, mock_get_trace, client, mock_group_context):
        """Test exception handling in get_trace_by_id."""
        mock_get_trace.side_effect = Exception("Database error")
        
        response = client.get("/traces/123")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    # Test create_trace endpoint
    @patch('src.api.execution_trace_router.ExecutionTraceService.create_trace_with_group')
    def test_create_trace_success(self, mock_create_trace, client, mock_group_context):
        """Test successful trace creation."""
        trace_data = {
            "run_id": 456,
            "job_id": "job-123",
            "event_source": "agent",
            "event_type": "execution",
            "input_data": {"step": 1, "action": "start"},
            "output_data": {"result": "success"}
        }
        
        mock_create_trace.return_value = {
            "id": 123,
            **trace_data
        }
        
        response = client.post("/traces/", json=trace_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["id"] == 123
        assert data["run_id"] == 456
        assert data["job_id"] == "job-123"
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.create_trace_with_group')
    def test_create_trace_exception(self, mock_create_trace, client, mock_group_context):
        """Test exception handling in create_trace."""
        mock_create_trace.side_effect = Exception("Database error")
        
        trace_data = {
            "run_id": 456,
            "job_id": "job-123",
            "event_source": "agent",
            "event_type": "execution"
        }
        
        response = client.post("/traces/", json=trace_data)
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]
    
    # Test delete_traces_by_run_id endpoint
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_traces_by_run_id_with_group_check')
    def test_delete_traces_by_run_id_success(self, mock_delete_traces, client, mock_group_context):
        """Test successful deletion of traces by run_id."""
        mock_delete_traces.return_value = {
            "message": "Traces deleted successfully",
            "deleted_traces": 3
        }
        
        response = client.delete("/traces/execution/456")
        
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Traces deleted successfully"
        assert data["deleted_traces"] == 3
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_traces_by_run_id_with_group_check')
    def test_delete_traces_by_run_id_exception(self, mock_delete_traces, client, mock_group_context):
        """Test exception handling in delete_traces_by_run_id."""
        mock_delete_traces.side_effect = Exception("Database error")
        
        response = client.delete("/traces/execution/456")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    # Test delete_traces_by_job_id endpoint
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_traces_by_job_id_with_group_check')
    def test_delete_traces_by_job_id_success(self, mock_delete_traces, client, mock_group_context):
        """Test successful deletion of traces by job_id."""
        mock_delete_traces.return_value = {
            "message": "Traces deleted successfully",
            "deleted_traces": 2
        }
        
        response = client.delete("/traces/job/job-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Traces deleted successfully"
        assert data["deleted_traces"] == 2
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_traces_by_job_id_with_group_check')
    def test_delete_traces_by_job_id_exception(self, mock_delete_traces, client, mock_group_context):
        """Test exception handling in delete_traces_by_job_id."""
        mock_delete_traces.side_effect = Exception("Database error")
        
        response = client.delete("/traces/job/job-123")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    # Test delete_trace endpoint
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_trace_with_group_check')
    def test_delete_trace_success(self, mock_delete_trace, client, mock_group_context):
        """Test successful deletion of a single trace."""
        mock_delete_trace.return_value = {
            "message": "Trace deleted successfully",
            "deleted_trace_id": 123
        }
        
        response = client.delete("/traces/123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Trace deleted successfully"
        assert data["deleted_trace_id"] == 123
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_trace_with_group_check')
    def test_delete_trace_not_found(self, mock_delete_trace, client, mock_group_context):
        """Test deletion of non-existent trace."""
        mock_delete_trace.return_value = None
        
        response = client.delete("/traces/999")
        
        assert response.status_code == 404
        assert "Trace with ID 999 not found" in response.json()["detail"]
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_trace_with_group_check')
    def test_delete_trace_http_exception_reraise(self, mock_delete_trace, client, mock_group_context):
        """Test HTTP exception re-raising in delete_trace."""
        mock_delete_trace.side_effect = HTTPException(status_code=404, detail="Not found")
        
        response = client.delete("/traces/123")
        
        assert response.status_code == 404
        assert "Not found" in response.json()["detail"]
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_trace_with_group_check')
    def test_delete_trace_exception(self, mock_delete_trace, client, mock_group_context):
        """Test exception handling in delete_trace."""
        mock_delete_trace.side_effect = Exception("Database error")
        
        response = client.delete("/traces/123")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    # Test delete_all_traces endpoint
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_all_traces_for_group')
    def test_delete_all_traces_success(self, mock_delete_all_traces, client, mock_group_context):
        """Test successful deletion of all traces."""
        mock_delete_all_traces.return_value = {
            "message": "All traces deleted successfully",
            "deleted_traces": 10
        }
        
        response = client.delete("/traces/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "All traces deleted successfully"
        assert data["deleted_traces"] == 10
        
    @patch('src.api.execution_trace_router.ExecutionTraceService.delete_all_traces_for_group')
    def test_delete_all_traces_exception(self, mock_delete_all_traces, client, mock_group_context):
        """Test exception handling in delete_all_traces."""
        mock_delete_all_traces.side_effect = Exception("Database error")
        
        response = client.delete("/traces/")
        
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    # Test query parameter validation
    @patch('src.api.execution_trace_router.ExecutionTraceService.get_all_traces_for_group')
    def test_get_all_traces_with_custom_params(self, mock_get_all_traces, client, mock_group_context):
        """Test get_all_traces with custom limit and offset parameters."""
        mock_get_all_traces.return_value = {
            "traces": [],
            "total": 0,
            "limit": 50,
            "offset": 10
        }
        
        response = client.get("/traces/?limit=50&offset=10")
        
        assert response.status_code == 200
        data = response.json()
        assert data["limit"] == 50
        assert data["offset"] == 10
    
    def test_get_traces_by_run_id_with_custom_params(self, client, mock_group_context):
        """Test get_traces_by_run_id with custom limit and offset parameters."""
        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_run_id') as mock_get_traces:
            mock_get_traces.return_value = {
                "run_id": 456,
                "traces": []
            }
            
            response = client.get("/traces/execution/456?limit=25&offset=5")
            
            assert response.status_code == 200
            mock_get_traces.assert_called_once_with(
                group_context=mock_group_context,
                run_id=456,
                limit=25,
                offset=5
            )
    
    def test_get_traces_by_job_id_with_custom_params(self, client, mock_group_context):
        """Test get_traces_by_job_id with custom limit and offset parameters."""
        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = {
                "job_id": "job-123",
                "traces": []
            }
            
            response = client.get("/traces/job/job-123?limit=25&offset=5")
            
            assert response.status_code == 200
            mock_get_traces.assert_called_once_with(
                group_context=mock_group_context,
                job_id="job-123",
                limit=25,
                offset=5
            )

    # Test high limit acceptance (limit up to 15000)
    def test_get_traces_accepts_high_limit(self, client, mock_group_context):
        """Test that trace endpoints accept limit values up to 15000."""
        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = {
                "job_id": "job-123",
                "traces": []
            }

            response = client.get("/traces/job/job-123?limit=1500")
            assert response.status_code == 200
            mock_get_traces.assert_called_once_with(
                group_context=mock_group_context,
                job_id="job-123",
                limit=1500,
                offset=0
            )

    def test_get_traces_rejects_limit_over_max(self, client, mock_group_context):
        """Test that trace endpoints reject limit values over 15000."""
        response = client.get("/traces/job/job-123?limit=20000")
        assert response.status_code == 422

    # Test get_current_crew_node_states endpoint
    def test_get_crew_node_states_success(self, client, mock_group_context):
        """Test successful retrieval of crew node states with task events."""
        mock_trace_started = MagicMock()
        mock_trace_started.event_type = "task_started"
        mock_trace_started.trace_metadata = {
            "crew_name": "research_crew",
            "agent_role": "Researcher"
        }
        mock_trace_started.event_context = "Research Task"
        mock_trace_started.created_at = datetime(2024, 1, 1, 10, 0, 0)

        mock_trace_completed = MagicMock()
        mock_trace_completed.event_type = "task_completed"
        mock_trace_completed.trace_metadata = {
            "crew_name": "research_crew",
            "agent_role": "Researcher"
        }
        mock_trace_completed.event_context = "Research Task"
        mock_trace_completed.created_at = datetime(2024, 1, 1, 10, 5, 0)

        mock_result = MagicMock()
        mock_result.traces = [mock_trace_started, mock_trace_completed]

        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = mock_result

            response = client.get("/traces/job/job-123/crew-node-states")

            assert response.status_code == 200
            data = response.json()
            assert "research_crew" in data
            assert data["research_crew"]["status"] == "completed"

    def test_get_crew_node_states_with_failure(self, client, mock_group_context):
        """Test crew node states when a task fails."""
        mock_trace_started = MagicMock()
        mock_trace_started.event_type = "task_started"
        mock_trace_started.trace_metadata = {
            "crew_name": "data_crew",
            "agent_role": "Analyst"
        }
        mock_trace_started.event_context = "Analysis Task"
        mock_trace_started.created_at = datetime(2024, 1, 1, 10, 0, 0)

        mock_trace_failed = MagicMock()
        mock_trace_failed.event_type = "task_failed"
        mock_trace_failed.trace_metadata = {
            "crew_name": "data_crew",
            "agent_role": "Analyst",
            "error": "Context window overflow"
        }
        mock_trace_failed.event_context = "Analysis Task"
        mock_trace_failed.output = None
        mock_trace_failed.created_at = datetime(2024, 1, 1, 10, 3, 0)

        mock_result = MagicMock()
        mock_result.traces = [mock_trace_started, mock_trace_failed]

        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = mock_result

            response = client.get("/traces/job/job-123/crew-node-states")

            assert response.status_code == 200
            data = response.json()
            assert "data_crew" in data
            assert data["data_crew"]["status"] == "failed"
            assert data["data_crew"]["error"] == "Context window overflow"

    def test_get_crew_node_states_not_found(self, client, mock_group_context):
        """Test crew node states when job not found."""
        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = None

            response = client.get("/traces/job/nonexistent/crew-node-states")

            assert response.status_code == 404

    def test_get_crew_node_states_error_from_output(self, client, mock_group_context):
        """Test crew node states extracts error from output.extra_data when trace_metadata has no error."""
        mock_trace_started = MagicMock()
        mock_trace_started.event_type = "task_started"
        mock_trace_started.trace_metadata = {"crew_name": "my_crew"}
        mock_trace_started.event_context = "Task1"
        mock_trace_started.created_at = datetime(2024, 1, 1, 10, 0, 0)

        mock_trace_failed = MagicMock()
        mock_trace_failed.event_type = "task_failed"
        mock_trace_failed.trace_metadata = {"crew_name": "my_crew"}
        mock_trace_failed.event_context = "Task1"
        mock_trace_failed.output = {"extra_data": {"error": "LLM rate limit exceeded"}}
        mock_trace_failed.created_at = datetime(2024, 1, 1, 10, 1, 0)

        mock_result = MagicMock()
        mock_result.traces = [mock_trace_started, mock_trace_failed]

        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = mock_result

            response = client.get("/traces/job/job-123/crew-node-states")

            assert response.status_code == 200
            data = response.json()
            assert data["my_crew"]["status"] == "failed"
            assert data["my_crew"]["error"] == "LLM rate limit exceeded"

    # Test get_current_task_states endpoint
    def test_get_task_states_success(self, client, mock_group_context):
        """Test successful retrieval of task states."""
        mock_trace_started = MagicMock()
        mock_trace_started.event_type = "task_started"
        mock_trace_started.trace_metadata = {"task_id": "task-uuid-1"}
        mock_trace_started.event_context = "Research Task"
        mock_trace_started.created_at = datetime(2024, 1, 1, 10, 0, 0)

        mock_trace_completed = MagicMock()
        mock_trace_completed.event_type = "task_completed"
        mock_trace_completed.trace_metadata = {"task_id": "task-uuid-1"}
        mock_trace_completed.event_context = "Research Task"
        mock_trace_completed.created_at = datetime(2024, 1, 1, 10, 5, 0)

        mock_result = MagicMock()
        mock_result.traces = [mock_trace_started, mock_trace_completed]

        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = mock_result

            response = client.get("/traces/job/job-123/task-states")

            assert response.status_code == 200
            data = response.json()
            assert "task-uuid-1" in data
            assert data["task-uuid-1"]["status"] == "completed"
            assert data["task-uuid-1"]["task_name"] == "Research Task"

    def test_get_task_states_with_failure(self, client, mock_group_context):
        """Test task states when a task fails."""
        mock_trace_started = MagicMock()
        mock_trace_started.event_type = "task_started"
        mock_trace_started.trace_metadata = {"task_id": "task-uuid-2"}
        mock_trace_started.event_context = "Analysis Task"
        mock_trace_started.created_at = datetime(2024, 1, 1, 10, 0, 0)

        mock_trace_failed = MagicMock()
        mock_trace_failed.event_type = "task_failed"
        mock_trace_failed.trace_metadata = {"task_id": "task-uuid-2"}
        mock_trace_failed.event_context = "Analysis Task"
        mock_trace_failed.created_at = datetime(2024, 1, 1, 10, 3, 0)

        mock_result = MagicMock()
        mock_result.traces = [mock_trace_started, mock_trace_failed]

        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = mock_result

            response = client.get("/traces/job/job-123/task-states")

            assert response.status_code == 200
            data = response.json()
            assert "task-uuid-2" in data
            assert data["task-uuid-2"]["status"] == "failed"

    def test_get_task_states_not_found(self, client, mock_group_context):
        """Test task states when job not found."""
        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = None

            response = client.get("/traces/job/nonexistent/task-states")

            assert response.status_code == 404

    def test_get_task_states_fallback_task_id(self, client, mock_group_context):
        """Test task states generates fallback task_id from event_context when no task_id in metadata."""
        mock_trace_started = MagicMock()
        mock_trace_started.event_type = "task_started"
        mock_trace_started.trace_metadata = {}
        mock_trace_started.event_context = "Unnamed Task"
        mock_trace_started.created_at = datetime(2024, 1, 1, 10, 0, 0)

        mock_result = MagicMock()
        mock_result.traces = [mock_trace_started]

        with patch('src.api.execution_trace_router.ExecutionTraceService.get_traces_by_job_id') as mock_get_traces:
            mock_get_traces.return_value = mock_result

            response = client.get("/traces/job/job-123/task-states")

            assert response.status_code == 200
            data = response.json()
            # Should have a generated task_id based on hash of event_context
            assert len(data) == 1
            task_data = list(data.values())[0]
            assert task_data["status"] == "running"
            assert task_data["task_name"] == "Unnamed Task"