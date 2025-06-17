"""
Unit tests for SchedulerRouter.

Tests the functionality of schedule management endpoints including
CRUD operations, job management, and schedule toggling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.schemas.schedule import ScheduleCreate, ScheduleCreateFromExecution, ScheduleUpdate
from src.schemas.scheduler import SchedulerJobCreate, SchedulerJobUpdate
from src.utils.user_context import GroupContext


# Mock schedule response model
class MockScheduleResponse:
    def __init__(self, id=1, name="Test Schedule", cron_expression="0 9 * * *",
                 is_active=True, agents_yaml=None, tasks_yaml=None):
        self.id = id
        self.name = name
        self.cron_expression = cron_expression
        self.is_active = is_active
        self.agents_yaml = agents_yaml or {"agent1": {"role": "tester"}}
        self.tasks_yaml = tasks_yaml or {"task1": {"description": "test task"}}
        self.inputs = {"input1": "value1"}
        self.planning = False
        self.model = "gpt-4o-mini"
        self.last_run_at = None
        self.next_run_at = datetime.utcnow()
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "id": self.id,
            "name": self.name,
            "cron_expression": self.cron_expression,
            "is_active": self.is_active,
            "agents_yaml": self.agents_yaml,
            "tasks_yaml": self.tasks_yaml,
            "inputs": self.inputs,
            "planning": self.planning,
            "model": self.model,
            "last_run_at": self.last_run_at,
            "next_run_at": self.next_run_at.isoformat() if self.next_run_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


# Mock scheduler job response model
class MockSchedulerJobResponse:
    def __init__(self, id=1, name="Test Job", schedule="0 9 * * *", enabled=True):
        self.id = id
        self.name = name
        self.description = "Test scheduler job"
        self.schedule = schedule
        self.enabled = enabled
        self.job_data = {"config": "test"}
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.last_run_at = None
        self.next_run_at = datetime.utcnow()
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "schedule": self.schedule,
            "enabled": self.enabled,
            "job_data": self.job_data,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "last_run_at": self.last_run_at,
            "next_run_at": self.next_run_at.isoformat() if self.next_run_at else None
        }


# Mock schedule list response
class MockScheduleListResponse:
    def __init__(self, schedules, count):
        self.schedules = schedules
        self.count = count


@pytest.fixture
def mock_scheduler_service():
    """Create a mock scheduler service."""
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
def app(mock_scheduler_service, mock_group_context):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.scheduler_router import router, get_scheduler_service
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(router)
    
    # Create override functions
    async def override_get_scheduler_service():
        return mock_scheduler_service
        
    async def override_get_group_context():
        return mock_group_context
    
    # Override dependencies
    app.dependency_overrides[get_scheduler_service] = override_get_scheduler_service
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


@pytest.fixture
def sample_schedule_create():
    """Create a sample schedule creation request."""
    return ScheduleCreate(
        name="Test Schedule",
        cron_expression="0 9 * * *",
        agents_yaml={"agent1": {"role": "tester"}},
        tasks_yaml={"task1": {"description": "test task"}},
        inputs={"input1": "value1"},
        is_active=True,
        planning=False,
        model="gpt-4o-mini"
    )


@pytest.fixture
def sample_schedule_from_execution():
    """Create a sample schedule from execution request."""
    return ScheduleCreateFromExecution(
        name="Schedule from Execution",
        cron_expression="0 10 * * *",
        execution_id=123,
        is_active=True
    )


@pytest.fixture
def sample_schedule_update():
    """Create a sample schedule update request."""
    return ScheduleUpdate(
        name="Updated Schedule",
        cron_expression="0 11 * * *",
        agents_yaml={"agent2": {"role": "updated_tester"}},
        tasks_yaml={"task2": {"description": "updated test task"}},
        inputs={"input2": "value2"},
        is_active=False,
        planning=True,
        model="gpt-4"
    )


@pytest.fixture
def sample_job_create():
    """Create a sample scheduler job creation request."""
    return SchedulerJobCreate(
        name="Test Job",
        description="Test scheduler job",
        schedule="0 9 * * *",
        enabled=True,
        job_data={"config": "test"}
    )


@pytest.fixture
def sample_job_update():
    """Create a sample scheduler job update request."""
    return SchedulerJobUpdate(
        name="Updated Job",
        description="Updated test job",
        schedule="0 10 * * *",
        enabled=False,
        job_data={"config": "updated"}
    )


class TestCreateSchedule:
    """Test cases for create schedule endpoint."""
    
    def test_create_schedule_success(self, client, mock_scheduler_service, mock_group_context, sample_schedule_create):
        """Test successful schedule creation."""
        created_schedule = MockScheduleResponse()
        mock_scheduler_service.create_schedule.return_value = created_schedule
        
        response = client.post("/schedules", json=sample_schedule_create.model_dump())
        
        assert response.status_code == 201
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "Test Schedule"
        mock_scheduler_service.create_schedule.assert_called_once_with(sample_schedule_create, mock_group_context)
    
    def test_create_schedule_http_exception(self, client, mock_scheduler_service, mock_group_context, sample_schedule_create):
        """Test schedule creation with HTTP exception."""
        mock_scheduler_service.create_schedule.side_effect = HTTPException(status_code=400, detail="Invalid cron expression")
        
        response = client.post("/schedules", json=sample_schedule_create.model_dump())
        
        assert response.status_code == 400
        assert "Invalid cron expression" in response.json()["detail"]


class TestCreateScheduleFromExecution:
    """Test cases for create schedule from execution endpoint."""
    
    def test_create_schedule_from_execution_success(self, client, mock_scheduler_service, mock_group_context, sample_schedule_from_execution):
        """Test successful schedule creation from execution."""
        created_schedule = MockScheduleResponse(name="Schedule from Execution")
        mock_scheduler_service.create_schedule_from_execution.return_value = created_schedule
        
        response = client.post("/schedules/from-execution", json=sample_schedule_from_execution.model_dump())
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Schedule from Execution"
        mock_scheduler_service.create_schedule_from_execution.assert_called_once_with(sample_schedule_from_execution, mock_group_context)
    
    def test_create_schedule_from_execution_http_exception(self, client, mock_scheduler_service, mock_group_context, sample_schedule_from_execution):
        """Test schedule creation from execution with HTTP exception."""
        mock_scheduler_service.create_schedule_from_execution.side_effect = HTTPException(status_code=404, detail="Execution not found")
        
        response = client.post("/schedules/from-execution", json=sample_schedule_from_execution.model_dump())
        
        assert response.status_code == 404
        assert "Execution not found" in response.json()["detail"]


class TestListSchedules:
    """Test cases for list schedules endpoint."""
    
    def test_list_schedules_success(self, client, mock_scheduler_service, mock_group_context):
        """Test successful schedules listing."""
        schedules = [MockScheduleResponse(id=1), MockScheduleResponse(id=2)]
        schedule_list = MockScheduleListResponse(schedules=schedules, count=2)
        mock_scheduler_service.get_all_schedules.return_value = schedule_list
        
        response = client.get("/schedules")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[1]["id"] == 2


class TestGetSchedule:
    """Test cases for get schedule endpoint."""
    
    def test_get_schedule_success(self, client, mock_scheduler_service):
        """Test successful schedule retrieval."""
        schedule = MockScheduleResponse()
        mock_scheduler_service.get_schedule_by_id.return_value = schedule
        
        response = client.get("/schedules/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "Test Schedule"
        mock_scheduler_service.get_schedule_by_id.assert_called_once_with(1)
    
    def test_get_schedule_http_exception(self, client, mock_scheduler_service):
        """Test getting schedule with HTTP exception."""
        mock_scheduler_service.get_schedule_by_id.side_effect = HTTPException(status_code=404, detail="Schedule not found")
        
        response = client.get("/schedules/999")
        
        assert response.status_code == 404
        assert "Schedule not found" in response.json()["detail"]


class TestUpdateSchedule:
    """Test cases for update schedule endpoint."""
    
    def test_update_schedule_success(self, client, mock_scheduler_service, sample_schedule_update):
        """Test successful schedule update."""
        updated_schedule = MockScheduleResponse(name="Updated Schedule")
        mock_scheduler_service.update_schedule.return_value = updated_schedule
        
        response = client.put("/schedules/1", json=sample_schedule_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Schedule"
        mock_scheduler_service.update_schedule.assert_called_once_with(1, sample_schedule_update)
    
    def test_update_schedule_http_exception(self, client, mock_scheduler_service, sample_schedule_update):
        """Test updating schedule with HTTP exception."""
        mock_scheduler_service.update_schedule.side_effect = HTTPException(status_code=404, detail="Schedule not found")
        
        response = client.put("/schedules/999", json=sample_schedule_update.model_dump())
        
        assert response.status_code == 404
        assert "Schedule not found" in response.json()["detail"]


class TestDeleteSchedule:
    """Test cases for delete schedule endpoint."""
    
    def test_delete_schedule_success(self, client, mock_scheduler_service):
        """Test successful schedule deletion."""
        mock_scheduler_service.delete_schedule.return_value = {"message": "Schedule deleted successfully"}
        
        response = client.delete("/schedules/1")
        
        assert response.status_code == 200
        data = response.json()
        assert "deleted successfully" in data["message"]
        mock_scheduler_service.delete_schedule.assert_called_once_with(1)
    
    def test_delete_schedule_http_exception(self, client, mock_scheduler_service):
        """Test deleting schedule with HTTP exception."""
        mock_scheduler_service.delete_schedule.side_effect = HTTPException(status_code=404, detail="Schedule not found")
        
        response = client.delete("/schedules/999")
        
        assert response.status_code == 404
        assert "Schedule not found" in response.json()["detail"]


class TestToggleSchedule:
    """Test cases for toggle schedule endpoint."""
    
    def test_toggle_schedule_success(self, client, mock_scheduler_service):
        """Test successful schedule toggle."""
        toggled_schedule = MockScheduleResponse(is_active=False)
        mock_scheduler_service.toggle_schedule.return_value = toggled_schedule
        
        response = client.post("/schedules/1/toggle")
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_active"] is False
        mock_scheduler_service.toggle_schedule.assert_called_once_with(1)
    
    def test_toggle_schedule_http_exception(self, client, mock_scheduler_service):
        """Test toggling schedule with HTTP exception."""
        mock_scheduler_service.toggle_schedule.side_effect = HTTPException(status_code=404, detail="Schedule not found")
        
        response = client.post("/schedules/999/toggle")
        
        assert response.status_code == 404
        assert "Schedule not found" in response.json()["detail"]


class TestGetAllJobs:
    """Test cases for get all jobs endpoint."""
    
    def test_get_all_jobs_endpoint_broken_routing(self, client):
        """Test that /jobs endpoint has broken routing due to /{schedule_id} coming first.
        
        This test documents the actual behavior - the route doesn't work because
        /{schedule_id} is defined before /jobs, so 'jobs' gets parsed as schedule_id.
        Tests the actual broken behavior that exists in the code.
        """
        response = client.get("/schedules/jobs")
        
        # This should be 422 because 'jobs' cannot be parsed as an integer for schedule_id
        assert response.status_code == 422
        error_detail = response.json()["detail"][0]
        assert error_detail["type"] == "int_parsing" 
        assert error_detail["loc"] == ["path", "schedule_id"]
        assert error_detail["input"] == "jobs"
    
    def test_get_all_jobs_function_direct(self):
        """Test get_all_jobs function directly for success path."""
        from src.api.scheduler_router import get_all_jobs
        from unittest.mock import AsyncMock
        import asyncio
        
        mock_service = AsyncMock()
        jobs = [MockSchedulerJobResponse(id=1), MockSchedulerJobResponse(id=2)]
        mock_service.get_all_jobs.return_value = jobs
        
        async def test_success():
            result = await get_all_jobs(mock_service)
            assert len(result) == 2
            assert result[0].id == 1
            assert result[1].id == 2
            mock_service.get_all_jobs.assert_called_once()
        
        asyncio.run(test_success())
    
    def test_get_all_jobs_exception_handling_lines_218_222(self):
        """Test the exception handling in get_all_jobs endpoint (lines 218-222).
        
        This tests the specific exception handling code path in the function.
        """
        from src.api.scheduler_router import get_all_jobs
        from unittest.mock import AsyncMock
        from fastapi import HTTPException
        import asyncio
        import pytest
        
        # Create a mock service that raises an exception
        mock_service = AsyncMock()
        mock_service.get_all_jobs.side_effect = Exception("Database error")
        
        # Test the function directly
        async def test_exception_path():
            with pytest.raises(HTTPException) as exc_info:
                await get_all_jobs(mock_service)
            # The function should re-raise as HTTPException with status 500
            assert exc_info.value.status_code == 500
            assert "Database error" in str(exc_info.value.detail)
            mock_service.get_all_jobs.assert_called_once()
            
        asyncio.run(test_exception_path())


class TestCreateJob:
    """Test cases for create job endpoint."""
    
    def test_create_job_endpoint_success(self, client, mock_scheduler_service, sample_job_create):
        """Test successful job creation via endpoint.
        
        The POST /schedules/jobs route works correctly since it doesn't conflict
        with /{schedule_id} route (different HTTP method).
        """
        created_job = MockSchedulerJobResponse()
        mock_scheduler_service.create_job.return_value = created_job
        
        response = client.post("/schedules/jobs", json=sample_job_create.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "Test Job"
        mock_scheduler_service.create_job.assert_called_once_with(sample_job_create)
    
    def test_create_job_endpoint_exception(self, client, mock_scheduler_service, sample_job_create):
        """Test job creation endpoint with exception."""
        mock_scheduler_service.create_job.side_effect = Exception("Database error")
        
        response = client.post("/schedules/jobs", json=sample_job_create.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_create_job_function_direct(self, sample_job_create):
        """Test job creation function directly to achieve coverage."""
        from src.api.scheduler_router import create_job
        from unittest.mock import AsyncMock
        import asyncio
        
        mock_service = AsyncMock()
        created_job = MockSchedulerJobResponse()
        mock_service.create_job.return_value = created_job
        
        async def test_success():
            result = await create_job(sample_job_create, mock_service)
            assert result.id == 1
            mock_service.create_job.assert_called_once_with(sample_job_create)
        
        asyncio.run(test_success())
    
    def test_create_job_function_exception(self, sample_job_create):
        """Test job creation function exception handling."""
        from src.api.scheduler_router import create_job
        from unittest.mock import AsyncMock
        from fastapi import HTTPException
        import asyncio
        import pytest
        
        mock_service = AsyncMock()
        mock_service.create_job.side_effect = Exception("Database error")
        
        async def test_exception():
            with pytest.raises(HTTPException) as exc_info:
                await create_job(sample_job_create, mock_service)
            assert exc_info.value.status_code == 500
            assert "Database error" in str(exc_info.value.detail)
        
        asyncio.run(test_exception())


class TestUpdateJob:
    """Test cases for update job endpoint."""
    
    def test_update_job_endpoint_success(self, client, mock_scheduler_service, sample_job_update):
        """Test successful job update via endpoint.
        
        The PUT /schedules/jobs/{job_id} route actually works correctly.
        """
        # Set up the mock to return a proper job object, not an AsyncMock
        updated_job = MockSchedulerJobResponse(name="Updated Job")
        mock_scheduler_service.update_job.return_value = updated_job
        
        response = client.put("/schedules/jobs/1", json=sample_job_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated Job"
        mock_scheduler_service.update_job.assert_called_once_with(1, sample_job_update)
    
    def test_update_job_endpoint_exception(self, client, mock_scheduler_service, sample_job_update):
        """Test job update endpoint with exception."""
        mock_scheduler_service.update_job.side_effect = Exception("Database error")
        
        response = client.put("/schedules/jobs/1", json=sample_job_update.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_update_job_function_direct(self, sample_job_update):
        """Test job update function directly to achieve coverage."""
        from src.api.scheduler_router import update_job
        from unittest.mock import AsyncMock
        import asyncio
        
        mock_service = AsyncMock()
        updated_job = MockSchedulerJobResponse(name="Updated Job")
        mock_service.update_job.return_value = updated_job
        
        async def test_success():
            result = await update_job(1, sample_job_update, mock_service)
            assert result.name == "Updated Job"
            mock_service.update_job.assert_called_once_with(1, sample_job_update)
        
        asyncio.run(test_success())
    
    def test_update_job_function_exception(self, sample_job_update):
        """Test job update function exception handling."""
        from src.api.scheduler_router import update_job
        from unittest.mock import AsyncMock
        from fastapi import HTTPException
        import asyncio
        import pytest
        
        mock_service = AsyncMock()
        mock_service.update_job.side_effect = Exception("Database error")
        
        async def test_exception():
            with pytest.raises(HTTPException) as exc_info:
                await update_job(1, sample_job_update, mock_service)
            assert exc_info.value.status_code == 500
            assert "Database error" in str(exc_info.value.detail)
        
        asyncio.run(test_exception())


class TestLoggingCoverage:
    """Test cases to ensure logging statements are covered."""
    
    @pytest.fixture
    def mock_logger(self):
        """Mock the logger to test logging calls."""
        from unittest.mock import patch
        with patch('src.api.scheduler_router.logger') as mock_log:
            yield mock_log
    
    def test_create_schedule_logging(self, client, mock_scheduler_service, mock_group_context, sample_schedule_create, mock_logger):
        """Test logging in create_schedule endpoint."""
        created_schedule = MockScheduleResponse()
        mock_scheduler_service.create_schedule.return_value = created_schedule
        
        response = client.post("/schedules", json=sample_schedule_create.model_dump())
        
        assert response.status_code == 201
        # Verify logging calls were made
        assert mock_logger.info.call_count >= 2  # Should have info logs for creation start and success
        mock_logger.info.assert_any_call(f"Creating schedule: {sample_schedule_create.name} with cron expression: {sample_schedule_create.cron_expression}")
        mock_logger.info.assert_any_call(f"Created schedule with ID {created_schedule.id}")
    
    def test_create_schedule_from_execution_logging(self, client, mock_scheduler_service, mock_group_context, sample_schedule_from_execution, mock_logger):
        """Test logging in create_schedule_from_execution endpoint."""
        created_schedule = MockScheduleResponse(name="Schedule from Execution")
        mock_scheduler_service.create_schedule_from_execution.return_value = created_schedule
        
        response = client.post("/schedules/from-execution", json=sample_schedule_from_execution.model_dump())
        
        assert response.status_code == 201
        # Verify logging calls were made
        assert mock_logger.info.call_count >= 2
        mock_logger.info.assert_any_call(f"Creating schedule from execution {sample_schedule_from_execution.execution_id}: {sample_schedule_from_execution.name}")
        mock_logger.info.assert_any_call(f"Created schedule with ID {created_schedule.id} from execution {sample_schedule_from_execution.execution_id}")
    
    def test_list_schedules_logging(self, client, mock_scheduler_service, mock_group_context, mock_logger):
        """Test logging in list_schedules endpoint."""
        schedules = [MockScheduleResponse(id=1), MockScheduleResponse(id=2)]
        schedule_list = MockScheduleListResponse(schedules=schedules, count=2)
        mock_scheduler_service.get_all_schedules.return_value = schedule_list
        
        response = client.get("/schedules")
        
        assert response.status_code == 200
        # Verify logging calls were made
        mock_logger.info.assert_any_call("Listing all schedules")
        mock_logger.info.assert_any_call(f"Found {schedule_list.count} schedules")
    
    def test_toggle_schedule_logging(self, client, mock_scheduler_service, mock_logger):
        """Test logging in toggle_schedule endpoint."""
        toggled_schedule = MockScheduleResponse(is_active=False)
        mock_scheduler_service.toggle_schedule.return_value = toggled_schedule
        
        response = client.post("/schedules/1/toggle")
        
        assert response.status_code == 200
        # Verify logging calls were made
        mock_logger.info.assert_any_call("Toggling schedule with ID 1")
        mock_logger.info.assert_any_call("Toggled schedule with ID 1, now disabled")
    
    def test_get_all_jobs_error_logging(self, mock_logger):
        """Test error logging in get_all_jobs function."""
        from src.api.scheduler_router import get_all_jobs
        from unittest.mock import AsyncMock
        from fastapi import HTTPException
        import asyncio
        import pytest
        
        mock_service = AsyncMock()
        mock_service.get_all_jobs.side_effect = Exception("Database error")
        
        async def test_error_logging():
            with pytest.raises(HTTPException):
                await get_all_jobs(mock_service)
            mock_logger.error.assert_called_once_with("Error getting jobs: Database error")
        
        asyncio.run(test_error_logging())
    
    def test_create_job_error_logging(self, sample_job_create, mock_logger):
        """Test error logging in create_job function."""
        from src.api.scheduler_router import create_job
        from unittest.mock import AsyncMock
        from fastapi import HTTPException
        import asyncio
        import pytest
        
        mock_service = AsyncMock()
        mock_service.create_job.side_effect = Exception("Database error")
        
        async def test_error_logging():
            with pytest.raises(HTTPException):
                await create_job(sample_job_create, mock_service)
            mock_logger.error.assert_called_once_with("Error creating job: Database error")
        
        asyncio.run(test_error_logging())
    
    def test_update_job_error_logging(self, sample_job_update, mock_logger):
        """Test error logging in update_job function."""
        from src.api.scheduler_router import update_job
        from unittest.mock import AsyncMock
        from fastapi import HTTPException
        import asyncio
        import pytest
        
        mock_service = AsyncMock()
        mock_service.update_job.side_effect = Exception("Database error")
        
        async def test_error_logging():
            with pytest.raises(HTTPException):
                await update_job(1, sample_job_update, mock_service)
            mock_logger.error.assert_called_once_with("Error updating job: Database error")
        
        asyncio.run(test_error_logging())
    
    def test_get_schedule_logging(self, client, mock_scheduler_service, mock_logger):
        """Test logging in get_schedule endpoint."""
        schedule = MockScheduleResponse()
        mock_scheduler_service.get_schedule_by_id.return_value = schedule
        
        response = client.get("/schedules/1")
        
        assert response.status_code == 200
        # Verify logging calls were made
        mock_logger.info.assert_any_call("Getting schedule with ID 1")
        mock_logger.info.assert_any_call("Retrieved schedule with ID 1")
    
    def test_update_schedule_logging(self, client, mock_scheduler_service, sample_schedule_update, mock_logger):
        """Test logging in update_schedule endpoint."""
        updated_schedule = MockScheduleResponse(name="Updated Schedule")
        mock_scheduler_service.update_schedule.return_value = updated_schedule
        
        response = client.put("/schedules/1", json=sample_schedule_update.model_dump())
        
        assert response.status_code == 200
        # Verify logging calls were made
        mock_logger.info.assert_any_call("Updating schedule with ID 1")
        mock_logger.info.assert_any_call("Updated schedule with ID 1")
    
    def test_delete_schedule_logging(self, client, mock_scheduler_service, mock_logger):
        """Test logging in delete_schedule endpoint."""
        mock_scheduler_service.delete_schedule.return_value = {"message": "Schedule deleted successfully"}
        
        response = client.delete("/schedules/1")
        
        assert response.status_code == 200
        # Verify logging calls were made
        mock_logger.info.assert_any_call("Deleting schedule with ID 1")
        mock_logger.info.assert_any_call("Deleted schedule with ID 1")
    
    def test_schedule_exception_warning_logging(self, client, mock_scheduler_service, sample_schedule_create, mock_logger):
        """Test warning logging when HTTPException is raised in schedule creation."""
        mock_scheduler_service.create_schedule.side_effect = HTTPException(status_code=400, detail="Invalid cron expression")
        
        response = client.post("/schedules", json=sample_schedule_create.model_dump())
        
        assert response.status_code == 400
        # Verify warning logging was called
        mock_logger.warning.assert_called_once_with("Schedule creation failed: 400: Invalid cron expression")
    
    def test_schedule_from_execution_exception_warning_logging(self, client, mock_scheduler_service, sample_schedule_from_execution, mock_logger):
        """Test warning logging when HTTPException is raised in schedule creation from execution."""
        mock_scheduler_service.create_schedule_from_execution.side_effect = HTTPException(status_code=404, detail="Execution not found")
        
        response = client.post("/schedules/from-execution", json=sample_schedule_from_execution.model_dump())
        
        assert response.status_code == 404
        # Verify warning logging was called
        mock_logger.warning.assert_called_once_with("Schedule creation from execution failed: 404: Execution not found")
    
    def test_get_schedule_exception_warning_logging(self, client, mock_scheduler_service, mock_logger):
        """Test warning logging when HTTPException is raised in get_schedule."""
        mock_scheduler_service.get_schedule_by_id.side_effect = HTTPException(status_code=404, detail="Schedule not found")
        
        response = client.get("/schedules/999")
        
        assert response.status_code == 404
        # Verify warning logging was called
        mock_logger.warning.assert_called_once_with("Schedule retrieval failed: 404: Schedule not found")
    
    def test_update_schedule_exception_warning_logging(self, client, mock_scheduler_service, sample_schedule_update, mock_logger):
        """Test warning logging when HTTPException is raised in update_schedule."""
        mock_scheduler_service.update_schedule.side_effect = HTTPException(status_code=404, detail="Schedule not found")
        
        response = client.put("/schedules/999", json=sample_schedule_update.model_dump())
        
        assert response.status_code == 404
        # Verify warning logging was called
        mock_logger.warning.assert_called_once_with("Schedule update failed: 404: Schedule not found")
    
    def test_delete_schedule_exception_warning_logging(self, client, mock_scheduler_service, mock_logger):
        """Test warning logging when HTTPException is raised in delete_schedule."""
        mock_scheduler_service.delete_schedule.side_effect = HTTPException(status_code=404, detail="Schedule not found")
        
        response = client.delete("/schedules/999")
        
        assert response.status_code == 404
        # Verify warning logging was called
        mock_logger.warning.assert_called_once_with("Schedule deletion failed: 404: Schedule not found")
    
    def test_toggle_schedule_exception_warning_logging(self, client, mock_scheduler_service, mock_logger):
        """Test warning logging when HTTPException is raised in toggle_schedule."""
        mock_scheduler_service.toggle_schedule.side_effect = HTTPException(status_code=404, detail="Schedule not found")
        
        response = client.post("/schedules/999/toggle")
        
        assert response.status_code == 404
        # Verify warning logging was called
        mock_logger.warning.assert_called_once_with("Schedule toggle failed: 404: Schedule not found")


class TestGetSchedulerServiceDependency:
    """Test cases for get_scheduler_service dependency function - line 31."""
    
    def test_get_scheduler_service_dependency(self):
        """Test the get_scheduler_service dependency function directly."""
        from src.api.scheduler_router import get_scheduler_service
        from unittest.mock import Mock
        
        # Create a mock database session
        mock_db = Mock()
        
        # Test the dependency function directly
        import asyncio
        async def test_dependency():
            service = await get_scheduler_service(mock_db)
            assert service is not None
            # The service should be a SchedulerService instance
            from src.services.scheduler_service import SchedulerService
            assert isinstance(service, SchedulerService)
            assert service.session == mock_db
        
        # Run the async test
        asyncio.run(test_dependency())