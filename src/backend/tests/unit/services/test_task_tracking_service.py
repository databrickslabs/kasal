"""
Unit tests for TaskTrackingService.

Tests the functionality of task tracking operations including
task status management, job execution status tracking, and callback handling.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from datetime import datetime, UTC
from sqlalchemy.orm import Session

# Import the actual module to ensure it's loaded for coverage
import src.services.task_tracking_service

from fastapi import HTTPException, status

from src.services.task_tracking_service import TaskTrackingService, get_task_tracking_service
from src.schemas.task_tracking import (
    TaskStatusEnum, TaskStatusCreate, TaskStatusUpdate, TaskStatusResponse,
    JobExecutionStatusResponse, TaskStatusSchema
)
from src.repositories.task_tracking_repository import TaskTrackingRepository
from src.models.execution_history import TaskStatus as DBTaskStatus, ExecutionHistory


# Mock models
class MockTaskStatus:
    def __init__(self, id=1, job_id="job-123", task_id="task-456", 
                 status="running", agent_name="test_agent", 
                 started_at=None, completed_at=None):
        self.id = id
        self.job_id = job_id
        self.task_id = task_id
        self.status = status
        self.agent_name = agent_name
        self.started_at = started_at or datetime.now(UTC)
        self.completed_at = completed_at


class MockExecutionHistory:
    def __init__(self, id=1, job_id="job-123", status="running"):
        self.id = id
        self.job_id = job_id
        self.status = status


@pytest.fixture
def mock_repository():
    """Create a mock repository for testing."""
    return AsyncMock(spec=TaskTrackingRepository)


@pytest.fixture
def mock_sync_repository():
    """Create a mock synchronous repository for testing."""
    mock_repo = Mock(spec=TaskTrackingRepository)
    mock_repo.db = Mock(spec=Session)
    return mock_repo


@pytest.fixture
def task_tracking_service(mock_repository):
    """Create a TaskTrackingService instance with mocked repository."""
    return TaskTrackingService(mock_repository)


@pytest.fixture
def sync_task_tracking_service(mock_sync_repository):
    """Create a TaskTrackingService instance with sync repository."""
    return TaskTrackingService(mock_sync_repository)


class TestTaskTrackingService:
    """Test cases for TaskTrackingService."""

    def test_init(self, mock_repository):
        """Test service initialization."""
        service = TaskTrackingService(mock_repository)
        assert service.repository == mock_repository

    @pytest.mark.asyncio
    async def test_get_job_status_success(self, task_tracking_service, mock_repository):
        """Test successful job status retrieval."""
        # Arrange
        job_id = "job-123"
        mock_job_status = {
            "job_id": job_id,
            "status": "running",
            "tasks": [
                {
                    "id": 1,
                    "task_id": "task-456",
                    "status": "running",
                    "agent_name": "test_agent",
                    "started_at": datetime.now(UTC),
                    "completed_at": None
                }
            ]
        }
        mock_repository.get_job_execution_status.return_value = mock_job_status

        # Act
        result = await task_tracking_service.get_job_status(job_id)

        # Assert
        assert isinstance(result, JobExecutionStatusResponse)
        assert result.job_id == job_id
        assert result.status == "running"
        assert len(result.tasks) == 1
        mock_repository.get_job_execution_status.assert_called_once_with(job_id)

    @pytest.mark.asyncio
    async def test_get_job_status_not_found(self, task_tracking_service, mock_repository):
        """Test job status retrieval when job not found."""
        # Arrange
        job_id = "nonexistent-job"
        mock_repository.get_job_execution_status.side_effect = ValueError("Job not found")

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await task_tracking_service.get_job_status(job_id)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "Job not found with ID: nonexistent-job" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_job_status_internal_error(self, task_tracking_service, mock_repository):
        """Test job status retrieval with internal error."""
        # Arrange
        job_id = "job-123"
        mock_repository.get_job_execution_status.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await task_tracking_service.get_job_status(job_id)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to retrieve job execution status" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_all_tasks_success(self, task_tracking_service, mock_repository):
        """Test successful retrieval of all tasks."""
        # Arrange
        mock_tasks = [
            MockTaskStatus(id=1, job_id="job-1", task_id="task-1"),
            MockTaskStatus(id=2, job_id="job-2", task_id="task-2")
        ]
        mock_repository.get_all_tasks.return_value = mock_tasks

        # Act
        result = await task_tracking_service.get_all_tasks()

        # Assert
        assert len(result) == 2
        assert all(isinstance(task, TaskStatusResponse) for task in result)
        mock_repository.get_all_tasks.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_all_tasks_error(self, task_tracking_service, mock_repository):
        """Test get all tasks with error."""
        # Arrange
        mock_repository.get_all_tasks.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await task_tracking_service.get_all_tasks()
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to retrieve tasks" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_create_task_success(self, task_tracking_service, mock_repository):
        """Test successful task creation."""
        # Arrange
        task_data = TaskStatusCreate(
            job_id="job-123",
            task_id="task-456",
            status=TaskStatusEnum.RUNNING,
            agent_name="test_agent"
        )
        mock_task = MockTaskStatus(id=1, job_id="job-123", task_id="task-456")
        mock_repository.create_task.return_value = mock_task

        # Act
        result = await task_tracking_service.create_task(task_data)

        # Assert
        assert isinstance(result, TaskStatusResponse)
        assert result.job_id == "job-123"
        assert result.task_id == "task-456"
        mock_repository.create_task.assert_called_once_with(task_data)

    @pytest.mark.asyncio
    async def test_create_task_error(self, task_tracking_service, mock_repository):
        """Test task creation with error."""
        # Arrange
        task_data = TaskStatusCreate(
            job_id="job-123",
            task_id="task-456",
            status=TaskStatusEnum.RUNNING
        )
        mock_repository.create_task.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await task_tracking_service.create_task(task_data)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to create task" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_update_task_success(self, task_tracking_service, mock_repository):
        """Test successful task update."""
        # Arrange
        task_id = 1
        task_data = TaskStatusUpdate(status=TaskStatusEnum.COMPLETED)
        mock_task = MockTaskStatus(id=task_id, status="completed")
        mock_repository.update_task.return_value = mock_task

        # Act
        result = await task_tracking_service.update_task(task_id, task_data)

        # Assert
        assert isinstance(result, TaskStatusResponse)
        assert result.id == task_id
        mock_repository.update_task.assert_called_once_with(task_id, task_data)

    @pytest.mark.asyncio
    async def test_update_task_not_found(self, task_tracking_service, mock_repository):
        """Test task update when task not found."""
        # Arrange
        task_id = 999
        task_data = TaskStatusUpdate(status=TaskStatusEnum.COMPLETED)
        mock_repository.update_task.return_value = None

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await task_tracking_service.update_task(task_id, task_data)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert f"Task with ID {task_id} not found" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_update_task_http_exception_passthrough(self, task_tracking_service, mock_repository):
        """Test that HTTPException is passed through without wrapping."""
        # Arrange
        task_id = 1
        task_data = TaskStatusUpdate(status=TaskStatusEnum.COMPLETED)
        original_exception = HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Bad request")
        mock_repository.update_task.side_effect = original_exception

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await task_tracking_service.update_task(task_id, task_data)
        
        assert exc_info.value == original_exception

    @pytest.mark.asyncio
    async def test_update_task_general_error(self, task_tracking_service, mock_repository):
        """Test task update with general error."""
        # Arrange
        task_id = 1
        task_data = TaskStatusUpdate(status=TaskStatusEnum.COMPLETED)
        mock_repository.update_task.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await task_tracking_service.update_task(task_id, task_data)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to update task" in str(exc_info.value.detail)

    def test_for_crew_classmethod(self):
        """Test for_crew class method."""
        # Arrange
        mock_db = Mock(spec=Session)

        # Act
        with patch('src.services.task_tracking_service.TaskTrackingRepository') as mock_repo_class:
            mock_repo_instance = Mock()
            mock_repo_class.return_value = mock_repo_instance
            
            service = TaskTrackingService.for_crew(mock_db)

            # Assert
            assert isinstance(service, TaskTrackingService)
            assert service.repository == mock_repo_instance
            assert service.db == mock_db
            mock_repo_class.assert_called_once_with(mock_db)

    def test_for_crew_with_repo_classmethod(self, mock_sync_repository):
        """Test for_crew_with_repo class method."""
        # Arrange
        mock_sync_repository.db = Mock(spec=Session)

        # Act
        service = TaskTrackingService.for_crew_with_repo(mock_sync_repository)

        # Assert
        assert isinstance(service, TaskTrackingService)
        assert service.repository == mock_sync_repository
        assert service.db == mock_sync_repository.db

    def test_for_crew_with_repo_no_db_attribute(self):
        """Test for_crew_with_repo when repository has no db attribute."""
        # Arrange
        mock_repo = Mock(spec=TaskTrackingRepository)
        # Don't set db attribute

        # Act
        service = TaskTrackingService.for_crew_with_repo(mock_repo)

        # Assert
        assert isinstance(service, TaskTrackingService)
        assert service.repository == mock_repo
        assert service.db is None

    @pytest.mark.asyncio
    async def test_create_task_status_success(self, task_tracking_service, mock_repository):
        """Test successful task status creation."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        agent_name = "test_agent"
        mock_task = MockTaskStatus(id=1, job_id=job_id, task_id=task_id, agent_name=agent_name)
        mock_repository.create_task_status.return_value = mock_task

        # Act
        result = await task_tracking_service.create_task_status(job_id, task_id, agent_name)

        # Assert
        assert isinstance(result, TaskStatusResponse)
        assert result.job_id == job_id
        assert result.task_id == task_id
        assert result.agent_name == agent_name
        
        # Verify the repository was called with correct TaskStatusCreate
        call_args = mock_repository.create_task_status.call_args[0][0]
        assert call_args.job_id == job_id
        assert call_args.task_id == task_id
        assert call_args.status == TaskStatusEnum.RUNNING
        assert call_args.agent_name == agent_name

    @pytest.mark.asyncio
    async def test_create_task_status_without_agent(self, task_tracking_service, mock_repository):
        """Test task status creation without agent name."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        mock_task = MockTaskStatus(id=1, job_id=job_id, task_id=task_id, agent_name=None)
        mock_repository.create_task_status.return_value = mock_task

        # Act
        result = await task_tracking_service.create_task_status(job_id, task_id)

        # Assert
        assert isinstance(result, TaskStatusResponse)
        assert result.agent_name is None

    @pytest.mark.asyncio
    async def test_update_task_status_success(self, task_tracking_service, mock_repository):
        """Test successful task status update."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        new_status = TaskStatusEnum.COMPLETED
        mock_task = MockTaskStatus(id=1, job_id=job_id, task_id=task_id, status="completed")
        mock_repository.update_task_status.return_value = mock_task

        # Act
        with patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            result = await task_tracking_service.update_task_status(job_id, task_id, new_status)

            # Assert
            assert isinstance(result, TaskStatusResponse)
            assert result.job_id == job_id
            assert result.task_id == task_id
            mock_logger.info.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_task_status_not_found(self, task_tracking_service, mock_repository):
        """Test task status update when task not found."""
        # Arrange
        job_id = "job-123"
        task_id = "nonexistent-task"
        new_status = TaskStatusEnum.COMPLETED
        mock_repository.update_task_status.return_value = None

        # Act
        with patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            result = await task_tracking_service.update_task_status(job_id, task_id, new_status)

            # Assert
            assert result is None
            mock_logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_task_status_success(self, task_tracking_service, mock_repository):
        """Test successful task status retrieval."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        mock_task = MockTaskStatus(id=1, job_id=job_id, task_id=task_id)
        mock_repository.get_task_status.return_value = mock_task

        # Act
        result = await task_tracking_service.get_task_status(job_id, task_id)

        # Assert
        assert isinstance(result, TaskStatusResponse)
        assert result.job_id == job_id
        assert result.task_id == task_id
        mock_repository.get_task_status.assert_called_once_with(job_id, task_id)

    @pytest.mark.asyncio
    async def test_get_task_status_not_found(self, task_tracking_service, mock_repository):
        """Test task status retrieval when task not found."""
        # Arrange
        job_id = "job-123"
        task_id = "nonexistent-task"
        mock_repository.get_task_status.return_value = None

        # Act
        result = await task_tracking_service.get_task_status(job_id, task_id)

        # Assert
        assert result is None

    def test_get_task_status_by_task_id_sync(self, task_tracking_service):
        """Test synchronous task status retrieval by task ID."""
        # Arrange
        task_id = "task-456"

        # Act
        result = task_tracking_service.get_task_status_by_task_id_sync(task_id)

        # Assert
        assert result is None  # Always returns None as per implementation

    @pytest.mark.asyncio
    async def test_get_task_status_by_task_id_success(self, task_tracking_service, mock_repository):
        """Test successful task status retrieval by task ID."""
        # Arrange
        task_id = "task-456"
        mock_task = MockTaskStatus(id=1, task_id=task_id)
        mock_repository.get_task_status_by_task_id.return_value = mock_task

        # Act
        result = await task_tracking_service.get_task_status_by_task_id(task_id)

        # Assert
        assert isinstance(result, TaskStatusResponse)
        assert result.task_id == task_id
        mock_repository.get_task_status_by_task_id.assert_called_once_with(task_id)

    @pytest.mark.asyncio
    async def test_get_task_status_by_task_id_not_found(self, task_tracking_service, mock_repository):
        """Test task status retrieval by task ID when not found."""
        # Arrange
        task_id = "nonexistent-task"
        mock_repository.get_task_status_by_task_id.return_value = None

        # Act
        result = await task_tracking_service.get_task_status_by_task_id(task_id)

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_get_all_task_statuses_success(self, task_tracking_service, mock_repository):
        """Test successful retrieval of all task statuses for a job."""
        # Arrange
        job_id = "job-123"
        mock_tasks = [
            MockTaskStatus(id=1, job_id=job_id, task_id="task-1"),
            MockTaskStatus(id=2, job_id=job_id, task_id="task-2")
        ]
        mock_repository.get_all_task_statuses.return_value = mock_tasks

        # Act
        result = await task_tracking_service.get_all_task_statuses(job_id)

        # Assert
        assert len(result) == 2
        assert all(isinstance(task, TaskStatusResponse) for task in result)
        assert all(task.job_id == job_id for task in result)
        mock_repository.get_all_task_statuses.assert_called_once_with(job_id)

    @pytest.mark.asyncio
    async def test_create_task_statuses_for_job_success(self, task_tracking_service, mock_repository):
        """Test successful creation of task statuses for a job."""
        # Arrange
        job_id = "job-123"
        tasks_config = {
            "task-1": {"description": "First task"},
            "task-2": {"description": "Second task"}
        }
        mock_tasks = [
            MockTaskStatus(id=1, job_id=job_id, task_id="task-1"),
            MockTaskStatus(id=2, job_id=job_id, task_id="task-2")
        ]
        mock_repository.create_task_statuses_for_job.return_value = mock_tasks

        # Act
        result = await task_tracking_service.create_task_statuses_for_job(job_id, tasks_config)

        # Assert
        assert len(result) == 2
        assert all(isinstance(task, TaskStatusResponse) for task in result)
        mock_repository.create_task_statuses_for_job.assert_called_once_with(job_id, tasks_config)

    def test_create_task_callbacks_structure(self, task_tracking_service):
        """Test that create_task_callbacks returns correct structure."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"

        # Act
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)

        # Assert
        assert isinstance(callbacks, dict)
        assert "on_start" in callbacks
        assert "on_end" in callbacks
        assert "on_error" in callbacks
        assert callable(callbacks["on_start"])
        assert callable(callbacks["on_end"])
        assert callable(callbacks["on_error"])

    def test_create_task_callbacks_on_start(self, task_tracking_service):
        """Test on_start callback functionality."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)

        # Act & Assert
        with patch('src.services.task_tracking_service.asyncio.run') as mock_run, \
             patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            
            callbacks["on_start"]()
            mock_run.assert_called_once()

    def test_create_task_callbacks_on_start_with_error(self, task_tracking_service):
        """Test on_start callback with error handling."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)

        # Act & Assert
        with patch('src.services.task_tracking_service.asyncio.run') as mock_run, \
             patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            
            mock_run.side_effect = Exception("Test error")
            callbacks["on_start"]()
            mock_logger.error.assert_called_once()

    def test_create_task_callbacks_on_end(self, task_tracking_service):
        """Test on_end callback functionality."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        test_output = "Task completed successfully"
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)

        # Act & Assert
        with patch('src.services.task_tracking_service.asyncio.run') as mock_run, \
             patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            
            result = callbacks["on_end"](test_output)
            assert result == test_output
            mock_run.assert_called_once()
            mock_logger.info.assert_called_once()

    def test_create_task_callbacks_on_end_with_error(self, task_tracking_service):
        """Test on_end callback with error handling."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        test_output = "Task completed successfully"
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)

        # Act & Assert
        with patch('src.services.task_tracking_service.asyncio.run') as mock_run, \
             patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            
            mock_run.side_effect = Exception("Test error")
            result = callbacks["on_end"](test_output)
            assert result == test_output
            mock_logger.error.assert_called_once()

    def test_create_task_callbacks_on_error(self, task_tracking_service):
        """Test on_error callback functionality."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        test_error = Exception("Test error")
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)

        # Act & Assert
        with patch('src.services.task_tracking_service.asyncio.run') as mock_run, \
             patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            
            result = callbacks["on_error"](test_error)
            assert result == test_error
            mock_logger.error.assert_called()

    def test_create_task_callbacks_on_error_with_db_and_execution_history(self, task_tracking_service):
        """Test on_error callback with database and execution history."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        test_error = Exception("Test error")
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)
        
        # Set up service with db attribute
        mock_db = Mock(spec=Session)
        mock_execution = MockExecutionHistory(id=1, job_id=job_id)
        mock_db.query.return_value.filter.return_value.first.return_value = mock_execution
        task_tracking_service.db = mock_db

        # Act & Assert
        with patch('src.services.task_tracking_service.asyncio.run') as mock_run, \
             patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            
            result = callbacks["on_error"](test_error)
            assert result == test_error
            mock_logger.error.assert_called()
            # Verify asyncio.run was called multiple times (once for update_task_status, once for record_error_trace)
            assert mock_run.call_count >= 2

    def test_create_task_callbacks_on_error_with_db_no_execution_history(self, task_tracking_service):
        """Test on_error callback with database but no execution history."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        test_error = Exception("Test error")
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)
        
        # Set up service with db attribute but no execution history
        mock_db = Mock(spec=Session)
        mock_db.query.return_value.filter.return_value.first.return_value = None
        task_tracking_service.db = mock_db

        # Act & Assert
        with patch('src.services.task_tracking_service.asyncio.run') as mock_run, \
             patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            
            result = callbacks["on_error"](test_error)
            assert result == test_error
            mock_logger.error.assert_called()

    def test_create_task_callbacks_on_error_with_trace_error(self, task_tracking_service):
        """Test on_error callback when error trace recording fails."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        test_error = Exception("Test error")
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)
        
        # Set up service with db attribute
        mock_db = Mock(spec=Session)
        mock_execution = MockExecutionHistory(id=1, job_id=job_id)
        mock_db.query.return_value.filter.return_value.first.return_value = mock_execution
        task_tracking_service.db = mock_db

        # Act & Assert
        with patch('src.services.task_tracking_service.asyncio.run') as mock_run, \
             patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            
            # Make the second asyncio.run call (error trace) fail
            mock_run.side_effect = [None, Exception("Trace error")]
            
            result = callbacks["on_error"](test_error)
            assert result == test_error
            mock_logger.error.assert_called()

    def test_create_task_callbacks_on_error_with_general_error(self, task_tracking_service):
        """Test on_error callback with general error handling."""
        # Arrange
        job_id = "job-123"
        task_id = "task-456"
        test_error = Exception("Test error")
        callbacks = task_tracking_service.create_task_callbacks(job_id, task_id)

        # Act & Assert
        with patch('src.services.task_tracking_service.asyncio.run') as mock_run, \
             patch('src.services.task_tracking_service.crew_logger') as mock_logger:
            
            mock_run.side_effect = Exception("General error")
            result = callbacks["on_error"](test_error)
            assert result == test_error
            mock_logger.error.assert_called()


class TestGetTaskTrackingService:
    """Test cases for get_task_tracking_service dependency function."""

    @pytest.mark.asyncio
    async def test_get_task_tracking_service_success(self):
        """Test successful creation of TaskTrackingService dependency."""
        # Act
        with patch('src.services.task_tracking_service.TaskTrackingRepository') as mock_repo_class:
            mock_repo_instance = Mock()
            mock_repo_class.return_value = mock_repo_instance
            
            # Use the generator
            service_gen = get_task_tracking_service()
            service = await service_gen.__anext__()

            # Assert
            assert isinstance(service, TaskTrackingService)
            assert service.repository == mock_repo_instance
            mock_repo_class.assert_called_once_with()

            # Test cleanup
            try:
                await service_gen.__anext__()
            except StopAsyncIteration:
                pass  # Expected

    @pytest.mark.asyncio
    async def test_get_task_tracking_service_cleanup(self):
        """Test that cleanup happens properly in dependency function."""
        # Act
        with patch('src.services.task_tracking_service.TaskTrackingRepository') as mock_repo_class:
            mock_repo_instance = Mock()
            mock_repo_class.return_value = mock_repo_instance
            
            # Simulate the dependency injection lifecycle
            async def simulate_dependency():
                async for service in get_task_tracking_service():
                    yield service
            
            # Use the dependency
            async for service in simulate_dependency():
                assert isinstance(service, TaskTrackingService)
                break


class TestTaskTrackingServiceIntegration:
    """Integration tests that exercise real code paths for coverage."""

    def test_create_real_service_instance(self):
        """Test creating a real service instance."""
        # Create a real repository mock that isn't completely mocked
        repository = Mock()
        service = TaskTrackingService(repository)
        
        # Verify initialization
        assert service.repository == repository
        
        # Test the classmethod constructors
        with patch('src.services.task_tracking_service.TaskTrackingRepository') as mock_repo_class:
            mock_repo_instance = Mock()
            mock_repo_class.return_value = mock_repo_instance
            mock_db = Mock(spec=Session)
            
            crew_service = TaskTrackingService.for_crew(mock_db)
            assert isinstance(crew_service, TaskTrackingService)
            assert crew_service.db == mock_db
            
            crew_service2 = TaskTrackingService.for_crew_with_repo(mock_repo_instance)
            assert isinstance(crew_service2, TaskTrackingService)

    def test_synchronous_methods(self):
        """Test synchronous methods for complete coverage."""
        repository = Mock()
        service = TaskTrackingService(repository)
        
        # Test get_task_status_by_task_id_sync
        result = service.get_task_status_by_task_id_sync("test-task")
        assert result is None
        
        # Test create_task_callbacks structure
        callbacks = service.create_task_callbacks("job-123", "task-456")
        assert "on_start" in callbacks
        assert "on_end" in callbacks  
        assert "on_error" in callbacks
        assert callable(callbacks["on_start"])
        assert callable(callbacks["on_end"])
        assert callable(callbacks["on_error"])

    def test_logger_initialization(self):
        """Test that loggers are properly initialized."""
        # This will import and execute module-level code
        from src.services.task_tracking_service import logger, crew_logger
        
        assert logger is not None
        assert crew_logger is not None