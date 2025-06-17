"""
Unit tests for task tracking schemas.

Tests the functionality of Pydantic schemas for task tracking and status management
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.task_tracking import (
    TaskStatusEnum, TaskStatusBase, TaskStatusCreate, TaskStatusUpdate,
    TaskStatusResponse, TaskCallbackMetadata, TaskErrorTrace, TaskStatusSchema,
    JobExecutionStatusResponse
)


class TestTaskStatusEnum:
    """Test cases for TaskStatusEnum."""
    
    def test_task_status_enum_values(self):
        """Test TaskStatusEnum values."""
        assert TaskStatusEnum.RUNNING == "running"
        assert TaskStatusEnum.COMPLETED == "completed"
        assert TaskStatusEnum.FAILED == "failed"
    
    def test_task_status_enum_all_values(self):
        """Test that all expected TaskStatusEnum values are present."""
        expected_values = {"running", "completed", "failed"}
        actual_values = {status.value for status in TaskStatusEnum}
        assert actual_values == expected_values
    
    def test_task_status_enum_iteration(self):
        """Test iterating over TaskStatusEnum."""
        statuses = list(TaskStatusEnum)
        assert len(statuses) == 3
        assert TaskStatusEnum.RUNNING in statuses
        assert TaskStatusEnum.COMPLETED in statuses
        assert TaskStatusEnum.FAILED in statuses


class TestTaskStatusBase:
    """Test cases for TaskStatusBase schema."""
    
    def test_valid_task_status_base_minimal(self):
        """Test TaskStatusBase with minimal required fields."""
        status_data = {
            "job_id": "job_123",
            "task_id": "task_456",
            "status": TaskStatusEnum.RUNNING
        }
        status = TaskStatusBase(**status_data)
        assert status.job_id == "job_123"
        assert status.task_id == "task_456"
        assert status.status == TaskStatusEnum.RUNNING
        assert status.agent_name is None
    
    def test_valid_task_status_base_complete(self):
        """Test TaskStatusBase with all fields."""
        status_data = {
            "job_id": "job_789",
            "task_id": "task_101",
            "status": TaskStatusEnum.COMPLETED,
            "agent_name": "data_analyst_agent"
        }
        status = TaskStatusBase(**status_data)
        assert status.job_id == "job_789"
        assert status.task_id == "task_101"
        assert status.status == TaskStatusEnum.COMPLETED
        assert status.agent_name == "data_analyst_agent"
    
    def test_task_status_base_missing_required_fields(self):
        """Test TaskStatusBase validation with missing required fields."""
        required_fields = ["job_id", "task_id", "status"]
        
        for missing_field in required_fields:
            status_data = {
                "job_id": "job_test",
                "task_id": "task_test",
                "status": TaskStatusEnum.RUNNING
            }
            del status_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                TaskStatusBase(**status_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_task_status_base_various_statuses(self):
        """Test TaskStatusBase with various status values."""
        for status in TaskStatusEnum:
            status_data = {
                "job_id": f"job_{status.value}",
                "task_id": f"task_{status.value}",
                "status": status
            }
            task_status = TaskStatusBase(**status_data)
            assert task_status.status == status
    
    def test_task_status_base_empty_strings(self):
        """Test TaskStatusBase with empty string fields."""
        status_data = {
            "job_id": "",
            "task_id": "",
            "status": TaskStatusEnum.RUNNING,
            "agent_name": ""
        }
        status = TaskStatusBase(**status_data)
        assert status.job_id == ""
        assert status.task_id == ""
        assert status.agent_name == ""


class TestTaskStatusCreate:
    """Test cases for TaskStatusCreate schema."""
    
    def test_task_status_create_inheritance(self):
        """Test that TaskStatusCreate inherits from TaskStatusBase."""
        create_data = {
            "job_id": "create_job_001",
            "task_id": "create_task_001",
            "status": TaskStatusEnum.RUNNING,
            "agent_name": "test_agent"
        }
        create_status = TaskStatusCreate(**create_data)
        
        # Should have all base class attributes
        assert hasattr(create_status, 'job_id')
        assert hasattr(create_status, 'task_id')
        assert hasattr(create_status, 'status')
        assert hasattr(create_status, 'agent_name')
        
        # Values should match
        assert create_status.job_id == "create_job_001"
        assert create_status.task_id == "create_task_001"
        assert create_status.status == TaskStatusEnum.RUNNING
        assert create_status.agent_name == "test_agent"
    
    def test_task_status_create_same_validation(self):
        """Test that TaskStatusCreate has same validation as base."""
        # Should fail with missing required fields
        with pytest.raises(ValidationError):
            TaskStatusCreate(job_id="test")
        
        # Should succeed with required fields
        create_status = TaskStatusCreate(
            job_id="valid_job",
            task_id="valid_task",
            status=TaskStatusEnum.RUNNING
        )
        assert create_status.status == TaskStatusEnum.RUNNING


class TestTaskStatusUpdate:
    """Test cases for TaskStatusUpdate schema."""
    
    def test_valid_task_status_update(self):
        """Test TaskStatusUpdate with valid data."""
        update_data = {"status": TaskStatusEnum.COMPLETED}
        update = TaskStatusUpdate(**update_data)
        assert update.status == TaskStatusEnum.COMPLETED
    
    def test_task_status_update_various_statuses(self):
        """Test TaskStatusUpdate with various status values."""
        for status in TaskStatusEnum:
            update = TaskStatusUpdate(status=status)
            assert update.status == status
    
    def test_task_status_update_missing_status(self):
        """Test TaskStatusUpdate validation with missing status."""
        with pytest.raises(ValidationError) as exc_info:
            TaskStatusUpdate()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "status" in missing_fields


class TestTaskStatusResponse:
    """Test cases for TaskStatusResponse schema."""
    
    def test_valid_task_status_response_minimal(self):
        """Test TaskStatusResponse with minimal required fields."""
        now = datetime.now()
        response_data = {
            "job_id": "response_job_123",
            "task_id": "response_task_456",
            "status": TaskStatusEnum.RUNNING,
            "id": 789,
            "started_at": now
        }
        response = TaskStatusResponse(**response_data)
        assert response.job_id == "response_job_123"
        assert response.task_id == "response_task_456"
        assert response.status == TaskStatusEnum.RUNNING
        assert response.id == 789
        assert response.started_at == now
        assert response.completed_at is None
    
    def test_valid_task_status_response_complete(self):
        """Test TaskStatusResponse with all fields."""
        now = datetime.now()
        started = datetime(2023, 1, 1, 10, 0, 0)
        completed = datetime(2023, 1, 1, 10, 30, 0)
        
        response_data = {
            "job_id": "complete_job_789",
            "task_id": "complete_task_101",
            "status": TaskStatusEnum.COMPLETED,
            "agent_name": "completion_agent",
            "id": 999,
            "started_at": started,
            "completed_at": completed
        }
        response = TaskStatusResponse(**response_data)
        assert response.job_id == "complete_job_789"
        assert response.task_id == "complete_task_101"
        assert response.status == TaskStatusEnum.COMPLETED
        assert response.agent_name == "completion_agent"
        assert response.id == 999
        assert response.started_at == started
        assert response.completed_at == completed
    
    def test_task_status_response_missing_response_fields(self):
        """Test TaskStatusResponse validation with missing response-specific fields."""
        base_data = {
            "job_id": "test_job",
            "task_id": "test_task",
            "status": TaskStatusEnum.RUNNING
        }
        
        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            TaskStatusResponse(**base_data, started_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        
        # Missing started_at
        with pytest.raises(ValidationError) as exc_info:
            TaskStatusResponse(**base_data, id=1)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "started_at" in missing_fields
    
    def test_task_status_response_config(self):
        """Test TaskStatusResponse model configuration."""
        assert hasattr(TaskStatusResponse, 'model_config')
        assert TaskStatusResponse.model_config.get('from_attributes') is True


class TestTaskCallbackMetadata:
    """Test cases for TaskCallbackMetadata schema."""
    
    def test_valid_task_callback_metadata_defaults(self):
        """Test TaskCallbackMetadata with default values."""
        metadata = TaskCallbackMetadata()
        assert metadata.callback_name is None
        assert metadata.retry_count == 0
        assert metadata.error is None
    
    def test_valid_task_callback_metadata_complete(self):
        """Test TaskCallbackMetadata with all fields."""
        metadata_data = {
            "callback_name": "data_processing_callback",
            "retry_count": 3,
            "error": "Connection timeout after 30 seconds"
        }
        metadata = TaskCallbackMetadata(**metadata_data)
        assert metadata.callback_name == "data_processing_callback"
        assert metadata.retry_count == 3
        assert metadata.error == "Connection timeout after 30 seconds"
    
    def test_task_callback_metadata_retry_scenarios(self):
        """Test TaskCallbackMetadata with various retry scenarios."""
        # No retries
        no_retry = TaskCallbackMetadata(retry_count=0)
        assert no_retry.retry_count == 0
        
        # Multiple retries
        multi_retry = TaskCallbackMetadata(retry_count=5)
        assert multi_retry.retry_count == 5
        
        # With error
        error_retry = TaskCallbackMetadata(
            retry_count=2,
            error="API rate limit exceeded"
        )
        assert error_retry.retry_count == 2
        assert "rate limit" in error_retry.error
    
    def test_task_callback_metadata_various_callbacks(self):
        """Test TaskCallbackMetadata with various callback names."""
        callback_names = [
            "success_callback",
            "error_callback",
            "retry_callback",
            "completion_notification",
            "status_update_webhook"
        ]
        
        for callback_name in callback_names:
            metadata = TaskCallbackMetadata(callback_name=callback_name)
            assert metadata.callback_name == callback_name


class TestTaskErrorTrace:
    """Test cases for TaskErrorTrace schema."""
    
    def test_valid_task_error_trace(self):
        """Test TaskErrorTrace with valid data."""
        now = datetime.now()
        trace_data = {
            "run_id": 12345,
            "task_key": "data_analysis_task",
            "error_type": "ValidationError",
            "error_message": "Input data validation failed: missing required field 'dataset'",
            "timestamp": now
        }
        trace = TaskErrorTrace(**trace_data)
        assert trace.run_id == 12345
        assert trace.task_key == "data_analysis_task"
        assert trace.error_type == "ValidationError"
        assert trace.error_message == "Input data validation failed: missing required field 'dataset'"
        assert trace.timestamp == now
        assert trace.error_metadata is None
    
    def test_valid_task_error_trace_with_metadata(self):
        """Test TaskErrorTrace with error metadata."""
        now = datetime.now()
        error_metadata = {
            "stack_trace": "Traceback (most recent call last)...",
            "error_code": "VAL_001",
            "affected_fields": ["dataset", "parameters"],
            "suggestion": "Ensure all required fields are provided",
            "retry_recommended": True
        }
        
        trace_data = {
            "run_id": 67890,
            "task_key": "model_training_task",
            "error_type": "ModelTrainingError",
            "error_message": "Model training failed due to insufficient data",
            "timestamp": now,
            "error_metadata": error_metadata
        }
        trace = TaskErrorTrace(**trace_data)
        assert trace.run_id == 67890
        assert trace.task_key == "model_training_task"
        assert trace.error_type == "ModelTrainingError"
        assert trace.error_metadata["error_code"] == "VAL_001"
        assert trace.error_metadata["retry_recommended"] is True
        assert "dataset" in trace.error_metadata["affected_fields"]
    
    def test_task_error_trace_missing_required_fields(self):
        """Test TaskErrorTrace validation with missing required fields."""
        required_fields = ["run_id", "task_key", "error_type", "error_message", "timestamp"]
        
        for missing_field in required_fields:
            trace_data = {
                "run_id": 1,
                "task_key": "test_task",
                "error_type": "TestError",
                "error_message": "Test error message",
                "timestamp": datetime.now()
            }
            del trace_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                TaskErrorTrace(**trace_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_task_error_trace_various_error_types(self):
        """Test TaskErrorTrace with various error types."""
        now = datetime.now()
        error_types = [
            "ValidationError",
            "ConnectionError",
            "TimeoutError",
            "AuthenticationError",
            "PermissionError",
            "DataProcessingError",
            "ModelInferenceError",
            "ConfigurationError"
        ]
        
        for error_type in error_types:
            trace = TaskErrorTrace(
                run_id=1,
                task_key="test_task",
                error_type=error_type,
                error_message=f"Test {error_type} occurred",
                timestamp=now
            )
            assert trace.error_type == error_type


class TestTaskStatusSchema:
    """Test cases for TaskStatusSchema."""
    
    def test_valid_task_status_schema(self):
        """Test TaskStatusSchema with valid data."""
        now = datetime.now()
        schema_data = {
            "id": 123,
            "task_id": "schema_task_456",
            "status": "running",
            "agent_name": "schema_agent",
            "started_at": now
        }
        schema = TaskStatusSchema(**schema_data)
        assert schema.id == 123
        assert schema.task_id == "schema_task_456"
        assert schema.status == "running"
        assert schema.agent_name == "schema_agent"
        assert schema.started_at == now
        assert schema.completed_at is None
    
    def test_valid_task_status_schema_completed(self):
        """Test TaskStatusSchema with completed task."""
        started = datetime(2023, 1, 1, 10, 0, 0)
        completed = datetime(2023, 1, 1, 10, 15, 0)
        
        schema_data = {
            "id": 456,
            "task_id": "completed_task_789",
            "status": "completed",
            "started_at": started,
            "completed_at": completed
        }
        schema = TaskStatusSchema(**schema_data)
        assert schema.id == 456
        assert schema.task_id == "completed_task_789"
        assert schema.status == "completed"
        assert schema.agent_name is None
        assert schema.started_at == started
        assert schema.completed_at == completed
    
    def test_task_status_schema_missing_required_fields(self):
        """Test TaskStatusSchema validation with missing required fields."""
        required_fields = ["id", "task_id", "status", "started_at"]
        
        for missing_field in required_fields:
            schema_data = {
                "id": 1,
                "task_id": "test_task",
                "status": "running",
                "started_at": datetime.now()
            }
            del schema_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                TaskStatusSchema(**schema_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_task_status_schema_config(self):
        """Test TaskStatusSchema model configuration."""
        assert hasattr(TaskStatusSchema, 'model_config')
        assert TaskStatusSchema.model_config.get('from_attributes') is True


class TestJobExecutionStatusResponse:
    """Test cases for JobExecutionStatusResponse schema."""
    
    def test_valid_job_execution_status_response_empty_tasks(self):
        """Test JobExecutionStatusResponse with empty tasks list."""
        response_data = {
            "job_id": "job_empty_001",
            "status": "pending",
            "tasks": []
        }
        response = JobExecutionStatusResponse(**response_data)
        assert response.job_id == "job_empty_001"
        assert response.status == "pending"
        assert response.tasks == []
    
    def test_valid_job_execution_status_response_with_tasks(self):
        """Test JobExecutionStatusResponse with task list."""
        now = datetime.now()
        tasks = [
            TaskStatusSchema(
                id=1,
                task_id="task_001",
                status="completed",
                agent_name="agent_1",
                started_at=now,
                completed_at=now
            ),
            TaskStatusSchema(
                id=2,
                task_id="task_002",
                status="running",
                agent_name="agent_2",
                started_at=now
            ),
            TaskStatusSchema(
                id=3,
                task_id="task_003",
                status="failed",
                agent_name="agent_3",
                started_at=now,
                completed_at=now
            )
        ]
        
        response_data = {
            "job_id": "job_with_tasks_001",
            "status": "running",
            "tasks": tasks
        }
        response = JobExecutionStatusResponse(**response_data)
        assert response.job_id == "job_with_tasks_001"
        assert response.status == "running"
        assert len(response.tasks) == 3
        assert response.tasks[0].status == "completed"
        assert response.tasks[1].status == "running"
        assert response.tasks[2].status == "failed"
    
    def test_job_execution_status_response_missing_fields(self):
        """Test JobExecutionStatusResponse validation with missing fields."""
        required_fields = ["job_id", "status", "tasks"]
        
        for missing_field in required_fields:
            response_data = {
                "job_id": "test_job",
                "status": "running",
                "tasks": []
            }
            del response_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                JobExecutionStatusResponse(**response_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields


class TestTaskTrackingSchemaIntegration:
    """Integration tests for task tracking schema interactions."""
    
    def test_complete_task_lifecycle(self):
        """Test complete task lifecycle workflow."""
        now = datetime.now()
        
        # Create task
        task_create = TaskStatusCreate(
            job_id="lifecycle_job_001",
            task_id="lifecycle_task_001",
            status=TaskStatusEnum.RUNNING,
            agent_name="lifecycle_agent"
        )
        
        # Task response after creation
        task_response = TaskStatusResponse(
            **task_create.model_dump(),
            id=100,
            started_at=now
        )
        
        # Update task to completed
        task_update = TaskStatusUpdate(status=TaskStatusEnum.COMPLETED)
        
        # Updated task response
        completed_response = TaskStatusResponse(
            **task_create.model_dump(exclude={"status"}),
            id=100,
            started_at=now,
            completed_at=now,
            status=TaskStatusEnum.COMPLETED
        )
        
        # Verify lifecycle
        assert task_create.job_id == task_response.job_id
        assert task_create.status == TaskStatusEnum.RUNNING
        assert task_response.status == TaskStatusEnum.RUNNING
        assert task_update.status == TaskStatusEnum.COMPLETED
        assert completed_response.status == TaskStatusEnum.COMPLETED
        assert completed_response.completed_at is not None
    
    def test_job_execution_with_multiple_tasks(self):
        """Test job execution status with multiple tasks."""
        now = datetime.now()
        
        # Create multiple task statuses
        task_statuses = [
            TaskStatusSchema(
                id=1,
                task_id="preprocessing",
                status="completed",
                agent_name="preprocessor",
                started_at=now,
                completed_at=now
            ),
            TaskStatusSchema(
                id=2,
                task_id="analysis",
                status="running",
                agent_name="analyzer",
                started_at=now
            ),
            TaskStatusSchema(
                id=3,
                task_id="reporting",
                status="pending",
                agent_name="reporter",
                started_at=now
            )
        ]
        
        # Job execution status
        job_status = JobExecutionStatusResponse(
            job_id="multi_task_job_001",
            status="running",
            tasks=task_statuses
        )
        
        # Verify job status
        assert job_status.job_id == "multi_task_job_001"
        assert job_status.status == "running"
        assert len(job_status.tasks) == 3
        
        # Count task statuses
        completed_tasks = [t for t in job_status.tasks if t.status == "completed"]
        running_tasks = [t for t in job_status.tasks if t.status == "running"]
        pending_tasks = [t for t in job_status.tasks if t.status == "pending"]
        
        assert len(completed_tasks) == 1
        assert len(running_tasks) == 1
        assert len(pending_tasks) == 1
    
    def test_error_tracking_workflow(self):
        """Test error tracking workflow."""
        now = datetime.now()
        
        # Task that fails
        failed_task = TaskStatusResponse(
            job_id="error_job_001",
            task_id="failing_task",
            status=TaskStatusEnum.FAILED,
            agent_name="error_prone_agent",
            id=200,
            started_at=now,
            completed_at=now
        )
        
        # Error trace for the failed task
        error_trace = TaskErrorTrace(
            run_id=200,
            task_key="failing_task",
            error_type="DataProcessingError",
            error_message="Unable to process malformed data: invalid JSON structure",
            timestamp=now,
            error_metadata={
                "input_file": "data.json",
                "line_number": 42,
                "character_position": 150,
                "suggested_fix": "Validate JSON structure before processing"
            }
        )
        
        # Callback metadata for retry
        callback_metadata = TaskCallbackMetadata(
            callback_name="retry_failed_task",
            retry_count=1,
            error="Initial attempt failed, retrying with corrected data"
        )
        
        # Verify error tracking
        assert failed_task.status == TaskStatusEnum.FAILED
        assert error_trace.error_type == "DataProcessingError"
        assert error_trace.run_id == failed_task.id
        assert callback_metadata.retry_count == 1
        assert "retry" in callback_metadata.callback_name
    
    def test_task_status_transitions(self):
        """Test valid task status transitions."""
        now = datetime.now()
        
        # Start with running task
        running_task = TaskStatusResponse(
            job_id="transition_job",
            task_id="transition_task",
            status=TaskStatusEnum.RUNNING,
            id=300,
            started_at=now
        )
        
        # Valid transition: running -> completed
        completed_update = TaskStatusUpdate(status=TaskStatusEnum.COMPLETED)
        completed_task = TaskStatusResponse(
            **running_task.model_dump(exclude={"status", "completed_at"}),
            status=TaskStatusEnum.COMPLETED,
            completed_at=now
        )
        
        # Valid transition: running -> failed
        failed_update = TaskStatusUpdate(status=TaskStatusEnum.FAILED)
        failed_task = TaskStatusResponse(
            **running_task.model_dump(exclude={"status", "completed_at"}),
            status=TaskStatusEnum.FAILED,
            completed_at=now
        )
        
        # Verify transitions
        assert running_task.status == TaskStatusEnum.RUNNING
        assert completed_update.status == TaskStatusEnum.COMPLETED
        assert completed_task.status == TaskStatusEnum.COMPLETED
        assert completed_task.completed_at is not None
        
        assert failed_update.status == TaskStatusEnum.FAILED
        assert failed_task.status == TaskStatusEnum.FAILED
        assert failed_task.completed_at is not None