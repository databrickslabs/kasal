"""
Unit tests for execution history models.

Tests the functionality of ExecutionHistory, TaskStatus, and ErrorTrace models
including relationships, validation, and database operations.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from uuid import uuid4

from src.models.execution_history import (
    ExecutionHistory, TaskStatus, ErrorTrace, generate_job_id
)


class TestGenerateJobId:
    """Test cases for generate_job_id function."""
    
    def test_generate_job_id_returns_string(self):
        """Test that generate_job_id returns a string."""
        job_id = generate_job_id()
        assert isinstance(job_id, str)
    
    def test_generate_job_id_is_unique(self):
        """Test that generate_job_id returns unique values."""
        job_id1 = generate_job_id()
        job_id2 = generate_job_id()
        assert job_id1 != job_id2
    
    def test_generate_job_id_is_uuid_format(self):
        """Test that generate_job_id returns valid UUID format."""
        job_id = generate_job_id()
        # Should be able to create UUID from the string
        from uuid import UUID
        uuid_obj = UUID(job_id)
        assert str(uuid_obj) == job_id
    
    @patch('src.models.execution_history.uuid4')
    def test_generate_job_id_calls_uuid4(self, mock_uuid4):
        """Test that generate_job_id calls uuid4."""
        mock_uuid = MagicMock()
        mock_uuid.__str__ = MagicMock(return_value="test-uuid")
        mock_uuid4.return_value = mock_uuid
        
        result = generate_job_id()
        
        mock_uuid4.assert_called_once()
        assert result == "test-uuid"


class TestExecutionHistory:
    """Test cases for ExecutionHistory model."""
    
    def test_execution_history_creation(self):
        """Test basic ExecutionHistory creation."""
        execution = ExecutionHistory(
            status="pending",
            inputs={"key": "value"},
            trigger_type="api",
            run_name="test_run"
        )
        
        assert execution.status == "pending"
        assert execution.inputs == {"key": "value"}
        assert execution.trigger_type == "api"
        assert execution.run_name == "test_run"
    
    def test_execution_history_defaults(self):
        """Test ExecutionHistory default values."""
        execution = ExecutionHistory()
        
        assert execution.status == "pending"
        assert execution.inputs == {}
        assert execution.trigger_type == "api"
        assert execution.planning is False
        assert execution.result is None
        assert execution.error is None
    
    def test_execution_history_job_id_generation(self):
        """Test that job_id is automatically generated."""
        execution = ExecutionHistory()
        
        # job_id should be set by default function
        assert execution.job_id is not None
        assert isinstance(execution.job_id, str)
    
    def test_execution_history_custom_job_id(self):
        """Test ExecutionHistory with custom job_id."""
        custom_job_id = "custom-job-123"
        execution = ExecutionHistory(job_id=custom_job_id)
        
        assert execution.job_id == custom_job_id
    
    def test_execution_history_created_at_default(self):
        """Test that created_at is set by default."""
        with patch('src.models.execution_history.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.utcnow.return_value = mock_now
            
            execution = ExecutionHistory()
            
            assert execution.created_at == mock_now
    
    def test_execution_history_group_fields(self):
        """Test ExecutionHistory group-related fields."""
        execution = ExecutionHistory(
            group_id="group_123",
            group_email="user@group.com"
        )
        
        assert execution.group_id == "group_123"
        assert execution.group_email == "user@group.com"
    
    def test_execution_history_legacy_tenant_fields(self):
        """Test ExecutionHistory legacy tenant fields - skip as tenant removed."""
        # Tenant concept has been removed from the codebase
        pass
    
    def test_execution_history_result_and_error(self):
        """Test ExecutionHistory result and error fields."""
        execution = ExecutionHistory(
            result={"output": "success"},
            error="Something went wrong"
        )
        
        assert execution.result == {"output": "success"}
        assert execution.error == "Something went wrong"
    
    def test_execution_history_completed_at(self):
        """Test ExecutionHistory completed_at field."""
        completion_time = datetime(2023, 1, 1, 13, 0, 0)
        execution = ExecutionHistory(completed_at=completion_time)
        
        assert execution.completed_at == completion_time
    
    def test_execution_history_tablename(self):
        """Test ExecutionHistory table name."""
        assert ExecutionHistory.__tablename__ == "executionhistory"
    
    def test_execution_history_relationships_defined(self):
        """Test that ExecutionHistory relationships are defined."""
        execution = ExecutionHistory()
        
        # Check relationship attributes exist
        assert hasattr(execution, 'task_statuses')
        assert hasattr(execution, 'error_traces')
        assert hasattr(execution, 'execution_traces')
        assert hasattr(execution, 'execution_traces_by_job_id')
    
    def test_execution_history_planning_field(self):
        """Test ExecutionHistory planning field."""
        execution_planning = ExecutionHistory(planning=True)
        execution_no_planning = ExecutionHistory(planning=False)
        
        assert execution_planning.planning is True
        assert execution_no_planning.planning is False


class TestTaskStatus:
    """Test cases for TaskStatus model."""
    
    def test_task_status_creation(self):
        """Test basic TaskStatus creation."""
        task_status = TaskStatus(
            job_id="job_123",
            task_id="task_456",
            status="running",
            agent_name="test_agent"
        )
        
        assert task_status.job_id == "job_123"
        assert task_status.task_id == "task_456"
        assert task_status.status == "running"
        assert task_status.agent_name == "test_agent"
    
    def test_task_status_required_fields(self):
        """Test TaskStatus with required fields only."""
        task_status = TaskStatus(
            job_id="job_123",
            task_id="task_456",
            status="completed"
        )
        
        assert task_status.job_id == "job_123"
        assert task_status.task_id == "task_456"
        assert task_status.status == "completed"
        assert task_status.agent_name is None
        assert task_status.completed_at is None
    
    def test_task_status_started_at_default(self):
        """Test that started_at is set by default."""
        with patch('src.models.execution_history.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 30, 0)
            mock_datetime.utcnow.return_value = mock_now
            
            task_status = TaskStatus(
                job_id="job_123",
                task_id="task_456",
                status="running"
            )
            
            assert task_status.started_at == mock_now
    
    def test_task_status_completed_at(self):
        """Test TaskStatus completed_at field."""
        completion_time = datetime(2023, 1, 1, 13, 30, 0)
        task_status = TaskStatus(
            job_id="job_123",
            task_id="task_456",
            status="completed",
            completed_at=completion_time
        )
        
        assert task_status.completed_at == completion_time
    
    def test_task_status_tablename(self):
        """Test TaskStatus table name."""
        assert TaskStatus.__tablename__ == "taskstatus"
    
    def test_task_status_relationship_defined(self):
        """Test that TaskStatus relationship is defined."""
        task_status = TaskStatus(
            job_id="job_123",
            task_id="task_456",
            status="running"
        )
        
        # Check relationship attribute exists
        assert hasattr(task_status, 'execution_history')
    
    def test_task_status_status_values(self):
        """Test different TaskStatus status values."""
        statuses = ["running", "completed", "failed"]
        
        for status in statuses:
            task_status = TaskStatus(
                job_id="job_123",
                task_id="task_456",
                status=status
            )
            assert task_status.status == status


class TestErrorTrace:
    """Test cases for ErrorTrace model."""
    
    def test_error_trace_creation(self):
        """Test basic ErrorTrace creation."""
        error_trace = ErrorTrace(
            run_id=1,
            task_key="task_123",
            error_type="ValidationError",
            error_message="Invalid input provided"
        )
        
        assert error_trace.run_id == 1
        assert error_trace.task_key == "task_123"
        assert error_trace.error_type == "ValidationError"
        assert error_trace.error_message == "Invalid input provided"
    
    def test_error_trace_with_metadata(self):
        """Test ErrorTrace with error metadata."""
        metadata = {"field": "name", "value": "", "constraint": "required"}
        error_trace = ErrorTrace(
            run_id=1,
            task_key="task_123",
            error_type="ValidationError",
            error_message="Invalid input provided",
            error_metadata=metadata
        )
        
        assert error_trace.error_metadata == metadata
    
    def test_error_trace_metadata_default(self):
        """Test ErrorTrace metadata default value."""
        error_trace = ErrorTrace(
            run_id=1,
            task_key="task_123",
            error_type="RuntimeError",
            error_message="Something went wrong"
        )
        
        assert error_trace.error_metadata == {}
    
    def test_error_trace_timestamp_default(self):
        """Test that timestamp is set by default."""
        with patch('src.models.execution_history.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 45, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = mock_now
            
            error_trace = ErrorTrace(
                run_id=1,
                task_key="task_123",
                error_type="RuntimeError",
                error_message="Something went wrong"
            )
            
            assert error_trace.timestamp == mock_now
            mock_datetime.now.assert_called_with(timezone.utc)
    
    def test_error_trace_custom_timestamp(self):
        """Test ErrorTrace with custom timestamp."""
        custom_time = datetime(2023, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
        error_trace = ErrorTrace(
            run_id=1,
            task_key="task_123",
            error_type="CustomError",
            error_message="Custom error occurred",
            timestamp=custom_time
        )
        
        assert error_trace.timestamp == custom_time
    
    def test_error_trace_tablename(self):
        """Test ErrorTrace table name."""
        assert ErrorTrace.__tablename__ == "errortrace"
    
    def test_error_trace_relationship_defined(self):
        """Test that ErrorTrace relationship is defined."""
        error_trace = ErrorTrace(
            run_id=1,
            task_key="task_123",
            error_type="RuntimeError",
            error_message="Something went wrong"
        )
        
        # Check relationship attribute exists
        assert hasattr(error_trace, 'execution_history')
    
    def test_error_trace_different_error_types(self):
        """Test ErrorTrace with different error types."""
        error_types = [
            "ValidationError",
            "RuntimeError", 
            "TimeoutError",
            "ConnectionError",
            "CustomError"
        ]
        
        for error_type in error_types:
            error_trace = ErrorTrace(
                run_id=1,
                task_key="task_123",
                error_type=error_type,
                error_message=f"Error of type {error_type}"
            )
            assert error_trace.error_type == error_type


class TestModelRelationships:
    """Test cases for model relationships."""
    
    def test_execution_history_task_statuses_relationship(self):
        """Test ExecutionHistory to TaskStatus relationship configuration."""
        # Check that relationship is defined
        assert hasattr(ExecutionHistory, 'task_statuses')
        assert hasattr(TaskStatus, 'execution_history')
    
    def test_execution_history_error_traces_relationship(self):
        """Test ExecutionHistory to ErrorTrace relationship configuration."""
        # Check that relationship is defined
        assert hasattr(ExecutionHistory, 'error_traces')
        assert hasattr(ErrorTrace, 'execution_history')
    
    def test_task_status_execution_history_relationship(self):
        """Test TaskStatus to ExecutionHistory relationship configuration."""
        # Check that relationship is defined
        assert hasattr(TaskStatus, 'execution_history')
    
    def test_error_trace_execution_history_relationship(self):
        """Test ErrorTrace to ExecutionHistory relationship configuration."""
        # Check that relationship is defined
        assert hasattr(ErrorTrace, 'execution_history')


class TestModelFieldTypes:
    """Test cases for model field types and constraints."""
    
    def test_execution_history_field_types(self):
        """Test ExecutionHistory field types."""
        execution = ExecutionHistory()
        
        # Check field existence and types
        assert hasattr(execution, 'id')
        assert hasattr(execution, 'job_id')
        assert hasattr(execution, 'status')
        assert hasattr(execution, 'inputs')
        assert hasattr(execution, 'result')
        assert hasattr(execution, 'error')
        assert hasattr(execution, 'planning')
        assert hasattr(execution, 'trigger_type')
        assert hasattr(execution, 'created_at')
        assert hasattr(execution, 'run_name')
        assert hasattr(execution, 'completed_at')
        assert hasattr(execution, 'group_id')
        assert hasattr(execution, 'group_email')
    
    def test_task_status_field_types(self):
        """Test TaskStatus field types."""
        task_status = TaskStatus(
            job_id="job_123",
            task_id="task_456", 
            status="running"
        )
        
        # Check field existence
        assert hasattr(task_status, 'id')
        assert hasattr(task_status, 'job_id')
        assert hasattr(task_status, 'task_id')
        assert hasattr(task_status, 'status')
        assert hasattr(task_status, 'agent_name')
        assert hasattr(task_status, 'started_at')
        assert hasattr(task_status, 'completed_at')
    
    def test_error_trace_field_types(self):
        """Test ErrorTrace field types."""
        error_trace = ErrorTrace(
            run_id=1,
            task_key="task_123",
            error_type="RuntimeError",
            error_message="Something went wrong"
        )
        
        # Check field existence
        assert hasattr(error_trace, 'id')
        assert hasattr(error_trace, 'run_id')
        assert hasattr(error_trace, 'task_key')
        assert hasattr(error_trace, 'error_type')
        assert hasattr(error_trace, 'error_message')
        assert hasattr(error_trace, 'timestamp')
        assert hasattr(error_trace, 'error_metadata')


class TestModelUsagePatterns:
    """Test cases for common model usage patterns."""
    
    def test_execution_history_workflow_simulation(self):
        """Test ExecutionHistory in a workflow simulation."""
        # Create execution
        execution = ExecutionHistory(
            status="running",
            inputs={"param": "value"},
            trigger_type="api",
            run_name="test_workflow",
            group_id="group_123",
            group_email="user@test.com"
        )
        
        # Start workflow
        assert execution.status == "running"
        assert execution.completed_at is None
        
        # Complete workflow
        execution.status = "completed"
        execution.result = {"output": "success"}
        execution.completed_at = datetime.utcnow()
        
        assert execution.status == "completed"
        assert execution.result == {"output": "success"}
        assert execution.completed_at is not None
    
    def test_task_status_lifecycle(self):
        """Test TaskStatus lifecycle."""
        # Create task
        task = TaskStatus(
            job_id="job_123",
            task_id="task_456",
            status="running",
            agent_name="agent_1"
        )
        
        assert task.status == "running"
        assert task.completed_at is None
        
        # Complete task
        task.status = "completed"
        task.completed_at = datetime.utcnow()
        
        assert task.status == "completed"
        assert task.completed_at is not None
    
    def test_error_trace_error_logging(self):
        """Test ErrorTrace for error logging."""
        # Log error
        error = ErrorTrace(
            run_id=1,
            task_key="validation_task",
            error_type="ValidationError",
            error_message="Required field missing",
            error_metadata={
                "field": "email",
                "validation": "required"
            }
        )
        
        assert error.error_type == "ValidationError"
        assert error.error_metadata["field"] == "email"
    
    def test_multi_group_support(self):
        """Test multi-group support in models."""
        # Group-based execution
        group_execution = ExecutionHistory(
            status="running",
            group_id="group_123",
            group_email="user@group.com"
        )
        
        assert group_execution.group_id == "group_123"
        assert group_execution.group_email == "user@group.com"
