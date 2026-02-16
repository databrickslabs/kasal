"""
Unit tests for execution workflow functionality.

Tests the core execution workflow components including
service methods, status management, and execution logic.
"""
import pytest
import uuid
import asyncio
import json
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import Dict, Any, List

from src.services.execution_service import ExecutionService
from src.schemas.execution import ExecutionStatus, CrewConfig, ExecutionCreateResponse
from src.services.crewai_execution_service import CrewAIExecutionService
from src.services.execution_status_service import ExecutionStatusService
from src.services.execution_name_service import ExecutionNameService
from src.utils.user_context import GroupContext


@pytest.fixture
def mock_group_context():
    """Create mock group context."""
    context = MagicMock(spec=GroupContext)
    context.group_ids = ["group-1", "group-2"]
    context.user_id = "user-123"
    context.primary_group_id = "group-1"
    context.group_email = "test@example.com"
    context.access_token = None
    return context


@pytest.fixture
def sample_crew_config():
    """Sample crew configuration for testing."""
    return CrewConfig(
        agents_yaml={
            "researcher": {
                "role": "Senior Research Analyst",
                "goal": "Find and analyze relevant information",
                "backstory": "You are an expert research analyst",
                "tools": ["web_search"]
            }
        },
        tasks_yaml={
            "research_task": {
                "description": "Research the latest trends in AI",
                "agent": "researcher",
                "expected_output": "A comprehensive report"
            }
        },
        model="gpt-4o-mini",
        planning=True,
        execution_type="crew",
        inputs={"topic": "artificial intelligence"},
        schema_detection_enabled=True
    )


@pytest.fixture
def sample_flow_config():
    """Sample flow configuration for testing."""
    flow_id = uuid.uuid4()
    config = CrewConfig(
        agents_yaml={},
        tasks_yaml={},
        model="gpt-4o-mini",
        planning=False,
        execution_type="flow",
        inputs={"flow_id": str(flow_id)},
        schema_detection_enabled=False
    )
    # Add flow-specific attributes dynamically
    setattr(config, 'flow_id', flow_id)
    setattr(config, 'nodes', [
        {"id": "start", "type": "agent", "data": {"name": "Start Agent"}},
        {"id": "task1", "type": "task", "data": {"name": "Process Data"}}
    ])
    setattr(config, 'edges', [
        {"source": "start", "target": "task1", "data": {}}
    ])
    return config


@pytest.fixture
def execution_service():
    """Create ExecutionService instance for testing."""
    with patch('src.services.execution_service.ExecutionNameService.create') as mock_name_service, \
         patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service:

        mock_name_service.return_value = MagicMock(spec=ExecutionNameService)
        mock_crew_service.return_value = MagicMock(spec=CrewAIExecutionService)

        service = ExecutionService()
        return service


class TestExecutionService:
    """Unit tests for ExecutionService class."""

    def test_create_execution_id(self):
        """Test execution ID creation."""
        execution_id = ExecutionService.create_execution_id()

        assert isinstance(execution_id, str)
        assert len(execution_id) > 0
        # Should be a valid UUID string
        uuid.UUID(execution_id)  # This will raise ValueError if not valid UUID

    def test_get_execution_from_memory(self):
        """Test getting execution from memory."""
        # Setup test data
        execution_id = "test-exec-123"
        test_data = {"status": "running", "result": None}
        ExecutionService.executions[execution_id] = test_data

        # Test retrieval
        result = ExecutionService.get_execution(execution_id)

        assert result == test_data

        # Test non-existent execution
        result = ExecutionService.get_execution("non-existent")
        assert result is None

        # Cleanup
        del ExecutionService.executions[execution_id]

    def test_add_execution_to_memory(self):
        """Test adding execution to memory."""
        execution_id = "test-exec-456"
        status = "RUNNING"
        run_name = "Test Run"
        created_at = datetime.now()

        ExecutionService.add_execution_to_memory(
            execution_id, status, run_name, created_at
        )

        stored_data = ExecutionService.executions[execution_id]
        assert stored_data["execution_id"] == execution_id
        assert stored_data["status"] == status
        assert stored_data["run_name"] == run_name
        assert stored_data["created_at"] == created_at
        assert stored_data["output"] == ""

        # Cleanup
        del ExecutionService.executions[execution_id]

    @pytest.mark.asyncio
    async def test_execute_flow_kasal_error_passthrough(self, execution_service):
        """Test that KasalErrors are re-raised in execute_flow."""
        from src.core.exceptions import KasalError
        with patch.object(execution_service.crewai_execution_service, 'run_flow_execution') as mock_run_flow:
            mock_run_flow.side_effect = KasalError(detail="Bad request")

            with pytest.raises(KasalError):
                await execution_service.execute_flow()

    @pytest.mark.asyncio
    async def test_get_executions_by_flow_error_handling(self, execution_service):
        """Test that errors are wrapped in KasalError in get_executions_by_flow."""
        from src.core.exceptions import KasalError

        flow_id = uuid.uuid4()
        with patch.object(execution_service.crewai_execution_service, 'get_flow_executions_by_flow') as mock_get_executions:
            mock_get_executions.side_effect = Exception("Database error")

            with pytest.raises(KasalError) as exc_info:
                await execution_service.get_executions_by_flow(flow_id)

            assert "Error getting executions" in str(exc_info.value.detail)

    def test_sanitize_for_database(self):
        """Test data sanitization for database storage."""
        test_uuid = uuid.uuid4()
        test_data = {
            "string_field": "test string",
            "dict_field": {"nested": "value"},
            "list_field": [1, 2, {"nested_dict": "in_list"}],
            "none_field": None,
            "bool_field": True,
            "uuid_field": test_uuid,
            "non_serializable": object()  # This should be converted to string
        }

        sanitized = ExecutionService.sanitize_for_database(test_data)

        # Check that dict fields are recursively sanitized
        assert isinstance(sanitized["dict_field"], dict)
        assert sanitized["dict_field"]["nested"] == "value"

        # Check that list fields with nested dicts are properly handled
        assert isinstance(sanitized["list_field"], list)
        assert sanitized["list_field"][0] == 1
        assert sanitized["list_field"][1] == 2
        assert isinstance(sanitized["list_field"][2], dict)

        # Check that UUID is converted to string
        assert sanitized["uuid_field"] == str(test_uuid)

        # Check that non-serializable objects are converted to string
        assert isinstance(sanitized["non_serializable"], str)

        # Check that other fields remain unchanged
        assert sanitized["string_field"] == "test string"
        assert sanitized["none_field"] is None
        assert sanitized["bool_field"] is True

    @pytest.mark.asyncio
    async def test_create_execution_success(self, execution_service, sample_crew_config, mock_group_context):
        """Test successful execution creation."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch.object(execution_service, '_check_for_running_jobs') as mock_check_jobs, \
             patch('asyncio.create_task') as mock_create_task:

            # Setup mocks
            mock_status_service.create_execution = AsyncMock(return_value=True)
            mock_check_jobs.return_value = None
            mock_create_task.return_value = MagicMock()

            # Mock the execution name service response
            mock_name_response = MagicMock()
            mock_name_response.name = "Test Execution Name"
            execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_name_response)

            # Test execution creation
            result = await execution_service.create_execution(
                sample_crew_config, group_context=mock_group_context
            )

            # Verify result is a dictionary (from model_dump())
            assert isinstance(result, dict)
            assert result["status"] == ExecutionStatus.RUNNING.value
            assert "execution_id" in result
            assert result["run_name"] == "Test Execution Name"

            # Verify UUID format
            uuid.UUID(result["execution_id"])

            # Verify mocks were called
            mock_status_service.create_execution.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_execution_with_flow_id(self, execution_service, sample_flow_config, mock_group_context):
        """Test execution creation with flow ID."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch.object(execution_service, '_check_for_running_jobs') as mock_check_jobs, \
             patch('asyncio.create_task') as mock_create_task:

            mock_status_service.create_execution = AsyncMock(return_value=True)
            mock_check_jobs.return_value = None
            mock_create_task.return_value = MagicMock()

            # Mock the execution name service response
            mock_name_response = MagicMock()
            mock_name_response.name = "Flow Execution Name"
            execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_name_response)

            result = await execution_service.create_execution(
                sample_flow_config, group_context=mock_group_context
            )

            assert isinstance(result, dict)
            assert result["status"] == ExecutionStatus.RUNNING.value
            uuid.UUID(result["execution_id"])

    @pytest.mark.asyncio
    async def test_create_execution_no_flow_id_error(self, execution_service, mock_group_context):
        """Test execution creation fails when no flow_id and no flows in DB."""
        flow_config = CrewConfig(
            agents_yaml={},
            tasks_yaml={},
            model="gpt-4o-mini",
            planning=False,
            execution_type="flow",
            inputs={},
            schema_detection_enabled=False
        )

        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch.object(execution_service, '_check_for_running_jobs') as mock_check_jobs, \
             patch('src.db.session.async_session_factory') as mock_session_factory:

            mock_status_service.create_execution.return_value = True
            mock_check_jobs.return_value = None

            # Mock the async session and query to return no flows
            mock_db = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalars.return_value.first.return_value = None
            mock_db.execute.return_value = mock_result
            mock_session_factory.return_value.__aenter__.return_value = mock_db

            # Mock the execution name service response
            mock_name_response = MagicMock()
            mock_name_response.name = "Flow Execution Name"
            execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_name_response)

            from src.core.exceptions import KasalError
            with pytest.raises(KasalError):
                await execution_service.create_execution(
                    flow_config, group_context=mock_group_context
                )

    @pytest.mark.asyncio
    async def test_create_execution_with_model_none(self, execution_service, mock_group_context):
        """Test execution creation with None model."""
        config = CrewConfig(
            agents_yaml={"agent": {"role": "test"}},
            tasks_yaml={"task": {"description": "test"}},
            model=None,  # None model
            planning=False,
            execution_type="crew",
            inputs={},
            schema_detection_enabled=True
        )

        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch.object(execution_service, '_check_for_running_jobs') as mock_check_jobs, \
             patch('asyncio.create_task') as mock_create_task:

            mock_status_service.create_execution = AsyncMock(return_value=True)
            mock_check_jobs.return_value = None
            mock_create_task.return_value = MagicMock()

            # Mock the execution name service response
            mock_name_response = MagicMock()
            mock_name_response.name = "Test None Model"
            execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_name_response)

            result = await execution_service.create_execution(
                config, group_context=mock_group_context
            )

            # Should handle None model by using default
            assert isinstance(result, dict)
            assert result["status"] == ExecutionStatus.RUNNING.value

    @pytest.mark.asyncio
    async def test_run_crew_execution_crew_type(self, sample_crew_config, mock_group_context):
        """Test crew execution with crew type."""
        execution_id = "test-crew-exec"

        with patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service_class:
            mock_crew_service = MagicMock()
            mock_crew_service.run_crew_execution = AsyncMock(return_value={"status": "completed", "result": {"output": "crew success"}})
            mock_crew_service_class.return_value = mock_crew_service

            result = await ExecutionService.run_crew_execution(
                execution_id, sample_crew_config, "crew", mock_group_context
            )

            assert result["status"] == "completed"
            assert result["result"]["output"] == "crew success"
            mock_crew_service.run_crew_execution.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_crew_execution_flow_type(self, sample_flow_config, mock_group_context):
        """Test crew execution with flow type."""
        execution_id = "test-flow-exec"

        with patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service_class:
            mock_crew_service = MagicMock()
            mock_crew_service.run_flow_execution = AsyncMock(return_value={"status": "completed", "result": {"output": "flow success"}})
            mock_crew_service_class.return_value = mock_crew_service

            result = await ExecutionService.run_crew_execution(
                execution_id, sample_flow_config, "flow", mock_group_context
            )

            assert result["status"] == "completed"
            assert result["result"]["output"] == "flow success"
            mock_crew_service.run_flow_execution.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_crew_execution_other_type(self, sample_crew_config, mock_group_context):
        """Test crew execution with other execution type."""
        execution_id = "test-other-exec"

        result = await ExecutionService.run_crew_execution(
            execution_id, sample_crew_config, "other", mock_group_context
        )

        assert result["execution_id"] == execution_id
        assert result["status"] == ExecutionStatus.RUNNING.value
        assert "execution started" in result["message"]

    @pytest.mark.asyncio
    async def test_run_crew_execution_with_error_handling(self, sample_crew_config, mock_group_context):
        """Test crew execution with error handling."""
        execution_id = "test-error-exec"

        with patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service_class, \
             patch('src.services.execution_status_service.ExecutionStatusService.update_status') as mock_update_status:

            mock_crew_service = MagicMock()
            mock_crew_service.run_crew_execution = AsyncMock(side_effect=Exception("Execution failed"))
            mock_crew_service_class.return_value = mock_crew_service
            mock_update_status.return_value = True

            with pytest.raises(Exception) as exc_info:
                await ExecutionService.run_crew_execution(
                    execution_id, sample_crew_config, "crew", mock_group_context
                )

            assert "Execution failed" in str(exc_info.value)
            mock_update_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_crew_execution_with_status_update_failure(self, sample_crew_config, mock_group_context):
        """Test crew execution when status update fails after error."""
        execution_id = "test-status-fail-exec"

        with patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service_class, \
             patch('src.services.execution_status_service.ExecutionStatusService.update_status', new_callable=AsyncMock) as mock_update_status, \
             patch('src.services.execution_service.LoggerManager.get_instance') as mock_logger_manager:

            mock_crew_service = MagicMock()
            mock_crew_service.run_crew_execution = AsyncMock(side_effect=Exception("Execution failed"))
            mock_crew_service_class.return_value = mock_crew_service

            # Create mock logger with critical method
            mock_logger = MagicMock()
            mock_logger_manager.return_value.crew = mock_logger

            # Status update also fails
            mock_update_status.side_effect = Exception("Status update failed")

            with pytest.raises(Exception) as exc_info:
                await ExecutionService.run_crew_execution(
                    execution_id, sample_crew_config, "crew", mock_group_context
                )

            assert "Execution failed" in str(exc_info.value)
            # Verify status update was attempted
            mock_update_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_execution_status_success(self, mock_group_context):
        """Test successful execution status retrieval via instance method."""
        execution_id = "test-exec-789"

        # Mock execution history repository
        mock_execution = MagicMock()
        mock_execution.status = ExecutionStatus.COMPLETED.value
        mock_execution.created_at = datetime.now(UTC)
        mock_execution.result = {"output": "success"}
        mock_execution.run_name = "Test Run"
        mock_execution.error = None
        mock_execution.mlflow_trace_id = None
        mock_execution.mlflow_experiment_name = None
        mock_execution.mlflow_evaluation_run_id = None

        mock_session = AsyncMock()

        with patch('src.repositories.execution_history_repository.ExecutionHistoryRepository') as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.get_execution_by_job_id = AsyncMock(return_value=mock_execution)
            mock_repo_class.return_value = mock_repo

            service = ExecutionService(session=mock_session)
            result = await service.get_execution_status(
                execution_id, mock_group_context.group_ids
            )

            assert result["execution_id"] == execution_id
            assert result["status"] == ExecutionStatus.COMPLETED.value
            assert result["result"] == {"output": "success"}
            assert result["run_name"] == "Test Run"
            assert result["error"] is None

    @pytest.mark.asyncio
    async def test_get_execution_status_not_found(self):
        """Test execution status retrieval when execution not found."""
        execution_id = "non-existent"
        mock_session = AsyncMock()

        with patch('src.repositories.execution_history_repository.ExecutionHistoryRepository') as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.get_execution_by_job_id = AsyncMock(return_value=None)
            mock_repo_class.return_value = mock_repo

            service = ExecutionService(session=mock_session)
            result = await service.get_execution_status(execution_id)

            assert result is None

    @pytest.mark.asyncio
    async def test_get_execution_status_no_session(self):
        """Test execution status retrieval when no session available."""
        service = ExecutionService(session=None)
        result = await service.get_execution_status("some-id")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_execution_status_exception(self):
        """Test execution status retrieval with exception handling."""
        execution_id = "error-exec"
        mock_session = AsyncMock()

        with patch('src.repositories.execution_history_repository.ExecutionHistoryRepository') as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.get_execution_by_job_id = AsyncMock(side_effect=Exception("Database error"))
            mock_repo_class.return_value = mock_repo

            service = ExecutionService(session=mock_session)
            result = await service.get_execution_status(execution_id)

            assert result is None

    @pytest.mark.asyncio
    async def test_list_executions_success(self):
        """Test successful execution listing."""
        mock_session = AsyncMock()
        # Clear any existing executions first
        ExecutionService.executions.clear()

        mock_executions = [MagicMock(), MagicMock()]
        mock_executions[0].job_id = "exec-1"
        mock_executions[0].status = ExecutionStatus.COMPLETED.value
        mock_executions[0].created_at = datetime.now(UTC)
        mock_executions[0].run_name = "Test Run 1"
        mock_executions[0].result = {"output": "result1"}
        mock_executions[0].error = None
        mock_executions[0].group_email = "test@example.com"
        mock_executions[0].group_id = "group-1"
        mock_executions[0].inputs = {}
        mock_executions[0].flow_id = None

        mock_executions[1].job_id = "exec-2"
        mock_executions[1].status = ExecutionStatus.RUNNING.value
        mock_executions[1].created_at = datetime.now(UTC)
        mock_executions[1].run_name = "Test Run 2"
        mock_executions[1].result = None
        mock_executions[1].error = None
        mock_executions[1].group_email = "test@example.com"
        mock_executions[1].group_id = "group-1"
        mock_executions[1].inputs = {}
        mock_executions[1].flow_id = None

        with patch('src.repositories.execution_repository.ExecutionRepository') as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.get_execution_history = AsyncMock(return_value=(mock_executions, len(mock_executions)))
            mock_repo_class.return_value = mock_repo

            service = ExecutionService(session=mock_session)
            result = await service.list_executions(["group-1"])

            assert len(result) == 2
            assert result[0]["execution_id"] == "exec-1"
            assert result[0]["status"] == ExecutionStatus.COMPLETED.value
            assert result[1]["execution_id"] == "exec-2"
            assert result[1]["status"] == ExecutionStatus.RUNNING.value

    @pytest.mark.asyncio
    async def test_list_executions_with_memory_executions(self):
        """Test execution listing includes both DB and memory executions."""
        mock_session = AsyncMock()
        # Clear and add some in-memory executions
        ExecutionService.executions.clear()
        ExecutionService.executions["mem-exec-1"] = {
            "execution_id": "mem-exec-1",
            "status": "RUNNING",
            "created_at": datetime.now(),
            "run_name": "Memory Exec",
            "output": ""
        }

        mock_db_execution = MagicMock()
        mock_db_execution.job_id = "db-exec-1"
        mock_db_execution.status = "COMPLETED"
        mock_db_execution.created_at = datetime.now(UTC)
        mock_db_execution.run_name = "DB Exec 1"
        mock_db_execution.result = {"output": "db result"}
        mock_db_execution.error = None
        mock_db_execution.group_email = "test@example.com"
        mock_db_execution.group_id = "group-1"
        mock_db_execution.inputs = {}
        mock_db_execution.flow_id = None

        with patch('src.repositories.execution_repository.ExecutionRepository') as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.get_execution_history = AsyncMock(return_value=([mock_db_execution], 1))
            mock_repo_class.return_value = mock_repo

            service = ExecutionService(session=mock_session)
            result = await service.list_executions(["group-1"])

            # Should have DB execution and memory execution not in DB
            assert len(result) == 2
            execution_ids = [r["execution_id"] for r in result]
            assert "db-exec-1" in execution_ids
            assert "mem-exec-1" in execution_ids

        # Cleanup
        ExecutionService.executions.clear()

    @pytest.mark.asyncio
    async def test_list_executions_no_session(self):
        """Test execution listing with no session falls back to memory."""
        ExecutionService.executions.clear()
        ExecutionService.executions["mem-only"] = {
            "execution_id": "mem-only",
            "status": "RUNNING",
            "created_at": datetime.now(),
            "run_name": "Memory Only",
            "output": ""
        }

        service = ExecutionService(session=None)
        result = await service.list_executions(["group-1"])

        assert len(result) == 1
        assert result[0]["execution_id"] == "mem-only"

        # Cleanup
        ExecutionService.executions.clear()

    def test_execute_crew_sync_method(self, sample_crew_config):
        """Test synchronous crew execution method."""
        execution_id = "test-sync-exec"
        execution_type = "crew"

        with patch('src.services.execution_service.create_and_run_loop') as mock_create_loop:
            mock_create_loop.return_value = None

            # Test crew execution
            ExecutionService._execute_crew(execution_id, sample_crew_config, execution_type)

            # Verify the loop was created for status update
            mock_create_loop.assert_called_once()

    def test_execute_crew_sync_flow_type(self, sample_flow_config):
        """Test synchronous flow execution method."""
        execution_id = "test-sync-flow"
        execution_type = "flow"

        with patch('src.services.execution_service.create_and_run_loop') as mock_create_loop:
            mock_create_loop.return_value = None

            ExecutionService._execute_crew(execution_id, sample_flow_config, execution_type)

            mock_create_loop.assert_called_once()

    def test_execute_crew_sync_with_error(self, sample_crew_config):
        """Test synchronous crew execution with error in loop creation."""
        execution_id = "test-sync-error"
        execution_type = "crew"

        with patch('src.services.execution_service.create_and_run_loop') as mock_create_loop:
            # create_and_run_loop raises - should be handled gracefully
            mock_create_loop.side_effect = Exception("Loop error")

            # Should not raise exception, should handle gracefully
            ExecutionService._execute_crew(execution_id, sample_crew_config, execution_type)

            mock_create_loop.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_execution_status_method(self):
        """Test execution status update method."""
        execution_id = "test-update-exec"
        status = "COMPLETED"
        result = {"output": "success"}

        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service:
            mock_status_service.update_status = AsyncMock(return_value=True)

            await ExecutionService._update_execution_status(execution_id, status, result)

            mock_status_service.update_status.assert_called_once_with(
                job_id=execution_id,
                status=status,
                message=f"Status updated to {status}",
                result=result
            )

    @pytest.mark.asyncio
    async def test_update_execution_status_failure(self):
        """Test execution status update with failure."""
        execution_id = "test-update-fail"
        status = "FAILED"

        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service:
            mock_status_service.update_status = AsyncMock(return_value=False)

            await ExecutionService._update_execution_status(execution_id, status)

            mock_status_service.update_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_execution_status_exception(self):
        """Test execution status update with exception."""
        execution_id = "test-update-exception"
        status = "COMPLETED"

        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service:
            mock_status_service.update_status = AsyncMock(side_effect=Exception("Update failed"))

            # Should not raise exception, should handle gracefully
            await ExecutionService._update_execution_status(execution_id, status)

    @pytest.mark.asyncio
    async def test_run_in_background_method(self, sample_crew_config, mock_group_context):
        """Test run in background method."""
        execution_id = "test-bg-exec"

        with patch('src.services.execution_service.ExecutionService.run_crew_execution') as mock_run_crew:
            mock_run_crew.return_value = {"status": "completed"}

            await ExecutionService._run_in_background(
                execution_id, sample_crew_config, "crew", mock_group_context
            )

            mock_run_crew.assert_called_once_with(
                execution_id=execution_id,
                config=sample_crew_config,
                execution_type="crew",
                group_context=mock_group_context,
                session=None
            )

    @pytest.mark.asyncio
    async def test_run_in_background_with_error(self, sample_crew_config, mock_group_context):
        """Test run in background method with error handling."""
        execution_id = "test-bg-error"

        with patch('src.services.execution_service.ExecutionService.run_crew_execution') as mock_run_crew:
            mock_run_crew.side_effect = Exception("Background execution failed")

            # Should handle error gracefully without raising
            await ExecutionService._run_in_background(
                execution_id, sample_crew_config, "crew", mock_group_context
            )

    @pytest.mark.asyncio
    async def test_check_for_running_jobs_success(self, execution_service, mock_group_context):
        """Test check for running jobs when none are running."""
        with patch('src.db.session.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session

            mock_repo = MagicMock()
            # Return empty list - no active executions
            mock_repo.get_execution_history = AsyncMock(return_value=([], 0))

            with patch('src.repositories.execution_repository.ExecutionRepository', return_value=mock_repo):
                # Should not raise any exception
                await execution_service._check_for_running_jobs(mock_group_context)

                mock_repo.get_execution_history.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_for_running_jobs_with_active_job(self, execution_service, mock_group_context):
        """Test check for running jobs when there is an active job."""
        with patch('src.db.session.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session

            # Mock an active execution
            mock_active_execution = MagicMock()
            mock_active_execution.run_name = "Active Job"
            mock_active_execution.status = "RUNNING"

            mock_repo = MagicMock()
            mock_repo.get_execution_history = AsyncMock(return_value=([mock_active_execution], 1))

            with patch('src.repositories.execution_repository.ExecutionRepository', return_value=mock_repo):
                with pytest.raises(ValueError) as exc_info:
                    await execution_service._check_for_running_jobs(mock_group_context)

                assert "Cannot start new job" in str(exc_info.value)
                assert "Active Job" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_check_for_running_jobs_database_error(self, execution_service, mock_group_context):
        """Test check for running jobs with database error."""
        with patch('src.db.session.async_session_factory') as mock_session_factory:
            mock_session_factory.side_effect = Exception("Database connection failed")

            # Should not raise exception, should handle gracefully
            await execution_service._check_for_running_jobs(mock_group_context)

    @pytest.mark.asyncio
    async def test_add_execution_to_memory_with_default_created_at(self):
        """Test adding execution to memory with default created_at."""
        execution_id = "test-default-time"
        status = "RUNNING"
        run_name = "Default Time Test"

        # Call without created_at to use default
        ExecutionService.add_execution_to_memory(execution_id, status, run_name)

        stored_data = ExecutionService.executions[execution_id]
        assert stored_data["execution_id"] == execution_id
        assert stored_data["status"] == status
        assert stored_data["run_name"] == run_name
        assert stored_data["created_at"] is not None  # Should have default value
        assert stored_data["output"] == ""

        # Cleanup
        del ExecutionService.executions[execution_id]

    @pytest.mark.asyncio
    async def test_create_execution_status_service_error(self, execution_service, sample_crew_config, mock_group_context):
        """Test create_execution when ExecutionStatusService.create_execution fails."""
        from src.core.exceptions import KasalError

        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch.object(execution_service, '_check_for_running_jobs') as mock_check_jobs:

            mock_status_service.create_execution = AsyncMock(return_value=False)  # Fails
            mock_check_jobs.return_value = None

            # Mock the execution name service response
            mock_name_response = MagicMock()
            mock_name_response.name = "Test Execution"
            execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_name_response)

            with pytest.raises(KasalError) as exc_info:
                await execution_service.create_execution(
                    sample_crew_config, group_context=mock_group_context
                )

            assert "Failed to create execution" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_create_execution_background_tasks_branch(self, execution_service, sample_crew_config, mock_group_context):
        """Test create_execution with background_tasks provided."""
        from fastapi import BackgroundTasks

        mock_background_tasks = MagicMock(spec=BackgroundTasks)

        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch.object(execution_service, '_check_for_running_jobs') as mock_check_jobs:

            mock_status_service.create_execution = AsyncMock(return_value=True)
            mock_check_jobs.return_value = None

            # Mock the execution name service response
            mock_name_response = MagicMock()
            mock_name_response.name = "Background Task Test"
            execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_name_response)

            result = await execution_service.create_execution(
                sample_crew_config,
                background_tasks=mock_background_tasks,
                group_context=mock_group_context
            )

            assert isinstance(result, dict)
            assert result["status"] == ExecutionStatus.RUNNING.value
            # Verify background task was added
            mock_background_tasks.add_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_flow_method(self, execution_service):
        """Test the execute_flow method."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        config = {"key": "value"}

        with patch.object(execution_service.crewai_execution_service, 'run_flow_execution') as mock_run_flow:
            mock_run_flow.return_value = {"status": "started", "execution_id": job_id}

            result = await execution_service.execute_flow(
                flow_id=flow_id,
                job_id=job_id,
                config=config
            )

            assert result["status"] == "started"
            assert result["execution_id"] == job_id

            mock_run_flow.assert_called_once_with(
                flow_id=str(flow_id),
                nodes=None,
                edges=None,
                job_id=job_id,
                config=config
            )

    @pytest.mark.asyncio
    async def test_execute_flow_with_nodes_edges(self, execution_service):
        """Test execute_flow with nodes and edges."""
        nodes = [{"id": "node1", "type": "agent"}]
        edges = [{"source": "node1", "target": "node2"}]

        with patch.object(execution_service.crewai_execution_service, 'run_flow_execution') as mock_run_flow:
            mock_run_flow.return_value = {"status": "started"}

            result = await execution_service.execute_flow(
                nodes=nodes,
                edges=edges
            )

            assert result["status"] == "started"

            # Verify that a job_id was generated
            call_args = mock_run_flow.call_args
            assert call_args[1]["job_id"] is not None
            uuid.UUID(call_args[1]["job_id"])  # Should be valid UUID

    @pytest.mark.asyncio
    async def test_execute_flow_exception_handling(self, execution_service):
        """Test execute_flow exception handling."""
        from src.core.exceptions import KasalError

        with patch.object(execution_service.crewai_execution_service, 'run_flow_execution') as mock_run_flow:
            mock_run_flow.side_effect = Exception("Flow execution failed")

            with pytest.raises(KasalError) as exc_info:
                await execution_service.execute_flow()

            assert "Unexpected error in execute_flow" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_executions_by_flow(self, execution_service):
        """Test get_executions_by_flow method."""
        flow_id = uuid.uuid4()
        expected_result = {"executions": [{"id": 1}, {"id": 2}]}

        with patch.object(execution_service.crewai_execution_service, 'get_flow_executions_by_flow') as mock_get_executions:
            mock_get_executions.return_value = expected_result

            result = await execution_service.get_executions_by_flow(flow_id)

            assert result == expected_result
            mock_get_executions.assert_called_once_with(str(flow_id))

    @pytest.mark.asyncio
    async def test_generate_execution_name_method(self, execution_service):
        """Test the generate_execution_name method."""
        from src.schemas.execution import ExecutionNameGenerationRequest

        request = ExecutionNameGenerationRequest(
            agents_yaml={"agent1": {"role": "test"}},
            tasks_yaml={"task1": {"description": "test"}},
            model="gpt-4"
        )

        mock_response = MagicMock()
        mock_response.name = "Generated Execution Name"
        execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_response)

        result = await execution_service.generate_execution_name(request)

        assert result == {"name": "Generated Execution Name"}
        execution_service.execution_name_service.generate_execution_name.assert_called_once_with(request)

    @pytest.mark.asyncio
    async def test_run_crew_execution_flow_with_flow_id_in_inputs(self, mock_group_context):
        """Test run_crew_execution flow with flow_id in inputs dict."""
        execution_id = "test-flow-id-inputs"
        flow_id = uuid.uuid4()

        mock_config = MagicMock()
        mock_config.model_dump.return_value = {"test": "config"}
        mock_config.inputs = {"flow_id": str(flow_id)}
        mock_config.flow_id = None  # No direct flow_id attribute

        with patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service_class:
            mock_crew_service = MagicMock()
            mock_crew_service.run_flow_execution = AsyncMock(return_value={"status": "completed"})
            mock_crew_service_class.return_value = mock_crew_service

            result = await ExecutionService.run_crew_execution(
                execution_id, mock_config, "flow", mock_group_context
            )

            assert result["status"] == "completed"
            # Verify flow_id was extracted from inputs
            call_args = mock_crew_service.run_flow_execution.call_args
            assert call_args[1]["flow_id"] == str(flow_id)


class TestExecutionWorkflowIntegration:
    """Integration-style unit tests for execution workflow components."""

    @pytest.mark.asyncio
    async def test_complete_crew_execution_workflow(self, execution_service, sample_crew_config, mock_group_context):
        """Test complete crew execution workflow from creation to completion."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('src.services.execution_service.ExecutionService.run_crew_execution') as mock_run_crew, \
             patch.object(execution_service, '_check_for_running_jobs') as mock_check_jobs, \
             patch('asyncio.create_task') as mock_create_task:

            # Setup mocks
            mock_status_service.create_execution = AsyncMock(return_value=True)
            mock_status_service.update_status = AsyncMock(return_value=True)
            mock_run_crew.return_value = {"status": "completed", "result": {"output": "workflow success"}}
            mock_check_jobs.return_value = None
            mock_create_task.return_value = MagicMock()

            # Mock the execution name service response
            mock_name_response = MagicMock()
            mock_name_response.name = "Test Workflow"
            execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_name_response)

            # Step 1: Create execution
            create_result = await execution_service.create_execution(
                sample_crew_config, group_context=mock_group_context
            )

            assert create_result["status"] == ExecutionStatus.RUNNING.value
            execution_id = create_result["execution_id"]

            # Step 2: Simulate background execution
            await ExecutionService._run_in_background(
                execution_id, sample_crew_config, "crew", mock_group_context
            )

            # Verify all service calls were made
            mock_status_service.create_execution.assert_called_once()
            mock_run_crew.assert_called_once()

    @pytest.mark.asyncio
    async def test_execution_workflow_with_error_recovery(self, execution_service, sample_crew_config, mock_group_context):
        """Test execution workflow with error handling and recovery."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('src.services.execution_service.ExecutionService.run_crew_execution') as mock_run_crew, \
             patch.object(execution_service, '_check_for_running_jobs') as mock_check_jobs, \
             patch('asyncio.create_task') as mock_create_task:

            # Setup mocks
            mock_status_service.create_execution = AsyncMock(return_value=True)
            mock_status_service.update_status = AsyncMock(return_value=True)
            mock_run_crew.side_effect = Exception("Simulated execution error")
            mock_check_jobs.return_value = None
            mock_create_task.return_value = MagicMock()

            # Mock the execution name service response
            mock_name_response = MagicMock()
            mock_name_response.name = "Error Workflow"
            execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_name_response)

            # Create and run execution
            create_result = await execution_service.create_execution(
                sample_crew_config, group_context=mock_group_context
            )
            execution_id = create_result["execution_id"]

            # Run background execution (should handle error gracefully)
            await ExecutionService._run_in_background(
                execution_id, sample_crew_config, "crew", mock_group_context
            )

            # Verify status updates were called
            mock_status_service.create_execution.assert_called_once()

    @pytest.mark.asyncio
    async def test_memory_and_database_integration(self):
        """Test integration between memory and database storage."""
        # Add execution to memory
        execution_id = "memory-db-test"
        ExecutionService.add_execution_to_memory(
            execution_id, "RUNNING", "Memory Test", datetime.now()
        )

        # Verify it's in memory
        memory_exec = ExecutionService.get_execution(execution_id)
        assert memory_exec is not None
        assert memory_exec["execution_id"] == execution_id

        # Test listing executions includes both DB and memory
        mock_session = AsyncMock()

        with patch('src.repositories.execution_repository.ExecutionRepository') as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.get_execution_history = AsyncMock(return_value=([], 0))  # Empty DB
            mock_repo_class.return_value = mock_repo

            service = ExecutionService(session=mock_session)
            result = await service.list_executions(["group-1"])

            # Should include the memory execution
            memory_executions = [r for r in result if r["execution_id"] == execution_id]
            assert len(memory_executions) == 1

        # Cleanup
        del ExecutionService.executions[execution_id]

    @pytest.mark.asyncio
    async def test_create_execution_flow_with_most_recent_flow(self, execution_service, mock_group_context):
        """Test flow execution creation that finds most recent flow."""
        config = CrewConfig(
            agents_yaml={},
            tasks_yaml={},
            model="gpt-4o-mini",
            planning=False,
            execution_type="flow",
            inputs={},  # No flow_id
            schema_detection_enabled=False
        )

        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch.object(execution_service, '_check_for_running_jobs') as mock_check_jobs, \
             patch('src.db.session.async_session_factory') as mock_session_factory, \
             patch('asyncio.create_task') as mock_create_task:

            mock_status_service.create_execution = AsyncMock(return_value=True)
            mock_check_jobs.return_value = None
            mock_create_task.return_value = MagicMock()

            # Mock the async session and query to return a flow
            mock_db = AsyncMock()
            mock_flow = MagicMock()
            mock_flow.id = uuid.uuid4()
            mock_result = MagicMock()
            mock_result.scalars.return_value.first.return_value = mock_flow
            mock_db.execute.return_value = mock_result
            mock_session_factory.return_value.__aenter__.return_value = mock_db

            # Mock the execution name service response
            mock_name_response = MagicMock()
            mock_name_response.name = "Flow Most Recent"
            execution_service.execution_name_service.generate_execution_name = AsyncMock(return_value=mock_name_response)

            result = await execution_service.create_execution(
                config, group_context=mock_group_context
            )

            assert isinstance(result, dict)
            assert result["status"] == ExecutionStatus.RUNNING.value
