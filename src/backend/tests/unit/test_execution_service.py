"""
Unit tests for ExecutionService.

Tests the functionality of the execution service including
creating, running, and managing executions.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.execution_service import ExecutionService
from src.schemas.execution import ExecutionStatus, CrewConfig, ExecutionNameGenerationRequest
from src.models.execution_history import ExecutionHistory


@pytest.fixture
def mock_crew_config():
    """Create a mock crew configuration."""
    config = MagicMock(spec=CrewConfig)
    config.agents_yaml = {"agent1": {"role": "researcher"}}
    config.tasks_yaml = {"task1": {"description": "research task"}}
    config.model = "gpt-4o-mini"
    config.planning = True
    config.execution_type = "crew"
    config.inputs = {}
    config.schema_detection_enabled = True
    config.nodes = None
    config.edges = None
    config.flow_config = None
    config.flow_id = None
    return config


@pytest.fixture
def mock_flow_config():
    """Create a mock flow configuration."""
    config = MagicMock(spec=CrewConfig)
    config.agents_yaml = {}
    config.tasks_yaml = {}
    config.model = "gpt-4o-mini"
    config.planning = False
    config.execution_type = "flow"
    config.inputs = {"flow_id": str(uuid.uuid4())}
    config.schema_detection_enabled = False
    config.nodes = [{"id": "node1", "type": "agent"}]
    config.edges = [{"source": "node1", "target": "node2"}]
    config.flow_config = {"setting": "value"}
    config.flow_id = uuid.uuid4()
    return config


@pytest.fixture
def mock_execution_history():
    """Create mock execution history objects."""
    execution1 = MagicMock(spec=ExecutionHistory)
    execution1.job_id = "job-123"
    execution1.status = "completed"
    execution1.created_at = datetime.now(UTC)
    execution1.run_name = "Test Execution 1"
    execution1.result = '{"status": "success"}'
    execution1.error = None
    
    execution2 = MagicMock(spec=ExecutionHistory)
    execution2.job_id = "job-456"
    execution2.status = "running"
    execution2.created_at = datetime.now(UTC)
    execution2.run_name = "Test Execution 2"
    execution2.result = None
    execution2.error = None
    
    return [execution1, execution2]


class TestExecutionService:
    """Test cases for ExecutionService."""
    
    def test_create_execution_id(self):
        """Test execution ID generation."""
        execution_id = ExecutionService.create_execution_id()
        
        assert execution_id is not None
        assert isinstance(execution_id, str)
        assert len(execution_id) > 0
        # Should be a valid UUID format
        uuid.UUID(execution_id)
    
    def test_add_execution_to_memory(self):
        """Test adding execution to in-memory storage."""
        execution_id = "test-execution-id"
        status = ExecutionStatus.PENDING.value
        run_name = "Test Execution"
        created_at = datetime.now(UTC)
        
        ExecutionService.add_execution_to_memory(
            execution_id=execution_id,
            status=status,
            run_name=run_name,
            created_at=created_at
        )
        
        assert execution_id in ExecutionService.executions
        execution = ExecutionService.executions[execution_id]
        assert execution["status"] == status
        assert execution["run_name"] == run_name
        assert execution["created_at"] == created_at
        
        # Clean up
        del ExecutionService.executions[execution_id]
    
    def test_sanitize_for_database_dict(self):
        """Test sanitizing dictionary data for database storage."""
        test_data = {
            "string_field": "test",
            "int_field": 123,
            "uuid_field": uuid.uuid4(),
            "nested_dict": {
                "inner_uuid": uuid.uuid4(),
                "inner_string": "inner"
            },
            "list_field": [
                {"item_uuid": uuid.uuid4()},
                "string_item"
            ]
        }
        
        result = ExecutionService.sanitize_for_database(test_data)
        
        assert isinstance(result["string_field"], str)
        assert isinstance(result["int_field"], int)
        assert isinstance(result["uuid_field"], str)
        assert isinstance(result["nested_dict"]["inner_uuid"], str)
        assert isinstance(result["list_field"][0]["item_uuid"], str)
    
    def test_sanitize_for_database_non_serializable(self):
        """Test sanitizing non-JSON serializable data."""
        class NonSerializable:
            pass
        
        test_data = {
            "normal_field": "test",
            "non_serializable": NonSerializable()
        }
        
        result = ExecutionService.sanitize_for_database(test_data)
        
        assert result["normal_field"] == "test"
        assert isinstance(result["non_serializable"], str)
    
    @pytest.mark.asyncio
    async def test_list_executions_success(self, mock_execution_history):
        """Test successful listing of executions."""
        with patch("src.services.execution_service.async_session_factory") as mock_session_factory, \
             patch("src.services.execution_service.ExecutionRepository") as mock_repo_class:
            
            # Setup mocks
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session
            
            mock_repo = AsyncMock()
            mock_repo.list.return_value = mock_execution_history
            mock_repo_class.return_value = mock_repo
            
            # Clear in-memory executions
            ExecutionService.executions.clear()
            
            # Test
            result = await ExecutionService.list_executions()
            
            # Verify
            assert len(result) == 2
            assert result[0]["execution_id"] == "job-123"
            assert result[0]["status"] == "completed"
            assert result[1]["execution_id"] == "job-456"
            assert result[1]["status"] == "running"
            
            mock_repo.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_executions_database_error(self):
        """Test listing executions when database fails."""
        with patch("src.services.execution_service.async_session_factory") as mock_session_factory:
            # Setup mock to raise exception
            mock_session_factory.side_effect = Exception("Database connection failed")
            
            # Add some in-memory executions
            ExecutionService.executions["mem-123"] = {
                "status": "running",
                "run_name": "Memory Execution"
            }
            
            # Test
            result = await ExecutionService.list_executions()
            
            # Verify fallback to memory
            assert len(result) == 1
            assert result[0]["execution_id"] == "mem-123"
            assert result[0]["status"] == "running"
            
            # Clean up
            ExecutionService.executions.clear()
    
    @pytest.mark.asyncio
    async def test_get_execution_status_success(self):
        """Test successful execution status retrieval."""
        execution_id = "test-execution-id"
        mock_execution = MagicMock()
        mock_execution.status = "completed"
        mock_execution.created_at = datetime.now(UTC)
        mock_execution.result = '{"output": "success"}'
        mock_execution.run_name = "Test Run"
        mock_execution.error = None
        
        with patch("src.services.execution_service.ExecutionStatusService") as mock_service:
            mock_service.get_status.return_value = mock_execution
            
            result = await ExecutionService.get_execution_status(execution_id)
            
            assert result is not None
            assert result["execution_id"] == execution_id
            assert result["status"] == "completed"
            assert result["run_name"] == "Test Run"
            
            mock_service.get_status.assert_called_once_with(execution_id)
    
    @pytest.mark.asyncio
    async def test_get_execution_status_not_found(self):
        """Test execution status retrieval when execution not found."""
        execution_id = "nonexistent-execution"
        
        with patch("src.services.execution_service.ExecutionStatusService") as mock_service:
            mock_service.get_status.return_value = None
            
            result = await ExecutionService.get_execution_status(execution_id)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_create_execution_crew_type(self, mock_crew_config):
        """Test creating a crew execution."""
        service = ExecutionService()
        
        with patch.object(service, "execution_name_service") as mock_name_service, \
             patch("src.services.execution_service.ExecutionStatusService") as mock_status_service, \
             patch("src.services.execution_service.ExecutionService.run_crew_execution") as mock_run:
            
            # Setup mocks
            mock_name_response = MagicMock()
            mock_name_response.name = "Generated Crew Name"
            mock_name_service.generate_execution_name.return_value = mock_name_response
            mock_status_service.create_execution.return_value = True
            
            # Mock background tasks
            mock_background_tasks = MagicMock()
            
            # Test
            result = await service.create_execution(mock_crew_config, mock_background_tasks)
            
            # Verify
            assert result["status"] == ExecutionStatus.PENDING.value
            assert result["run_name"] == "Generated Crew Name"
            assert "execution_id" in result
            
            mock_status_service.create_execution.assert_called_once()
            mock_background_tasks.add_task.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_execution_flow_type(self, mock_flow_config):
        """Test creating a flow execution."""
        service = ExecutionService()
        
        with patch.object(service, "execution_name_service") as mock_name_service, \
             patch("src.services.execution_service.ExecutionStatusService") as mock_status_service:
            
            # Setup mocks
            mock_name_response = MagicMock()
            mock_name_response.name = "Generated Flow Name"
            mock_name_service.generate_execution_name.return_value = mock_name_response
            mock_status_service.create_execution.return_value = True
            
            # Test
            result = await service.create_execution(mock_flow_config)
            
            # Verify
            assert result["status"] == ExecutionStatus.PENDING.value
            assert result["run_name"] == "Generated Flow Name"
            assert "execution_id" in result
    
    @pytest.mark.asyncio
    async def test_run_crew_execution_crew_type(self, mock_crew_config):
        """Test running a crew execution."""
        execution_id = "test-execution-id"
        
        with patch("src.services.execution_service.ExecutionStatusService") as mock_status_service, \
             patch("src.services.execution_service.CrewAIExecutionService") as mock_crew_service:
            
            # Setup mocks
            mock_crew_instance = AsyncMock()
            mock_crew_instance.run_crew_execution.return_value = {"status": "completed"}
            mock_crew_service.return_value = mock_crew_instance
            
            # Test
            result = await ExecutionService.run_crew_execution(
                execution_id=execution_id,
                config=mock_crew_config,
                execution_type="crew"
            )
            
            # Verify
            assert result["status"] == "completed"
            mock_status_service.update_status.assert_called()
            mock_crew_instance.run_crew_execution.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_crew_execution_flow_type(self, mock_flow_config):
        """Test running a flow execution."""
        execution_id = "test-execution-id"
        
        with patch("src.services.execution_service.ExecutionStatusService") as mock_status_service, \
             patch("src.services.execution_service.CrewAIExecutionService") as mock_crew_service:
            
            # Setup mocks
            mock_crew_instance = AsyncMock()
            mock_crew_instance.run_flow_execution.return_value = {"status": "completed"}
            mock_crew_service.return_value = mock_crew_instance
            
            # Test
            result = await ExecutionService.run_crew_execution(
                execution_id=execution_id,
                config=mock_flow_config,
                execution_type="flow"
            )
            
            # Verify
            assert result["status"] == "completed"
            mock_crew_instance.run_flow_execution.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_crew_execution_error_handling(self, mock_crew_config):
        """Test error handling in crew execution."""
        execution_id = "test-execution-id"
        
        with patch("src.services.execution_service.ExecutionStatusService") as mock_status_service, \
             patch("src.services.execution_service.CrewAIExecutionService") as mock_crew_service:
            
            # Setup mocks to raise exception
            mock_crew_instance = AsyncMock()
            mock_crew_instance.run_crew_execution.side_effect = Exception("Execution failed")
            mock_crew_service.return_value = mock_crew_instance
            
            # Test
            with pytest.raises(Exception, match="Execution failed"):
                await ExecutionService.run_crew_execution(
                    execution_id=execution_id,
                    config=mock_crew_config,
                    execution_type="crew"
                )
            
            # Verify error status update was attempted
            mock_status_service.update_status.assert_called()
            call_args = mock_status_service.update_status.call_args
            assert call_args[1]["status"] == "failed"
    
    @pytest.mark.asyncio
    async def test_execute_flow_with_flow_id(self):
        """Test executing a flow with flow_id."""
        service = ExecutionService()
        flow_id = uuid.uuid4()
        job_id = "test-job-id"
        
        with patch.object(service, "crewai_execution_service") as mock_crewai_service:
            mock_crewai_service.run_flow_execution.return_value = {"status": "started"}
            
            result = await service.execute_flow(
                flow_id=flow_id,
                job_id=job_id
            )
            
            assert result["status"] == "started"
            mock_crewai_service.run_flow_execution.assert_called_once_with(
                flow_id=str(flow_id),
                nodes=None,
                edges=None,
                job_id=job_id,
                config={}
            )
    
    @pytest.mark.asyncio
    async def test_execute_flow_with_nodes_and_edges(self):
        """Test executing a flow with nodes and edges."""
        service = ExecutionService()
        nodes = [{"id": "node1", "type": "agent"}]
        edges = [{"source": "node1", "target": "node2"}]
        
        with patch.object(service, "crewai_execution_service") as mock_crewai_service:
            mock_crewai_service.run_flow_execution.return_value = {"status": "started"}
            
            result = await service.execute_flow(
                nodes=nodes,
                edges=edges
            )
            
            assert result["status"] == "started"
            mock_crewai_service.run_flow_execution.assert_called_once()
            call_args = mock_crewai_service.run_flow_execution.call_args
            assert call_args[1]["nodes"] == nodes
            assert call_args[1]["edges"] == edges
    
    @pytest.mark.asyncio
    async def test_update_execution_status(self):
        """Test updating execution status."""
        execution_id = "test-execution"
        status = "completed"
        result = {"output": "success"}
        
        with patch("src.services.execution_service.ExecutionStatusService") as mock_service:
            mock_service.update_status.return_value = True
            
            await ExecutionService._update_execution_status(
                execution_id=execution_id,
                status=status,
                result=result
            )
            
            mock_service.update_status.assert_called_once()
            call_args = mock_service.update_status.call_args
            assert call_args[1]["job_id"] == execution_id
            assert call_args[1]["status"] == status
            assert call_args[1]["result"] == result