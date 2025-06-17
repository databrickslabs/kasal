"""
Unit tests for CrewAIExecutionService.

Tests the functionality of CrewAI execution operations including
crew execution, flow execution, status tracking, and execution management.
"""
import pytest
import uuid
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from datetime import datetime, UTC
from typing import Dict, Any, Optional, List

from src.services.crewai_execution_service import CrewAIExecutionService, JobStatus, executions, _active_tasks
from src.schemas.execution import CrewConfig
from src.models.execution_status import ExecutionStatus
from src.utils.user_context import GroupContext


# Test fixtures
@pytest.fixture
def crew_config():
    """Create a test CrewConfig instance."""
    return CrewConfig(
        agents_yaml={"agent1": {"role": "researcher", "goal": "research"}},
        tasks_yaml={"task1": {"description": "research task", "agent": "agent1"}},
        inputs={"query": "test query"},
        planning=False,
        model="gpt-4o-mini"
    )


@pytest.fixture
def group_context():
    """Create a test GroupContext instance."""
    return GroupContext(
        group_ids=["test-group"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="test-user",
        access_token="test-token"
    )


@pytest.fixture
def execution_service():
    """Create a CrewAIExecutionService instance."""
    return CrewAIExecutionService()


@pytest.fixture(autouse=True)
def cleanup_executions():
    """Clean up executions dictionary after each test."""
    yield
    executions.clear()
    _active_tasks.clear()


class TestCrewAIExecutionService:
    """Test cases for CrewAIExecutionService."""
    
    def test_init(self, execution_service):
        """Test service initialization."""
        assert isinstance(execution_service, CrewAIExecutionService)
    
    @pytest.mark.asyncio
    async def test_prepare_and_run_crew_success(self, execution_service, crew_config, group_context):
        """Test successful crew preparation and execution."""
        execution_id = "test-exec-123"
        
        # Mock the engine factory and engine
        mock_engine = AsyncMock()
        # Create a proper mock task that's already done
        mock_init_task = Mock()
        mock_init_task.done.return_value = True
        mock_engine._init_task = mock_init_task
        mock_engine.run_execution.return_value = {"status": "running"}
        
        with patch('src.services.crewai_execution_service.EngineFactory.get_engine', return_value=mock_engine):
            result = await execution_service.prepare_and_run_crew(
                execution_id=execution_id,
                config=crew_config,
                group_context=group_context
            )
            
            assert result["execution_id"] == execution_id
            assert result["status"] == ExecutionStatus.RUNNING.value
            mock_engine.run_execution.assert_called_once_with(execution_id, crew_config, group_context)
    
    @pytest.mark.asyncio
    async def test_prepare_and_run_crew_with_init_task_waiting(self, execution_service, crew_config):
        """Test crew execution with waiting for engine initialization."""
        execution_id = "test-exec-456"
        
        # Mock the engine factory and engine with init task not done
        mock_engine = AsyncMock()
        # Create a proper async task that can be awaited
        mock_init_task = asyncio.create_task(asyncio.sleep(0))
        mock_engine._init_task = mock_init_task
        mock_engine.run_execution.return_value = {"status": "running"}
        
        # Mock the done method to return False initially
        with patch.object(mock_init_task, 'done', return_value=False):
            with patch('src.services.crewai_execution_service.EngineFactory.get_engine', return_value=mock_engine):
                result = await execution_service.prepare_and_run_crew(
                    execution_id=execution_id,
                    config=crew_config
                )
                
                assert result["execution_id"] == execution_id
                assert result["status"] == ExecutionStatus.RUNNING.value
    
    @pytest.mark.asyncio
    async def test_prepare_and_run_crew_without_init_task(self, execution_service, crew_config):
        """Test crew execution when engine doesn't have _init_task attribute."""
        execution_id = "test-exec-no-init"
        
        # Mock the engine factory and engine without _init_task attribute
        mock_engine = AsyncMock()
        # Don't set _init_task attribute
        mock_engine.run_execution.return_value = {"status": "running"}
        
        with patch('src.services.crewai_execution_service.EngineFactory.get_engine', return_value=mock_engine):
            result = await execution_service.prepare_and_run_crew(
                execution_id=execution_id,
                config=crew_config
            )
            
            assert result["execution_id"] == execution_id
            assert result["status"] == ExecutionStatus.RUNNING.value
            mock_engine.run_execution.assert_called_once_with(execution_id, crew_config, None)
    
    @pytest.mark.asyncio
    async def test_prepare_and_run_crew_failure(self, execution_service, crew_config):
        """Test crew execution failure."""
        execution_id = "test-exec-789"
        error_message = "Engine initialization failed"
        
        with patch('src.services.crewai_execution_service.EngineFactory.get_engine', side_effect=Exception(error_message)), \
             patch('src.services.crewai_execution_service.ExecutionStatusService.update_status') as mock_update_status:
            
            with pytest.raises(Exception) as exc_info:
                await execution_service.prepare_and_run_crew(
                    execution_id=execution_id,
                    config=crew_config
                )
            
            assert str(exc_info.value) == error_message
            mock_update_status.assert_called_once_with(
                job_id=execution_id,
                status=ExecutionStatus.FAILED.value,
                message=f"Crew execution failed: {error_message}"
            )
    
    @pytest.mark.asyncio
    async def test_prepare_engine_success(self, execution_service, crew_config):
        """Test successful engine preparation."""
        mock_engine = AsyncMock()
        
        with patch('src.services.crewai_execution_service.EngineFactory.get_engine', return_value=mock_engine):
            result = await execution_service._prepare_engine(crew_config)
            
            assert result == mock_engine
    
    @pytest.mark.asyncio
    async def test_prepare_engine_failure(self, execution_service, crew_config):
        """Test engine preparation failure."""
        with patch('src.services.crewai_execution_service.EngineFactory.get_engine', return_value=None):
            with pytest.raises(ValueError) as exc_info:
                await execution_service._prepare_engine(crew_config)
            
            assert str(exc_info.value) == "Failed to initialize CrewAI engine"
    
    @pytest.mark.asyncio
    async def test_run_crew_execution_success(self, execution_service, crew_config, group_context):
        """Test successful crew execution run."""
        execution_id = "test-exec-abc"
        
        # Mock asyncio.create_task to return a mock task
        mock_task = Mock()
        mock_task.add_done_callback = Mock()
        
        with patch('asyncio.create_task', return_value=mock_task), \
             patch.object(execution_service, 'prepare_and_run_crew') as mock_prepare_run:
            mock_prepare_run.return_value = {"execution_id": execution_id, "status": "running"}
            
            result = await execution_service.run_crew_execution(
                execution_id=execution_id,
                config=crew_config,
                group_context=group_context
            )
            
            assert result["execution_id"] == execution_id
            assert result["status"] == ExecutionStatus.RUNNING.value
            assert result["message"] == "CrewAI execution started successfully"
            
            # Verify execution was stored in memory
            assert execution_id in executions
            assert executions[execution_id]["status"] == ExecutionStatus.PENDING.value
            assert "task" in executions[execution_id]
            assert "created_at" in executions[execution_id]
    
    def test_get_execution_found(self, execution_service):
        """Test getting execution that exists."""
        execution_id = "test-exec-def"
        execution_data = {
            "execution_id": execution_id,
            "status": "running",
            "created_at": datetime.now()
        }
        executions[execution_id] = execution_data
        
        result = CrewAIExecutionService.get_execution(execution_id)
        assert result == execution_data
    
    def test_get_execution_not_found(self, execution_service):
        """Test getting execution that doesn't exist."""
        execution_id = "non-existent"
        
        result = CrewAIExecutionService.get_execution(execution_id)
        assert result is None
    
    def test_add_execution_to_memory(self, execution_service):
        """Test adding execution to memory."""
        execution_id = "test-exec-ghi"
        status = "running"
        run_name = "test_run"
        created_at = datetime.now()
        
        CrewAIExecutionService.add_execution_to_memory(
            execution_id=execution_id,
            status=status,
            run_name=run_name,
            created_at=created_at
        )
        
        assert execution_id in executions
        assert executions[execution_id]["execution_id"] == execution_id
        assert executions[execution_id]["status"] == status
        assert executions[execution_id]["run_name"] == run_name
        assert executions[execution_id]["created_at"] == created_at
    
    def test_add_execution_to_memory_no_created_at(self, execution_service):
        """Test adding execution to memory without created_at."""
        execution_id = "test-exec-jkl"
        status = "pending"
        run_name = "test_run_2"
        
        CrewAIExecutionService.add_execution_to_memory(
            execution_id=execution_id,
            status=status,
            run_name=run_name
        )
        
        assert execution_id in executions
        assert executions[execution_id]["execution_id"] == execution_id
        assert executions[execution_id]["status"] == status
        assert executions[execution_id]["run_name"] == run_name
        assert isinstance(executions[execution_id]["created_at"], datetime)
    
    @pytest.mark.asyncio
    async def test_update_execution_status_with_existing_execution(self, execution_service):
        """Test updating status of existing execution."""
        execution_id = "test-exec-mno"
        executions[execution_id] = {"status": "running"}
        
        with patch('src.services.crewai_execution_service.ExecutionStatusService.update_status') as mock_update:
            await execution_service.update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.COMPLETED,
                message="Execution completed",
                result={"output": "test result"}
            )
            
            # Verify memory update
            assert executions[execution_id]["status"] == ExecutionStatus.COMPLETED.value
            assert executions[execution_id]["message"] == "Execution completed"
            assert executions[execution_id]["result"] == {"output": "test result"}
            
            # Verify database update
            mock_update.assert_called_once_with(
                job_id=execution_id,
                status=ExecutionStatus.COMPLETED.value,
                message="Execution completed",
                result={"output": "test result"}
            )
    
    @pytest.mark.asyncio
    async def test_update_execution_status_without_existing_execution(self, execution_service):
        """Test updating status of non-existing execution."""
        execution_id = "test-exec-pqr"
        
        with patch('src.services.crewai_execution_service.ExecutionStatusService.update_status') as mock_update:
            await execution_service.update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                message="Execution failed"
            )
            
            # Verify execution not in memory
            assert execution_id not in executions
            
            # Verify database update still called
            mock_update.assert_called_once_with(
                job_id=execution_id,
                status=ExecutionStatus.FAILED.value,
                message="Execution failed",
                result=None
            )
    
    @pytest.mark.asyncio
    async def test_cancel_execution_success(self, execution_service):
        """Test successful execution cancellation."""
        execution_id = "test-exec-stu"
        executions[execution_id] = {"status": "running"}
        
        mock_engine = AsyncMock()
        mock_engine.cancel_execution.return_value = True
        
        with patch('src.services.crewai_execution_service.EngineFactory.get_engine', return_value=mock_engine):
            result = await execution_service.cancel_execution(execution_id)
            
            assert result is True
            mock_engine.cancel_execution.assert_called_once_with(execution_id)
    
    @pytest.mark.asyncio
    async def test_cancel_execution_not_found(self, execution_service):
        """Test cancelling execution that doesn't exist."""
        execution_id = "non-existent"
        
        result = await execution_service.cancel_execution(execution_id)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_cancel_execution_no_engine(self, execution_service):
        """Test cancelling execution when engine is not available."""
        execution_id = "test-exec-vwx"
        executions[execution_id] = {"status": "running"}
        
        with patch('src.services.crewai_execution_service.EngineFactory.get_engine', return_value=None):
            result = await execution_service.cancel_execution(execution_id)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_get_execution_status_from_memory_terminal(self, execution_service):
        """Test getting status from memory for terminal states."""
        execution_id = "test-exec-yz"
        execution_data = {
            "status": ExecutionStatus.COMPLETED.value,
            "result": "test result"
        }
        executions[execution_id] = execution_data
        
        result = await execution_service.get_execution_status(execution_id)
        assert result == execution_data
    
    @pytest.mark.asyncio
    async def test_get_execution_status_from_engine(self, execution_service):
        """Test getting status from engine for non-terminal states."""
        execution_id = "test-exec-123"
        executions[execution_id] = {"status": ExecutionStatus.RUNNING.value}
        
        mock_engine = AsyncMock()
        mock_engine.get_execution_status.return_value = {"status": "running", "progress": 50}
        
        with patch('src.services.crewai_execution_service.EngineFactory.get_engine', return_value=mock_engine):
            result = await execution_service.get_execution_status(execution_id)
            
            assert result == {"status": "running", "progress": 50}
            mock_engine.get_execution_status.assert_called_once_with(execution_id)
    
    @pytest.mark.asyncio
    async def test_get_execution_status_no_engine(self, execution_service):
        """Test getting status when engine is not available."""
        execution_id = "test-exec-456"
        executions[execution_id] = {"status": ExecutionStatus.RUNNING.value}
        
        with patch('src.services.crewai_execution_service.EngineFactory.get_engine', return_value=None):
            result = await execution_service.get_execution_status(execution_id)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_with_flow_id_success(self, execution_service, group_context):
        """Test successful flow execution with flow_id."""
        flow_id = "test-flow-123"
        job_id = "test-job-456"
        config = {"param1": "value1"}
        
        # Mock flow repository
        mock_flow = Mock()
        mock_flow.nodes = [{"id": "node1", "type": "agent"}]
        mock_flow.edges = [{"source": "node1", "target": "node2"}]
        mock_flow.flow_config = {"setting": "value"}
        
        mock_flow_repo = Mock()
        mock_flow_repo.find_by_id.return_value = mock_flow
        
        # Mock flow service
        mock_flow_service = AsyncMock()
        mock_flow_service.run_flow.return_value = {"success": True, "job_id": job_id}
        
        with patch('src.services.crewai_execution_service.get_sync_flow_repository', return_value=mock_flow_repo), \
             patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service):
            
            result = await execution_service.run_flow_execution(
                flow_id=flow_id,
                job_id=job_id,
                config=config,
                group_context=group_context
            )
            
            assert result == {"success": True, "job_id": job_id}
            mock_flow_repo.find_by_id.assert_called_once_with(flow_id)
            
            # Check the arguments passed to run_flow
            call_args = mock_flow_service.run_flow.call_args
            assert call_args[1]['flow_id'] == flow_id
            assert call_args[1]['job_id'] == job_id
            assert 'nodes' in call_args[1]['config']
            assert 'edges' in call_args[1]['config']
            assert 'flow_config' in call_args[1]['config']
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_with_nodes_success(self, execution_service):
        """Test successful flow execution with nodes provided."""
        nodes = [{"id": "node1", "type": "agent"}]
        edges = [{"source": "node1", "target": "node2"}]
        job_id = "test-job-789"
        
        # Mock flow service
        mock_flow_service = AsyncMock()
        mock_flow_service.run_flow.return_value = {"success": True, "job_id": job_id}
        
        with patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service):
            result = await execution_service.run_flow_execution(
                nodes=nodes,
                edges=edges,
                job_id=job_id
            )
            
            assert result == {"success": True, "job_id": job_id}
            mock_flow_service.run_flow.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_generates_job_id(self, execution_service):
        """Test flow execution generates job_id when not provided."""
        nodes = [{"id": "node1", "type": "agent"}]
        
        mock_flow_service = AsyncMock()
        mock_flow_service.run_flow.return_value = {"success": True, "job_id": "generated-id"}
        
        with patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service), \
             patch('uuid.uuid4', return_value=Mock(spec=str)) as mock_uuid:
            mock_uuid.return_value = "generated-uuid"
            
            result = await execution_service.run_flow_execution(nodes=nodes)
            
            assert result == {"success": True, "job_id": "generated-id"}
            mock_flow_service.run_flow.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_flow_not_found(self, execution_service):
        """Test flow execution when flow is not found."""
        flow_id = "non-existent-flow"
        job_id = "test-job-abc"
        
        mock_flow_repo = Mock()
        mock_flow_repo.find_by_id.return_value = None
        
        with patch('src.services.crewai_execution_service.get_sync_flow_repository', return_value=mock_flow_repo):
            result = await execution_service.run_flow_execution(
                flow_id=flow_id,
                job_id=job_id
            )
            
            assert result["success"] is False
            assert f"Flow with ID {flow_id} not found" in result["error"]
            assert result["job_id"] == job_id
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_no_flow_id_no_nodes(self, execution_service):
        """Test flow execution with neither flow_id nor nodes."""
        job_id = "test-job-def"
        
        result = await execution_service.run_flow_execution(job_id=job_id)
        
        assert result["success"] is False
        assert "Either flow_id or nodes must be provided" in result["error"]
        assert result["job_id"] == job_id
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_repository_error(self, execution_service):
        """Test flow execution with repository error."""
        flow_id = "test-flow-error"
        job_id = "test-job-error"
        
        mock_flow_repo = Mock()
        mock_flow_repo.find_by_id.side_effect = Exception("Database error")
        
        with patch('src.services.crewai_execution_service.get_sync_flow_repository', return_value=mock_flow_repo):
            result = await execution_service.run_flow_execution(
                flow_id=flow_id,
                job_id=job_id
            )
            
            assert result["success"] is False
            assert "Error loading flow data" in result["error"]
            assert result["job_id"] == job_id
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_flow_service_error(self, execution_service):
        """Test flow execution with flow service error."""
        nodes = [{"id": "node1", "type": "agent"}]
        job_id = "test-job-error"
        error_message = "Flow service error"
        
        mock_flow_service = AsyncMock()
        mock_flow_service.run_flow.side_effect = Exception(error_message)
        
        with patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service), \
             patch('src.services.crewai_execution_service.ExecutionStatusService.update_status') as mock_update_status:
            
            result = await execution_service.run_flow_execution(
                nodes=nodes,
                job_id=job_id
            )
            
            assert result["success"] is False
            assert error_message in result["error"]
            assert result["job_id"] == job_id
            
            mock_update_status.assert_called_once_with(
                job_id=job_id,
                status=ExecutionStatus.FAILED.value,
                message=f"Flow execution failed: {error_message}"
            )
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_with_flow_id_and_empty_nodes_list(self, execution_service):
        """Test flow execution with flow_id and empty nodes list triggers repository lookup."""
        flow_id = "test-flow-with-empty-nodes"
        job_id = "test-job-empty-nodes"
        nodes = []  # Empty list should trigger repository lookup
        
        # Mock flow repository
        mock_flow = Mock()
        mock_flow.nodes = [{"id": "node1", "type": "agent"}]
        mock_flow.edges = [{"source": "node1", "target": "node2"}]
        mock_flow.flow_config = {"setting": "value"}
        
        mock_flow_repo = Mock()
        mock_flow_repo.find_by_id.return_value = mock_flow
        
        # Mock flow service
        mock_flow_service = AsyncMock()
        mock_flow_service.run_flow.return_value = {"success": True, "job_id": job_id}
        
        with patch('src.services.crewai_execution_service.get_sync_flow_repository', return_value=mock_flow_repo), \
             patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service):
            
            result = await execution_service.run_flow_execution(
                flow_id=flow_id,
                nodes=nodes,  # Empty list
                job_id=job_id
            )
            
            assert result == {"success": True, "job_id": job_id}
            mock_flow_repo.find_by_id.assert_called_once_with(flow_id)
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_with_flow_id_and_non_list_nodes(self, execution_service):
        """Test flow execution with flow_id and non-list nodes triggers repository lookup."""
        flow_id = "test-flow-with-non-list-nodes"
        job_id = "test-job-non-list-nodes"
        nodes = "not-a-list"  # Non-list should trigger repository lookup
        
        # Mock flow repository
        mock_flow = Mock()
        mock_flow.nodes = [{"id": "node1", "type": "agent"}]
        mock_flow.edges = [{"source": "node1", "target": "node2"}]
        mock_flow.flow_config = {"setting": "value"}
        
        mock_flow_repo = Mock()
        mock_flow_repo.find_by_id.return_value = mock_flow
        
        # Mock flow service
        mock_flow_service = AsyncMock()
        mock_flow_service.run_flow.return_value = {"success": True, "job_id": job_id}
        
        with patch('src.services.crewai_execution_service.get_sync_flow_repository', return_value=mock_flow_repo), \
             patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service):
            
            result = await execution_service.run_flow_execution(
                flow_id=flow_id,
                nodes=nodes,  # Non-list
                job_id=job_id
            )
            
            assert result == {"success": True, "job_id": job_id}
            mock_flow_repo.find_by_id.assert_called_once_with(flow_id)
    
    @pytest.mark.asyncio
    async def test_run_flow_execution_outer_exception_handler(self, execution_service):
        """Test flow execution outer exception handler (lines 424-431)."""
        job_id = "test-job-outer-exception"
        error_message = "Outer exception error"
        
        # Mock CrewAIFlowService constructor to fail - this happens inside the try block
        with patch('src.services.crewai_execution_service.CrewAIFlowService', side_effect=Exception(error_message)), \
             patch('src.services.crewai_execution_service.ExecutionStatusService.update_status') as mock_update_status:
            
            result = await execution_service.run_flow_execution(
                job_id=job_id,
                nodes=[{"id": "node1"}]
            )
            
            assert result["success"] is False
            assert error_message in result["error"]
            assert result["job_id"] == job_id
            
            mock_update_status.assert_called_once_with(
                job_id=job_id,
                status=ExecutionStatus.FAILED.value,
                message=f"Unexpected error in flow execution: {error_message}"
            )
    
    @pytest.mark.asyncio
    async def test_get_flow_execution_success(self, execution_service):
        """Test successful flow execution retrieval."""
        execution_id = 123
        expected_result = {"id": execution_id, "status": "completed"}
        
        mock_flow_service = AsyncMock()
        mock_flow_service.get_flow_execution.return_value = expected_result
        
        with patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service):
            result = await execution_service.get_flow_execution(execution_id)
            
            assert result == expected_result
            mock_flow_service.get_flow_execution.assert_called_once_with(execution_id)
    
    @pytest.mark.asyncio
    async def test_get_flow_execution_error(self, execution_service):
        """Test flow execution retrieval with error."""
        execution_id = 456
        error_message = "Flow execution not found"
        
        mock_flow_service = AsyncMock()
        mock_flow_service.get_flow_execution.side_effect = Exception(error_message)
        
        with patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service):
            with pytest.raises(Exception) as exc_info:
                await execution_service.get_flow_execution(execution_id)
            
            assert str(exc_info.value) == error_message
    
    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_success(self, execution_service):
        """Test successful flow executions retrieval by flow."""
        flow_id = "test-flow-789"
        expected_result = {"executions": [{"id": 1, "status": "completed"}]}
        
        mock_flow_service = AsyncMock()
        mock_flow_service.get_flow_executions_by_flow.return_value = expected_result
        
        with patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service):
            result = await execution_service.get_flow_executions_by_flow(flow_id)
            
            assert result == expected_result
            mock_flow_service.get_flow_executions_by_flow.assert_called_once_with(flow_id)
    
    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_error(self, execution_service):
        """Test flow executions retrieval by flow with error."""
        flow_id = "test-flow-error"
        error_message = "Flow not found"
        
        mock_flow_service = AsyncMock()
        mock_flow_service.get_flow_executions_by_flow.side_effect = Exception(error_message)
        
        with patch('src.services.crewai_execution_service.CrewAIFlowService', return_value=mock_flow_service):
            with pytest.raises(Exception) as exc_info:
                await execution_service.get_flow_executions_by_flow(flow_id)
            
            assert str(exc_info.value) == error_message


class TestJobStatus:
    """Test cases for JobStatus enum."""
    
    def test_job_status_values(self):
        """Test JobStatus enum values."""
        assert JobStatus.PENDING.value == "PENDING"
        assert JobStatus.PREPARING.value == "PREPARING"
        assert JobStatus.RUNNING.value == "RUNNING"
        assert JobStatus.COMPLETED.value == "COMPLETED"
        assert JobStatus.FAILED.value == "FAILED"
        assert JobStatus.CANCELLED.value == "CANCELLED"


class TestGlobalVariables:
    """Test cases for global variables and module-level functionality."""
    
    def test_executions_global_variable(self):
        """Test the global executions dictionary."""
        # Should be empty at start
        assert isinstance(executions, dict)
        
        # Can add items
        executions["test"] = {"status": "running"}
        assert "test" in executions
        
        # Clean up
        executions.clear()
    
    def test_active_tasks_global_variable(self):
        """Test the global _active_tasks set."""
        assert isinstance(_active_tasks, set)
        
        # Can add items
        mock_task = Mock()
        _active_tasks.add(mock_task)
        assert mock_task in _active_tasks
        
        # Clean up
        _active_tasks.clear()