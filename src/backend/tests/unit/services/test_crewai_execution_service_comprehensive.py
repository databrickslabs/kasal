"""
Comprehensive unit tests for CrewAIExecutionService.

Tests cover:
- Service initialization
- Crew preparation and execution
- Engine management
- Execution status tracking
- Flow execution
- Cancellation handling
- Memory management for executions
"""

import pytest
import asyncio
import uuid
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock
from datetime import datetime

from src.services.crewai_execution_service import (
    CrewAIExecutionService,
    JobStatus,
    executions,
    _active_tasks
)
from src.models.execution_status import ExecutionStatus
from src.schemas.execution import CrewConfig
from src.utils.user_context import GroupContext


class TestCrewAIExecutionServiceInit:
    """Tests for service initialization."""

    def test_init_creates_instance(self):
        """Test basic initialization."""
        service = CrewAIExecutionService()
        assert service is not None


class TestJobStatus:
    """Tests for JobStatus enum."""

    def test_job_status_values(self):
        """Test all JobStatus values exist."""
        assert JobStatus.PENDING.value == "PENDING"
        assert JobStatus.PREPARING.value == "PREPARING"
        assert JobStatus.RUNNING.value == "RUNNING"
        assert JobStatus.COMPLETED.value == "COMPLETED"
        assert JobStatus.FAILED.value == "FAILED"
        assert JobStatus.CANCELLED.value == "CANCELLED"


class TestPrepareAndRunCrew:
    """Tests for prepare_and_run_crew method."""

    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CrewAIExecutionService()

    @pytest.fixture
    def mock_config(self):
        """Create mock CrewConfig."""
        config = MagicMock(spec=CrewConfig)
        config.inputs = {'key': 'value'}
        config.agents = None
        config.agents_yaml = {'agent_1': {'role': 'researcher', 'name': 'Researcher'}}
        config.tasks = None
        config.tasks_yaml = {'task_1': {'description': 'Research task', 'name': 'Research'}}
        config.model = 'gpt-4'
        config.planning = False
        config.reasoning = False
        return config

    @pytest.fixture
    def mock_group_context(self):
        """Create mock GroupContext."""
        context = MagicMock(spec=GroupContext)
        context.group_ids = ['group-1']
        context.primary_group_id = 'group-1'
        return context

    @pytest.mark.asyncio
    async def test_prepare_and_run_crew_success(self, service, mock_config, mock_group_context):
        """Test successful crew preparation and execution."""
        execution_id = 'exec-123'

        with patch.object(service, '_prepare_engine', new_callable=AsyncMock) as mock_prepare:
            mock_engine = MagicMock()
            mock_engine.run_execution = AsyncMock(return_value={'status': 'RUNNING'})
            mock_engine._init_task = MagicMock()
            mock_engine._init_task.done.return_value = True
            mock_prepare.return_value = mock_engine

            with patch('src.services.crewai_execution_service.request_scoped_session') as mock_factory:
                mock_session = MagicMock()
                mock_context = MagicMock()
                mock_context.__aenter__ = AsyncMock(return_value=mock_session)
                mock_context.__aexit__ = AsyncMock(return_value=None)
                mock_factory.return_value = mock_context

                with patch('src.services.crewai_execution_service.AgentService') as mock_agent_svc:
                    mock_agent_svc_inst = MagicMock()
                    mock_agent_svc_inst.find_by_name = AsyncMock(return_value=None)
                    mock_agent_svc_inst.get = AsyncMock(return_value=None)
                    mock_agent_svc.return_value = mock_agent_svc_inst

                    with patch('src.services.crewai_execution_service.TaskService') as mock_task_svc:
                        mock_task_svc_inst = MagicMock()
                        mock_task_svc_inst.find_by_name = AsyncMock(return_value=None)
                        mock_task_svc_inst.get = AsyncMock(return_value=None)
                        mock_task_svc.return_value = mock_task_svc_inst

                        result = await service.prepare_and_run_crew(
                            execution_id=execution_id,
                            config=mock_config,
                            group_context=mock_group_context
                        )

        assert result['execution_id'] == execution_id
        assert result['status'] == ExecutionStatus.RUNNING.value

    @pytest.mark.asyncio
    async def test_prepare_and_run_crew_with_db_agent(self, service, mock_config, mock_group_context):
        """Test crew preparation with agent from database."""
        execution_id = 'exec-123'

        mock_db_agent = MagicMock()
        mock_db_agent.tool_configs = {'tool1': {'param': 'value'}}

        with patch.object(service, '_prepare_engine', new_callable=AsyncMock) as mock_prepare:
            mock_engine = MagicMock()
            mock_engine.run_execution = AsyncMock(return_value={'status': 'RUNNING'})
            mock_engine._init_task = MagicMock()
            mock_engine._init_task.done.return_value = True
            mock_prepare.return_value = mock_engine

            with patch('src.services.crewai_execution_service.request_scoped_session') as mock_factory:
                mock_session = MagicMock()
                mock_context = MagicMock()
                mock_context.__aenter__ = AsyncMock(return_value=mock_session)
                mock_context.__aexit__ = AsyncMock(return_value=None)
                mock_factory.return_value = mock_context

                with patch('src.services.crewai_execution_service.AgentService') as mock_agent_svc:
                    mock_agent_svc_inst = MagicMock()
                    mock_agent_svc_inst.find_by_name = AsyncMock(return_value=mock_db_agent)
                    mock_agent_svc.return_value = mock_agent_svc_inst

                    with patch('src.services.crewai_execution_service.TaskService') as mock_task_svc:
                        mock_task_svc_inst = MagicMock()
                        mock_task_svc_inst.find_by_name = AsyncMock(return_value=None)
                        mock_task_svc_inst.get = AsyncMock(return_value=None)
                        mock_task_svc.return_value = mock_task_svc_inst

                        result = await service.prepare_and_run_crew(
                            execution_id=execution_id,
                            config=mock_config,
                            group_context=mock_group_context
                        )

        assert result['status'] == ExecutionStatus.RUNNING.value

    @pytest.mark.asyncio
    async def test_prepare_and_run_crew_failure(self, service, mock_config, mock_group_context):
        """Test handling of execution failure."""
        execution_id = 'exec-123'

        with patch.object(service, '_prepare_engine', new_callable=AsyncMock) as mock_prepare:
            mock_prepare.side_effect = Exception("Engine initialization failed")

            with patch('src.services.crewai_execution_service.ExecutionStatusService') as mock_status:
                mock_status.update_status = AsyncMock()

                with pytest.raises(Exception, match="Engine initialization failed"):
                    await service.prepare_and_run_crew(
                        execution_id=execution_id,
                        config=mock_config,
                        group_context=mock_group_context
                    )

                mock_status.update_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_prepare_and_run_crew_hierarchical_process(self, service, mock_group_context):
        """Test crew with hierarchical process type."""
        execution_id = 'exec-123'

        mock_config = MagicMock(spec=CrewConfig)
        mock_config.inputs = {'process': 'hierarchical', 'manager_llm': 'gpt-4'}
        mock_config.agents = [{'id': 'agent-1', 'role': 'researcher'}]
        mock_config.agents_yaml = None
        mock_config.tasks = [{'id': 'task-1', 'description': 'task'}]
        mock_config.tasks_yaml = None
        mock_config.model = 'gpt-4'
        mock_config.planning = False
        mock_config.reasoning = False

        with patch.object(service, '_prepare_engine', new_callable=AsyncMock) as mock_prepare:
            mock_engine = MagicMock()
            mock_engine.run_execution = AsyncMock(return_value={'status': 'RUNNING'})
            mock_engine._init_task = MagicMock()
            mock_engine._init_task.done.return_value = True
            mock_prepare.return_value = mock_engine

            result = await service.prepare_and_run_crew(
                execution_id=execution_id,
                config=mock_config,
                group_context=mock_group_context
            )

        assert result['status'] == ExecutionStatus.RUNNING.value


class TestPrepareEngine:
    """Tests for _prepare_engine method."""

    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CrewAIExecutionService()

    @pytest.mark.asyncio
    async def test_prepare_engine_success(self, service):
        """Test successful engine preparation."""
        mock_config = MagicMock(spec=CrewConfig)
        mock_config.model = 'gpt-4'

        mock_engine = MagicMock()

        with patch('src.services.crewai_execution_service.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service._prepare_engine(mock_config)

            assert result == mock_engine
            mock_factory.get_engine.assert_called_once_with(
                engine_type="crewai",
                initialize=True,
                model='gpt-4'
            )

    @pytest.mark.asyncio
    async def test_prepare_engine_failure(self, service):
        """Test engine preparation failure."""
        mock_config = MagicMock(spec=CrewConfig)
        mock_config.model = 'gpt-4'

        with patch('src.services.crewai_execution_service.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=None)

            with pytest.raises(ValueError, match="Failed to initialize"):
                await service._prepare_engine(mock_config)


class TestRunCrewExecution:
    """Tests for run_crew_execution method."""

    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CrewAIExecutionService()

    @pytest.mark.asyncio
    async def test_run_crew_execution_creates_task(self, service):
        """Test that execution creates async task."""
        execution_id = 'exec-123'
        mock_config = MagicMock(spec=CrewConfig)

        with patch.object(service, 'prepare_and_run_crew', new_callable=AsyncMock) as mock_prepare:
            mock_prepare.return_value = {'status': 'RUNNING'}

            result = await service.run_crew_execution(
                execution_id=execution_id,
                config=mock_config
            )

        assert result['execution_id'] == execution_id
        assert result['status'] == ExecutionStatus.RUNNING.value
        assert execution_id in executions

        # Clean up
        if execution_id in executions:
            task = executions[execution_id].get('task')
            if task:
                task.cancel()
            del executions[execution_id]


class TestGetExecution:
    """Tests for get_execution static method."""

    def test_get_execution_found(self):
        """Test getting existing execution."""
        execution_id = 'test-exec-1'
        executions[execution_id] = {
            'status': 'RUNNING',
            'created_at': datetime.now()
        }

        result = CrewAIExecutionService.get_execution(execution_id)

        assert result is not None
        assert result['status'] == 'RUNNING'

        # Clean up
        del executions[execution_id]

    def test_get_execution_not_found(self):
        """Test getting non-existent execution."""
        result = CrewAIExecutionService.get_execution('non-existent')
        assert result is None


class TestAddExecutionToMemory:
    """Tests for add_execution_to_memory static method."""

    def test_add_execution_to_memory(self):
        """Test adding execution to memory."""
        execution_id = 'mem-exec-1'

        CrewAIExecutionService.add_execution_to_memory(
            execution_id=execution_id,
            status='PENDING',
            run_name='Test Run'
        )

        assert execution_id in executions
        assert executions[execution_id]['status'] == 'PENDING'
        assert executions[execution_id]['run_name'] == 'Test Run'

        # Clean up
        del executions[execution_id]

    def test_add_execution_with_timestamp(self):
        """Test adding execution with custom timestamp."""
        execution_id = 'mem-exec-2'
        custom_time = datetime(2024, 1, 1, 12, 0, 0)

        CrewAIExecutionService.add_execution_to_memory(
            execution_id=execution_id,
            status='RUNNING',
            run_name='Test Run 2',
            created_at=custom_time
        )

        assert executions[execution_id]['created_at'] == custom_time

        # Clean up
        del executions[execution_id]


class TestUpdateExecutionStatus:
    """Tests for update_execution_status method."""

    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CrewAIExecutionService()

    @pytest.mark.asyncio
    async def test_update_execution_status_in_memory_non_terminal(self, service):
        """Test updating execution status in memory with non-terminal status."""
        execution_id = 'status-exec-1'
        executions[execution_id] = {
            'status': 'PENDING',
            'created_at': datetime.now()
        }

        with patch('src.services.crewai_execution_service.ExecutionStatusService') as mock_status:
            mock_status.update_status = AsyncMock()

            await service.update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.RUNNING,
                message='Execution running',
                result=None
            )

        # Non-terminal status should keep the entry in memory
        assert execution_id in executions
        assert executions[execution_id]['status'] == ExecutionStatus.RUNNING.value

        # Clean up
        del executions[execution_id]

    @pytest.mark.asyncio
    async def test_update_execution_status_terminal_cleans_up(self, service):
        """Test that terminal status removes entry from in-memory executions."""
        execution_id = 'status-exec-terminal'
        executions[execution_id] = {
            'status': 'RUNNING',
            'created_at': datetime.now()
        }

        with patch('src.services.crewai_execution_service.ExecutionStatusService') as mock_status:
            mock_status.update_status = AsyncMock()

            await service.update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.COMPLETED,
                message='Execution completed',
                result={'output': 'test'}
            )

        # Terminal status should remove entry from memory after DB persist
        assert execution_id not in executions

    @pytest.mark.asyncio
    async def test_update_execution_status_all_terminal_statuses_clean_up(self, service):
        """Test that all terminal statuses clean up in-memory entries."""
        terminal_statuses = [
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.STOPPED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        ]
        for terminal_status in terminal_statuses:
            execution_id = f'terminal-{terminal_status.value}'
            executions[execution_id] = {
                'status': 'RUNNING',
                'created_at': datetime.now()
            }

            with patch('src.services.crewai_execution_service.ExecutionStatusService') as mock_status:
                mock_status.update_status = AsyncMock()

                await service.update_execution_status(
                    execution_id=execution_id,
                    status=terminal_status,
                    message=f'Status: {terminal_status.value}',
                )

            assert execution_id not in executions, (
                f"Expected {execution_id} to be removed for terminal status {terminal_status.value}"
            )

    @pytest.mark.asyncio
    async def test_update_execution_status_not_in_memory(self, service):
        """Test updating status when execution not in memory."""
        execution_id = 'not-in-mem'

        with patch('src.services.crewai_execution_service.ExecutionStatusService') as mock_status:
            mock_status.update_status = AsyncMock()

            # Should not raise, just update database
            await service.update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                message='Failed'
            )

            mock_status.update_status.assert_called_once()

        # Should still not be in memory (pop on non-existent key is safe)
        assert execution_id not in executions


class TestCancelExecution:
    """Tests for cancel_execution method."""

    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CrewAIExecutionService()

    @pytest.mark.asyncio
    async def test_cancel_execution_success(self, service):
        """Test successful execution cancellation."""
        execution_id = 'cancel-exec-1'
        executions[execution_id] = {
            'status': 'RUNNING',
            'created_at': datetime.now()
        }

        mock_engine = MagicMock()
        mock_engine.cancel_execution = AsyncMock(return_value=True)

        with patch('src.services.crewai_execution_service.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.cancel_execution(execution_id)

        assert result is True
        mock_engine.cancel_execution.assert_called_once_with(execution_id)

        # Clean up
        del executions[execution_id]

    @pytest.mark.asyncio
    async def test_cancel_execution_not_found(self, service):
        """Test cancelling non-existent execution."""
        result = await service.cancel_execution('non-existent')
        assert result is False

    @pytest.mark.asyncio
    async def test_cancel_execution_no_engine(self, service):
        """Test cancellation when engine not available."""
        execution_id = 'cancel-exec-2'
        executions[execution_id] = {
            'status': 'RUNNING',
            'created_at': datetime.now()
        }

        with patch('src.services.crewai_execution_service.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=None)

            result = await service.cancel_execution(execution_id)

        assert result is False

        # Clean up
        del executions[execution_id]


class TestGetExecutionStatus:
    """Tests for get_execution_status method."""

    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CrewAIExecutionService()

    @pytest.mark.asyncio
    async def test_get_execution_status_terminal(self, service):
        """Test getting terminal status from memory."""
        execution_id = 'status-get-1'
        executions[execution_id] = {
            'status': ExecutionStatus.COMPLETED.value,
            'result': {'output': 'test'},
            'created_at': datetime.now()
        }

        result = await service.get_execution_status(execution_id)

        assert result['status'] == ExecutionStatus.COMPLETED.value

        # Clean up
        del executions[execution_id]

    @pytest.mark.asyncio
    async def test_get_execution_status_from_engine(self, service):
        """Test getting status from engine for running execution."""
        execution_id = 'status-get-2'
        executions[execution_id] = {
            'status': ExecutionStatus.RUNNING.value,
            'created_at': datetime.now()
        }

        mock_engine = MagicMock()
        mock_engine.get_execution_status = AsyncMock(return_value={
            'status': 'RUNNING',
            'progress': 50
        })

        with patch('src.services.crewai_execution_service.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.get_execution_status(execution_id)

        assert result is not None

        # Clean up
        del executions[execution_id]


class TestRunFlowExecution:
    """Tests for run_flow_execution method."""

    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CrewAIExecutionService()

    @pytest.fixture
    def mock_group_context(self):
        """Create mock GroupContext."""
        context = MagicMock(spec=GroupContext)
        context.group_ids = ['group-1']
        context.primary_group_id = 'group-1'
        context.access_token = 'token-123'
        return context

    @pytest.mark.asyncio
    async def test_run_flow_execution_with_flow_id(self, service, mock_group_context):
        """Test running flow execution with flow_id."""
        flow_id = str(uuid.uuid4())
        job_id = 'flow-job-1'

        mock_flow = MagicMock()
        mock_flow.nodes = [{'id': 'node-1'}]
        mock_flow.edges = []
        mock_flow.flow_config = {}

        with patch('src.services.crewai_execution_service.request_scoped_session') as mock_factory:
            mock_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.repositories.flow_repository.FlowRepository') as mock_flow_repo:
                mock_flow_repo_inst = MagicMock()
                mock_flow_repo_inst.get = AsyncMock(return_value=mock_flow)
                mock_flow_repo.return_value = mock_flow_repo_inst

                with patch('src.services.crewai_execution_service.CrewAIFlowService') as mock_flow_svc:
                    mock_flow_svc_inst = MagicMock()
                    mock_flow_svc_inst.run_flow = AsyncMock(return_value={
                        'success': True,
                        'job_id': job_id
                    })
                    mock_flow_svc.return_value = mock_flow_svc_inst

                    with patch('src.utils.user_context.UserContext'):
                        result = await service.run_flow_execution(
                            flow_id=flow_id,
                            job_id=job_id,
                            group_context=mock_group_context
                        )

        assert result['success'] is True

    @pytest.mark.asyncio
    async def test_run_flow_execution_with_nodes(self, service, mock_group_context):
        """Test running flow execution with direct nodes."""
        job_id = 'flow-job-2'
        nodes = [{'id': 'node-1', 'type': 'crew'}]
        edges = []

        with patch('src.services.crewai_execution_service.request_scoped_session') as mock_factory:
            mock_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.services.crewai_execution_service.CrewAIFlowService') as mock_flow_svc:
                mock_flow_svc_inst = MagicMock()
                mock_flow_svc_inst.run_flow = AsyncMock(return_value={
                    'success': True,
                    'job_id': job_id
                })
                mock_flow_svc.return_value = mock_flow_svc_inst

                with patch('src.utils.user_context.UserContext'):
                    result = await service.run_flow_execution(
                        nodes=nodes,
                        edges=edges,
                        job_id=job_id,
                        group_context=mock_group_context
                    )

        assert result['success'] is True

    @pytest.mark.asyncio
    async def test_run_flow_execution_flow_not_found(self, service, mock_group_context):
        """Test flow execution when flow not found."""
        flow_id = str(uuid.uuid4())
        job_id = 'flow-job-3'

        with patch('src.services.crewai_execution_service.request_scoped_session') as mock_factory:
            mock_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.repositories.flow_repository.FlowRepository') as mock_flow_repo:
                mock_flow_repo_inst = MagicMock()
                mock_flow_repo_inst.get = AsyncMock(return_value=None)
                mock_flow_repo.return_value = mock_flow_repo_inst

                result = await service.run_flow_execution(
                    flow_id=flow_id,
                    job_id=job_id,
                    group_context=mock_group_context
                )

        assert result['success'] is False
        assert 'not found' in result['error']

    @pytest.mark.asyncio
    async def test_run_flow_execution_no_flow_or_nodes(self, service):
        """Test flow execution without flow_id or nodes."""
        result = await service.run_flow_execution()

        assert result['success'] is False
        assert 'Either flow_id or nodes must be provided' in result['error']

    @pytest.mark.asyncio
    async def test_run_flow_execution_with_resume(self, service, mock_group_context):
        """Test flow execution with checkpoint resume."""
        flow_id = str(uuid.uuid4())
        job_id = 'flow-job-resume'
        nodes = [{'id': 'node-1'}]

        with patch('src.services.crewai_execution_service.request_scoped_session') as mock_factory:
            mock_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.services.crewai_execution_service.CrewAIFlowService') as mock_flow_svc:
                mock_flow_svc_inst = MagicMock()
                mock_flow_svc_inst.run_flow = AsyncMock(return_value={
                    'success': True,
                    'job_id': job_id
                })
                mock_flow_svc.return_value = mock_flow_svc_inst

                with patch('src.utils.user_context.UserContext'):
                    result = await service.run_flow_execution(
                        flow_id=flow_id,
                        nodes=nodes,
                        job_id=job_id,
                        config={
                            'resume_from_flow_uuid': 'uuid-123',
                            'resume_from_execution_id': 1,
                            'resume_from_crew_sequence': 2
                        },
                        group_context=mock_group_context
                    )

        assert result['success'] is True


class TestGetFlowExecution:
    """Tests for get_flow_execution method."""

    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CrewAIExecutionService()

    @pytest.mark.asyncio
    async def test_get_flow_execution_success(self, service):
        """Test getting flow execution details."""
        execution_id = 1

        with patch('src.services.crewai_execution_service.request_scoped_session') as mock_factory:
            mock_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.services.crewai_execution_service.CrewAIFlowService') as mock_flow_svc:
                mock_flow_svc_inst = MagicMock()
                mock_flow_svc_inst.get_flow_execution = AsyncMock(return_value={
                    'success': True,
                    'execution': {'id': 1, 'status': 'COMPLETED'}
                })
                mock_flow_svc.return_value = mock_flow_svc_inst

                result = await service.get_flow_execution(execution_id)

        assert result['success'] is True

    @pytest.mark.asyncio
    async def test_get_flow_execution_error(self, service):
        """Test error handling in get_flow_execution."""
        execution_id = 1

        with patch('src.services.crewai_execution_service.request_scoped_session') as mock_factory:
            mock_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.services.crewai_execution_service.CrewAIFlowService') as mock_flow_svc:
                mock_flow_svc_inst = MagicMock()
                mock_flow_svc_inst.get_flow_execution = AsyncMock(
                    side_effect=Exception("Database error")
                )
                mock_flow_svc.return_value = mock_flow_svc_inst

                with pytest.raises(Exception, match="Database error"):
                    await service.get_flow_execution(execution_id)


class TestGetFlowExecutionsByFlow:
    """Tests for get_flow_executions_by_flow method."""

    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CrewAIExecutionService()

    @pytest.mark.asyncio
    async def test_get_flow_executions_success(self, service):
        """Test getting flow executions by flow."""
        flow_id = str(uuid.uuid4())

        with patch('src.services.crewai_execution_service.request_scoped_session') as mock_factory:
            mock_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.services.crewai_execution_service.CrewAIFlowService') as mock_flow_svc:
                mock_flow_svc_inst = MagicMock()
                mock_flow_svc_inst.get_flow_executions_by_flow = AsyncMock(return_value={
                    'success': True,
                    'executions': [{'id': 1}, {'id': 2}]
                })
                mock_flow_svc.return_value = mock_flow_svc_inst

                result = await service.get_flow_executions_by_flow(flow_id)

        assert result['success'] is True
        assert len(result['executions']) == 2


class TestMemoryCleanup:
    """Tests for memory management."""

    def test_executions_dict_exists(self):
        """Test that global executions dict exists."""
        assert executions is not None
        assert isinstance(executions, dict)

    def test_active_tasks_set_exists(self):
        """Test that global active tasks set exists."""
        assert _active_tasks is not None
        assert isinstance(_active_tasks, set)
