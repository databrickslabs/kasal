"""
Comprehensive unit tests for FlowRunnerService.

Tests cover:
- Flow execution creation
- Dynamic and existing flow execution
- API key initialization
- Provider detection
- Flow data loading from database
- Error handling and edge cases
"""

import pytest
import uuid
from unittest.mock import MagicMock, AsyncMock, patch, Mock
from datetime import datetime
import inspect

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from src.engines.crewai.flow.flow_runner_service import FlowRunnerService
from src.schemas.flow_execution import FlowExecutionStatus


class TestFlowRunnerServiceInit:
    """Tests for FlowRunnerService initialization."""

    def test_init_with_session(self):
        """Test initialization with database session."""
        mock_session = MagicMock(spec=AsyncSession)

        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                service = FlowRunnerService(mock_session)

        assert service.db == mock_session

    def test_flow_runner_service_init_creates_repositories(self):
        """Test FlowRunnerService __init__ creates all repository instances"""
        mock_db = Mock()

        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_flow_exec_service:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository') as mock_flow_repo:
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository') as mock_task_repo:
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository') as mock_agent_repo:
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository') as mock_tool_repo:
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository') as mock_crew_repo:
                                service = FlowRunnerService(mock_db)

                                # Verify all repositories were created with the database session
                                mock_flow_exec_service.assert_called_once_with(mock_db)
                                mock_flow_repo.assert_called_once_with(mock_db)
                                mock_task_repo.assert_called_once_with(mock_db)
                                mock_agent_repo.assert_called_once_with(mock_db)
                                mock_tool_repo.assert_called_once_with(mock_db)
                                mock_crew_repo.assert_called_once_with(mock_db)

    def test_flow_runner_service_init_stores_attributes(self):
        """Test FlowRunnerService __init__ stores all attributes correctly"""
        mock_db = Mock()

        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                service = FlowRunnerService(mock_db)

                                # Check all attributes are stored
                                assert hasattr(service, 'db')
                                assert hasattr(service, 'flow_execution_service')
                                assert hasattr(service, 'flow_repo')
                                assert hasattr(service, 'task_repo')
                                assert hasattr(service, 'agent_repo')
                                assert hasattr(service, 'tool_repo')
                                assert hasattr(service, 'crew_repo')

                                assert service.db == mock_db


class TestCreateFlowExecution:
    """Tests for create_flow_execution method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return MagicMock(spec=AsyncSession)

    @pytest.fixture
    def service(self, mock_session):
        """Create FlowRunnerService instance."""
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_service:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                svc = FlowRunnerService(mock_session)
                                svc.flow_execution_service = mock_exec_service.return_value
                                return svc

    @pytest.mark.asyncio
    async def test_create_flow_execution_success(self, service):
        """Test successful flow execution creation."""
        mock_execution = MagicMock()
        mock_execution.id = 1
        mock_execution.flow_id = uuid.uuid4()
        mock_execution.status = FlowExecutionStatus.PENDING

        service.flow_execution_service.create_execution = AsyncMock(return_value=mock_execution)

        result = await service.create_flow_execution(
            flow_id='test-flow-123',
            job_id='job-123',
            config={'group_id': 'group-1'}
        )

        assert result['success'] is True
        assert result['job_id'] == 'job-123'
        assert result['execution_id'] == 1

    @pytest.mark.asyncio
    async def test_create_flow_execution_with_uuid_flow_id(self, service):
        """Test create_flow_execution with UUID flow_id"""
        mock_execution = MagicMock()
        mock_execution.id = 1
        mock_execution.flow_id = uuid.uuid4()
        mock_execution.status = "pending"

        service.flow_execution_service.create_execution = AsyncMock(return_value=mock_execution)

        flow_id = uuid.uuid4()
        job_id = "test-job-id"
        config = {"test": "config"}

        result = await service.create_flow_execution(flow_id, job_id, config)

        assert isinstance(result, dict)
        assert result["success"] is True
        assert "execution_id" in result
        assert "job_id" in result
        assert result["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_create_flow_execution_invalid_uuid(self, service):
        """Test handling of invalid UUID format."""
        service.flow_execution_service.create_execution = AsyncMock(
            side_effect=ValueError("Invalid UUID")
        )

        result = await service.create_flow_execution(
            flow_id='invalid-uuid',
            job_id='job-123'
        )

        assert result['success'] is False
        assert 'Invalid UUID' in result['error']

    @pytest.mark.asyncio
    async def test_create_flow_execution_exception(self, service):
        """Test handling of general exception."""
        service.flow_execution_service.create_execution = AsyncMock(
            side_effect=Exception("Database error")
        )

        result = await service.create_flow_execution(
            flow_id='test-flow',
            job_id='job-123'
        )

        assert result['success'] is False
        assert 'Database error' in result['error']

    @pytest.mark.asyncio
    async def test_create_flow_execution_with_none_config(self, service):
        """Test create_flow_execution with None config"""
        mock_execution = MagicMock()
        mock_execution.id = 1
        mock_execution.flow_id = uuid.uuid4()
        mock_execution.status = "pending"

        service.flow_execution_service.create_execution = AsyncMock(return_value=mock_execution)

        flow_id = uuid.uuid4()
        job_id = "test-job-id"

        result = await service.create_flow_execution(flow_id, job_id, None)

        assert isinstance(result, dict)
        assert result["success"] is True


class TestRunFlow:
    """Tests for run_flow method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return MagicMock(spec=AsyncSession)

    @pytest.fixture
    def service(self, mock_session):
        """Create FlowRunnerService instance."""
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_svc:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository') as mock_flow_repo:
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                svc = FlowRunnerService(mock_session)
                                svc.flow_execution_service = mock_exec_svc.return_value
                                svc.flow_repo = mock_flow_repo.return_value
                                return svc

    @pytest.mark.asyncio
    async def test_run_flow_with_config_nodes(self, service):
        """Test running flow with nodes in config."""
        flow_id = str(uuid.uuid4())
        mock_execution = MagicMock()
        mock_execution.id = 1

        service.flow_execution_service.create_execution = AsyncMock(return_value=mock_execution)

        config = {
            'nodes': [{'id': 'node-1', 'type': 'crew'}],
            'edges': [],
            'flow_config': {'startingPoints': [{'nodeId': 'node-1'}]}
        }

        with patch.object(service, '_run_flow_execution', new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {'success': True, 'result': {'output': 'test'}}

            result = await service.run_flow(
                flow_id=flow_id,
                job_id='job-123',
                run_name='test-run',
                config=config
            )

        assert result['status'] == FlowExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_run_flow_loads_from_database(self, service):
        """Test that flow data is loaded from database when not in config."""
        flow_id = uuid.uuid4()
        mock_flow = MagicMock()
        mock_flow.nodes = [{'id': 'node-1'}]
        mock_flow.edges = []
        mock_flow.flow_config = {'startingPoints': []}
        mock_flow.group_id = 'group-1'

        mock_execution = MagicMock()
        mock_execution.id = 1

        service.flow_repo.get = AsyncMock(return_value=mock_flow)
        service.flow_execution_service.create_execution = AsyncMock(return_value=mock_execution)

        with patch.object(service, '_run_flow_execution', new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {'success': True, 'result': {}}

            result = await service.run_flow(
                flow_id=flow_id,
                job_id='job-123',
                config={}
            )

        service.flow_repo.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_flow_not_found(self, service):
        """Test handling when flow not found in database."""
        flow_id = uuid.uuid4()
        service.flow_repo.get = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await service.run_flow(
                flow_id=flow_id,
                job_id='job-123',
                config={}
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_run_flow_invalid_uuid(self, service):
        """Test handling of invalid UUID format."""
        with pytest.raises(HTTPException) as exc_info:
            await service.run_flow(
                flow_id='not-a-valid-uuid',
                job_id='job-123',
                config={}
            )

        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_run_flow_dynamic_no_nodes(self, service):
        """Test dynamic flow with no nodes raises error."""
        with pytest.raises(HTTPException) as exc_info:
            await service.run_flow(
                flow_id=None,
                job_id='job-123',
                config={'nodes': []}
            )

        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_run_flow_failed_execution(self, service):
        """Test handling of failed flow execution."""
        flow_id = str(uuid.uuid4())
        mock_execution = MagicMock()
        mock_execution.id = 1

        service.flow_execution_service.create_execution = AsyncMock(return_value=mock_execution)

        config = {
            'nodes': [{'id': 'node-1'}],
            'edges': [],
            'flow_config': {'startingPoints': []}
        }

        with patch.object(service, '_run_flow_execution', new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {'success': False, 'error': 'Execution failed'}

            result = await service.run_flow(
                flow_id=flow_id,
                job_id='job-123',
                config=config
            )

        assert result['status'] == FlowExecutionStatus.FAILED
        assert 'failed' in result['message'].lower()

    @pytest.mark.asyncio
    async def test_run_flow_extracts_flow_id_from_config(self, service):
        """Test that flow_id is extracted from config when not provided."""
        flow_id = str(uuid.uuid4())
        mock_flow = MagicMock()
        mock_flow.nodes = [{'id': 'node-1'}]
        mock_flow.edges = []
        mock_flow.flow_config = {}
        mock_flow.group_id = None

        mock_execution = MagicMock()
        mock_execution.id = 1

        service.flow_repo.get = AsyncMock(return_value=mock_flow)
        service.flow_execution_service.create_execution = AsyncMock(return_value=mock_execution)

        with patch.object(service, '_run_flow_execution', new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {'success': True, 'result': {}}

            result = await service.run_flow(
                flow_id=None,
                job_id='job-123',
                config={'flow_id': flow_id}
            )

        # Should have extracted flow_id from config


class TestRunDynamicFlow:
    """Tests for _run_dynamic_flow method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return MagicMock(spec=AsyncSession)

    @pytest.mark.asyncio
    async def test_run_dynamic_flow_success(self, mock_session):
        """Test successful dynamic flow execution."""
        with patch('src.engines.crewai.flow.flow_runner_service._smart_db_session') as mock_factory:
            mock_new_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_new_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_svc:
                mock_exec_svc_instance = MagicMock()
                mock_exec_svc_instance.update_execution_status = AsyncMock()
                mock_exec_svc.return_value = mock_exec_svc_instance

                with patch('src.engines.crewai.flow.backend_flow.BackendFlow') as mock_backend:
                    mock_flow = MagicMock()
                    mock_flow.kickoff = AsyncMock(return_value={
                        'success': True,
                        'result': {'output': 'test result'}
                    })
                    mock_backend.return_value = mock_flow

                    with patch('src.engines.crewai.flow.flow_runner_service.ApiKeysService'):
                        with patch('os.makedirs'):
                            service = FlowRunnerService(mock_session)

                            config = {
                                'nodes': [{'id': 'node-1', 'type': 'crew', 'data': {}}],
                                'edges': [],
                                'flow_config': {'listeners': [], 'actions': [], 'startingPoints': []}
                            }

                            result = await service._run_dynamic_flow(1, 'job-123', config)

                            assert result['success'] is True

    @pytest.mark.asyncio
    async def test_run_dynamic_flow_kickoff_error(self, mock_session):
        """Test handling of kickoff error."""
        with patch('src.engines.crewai.flow.flow_runner_service._smart_db_session') as mock_factory:
            mock_new_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_new_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_svc:
                mock_exec_svc_instance = MagicMock()
                mock_exec_svc_instance.update_execution_status = AsyncMock()
                mock_exec_svc.return_value = mock_exec_svc_instance

                with patch('src.engines.crewai.flow.backend_flow.BackendFlow') as mock_backend:
                    mock_flow = MagicMock()
                    mock_flow.kickoff = AsyncMock(side_effect=Exception("Kickoff failed"))
                    mock_backend.return_value = mock_flow

                    with patch('src.engines.crewai.flow.flow_runner_service.ApiKeysService'):
                        with patch('os.makedirs'):
                            service = FlowRunnerService(mock_session)

                            config = {
                                'nodes': [{'id': 'node-1'}],
                                'edges': [],
                                'flow_config': {}
                            }

                            result = await service._run_dynamic_flow(1, 'job-123', config)

                            assert result['success'] is False
                            assert 'Kickoff failed' in result['error']

    @pytest.mark.asyncio
    async def test_run_dynamic_flow_with_flow_uuid(self, mock_session):
        """Test dynamic flow execution returns flow_uuid for checkpointing."""
        with patch('src.engines.crewai.flow.flow_runner_service._smart_db_session') as mock_factory:
            mock_new_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_new_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_svc:
                mock_exec_svc_instance = MagicMock()
                mock_exec_svc_instance.update_execution_status = AsyncMock()
                mock_exec_svc.return_value = mock_exec_svc_instance

                with patch('src.engines.crewai.flow.backend_flow.BackendFlow') as mock_backend:
                    mock_flow = MagicMock()
                    mock_flow.kickoff = AsyncMock(return_value={
                        'success': True,
                        'result': {'output': 'test'},
                        'flow_uuid': 'flow-uuid-123'
                    })
                    mock_backend.return_value = mock_flow

                    with patch('src.engines.crewai.flow.flow_runner_service.ApiKeysService'):
                        with patch('os.makedirs'):
                            with patch('src.services.execution_history_service.ExecutionHistoryService'):
                                service = FlowRunnerService(mock_session)

                                config = {
                                    'nodes': [{'id': 'node-1'}],
                                    'edges': [],
                                    'flow_config': {}
                                }

                                result = await service._run_dynamic_flow(1, 'job-123', config)

                                assert result['success'] is True
                                assert result.get('flow_uuid') == 'flow-uuid-123'


class TestGetRequiredProviders:
    """Tests for _get_required_providers method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return MagicMock(spec=AsyncSession)

    @pytest.fixture
    def service(self, mock_session):
        """Create FlowRunnerService instance."""
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                return FlowRunnerService(mock_session)

    @pytest.mark.asyncio
    async def test_get_required_providers_from_model(self, service, mock_session):
        """Test extracting providers from model config."""
        config = {'model': 'gpt-4'}

        with patch('src.services.model_config_service.ModelConfigService') as mock_model_svc:
            mock_model_svc_instance = MagicMock()
            mock_model_svc_instance.get_model_config = AsyncMock(return_value={'provider': 'openai'})
            mock_model_svc.return_value = mock_model_svc_instance

            providers = await service._get_required_providers(mock_session, config)

            assert 'OPENAI' in providers

    @pytest.mark.asyncio
    async def test_get_required_providers_from_crew_config(self, service, mock_session):
        """Test extracting providers from crew config."""
        config = {
            'model': 'gpt-4',
            'crew': {
                'planning_llm': 'claude-3',
                'manager_llm': 'gpt-4'
            }
        }

        with patch('src.services.model_config_service.ModelConfigService') as mock_model_svc:
            mock_model_svc_instance = MagicMock()

            async def get_config(model_name):
                if 'gpt' in model_name:
                    return {'provider': 'openai'}
                elif 'claude' in model_name:
                    return {'provider': 'anthropic'}
                return None

            mock_model_svc_instance.get_model_config = AsyncMock(side_effect=get_config)
            mock_model_svc.return_value = mock_model_svc_instance

            providers = await service._get_required_providers(mock_session, config)

            assert 'OPENAI' in providers
            assert 'ANTHROPIC' in providers

    @pytest.mark.asyncio
    async def test_get_required_providers_empty_config(self, service, mock_session):
        """Test with empty config returns empty list."""
        config = {}

        providers = await service._get_required_providers(mock_session, config)

        assert providers == []

    @pytest.mark.asyncio
    async def test_get_required_providers_error_handling(self, service, mock_session):
        """Test that errors are handled gracefully."""
        config = {'model': 'unknown-model'}

        with patch('src.services.model_config_service.ModelConfigService') as mock_model_svc:
            mock_model_svc_instance = MagicMock()
            mock_model_svc_instance.get_model_config = AsyncMock(side_effect=Exception("Not found"))
            mock_model_svc.return_value = mock_model_svc_instance

            providers = await service._get_required_providers(mock_session, config)

            # Should return empty list, not raise exception
            assert providers == []

    @pytest.mark.asyncio
    async def test_get_required_providers_from_top_level_config(self, service, mock_session):
        """Test extracting providers from top-level config fields."""
        config = {
            'planning_llm': 'gpt-4',
            'reasoning_llm': 'claude-3'
        }

        with patch('src.services.model_config_service.ModelConfigService') as mock_model_svc:
            mock_model_svc_instance = MagicMock()

            async def get_config(model_name):
                if 'gpt' in model_name:
                    return {'provider': 'openai'}
                elif 'claude' in model_name:
                    return {'provider': 'anthropic'}
                return None

            mock_model_svc_instance.get_model_config = AsyncMock(side_effect=get_config)
            mock_model_svc.return_value = mock_model_svc_instance

            providers = await service._get_required_providers(mock_session, config)

            assert len(providers) == 2


class TestRunFlowExecution:
    """Tests for _run_flow_execution method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return MagicMock(spec=AsyncSession)

    @pytest.mark.asyncio
    async def test_run_flow_execution_success(self, mock_session):
        """Test successful flow execution."""
        flow_id = uuid.uuid4()

        with patch('src.engines.crewai.flow.flow_runner_service._smart_db_session') as mock_factory:
            mock_new_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_new_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_svc:
                mock_exec_svc_instance = MagicMock()
                mock_exec_svc_instance.update_execution_status = AsyncMock()
                mock_exec_svc.return_value = mock_exec_svc_instance

                # Patch both at source and usage location for module-level import
                with patch('src.engines.crewai.flow.flow_runner_service.BackendFlow') as mock_backend:
                    mock_flow = MagicMock()
                    mock_flow.load_flow = AsyncMock(return_value={
                        'nodes': [],
                        'edges': [],
                        'flow_config': {}
                    })
                    mock_flow.kickoff = AsyncMock(return_value={
                        'success': True,
                        'result': {'output': 'test'},
                        'flow_uuid': 'uuid-123'
                    })
                    mock_backend.return_value = mock_flow

                    with patch('src.engines.crewai.flow.flow_runner_service.ApiKeysService'):
                        with patch('os.makedirs'):
                            service = FlowRunnerService(mock_session)

                            config = {
                                'nodes': [{'id': 'node-1'}],
                                'flow_config': {'startingPoints': []}
                            }

                            result = await service._run_flow_execution(1, flow_id, 'job-123', config)

                            assert result['success'] is True
                            assert result['flow_uuid'] == 'uuid-123'

    @pytest.mark.asyncio
    async def test_run_flow_execution_invalid_uuid(self, mock_session):
        """Test handling of invalid UUID string."""
        with patch('src.engines.crewai.flow.flow_runner_service._smart_db_session') as mock_factory:
            mock_new_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_new_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_svc:
                mock_exec_svc_instance = MagicMock()
                mock_exec_svc_instance.update_execution_status = AsyncMock()
                mock_exec_svc.return_value = mock_exec_svc_instance

                service = FlowRunnerService(mock_session)

                result = await service._run_flow_execution(1, 'invalid-uuid', 'job-123', {})

                assert result['success'] is False
                assert 'Invalid UUID' in result['error']

    @pytest.mark.asyncio
    async def test_run_flow_execution_builds_starting_points(self, mock_session):
        """Test that starting points are built when missing."""
        flow_id = uuid.uuid4()

        with patch('src.engines.crewai.flow.flow_runner_service._smart_db_session') as mock_factory:
            mock_new_session = MagicMock()
            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_new_session)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory.return_value = mock_context

            with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_svc:
                mock_exec_svc_instance = MagicMock()
                mock_exec_svc_instance.update_execution_status = AsyncMock()
                mock_exec_svc.return_value = mock_exec_svc_instance

                # Patch at usage location for module-level import
                with patch('src.engines.crewai.flow.flow_runner_service.BackendFlow') as mock_backend:
                    mock_flow = MagicMock()
                    mock_flow.load_flow = AsyncMock(return_value={
                        'nodes': [],
                        'edges': [],
                        'flow_config': {}
                    })
                    mock_flow.kickoff = AsyncMock(return_value={'success': True, 'result': {}})
                    mock_backend.return_value = mock_flow

                    with patch('src.engines.crewai.flow.flow_runner_service.ApiKeysService'):
                        with patch('os.makedirs'):
                            service = FlowRunnerService(mock_session)

                            # Config with nodes/edges but no startingPoints
                            config = {
                                'nodes': [
                                    {'id': 'node-1', 'type': 'crew', 'data': {}},
                                    {'id': 'node-2', 'type': 'crew', 'data': {}}
                                ],
                                'edges': [{'source': 'node-1', 'target': 'node-2'}],
                                'flow_config': {}  # No startingPoints
                            }

                            result = await service._run_flow_execution(1, flow_id, 'job-123', config)

                            assert result['success'] is True


class TestGetFlowExecution:
    """Tests for get_flow_execution method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return MagicMock(spec=AsyncSession)

    @pytest.fixture
    def service(self, mock_session):
        """Create FlowRunnerService instance."""
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_svc:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                svc = FlowRunnerService(mock_session)
                                svc.flow_execution_service = mock_exec_svc.return_value
                                return svc

    @pytest.mark.asyncio
    async def test_get_flow_execution_found(self, service):
        """Test getting existing flow execution."""
        mock_execution = MagicMock()
        mock_execution.id = 1
        mock_execution.flow_id = uuid.uuid4()
        mock_execution.job_id = 'job-123'
        mock_execution.status = FlowExecutionStatus.COMPLETED
        mock_execution.result = {'output': 'test'}
        mock_execution.error = None
        mock_execution.created_at = datetime.now()
        mock_execution.updated_at = datetime.now()
        mock_execution.completed_at = datetime.now()

        service.flow_execution_service.get_execution = AsyncMock(return_value=mock_execution)
        service.flow_execution_service.get_node_executions = AsyncMock(return_value=[])

        result = await service.get_flow_execution(1)

        assert result['success'] is True
        assert result['execution']['id'] == 1

    @pytest.mark.asyncio
    async def test_get_flow_execution_not_found(self, service):
        """Test getting non-existent flow execution."""
        service.flow_execution_service.get_execution = AsyncMock(return_value=None)

        result = await service.get_flow_execution(999)

        assert result['success'] is False
        assert 'not found' in result['error']

    @pytest.mark.asyncio
    async def test_get_flow_execution_with_nodes(self, service):
        """Test getting execution with node executions."""
        mock_execution = MagicMock()
        mock_execution.id = 1
        mock_execution.flow_id = uuid.uuid4()
        mock_execution.job_id = 'job-123'
        mock_execution.status = FlowExecutionStatus.COMPLETED
        mock_execution.result = {}
        mock_execution.error = None
        mock_execution.created_at = datetime.now()
        mock_execution.updated_at = datetime.now()
        mock_execution.completed_at = datetime.now()

        mock_node = MagicMock()
        mock_node.id = 1
        mock_node.node_id = 'node-1'
        mock_node.status = 'completed'
        mock_node.agent_id = 'agent-1'
        mock_node.task_id = 'task-1'
        mock_node.result = {'output': 'node result'}
        mock_node.error = None
        mock_node.created_at = datetime.now()
        mock_node.updated_at = datetime.now()
        mock_node.completed_at = datetime.now()

        service.flow_execution_service.get_execution = AsyncMock(return_value=mock_execution)
        service.flow_execution_service.get_node_executions = AsyncMock(return_value=[mock_node])

        result = await service.get_flow_execution(1)

        assert result['success'] is True
        assert len(result['execution']['nodes']) == 1
        assert result['execution']['nodes'][0]['node_id'] == 'node-1'

    @pytest.mark.asyncio
    async def test_get_flow_execution_error(self, service):
        """Test error handling in get_flow_execution."""
        service.flow_execution_service.get_execution = AsyncMock(
            side_effect=Exception("Database error")
        )

        result = await service.get_flow_execution(1)

        assert result['success'] is False
        assert 'Database error' in result['error']


class TestGetFlowExecutionsByFlow:
    """Tests for get_flow_executions_by_flow method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        return MagicMock(spec=AsyncSession)

    @pytest.fixture
    def service(self, mock_session):
        """Create FlowRunnerService instance."""
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_exec_svc:
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                svc = FlowRunnerService(mock_session)
                                svc.flow_execution_service = mock_exec_svc.return_value
                                return svc

    @pytest.mark.asyncio
    async def test_get_flow_executions_success(self, service):
        """Test getting executions for a flow."""
        flow_id = uuid.uuid4()

        mock_exec1 = MagicMock()
        mock_exec1.id = 1
        mock_exec1.job_id = 'job-1'
        mock_exec1.status = FlowExecutionStatus.COMPLETED
        mock_exec1.created_at = datetime.now()
        mock_exec1.completed_at = datetime.now()

        mock_exec2 = MagicMock()
        mock_exec2.id = 2
        mock_exec2.job_id = 'job-2'
        mock_exec2.status = FlowExecutionStatus.RUNNING
        mock_exec2.created_at = datetime.now()
        mock_exec2.completed_at = None

        service.flow_execution_service.get_executions_by_flow = AsyncMock(
            return_value=[mock_exec1, mock_exec2]
        )

        result = await service.get_flow_executions_by_flow(flow_id)

        assert result['success'] is True
        assert result['flow_id'] == flow_id
        assert len(result['executions']) == 2

    @pytest.mark.asyncio
    async def test_get_flow_executions_empty(self, service):
        """Test getting executions when none exist."""
        flow_id = uuid.uuid4()

        service.flow_execution_service.get_executions_by_flow = AsyncMock(return_value=[])

        result = await service.get_flow_executions_by_flow(flow_id)

        assert result['success'] is True
        assert len(result['executions']) == 0

    @pytest.mark.asyncio
    async def test_get_flow_executions_error(self, service):
        """Test error handling."""
        flow_id = uuid.uuid4()

        service.flow_execution_service.get_executions_by_flow = AsyncMock(
            side_effect=Exception("Database error")
        )

        result = await service.get_flow_executions_by_flow(flow_id)

        assert result['success'] is False
        assert 'Database error' in result['error']


class TestFlowRunnerServiceConstants:
    """Test FlowRunnerService constants and module-level attributes"""

    def test_logger_initialization(self):
        """Test logger is properly initialized"""
        from src.engines.crewai.flow.flow_runner_service import logger

        assert logger is not None
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'warning')

    def test_required_imports(self):
        """Test that required imports are available"""
        from src.engines.crewai.flow import flow_runner_service

        # Check module has the expected structure
        assert hasattr(flow_runner_service, 'FlowRunnerService')
        assert hasattr(flow_runner_service, 'logger')


class TestSmartDbSession:
    """Tests for _smart_db_session async context manager wrapper."""

    @pytest.mark.asyncio
    async def test_smart_db_session_yields_session(self):
        """Test that _smart_db_session yields a session and cleans up."""
        mock_session = MagicMock(spec=AsyncSession)

        async def fake_get_smart():
            yield mock_session

        with patch('src.engines.crewai.flow.flow_runner_service.get_smart_db_session', fake_get_smart):
            from src.engines.crewai.flow.flow_runner_service import _smart_db_session
            async with _smart_db_session() as session:
                assert session is mock_session

    @pytest.mark.asyncio
    async def test_smart_db_session_propagates_exception(self):
        """Test that exceptions inside _smart_db_session propagate correctly."""
        mock_session = MagicMock(spec=AsyncSession)

        async def fake_get_smart():
            yield mock_session

        with patch('src.engines.crewai.flow.flow_runner_service.get_smart_db_session', fake_get_smart):
            from src.engines.crewai.flow.flow_runner_service import _smart_db_session
            with pytest.raises(ValueError, match="test error"):
                async with _smart_db_session() as session:
                    raise ValueError("test error")

    @pytest.mark.asyncio
    async def test_safe_session_uses_smart_db(self):
        """Test that _safe_session delegates to _smart_db_session."""
        mock_session = MagicMock(spec=AsyncSession)

        async def fake_get_smart():
            yield mock_session

        with patch('src.engines.crewai.flow.flow_runner_service.get_smart_db_session', fake_get_smart):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService'):
                with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                                with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                    service = FlowRunnerService(MagicMock())
                                    async with service._safe_session() as session:
                                        assert session is mock_session


class TestFlowRunnerServiceMethodSignatures:
    """Test FlowRunnerService method signatures and basic structure"""

    @pytest.fixture
    def service(self):
        """Create a service with mocked dependencies"""
        mock_db = Mock()
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService'):
            with patch('src.engines.crewai.flow.flow_runner_service.FlowRepository'):
                with patch('src.engines.crewai.flow.flow_runner_service.TaskRepository'):
                    with patch('src.engines.crewai.flow.flow_runner_service.AgentRepository'):
                        with patch('src.engines.crewai.flow.flow_runner_service.ToolRepository'):
                            with patch('src.engines.crewai.flow.flow_runner_service.CrewRepository'):
                                return FlowRunnerService(mock_db)

    def test_create_flow_execution_is_async(self, service):
        """Test create_flow_execution method is async"""
        assert hasattr(service, 'create_flow_execution')
        assert callable(service.create_flow_execution)
        assert inspect.iscoroutinefunction(service.create_flow_execution)

    def test_run_flow_is_async(self, service):
        """Test run_flow method is async"""
        assert hasattr(service, 'run_flow')
        assert callable(service.run_flow)
        assert inspect.iscoroutinefunction(service.run_flow)

    def test_get_flow_execution_is_async(self, service):
        """Test get_flow_execution method is async"""
        assert hasattr(service, 'get_flow_execution')
        assert callable(service.get_flow_execution)
        assert inspect.iscoroutinefunction(service.get_flow_execution)

    def test_all_required_methods_exist(self, service):
        """Test that all required methods exist"""
        required_methods = [
            'create_flow_execution',
            'run_flow',
            'get_flow_execution',
            'get_flow_executions_by_flow',
            '_run_dynamic_flow',
            '_run_flow_execution',
            '_get_required_providers'
        ]

        for method_name in required_methods:
            assert hasattr(service, method_name)
            assert callable(getattr(service, method_name))
