"""
Comprehensive unit tests for CrewAIFlowService.

This module provides comprehensive test coverage for the CrewAIFlowService class,
which interfaces with the CrewAI Flow Runner for flow execution operations.
"""

import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException

from src.engines.crewai.crewai_flow_service import CrewAIFlowService


class TestCrewAIFlowServiceInitialization:
    """Tests for CrewAIFlowService initialization."""

    def test_initialization_with_session(self):
        """Test service initialization with a database session."""
        mock_session = MagicMock()
        service = CrewAIFlowService(session=mock_session)

        assert service.session == mock_session

    def test_initialization_without_session(self):
        """Test service initialization without a database session."""
        service = CrewAIFlowService()

        assert service.session is None


class TestGetFlowRunner:
    """Tests for _get_flow_runner method."""

    def test_get_flow_runner_with_session(self):
        """Test getting flow runner when session is available."""
        mock_session = MagicMock()
        service = CrewAIFlowService(session=mock_session)

        with patch('src.engines.crewai.crewai_flow_service.FlowRunnerService') as mock_flow_runner:
            flow_runner = service._get_flow_runner()
            mock_flow_runner.assert_called_once_with(mock_session)

    @patch('src.engines.crewai.crewai_flow_service.logger')
    def test_get_flow_runner_without_session_logs_warning(self, mock_logger):
        """Test getting flow runner without session logs warning."""
        service = CrewAIFlowService()

        with patch('src.engines.crewai.crewai_flow_service.FlowRunnerService') as mock_flow_runner:
            flow_runner = service._get_flow_runner()
            mock_flow_runner.assert_called_once_with(None)
            mock_logger.warning.assert_called()


class TestRunFlow:
    """Tests for run_flow method."""

    @pytest.fixture
    def service(self):
        """Create a service instance for testing."""
        mock_session = MagicMock()
        return CrewAIFlowService(session=mock_session)

    @pytest.fixture
    def mock_config(self):
        """Create a mock flow configuration."""
        return {
            'nodes': [
                {
                    'id': 'node1',
                    'type': 'crewnode',
                    'data': {
                        'label': 'Test Crew',
                        'allAgents': [
                            {'id': 'agent1', 'role': 'Researcher', 'goal': 'Research topics', 'backstory': 'Expert researcher'}
                        ],
                        'allTasks': [
                            {'id': 'task1', 'name': 'Research Task', 'description': 'Research the topic', 'expected_output': 'Report'}
                        ]
                    }
                }
            ],
            'edges': [{'source': 'start', 'target': 'node1'}],
            'flow_config': {'startingPoints': [], 'listeners': []},
            'inputs': {'topic': 'AI'}
        }

    @pytest.mark.asyncio
    async def test_run_flow_success(self, service, mock_config):
        """Test successful flow execution."""
        flow_id = uuid.uuid4()
        job_id = 'test-job-123'

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.run_flow(
                flow_id=flow_id,
                job_id=job_id,
                config=mock_config
            )

            assert result['success'] is True
            assert result['execution_id'] == 'exec-123'
            assert result['job_id'] == job_id

    @pytest.mark.asyncio
    async def test_run_flow_generates_job_id_if_not_provided(self, service, mock_config):
        """Test that job_id is generated if not provided."""
        flow_id = uuid.uuid4()

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.run_flow(
                flow_id=flow_id,
                config=mock_config
            )

            assert result['success'] is True
            assert result['job_id'] is not None
            # Should be a valid UUID
            uuid.UUID(result['job_id'])

    @pytest.mark.asyncio
    async def test_run_flow_with_resume_parameters(self, service, mock_config):
        """Test flow execution with checkpoint resume parameters."""
        flow_id = uuid.uuid4()
        job_id = 'test-job-123'
        resume_flow_uuid = 'resume-uuid-456'
        resume_execution_id = 42
        resume_crew_sequence = 2

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.run_flow(
                flow_id=flow_id,
                job_id=job_id,
                config=mock_config,
                resume_from_flow_uuid=resume_flow_uuid,
                resume_from_execution_id=resume_execution_id,
                resume_from_crew_sequence=resume_crew_sequence
            )

            assert result['success'] is True
            assert 'resumed from checkpoint' in result['message'].lower()
            assert result['resumed_from'] == resume_execution_id

    @pytest.mark.asyncio
    async def test_run_flow_with_group_context(self, service, mock_config):
        """Test flow execution with group context."""
        flow_id = uuid.uuid4()
        job_id = 'test-job-123'

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = 'group-123'

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.run_flow(
                flow_id=flow_id,
                job_id=job_id,
                config=mock_config,
                group_context=mock_group_context
            )

            assert result['success'] is True
            # Verify group_context was passed to engine
            call_kwargs = mock_engine.run_flow.call_args.kwargs
            assert call_kwargs['group_context'] == mock_group_context

    @pytest.mark.asyncio
    async def test_run_flow_with_user_token(self, service, mock_config):
        """Test flow execution with user token for OBO authentication."""
        flow_id = uuid.uuid4()
        job_id = 'test-job-123'
        user_token = 'test-token-xyz'

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.run_flow(
                flow_id=flow_id,
                job_id=job_id,
                config=mock_config,
                user_token=user_token
            )

            assert result['success'] is True
            # Verify user_token was passed to engine
            call_kwargs = mock_engine.run_flow.call_args.kwargs
            assert call_kwargs['user_token'] == user_token

    @pytest.mark.asyncio
    async def test_run_flow_generates_run_name(self, service, mock_config):
        """Test that run_name is generated from execution name service."""
        flow_id = uuid.uuid4()

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        mock_name_service = MagicMock()
        mock_name_service.generate_execution_name = AsyncMock(
            return_value=MagicMock(name='Generated Test Flow')
        )

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            with patch('src.services.execution_name_service.ExecutionNameService') as mock_name_class:
                mock_name_class.create.return_value = mock_name_service

                result = await service.run_flow(
                    flow_id=flow_id,
                    config=mock_config
                )

                assert result['success'] is True

    @pytest.mark.asyncio
    async def test_run_flow_engine_not_found_raises_error(self, service, mock_config):
        """Test that error is raised when engine cannot be obtained."""
        flow_id = uuid.uuid4()

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=None)

            with pytest.raises(HTTPException) as exc_info:
                await service.run_flow(
                    flow_id=flow_id,
                    config=mock_config
                )

            assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_run_flow_engine_error_raises_http_exception(self, service, mock_config):
        """Test that engine errors are converted to HTTP exceptions."""
        flow_id = uuid.uuid4()

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(side_effect=ValueError("Engine error"))

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            with pytest.raises(HTTPException) as exc_info:
                await service.run_flow(
                    flow_id=flow_id,
                    config=mock_config
                )

            assert exc_info.value.status_code == 500
            assert 'Engine error' in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_run_flow_with_string_flow_id(self, service, mock_config):
        """Test flow execution with string flow_id."""
        flow_id = str(uuid.uuid4())

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.run_flow(
                flow_id=flow_id,
                config=mock_config
            )

            assert result['success'] is True


class TestGetFlowExecution:
    """Tests for get_flow_execution method."""

    @pytest.fixture
    def service(self):
        """Create a service instance for testing."""
        mock_session = MagicMock()
        return CrewAIFlowService(session=mock_session)

    @pytest.mark.asyncio
    async def test_get_flow_execution_success(self, service):
        """Test successful retrieval of flow execution."""
        execution_id = 123
        expected_result = {
            'id': execution_id,
            'status': 'COMPLETED',
            'result': {'output': 'test result'}
        }

        mock_flow_runner = MagicMock()
        mock_flow_runner.get_flow_execution.return_value = expected_result

        with patch.object(service, '_get_flow_runner', return_value=mock_flow_runner):
            result = await service.get_flow_execution(execution_id)

            assert result == expected_result
            mock_flow_runner.get_flow_execution.assert_called_once_with(execution_id)

    @pytest.mark.asyncio
    async def test_get_flow_execution_error_raises_http_exception(self, service):
        """Test that errors are converted to HTTP exceptions."""
        execution_id = 123

        mock_flow_runner = MagicMock()
        mock_flow_runner.get_flow_execution.side_effect = ValueError("Not found")

        with patch.object(service, '_get_flow_runner', return_value=mock_flow_runner):
            with pytest.raises(HTTPException) as exc_info:
                await service.get_flow_execution(execution_id)

            assert exc_info.value.status_code == 500


class TestGetFlowExecutionsByFlow:
    """Tests for get_flow_executions_by_flow method."""

    @pytest.fixture
    def service(self):
        """Create a service instance for testing."""
        mock_session = MagicMock()
        return CrewAIFlowService(session=mock_session)

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_with_uuid(self, service):
        """Test retrieval of executions with UUID flow_id."""
        flow_id = uuid.uuid4()
        expected_result = {
            'executions': [
                {'id': 1, 'status': 'COMPLETED'},
                {'id': 2, 'status': 'RUNNING'}
            ]
        }

        mock_flow_runner = MagicMock()
        mock_flow_runner.get_flow_executions_by_flow.return_value = expected_result

        with patch.object(service, '_get_flow_runner', return_value=mock_flow_runner):
            result = await service.get_flow_executions_by_flow(flow_id)

            assert result == expected_result

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_with_valid_string(self, service):
        """Test retrieval of executions with valid string flow_id."""
        flow_id = str(uuid.uuid4())
        expected_result = {'executions': []}

        mock_flow_runner = MagicMock()
        mock_flow_runner.get_flow_executions_by_flow.return_value = expected_result

        with patch.object(service, '_get_flow_runner', return_value=mock_flow_runner):
            result = await service.get_flow_executions_by_flow(flow_id)

            assert result == expected_result

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_with_invalid_string(self, service):
        """Test that invalid string flow_id raises HTTP exception."""
        flow_id = 'not-a-valid-uuid'

        with pytest.raises(HTTPException) as exc_info:
            await service.get_flow_executions_by_flow(flow_id)

        assert exc_info.value.status_code == 400
        assert 'Invalid flow_id format' in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_flow_executions_by_flow_error_raises_http_exception(self, service):
        """Test that errors are converted to HTTP exceptions."""
        flow_id = uuid.uuid4()

        mock_flow_runner = MagicMock()
        mock_flow_runner.get_flow_executions_by_flow.side_effect = ValueError("Database error")

        with patch.object(service, '_get_flow_runner', return_value=mock_flow_runner):
            with pytest.raises(HTTPException) as exc_info:
                await service.get_flow_executions_by_flow(flow_id)

            assert exc_info.value.status_code == 500


class TestRunFlowIntegration:
    """Integration-style tests for run_flow method with full config processing."""

    @pytest.fixture
    def service(self):
        """Create a service instance for testing."""
        mock_session = MagicMock()
        return CrewAIFlowService(session=mock_session)

    @pytest.mark.asyncio
    async def test_run_flow_with_empty_config(self, service):
        """Test flow execution with empty config."""
        flow_id = uuid.uuid4()

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.run_flow(
                flow_id=flow_id,
                config={}
            )

            assert result['success'] is True

    @pytest.mark.asyncio
    async def test_run_flow_with_none_config(self, service):
        """Test flow execution with None config."""
        flow_id = uuid.uuid4()

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            result = await service.run_flow(
                flow_id=flow_id,
                config=None
            )

            assert result['success'] is True

    @pytest.mark.asyncio
    async def test_run_flow_logs_configuration(self, service):
        """Test that flow configuration is properly logged."""
        flow_id = uuid.uuid4()
        config = {
            'nodes': [{'id': 'node1'}],
            'edges': [{'source': 'start', 'target': 'node1'}],
            'flow_config': {'startingPoints': ['node1'], 'listeners': []}
        }

        mock_engine = MagicMock()
        mock_engine.run_flow = AsyncMock(return_value='exec-123')

        with patch('src.engines.engine_factory.EngineFactory') as mock_factory:
            mock_factory.get_engine = AsyncMock(return_value=mock_engine)

            with patch('src.engines.crewai.crewai_flow_service.logger') as mock_logger:
                result = await service.run_flow(
                    flow_id=flow_id,
                    config=config
                )

                # Verify logging was called
                assert mock_logger.info.called
