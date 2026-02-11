"""
Comprehensive unit tests for ExecutionTraceRepository.

This module provides comprehensive test coverage for the ExecutionTraceRepository class,
which handles CRUD operations for execution traces.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.execution_trace_repository import ExecutionTraceRepository
from src.models.execution_trace import ExecutionTrace
from src.models.execution_history import ExecutionHistory


class TestExecutionTraceRepositoryInitialization:
    """Tests for ExecutionTraceRepository initialization."""

    def test_initialization_with_session(self):
        """Test repository initialization with a database session."""
        mock_session = MagicMock()
        repo = ExecutionTraceRepository(mock_session)

        assert repo.session == mock_session


class TestCreateTrace:
    """Tests for create trace methods."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        session = AsyncMock()
        session.add = MagicMock()
        session.flush = AsyncMock()
        session.commit = AsyncMock()
        session.refresh = AsyncMock()
        session.rollback = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create a repository instance with mock session."""
        return ExecutionTraceRepository(mock_session)

    @pytest.fixture
    def trace_data(self):
        """Create sample trace data."""
        return {
            'job_id': 'test-job-123',
            'run_id': 1,
            'event_type': 'task_completed',
            'trace_metadata': {'agent_role': 'Researcher'},
            'output': {'result': 'test output'}
        }

    @pytest.mark.asyncio
    async def test_create_trace_success(self, repository, mock_session, trace_data):
        """Test successful trace creation."""
        # Mock the execution history check
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = MagicMock(id=1, job_id='test-job-123')
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.create(trace_data)

        mock_session.add.assert_called_once()
        mock_session.flush.assert_called()
        mock_session.flush.assert_called()

    @pytest.mark.asyncio
    async def test_create_trace_sets_run_id_from_job(self, repository, mock_session):
        """Test that run_id is set from existing job."""
        trace_data = {
            'job_id': 'test-job-123',
            'event_type': 'task_started',
            'trace_metadata': {}
        }

        mock_execution = MagicMock()
        mock_execution.id = 42
        mock_execution.job_id = 'test-job-123'

        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        await repository.create(trace_data)

        # Verify run_id was set
        assert trace_data.get('run_id') == 42

    @pytest.mark.asyncio
    async def test_create_trace_raises_error_for_nonexistent_job(self, repository, mock_session):
        """Test that error is raised when job doesn't exist."""
        trace_data = {
            'job_id': 'nonexistent-job',
            'event_type': 'task_started'
        }

        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(ValueError) as exc_info:
            await repository.create(trace_data)

        assert 'does not exist' in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_trace_handles_database_error(self, repository, mock_session, trace_data):
        """Test handling of database errors during creation."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = MagicMock(id=1)
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.flush = AsyncMock(side_effect=SQLAlchemyError("DB error"))

        with pytest.raises(SQLAlchemyError):
            await repository.create(trace_data)

        mock_session.rollback.assert_called()

    @pytest.mark.asyncio
    async def test_internal_create_without_job_check(self, repository, mock_session, trace_data):
        """Test _create method without job existence check."""
        # _create doesn't check for job existence
        await repository._create(trace_data)

        mock_session.add.assert_called_once()
        mock_session.flush.assert_called()


class TestGetTraceMethods:
    """Tests for trace retrieval methods."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        return AsyncMock()

    @pytest.fixture
    def repository(self, mock_session):
        """Create a repository instance with mock session."""
        return ExecutionTraceRepository(mock_session)

    @pytest.fixture
    def mock_traces(self):
        """Create mock trace objects."""
        traces = []
        for i in range(3):
            trace = MagicMock(spec=ExecutionTrace)
            trace.id = i + 1
            trace.job_id = f'job-{i + 1}'
            trace.run_id = i + 1
            trace.event_type = 'task_completed'
            trace.created_at = datetime.now()
            traces.append(trace)
        return traces

    @pytest.mark.asyncio
    async def test_get_by_id_found(self, repository, mock_session, mock_traces):
        """Test getting trace by ID when found."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_traces[0]
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_by_id(1)

        assert result == mock_traces[0]

    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, repository, mock_session):
        """Test getting trace by ID when not found."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_by_id(999)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_by_run_id(self, repository, mock_session, mock_traces):
        """Test getting traces by run_id."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_traces
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_by_run_id(1)

        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_get_by_run_id_with_pagination(self, repository, mock_session, mock_traces):
        """Test getting traces by run_id with pagination."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_traces[:2]
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_by_run_id(1, limit=2, offset=0)

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_by_job_id(self, repository, mock_session, mock_traces):
        """Test getting traces by job_id."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_traces
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_by_job_id('test-job-123')

        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_get_by_job_id_with_pagination(self, repository, mock_session, mock_traces):
        """Test getting traces by job_id with pagination."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_traces[:1]
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_by_job_id('test-job-123', limit=1, offset=0)

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_all_traces(self, repository, mock_session, mock_traces):
        """Test getting all traces with pagination."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_traces

        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 10

        mock_session.execute = AsyncMock(side_effect=[mock_result, mock_count_result])

        traces, total = await repository.get_all_traces(limit=3, offset=0)

        assert len(traces) == 3
        assert total == 10

    @pytest.mark.asyncio
    async def test_get_all_traces_empty(self, repository, mock_session):
        """Test getting all traces when none exist."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []

        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 0

        mock_session.execute = AsyncMock(side_effect=[mock_result, mock_count_result])

        traces, total = await repository.get_all_traces()

        assert traces == []
        assert total == 0


class TestGetExecutionIdMethods:
    """Tests for execution ID lookup methods."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        return AsyncMock()

    @pytest.fixture
    def repository(self, mock_session):
        """Create a repository instance with mock session."""
        return ExecutionTraceRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_execution_job_id_by_run_id(self, repository, mock_session):
        """Test getting job_id by run_id."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = 'test-job-123'
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_execution_job_id_by_run_id(1)

        assert result == 'test-job-123'

    @pytest.mark.asyncio
    async def test_get_execution_job_id_by_run_id_not_found(self, repository, mock_session):
        """Test getting job_id by run_id when not found."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_execution_job_id_by_run_id(999)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_execution_run_id_by_job_id(self, repository, mock_session):
        """Test getting run_id by job_id."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = 42
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_execution_run_id_by_job_id('test-job-123')

        assert result == 42

    @pytest.mark.asyncio
    async def test_get_execution_run_id_by_job_id_not_found(self, repository, mock_session):
        """Test getting run_id by job_id when not found."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_execution_run_id_by_job_id('nonexistent')

        assert result is None


class TestDeleteTraceMethods:
    """Tests for trace deletion methods."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        session = AsyncMock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create a repository instance with mock session."""
        return ExecutionTraceRepository(mock_session)

    @pytest.mark.asyncio
    async def test_delete_by_id_success(self, repository, mock_session):
        """Test deleting trace by ID."""
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.delete_by_id(1)

        assert result == 1
        mock_session.flush.assert_called()

    @pytest.mark.asyncio
    async def test_delete_by_id_not_found(self, repository, mock_session):
        """Test deleting trace by ID when not found."""
        mock_result = MagicMock()
        mock_result.rowcount = 0
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.delete_by_id(999)

        assert result == 0

    @pytest.mark.asyncio
    async def test_delete_by_run_id(self, repository, mock_session):
        """Test deleting traces by run_id."""
        mock_result = MagicMock()
        mock_result.rowcount = 5
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.delete_by_run_id(1)

        assert result == 5
        mock_session.flush.assert_called()

    @pytest.mark.asyncio
    async def test_delete_by_job_id(self, repository, mock_session):
        """Test deleting traces by job_id."""
        mock_result = MagicMock()
        mock_result.rowcount = 3
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.delete_by_job_id('test-job-123')

        assert result == 3
        mock_session.flush.assert_called()

    @pytest.mark.asyncio
    async def test_delete_all(self, repository, mock_session):
        """Test deleting all traces."""
        mock_result = MagicMock()
        mock_result.rowcount = 100
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.delete_all()

        assert result == 100
        mock_session.flush.assert_called()

    @pytest.mark.asyncio
    async def test_delete_handles_database_error(self, repository, mock_session):
        """Test handling of database errors during deletion."""
        mock_session.execute = AsyncMock(side_effect=SQLAlchemyError("DB error"))

        with pytest.raises(SQLAlchemyError):
            await repository.delete_by_id(1)

        mock_session.rollback.assert_called()


class TestGetCrewCheckpoints:
    """Tests for get_crew_checkpoints_by_job_id method."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        return AsyncMock()

    @pytest.fixture
    def repository(self, mock_session):
        """Create a repository instance with mock session."""
        return ExecutionTraceRepository(mock_session)

    @pytest.fixture
    def mock_checkpoint_traces(self):
        """Create mock traces for checkpoint extraction."""
        traces = []
        for i, crew_name in enumerate(['ResearchCrew', 'AnalysisCrew', 'WritingCrew']):
            trace = MagicMock(spec=ExecutionTrace)
            trace.id = i + 1
            trace.job_id = 'test-job-123'
            trace.event_type = 'task_completed'
            trace.created_at = datetime(2024, 1, 1, 10, i)
            trace.trace_metadata = {
                'crew_name': crew_name,
                'agent_role': f'Agent{i}'
            }
            trace.output = {
                'output_content': f'Output from {crew_name}'
            }
            traces.append(trace)
        return traces

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_extracts_crew_info(self, repository, mock_session, mock_checkpoint_traces):
        """Test extraction of crew checkpoint information from traces."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_checkpoint_traces
        mock_session.execute = AsyncMock(return_value=mock_result)

        checkpoints = await repository.get_crew_checkpoints_by_job_id('test-job-123')

        assert len(checkpoints) == 3
        assert checkpoints[0]['crew_name'] == 'ResearchCrew'
        assert checkpoints[0]['sequence'] == 1
        assert checkpoints[0]['status'] == 'completed'

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_orders_by_sequence(self, repository, mock_session, mock_checkpoint_traces):
        """Test that checkpoints are ordered by sequence."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_checkpoint_traces
        mock_session.execute = AsyncMock(return_value=mock_result)

        checkpoints = await repository.get_crew_checkpoints_by_job_id('test-job-123')

        sequences = [cp['sequence'] for cp in checkpoints]
        assert sequences == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_deduplicates_crews(self, repository, mock_session):
        """Test that duplicate crew completions are filtered out."""
        # Create traces with duplicate crew names
        traces = []
        for i in range(4):
            trace = MagicMock(spec=ExecutionTrace)
            trace.id = i + 1
            trace.job_id = 'test-job-123'
            trace.event_type = 'task_completed'
            trace.created_at = datetime(2024, 1, 1, 10, i)
            # Two traces for same crew
            trace.trace_metadata = {
                'crew_name': 'ResearchCrew' if i < 2 else 'AnalysisCrew'
            }
            trace.output = {'output_content': f'Output {i}'}
            traces.append(trace)

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = traces
        mock_session.execute = AsyncMock(return_value=mock_result)

        checkpoints = await repository.get_crew_checkpoints_by_job_id('test-job-123')

        # Should only have 2 unique crews
        assert len(checkpoints) == 2
        crew_names = [cp['crew_name'] for cp in checkpoints]
        assert 'ResearchCrew' in crew_names
        assert 'AnalysisCrew' in crew_names

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_handles_missing_crew_name(self, repository, mock_session):
        """Test handling of traces without crew_name."""
        trace = MagicMock(spec=ExecutionTrace)
        trace.id = 1
        trace.job_id = 'test-job-123'
        trace.event_type = 'task_completed'
        trace.created_at = datetime.now()
        trace.trace_metadata = {}  # No crew_name
        trace.output = {}

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [trace]
        mock_session.execute = AsyncMock(return_value=mock_result)

        checkpoints = await repository.get_crew_checkpoints_by_job_id('test-job-123')

        # Should return empty list since no crew_name found
        assert checkpoints == []

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_extracts_output_preview(self, repository, mock_session, mock_checkpoint_traces):
        """Test that output preview is extracted."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_checkpoint_traces
        mock_session.execute = AsyncMock(return_value=mock_result)

        checkpoints = await repository.get_crew_checkpoints_by_job_id('test-job-123')

        for cp in checkpoints:
            assert 'output_preview' in cp
            assert cp['output_preview'] != ''

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_includes_timestamp(self, repository, mock_session, mock_checkpoint_traces):
        """Test that completed_at timestamp is included."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_checkpoint_traces
        mock_session.execute = AsyncMock(return_value=mock_result)

        checkpoints = await repository.get_crew_checkpoints_by_job_id('test-job-123')

        for cp in checkpoints:
            assert 'completed_at' in cp
            assert cp['completed_at'] is not None

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_handles_database_error(self, repository, mock_session):
        """Test handling of database errors."""
        mock_session.execute = AsyncMock(side_effect=SQLAlchemyError("DB error"))

        checkpoints = await repository.get_crew_checkpoints_by_job_id('test-job-123')

        # Should return empty list on error
        assert checkpoints == []

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_fallback_to_agent_role(self, repository, mock_session):
        """Test fallback to agent_role when crew_name is not available."""
        trace = MagicMock(spec=ExecutionTrace)
        trace.id = 1
        trace.job_id = 'test-job-123'
        trace.event_type = 'task_completed'
        trace.created_at = datetime.now()
        trace.trace_metadata = {
            'agent_role': 'ResearchAgent'  # No crew_name, but has agent_role
        }
        trace.output = {'output_content': 'Test output'}

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [trace]
        mock_session.execute = AsyncMock(return_value=mock_result)

        checkpoints = await repository.get_crew_checkpoints_by_job_id('test-job-123')

        assert len(checkpoints) == 1
        assert checkpoints[0]['crew_name'] == 'ResearchAgent'


class TestGetCrewOutputsForResume:
    """Tests for get_crew_outputs_for_resume method."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        return AsyncMock()

    @pytest.fixture
    def repository(self, mock_session):
        """Create a repository instance with mock session."""
        return ExecutionTraceRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_crew_outputs_returns_mapping(self, repository, mock_session):
        """Test that crew outputs are returned as a mapping."""
        traces = []
        for i, crew_name in enumerate(['ResearchCrew', 'AnalysisCrew']):
            trace = MagicMock(spec=ExecutionTrace)
            trace.id = i + 1
            trace.job_id = 'test-job-123'
            trace.event_type = 'task_completed'
            trace.created_at = datetime(2024, 1, 1, 10, i)
            trace.trace_metadata = {'crew_name': crew_name}
            trace.output = {
                'output_content': f'Full output from {crew_name}'
            }
            traces.append(trace)

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = traces
        mock_session.execute = AsyncMock(return_value=mock_result)

        outputs = await repository.get_crew_outputs_for_resume('test-job-123')

        assert 'ResearchCrew' in outputs
        assert 'AnalysisCrew' in outputs
        assert 'Full output from ResearchCrew' in str(outputs['ResearchCrew'])

    @pytest.mark.asyncio
    async def test_get_crew_outputs_keeps_last_output_per_crew(self, repository, mock_session):
        """Test that the last output is kept for each crew."""
        # Create multiple traces for same crew
        traces = []
        for i in range(3):
            trace = MagicMock(spec=ExecutionTrace)
            trace.id = i + 1
            trace.job_id = 'test-job-123'
            trace.event_type = 'task_completed'
            trace.created_at = datetime(2024, 1, 1, 10, i)
            trace.trace_metadata = {'crew_name': 'ResearchCrew'}
            trace.output = {'output_content': f'Output version {i + 1}'}
            traces.append(trace)

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = traces
        mock_session.execute = AsyncMock(return_value=mock_result)

        outputs = await repository.get_crew_outputs_for_resume('test-job-123')

        # Should have the last output (version 3)
        assert 'Output version 3' in str(outputs.get('ResearchCrew', ''))

    @pytest.mark.asyncio
    async def test_get_crew_outputs_handles_empty_results(self, repository, mock_session):
        """Test handling when no traces found."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=mock_result)

        outputs = await repository.get_crew_outputs_for_resume('test-job-123')

        assert outputs == {}

    @pytest.mark.asyncio
    async def test_get_crew_outputs_handles_database_error(self, repository, mock_session):
        """Test handling of database errors."""
        mock_session.execute = AsyncMock(side_effect=SQLAlchemyError("DB error"))

        outputs = await repository.get_crew_outputs_for_resume('test-job-123')

        # Should return empty dict on error
        assert outputs == {}


class TestDatabaseErrorHandling:
    """Tests for database error handling across methods."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        session = AsyncMock()
        session.rollback = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create a repository instance with mock session."""
        return ExecutionTraceRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_by_id_handles_error(self, repository, mock_session):
        """Test get_by_id handles database errors."""
        mock_session.execute = AsyncMock(side_effect=SQLAlchemyError("DB error"))

        with pytest.raises(SQLAlchemyError):
            await repository.get_by_id(1)

    @pytest.mark.asyncio
    async def test_get_by_run_id_handles_error(self, repository, mock_session):
        """Test get_by_run_id handles database errors."""
        mock_session.execute = AsyncMock(side_effect=SQLAlchemyError("DB error"))

        with pytest.raises(SQLAlchemyError):
            await repository.get_by_run_id(1)

    @pytest.mark.asyncio
    async def test_get_by_job_id_handles_error(self, repository, mock_session):
        """Test get_by_job_id handles database errors."""
        mock_session.execute = AsyncMock(side_effect=SQLAlchemyError("DB error"))

        with pytest.raises(SQLAlchemyError):
            await repository.get_by_job_id('test-job')

    @pytest.mark.asyncio
    async def test_get_all_traces_handles_error(self, repository, mock_session):
        """Test get_all_traces handles database errors."""
        mock_session.execute = AsyncMock(side_effect=SQLAlchemyError("DB error"))

        with pytest.raises(SQLAlchemyError):
            await repository.get_all_traces()
