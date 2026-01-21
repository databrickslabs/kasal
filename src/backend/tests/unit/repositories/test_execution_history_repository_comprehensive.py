"""
Comprehensive unit tests for ExecutionHistoryRepository.

Tests cover:
- CRUD operations for execution history
- Group-based filtering (multi-tenant isolation)
- Pagination support
- Checkpoint management
- MLflow integration
- Error handling and edge cases
"""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.models.execution_history import ExecutionHistory, TaskStatus, ErrorTrace


class TestExecutionHistoryRepositoryInit:
    """Tests for repository initialization."""

    def test_init_with_session(self):
        """Test initialization with session."""
        mock_session = MagicMock(spec=AsyncSession)
        repo = ExecutionHistoryRepository(mock_session)
        assert repo.session == mock_session

    def test_init_stores_session(self):
        """Test that session is stored correctly."""
        mock_session = MagicMock(spec=AsyncSession)
        repo = ExecutionHistoryRepository(mock_session)
        assert repo.session is mock_session


class TestGetExecutionHistory:
    """Tests for get_execution_history method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_execution_history_with_group_filtering(self, repository, mock_session):
        """Test getting execution history with group filtering."""
        mock_executions = [
            MagicMock(id=1, job_id='job-1', group_id='group-1'),
            MagicMock(id=2, job_id='job-2', group_id='group-1')
        ]

        # Mock count result
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 2

        # Mock executions result
        mock_exec_result = MagicMock()
        mock_exec_result.scalars.return_value.all.return_value = mock_executions

        mock_session.execute = AsyncMock(side_effect=[mock_count_result, mock_exec_result])

        runs, total = await repository.get_execution_history(
            limit=50,
            offset=0,
            group_ids=['group-1']
        )

        assert len(runs) == 2
        assert total == 2
        assert mock_session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_get_execution_history_without_group_filtering(self, repository, mock_session):
        """Test getting execution history without group filtering (admin access)."""
        mock_executions = [
            MagicMock(id=1, job_id='job-1'),
            MagicMock(id=2, job_id='job-2'),
            MagicMock(id=3, job_id='job-3')
        ]

        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 3

        mock_exec_result = MagicMock()
        mock_exec_result.scalars.return_value.all.return_value = mock_executions

        mock_session.execute = AsyncMock(side_effect=[mock_count_result, mock_exec_result])

        runs, total = await repository.get_execution_history(limit=50, offset=0)

        assert len(runs) == 3
        assert total == 3

    @pytest.mark.asyncio
    async def test_get_execution_history_pagination(self, repository, mock_session):
        """Test pagination in get_execution_history."""
        mock_executions = [MagicMock(id=3, job_id='job-3')]

        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 10

        mock_exec_result = MagicMock()
        mock_exec_result.scalars.return_value.all.return_value = mock_executions

        mock_session.execute = AsyncMock(side_effect=[mock_count_result, mock_exec_result])

        runs, total = await repository.get_execution_history(limit=1, offset=2)

        assert len(runs) == 1
        assert total == 10

    @pytest.mark.asyncio
    async def test_get_execution_history_no_session_raises(self):
        """Test that missing session raises RuntimeError."""
        repo = ExecutionHistoryRepository(None)

        with pytest.raises(RuntimeError, match="requires a session"):
            await repo.get_execution_history()

    @pytest.mark.asyncio
    async def test_get_execution_history_empty_group_ids(self, repository, mock_session):
        """Test with empty group_ids list (should not filter)."""
        mock_executions = [MagicMock(id=1)]

        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 1

        mock_exec_result = MagicMock()
        mock_exec_result.scalars.return_value.all.return_value = mock_executions

        mock_session.execute = AsyncMock(side_effect=[mock_count_result, mock_exec_result])

        runs, total = await repository.get_execution_history(group_ids=[])

        assert len(runs) == 1


class TestGetExecutionById:
    """Tests for get_execution_by_id method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_execution_by_id_found(self, repository, mock_session):
        """Test getting execution by ID when found."""
        mock_execution = MagicMock(id=1, job_id='job-1')
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_execution_by_id(1)

        assert result == mock_execution
        assert result.id == 1

    @pytest.mark.asyncio
    async def test_get_execution_by_id_not_found(self, repository, mock_session):
        """Test getting execution by ID when not found."""
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_execution_by_id(999)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_execution_by_id_with_group_filter(self, repository, mock_session):
        """Test getting execution by ID with group filtering."""
        mock_execution = MagicMock(id=1, job_id='job-1', group_id='group-1')
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_execution_by_id(1, group_ids=['group-1'])

        assert result == mock_execution

    @pytest.mark.asyncio
    async def test_get_execution_by_id_no_session(self):
        """Test that missing session raises RuntimeError."""
        repo = ExecutionHistoryRepository(None)

        with pytest.raises(RuntimeError, match="requires a session"):
            await repo.get_execution_by_id(1)


class TestGetExecutionByJobId:
    """Tests for get_execution_by_job_id method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_execution_by_job_id_found(self, repository, mock_session):
        """Test getting execution by job_id when found."""
        mock_execution = MagicMock(id=1, job_id='job-123')
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_execution_by_job_id('job-123')

        assert result == mock_execution
        assert result.job_id == 'job-123'

    @pytest.mark.asyncio
    async def test_get_execution_by_job_id_with_group_filter(self, repository, mock_session):
        """Test getting execution by job_id with group filtering."""
        mock_execution = MagicMock(id=1, job_id='job-123', group_id='group-1')
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_execution_by_job_id('job-123', group_ids=['group-1'])

        assert result == mock_execution


class TestFindById:
    """Tests for find_by_id method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_find_by_id_found(self, repository, mock_session):
        """Test finding execution by ID."""
        mock_execution = MagicMock(id=1)
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.find_by_id(1)

        assert result == mock_execution


class TestCheckExecutionExists:
    """Tests for check_execution_exists method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_check_execution_exists_true(self, repository, mock_session):
        """Test checking when execution exists."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = 1
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.check_execution_exists(1)

        assert result is True

    @pytest.mark.asyncio
    async def test_check_execution_exists_false(self, repository, mock_session):
        """Test checking when execution does not exist."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = 0
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.check_execution_exists(999)

        assert result is False


class TestDeleteExecution:
    """Tests for delete_execution method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        session.flush = AsyncMock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_delete_execution_success(self, repository, mock_session):
        """Test successful execution deletion."""
        mock_execution = MagicMock(id=1, job_id='job-123')
        mock_find_result = MagicMock()
        mock_find_result.scalars.return_value.first.return_value = mock_execution

        mock_task_delete_result = MagicMock(rowcount=2)
        mock_error_delete_result = MagicMock(rowcount=1)
        mock_run_delete_result = MagicMock(rowcount=1)

        mock_session.execute = AsyncMock(side_effect=[
            mock_find_result,
            mock_task_delete_result,
            mock_error_delete_result,
            mock_run_delete_result
        ])

        result = await repository.delete_execution(1)

        assert result is not None
        assert result['execution_id'] == 1
        assert result['job_id'] == 'job-123'
        assert result['task_status_count'] == 2
        assert result['error_trace_count'] == 1
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_execution_not_found(self, repository, mock_session):
        """Test deletion when execution not found."""
        mock_find_result = MagicMock()
        mock_find_result.scalars.return_value.first.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_find_result)

        result = await repository.delete_execution(999)

        assert result is None


class TestDeleteExecutionByJobId:
    """Tests for delete_execution_by_job_id method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        session.flush = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_delete_by_job_id_success(self, repository, mock_session):
        """Test successful deletion by job_id."""
        mock_execution = MagicMock(id=1, job_id='job-123')
        mock_find_result = MagicMock()
        mock_find_result.scalars.return_value.first.return_value = mock_execution

        mock_task_delete_result = MagicMock(rowcount=2)
        mock_error_delete_result = MagicMock(rowcount=0)
        mock_run_delete_result = MagicMock(rowcount=1)

        mock_session.execute = AsyncMock(side_effect=[
            mock_find_result,
            mock_task_delete_result,
            mock_error_delete_result,
            mock_run_delete_result
        ])

        result = await repository.delete_execution_by_job_id('job-123')

        assert result is not None
        assert result['job_id'] == 'job-123'


class TestDeleteAllExecutions:
    """Tests for delete_all_executions method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        session.flush = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_delete_all_with_group_filter(self, repository, mock_session):
        """Test deleting all executions for specific groups."""
        # Mock finding executions for the group
        mock_exec_result = MagicMock()
        mock_exec_result.fetchall.return_value = [(1, 'job-1'), (2, 'job-2')]

        mock_task_delete_result = MagicMock(rowcount=5)
        mock_error_delete_result = MagicMock(rowcount=2)
        mock_run_delete_result = MagicMock(rowcount=2)

        mock_session.execute = AsyncMock(side_effect=[
            mock_exec_result,
            mock_task_delete_result,
            mock_error_delete_result,
            mock_run_delete_result
        ])

        result = await repository.delete_all_executions(group_ids=['group-1'])

        assert result['run_count'] == 2
        assert result['task_status_count'] == 5
        assert result['error_trace_count'] == 2

    @pytest.mark.asyncio
    async def test_delete_all_without_group_filter(self, repository, mock_session):
        """Test deleting all executions (admin access)."""
        mock_task_delete_result = MagicMock(rowcount=10)
        mock_error_delete_result = MagicMock(rowcount=5)
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 5
        mock_run_delete_result = MagicMock(rowcount=5)

        mock_session.execute = AsyncMock(side_effect=[
            mock_task_delete_result,
            mock_error_delete_result,
            mock_count_result,
            mock_run_delete_result
        ])

        result = await repository.delete_all_executions()

        assert result['run_count'] == 5
        assert result['task_status_count'] == 10

    @pytest.mark.asyncio
    async def test_delete_all_empty_result(self, repository, mock_session):
        """Test deleting when no executions exist for group."""
        mock_exec_result = MagicMock()
        mock_exec_result.fetchall.return_value = []

        mock_session.execute = AsyncMock(return_value=mock_exec_result)

        result = await repository.delete_all_executions(group_ids=['empty-group'])

        assert result['run_count'] == 0
        assert result['task_status_count'] == 0
        assert result['error_trace_count'] == 0


class TestUpdateMlflowEvaluationRunId:
    """Tests for update_mlflow_evaluation_run_id method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        session.flush = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_update_mlflow_run_id_success(self, repository, mock_session):
        """Test successful MLflow run ID update."""
        mock_execution = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.update_mlflow_evaluation_run_id('job-123', 'mlflow-run-456')

        assert result is True
        assert mock_execution.mlflow_evaluation_run_id == 'mlflow-run-456'
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_mlflow_run_id_not_found(self, repository, mock_session):
        """Test update when execution not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.update_mlflow_evaluation_run_id('non-existent', 'mlflow-run')

        assert result is False

    @pytest.mark.asyncio
    async def test_update_mlflow_run_id_error(self, repository, mock_session):
        """Test error handling during update."""
        mock_session.execute = AsyncMock(side_effect=Exception("Database error"))

        result = await repository.update_mlflow_evaluation_run_id('job-123', 'mlflow-run')

        assert result is False


class TestGetCheckpointsForFlow:
    """Tests for get_checkpoints_for_flow method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_checkpoints_success(self, repository, mock_session):
        """Test getting checkpoints for a flow."""
        mock_checkpoints = [
            MagicMock(id=1, flow_uuid='uuid-1', checkpoint_status='active'),
            MagicMock(id=2, flow_uuid='uuid-2', checkpoint_status='active')
        ]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_checkpoints
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_checkpoints_for_flow('flow-123')

        assert len(result) == 2
        assert result[0].flow_uuid == 'uuid-1'

    @pytest.mark.asyncio
    async def test_get_checkpoints_with_group_filter(self, repository, mock_session):
        """Test getting checkpoints with group filtering."""
        mock_checkpoints = [MagicMock(id=1, flow_uuid='uuid-1')]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_checkpoints
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_checkpoints_for_flow('flow-123', group_id='group-1')

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_checkpoints_with_status_filter(self, repository, mock_session):
        """Test getting checkpoints with status filtering."""
        mock_checkpoints = [MagicMock(id=1, checkpoint_status='resumed')]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_checkpoints
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_checkpoints_for_flow(
            'flow-123',
            status_filter='resumed'
        )

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_checkpoints_no_filter(self, repository, mock_session):
        """Test getting all checkpoints without status filter."""
        mock_checkpoints = [
            MagicMock(checkpoint_status='active'),
            MagicMock(checkpoint_status='resumed'),
            MagicMock(checkpoint_status='expired')
        ]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_checkpoints
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_checkpoints_for_flow('flow-123', status_filter=None)

        assert len(result) == 3


class TestUpdateCheckpointStatus:
    """Tests for update_checkpoint_status method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        session.flush = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_update_checkpoint_status_success(self, repository, mock_session):
        """Test successful checkpoint status update."""
        mock_execution = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.update_checkpoint_status(1, 'resumed')

        assert result is True
        assert mock_execution.checkpoint_status == 'resumed'
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_checkpoint_status_with_group(self, repository, mock_session):
        """Test checkpoint status update with group filtering."""
        mock_execution = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.update_checkpoint_status(1, 'expired', group_id='group-1')

        assert result is True

    @pytest.mark.asyncio
    async def test_update_checkpoint_status_not_found(self, repository, mock_session):
        """Test update when execution not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.update_checkpoint_status(999, 'resumed')

        assert result is False


class TestSetCheckpointInfo:
    """Tests for set_checkpoint_info method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        session.flush = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_set_checkpoint_info_success(self, repository, mock_session):
        """Test successful checkpoint info setting."""
        mock_execution = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.set_checkpoint_info(
            execution_id=1,
            flow_uuid='flow-uuid-123',
            checkpoint_status='active',
            checkpoint_method='method_name'
        )

        assert result is True
        assert mock_execution.flow_uuid == 'flow-uuid-123'
        assert mock_execution.checkpoint_status == 'active'
        assert mock_execution.checkpoint_method == 'method_name'

    @pytest.mark.asyncio
    async def test_set_checkpoint_info_not_found(self, repository, mock_session):
        """Test setting checkpoint info when execution not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.set_checkpoint_info(999, 'flow-uuid')

        assert result is False


class TestAddCrewCheckpoint:
    """Tests for add_crew_checkpoint method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        session.flush = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_add_crew_checkpoint_new(self, repository, mock_session):
        """Test adding first crew checkpoint."""
        mock_execution = MagicMock()
        mock_execution.checkpoint_data = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.add_crew_checkpoint(
            job_id='job-123',
            crew_node_id='node-1',
            crew_name='Research Crew',
            sequence=1,
            status='completed',
            output_preview='First 500 chars...',
            completed_at='2024-01-01T12:00:00Z'
        )

        assert result is True
        assert mock_execution.checkpoint_data is not None
        assert len(mock_execution.checkpoint_data['crew_checkpoints']) == 1

    @pytest.mark.asyncio
    async def test_add_crew_checkpoint_existing(self, repository, mock_session):
        """Test adding additional crew checkpoint."""
        existing_checkpoint = {
            'crew_checkpoints': [
                {'crew_node_id': 'node-1', 'sequence': 1}
            ]
        }
        mock_execution = MagicMock()
        mock_execution.checkpoint_data = existing_checkpoint
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.add_crew_checkpoint(
            job_id='job-123',
            crew_node_id='node-2',
            crew_name='Analysis Crew',
            sequence=2,
            status='completed',
            output_preview='Output...',
            completed_at='2024-01-01T12:01:00Z'
        )

        assert result is True
        assert len(mock_execution.checkpoint_data['crew_checkpoints']) == 2

    @pytest.mark.asyncio
    async def test_add_crew_checkpoint_not_found(self, repository, mock_session):
        """Test adding checkpoint when execution not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.add_crew_checkpoint(
            job_id='non-existent',
            crew_node_id='node-1',
            crew_name='Crew',
            sequence=1,
            status='completed',
            output_preview='',
            completed_at='2024-01-01T12:00:00Z'
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_add_crew_checkpoint_truncates_output(self, repository, mock_session):
        """Test that output preview is truncated to 500 chars."""
        mock_execution = MagicMock()
        mock_execution.checkpoint_data = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        long_output = 'x' * 1000

        await repository.add_crew_checkpoint(
            job_id='job-123',
            crew_node_id='node-1',
            crew_name='Crew',
            sequence=1,
            status='completed',
            output_preview=long_output,
            completed_at='2024-01-01T12:00:00Z'
        )

        # Verify output was truncated
        checkpoint = mock_execution.checkpoint_data['crew_checkpoints'][0]
        assert len(checkpoint['output_preview']) == 500


class TestGetCrewCheckpoints:
    """Tests for get_crew_checkpoints method."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_success(self, repository, mock_session):
        """Test getting crew checkpoints."""
        checkpoints = [
            {'crew_node_id': 'node-1', 'sequence': 1},
            {'crew_node_id': 'node-2', 'sequence': 2}
        ]
        mock_execution = MagicMock()
        mock_execution.checkpoint_data = {'crew_checkpoints': checkpoints}
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_crew_checkpoints('job-123')

        assert len(result) == 2
        assert result[0]['crew_node_id'] == 'node-1'

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_empty(self, repository, mock_session):
        """Test getting checkpoints when none exist."""
        mock_execution = MagicMock()
        mock_execution.checkpoint_data = {}
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_crew_checkpoints('job-123')

        assert result == []

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_not_found(self, repository, mock_session):
        """Test getting checkpoints when execution not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_crew_checkpoints('non-existent')

        assert result == []

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_no_checkpoint_data(self, repository, mock_session):
        """Test getting checkpoints when checkpoint_data is None."""
        mock_execution = MagicMock()
        mock_execution.checkpoint_data = None
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await repository.get_crew_checkpoints('job-123')

        assert result == []


class TestErrorHandling:
    """Tests for error handling across repository methods."""

    @pytest.fixture
    def mock_session(self):
        """Create mock async session."""
        session = MagicMock(spec=AsyncSession)
        session.execute = AsyncMock()
        session.rollback = AsyncMock()
        return session

    @pytest.fixture
    def repository(self, mock_session):
        """Create repository with mock session."""
        return ExecutionHistoryRepository(mock_session)

    @pytest.mark.asyncio
    async def test_delete_execution_rollback_on_error(self, mock_session):
        """Test rollback on deletion error with commit=True."""
        # Create mock that will fail
        mock_find_result = MagicMock()
        mock_find_result.scalars.return_value.first.return_value = MagicMock(id=1, job_id='job-1')
        mock_session.execute = AsyncMock(side_effect=[mock_find_result, Exception("DB Error")])

        repo = ExecutionHistoryRepository(mock_session)

        with pytest.raises(Exception, match="DB Error"):
            await repo._delete_execution_with_session(mock_session, 1, commit=True)

        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_checkpoints_error_handling(self, repository, mock_session):
        """Test error handling in get_checkpoints_for_flow."""
        mock_session.execute = AsyncMock(side_effect=Exception("Query failed"))

        with pytest.raises(Exception):
            await repository.get_checkpoints_for_flow('flow-123')

    @pytest.mark.asyncio
    async def test_update_checkpoint_status_error_handling(self, repository, mock_session):
        """Test error handling in update_checkpoint_status."""
        mock_session.execute = AsyncMock(side_effect=Exception("Update failed"))

        result = await repository.update_checkpoint_status(1, 'resumed')

        assert result is False

    @pytest.mark.asyncio
    async def test_set_checkpoint_info_error_handling(self, repository, mock_session):
        """Test error handling in set_checkpoint_info."""
        mock_session.execute = AsyncMock(side_effect=Exception("DB error"))

        result = await repository.set_checkpoint_info(1, 'uuid')

        assert result is False

    @pytest.mark.asyncio
    async def test_add_crew_checkpoint_error_handling(self, repository, mock_session):
        """Test error handling in add_crew_checkpoint."""
        mock_session.execute = AsyncMock(side_effect=Exception("Insert failed"))

        result = await repository.add_crew_checkpoint(
            'job-123', 'node-1', 'Crew', 1, 'completed', '', '2024-01-01'
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_get_crew_checkpoints_error_handling(self, repository, mock_session):
        """Test error handling in get_crew_checkpoints."""
        mock_session.execute = AsyncMock(side_effect=Exception("Query failed"))

        result = await repository.get_crew_checkpoints('job-123')

        assert result == []
