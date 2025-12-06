"""
Comprehensive unit tests for execution_history_service.py module.
Target: 80%+ coverage
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError
import uuid


class TestExecutionHistoryService:
    """Tests for ExecutionHistoryService class."""

    @pytest.fixture
    def mock_session(self):
        """Create mock database session."""
        session = AsyncMock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        return session

    @pytest.fixture
    def mock_history_repo(self):
        """Create mock history repository."""
        repo = AsyncMock()
        return repo

    @pytest.fixture
    def mock_logs_repo(self):
        """Create mock logs repository."""
        repo = AsyncMock()
        return repo

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        """Create service instance with mocks."""
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )


class TestGetExecutionHistory:
    """Tests for get_execution_history method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_get_execution_history_success(self, service, mock_history_repo):
        """Test successful retrieval of execution history."""
        mock_run = MagicMock()
        mock_run.id = 1
        mock_run.job_id = str(uuid.uuid4())
        mock_run.status = "completed"
        mock_run.result = {"content": "Test result"}
        mock_run.inputs = {}
        mock_run.created_at = "2024-01-01T00:00:00"

        mock_history_repo.get_execution_history = AsyncMock(return_value=([mock_run], 1))

        result = await service.get_execution_history(limit=50, offset=0)

        assert result.total == 1
        assert result.limit == 50
        assert result.offset == 0
        assert len(result.executions) == 1

    @pytest.mark.asyncio
    async def test_get_execution_history_with_group_ids(self, service, mock_history_repo):
        """Test retrieval of execution history with group filtering."""
        group_ids = [str(uuid.uuid4())]
        mock_history_repo.get_execution_history = AsyncMock(return_value=([], 0))

        result = await service.get_execution_history(limit=50, offset=0, group_ids=group_ids)

        mock_history_repo.get_execution_history.assert_called_once_with(
            limit=50, offset=0, group_ids=group_ids
        )
        assert result.total == 0

    @pytest.mark.asyncio
    async def test_get_execution_history_string_result(self, service, mock_history_repo):
        """Test handling of string result field."""
        mock_run = MagicMock()
        mock_run.id = 1
        mock_run.job_id = str(uuid.uuid4())
        mock_run.status = "completed"
        mock_run.result = "String result"  # String result
        mock_run.inputs = {}
        mock_run.created_at = "2024-01-01T00:00:00"

        mock_history_repo.get_execution_history = AsyncMock(return_value=([mock_run], 1))

        result = await service.get_execution_history()

        assert result.total == 1

    @pytest.mark.asyncio
    async def test_get_execution_history_with_yaml_inputs(self, service, mock_history_repo):
        """Test handling of agents_yaml and tasks_yaml in inputs."""
        mock_run = MagicMock()
        mock_run.id = 1
        mock_run.job_id = str(uuid.uuid4())
        mock_run.status = "completed"
        mock_run.result = {"content": "Test"}
        mock_run.inputs = {
            'agents_yaml': {'agent1': {'role': 'test'}},
            'tasks_yaml': {'task1': {'description': 'test'}}
        }
        mock_run.created_at = "2024-01-01T00:00:00"

        mock_history_repo.get_execution_history = AsyncMock(return_value=([mock_run], 1))

        result = await service.get_execution_history()

        assert result.total == 1

    @pytest.mark.asyncio
    async def test_get_execution_history_sqlalchemy_error(self, service, mock_history_repo):
        """Test handling of SQLAlchemy errors."""
        mock_history_repo.get_execution_history = AsyncMock(
            side_effect=SQLAlchemyError("Database error")
        )

        with pytest.raises(SQLAlchemyError):
            await service.get_execution_history()

    @pytest.mark.asyncio
    async def test_get_execution_history_general_error(self, service, mock_history_repo):
        """Test handling of general errors."""
        mock_history_repo.get_execution_history = AsyncMock(
            side_effect=Exception("General error")
        )

        with pytest.raises(Exception):
            await service.get_execution_history()


class TestGetExecutionById:
    """Tests for get_execution_by_id method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_get_execution_by_id_success(self, service, mock_history_repo):
        """Test successful retrieval of execution by ID."""
        mock_run = MagicMock()
        mock_run.id = 1
        mock_run.job_id = str(uuid.uuid4())
        mock_run.status = "completed"
        mock_run.result = {"content": "Test"}
        mock_run.inputs = {}
        mock_run.created_at = "2024-01-01T00:00:00"

        mock_history_repo.get_execution_by_id = AsyncMock(return_value=mock_run)

        result = await service.get_execution_by_id(1)

        assert result is not None

    @pytest.mark.asyncio
    async def test_get_execution_by_id_not_found(self, service, mock_history_repo):
        """Test retrieval of non-existent execution."""
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=None)

        result = await service.get_execution_by_id(999)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_execution_by_id_with_tenant_ids(self, service, mock_history_repo):
        """Test retrieval with tenant filtering."""
        tenant_ids = [str(uuid.uuid4())]
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=None)

        result = await service.get_execution_by_id(1, tenant_ids=tenant_ids)

        mock_history_repo.get_execution_by_id.assert_called_once_with(1, tenant_ids=tenant_ids)

    @pytest.mark.asyncio
    async def test_get_execution_by_id_sqlalchemy_error(self, service, mock_history_repo):
        """Test handling of SQLAlchemy errors."""
        mock_history_repo.get_execution_by_id = AsyncMock(
            side_effect=SQLAlchemyError("Database error")
        )

        with pytest.raises(SQLAlchemyError):
            await service.get_execution_by_id(1)


class TestCheckExecutionExists:
    """Tests for check_execution_exists method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_check_execution_exists_true(self, service, mock_history_repo):
        """Test check when execution exists."""
        mock_history_repo.check_execution_exists = AsyncMock(return_value=True)

        result = await service.check_execution_exists(1)

        assert result is True

    @pytest.mark.asyncio
    async def test_check_execution_exists_false(self, service, mock_history_repo):
        """Test check when execution does not exist."""
        mock_history_repo.check_execution_exists = AsyncMock(return_value=False)

        result = await service.check_execution_exists(999)

        assert result is False

    @pytest.mark.asyncio
    async def test_check_execution_exists_error(self, service, mock_history_repo):
        """Test handling of errors."""
        mock_history_repo.check_execution_exists = AsyncMock(
            side_effect=SQLAlchemyError("Database error")
        )

        with pytest.raises(SQLAlchemyError):
            await service.check_execution_exists(1)


class TestGetExecutionOutputs:
    """Tests for get_execution_outputs method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_get_execution_outputs_success(self, service, mock_history_repo, mock_logs_repo, mock_session):
        """Test successful retrieval of execution outputs."""
        job_id = str(uuid.uuid4())

        mock_log = MagicMock()
        mock_log.id = 1
        mock_log.execution_id = job_id
        mock_log.content = "Test output"
        mock_log.timestamp = "2024-01-01T00:00:00"

        mock_logs_repo.get_logs_by_execution_id = AsyncMock(return_value=[mock_log])
        mock_logs_repo.count_by_execution_id = AsyncMock(return_value=1)

        result = await service.get_execution_outputs(job_id)

        assert result.execution_id == job_id
        assert len(result.outputs) == 1
        assert result.total == 1

    @pytest.mark.asyncio
    async def test_get_execution_outputs_with_tenant_filter(self, service, mock_history_repo, mock_logs_repo, mock_session):
        """Test retrieval with tenant filtering when execution not found."""
        job_id = str(uuid.uuid4())
        tenant_ids = [str(uuid.uuid4())]

        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

        result = await service.get_execution_outputs(job_id, tenant_ids=tenant_ids)

        assert result.total == 0
        assert len(result.outputs) == 0

    @pytest.mark.asyncio
    async def test_get_execution_outputs_with_pagination(self, service, mock_history_repo, mock_logs_repo, mock_session):
        """Test retrieval with pagination."""
        job_id = str(uuid.uuid4())

        mock_logs_repo.get_logs_by_execution_id = AsyncMock(return_value=[])
        mock_logs_repo.count_by_execution_id = AsyncMock(return_value=0)

        result = await service.get_execution_outputs(job_id, limit=10, offset=5)

        assert result.limit == 10
        assert result.offset == 5


class TestGetDebugOutputs:
    """Tests for get_debug_outputs method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_get_debug_outputs_success(self, service, mock_history_repo, mock_logs_repo):
        """Test successful retrieval of debug outputs."""
        job_id = str(uuid.uuid4())

        mock_run = MagicMock()
        mock_run.id = 1

        mock_log = MagicMock()
        mock_log.id = 1
        mock_log.timestamp = "2024-01-01T00:00:00"
        mock_log.content = "Test debug content"

        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=mock_run)
        mock_logs_repo.get_logs_by_execution_id = AsyncMock(return_value=[mock_log])

        result = await service.get_debug_outputs(job_id)

        assert result is not None
        assert result.execution_id == job_id
        assert result.run_id == 1
        assert result.total_outputs == 1
        assert len(result.outputs) == 1

    @pytest.mark.asyncio
    async def test_get_debug_outputs_not_found(self, service, mock_history_repo):
        """Test retrieval when execution not found."""
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

        result = await service.get_debug_outputs("nonexistent")

        assert result is None


class TestDeleteAllExecutions:
    """Tests for delete_all_executions method."""

    @pytest.fixture
    def mock_session(self):
        session = AsyncMock()
        session.execute = AsyncMock()
        return session

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_delete_all_executions_success(self, service, mock_history_repo, mock_session):
        """Test successful deletion of all executions."""
        mock_history_repo.delete_all_executions = AsyncMock(return_value={
            'run_count': 5,
            'task_status_count': 10,
            'error_trace_count': 2
        })

        with patch('src.services.execution_trace_service.ExecutionTraceService') as mock_trace_service, \
             patch('src.services.execution_logs_service.ExecutionLogsService') as mock_logs_service, \
             patch('src.services.execution_service.ExecutionService') as mock_exec_service:
            mock_trace_instance = MagicMock()
            mock_trace_instance.repository = MagicMock()
            mock_trace_instance.repository.delete_all = AsyncMock(return_value=3)
            mock_trace_service.return_value = mock_trace_instance

            mock_logs_instance = MagicMock()
            mock_logs_instance.delete_all_logs = AsyncMock(return_value=5)
            mock_logs_service.return_value = mock_logs_instance

            mock_exec_service.executions = {}

            result = await service.delete_all_executions()

            assert result.success is True

    @pytest.mark.asyncio
    async def test_delete_all_executions_with_group_ids(self, service, mock_history_repo, mock_session):
        """Test deletion with group filtering."""
        group_ids = [str(uuid.uuid4())]
        job_id = str(uuid.uuid4())

        # Mock the session.execute to return job_ids
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(job_id,)]
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_history_repo.delete_all_executions = AsyncMock(return_value={
            'run_count': 1,
            'task_status_count': 2,
            'error_trace_count': 1
        })

        with patch('src.services.execution_trace_service.ExecutionTraceService') as mock_trace_service, \
             patch('src.services.execution_logs_service.ExecutionLogsService') as mock_logs_service, \
             patch('src.services.execution_service.ExecutionService') as mock_exec_service:
            mock_trace_instance = MagicMock()
            mock_trace_instance.repository = MagicMock()
            mock_trace_instance.repository.delete_by_job_id = AsyncMock(return_value=1)
            mock_trace_service.return_value = mock_trace_instance

            mock_logs_instance = MagicMock()
            mock_logs_instance.delete_by_execution_id = AsyncMock(return_value=1)
            mock_logs_service.return_value = mock_logs_instance

            mock_exec_service.executions = {}

            result = await service.delete_all_executions(group_ids=group_ids)

            assert result.success is True

    @pytest.mark.asyncio
    async def test_delete_all_executions_no_executions_found(self, service, mock_history_repo, mock_session):
        """Test deletion when no executions found for groups."""
        group_ids = [str(uuid.uuid4())]

        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute = AsyncMock(return_value=mock_result)

        result = await service.delete_all_executions(group_ids=group_ids)

        assert result.success is True
        assert "No executions found" in result.message


class TestDeleteExecution:
    """Tests for delete_execution method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_delete_execution_success(self, service, mock_history_repo):
        """Test successful deletion of a single execution."""
        job_id = str(uuid.uuid4())

        mock_run = MagicMock()
        mock_run.job_id = job_id

        mock_history_repo.get_execution_by_id = AsyncMock(return_value=mock_run)
        mock_history_repo.delete_execution = AsyncMock(return_value={
            'task_status_count': 2,
            'error_trace_count': 1
        })

        with patch('src.services.execution_trace_service.ExecutionTraceService') as mock_trace_service, \
             patch('src.services.execution_logs_service.ExecutionLogsService') as mock_logs_service, \
             patch('src.services.execution_service.ExecutionService') as mock_exec_service:
            mock_trace_instance = MagicMock()
            mock_trace_instance.repository = MagicMock()
            mock_trace_instance.repository.delete_by_job_id = AsyncMock(return_value=1)
            mock_trace_service.return_value = mock_trace_instance

            mock_logs_instance = MagicMock()
            mock_logs_instance.delete_by_execution_id = AsyncMock(return_value=1)
            mock_logs_service.return_value = mock_logs_instance

            mock_exec_service.executions = {}

            result = await service.delete_execution(1)

            assert result.success is True

    @pytest.mark.asyncio
    async def test_delete_execution_not_found(self, service, mock_history_repo):
        """Test deletion of non-existent execution."""
        mock_history_repo.get_execution_by_id = AsyncMock(return_value=None)

        result = await service.delete_execution(999)

        assert result is None


class TestDeleteExecutionByJobId:
    """Tests for delete_execution_by_job_id method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_delete_execution_by_job_id_success(self, service, mock_history_repo):
        """Test successful deletion by job_id."""
        job_id = str(uuid.uuid4())

        mock_run = MagicMock()
        mock_run.id = 1

        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=mock_run)
        mock_history_repo.delete_execution_by_job_id = AsyncMock(return_value={
            'task_status_count': 2,
            'error_trace_count': 1
        })

        with patch('src.services.execution_trace_service.ExecutionTraceService') as mock_trace_service, \
             patch('src.services.execution_logs_service.ExecutionLogsService') as mock_logs_service, \
             patch('src.services.execution_service.ExecutionService') as mock_exec_service:
            mock_trace_instance = MagicMock()
            mock_trace_instance.repository = MagicMock()
            mock_trace_instance.repository.delete_by_job_id = AsyncMock(return_value=1)
            mock_trace_service.return_value = mock_trace_instance

            mock_logs_instance = MagicMock()
            mock_logs_instance.delete_by_execution_id = AsyncMock(return_value=1)
            mock_logs_service.return_value = mock_logs_instance

            mock_exec_service.executions = {}

            result = await service.delete_execution_by_job_id(job_id)

            assert result.success is True

    @pytest.mark.asyncio
    async def test_delete_execution_by_job_id_not_found(self, service, mock_history_repo):
        """Test deletion when job_id not found."""
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

        result = await service.delete_execution_by_job_id("nonexistent")

        assert result is None


class TestCheckpointMethods:
    """Tests for checkpoint-related methods."""

    @pytest.fixture
    def mock_session(self):
        session = AsyncMock()
        session.commit = AsyncMock()
        return session

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_get_checkpoints_for_flow(self, service, mock_history_repo):
        """Test getting checkpoints for a flow."""
        flow_id = str(uuid.uuid4())
        mock_checkpoint = MagicMock()
        mock_history_repo.get_checkpoints_for_flow = AsyncMock(return_value=[mock_checkpoint])

        result = await service.get_checkpoints_for_flow(flow_id)

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_expire_checkpoint(self, service, mock_history_repo):
        """Test expiring a checkpoint."""
        mock_history_repo.update_checkpoint_status = AsyncMock(return_value=True)

        result = await service.expire_checkpoint(1)

        assert result is True
        mock_history_repo.update_checkpoint_status.assert_called_once_with(
            execution_id=1, status="expired", group_id=None
        )

    @pytest.mark.asyncio
    async def test_set_checkpoint_active(self, service, mock_history_repo, mock_session):
        """Test setting checkpoint as active."""
        flow_uuid = str(uuid.uuid4())
        mock_history_repo.set_checkpoint_info = AsyncMock(return_value=True)

        result = await service.set_checkpoint_active(1, flow_uuid, "method_name")

        assert result is True
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_mark_checkpoint_resumed(self, service, mock_history_repo):
        """Test marking a checkpoint as resumed."""
        mock_history_repo.update_checkpoint_status = AsyncMock(return_value=True)

        result = await service.mark_checkpoint_resumed(1, 2)

        assert result is True
        mock_history_repo.update_checkpoint_status.assert_called_once_with(
            execution_id=1, status="resumed"
        )


class TestGetExecutionByJobId:
    """Tests for get_execution_by_job_id method."""

    @pytest.fixture
    def mock_session(self):
        return AsyncMock()

    @pytest.fixture
    def mock_history_repo(self):
        return AsyncMock()

    @pytest.fixture
    def mock_logs_repo(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_session, mock_history_repo, mock_logs_repo):
        from src.services.execution_history_service import ExecutionHistoryService
        return ExecutionHistoryService(
            session=mock_session,
            execution_history_repository=mock_history_repo,
            execution_logs_repository=mock_logs_repo
        )

    @pytest.mark.asyncio
    async def test_get_execution_by_job_id_success(self, service, mock_history_repo):
        """Test successful retrieval by job_id."""
        job_id = str(uuid.uuid4())

        mock_run = MagicMock()
        mock_run.id = 1
        mock_run.job_id = job_id
        mock_run.status = "completed"
        mock_run.result = {"content": "Test"}
        mock_run.inputs = {}
        mock_run.created_at = "2024-01-01T00:00:00"

        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=mock_run)

        result = await service.get_execution_by_job_id(job_id)

        assert result is not None

    @pytest.mark.asyncio
    async def test_get_execution_by_job_id_not_found(self, service, mock_history_repo):
        """Test retrieval when job_id not found."""
        mock_history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

        result = await service.get_execution_by_job_id("nonexistent")

        assert result is None


class TestFactoryFunction:
    """Tests for factory function."""

    def test_get_execution_history_service(self):
        """Test factory function creates service correctly."""
        from src.services.execution_history_service import get_execution_history_service

        mock_session = AsyncMock()
        service = get_execution_history_service(mock_session)

        assert service is not None
        assert service.session == mock_session
