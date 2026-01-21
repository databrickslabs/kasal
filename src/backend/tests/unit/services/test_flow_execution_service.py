"""
Unit tests for FlowExecutionService.

Tests the flow execution service layer with multi-tenancy support,
following the service architecture pattern.

NOTE: This service now uses the consolidated ExecutionHistory model instead of
the deprecated FlowExecution model. All flow executions are tracked in the
executionhistory table with execution_type='flow'.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.services.flow_execution_service import FlowExecutionService
from src.models.execution_history import ExecutionHistory


# Mock models
class MockExecutionHistory:
    def __init__(
        self,
        id=1,
        flow_id=None,
        job_id="test-job-123",
        status="pending",
        inputs=None,
        result=None,
        error=None,
        group_id="group-123",
        run_name="Test Run",
        execution_type="flow",
        created_at=None,
        updated_at=None,
        completed_at=None
    ):
        self.id = id
        self.flow_id = flow_id or uuid.uuid4()
        self.job_id = job_id
        self.status = status
        self.inputs = inputs or {}
        self.result = result
        self.error = error
        self.group_id = group_id
        self.run_name = run_name
        self.execution_type = execution_type
        self.created_at = created_at or datetime.now(UTC)
        self.updated_at = updated_at or datetime.now(UTC)
        self.completed_at = completed_at


class MockFlow:
    def __init__(self, id=None, group_id="group-123"):
        self.id = id or uuid.uuid4()
        self.group_id = group_id


class MockScalarResult:
    """Mock for SQLAlchemy scalar_one_or_none() result."""
    def __init__(self, result=None):
        self._result = result

    def scalar_one_or_none(self):
        return self._result


class MockScalarsResult:
    """Mock for SQLAlchemy scalars().all() result."""
    def __init__(self, results=None):
        self._results = results or []

    def scalars(self):
        return self

    def all(self):
        return self._results


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    session = AsyncMock()
    session.add = MagicMock()
    session.delete = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.execute = AsyncMock()
    return session


@pytest.fixture
def flow_execution_service(mock_session):
    """Create a FlowExecutionService instance with mock session."""
    return FlowExecutionService(mock_session)


@pytest.fixture
def mock_execution():
    """Create a mock execution history."""
    return MockExecutionHistory()


class TestFlowExecutionService:
    """Test cases for FlowExecutionService."""

    # ========== create_execution Tests ==========

    @pytest.mark.asyncio
    async def test_create_execution_success(self, flow_execution_service, mock_session, mock_execution):
        """Test successful flow execution creation."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        config = {"key": "value"}
        group_id = "group-123"

        # Mock no existing execution
        mock_session.execute.return_value = MockScalarResult(None)

        result = await flow_execution_service.create_execution(
            flow_id=flow_id,
            job_id=job_id,
            config=config,
            group_id=group_id
        )

        # Should add new execution to session
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_execution_inherits_group_id_from_flow(self, flow_execution_service, mock_session):
        """Test that group_id is inherited from parent flow when not provided."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        mock_flow = MockFlow(id=flow_id, group_id="inherited-group")

        # Mock no existing execution
        mock_session.execute.return_value = MockScalarResult(None)

        # Patch where FlowRepository is imported (inside the function)
        with patch('src.repositories.flow_repository.FlowRepository') as MockFlowRepo:
            mock_flow_repo = MagicMock()
            mock_flow_repo.get = AsyncMock(return_value=mock_flow)
            MockFlowRepo.return_value = mock_flow_repo

            await flow_execution_service.create_execution(
                flow_id=flow_id,
                job_id=job_id,
                group_id=None  # Not provided
            )

            # Should have tried to get parent flow
            mock_flow_repo.get.assert_called_once_with(flow_id)
            mock_session.add.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_execution_with_string_flow_id(self, flow_execution_service, mock_session):
        """Test creation with string flow_id (converted to UUID)."""
        flow_id = uuid.uuid4()
        flow_id_str = str(flow_id)
        job_id = "test-job-123"

        # Mock no existing execution
        mock_session.execute.return_value = MockScalarResult(None)

        await flow_execution_service.create_execution(
            flow_id=flow_id_str,
            job_id=job_id,
            group_id="group-123"
        )

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_execution_invalid_flow_id(self, flow_execution_service):
        """Test creation with invalid flow_id raises ValueError."""
        with pytest.raises(ValueError, match="Invalid UUID format"):
            await flow_execution_service.create_execution(
                flow_id="invalid-uuid",
                job_id="test-job",
                group_id="group-123"
            )

    # ========== get_execution Tests ==========

    @pytest.mark.asyncio
    async def test_get_execution_success(self, flow_execution_service, mock_session, mock_execution):
        """Test successful execution retrieval."""
        execution_id = 1

        mock_session.execute.return_value = MockScalarResult(mock_execution)

        result = await flow_execution_service.get_execution(execution_id)

        assert result == mock_execution
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_execution_not_found(self, flow_execution_service, mock_session):
        """Test execution retrieval when not found."""
        execution_id = 999

        mock_session.execute.return_value = MockScalarResult(None)

        result = await flow_execution_service.get_execution(execution_id)

        assert result is None

    # ========== update_execution_status Tests ==========

    @pytest.mark.asyncio
    async def test_update_execution_status_to_completed(self, flow_execution_service, mock_session, mock_execution):
        """Test updating execution status to completed."""
        execution_id = 1
        result_data = {"output": "success"}

        mock_session.execute.return_value = MockScalarResult(mock_execution)

        result = await flow_execution_service.update_execution_status(
            execution_id=execution_id,
            status="completed",
            result=result_data
        )

        assert result.status == "completed"
        assert result.result == result_data
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_execution_status_to_failed(self, flow_execution_service, mock_session, mock_execution):
        """Test updating execution status to failed with error."""
        execution_id = 1
        error_msg = "Execution failed"

        mock_session.execute.return_value = MockScalarResult(mock_execution)

        result = await flow_execution_service.update_execution_status(
            execution_id=execution_id,
            status="failed",
            error=error_msg
        )

        assert result.status == "failed"
        assert result.error == error_msg

    # ========== update_execution_config Tests ==========

    @pytest.mark.asyncio
    async def test_update_execution_config(self, flow_execution_service, mock_session, mock_execution):
        """Test updating execution config for state persistence."""
        execution_id = 1
        new_config = {"state": "updated", "counter": 5}

        mock_session.execute.return_value = MockScalarResult(mock_execution)

        result = await flow_execution_service.update_execution_config(
            execution_id=execution_id,
            config=new_config
        )

        assert result.inputs == new_config
        mock_session.commit.assert_called_once()

    # ========== get_node_executions Tests ==========

    @pytest.mark.asyncio
    async def test_get_node_executions(self, flow_execution_service):
        """Test getting node executions - returns empty list (no longer tracked)."""
        flow_execution_id = 1

        result = await flow_execution_service.get_node_executions(flow_execution_id)

        # Node execution tracking was removed - should return empty list
        assert result == []

    # ========== delete_execution Tests ==========

    @pytest.mark.asyncio
    async def test_delete_execution_success(self, flow_execution_service, mock_session, mock_execution):
        """Test successful deletion of flow execution."""
        execution_id = 1

        mock_session.execute.return_value = MockScalarResult(mock_execution)

        result = await flow_execution_service.delete_execution(execution_id)

        assert result is True
        mock_session.delete.assert_called_once_with(mock_execution)
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_execution_not_found(self, flow_execution_service, mock_session):
        """Test deletion when execution not found."""
        execution_id = 999

        mock_session.execute.return_value = MockScalarResult(None)

        result = await flow_execution_service.delete_execution(execution_id)

        assert result is False
        mock_session.delete.assert_not_called()

    # ========== get_executions_by_flow Tests ==========

    @pytest.mark.asyncio
    async def test_get_executions_by_flow_uuid(self, flow_execution_service, mock_session, mock_execution):
        """Test getting executions by flow UUID."""
        flow_id = uuid.uuid4()
        mock_executions = [mock_execution, MockExecutionHistory(id=2)]

        mock_session.execute.return_value = MockScalarsResult(mock_executions)

        result = await flow_execution_service.get_executions_by_flow(flow_id)

        assert len(result) == 2
        assert result == mock_executions
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_executions_by_flow_string(self, flow_execution_service, mock_session, mock_execution):
        """Test getting executions by flow ID string."""
        flow_id = uuid.uuid4()
        flow_id_str = str(flow_id)

        mock_session.execute.return_value = MockScalarsResult([mock_execution])

        result = await flow_execution_service.get_executions_by_flow(flow_id_str)

        assert len(result) == 1
        mock_session.execute.assert_called_once()

    # ========== Service Initialization Tests ==========

    def test_service_initialization(self, flow_execution_service, mock_session):
        """Test FlowExecutionService initialization."""
        assert flow_execution_service.session == mock_session
        assert hasattr(flow_execution_service, 'execution_repo')

    # ========== Multi-Tenancy Tests ==========

    @pytest.mark.asyncio
    async def test_multi_tenant_isolation(self, flow_execution_service, mock_session):
        """Test that group_id is properly set for multi-tenant isolation."""
        flow_id = uuid.uuid4()
        job_id = "test-job"
        group_id = "tenant-abc"

        # Mock no existing execution
        mock_session.execute.return_value = MockScalarResult(None)

        await flow_execution_service.create_execution(
            flow_id=flow_id,
            job_id=job_id,
            group_id=group_id
        )

        # Should add execution with group_id
        mock_session.add.assert_called_once()
        added_execution = mock_session.add.call_args[0][0]
        assert added_execution.group_id == group_id

    # ========== Existing Execution Update Tests ==========

    @pytest.mark.asyncio
    async def test_create_execution_updates_existing(self, flow_execution_service, mock_session, mock_execution):
        """Test that create_execution updates existing record if job_id exists."""
        flow_id = uuid.uuid4()
        job_id = "existing-job"

        # Mock existing execution found
        mock_session.execute.return_value = MockScalarResult(mock_execution)

        await flow_execution_service.create_execution(
            flow_id=flow_id,
            job_id=job_id,
            group_id="group-123"
        )

        # Should NOT add new execution - just update existing
        mock_session.add.assert_not_called()
        mock_session.commit.assert_called_once()
        # Execution should be updated
        assert mock_execution.execution_type == "flow"
        assert mock_execution.flow_id == flow_id
