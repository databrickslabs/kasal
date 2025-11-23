"""
Unit tests for FlowExecutionService.

Tests the flow execution service layer with multi-tenancy support,
following the service architecture pattern.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC

from src.services.flow_execution_service import FlowExecutionService
from src.models.flow_execution import FlowExecution, FlowNodeExecution
from src.schemas.flow_execution import (
    FlowExecutionCreate,
    FlowExecutionUpdate,
    FlowNodeExecutionCreate,
    FlowNodeExecutionUpdate,
    FlowExecutionStatus
)


# Mock models
class MockFlowExecution:
    def __init__(
        self,
        id=1,
        flow_id=None,
        job_id="test-job-123",
        status=FlowExecutionStatus.PENDING,
        config=None,
        result=None,
        error=None,
        group_id="group-123",
        created_at=None,
        updated_at=None,
        completed_at=None
    ):
        self.id = id
        self.flow_id = flow_id or uuid.uuid4()
        self.job_id = job_id
        self.status = status
        self.config = config or {}
        self.result = result
        self.error = error
        self.group_id = group_id
        self.created_at = created_at or datetime.now(UTC)
        self.updated_at = updated_at or datetime.now(UTC)
        self.completed_at = completed_at


class MockFlowNodeExecution:
    def __init__(
        self,
        id=1,
        flow_execution_id=1,
        node_id="node-123",
        status=FlowExecutionStatus.RUNNING,
        agent_id=None,
        task_id=None,
        result=None,
        error=None,
        group_id="group-123",
        created_at=None,
        updated_at=None,
        completed_at=None
    ):
        self.id = id
        self.flow_execution_id = flow_execution_id
        self.node_id = node_id
        self.status = status
        self.agent_id = agent_id
        self.task_id = task_id
        self.result = result
        self.error = error
        self.group_id = group_id
        self.created_at = created_at or datetime.now(UTC)
        self.updated_at = updated_at or datetime.now(UTC)
        self.completed_at = completed_at


class MockFlow:
    def __init__(self, id=None, group_id="group-123"):
        self.id = id or uuid.uuid4()
        self.group_id = group_id


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock()


@pytest.fixture
def flow_execution_service(mock_session):
    """Create a FlowExecutionService instance with mock session."""
    return FlowExecutionService(mock_session)


@pytest.fixture
def mock_flow_execution():
    """Create a mock flow execution."""
    return MockFlowExecution()


@pytest.fixture
def mock_node_execution():
    """Create a mock node execution."""
    return MockFlowNodeExecution()


class TestFlowExecutionService:
    """Test cases for FlowExecutionService."""

    # ========== create_execution Tests ==========

    @pytest.mark.asyncio
    async def test_create_execution_success(self, flow_execution_service, mock_flow_execution):
        """Test successful flow execution creation."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        config = {"key": "value"}
        group_id = "group-123"

        with patch.object(flow_execution_service.flow_execution_repo, 'create',
                         new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_flow_execution

            result = await flow_execution_service.create_execution(
                flow_id=flow_id,
                job_id=job_id,
                config=config,
                group_id=group_id
            )

            assert result == mock_flow_execution
            mock_create.assert_called_once()
            call_args = mock_create.call_args[0][0]
            assert call_args.flow_id == flow_id
            assert call_args.job_id == job_id
            assert call_args.config == config
            assert call_args.group_id == group_id

    @pytest.mark.asyncio
    async def test_create_execution_inherits_group_id_from_flow(self, flow_execution_service, mock_flow_execution):
        """Test that group_id is inherited from parent flow when not provided."""
        flow_id = uuid.uuid4()
        job_id = "test-job-123"
        mock_flow = MockFlow(id=flow_id, group_id="inherited-group")

        with patch.object(flow_execution_service.flow_execution_repo, 'create',
                         new_callable=AsyncMock) as mock_create, \
             patch('src.repositories.flow_repository.FlowRepository') as MockFlowRepo:

            mock_flow_repo = MagicMock()
            mock_flow_repo.get = AsyncMock(return_value=mock_flow)
            MockFlowRepo.return_value = mock_flow_repo
            mock_create.return_value = mock_flow_execution

            result = await flow_execution_service.create_execution(
                flow_id=flow_id,
                job_id=job_id,
                group_id=None  # Not provided
            )

            # Should inherit group_id from flow
            call_args = mock_create.call_args[0][0]
            assert call_args.group_id == "inherited-group"

    @pytest.mark.asyncio
    async def test_create_execution_with_string_flow_id(self, flow_execution_service, mock_flow_execution):
        """Test creation with string flow_id (converted to UUID)."""
        flow_id_str = str(uuid.uuid4())
        job_id = "test-job-123"

        with patch.object(flow_execution_service.flow_execution_repo, 'create',
                         new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_flow_execution

            result = await flow_execution_service.create_execution(
                flow_id=flow_id_str,
                job_id=job_id,
                group_id="group-123"
            )

            assert result == mock_flow_execution
            call_args = mock_create.call_args[0][0]
            assert isinstance(call_args.flow_id, uuid.UUID)

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
    async def test_get_execution_success(self, flow_execution_service, mock_flow_execution):
        """Test successful execution retrieval."""
        execution_id = 1

        with patch.object(flow_execution_service.flow_execution_repo, 'get',
                         new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_flow_execution

            result = await flow_execution_service.get_execution(execution_id)

            assert result == mock_flow_execution
            mock_get.assert_called_once_with(execution_id)

    @pytest.mark.asyncio
    async def test_get_execution_not_found(self, flow_execution_service):
        """Test execution retrieval when not found."""
        execution_id = 999

        with patch.object(flow_execution_service.flow_execution_repo, 'get',
                         new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None

            result = await flow_execution_service.get_execution(execution_id)

            assert result is None

    # ========== update_execution_status Tests ==========

    @pytest.mark.asyncio
    async def test_update_execution_status_to_completed(self, flow_execution_service, mock_flow_execution):
        """Test updating execution status to completed."""
        execution_id = 1
        result_data = {"output": "success"}

        mock_flow_execution.status = FlowExecutionStatus.COMPLETED
        mock_flow_execution.result = result_data

        with patch.object(flow_execution_service.flow_execution_repo, 'update',
                         new_callable=AsyncMock) as mock_update:
            mock_update.return_value = mock_flow_execution

            result = await flow_execution_service.update_execution_status(
                execution_id=execution_id,
                status=FlowExecutionStatus.COMPLETED,
                result=result_data
            )

            assert result.status == FlowExecutionStatus.COMPLETED
            assert result.result == result_data
            mock_update.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_execution_status_to_failed(self, flow_execution_service, mock_flow_execution):
        """Test updating execution status to failed with error."""
        execution_id = 1
        error_msg = "Execution failed"

        mock_flow_execution.status = FlowExecutionStatus.FAILED
        mock_flow_execution.error = error_msg

        with patch.object(flow_execution_service.flow_execution_repo, 'update',
                         new_callable=AsyncMock) as mock_update:
            mock_update.return_value = mock_flow_execution

            result = await flow_execution_service.update_execution_status(
                execution_id=execution_id,
                status=FlowExecutionStatus.FAILED,
                error=error_msg
            )

            assert result.status == FlowExecutionStatus.FAILED
            assert result.error == error_msg

    # ========== update_execution_config Tests ==========

    @pytest.mark.asyncio
    async def test_update_execution_config(self, flow_execution_service, mock_flow_execution):
        """Test updating execution config for state persistence."""
        execution_id = 1
        new_config = {"state": "updated", "counter": 5}

        mock_flow_execution.config = new_config

        with patch.object(flow_execution_service.flow_execution_repo, 'update',
                         new_callable=AsyncMock) as mock_update:
            mock_update.return_value = mock_flow_execution

            result = await flow_execution_service.update_execution_config(
                execution_id=execution_id,
                config=new_config
            )

            assert result.config == new_config
            mock_update.assert_called_once()

    # ========== create_node_execution Tests ==========

    @pytest.mark.asyncio
    async def test_create_node_execution_success(self, flow_execution_service, mock_node_execution):
        """Test successful node execution creation."""
        flow_execution_id = 1
        node_id = "node-123"
        agent_id = 10
        task_id = 20
        group_id = "group-123"

        with patch.object(flow_execution_service.node_execution_repo, 'create',
                         new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_node_execution

            result = await flow_execution_service.create_node_execution(
                flow_execution_id=flow_execution_id,
                node_id=node_id,
                agent_id=agent_id,
                task_id=task_id,
                group_id=group_id
            )

            assert result == mock_node_execution
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_node_execution_inherits_group_id(self, flow_execution_service, mock_node_execution, mock_flow_execution):
        """Test that node execution inherits group_id from flow execution."""
        flow_execution_id = 1
        node_id = "node-123"

        with patch.object(flow_execution_service.node_execution_repo, 'create',
                         new_callable=AsyncMock) as mock_create, \
             patch.object(flow_execution_service, 'get_execution',
                         new_callable=AsyncMock) as mock_get_execution:

            mock_get_execution.return_value = mock_flow_execution
            mock_create.return_value = mock_node_execution

            result = await flow_execution_service.create_node_execution(
                flow_execution_id=flow_execution_id,
                node_id=node_id,
                group_id=None  # Not provided
            )

            # Should inherit group_id from flow execution
            call_args = mock_create.call_args[0][0]
            assert call_args.group_id == mock_flow_execution.group_id

    # ========== update_node_execution Tests ==========

    @pytest.mark.asyncio
    async def test_update_node_execution_status(self, flow_execution_service, mock_node_execution):
        """Test updating node execution status."""
        node_execution_id = 1
        result_data = {"output": "node completed"}

        mock_node_execution.status = FlowExecutionStatus.COMPLETED
        mock_node_execution.result = result_data

        with patch.object(flow_execution_service.node_execution_repo, 'update',
                         new_callable=AsyncMock) as mock_update:
            mock_update.return_value = mock_node_execution

            result = await flow_execution_service.update_node_execution(
                node_execution_id=node_execution_id,
                status=FlowExecutionStatus.COMPLETED,
                result=result_data
            )

            assert result.status == FlowExecutionStatus.COMPLETED
            assert result.result == result_data

    # ========== get_node_executions Tests ==========

    @pytest.mark.asyncio
    async def test_get_node_executions(self, flow_execution_service, mock_node_execution):
        """Test getting all node executions for a flow execution."""
        flow_execution_id = 1
        mock_nodes = [mock_node_execution, MockFlowNodeExecution(id=2, node_id="node-456")]

        with patch.object(flow_execution_service.node_execution_repo, 'get_by_flow_execution_id',
                         new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_nodes

            result = await flow_execution_service.get_node_executions(flow_execution_id)

            assert len(result) == 2
            assert result == mock_nodes
            mock_get.assert_called_once_with(flow_execution_id)

    # ========== delete_execution Tests ==========

    @pytest.mark.asyncio
    async def test_delete_execution_success(self, flow_execution_service, mock_node_execution):
        """Test successful deletion of flow execution and its nodes."""
        execution_id = 1
        mock_nodes = [mock_node_execution, MockFlowNodeExecution(id=2)]

        with patch.object(flow_execution_service, 'get_node_executions',
                         new_callable=AsyncMock) as mock_get_nodes, \
             patch.object(flow_execution_service.node_execution_repo, 'delete',
                         new_callable=AsyncMock) as mock_delete_node, \
             patch.object(flow_execution_service.flow_execution_repo, 'delete',
                         new_callable=AsyncMock) as mock_delete_flow:

            mock_get_nodes.return_value = mock_nodes

            result = await flow_execution_service.delete_execution(execution_id)

            assert result is True
            # Should delete all node executions
            assert mock_delete_node.call_count == 2
            # Should delete flow execution
            mock_delete_flow.assert_called_once_with(execution_id)

    # ========== get_executions_by_flow Tests ==========

    @pytest.mark.asyncio
    async def test_get_executions_by_flow_uuid(self, flow_execution_service, mock_flow_execution):
        """Test getting executions by flow UUID."""
        flow_id = uuid.uuid4()
        mock_executions = [mock_flow_execution, MockFlowExecution(id=2)]

        with patch.object(flow_execution_service.flow_execution_repo, 'get_by_flow_id',
                         new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_executions

            result = await flow_execution_service.get_executions_by_flow(flow_id)

            assert len(result) == 2
            assert result == mock_executions
            mock_get.assert_called_once_with(flow_id)

    @pytest.mark.asyncio
    async def test_get_executions_by_flow_string(self, flow_execution_service, mock_flow_execution):
        """Test getting executions by flow ID string."""
        flow_id = uuid.uuid4()
        flow_id_str = str(flow_id)

        with patch.object(flow_execution_service.flow_execution_repo, 'get_by_flow_id',
                         new_callable=AsyncMock) as mock_get:
            mock_get.return_value = [mock_flow_execution]

            result = await flow_execution_service.get_executions_by_flow(flow_id_str)

            # Should convert string to UUID
            called_with = mock_get.call_args[0][0]
            assert isinstance(called_with, uuid.UUID)
            assert called_with == flow_id

    # ========== Service Initialization Tests ==========

    def test_service_initialization(self, flow_execution_service, mock_session):
        """Test FlowExecutionService initialization."""
        assert flow_execution_service.session == mock_session
        assert hasattr(flow_execution_service, 'flow_execution_repo')
        assert hasattr(flow_execution_service, 'node_execution_repo')

    # ========== Multi-Tenancy Tests ==========

    @pytest.mark.asyncio
    async def test_multi_tenant_isolation(self, flow_execution_service, mock_flow_execution):
        """Test that group_id is properly set for multi-tenant isolation."""
        flow_id = uuid.uuid4()
        job_id = "test-job"
        group_id = "tenant-abc"

        with patch.object(flow_execution_service.flow_execution_repo, 'create',
                         new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_flow_execution

            await flow_execution_service.create_execution(
                flow_id=flow_id,
                job_id=job_id,
                group_id=group_id
            )

            call_args = mock_create.call_args[0][0]
            assert call_args.group_id == group_id

    @pytest.mark.asyncio
    async def test_group_id_propagation_to_nodes(self, flow_execution_service, mock_flow_execution, mock_node_execution):
        """Test that group_id propagates from flow execution to node executions."""
        flow_execution_id = 1
        node_id = "node-123"
        expected_group_id = "tenant-xyz"

        mock_flow_execution.group_id = expected_group_id

        with patch.object(flow_execution_service, 'get_execution',
                         new_callable=AsyncMock) as mock_get, \
             patch.object(flow_execution_service.node_execution_repo, 'create',
                         new_callable=AsyncMock) as mock_create:

            mock_get.return_value = mock_flow_execution
            mock_create.return_value = mock_node_execution

            await flow_execution_service.create_node_execution(
                flow_execution_id=flow_execution_id,
                node_id=node_id,
                group_id=None  # Should inherit
            )

            call_args = mock_create.call_args[0][0]
            assert call_args.group_id == expected_group_id
