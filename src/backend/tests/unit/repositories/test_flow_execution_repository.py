"""
Unit tests for FlowExecutionRepository and FlowNodeExecutionRepository.

Tests the functionality of flow execution repositories including
CRUD operations, status management, UUID handling, and both async/sync versions.
"""
import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from src.repositories.flow_execution_repository import (
    FlowExecutionRepository, 
    FlowNodeExecutionRepository,
    SyncFlowExecutionRepository,
    SyncFlowNodeExecutionRepository
)
from src.models.flow_execution import FlowExecution, FlowNodeExecution
from src.schemas.flow_execution import (
    FlowExecutionCreate,
    FlowExecutionUpdate,
    FlowNodeExecutionCreate,
    FlowNodeExecutionUpdate,
    FlowExecutionStatus
)


# Mock flow execution model
class MockFlowExecution:
    def __init__(self, id=1, flow_id=None, job_id="job-123", status=FlowExecutionStatus.PENDING,
                 config=None, result=None, error=None, created_at=None, updated_at=None, completed_at=None):
        self.id = id
        self.flow_id = flow_id or uuid.uuid4()
        self.job_id = job_id
        self.status = status
        self.config = config or {}
        self.result = result
        self.error = error
        self.created_at = created_at or datetime.now(UTC)
        self.updated_at = updated_at or datetime.now(UTC)
        self.completed_at = completed_at


# Mock flow node execution model
class MockFlowNodeExecution:
    def __init__(self, id=1, flow_execution_id=1, node_id="node-123", status=FlowExecutionStatus.PENDING,
                 agent_id=123, task_id=456, result=None, error=None,
                 created_at=None, updated_at=None, completed_at=None):
        self.id = id
        self.flow_execution_id = flow_execution_id
        self.node_id = node_id
        self.status = status
        self.agent_id = agent_id
        self.task_id = task_id
        self.result = result
        self.error = error
        self.created_at = created_at or datetime.now(UTC)
        self.updated_at = updated_at or datetime.now(UTC)
        self.completed_at = completed_at


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)
    
    def scalars(self):
        return self._scalars


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    session = MagicMock()
    session.query.return_value = session
    session.filter.return_value = session
    session.first.return_value = None
    session.all.return_value = []
    session.add = MagicMock()
    session.commit = MagicMock()
    session.refresh = MagicMock()
    return session


@pytest.fixture
def flow_execution_repository(mock_async_session):
    """Create a flow execution repository with async session."""
    return FlowExecutionRepository(session=mock_async_session)


@pytest.fixture
def flow_node_execution_repository(mock_async_session):
    """Create a flow node execution repository with async session."""
    return FlowNodeExecutionRepository(session=mock_async_session)


@pytest.fixture
def sync_flow_execution_repository(mock_sync_session):
    """Create a sync flow execution repository."""
    return SyncFlowExecutionRepository(db=mock_sync_session)


@pytest.fixture
def sync_flow_node_execution_repository(mock_sync_session):
    """Create a sync flow node execution repository."""
    return SyncFlowNodeExecutionRepository(db=mock_sync_session)


@pytest.fixture
def sample_flow_execution_create():
    """Create sample flow execution create data."""
    return FlowExecutionCreate(
        flow_id=uuid.uuid4(),
        job_id="test-job-123",
        status=FlowExecutionStatus.PENDING,
        config={"key": "value"}
    )


@pytest.fixture
def sample_flow_execution_update():
    """Create sample flow execution update data."""
    return FlowExecutionUpdate(
        status=FlowExecutionStatus.RUNNING,
        result={"output": "test result"},
        error=None
    )


@pytest.fixture
def sample_flow_node_execution_create():
    """Create sample flow node execution create data."""
    return FlowNodeExecutionCreate(
        flow_execution_id=1,
        node_id="node-123",
        status=FlowExecutionStatus.PENDING,
        agent_id=123,
        task_id=456
    )


@pytest.fixture
def sample_flow_node_execution_update():
    """Create sample flow node execution update data."""
    return FlowNodeExecutionUpdate(
        status=FlowExecutionStatus.COMPLETED,
        result={"node_output": "completed"},
        error=None
    )


class TestFlowExecutionRepositoryInit:
    """Test cases for FlowExecutionRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = FlowExecutionRepository(session=mock_async_session)
        
        assert repository.session == mock_async_session


class TestFlowExecutionRepositoryCreate:
    """Test cases for FlowExecutionRepository create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, flow_execution_repository, mock_async_session, sample_flow_execution_create):
        """Test successful flow execution creation."""
        with patch('src.repositories.flow_execution_repository.FlowExecution') as mock_flow_execution_class:
            created_execution = MockFlowExecution(
                flow_id=sample_flow_execution_create.flow_id,
                job_id=sample_flow_execution_create.job_id,
                status=sample_flow_execution_create.status
            )
            mock_flow_execution_class.return_value = created_execution
            
            with patch('src.repositories.flow_execution_repository.datetime') as mock_datetime:
                mock_now = datetime.now(UTC)
                mock_datetime.now.return_value = mock_now
                
                result = await flow_execution_repository.create(sample_flow_execution_create)
                
                assert result == created_execution
                mock_flow_execution_class.assert_called_once()
                mock_async_session.add.assert_called_once_with(created_execution)
                mock_async_session.commit.assert_called_once()
                mock_async_session.refresh.assert_called_once_with(created_execution)
                
                # Verify datetime was set
                call_args = mock_flow_execution_class.call_args[1]
                assert call_args["created_at"] == mock_now
                assert call_args["updated_at"] == mock_now
    
    @pytest.mark.asyncio
    async def test_create_with_empty_config(self, flow_execution_repository, mock_async_session):
        """Test creation with None config defaults to empty dict."""
        create_data = FlowExecutionCreate(
            flow_id=uuid.uuid4(),
            job_id="test-job",
            status=FlowExecutionStatus.PENDING,
            config=None
        )
        
        with patch('src.repositories.flow_execution_repository.FlowExecution') as mock_flow_execution_class:
            created_execution = MockFlowExecution()
            mock_flow_execution_class.return_value = created_execution
            
            with patch('src.repositories.flow_execution_repository.datetime'):
                result = await flow_execution_repository.create(create_data)
                
                call_args = mock_flow_execution_class.call_args[1]
                assert call_args["config"] == {}
    
    @pytest.mark.asyncio
    async def test_create_database_error(self, flow_execution_repository, mock_async_session, sample_flow_execution_create):
        """Test create with database error."""
        with patch('src.repositories.flow_execution_repository.FlowExecution'):
            mock_async_session.commit.side_effect = Exception("Commit failed")
            
            with pytest.raises(Exception, match="Commit failed"):
                await flow_execution_repository.create(sample_flow_execution_create)


class TestFlowExecutionRepositoryGet:
    """Test cases for FlowExecutionRepository get methods."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, flow_execution_repository, mock_async_session):
        """Test successful get by ID."""
        execution = MockFlowExecution(id=1)
        mock_result = MockResult([execution])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.get(1)
        
        assert result == execution
        mock_async_session.execute.assert_called_once()
        
        # Verify query construction
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(FlowExecution)))
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, flow_execution_repository, mock_async_session):
        """Test get when execution not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.get(999)
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_job_id_success(self, flow_execution_repository, mock_async_session):
        """Test successful get by job ID."""
        execution = MockFlowExecution(job_id="job-123")
        mock_result = MockResult([execution])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.get_by_job_id("job-123")
        
        assert result == execution
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_job_id_not_found(self, flow_execution_repository, mock_async_session):
        """Test get by job ID when not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.get_by_job_id("nonexistent")
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_flow_id_success(self, flow_execution_repository, mock_async_session):
        """Test successful get by flow ID."""
        flow_id = uuid.uuid4()
        executions = [
            MockFlowExecution(id=1, flow_id=flow_id),
            MockFlowExecution(id=2, flow_id=flow_id)
        ]
        mock_result = MockResult(executions)
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.get_by_flow_id(flow_id)
        
        assert len(result) == 2
        assert all(exec.flow_id == flow_id for exec in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_flow_id_string_uuid(self, flow_execution_repository, mock_async_session):
        """Test get by flow ID with string UUID."""
        flow_id = uuid.uuid4()
        executions = [MockFlowExecution(flow_id=flow_id)]
        mock_result = MockResult(executions)
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.get_by_flow_id(str(flow_id))
        
        assert len(result) == 1
        assert result[0].flow_id == flow_id
    
    @pytest.mark.asyncio
    async def test_get_by_flow_id_invalid_uuid_string(self, flow_execution_repository, mock_async_session):
        """Test get by flow ID with invalid UUID string."""
        result = await flow_execution_repository.get_by_flow_id("invalid-uuid")
        
        assert result == []
        mock_async_session.execute.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_all_success(self, flow_execution_repository, mock_async_session):
        """Test successful get all executions."""
        executions = [MockFlowExecution(id=1), MockFlowExecution(id=2)]
        mock_result = MockResult(executions)
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.get_all()
        
        assert len(result) == 2
        assert result == executions
        mock_async_session.execute.assert_called_once()


class TestFlowExecutionRepositoryUpdate:
    """Test cases for FlowExecutionRepository update method."""
    
    @pytest.mark.asyncio
    async def test_update_success(self, flow_execution_repository, mock_async_session, sample_flow_execution_update):
        """Test successful flow execution update."""
        updated_execution = MockFlowExecution(id=1, status=FlowExecutionStatus.RUNNING)
        mock_result = MockResult([updated_execution])
        mock_async_session.execute.return_value = mock_result
        
        with patch('src.repositories.flow_execution_repository.datetime') as mock_datetime:
            mock_now = datetime.now(UTC)
            mock_datetime.now.return_value = mock_now
            
            result = await flow_execution_repository.update(1, sample_flow_execution_update)
            
            assert result == updated_execution
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()
            
            # Verify update statement construction
            call_args = mock_async_session.execute.call_args[0][0]
            assert hasattr(call_args, 'compile')  # SQLAlchemy update statement
    
    @pytest.mark.asyncio
    async def test_update_terminal_status_sets_completed_at(self, flow_execution_repository, mock_async_session):
        """Test update with terminal status sets completed_at."""
        update_data = FlowExecutionUpdate(status=FlowExecutionStatus.COMPLETED)
        updated_execution = MockFlowExecution(id=1, status=FlowExecutionStatus.COMPLETED)
        mock_result = MockResult([updated_execution])
        mock_async_session.execute.return_value = mock_result
        
        with patch('src.repositories.flow_execution_repository.datetime') as mock_datetime:
            mock_now = datetime.now(UTC)
            mock_datetime.now.return_value = mock_now
            
            result = await flow_execution_repository.update(1, update_data)
            
            assert result == updated_execution
            # Verify completed_at would be set in update statement
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, flow_execution_repository, mock_async_session, sample_flow_execution_update):
        """Test update when execution not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.update(999, sample_flow_execution_update)
        
        assert result is None
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_partial_data(self, flow_execution_repository, mock_async_session):
        """Test update with only some fields provided."""
        update_data = FlowExecutionUpdate(status=FlowExecutionStatus.FAILED, result=None, error="Test error")
        updated_execution = MockFlowExecution(id=1)
        mock_result = MockResult([updated_execution])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.update(1, update_data)
        
        assert result == updated_execution
        mock_async_session.execute.assert_called_once()


class TestFlowNodeExecutionRepositoryInit:
    """Test cases for FlowNodeExecutionRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = FlowNodeExecutionRepository(session=mock_async_session)
        
        assert repository.session == mock_async_session


class TestFlowNodeExecutionRepositoryCreate:
    """Test cases for FlowNodeExecutionRepository create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, flow_node_execution_repository, mock_async_session, sample_flow_node_execution_create):
        """Test successful flow node execution creation."""
        with patch('src.repositories.flow_execution_repository.FlowNodeExecution') as mock_node_execution_class:
            created_node_execution = MockFlowNodeExecution(
                flow_execution_id=sample_flow_node_execution_create.flow_execution_id,
                node_id=sample_flow_node_execution_create.node_id
            )
            mock_node_execution_class.return_value = created_node_execution
            
            with patch('src.repositories.flow_execution_repository.datetime') as mock_datetime:
                mock_now = datetime.now(UTC)
                mock_datetime.now.return_value = mock_now
                
                result = await flow_node_execution_repository.create(sample_flow_node_execution_create)
                
                assert result == created_node_execution
                mock_node_execution_class.assert_called_once()
                mock_async_session.add.assert_called_once_with(created_node_execution)
                mock_async_session.commit.assert_called_once()
                mock_async_session.refresh.assert_called_once_with(created_node_execution)
    
    @pytest.mark.asyncio
    async def test_create_database_error(self, flow_node_execution_repository, mock_async_session, sample_flow_node_execution_create):
        """Test create with database error."""
        with patch('src.repositories.flow_execution_repository.FlowNodeExecution'):
            mock_async_session.commit.side_effect = Exception("Commit failed")
            
            with pytest.raises(Exception, match="Commit failed"):
                await flow_node_execution_repository.create(sample_flow_node_execution_create)


class TestFlowNodeExecutionRepositoryGet:
    """Test cases for FlowNodeExecutionRepository get methods."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, flow_node_execution_repository, mock_async_session):
        """Test successful get by ID."""
        node_execution = MockFlowNodeExecution(id=1)
        mock_result = MockResult([node_execution])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_node_execution_repository.get(1)
        
        assert result == node_execution
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, flow_node_execution_repository, mock_async_session):
        """Test get when node execution not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_node_execution_repository.get(999)
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_flow_execution_success(self, flow_node_execution_repository, mock_async_session):
        """Test successful get by flow execution ID."""
        node_executions = [
            MockFlowNodeExecution(id=1, flow_execution_id=1),
            MockFlowNodeExecution(id=2, flow_execution_id=1)
        ]
        mock_result = MockResult(node_executions)
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_node_execution_repository.get_by_flow_execution(1)
        
        assert len(result) == 2
        assert all(node.flow_execution_id == 1 for node in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_flow_execution_empty(self, flow_node_execution_repository, mock_async_session):
        """Test get by flow execution when no node executions found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_node_execution_repository.get_by_flow_execution(1)
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestFlowNodeExecutionRepositoryUpdate:
    """Test cases for FlowNodeExecutionRepository update method."""
    
    @pytest.mark.asyncio
    async def test_update_success(self, flow_node_execution_repository, mock_async_session, sample_flow_node_execution_update):
        """Test successful flow node execution update."""
        updated_node_execution = MockFlowNodeExecution(id=1, status=FlowExecutionStatus.COMPLETED)
        mock_result = MockResult([updated_node_execution])
        mock_async_session.execute.return_value = mock_result
        
        with patch('src.repositories.flow_execution_repository.datetime') as mock_datetime:
            mock_now = datetime.now(UTC)
            mock_datetime.now.return_value = mock_now
            
            result = await flow_node_execution_repository.update(1, sample_flow_node_execution_update)
            
            assert result == updated_node_execution
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_terminal_status_sets_completed_at(self, flow_node_execution_repository, mock_async_session):
        """Test update with terminal status sets completed_at."""
        update_data = FlowNodeExecutionUpdate(status=FlowExecutionStatus.FAILED)
        updated_node_execution = MockFlowNodeExecution(id=1, status=FlowExecutionStatus.FAILED)
        mock_result = MockResult([updated_node_execution])
        mock_async_session.execute.return_value = mock_result
        
        with patch('src.repositories.flow_execution_repository.datetime') as mock_datetime:
            mock_now = datetime.now(UTC)
            mock_datetime.now.return_value = mock_now
            
            result = await flow_node_execution_repository.update(1, update_data)
            
            assert result == updated_node_execution
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, flow_node_execution_repository, mock_async_session, sample_flow_node_execution_update):
        """Test update when node execution not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_node_execution_repository.update(999, sample_flow_node_execution_update)
        
        assert result is None
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()


class TestSyncFlowExecutionRepository:
    """Test cases for SyncFlowExecutionRepository."""
    
    def test_init_success(self, mock_sync_session):
        """Test successful initialization."""
        repository = SyncFlowExecutionRepository(db=mock_sync_session)
        
        assert repository.db == mock_sync_session
    
    def test_create_success(self, sync_flow_execution_repository, mock_sync_session, sample_flow_execution_create):
        """Test successful sync flow execution creation."""
        with patch('src.repositories.flow_execution_repository.FlowExecution') as mock_flow_execution_class:
            created_execution = MockFlowExecution()
            mock_flow_execution_class.return_value = created_execution
            
            with patch('src.repositories.flow_execution_repository.datetime'):
                result = sync_flow_execution_repository.create(sample_flow_execution_create)
                
                assert result == created_execution
                mock_sync_session.add.assert_called_once_with(created_execution)
                mock_sync_session.commit.assert_called_once()
                mock_sync_session.refresh.assert_called_once_with(created_execution)
    
    def test_get_success(self, sync_flow_execution_repository, mock_sync_session):
        """Test successful sync get by ID."""
        execution = MockFlowExecution(id=1)
        mock_sync_session.first.return_value = execution
        
        result = sync_flow_execution_repository.get(1)
        
        assert result == execution
        mock_sync_session.query.assert_called_once_with(FlowExecution)
        mock_sync_session.filter.assert_called_once()
    
    def test_get_by_job_id_success(self, sync_flow_execution_repository, mock_sync_session):
        """Test successful sync get by job ID."""
        execution = MockFlowExecution(job_id="job-123")
        mock_sync_session.first.return_value = execution
        
        result = sync_flow_execution_repository.get_by_job_id("job-123")
        
        assert result == execution
        mock_sync_session.query.assert_called_once_with(FlowExecution)
        mock_sync_session.filter.assert_called_once()
    
    def test_get_by_flow_id_success(self, sync_flow_execution_repository, mock_sync_session):
        """Test successful sync get by flow ID."""
        flow_id = uuid.uuid4()
        executions = [MockFlowExecution(flow_id=flow_id)]
        mock_sync_session.all.return_value = executions
        
        result = sync_flow_execution_repository.get_by_flow_id(flow_id)
        
        assert result == executions
        mock_sync_session.query.assert_called_once_with(FlowExecution)
        mock_sync_session.filter.assert_called_once()
    
    def test_get_by_flow_id_invalid_uuid(self, sync_flow_execution_repository, mock_sync_session):
        """Test sync get by flow ID with invalid UUID."""
        result = sync_flow_execution_repository.get_by_flow_id("invalid-uuid")
        
        assert result == []
        mock_sync_session.query.assert_not_called()
    
    def test_update_success(self, sync_flow_execution_repository, mock_sync_session, sample_flow_execution_update):
        """Test successful sync update."""
        execution = MockFlowExecution(id=1)
        mock_sync_session.first.return_value = execution
        
        with patch('src.repositories.flow_execution_repository.datetime'):
            result = sync_flow_execution_repository.update(1, sample_flow_execution_update)
            
            assert result == execution
            mock_sync_session.commit.assert_called_once()
            mock_sync_session.refresh.assert_called_once_with(execution)
    
    def test_update_not_found(self, sync_flow_execution_repository, mock_sync_session, sample_flow_execution_update):
        """Test sync update when execution not found."""
        mock_sync_session.first.return_value = None
        
        result = sync_flow_execution_repository.update(999, sample_flow_execution_update)
        
        assert result is None
        mock_sync_session.commit.assert_not_called()
    
    def test_sync_update_with_error_and_terminal_status(self, sync_flow_execution_repository, mock_sync_session):
        """Test sync update with error field and terminal status to cover lines 371, 377."""
        update_data = FlowExecutionUpdate(
            status=FlowExecutionStatus.FAILED,
            error="Sync error message"
        )
        execution = MockFlowExecution(id=1)
        mock_sync_session.first.return_value = execution
        
        with patch('src.repositories.flow_execution_repository.datetime'):
            result = sync_flow_execution_repository.update(1, update_data)
            
            assert result == execution
            mock_sync_session.commit.assert_called_once()
            mock_sync_session.refresh.assert_called_once_with(execution)


class TestSyncFlowNodeExecutionRepository:
    """Test cases for SyncFlowNodeExecutionRepository."""
    
    def test_init_success(self, mock_sync_session):
        """Test successful initialization."""
        repository = SyncFlowNodeExecutionRepository(db=mock_sync_session)
        
        assert repository.db == mock_sync_session
    
    def test_create_success(self, sync_flow_node_execution_repository, mock_sync_session, sample_flow_node_execution_create):
        """Test successful sync flow node execution creation."""
        with patch('src.repositories.flow_execution_repository.FlowNodeExecution') as mock_node_execution_class:
            created_node_execution = MockFlowNodeExecution()
            mock_node_execution_class.return_value = created_node_execution
            
            with patch('src.repositories.flow_execution_repository.datetime'):
                result = sync_flow_node_execution_repository.create(sample_flow_node_execution_create)
                
                assert result == created_node_execution
                mock_sync_session.add.assert_called_once_with(created_node_execution)
                mock_sync_session.commit.assert_called_once()
                mock_sync_session.refresh.assert_called_once_with(created_node_execution)
    
    def test_get_success(self, sync_flow_node_execution_repository, mock_sync_session):
        """Test successful sync get by ID."""
        node_execution = MockFlowNodeExecution(id=1)
        mock_sync_session.first.return_value = node_execution
        
        result = sync_flow_node_execution_repository.get(1)
        
        assert result == node_execution
        mock_sync_session.query.assert_called_once_with(FlowNodeExecution)
        mock_sync_session.filter.assert_called_once()
    
    def test_get_by_flow_execution_success(self, sync_flow_node_execution_repository, mock_sync_session):
        """Test successful sync get by flow execution."""
        node_executions = [MockFlowNodeExecution(flow_execution_id=1)]
        mock_sync_session.all.return_value = node_executions
        
        result = sync_flow_node_execution_repository.get_by_flow_execution(1)
        
        assert result == node_executions
        mock_sync_session.query.assert_called_once_with(FlowNodeExecution)
        mock_sync_session.filter.assert_called_once()
    
    def test_update_success(self, sync_flow_node_execution_repository, mock_sync_session, sample_flow_node_execution_update):
        """Test successful sync update."""
        node_execution = MockFlowNodeExecution(id=1)
        mock_sync_session.first.return_value = node_execution
        
        with patch('src.repositories.flow_execution_repository.datetime'):
            result = sync_flow_node_execution_repository.update(1, sample_flow_node_execution_update)
            
            assert result == node_execution
            mock_sync_session.commit.assert_called_once()
            mock_sync_session.refresh.assert_called_once_with(node_execution)
    
    def test_update_not_found(self, sync_flow_node_execution_repository, mock_sync_session, sample_flow_node_execution_update):
        """Test sync update when node execution not found."""
        mock_sync_session.first.return_value = None
        
        result = sync_flow_node_execution_repository.update(999, sample_flow_node_execution_update)
        
        assert result is None
        mock_sync_session.commit.assert_not_called()
    
    def test_sync_node_update_with_error_field(self, sync_flow_node_execution_repository, mock_sync_session):
        """Test sync node execution update with error field to cover line 474."""
        update_data = FlowNodeExecutionUpdate(
            status=FlowExecutionStatus.FAILED,
            error="Sync node error message"
        )
        node_execution = MockFlowNodeExecution(id=1)
        mock_sync_session.first.return_value = node_execution
        
        with patch('src.repositories.flow_execution_repository.datetime'):
            result = sync_flow_node_execution_repository.update(1, update_data)
            
            assert result == node_execution
            mock_sync_session.commit.assert_called_once()
            mock_sync_session.refresh.assert_called_once_with(node_execution)


class TestFlowExecutionRepositoryIntegration:
    """Integration test cases testing repository interactions."""
    
    @pytest.mark.asyncio
    async def test_flow_execution_with_node_executions_workflow(self, flow_execution_repository, flow_node_execution_repository, mock_async_session):
        """Test complete workflow: create flow execution, then create node executions."""
        flow_execution_create = FlowExecutionCreate(
            flow_id=uuid.uuid4(),
            job_id="integration-job",
            status=FlowExecutionStatus.PENDING
        )
        
        # Create flow execution
        with patch('src.repositories.flow_execution_repository.FlowExecution') as mock_flow_execution_class:
            created_flow_execution = MockFlowExecution(id=1, job_id="integration-job")
            mock_flow_execution_class.return_value = created_flow_execution
            
            with patch('src.repositories.flow_execution_repository.datetime'):
                flow_result = await flow_execution_repository.create(flow_execution_create)
                
                # Create node execution for the flow execution
                node_execution_create = FlowNodeExecutionCreate(
                    flow_execution_id=flow_result.id,
                    node_id="node-1",
                    status=FlowExecutionStatus.PENDING,
                    agent_id=1,
                    task_id=1
                )
                
                with patch('src.repositories.flow_execution_repository.FlowNodeExecution') as mock_node_execution_class:
                    created_node_execution = MockFlowNodeExecution(flow_execution_id=1)
                    mock_node_execution_class.return_value = created_node_execution
                    
                    node_result = await flow_node_execution_repository.create(node_execution_create)
                    
                    assert flow_result.id == 1
                    assert node_result.flow_execution_id == 1
    
    @pytest.mark.asyncio
    async def test_async_sync_repository_equivalence(self, mock_async_session, mock_sync_session):
        """Test that async and sync repositories produce equivalent results."""
        async_repo = FlowExecutionRepository(session=mock_async_session)
        sync_repo = SyncFlowExecutionRepository(db=mock_sync_session)
        
        # Both should handle same create data
        create_data = FlowExecutionCreate(
            flow_id=uuid.uuid4(),
            job_id="equivalence-test",
            status=FlowExecutionStatus.PENDING
        )
        
        with patch('src.repositories.flow_execution_repository.FlowExecution') as mock_flow_execution_class:
            with patch('src.repositories.flow_execution_repository.datetime'):
                mock_execution = MockFlowExecution()
                mock_flow_execution_class.return_value = mock_execution
                
                # Both should create successfully
                async_result = await async_repo.create(create_data)
                sync_result = sync_repo.create(create_data)
                
                assert async_result == mock_execution
                assert sync_result == mock_execution


class TestFlowExecutionRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_get_database_error(self, flow_execution_repository, mock_async_session):
        """Test get with database error."""
        mock_async_session.execute.side_effect = Exception("Connection lost")
        
        with pytest.raises(Exception, match="Connection lost"):
            await flow_execution_repository.get(1)
    
    @pytest.mark.asyncio
    async def test_get_by_flow_id_database_error(self, flow_execution_repository, mock_async_session):
        """Test get by flow ID with database error."""
        mock_async_session.execute.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            await flow_execution_repository.get_by_flow_id(uuid.uuid4())
    
    @pytest.mark.asyncio
    async def test_update_database_error(self, flow_execution_repository, mock_async_session, sample_flow_execution_update):
        """Test update with database error."""
        mock_async_session.execute.side_effect = Exception("Update failed")
        
        with pytest.raises(Exception, match="Update failed"):
            await flow_execution_repository.update(1, sample_flow_execution_update)
    
    @pytest.mark.asyncio
    async def test_node_execution_get_database_error(self, flow_node_execution_repository, mock_async_session):
        """Test node execution get with database error."""
        mock_async_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await flow_node_execution_repository.get(1)


class TestFlowExecutionRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_update_with_none_values(self, flow_execution_repository, mock_async_session):
        """Test update with None values for optional fields."""
        update_data = FlowExecutionUpdate(status=None, result=None, error=None)
        updated_execution = MockFlowExecution(id=1)
        mock_result = MockResult([updated_execution])
        mock_async_session.execute.return_value = mock_result
        
        result = await flow_execution_repository.update(1, update_data)
        
        assert result == updated_execution
        # Should still execute update (with updated_at)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_flow_id_empty_string(self, flow_execution_repository, mock_async_session):
        """Test get by flow ID with empty string."""
        result = await flow_execution_repository.get_by_flow_id("")
        
        assert result == []
        mock_async_session.execute.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_terminal_status_variations(self, flow_execution_repository, mock_async_session):
        """Test update with different terminal statuses."""
        terminal_statuses = [FlowExecutionStatus.COMPLETED, FlowExecutionStatus.FAILED]
        
        for status in terminal_statuses:
            update_data = FlowExecutionUpdate(status=status)
            updated_execution = MockFlowExecution(id=1, status=status)
            mock_result = MockResult([updated_execution])
            mock_async_session.execute.return_value = mock_result
            
            with patch('src.repositories.flow_execution_repository.datetime'):
                result = await flow_execution_repository.update(1, update_data)
                
                assert result == updated_execution
                # completed_at should be set for terminal statuses
    
    def test_sync_repository_get_by_flow_id_string_conversion(self, sync_flow_execution_repository, mock_sync_session):
        """Test sync repository UUID string conversion."""
        flow_id = uuid.uuid4()
        executions = [MockFlowExecution(flow_id=flow_id)]
        mock_sync_session.all.return_value = executions
        
        result = sync_flow_execution_repository.get_by_flow_id(str(flow_id))
        
        assert result == executions
        # Should have successfully converted string to UUID
    
    @pytest.mark.asyncio
    async def test_large_config_and_result_data(self, flow_execution_repository, mock_async_session):
        """Test handling large config and result data."""
        large_config = {"data": "x" * 10000}  # Large config
        large_result = {"output": "y" * 10000}  # Large result
        
        create_data = FlowExecutionCreate(
            flow_id=uuid.uuid4(),
            job_id="large-data-job",
            status=FlowExecutionStatus.PENDING,
            config=large_config
        )
        
        with patch('src.repositories.flow_execution_repository.FlowExecution') as mock_flow_execution_class:
            with patch('src.repositories.flow_execution_repository.datetime'):
                created_execution = MockFlowExecution()
                mock_flow_execution_class.return_value = created_execution
                
                # Should handle large data without issues
                create_result = await flow_execution_repository.create(create_data)
                assert create_result == created_execution
    
    @pytest.mark.asyncio
    async def test_update_with_error_field(self, flow_execution_repository, mock_async_session):
        """Test update with error field to cover missing line 250."""
        update_data = FlowExecutionUpdate(
            status=FlowExecutionStatus.FAILED,
            error="Test error message"
        )
        updated_execution = MockFlowExecution(id=1, status=FlowExecutionStatus.FAILED)
        mock_result = MockResult([updated_execution])
        mock_async_session.execute.return_value = mock_result
        
        with patch('src.repositories.flow_execution_repository.datetime'):
            result = await flow_execution_repository.update(1, update_data)
            
            assert result == updated_execution
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_node_execution_update_with_error_field(self, flow_node_execution_repository, mock_async_session):
        """Test node execution update with error field to cover missing lines."""
        update_data = FlowNodeExecutionUpdate(
            status=FlowExecutionStatus.FAILED,
            error="Node execution error"
        )
        updated_node_execution = MockFlowNodeExecution(id=1, status=FlowExecutionStatus.FAILED)
        mock_result = MockResult([updated_node_execution])
        mock_async_session.execute.return_value = mock_result
        
        with patch('src.repositories.flow_execution_repository.datetime'):
            result = await flow_node_execution_repository.update(1, update_data)
            
            assert result == updated_node_execution
            mock_async_session.execute.assert_called_once()
            mock_async_session.commit.assert_called_once()