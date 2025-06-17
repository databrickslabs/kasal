"""
Unit tests for ExecutionTraceRepository.

Tests the functionality of execution trace repository including
CRUD operations, job lookup, pagination, auto-creation of missing jobs, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import delete, func
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.execution_trace_repository import ExecutionTraceRepository
from src.models.execution_trace import ExecutionTrace
from src.models.execution_history import ExecutionHistory


# Mock execution trace model
class MockExecutionTrace:
    def __init__(self, id=1, run_id=1, job_id="job-123", event_type="start",
                 event_data=None, created_at=None, **kwargs):
        self.id = id
        self.run_id = run_id
        self.job_id = job_id
        self.event_type = event_type
        self.event_data = event_data or {}
        self.created_at = created_at or datetime.now(timezone.utc)
        for key, value in kwargs.items():
            setattr(self, key, value)


# Mock execution history model
class MockExecutionHistory:
    def __init__(self, id=1, job_id="job-123", status="running",
                 trigger_type="api", run_name="Test Run", inputs=None, **kwargs):
        self.id = id
        self.job_id = job_id
        self.status = status
        self.trigger_type = trigger_type
        self.run_name = run_name
        self.inputs = inputs or {}
        for key, value in kwargs.items():
            setattr(self, key, value)


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results=None, scalar_value=None, rowcount=0):
        self._scalars = MockScalars(results or [])
        self._scalar_value = scalar_value
        self.rowcount = rowcount
    
    def scalars(self):
        return self._scalars
    
    def scalar(self):
        return self._scalar_value


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def execution_trace_repository():
    """Create an execution trace repository."""
    return ExecutionTraceRepository()


@pytest.fixture
def sample_execution_traces():
    """Create sample execution traces for testing."""
    return [
        MockExecutionTrace(id=1, run_id=1, job_id="job-1", event_type="start"),
        MockExecutionTrace(id=2, run_id=1, job_id="job-1", event_type="step"),
        MockExecutionTrace(id=3, run_id=2, job_id="job-2", event_type="end")
    ]


@pytest.fixture
def sample_execution_history():
    """Create sample execution history for testing."""
    return [
        MockExecutionHistory(id=1, job_id="job-1", status="running"),
        MockExecutionHistory(id=2, job_id="job-2", status="completed")
    ]


@pytest.fixture
def sample_trace_data():
    """Create sample trace data for creation."""
    return {
        "job_id": "job-123",
        "run_id": 1,
        "event_type": "start",
        "event_data": {"step": "initialization"},
        "agent_name": "test_agent",
        "task_name": "test_task"
    }


class TestExecutionTraceRepositoryPrivateCreate:
    """Test cases for _create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, execution_trace_repository, mock_async_session, sample_trace_data):
        """Test successful trace creation with session."""
        with patch('src.repositories.execution_trace_repository.ExecutionTrace') as mock_trace_class:
            created_trace = MockExecutionTrace(**sample_trace_data)
            mock_trace_class.return_value = created_trace
            
            result = await execution_trace_repository._create(mock_async_session, sample_trace_data)
            
            assert result == created_trace
            mock_trace_class.assert_called_once_with(**sample_trace_data)
            mock_async_session.add.assert_called_once_with(created_trace)
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(created_trace)
    
    @pytest.mark.asyncio
    async def test_create_database_error(self, execution_trace_repository, mock_async_session, sample_trace_data):
        """Test trace creation with database error."""
        mock_async_session.commit.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.ExecutionTrace') as mock_trace_class:
            mock_trace_class.return_value = MockExecutionTrace()
            
            with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
                with pytest.raises(SQLAlchemyError, match="Database error"):
                    await execution_trace_repository._create(mock_async_session, sample_trace_data)
                
                mock_async_session.rollback.assert_called_once()
                mock_logger.error.assert_called()


class TestExecutionTraceRepositoryPrivateGetById:
    """Test cases for _get_by_id method."""
    
    @pytest.mark.asyncio
    async def test_get_by_id_success(self, execution_trace_repository, mock_async_session, sample_execution_traces):
        """Test successful retrieval by ID with session."""
        trace_id = 1
        target_trace = sample_execution_traces[0]
        
        mock_result = MockResult([target_trace])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_by_id(mock_async_session, trace_id)
        
        assert result == target_trace
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, execution_trace_repository, mock_async_session):
        """Test retrieval when trace not found."""
        trace_id = 999
        
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_by_id(mock_async_session, trace_id)
        
        assert result is None
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_id_database_error(self, execution_trace_repository, mock_async_session):
        """Test retrieval with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._get_by_id(mock_async_session, 1)
            
            mock_logger.error.assert_called()


class TestExecutionTraceRepositoryPrivateGetByRunId:
    """Test cases for _get_by_run_id method."""
    
    @pytest.mark.asyncio
    async def test_get_by_run_id_success(self, execution_trace_repository, mock_async_session, sample_execution_traces):
        """Test successful retrieval by run ID."""
        run_id = 1
        matching_traces = [trace for trace in sample_execution_traces if trace.run_id == run_id]
        
        mock_result = MockResult(matching_traces)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_by_run_id(mock_async_session, run_id)
        
        assert result == matching_traces
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_run_id_with_pagination(self, execution_trace_repository, mock_async_session):
        """Test retrieval by run ID with pagination."""
        run_id = 1
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_by_run_id(
            mock_async_session, run_id, limit=10, offset=5
        )
        
        assert result == []
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_run_id_no_pagination(self, execution_trace_repository, mock_async_session):
        """Test retrieval by run ID without pagination."""
        run_id = 1
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_by_run_id(
            mock_async_session, run_id, limit=None, offset=None
        )
        
        assert result == []
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_run_id_database_error(self, execution_trace_repository, mock_async_session):
        """Test retrieval by run ID with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._get_by_run_id(mock_async_session, 1)
            
            mock_logger.error.assert_called()


class TestExecutionTraceRepositoryPrivateGetByJobId:
    """Test cases for _get_by_job_id method."""
    
    @pytest.mark.asyncio
    async def test_get_by_job_id_success(self, execution_trace_repository, mock_async_session, sample_execution_traces):
        """Test successful retrieval by job ID."""
        job_id = "job-1"
        matching_traces = [trace for trace in sample_execution_traces if trace.job_id == job_id]
        
        mock_result = MockResult(matching_traces)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_by_job_id(mock_async_session, job_id)
        
        assert result == matching_traces
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_job_id_with_pagination(self, execution_trace_repository, mock_async_session):
        """Test retrieval by job ID with pagination."""
        job_id = "job-1"
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_by_job_id(
            mock_async_session, job_id, limit=20, offset=10
        )
        
        assert result == []
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_job_id_database_error(self, execution_trace_repository, mock_async_session):
        """Test retrieval by job ID with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._get_by_job_id(mock_async_session, "job-1")
            
            mock_logger.error.assert_called()


class TestExecutionTraceRepositoryPrivateGetAllTraces:
    """Test cases for _get_all_traces method."""
    
    @pytest.mark.asyncio
    async def test_get_all_traces_success(self, execution_trace_repository, mock_async_session, sample_execution_traces):
        """Test successful retrieval of all traces."""
        # Mock traces query
        traces_result = MockResult(sample_execution_traces)
        # Mock count query
        count_result = MockResult(scalar_value=len(sample_execution_traces))
        
        mock_async_session.execute.side_effect = [traces_result, count_result]
        
        traces, total_count = await execution_trace_repository._get_all_traces(mock_async_session)
        
        assert traces == sample_execution_traces
        assert total_count == len(sample_execution_traces)
        assert mock_async_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_all_traces_with_pagination(self, execution_trace_repository, mock_async_session):
        """Test retrieval of all traces with pagination."""
        traces_result = MockResult([])
        count_result = MockResult(scalar_value=0)
        
        mock_async_session.execute.side_effect = [traces_result, count_result]
        
        traces, total_count = await execution_trace_repository._get_all_traces(
            mock_async_session, limit=50, offset=25
        )
        
        assert traces == []
        assert total_count == 0
        assert mock_async_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_all_traces_none_count(self, execution_trace_repository, mock_async_session):
        """Test retrieval when count returns None."""
        traces_result = MockResult([])
        count_result = MockResult(scalar_value=None)
        
        mock_async_session.execute.side_effect = [traces_result, count_result]
        
        traces, total_count = await execution_trace_repository._get_all_traces(mock_async_session)
        
        assert traces == []
        assert total_count == 0  # Should default to 0
    
    @pytest.mark.asyncio
    async def test_get_all_traces_database_error(self, execution_trace_repository, mock_async_session):
        """Test retrieval of all traces with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._get_all_traces(mock_async_session)
            
            mock_logger.error.assert_called()


class TestExecutionTraceRepositoryPrivateJobLookup:
    """Test cases for job ID/run ID lookup methods."""
    
    @pytest.mark.asyncio
    async def test_get_execution_job_id_by_run_id_success(self, execution_trace_repository, mock_async_session):
        """Test successful job ID lookup by run ID."""
        run_id = 1
        job_id = "job-123"
        
        mock_result = MockResult(scalar_value=job_id)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_execution_job_id_by_run_id(mock_async_session, run_id)
        
        assert result == job_id
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_execution_job_id_by_run_id_not_found(self, execution_trace_repository, mock_async_session):
        """Test job ID lookup when run ID not found."""
        run_id = 999
        
        mock_result = MockResult(scalar_value=None)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_execution_job_id_by_run_id(mock_async_session, run_id)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_execution_run_id_by_job_id_success(self, execution_trace_repository, mock_async_session):
        """Test successful run ID lookup by job ID."""
        job_id = "job-123"
        run_id = 1
        
        mock_result = MockResult(scalar_value=run_id)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_execution_run_id_by_job_id(mock_async_session, job_id)
        
        assert result == run_id
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_execution_run_id_by_job_id_not_found(self, execution_trace_repository, mock_async_session):
        """Test run ID lookup when job ID not found."""
        job_id = "nonexistent"
        
        mock_result = MockResult(scalar_value=None)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._get_execution_run_id_by_job_id(mock_async_session, job_id)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_execution_job_id_by_run_id_database_error(self, execution_trace_repository, mock_async_session):
        """Test job ID lookup with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._get_execution_job_id_by_run_id(mock_async_session, 1)
            
            mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_execution_run_id_by_job_id_database_error(self, execution_trace_repository, mock_async_session):
        """Test run ID lookup with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._get_execution_run_id_by_job_id(mock_async_session, "job-1")
            
            mock_logger.error.assert_called()


class TestExecutionTraceRepositoryPrivateDelete:
    """Test cases for private delete methods."""
    
    @pytest.mark.asyncio
    async def test_delete_by_id_success(self, execution_trace_repository, mock_async_session):
        """Test successful deletion by ID."""
        trace_id = 1
        
        mock_result = MockResult(rowcount=1)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._delete_by_id(mock_async_session, trace_id)
        
        assert result == 1
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_by_id_not_found(self, execution_trace_repository, mock_async_session):
        """Test deletion when trace not found."""
        trace_id = 999
        
        mock_result = MockResult(rowcount=0)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._delete_by_id(mock_async_session, trace_id)
        
        assert result == 0
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_by_run_id_success(self, execution_trace_repository, mock_async_session):
        """Test successful deletion by run ID."""
        run_id = 1
        
        mock_result = MockResult(rowcount=3)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._delete_by_run_id(mock_async_session, run_id)
        
        assert result == 3
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_by_job_id_success(self, execution_trace_repository, mock_async_session):
        """Test successful deletion by job ID."""
        job_id = "job-123"
        
        mock_result = MockResult(rowcount=5)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._delete_by_job_id(mock_async_session, job_id)
        
        assert result == 5
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_all_success(self, execution_trace_repository, mock_async_session):
        """Test successful deletion of all traces."""
        mock_result = MockResult(rowcount=100)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_trace_repository._delete_all(mock_async_session)
        
        assert result == 100
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_by_id_database_error(self, execution_trace_repository, mock_async_session):
        """Test deletion by ID with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._delete_by_id(mock_async_session, 1)
            
            mock_async_session.rollback.assert_called_once()
            mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_delete_by_run_id_database_error(self, execution_trace_repository, mock_async_session):
        """Test deletion by run ID with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._delete_by_run_id(mock_async_session, 1)
            
            mock_async_session.rollback.assert_called_once()
            mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_delete_by_job_id_database_error(self, execution_trace_repository, mock_async_session):
        """Test deletion by job ID with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._delete_by_job_id(mock_async_session, "job-1")
            
            mock_async_session.rollback.assert_called_once()
            mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_delete_all_database_error(self, execution_trace_repository, mock_async_session):
        """Test delete all with database error."""
        mock_async_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
            with pytest.raises(SQLAlchemyError, match="Database error"):
                await execution_trace_repository._delete_all(mock_async_session)
            
            mock_async_session.rollback.assert_called_once()
            mock_logger.error.assert_called()


class TestExecutionTraceRepositoryCreate:
    """Test cases for public create method."""
    
    @pytest.mark.asyncio
    async def test_create_with_existing_job(self, execution_trace_repository, sample_trace_data):
        """Test creation when job already exists."""
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock existing job
            existing_job = MockExecutionHistory(id=1, job_id="job-123")
            job_result = MockResult([existing_job])
            mock_session.execute.return_value = job_result
            
            created_trace = MockExecutionTrace(**sample_trace_data)
            with patch.object(execution_trace_repository, '_create', return_value=created_trace) as mock_create:
                result = await execution_trace_repository.create(sample_trace_data)
                
                assert result == created_trace
                # Should not create job record
                mock_session.add.assert_not_called()
                mock_create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_with_missing_job_auto_creation(self, execution_trace_repository, sample_trace_data):
        """Test creation with auto-creation of missing job."""
        trace_data_without_run_id = {k: v for k, v in sample_trace_data.items() if k != "run_id"}
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock the select to avoid the model error - need two execute calls
            job_result = MockResult([])  # No job found
            mock_session.execute.side_effect = [job_result]  # For job lookup
            
            # Mock created job record
            created_job = MockExecutionHistory(id=123, job_id="job-123")
            with patch('src.repositories.execution_trace_repository.ExecutionHistory', return_value=created_job):
                with patch('src.repositories.execution_trace_repository.select') as mock_select:
                    mock_select.return_value.where.return_value = "mocked_stmt"
                    
                    created_trace = MockExecutionTrace(**sample_trace_data)
                    with patch.object(execution_trace_repository, '_create', return_value=created_trace) as mock_create:
                        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
                            result = await execution_trace_repository.create(trace_data_without_run_id)
                            
                            assert result == created_trace
                            mock_session.add.assert_called_once_with(created_job)
                            mock_session.flush.assert_called_once()
                            mock_logger.info.assert_called()
                            
                            # Verify run_id was added to trace_data
                            create_call_args = mock_create.call_args[0][1]
                            assert create_call_args["run_id"] == 123
    
    @pytest.mark.asyncio
    async def test_create_with_existing_job_missing_run_id(self, execution_trace_repository):
        """Test creation when job exists but trace_data lacks run_id."""
        trace_data = {"job_id": "job-123", "event_type": "start"}
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock existing job
            existing_job = MockExecutionHistory(id=456, job_id="job-123")
            job_result = MockResult([existing_job])
            mock_session.execute.return_value = job_result
            
            created_trace = MockExecutionTrace(**trace_data)
            with patch.object(execution_trace_repository, '_create', return_value=created_trace) as mock_create:
                with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
                    result = await execution_trace_repository.create(trace_data)
                    
                    assert result == created_trace
                    mock_logger.info.assert_called()
                    
                    # Verify run_id was added to trace_data
                    create_call_args = mock_create.call_args[0][1]
                    assert create_call_args["run_id"] == 456
    
    @pytest.mark.asyncio
    async def test_create_without_job_id(self, execution_trace_repository):
        """Test creation without job_id in trace_data."""
        trace_data = {"event_type": "start", "event_data": {}}
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            created_trace = MockExecutionTrace(**trace_data)
            with patch.object(execution_trace_repository, '_create', return_value=created_trace) as mock_create:
                result = await execution_trace_repository.create(trace_data)
                
                assert result == created_trace
                # Should not try to check for job existence
                mock_session.execute.assert_not_called()
                mock_create.assert_called_once_with(mock_session, trace_data)


class TestExecutionTraceRepositoryPublicMethods:
    """Test cases for public methods that manage their own sessions."""
    
    @pytest.mark.asyncio
    async def test_get_by_id_public(self, execution_trace_repository, sample_execution_traces):
        """Test public get by ID method."""
        trace_id = 1
        target_trace = sample_execution_traces[0]
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_get_by_id', return_value=target_trace) as mock_get:
                result = await execution_trace_repository.get_by_id(trace_id)
                
                assert result == target_trace
                mock_get.assert_called_once_with(mock_session, trace_id)
    
    @pytest.mark.asyncio
    async def test_get_by_run_id_public(self, execution_trace_repository, sample_execution_traces):
        """Test public get by run ID method."""
        run_id = 1
        matching_traces = [trace for trace in sample_execution_traces if trace.run_id == run_id]
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_get_by_run_id', return_value=matching_traces) as mock_get:
                result = await execution_trace_repository.get_by_run_id(run_id, limit=10, offset=5)
                
                assert result == matching_traces
                mock_get.assert_called_once_with(mock_session, run_id, 10, 5)
    
    @pytest.mark.asyncio
    async def test_get_by_job_id_public(self, execution_trace_repository, sample_execution_traces):
        """Test public get by job ID method."""
        job_id = "job-1"
        matching_traces = [trace for trace in sample_execution_traces if trace.job_id == job_id]
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_get_by_job_id', return_value=matching_traces) as mock_get:
                result = await execution_trace_repository.get_by_job_id(job_id, limit=20, offset=10)
                
                assert result == matching_traces
                mock_get.assert_called_once_with(mock_session, job_id, 20, 10)
    
    @pytest.mark.asyncio
    async def test_get_all_traces_public(self, execution_trace_repository, sample_execution_traces):
        """Test public get all traces method."""
        total_count = len(sample_execution_traces)
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_get_all_traces', 
                            return_value=(sample_execution_traces, total_count)) as mock_get:
                traces, count = await execution_trace_repository.get_all_traces(limit=50, offset=25)
                
                assert traces == sample_execution_traces
                assert count == total_count
                mock_get.assert_called_once_with(mock_session, 50, 25)
    
    @pytest.mark.asyncio
    async def test_get_execution_job_id_by_run_id_public(self, execution_trace_repository):
        """Test public get job ID by run ID method."""
        run_id = 1
        job_id = "job-123"
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_get_execution_job_id_by_run_id', 
                            return_value=job_id) as mock_get:
                result = await execution_trace_repository.get_execution_job_id_by_run_id(run_id)
                
                assert result == job_id
                mock_get.assert_called_once_with(mock_session, run_id)
    
    @pytest.mark.asyncio
    async def test_get_execution_run_id_by_job_id_public(self, execution_trace_repository):
        """Test public get run ID by job ID method."""
        job_id = "job-123"
        run_id = 1
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_get_execution_run_id_by_job_id', 
                            return_value=run_id) as mock_get:
                result = await execution_trace_repository.get_execution_run_id_by_job_id(job_id)
                
                assert result == run_id
                mock_get.assert_called_once_with(mock_session, job_id)


class TestExecutionTraceRepositoryPublicDeleteMethods:
    """Test cases for public delete methods."""
    
    @pytest.mark.asyncio
    async def test_delete_by_id_public(self, execution_trace_repository):
        """Test public delete by ID method."""
        trace_id = 1
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_delete_by_id', return_value=1) as mock_delete:
                result = await execution_trace_repository.delete_by_id(trace_id)
                
                assert result == 1
                mock_delete.assert_called_once_with(mock_session, trace_id)
    
    @pytest.mark.asyncio
    async def test_delete_by_run_id_public(self, execution_trace_repository):
        """Test public delete by run ID method."""
        run_id = 1
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_delete_by_run_id', return_value=3) as mock_delete:
                result = await execution_trace_repository.delete_by_run_id(run_id)
                
                assert result == 3
                mock_delete.assert_called_once_with(mock_session, run_id)
    
    @pytest.mark.asyncio
    async def test_delete_by_job_id_public(self, execution_trace_repository):
        """Test public delete by job ID method."""
        job_id = "job-123"
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_delete_by_job_id', return_value=5) as mock_delete:
                result = await execution_trace_repository.delete_by_job_id(job_id)
                
                assert result == 5
                mock_delete.assert_called_once_with(mock_session, job_id)
    
    @pytest.mark.asyncio
    async def test_delete_all_public(self, execution_trace_repository):
        """Test public delete all method."""
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_delete_all', return_value=100) as mock_delete:
                result = await execution_trace_repository.delete_all()
                
                assert result == 100
                mock_delete.assert_called_once_with(mock_session)


class TestExecutionTraceRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_get_workflow(self, execution_trace_repository, sample_trace_data):
        """Test workflow of creating then retrieving traces."""
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock job exists
            existing_job = MockExecutionHistory(id=1, job_id="job-123")
            job_result = MockResult([existing_job])
            mock_session.execute.return_value = job_result
            
            # Create trace
            created_trace = MockExecutionTrace(**sample_trace_data)
            with patch.object(execution_trace_repository, '_create', return_value=created_trace):
                create_result = await execution_trace_repository.create(sample_trace_data)
                assert create_result == created_trace
            
            # Get trace
            with patch.object(execution_trace_repository, '_get_by_id', return_value=created_trace):
                get_result = await execution_trace_repository.get_by_id(1)
                assert get_result == created_trace
    
    @pytest.mark.asyncio
    async def test_create_get_delete_workflow(self, execution_trace_repository, sample_trace_data):
        """Test complete workflow of create, get, then delete."""
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock job exists
            existing_job = MockExecutionHistory(id=1, job_id="job-123")
            job_result = MockResult([existing_job])
            mock_session.execute.return_value = job_result
            
            # Create trace
            created_trace = MockExecutionTrace(id=1, **sample_trace_data)
            with patch.object(execution_trace_repository, '_create', return_value=created_trace):
                create_result = await execution_trace_repository.create(sample_trace_data)
                assert create_result.id == 1
            
            # Get traces by job ID
            with patch.object(execution_trace_repository, '_get_by_job_id', return_value=[created_trace]):
                get_result = await execution_trace_repository.get_by_job_id("job-123")
                assert len(get_result) == 1
                assert get_result[0].id == 1
            
            # Delete trace
            with patch.object(execution_trace_repository, '_delete_by_id', return_value=1):
                delete_result = await execution_trace_repository.delete_by_id(1)
                assert delete_result == 1


class TestExecutionTraceRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_create_job_lookup_error(self, execution_trace_repository, sample_trace_data):
        """Test creation when job lookup fails."""
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            mock_session.execute.side_effect = SQLAlchemyError("Job lookup failed")
            
            with pytest.raises(SQLAlchemyError, match="Job lookup failed"):
                await execution_trace_repository.create(sample_trace_data)
    
    @pytest.mark.asyncio
    async def test_public_method_session_error(self, execution_trace_repository):
        """Test public method when session factory fails."""
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_factory.side_effect = Exception("Session factory failed")
            
            with pytest.raises(Exception, match="Session factory failed"):
                await execution_trace_repository.get_by_id(1)


class TestExecutionTraceRepositorySingleton:
    """Test cases for singleton instance."""
    
    def test_singleton_instance_creation(self):
        """Test that singleton instance is created correctly."""
        from src.repositories.execution_trace_repository import execution_trace_repository
        
        assert execution_trace_repository is not None
        assert isinstance(execution_trace_repository, ExecutionTraceRepository)


class TestExecutionTraceRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_create_with_empty_trace_data(self, execution_trace_repository):
        """Test creation with minimal trace data."""
        trace_data = {}
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            created_trace = MockExecutionTrace(**trace_data)
            with patch.object(execution_trace_repository, '_create', return_value=created_trace):
                result = await execution_trace_repository.create(trace_data)
                
                assert result == created_trace
                # Should not try to check for job existence since no job_id
                mock_session.execute.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_by_run_id_with_zero_limit(self, execution_trace_repository):
        """Test get by run ID with zero limit."""
        run_id = 1
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_get_by_run_id', return_value=[]) as mock_get:
                result = await execution_trace_repository.get_by_run_id(run_id, limit=0)
                
                assert result == []
                mock_get.assert_called_once_with(mock_session, run_id, 0, 0)
    
    @pytest.mark.asyncio
    async def test_delete_by_job_id_with_special_characters(self, execution_trace_repository):
        """Test deletion with job ID containing special characters."""
        job_id = "job-with-special-chars!@#$%"
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_trace_repository, '_delete_by_job_id', return_value=2) as mock_delete:
                result = await execution_trace_repository.delete_by_job_id(job_id)
                
                assert result == 2
                mock_delete.assert_called_once_with(mock_session, job_id)
    
    @pytest.mark.asyncio
    async def test_create_auto_creation_with_existing_run_id(self, execution_trace_repository):
        """Test creation with auto job creation when run_id already exists in trace_data."""
        trace_data = {"job_id": "job-123", "run_id": 999, "event_type": "start"}
        
        with patch('src.repositories.execution_trace_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock job not found
            job_result = MockResult([])
            mock_session.execute.side_effect = [job_result]
            
            # Mock created job record
            created_job = MockExecutionHistory(id=123, job_id="job-123")
            with patch('src.repositories.execution_trace_repository.ExecutionHistory', return_value=created_job):
                with patch('src.repositories.execution_trace_repository.select') as mock_select:
                    mock_select.return_value.where.return_value = "mocked_stmt"
                    
                    created_trace = MockExecutionTrace(**trace_data)
                    with patch.object(execution_trace_repository, '_create', return_value=created_trace) as mock_create:
                        with patch('src.repositories.execution_trace_repository.logger') as mock_logger:
                            result = await execution_trace_repository.create(trace_data)
                            
                            assert result == created_trace
                            # Should NOT override existing run_id
                            create_call_args = mock_create.call_args[0][1]
                            assert create_call_args["run_id"] == 999  # Original value preserved
                            mock_logger.info.assert_called()  # For job creation logging