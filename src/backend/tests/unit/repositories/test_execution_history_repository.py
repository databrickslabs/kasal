"""
Unit tests for ExecutionHistoryRepository.

Tests the functionality of execution history repository including
pagination, group filtering, deletion operations, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, UTC
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, delete

from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.models.execution_history import ExecutionHistory, TaskStatus, ErrorTrace


# Mock execution history model
class MockExecutionHistory:
    def __init__(self, id=1, job_id="job-123", group_id="group-1", 
                 created_at=None, **kwargs):
        self.id = id
        self.job_id = job_id
        self.group_id = group_id
        self.created_at = created_at or datetime.now(UTC)
        for key, value in kwargs.items():
            setattr(self, key, value)


# Mock task status model
class MockTaskStatus:
    def __init__(self, id=1, job_id="job-123", status="PENDING", **kwargs):
        self.id = id
        self.job_id = job_id
        self.status = status
        for key, value in kwargs.items():
            setattr(self, key, value)


# Mock error trace model
class MockErrorTrace:
    def __init__(self, id=1, run_id=1, error="Test error", **kwargs):
        self.id = id
        self.run_id = run_id
        self.error = error
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
    def __init__(self, results=None, scalar_value=None):
        self._scalars = MockScalars(results or [])
        self._scalar_value = scalar_value
    
    def scalars(self):
        return self._scalars
    
    def scalar(self):
        return self._scalar_value


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def execution_history_repository():
    """Create an execution history repository."""
    return ExecutionHistoryRepository()


@pytest.fixture
def execution_history_repository_with_session(mock_async_session):
    """Create an execution history repository with session."""
    return ExecutionHistoryRepository(session=mock_async_session)


@pytest.fixture
def sample_executions():
    """Create sample execution history records for testing."""
    return [
        MockExecutionHistory(id=1, job_id="job-1", group_id="group-1"),
        MockExecutionHistory(id=2, job_id="job-2", group_id="group-1"),
        MockExecutionHistory(id=3, job_id="job-3", group_id="group-2")
    ]


class TestExecutionHistoryRepositoryInit:
    """Test cases for ExecutionHistoryRepository initialization."""
    
    def test_init_without_session(self):
        """Test initialization without session."""
        repository = ExecutionHistoryRepository()
        assert repository.session is None
    
    def test_init_with_session(self, mock_async_session):
        """Test initialization with session."""
        repository = ExecutionHistoryRepository(session=mock_async_session)
        assert repository.session == mock_async_session


class TestExecutionHistoryRepositoryGetExecutionHistory:
    """Test cases for get_execution_history method."""
    
    @pytest.mark.asyncio
    async def test_get_execution_history_success(self, execution_history_repository, sample_executions):
        """Test successful retrieval of execution history."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock count query
            count_result = MockResult(scalar_value=3)
            # Mock data query
            data_result = MockResult(results=sample_executions)
            
            mock_session.execute.side_effect = [count_result, data_result]
            
            executions, total_count = await execution_history_repository.get_execution_history(
                limit=10, offset=0
            )
            
            assert len(executions) == 3
            assert total_count == 3
            assert executions == sample_executions
            assert mock_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_execution_history_with_group_filtering(self, execution_history_repository, sample_executions):
        """Test execution history retrieval with group filtering."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Filter to group-1 only (2 executions)
            filtered_executions = [exec for exec in sample_executions if exec.group_id == "group-1"]
            
            count_result = MockResult(scalar_value=2)
            data_result = MockResult(results=filtered_executions)
            
            mock_session.execute.side_effect = [count_result, data_result]
            
            executions, total_count = await execution_history_repository.get_execution_history(
                limit=10, offset=0, group_ids=["group-1"]
            )
            
            assert len(executions) == 2
            assert total_count == 2
            assert all(exec.group_id == "group-1" for exec in executions)
    
    @pytest.mark.asyncio
    async def test_get_execution_history_with_pagination(self, execution_history_repository, sample_executions):
        """Test execution history retrieval with pagination."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Simulate page 2 with limit 1
            paginated_executions = [sample_executions[1]]
            
            count_result = MockResult(scalar_value=3)
            data_result = MockResult(results=paginated_executions)
            
            mock_session.execute.side_effect = [count_result, data_result]
            
            executions, total_count = await execution_history_repository.get_execution_history(
                limit=1, offset=1
            )
            
            assert len(executions) == 1
            assert total_count == 3
            assert executions[0].id == 2
    
    @pytest.mark.asyncio
    async def test_get_execution_history_empty_result(self, execution_history_repository):
        """Test execution history retrieval with empty result."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            count_result = MockResult(scalar_value=0)
            data_result = MockResult(results=[])
            
            mock_session.execute.side_effect = [count_result, data_result]
            
            executions, total_count = await execution_history_repository.get_execution_history()
            
            assert len(executions) == 0
            assert total_count == 0
    
    @pytest.mark.asyncio
    async def test_get_execution_history_empty_group_ids(self, execution_history_repository, sample_executions):
        """Test execution history retrieval with empty group IDs list."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            count_result = MockResult(scalar_value=3)
            data_result = MockResult(results=sample_executions)
            
            mock_session.execute.side_effect = [count_result, data_result]
            
            executions, total_count = await execution_history_repository.get_execution_history(
                group_ids=[]
            )
            
            assert len(executions) == 3
            assert total_count == 3


class TestExecutionHistoryRepositoryGetExecutionById:
    """Test cases for get_execution_by_id method."""
    
    @pytest.mark.asyncio
    async def test_get_execution_by_id_success(self, execution_history_repository, sample_executions):
        """Test successful retrieval of execution by ID."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            result = MockResult(results=[execution])
            mock_session.execute.return_value = result
            
            found_execution = await execution_history_repository.get_execution_by_id(1)
            
            assert found_execution == execution
            mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_execution_by_id_with_group_filtering(self, execution_history_repository, sample_executions):
        """Test execution retrieval by ID with group filtering."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            result = MockResult(results=[execution])
            mock_session.execute.return_value = result
            
            found_execution = await execution_history_repository.get_execution_by_id(
                1, group_ids=["group-1"]
            )
            
            assert found_execution == execution
            mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_execution_by_id_not_found(self, execution_history_repository):
        """Test execution retrieval by ID when not found."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            result = MockResult(results=[])
            mock_session.execute.return_value = result
            
            found_execution = await execution_history_repository.get_execution_by_id(999)
            
            assert found_execution is None
    
    @pytest.mark.asyncio
    async def test_get_execution_by_id_group_filtering_blocks_access(self, execution_history_repository):
        """Test execution retrieval blocked by group filtering."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            result = MockResult(results=[])
            mock_session.execute.return_value = result
            
            found_execution = await execution_history_repository.get_execution_by_id(
                1, group_ids=["different-group"]
            )
            
            assert found_execution is None


class TestExecutionHistoryRepositoryGetExecutionByJobId:
    """Test cases for get_execution_by_job_id method."""
    
    @pytest.mark.asyncio
    async def test_get_execution_by_job_id_success(self, execution_history_repository, sample_executions):
        """Test successful retrieval of execution by job ID."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            result = MockResult(results=[execution])
            mock_session.execute.return_value = result
            
            found_execution = await execution_history_repository.get_execution_by_job_id("job-1")
            
            assert found_execution == execution
            mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_execution_by_job_id_with_group_filtering(self, execution_history_repository, sample_executions):
        """Test execution retrieval by job ID with group filtering."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            result = MockResult(results=[execution])
            mock_session.execute.return_value = result
            
            found_execution = await execution_history_repository.get_execution_by_job_id(
                "job-1", group_ids=["group-1"]
            )
            
            assert found_execution == execution
    
    @pytest.mark.asyncio
    async def test_get_execution_by_job_id_not_found(self, execution_history_repository):
        """Test execution retrieval by job ID when not found."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            result = MockResult(results=[])
            mock_session.execute.return_value = result
            
            found_execution = await execution_history_repository.get_execution_by_job_id("nonexistent")
            
            assert found_execution is None


class TestExecutionHistoryRepositoryFindById:
    """Test cases for find_by_id method."""
    
    @pytest.mark.asyncio
    async def test_find_by_id_with_session(self, execution_history_repository_with_session, mock_async_session, sample_executions):
        """Test find by ID using repository's session."""
        execution = sample_executions[0]
        result = MockResult(results=[execution])
        mock_async_session.execute.return_value = result
        
        found_execution = await execution_history_repository_with_session.find_by_id(1)
        
        assert found_execution == execution
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_id_without_session(self, execution_history_repository, sample_executions):
        """Test find by ID creating new session."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            result = MockResult(results=[execution])
            mock_session.execute.return_value = result
            
            found_execution = await execution_history_repository.find_by_id(1)
            
            assert found_execution == execution
    
    @pytest.mark.asyncio
    async def test_find_by_id_not_found(self, execution_history_repository_with_session, mock_async_session):
        """Test find by ID when execution not found."""
        result = MockResult(results=[])
        mock_async_session.execute.return_value = result
        
        found_execution = await execution_history_repository_with_session.find_by_id(999)
        
        assert found_execution is None


class TestExecutionHistoryRepositoryCheckExecutionExists:
    """Test cases for check_execution_exists method."""
    
    @pytest.mark.asyncio
    async def test_check_execution_exists_true(self, execution_history_repository):
        """Test checking execution that exists."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            result = MockResult(scalar_value=1)
            mock_session.execute.return_value = result
            
            exists = await execution_history_repository.check_execution_exists(1)
            
            assert exists is True
            mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_execution_exists_false(self, execution_history_repository):
        """Test checking execution that doesn't exist."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            result = MockResult(scalar_value=0)
            mock_session.execute.return_value = result
            
            exists = await execution_history_repository.check_execution_exists(999)
            
            assert exists is False
    
    @pytest.mark.asyncio
    async def test_check_execution_exists_none_scalar(self, execution_history_repository):
        """Test checking execution when scalar returns None."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            result = MockResult(scalar_value=None)
            mock_session.execute.return_value = result
            
            exists = await execution_history_repository.check_execution_exists(1)
            
            assert exists is False


class TestExecutionHistoryRepositoryDeleteExecution:
    """Test cases for delete_execution method."""
    
    @pytest.mark.asyncio
    async def test_delete_execution_success(self, execution_history_repository, sample_executions):
        """Test successful execution deletion."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            
            # Mock _get_execution_by_id_internal
            with patch.object(execution_history_repository, '_get_execution_by_id_internal', 
                            return_value=execution):
                # Mock delete results
                task_status_result = MagicMock()
                task_status_result.rowcount = 2
                error_trace_result = MagicMock()
                error_trace_result.rowcount = 1
                
                mock_session.execute.side_effect = [
                    task_status_result,  # Delete task statuses
                    error_trace_result,  # Delete error traces  
                    MagicMock()         # Delete execution
                ]
                
                result = await execution_history_repository.delete_execution(1)
                
                assert result == {
                    'execution_id': 1,
                    'job_id': 'job-1',
                    'task_status_count': 2,
                    'error_trace_count': 1
                }
                assert mock_session.execute.call_count == 3
                mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_execution_not_found(self, execution_history_repository):
        """Test deletion when execution not found."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_history_repository, '_get_execution_by_id_internal', 
                            return_value=None):
                result = await execution_history_repository.delete_execution(999)
                
                assert result is None
                mock_session.execute.assert_not_called()
                mock_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_execution_database_error(self, execution_history_repository, sample_executions):
        """Test deletion with database error."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            
            with patch.object(execution_history_repository, '_get_execution_by_id_internal', 
                            return_value=execution):
                mock_session.execute.side_effect = Exception("Database error")
                
                with pytest.raises(Exception, match="Database error"):
                    await execution_history_repository.delete_execution(1)
                
                mock_session.rollback.assert_called_once()


class TestExecutionHistoryRepositoryDeleteExecutionByJobId:
    """Test cases for delete_execution_by_job_id method."""
    
    @pytest.mark.asyncio
    async def test_delete_execution_by_job_id_success(self, execution_history_repository, sample_executions):
        """Test successful execution deletion by job ID."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            
            with patch.object(execution_history_repository, '_get_execution_by_job_id_internal', 
                            return_value=execution):
                # Mock delete results
                task_status_result = MagicMock()
                task_status_result.rowcount = 3
                error_trace_result = MagicMock()
                error_trace_result.rowcount = 2
                
                mock_session.execute.side_effect = [
                    task_status_result,  # Delete task statuses
                    error_trace_result,  # Delete error traces
                    MagicMock()         # Delete execution
                ]
                
                result = await execution_history_repository.delete_execution_by_job_id("job-1")
                
                assert result == {
                    'execution_id': 1,
                    'job_id': 'job-1',
                    'task_status_count': 3,
                    'error_trace_count': 2
                }
                assert mock_session.execute.call_count == 3
                mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_execution_by_job_id_not_found(self, execution_history_repository):
        """Test deletion by job ID when execution not found."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_history_repository, '_get_execution_by_job_id_internal', 
                            return_value=None):
                result = await execution_history_repository.delete_execution_by_job_id("nonexistent")
                
                assert result is None
                mock_session.execute.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_execution_by_job_id_database_error(self, execution_history_repository, sample_executions):
        """Test deletion by job ID with database error."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            
            with patch.object(execution_history_repository, '_get_execution_by_job_id_internal', 
                            return_value=execution):
                mock_session.execute.side_effect = Exception("Database error")
                
                with pytest.raises(Exception, match="Database error"):
                    await execution_history_repository.delete_execution_by_job_id("job-1")
                
                mock_session.rollback.assert_called_once()


class TestExecutionHistoryRepositoryDeleteAllExecutions:
    """Test cases for delete_all_executions method."""
    
    @pytest.mark.asyncio
    async def test_delete_all_executions_success(self, execution_history_repository):
        """Test successful deletion of all executions."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock delete results
            task_status_result = MagicMock()
            task_status_result.rowcount = 10
            error_trace_result = MagicMock()
            error_trace_result.rowcount = 5
            count_result = MockResult(scalar_value=3)
            
            mock_session.execute.side_effect = [
                task_status_result,  # Delete task statuses
                error_trace_result,  # Delete error traces
                count_result,        # Count executions
                MagicMock()         # Delete executions
            ]
            
            result = await execution_history_repository.delete_all_executions()
            
            assert result == {
                'run_count': 3,
                'task_status_count': 10,
                'error_trace_count': 5
            }
            assert mock_session.execute.call_count == 4
            mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_all_executions_empty_database(self, execution_history_repository):
        """Test deletion when database is empty."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock empty results
            task_status_result = MagicMock()
            task_status_result.rowcount = 0
            error_trace_result = MagicMock()
            error_trace_result.rowcount = 0
            count_result = MockResult(scalar_value=0)
            
            mock_session.execute.side_effect = [
                task_status_result,
                error_trace_result,
                count_result,
                MagicMock()
            ]
            
            result = await execution_history_repository.delete_all_executions()
            
            assert result == {
                'run_count': 0,
                'task_status_count': 0,
                'error_trace_count': 0
            }
    
    @pytest.mark.asyncio
    async def test_delete_all_executions_none_count(self, execution_history_repository):
        """Test deletion when count returns None."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            task_status_result = MagicMock()
            task_status_result.rowcount = 5
            error_trace_result = MagicMock()
            error_trace_result.rowcount = 2
            count_result = MockResult(scalar_value=None)
            
            mock_session.execute.side_effect = [
                task_status_result,
                error_trace_result,
                count_result,
                MagicMock()
            ]
            
            result = await execution_history_repository.delete_all_executions()
            
            assert result == {
                'run_count': 0,  # Should default to 0 when None
                'task_status_count': 5,
                'error_trace_count': 2
            }
    
    @pytest.mark.asyncio
    async def test_delete_all_executions_database_error(self, execution_history_repository):
        """Test deletion with database error."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            mock_session.execute.side_effect = Exception("Database error")
            
            with pytest.raises(Exception, match="Database error"):
                await execution_history_repository.delete_all_executions()
            
            mock_session.rollback.assert_called_once()


class TestExecutionHistoryRepositoryInternalMethods:
    """Test cases for internal helper methods."""
    
    @pytest.mark.asyncio
    async def test_get_execution_by_id_internal(self, execution_history_repository, mock_async_session, sample_executions):
        """Test internal method for getting execution by ID."""
        execution = sample_executions[0]
        result = MockResult(results=[execution])
        mock_async_session.execute.return_value = result
        
        found_execution = await execution_history_repository._get_execution_by_id_internal(
            mock_async_session, 1
        )
        
        assert found_execution == execution
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_execution_by_job_id_internal(self, execution_history_repository, mock_async_session, sample_executions):
        """Test internal method for getting execution by job ID."""
        execution = sample_executions[0]
        result = MockResult(results=[execution])
        mock_async_session.execute.return_value = result
        
        found_execution = await execution_history_repository._get_execution_by_job_id_internal(
            mock_async_session, "job-1"
        )
        
        assert found_execution == execution
        mock_async_session.execute.assert_called_once()


class TestExecutionHistoryRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_get_then_delete_execution_workflow(self, execution_history_repository, sample_executions):
        """Test workflow of getting then deleting execution."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            
            # Mock get execution
            get_result = MockResult(results=[execution])
            mock_session.execute.return_value = get_result
            
            # Get execution first
            found_execution = await execution_history_repository.get_execution_by_id(1)
            assert found_execution == execution
            
            # Reset mock for delete operation
            mock_session.reset_mock()
            
            # Mock delete operations
            with patch.object(execution_history_repository, '_get_execution_by_id_internal', 
                            return_value=execution):
                task_status_result = MagicMock()
                task_status_result.rowcount = 1
                error_trace_result = MagicMock()
                error_trace_result.rowcount = 0
                
                mock_session.execute.side_effect = [
                    task_status_result,
                    error_trace_result,
                    MagicMock()
                ]
                
                delete_result = await execution_history_repository.delete_execution(1)
                
                assert delete_result['execution_id'] == 1
                assert delete_result['job_id'] == 'job-1'
    
    @pytest.mark.asyncio
    async def test_check_exists_then_delete_workflow(self, execution_history_repository, sample_executions):
        """Test workflow of checking existence then deleting."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock existence check
            exists_result = MockResult(scalar_value=1)
            mock_session.execute.return_value = exists_result
            
            exists = await execution_history_repository.check_execution_exists(1)
            assert exists is True
            
            # Reset mock for delete
            mock_session.reset_mock()
            
            execution = sample_executions[0]
            with patch.object(execution_history_repository, '_get_execution_by_id_internal', 
                            return_value=execution):
                task_status_result = MagicMock()
                task_status_result.rowcount = 0
                error_trace_result = MagicMock()
                error_trace_result.rowcount = 0
                
                mock_session.execute.side_effect = [
                    task_status_result,
                    error_trace_result,
                    MagicMock()
                ]
                
                delete_result = await execution_history_repository.delete_execution(1)
                
                assert delete_result is not None
                assert delete_result['execution_id'] == 1


class TestExecutionHistoryRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_get_execution_history_database_error(self, execution_history_repository):
        """Test get execution history with database error."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            mock_session.execute.side_effect = Exception("Connection lost")
            
            with pytest.raises(Exception, match="Connection lost"):
                await execution_history_repository.get_execution_history()
    
    @pytest.mark.asyncio
    async def test_find_by_id_database_error(self, execution_history_repository_with_session, mock_async_session):
        """Test find by ID with database error."""
        mock_async_session.execute.side_effect = Exception("Query failed")
        
        with pytest.raises(Exception, match="Query failed"):
            await execution_history_repository_with_session.find_by_id(1)
    
    @pytest.mark.asyncio
    async def test_check_execution_exists_database_error(self, execution_history_repository):
        """Test check execution exists with database error."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            mock_session.execute.side_effect = Exception("Database timeout")
            
            with pytest.raises(Exception, match="Database timeout"):
                await execution_history_repository.check_execution_exists(1)


class TestExecutionHistoryRepositorySingleton:
    """Test cases for singleton instance."""
    
    def test_singleton_instance_creation(self):
        """Test that singleton instance is created correctly."""
        from src.repositories.execution_history_repository import execution_history_repository
        
        assert execution_history_repository is not None
        assert isinstance(execution_history_repository, ExecutionHistoryRepository)
        assert execution_history_repository.session is None


class TestExecutionHistoryRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_get_execution_history_large_offset(self, execution_history_repository):
        """Test execution history with very large offset."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            count_result = MockResult(scalar_value=10)
            data_result = MockResult(results=[])
            
            mock_session.execute.side_effect = [count_result, data_result]
            
            executions, total_count = await execution_history_repository.get_execution_history(
                limit=10, offset=1000
            )
            
            assert len(executions) == 0
            assert total_count == 10
    
    @pytest.mark.asyncio
    async def test_get_execution_history_zero_limit(self, execution_history_repository):
        """Test execution history with zero limit."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            count_result = MockResult(scalar_value=5)
            data_result = MockResult(results=[])
            
            mock_session.execute.side_effect = [count_result, data_result]
            
            executions, total_count = await execution_history_repository.get_execution_history(
                limit=0, offset=0
            )
            
            assert len(executions) == 0
            assert total_count == 5
    
    @pytest.mark.asyncio
    async def test_delete_execution_with_no_associated_data(self, execution_history_repository, sample_executions):
        """Test deleting execution with no associated task statuses or error traces."""
        with patch('src.repositories.execution_history_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            execution = sample_executions[0]
            
            with patch.object(execution_history_repository, '_get_execution_by_id_internal', 
                            return_value=execution):
                # Mock zero deletions for associated data
                task_status_result = MagicMock()
                task_status_result.rowcount = 0
                error_trace_result = MagicMock()
                error_trace_result.rowcount = 0
                
                mock_session.execute.side_effect = [
                    task_status_result,
                    error_trace_result,
                    MagicMock()
                ]
                
                result = await execution_history_repository.delete_execution(1)
                
                assert result == {
                    'execution_id': 1,
                    'job_id': 'job-1',
                    'task_status_count': 0,
                    'error_trace_count': 0
                }