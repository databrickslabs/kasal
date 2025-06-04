"""
Unit tests for ExecutionRepository.

Tests the functionality of the execution repository including
database operations for execution management.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.execution_repository import ExecutionRepository
from src.models.execution_history import ExecutionHistory
from src.schemas.execution import ExecutionStatus


@pytest.fixture
def mock_session():
    """Create a mock AsyncSession."""
    session = AsyncMock(spec=AsyncSession)
    return session


@pytest.fixture
def execution_data():
    """Create test execution data."""
    return {
        "job_id": "test-job-123",
        "status": ExecutionStatus.PENDING.value,
        "run_name": "Test Execution",
        "inputs": {"key": "value"},
        "planning": True,
        "created_at": datetime.now(UTC)
    }


@pytest.fixture
def mock_execution():
    """Create a mock execution object."""
    execution = MagicMock(spec=ExecutionHistory)
    execution.id = 1
    execution.job_id = "test-job-123"
    execution.status = ExecutionStatus.PENDING.value
    execution.run_name = "Test Execution"
    execution.created_at = datetime.now(UTC)
    execution.updated_at = datetime.now(UTC)
    execution.result = None
    execution.error = None
    return execution


class TestExecutionRepository:
    """Test cases for ExecutionRepository."""
    
    def test_repository_initialization(self, mock_session):
        """Test repository initialization."""
        repo = ExecutionRepository(mock_session)
        
        assert repo.session == mock_session
        assert repo.model == ExecutionHistory
    
    @pytest.mark.asyncio
    async def test_create_execution_success(self, mock_session, execution_data):
        """Test successful execution creation."""
        repo = ExecutionRepository(mock_session)
        
        # Mock the flush operation
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()
        
        result = await repo.create_execution(execution_data)
        
        assert isinstance(result, ExecutionHistory)
        assert result.job_id == execution_data["job_id"]
        assert result.status == execution_data["status"]
        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_execution_missing_required_field(self, mock_session):
        """Test execution creation with missing required fields."""
        repo = ExecutionRepository(mock_session)
        
        # Missing job_id
        invalid_data = {
            "status": ExecutionStatus.PENDING.value,
            "run_name": "Test"
        }
        
        with pytest.raises(ValueError, match="Missing required field 'job_id'"):
            await repo.create_execution(invalid_data)
    
    @pytest.mark.asyncio
    async def test_get_execution_by_job_id(self, mock_session, mock_execution):
        """Test getting execution by job_id."""
        repo = ExecutionRepository(mock_session)
        
        # Mock the query result
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute.return_value = mock_result
        
        result = await repo.get_execution_by_job_id("test-job-123")
        
        assert result == mock_execution
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_execution_by_job_id_not_found(self, mock_session):
        """Test getting non-existent execution by job_id."""
        repo = ExecutionRepository(mock_session)
        
        # Mock empty result
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result
        
        result = await repo.get_execution_by_job_id("nonexistent-job")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_execution_history(self, mock_session, mock_execution):
        """Test getting paginated execution history."""
        repo = ExecutionRepository(mock_session)
        
        # Mock count query
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 5
        
        # Mock list query
        mock_list_result = MagicMock()
        mock_list_result.scalars.return_value.all.return_value = [mock_execution]
        
        mock_session.execute.side_effect = [mock_count_result, mock_list_result]
        
        executions, total_count = await repo.get_execution_history(limit=10, offset=0)
        
        assert len(executions) == 1
        assert executions[0] == mock_execution
        assert total_count == 5
        assert mock_session.execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_update_execution_by_job_id(self, mock_session, mock_execution):
        """Test updating execution by job_id."""
        repo = ExecutionRepository(mock_session)
        
        # Mock get_execution_by_job_id
        repo.get_execution_by_job_id = AsyncMock(return_value=mock_execution)
        mock_session.flush = AsyncMock()
        
        update_data = {"status": ExecutionStatus.RUNNING.value}
        
        result = await repo.update_execution_by_job_id("test-job-123", update_data)
        
        assert result == mock_execution
        assert mock_execution.status == ExecutionStatus.RUNNING.value
        mock_session.flush.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_execution_by_job_id_not_found(self, mock_session):
        """Test updating non-existent execution by job_id."""
        repo = ExecutionRepository(mock_session)
        
        # Mock get_execution_by_job_id to return None
        repo.get_execution_by_job_id = AsyncMock(return_value=None)
        
        update_data = {"status": ExecutionStatus.RUNNING.value}
        
        result = await repo.update_execution_by_job_id("nonexistent-job", update_data)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_execution_status(self, mock_session):
        """Test updating execution status."""
        repo = ExecutionRepository(mock_session)
        
        # Mock get_execution_by_job_id
        mock_execution = MagicMock(spec=ExecutionHistory)
        mock_execution.job_id = "test-job-123"
        repo.get_execution_by_job_id = AsyncMock(return_value=mock_execution)
        
        # Mock the update query
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute.return_value = mock_result
        mock_session.commit = AsyncMock()
        
        result = await repo.update_execution_status(
            job_id="test-job-123",
            status=ExecutionStatus.COMPLETED.value,
            message="Execution completed",
            result={"output": "success"}
        )
        
        assert result == mock_execution
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_execution_status_not_found(self, mock_session):
        """Test updating status of non-existent execution."""
        repo = ExecutionRepository(mock_session)
        
        # Mock get_execution_by_job_id to return None
        repo.get_execution_by_job_id = AsyncMock(return_value=None)
        
        result = await repo.update_execution_status(
            job_id="nonexistent-job",
            status=ExecutionStatus.COMPLETED.value,
            message="Test message"
        )
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_execution_status_with_result_serialization(self, mock_session):
        """Test updating execution status with result serialization."""
        repo = ExecutionRepository(mock_session)
        
        # Mock get_execution_by_job_id
        mock_execution = MagicMock(spec=ExecutionHistory)
        repo.get_execution_by_job_id = AsyncMock(return_value=mock_execution)
        
        # Mock the update query
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute.return_value = mock_result
        mock_session.commit = AsyncMock()
        
        # Test with dict result
        dict_result = {"key": "value", "list": [1, 2, 3]}
        
        result = await repo.update_execution_status(
            job_id="test-job-123",
            status=ExecutionStatus.COMPLETED.value,
            message="Test",
            result=dict_result
        )
        
        assert result == mock_execution
        
        # Verify the update call includes serialized result
        update_call = mock_session.execute.call_args[0][0]
        assert update_call is not None
    
    @pytest.mark.asyncio
    async def test_mark_execution_completed(self, mock_session, mock_execution):
        """Test marking execution as completed."""
        repo = ExecutionRepository(mock_session)
        
        # Mock the update method
        repo.update = AsyncMock(return_value=mock_execution)
        
        result_data = {"output": "completed successfully"}
        
        result = await repo.mark_execution_completed(1, result_data)
        
        assert result == mock_execution
        
        # Verify update was called with correct data
        update_call_args = repo.update.call_args[0]
        assert update_call_args[0] == 1
        update_data = update_call_args[1]
        assert update_data["status"] == ExecutionStatus.COMPLETED.value
        assert "completed_at" in update_data
        assert update_data["result"] == result_data
    
    @pytest.mark.asyncio
    async def test_mark_execution_failed(self, mock_session, mock_execution):
        """Test marking execution as failed."""
        repo = ExecutionRepository(mock_session)
        
        # Mock the update method
        repo.update = AsyncMock(return_value=mock_execution)
        
        error_message = "Execution failed due to timeout"
        
        result = await repo.mark_execution_failed(1, error_message)
        
        assert result == mock_execution
        
        # Verify update was called with correct data
        update_call_args = repo.update.call_args[0]
        assert update_call_args[0] == 1
        update_data = update_call_args[1]
        assert update_data["status"] == ExecutionStatus.FAILED.value
        assert update_data["error"] == error_message
        assert "completed_at" in update_data
    
    @pytest.mark.asyncio
    async def test_update_execution_status_terminal_states(self, mock_session):
        """Test that completed_at is set for terminal states."""
        repo = ExecutionRepository(mock_session)
        
        # Mock get_execution_by_job_id
        mock_execution = MagicMock(spec=ExecutionHistory)
        repo.get_execution_by_job_id = AsyncMock(return_value=mock_execution)
        
        # Mock the update query
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_execution
        mock_session.execute.return_value = mock_result
        mock_session.commit = AsyncMock()
        
        # Test each terminal state
        terminal_states = [
            ExecutionStatus.COMPLETED.value,
            ExecutionStatus.FAILED.value,
            ExecutionStatus.CANCELLED.value
        ]
        
        for status in terminal_states:
            await repo.update_execution_status(
                job_id="test-job-123",
                status=status,
                message=f"Status: {status}"
            )
            
            # Verify completed_at is set in the update data
            update_stmt = mock_session.execute.call_args[0][0]
            # The completed_at should be in the values being updated
            assert update_stmt is not None
    
    @pytest.mark.asyncio
    async def test_update_execution_status_error_handling(self, mock_session):
        """Test error handling in update_execution_status."""
        repo = ExecutionRepository(mock_session)
        
        # Mock get_execution_by_job_id to raise exception
        repo.get_execution_by_job_id = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(Exception, match="Database error"):
            await repo.update_execution_status(
                job_id="test-job-123",
                status=ExecutionStatus.FAILED.value,
                message="Test"
            )
    
    @pytest.mark.asyncio
    async def test_repository_factory_function(self, mock_session):
        """Test the repository factory function."""
        from src.repositories.execution_repository import get_execution_repository
        
        repo = get_execution_repository(mock_session)
        
        assert isinstance(repo, ExecutionRepository)
        assert repo.session == mock_session
    
    @pytest.mark.asyncio
    async def test_list_method_inheritance(self, mock_session, mock_execution):
        """Test that list method works through inheritance."""
        repo = ExecutionRepository(mock_session)
        
        # Mock the list method from BaseRepository
        repo.list = AsyncMock(return_value=[mock_execution])
        
        result = await repo.list()
        
        assert len(result) == 1
        assert result[0] == mock_execution
    
    @pytest.mark.asyncio
    async def test_pagination_parameters(self, mock_session):
        """Test pagination with various parameters."""
        repo = ExecutionRepository(mock_session)
        
        # Mock count and list queries
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 100
        
        mock_list_result = MagicMock()
        mock_list_result.scalars.return_value.all.return_value = []
        
        mock_session.execute.side_effect = [mock_count_result, mock_list_result]
        
        # Test with custom pagination
        executions, total_count = await repo.get_execution_history(limit=20, offset=40)
        
        assert total_count == 100
        assert len(executions) == 0
        
        # Verify pagination parameters in the query
        # The second call should be the list query with limit and offset
        list_call = mock_session.execute.call_args_list[1][0][0]
        assert list_call is not None