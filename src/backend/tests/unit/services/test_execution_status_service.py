"""
Unit tests for ExecutionStatusService.

Tests the functionality of execution status operations including
status updates, status retrieval, execution creation, and error handling.
"""
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.execution_status_service import ExecutionStatusService
from src.models.execution_status import ExecutionStatus


class MockExecution:
    """Mock execution record for testing."""
    
    def __init__(self, id=1, job_id="test-job-123", status="RUNNING", error=None, result=None):
        self.id = id
        self.job_id = job_id
        self.status = status
        self.error = error
        self.result = result


class MockGroupContext:
    """Mock group context for testing."""
    
    def __init__(self, primary_group_id="group-123", group_ids=None, group_email="test@example.com"):
        self.primary_group_id = primary_group_id
        self.group_ids = group_ids or ["group-123"]
        self.group_email = group_email


@pytest.fixture
def mock_execution():
    """Create a mock execution record."""
    return MockExecution()


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    return MockGroupContext()


@pytest.fixture
def sample_execution_data():
    """Create sample execution data."""
    return {
        "job_id": "test-job-456",
        "status": "RUNNING",
        "created_at": datetime.now(),
        "user_id": 1
    }


class TestExecutionStatusService:
    """Test cases for ExecutionStatusService."""
    
    @pytest.mark.asyncio
    async def test_update_status_success(self, mock_execution):
        """Test successful status update."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Task completed successfully"
            )
            
            assert result is True
            mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_status_with_result(self, mock_execution):
        """Test status update with result data."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            result_data = {"output": "success", "count": 5}
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Task completed",
                result=result_data
            )
            
            assert result is True
            mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_status_invalid_job_id_none(self):
        """Test status update with None job_id."""
        result = await ExecutionStatusService.update_status(
            job_id=None,
            status="COMPLETED",
            message="Test message"
        )
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_update_status_invalid_job_id_empty_string(self):
        """Test status update with empty string job_id."""
        result = await ExecutionStatusService.update_status(
            job_id="",
            status="COMPLETED",
            message="Test message"
        )
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_update_status_invalid_job_id_wrong_type(self):
        """Test status update with wrong type job_id."""
        result = await ExecutionStatusService.update_status(
            job_id=123,  # Should be string
            status="COMPLETED",
            message="Test message"
        )
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_update_status_database_operation_failure(self):
        """Test status update when database operation fails."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = False
            
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Test message"
            )
            
            assert result is False
            mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_status_database_operation_exception(self):
        """Test status update when database operation raises exception."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.side_effect = Exception("Database error")
            
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Test message"
            )
            
            assert result is False
            mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_status_result_processing_dict(self):
        """Test status update with dictionary result."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            result_data = {"key": "value", "number": 42}
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Test message",
                result=result_data
            )
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_update_status_result_processing_list(self):
        """Test status update with list result."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            result_data = ["item1", "item2", "item3"]
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Test message",
                result=result_data
            )
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_update_status_result_processing_string(self):
        """Test status update with string result."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Test message",
                result="string result"
            )
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_update_status_result_processing_number(self):
        """Test status update with number result."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Test message",
                result=42
            )
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_update_status_terminal_status_completed(self):
        """Test status update sets completed_at for COMPLETED status."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status=ExecutionStatus.COMPLETED.value,
                message="Task completed"
            )
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_update_status_terminal_status_failed(self):
        """Test status update sets completed_at for FAILED status."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status=ExecutionStatus.FAILED.value,
                message="Task failed"
            )
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_update_status_terminal_status_cancelled(self):
        """Test status update sets completed_at for CANCELLED status."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status=ExecutionStatus.CANCELLED.value,
                message="Task cancelled"
            )
            
            assert result is True
    
    @pytest.mark.asyncio
    async def test_get_status_success(self, mock_execution):
        """Test successful status retrieval."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = mock_execution
            
            result = await ExecutionStatusService.get_status("test-job-123")
            
            assert result == mock_execution
            mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_status_not_found(self):
        """Test status retrieval when execution not found."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = None
            
            result = await ExecutionStatusService.get_status("nonexistent-job")
            
            assert result is None
            mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_status_invalid_execution_id_none(self):
        """Test status retrieval with None execution_id."""
        result = await ExecutionStatusService.get_status(None)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_status_invalid_execution_id_empty_string(self):
        """Test status retrieval with empty string execution_id."""
        result = await ExecutionStatusService.get_status("")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_status_invalid_execution_id_wrong_type(self):
        """Test status retrieval with wrong type execution_id."""
        result = await ExecutionStatusService.get_status(123)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_status_database_operation_exception(self):
        """Test status retrieval when database operation raises exception."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.side_effect = Exception("Database error")
            
            result = await ExecutionStatusService.get_status("test-job-123")
            
            assert result is None
            mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_execution_success(self, sample_execution_data):
        """Test successful execution creation."""
        with patch('src.db.session.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session
            mock_session_factory.return_value.__aexit__.return_value = None
            
            with patch('src.repositories.execution_repository.ExecutionRepository') as mock_repo_class:
                mock_repo = AsyncMock()
                mock_repo.get_execution_by_job_id.return_value = None  # No existing record
                mock_repo.create_execution.return_value = MockExecution()
                mock_repo_class.return_value = mock_repo
                
                result = await ExecutionStatusService.create_execution(sample_execution_data)
                
                assert result is True
                mock_repo.create_execution.assert_called_once_with(data=sample_execution_data)
                mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_execution_with_group_context(self, sample_execution_data, mock_group_context):
        """Test execution creation with group context."""
        with patch('src.db.session.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session
            mock_session_factory.return_value.__aexit__.return_value = None
            
            with patch('src.repositories.execution_repository.ExecutionRepository') as mock_repo_class:
                mock_repo = AsyncMock()
                mock_repo.get_execution_by_job_id.return_value = None
                mock_repo.create_execution.return_value = MockExecution()
                mock_repo_class.return_value = mock_repo
                
                result = await ExecutionStatusService.create_execution(
                    sample_execution_data, 
                    group_context=mock_group_context
                )
                
                assert result is True
                # Verify group context was added to execution data
                expected_data = sample_execution_data.copy()
                expected_data["group_id"] = mock_group_context.primary_group_id
                expected_data["group_email"] = mock_group_context.group_email
                mock_repo.create_execution.assert_called_once_with(data=expected_data)
    
    @pytest.mark.asyncio
    async def test_create_execution_already_exists(self, sample_execution_data):
        """Test execution creation when record already exists."""
        with patch('src.db.session.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session
            mock_session_factory.return_value.__aexit__.return_value = None
            
            with patch('src.repositories.execution_repository.ExecutionRepository') as mock_repo_class:
                mock_repo = AsyncMock()
                mock_repo.get_execution_by_job_id.return_value = MockExecution()  # Existing record
                mock_repo_class.return_value = mock_repo
                
                result = await ExecutionStatusService.create_execution(sample_execution_data)
                
                assert result is True
                mock_repo.create_execution.assert_not_called()  # Should not create if exists
    
    @pytest.mark.asyncio
    async def test_create_execution_invalid_job_id_none(self):
        """Test execution creation with None job_id."""
        execution_data = {"job_id": None, "status": "RUNNING"}
        
        result = await ExecutionStatusService.create_execution(execution_data)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_create_execution_invalid_job_id_empty_string(self):
        """Test execution creation with empty string job_id."""
        execution_data = {"job_id": "", "status": "RUNNING"}
        
        result = await ExecutionStatusService.create_execution(execution_data)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_create_execution_invalid_job_id_wrong_type(self):
        """Test execution creation with wrong type job_id."""
        execution_data = {"job_id": 123, "status": "RUNNING"}
        
        result = await ExecutionStatusService.create_execution(execution_data)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_create_execution_missing_job_id(self):
        """Test execution creation with missing job_id."""
        execution_data = {"status": "RUNNING"}
        
        result = await ExecutionStatusService.create_execution(execution_data)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_create_execution_database_exception(self, sample_execution_data):
        """Test execution creation when database operation raises exception."""
        with patch('src.db.session.async_session_factory') as mock_session_factory:
            mock_session_factory.side_effect = Exception("Database connection error")
            
            result = await ExecutionStatusService.create_execution(sample_execution_data)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_create_execution_repository_exception(self, sample_execution_data):
        """Test execution creation when repository operation raises exception."""
        with patch('src.db.session.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session
            mock_session_factory.return_value.__aexit__.return_value = None
            
            with patch('src.repositories.execution_repository.ExecutionRepository') as mock_repo_class:
                mock_repo = AsyncMock()
                mock_repo.get_execution_by_job_id.side_effect = Exception("Repository error")
                mock_repo_class.return_value = mock_repo
                
                result = await ExecutionStatusService.create_execution(sample_execution_data)
                
                assert result is False
    
    @pytest.mark.asyncio
    async def test_create_execution_with_group_context_filtering(self, sample_execution_data, mock_group_context):
        """Test execution creation with group context filtering."""
        with patch('src.db.session.async_session_factory') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session_factory.return_value.__aenter__.return_value = mock_session
            mock_session_factory.return_value.__aexit__.return_value = None
            
            with patch('src.repositories.execution_repository.ExecutionRepository') as mock_repo_class:
                mock_repo = AsyncMock()
                mock_repo.get_execution_by_job_id.return_value = None
                mock_repo.create_execution.return_value = MockExecution()
                mock_repo_class.return_value = mock_repo
                
                result = await ExecutionStatusService.create_execution(
                    sample_execution_data, 
                    group_context=mock_group_context
                )
                
                assert result is True
                # Verify group_ids were passed for filtering
                mock_repo.get_execution_by_job_id.assert_called_once_with(
                    job_id=sample_execution_data["job_id"], 
                    group_ids=mock_group_context.group_ids
                )
    
    @pytest.mark.asyncio
    @patch('src.services.execution_status_service.logger')
    async def test_logging_during_operations(self, mock_logger, sample_execution_data):
        """Test that appropriate logging occurs during operations."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            # Test update_status logging
            await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Test message"
            )
            
            # Should not log error for successful operation
            mock_logger.error.assert_not_called()
        
        # Test error logging for invalid job_id
        await ExecutionStatusService.update_status(
            job_id=None,
            status="COMPLETED",
            message="Test message"
        )
        
        mock_logger.error.assert_called()
        assert "Invalid job_id" in mock_logger.error.call_args[0][0]
    
    def test_service_static_methods(self):
        """Test that service methods are static."""
        # These methods should be accessible without instantiation
        assert hasattr(ExecutionStatusService, 'update_status')
        assert hasattr(ExecutionStatusService, 'get_status')
        assert hasattr(ExecutionStatusService, 'create_execution')
        
        # Verify they are static methods
        assert callable(ExecutionStatusService.update_status)
        assert callable(ExecutionStatusService.get_status)
        assert callable(ExecutionStatusService.create_execution)
    
    @pytest.mark.asyncio
    async def test_execution_status_enum_values(self):
        """Test that the service works with ExecutionStatus enum values."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            # Test all terminal status values
            terminal_statuses = [
                ExecutionStatus.COMPLETED.value,
                ExecutionStatus.FAILED.value,
                ExecutionStatus.CANCELLED.value
            ]
            
            for status in terminal_statuses:
                result = await ExecutionStatusService.update_status(
                    job_id="test-job-123",
                    status=status,
                    message=f"Test message for {status}"
                )
                
                assert result is True
    
    @pytest.mark.asyncio
    async def test_update_status_complex_result_data(self):
        """Test status update with complex nested result data."""
        with patch('src.services.execution_status_service.execute_db_operation_with_fresh_engine') as mock_execute:
            mock_execute.return_value = True
            
            complex_result = {
                "summary": {
                    "total_items": 100,
                    "processed": 95,
                    "errors": 5
                },
                "details": [
                    {"id": 1, "status": "success"},
                    {"id": 2, "status": "error", "message": "Invalid data"}
                ],
                "metadata": {
                    "timestamp": "2023-06-15T10:30:00Z",
                    "version": "1.2.3"
                }
            }
            
            result = await ExecutionStatusService.update_status(
                job_id="test-job-123",
                status="COMPLETED",
                message="Complex task completed",
                result=complex_result
            )
            
            assert result is True