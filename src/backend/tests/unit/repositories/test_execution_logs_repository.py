"""
Unit tests for ExecutionLogsRepository.

Tests the functionality of execution logs repository including
timestamp normalization, session management, group context handling, and error recovery.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import desc, func, delete, text

from src.repositories.execution_logs_repository import ExecutionLogsRepository
from src.models.execution_logs import ExecutionLog
from src.utils.user_context import GroupContext


# Mock execution log model
class MockExecutionLog:
    def __init__(self, id=1, execution_id="exec-123", content="Test log", 
                 timestamp=None, group_id=None, group_email=None):
        self.id = id
        self.execution_id = execution_id
        self.content = content
        self.timestamp = timestamp or datetime.now(timezone.utc).replace(tzinfo=None)
        self.group_id = group_id
        self.group_email = group_email


# Mock group context
class MockGroupContext:
    def __init__(self, primary_group_id="group-1", group_email="group@test.com"):
        self.primary_group_id = primary_group_id
        self.group_email = group_email


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
    
    def scalar_one(self):
        return self._scalar_value
    
    def scalar(self):
        return self._scalar_value


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
def execution_logs_repository():
    """Create an execution logs repository."""
    return ExecutionLogsRepository()


@pytest.fixture
def sample_execution_logs():
    """Create sample execution logs for testing."""
    return [
        MockExecutionLog(id=1, execution_id="exec-1", content="Log 1"),
        MockExecutionLog(id=2, execution_id="exec-1", content="Log 2"),
        MockExecutionLog(id=3, execution_id="exec-2", content="Log 3")
    ]


@pytest.fixture
def sample_group_context():
    """Create sample group context for testing."""
    return MockGroupContext(primary_group_id="group-1", group_email="group@test.com")


class TestExecutionLogsRepositoryNormalizeTimestamp:
    """Test cases for _normalize_timestamp method."""
    
    def test_normalize_timestamp_none(self, execution_logs_repository):
        """Test timestamp normalization with None input."""
        result = execution_logs_repository._normalize_timestamp(None)
        assert result is None
    
    def test_normalize_timestamp_timezone_aware(self, execution_logs_repository):
        """Test timestamp normalization with timezone-aware datetime."""
        # Create timezone-aware datetime
        tz_aware = datetime(2023, 12, 25, 10, 30, 45, tzinfo=timezone.utc)
        
        result = execution_logs_repository._normalize_timestamp(tz_aware)
        
        assert result.tzinfo is None  # Should be timezone-naive
        assert result.year == 2023
        assert result.month == 12
        assert result.day == 25
        assert result.hour == 10
        assert result.minute == 30
        assert result.second == 45
    
    def test_normalize_timestamp_timezone_naive(self, execution_logs_repository):
        """Test timestamp normalization with timezone-naive datetime."""
        # Create timezone-naive datetime
        tz_naive = datetime(2023, 12, 25, 10, 30, 45)
        
        result = execution_logs_repository._normalize_timestamp(tz_naive)
        
        assert result == tz_naive  # Should return as-is
        assert result.tzinfo is None
    
    def test_normalize_timestamp_non_utc_timezone(self, execution_logs_repository):
        """Test timestamp normalization with non-UTC timezone."""
        # Create datetime in Eastern timezone (UTC-5)
        from datetime import timezone, timedelta
        eastern = timezone(timedelta(hours=-5))
        eastern_time = datetime(2023, 12, 25, 10, 30, 45, tzinfo=eastern)
        
        result = execution_logs_repository._normalize_timestamp(eastern_time)
        
        assert result.tzinfo is None  # Should be timezone-naive
        # Should be converted to UTC (10:30 EST = 15:30 UTC)
        assert result.hour == 15
        assert result.minute == 30
    
    def test_normalize_timestamp_object_without_tzinfo(self, execution_logs_repository):
        """Test timestamp normalization with object that doesn't have tzinfo."""
        class MockTimestamp:
            pass
        
        mock_obj = MockTimestamp()
        result = execution_logs_repository._normalize_timestamp(mock_obj)
        
        assert result == mock_obj  # Should return as-is


class TestExecutionLogsRepositoryCreate:
    """Test cases for create method."""
    
    @pytest.mark.asyncio
    async def test_create_success(self, execution_logs_repository, mock_async_session):
        """Test successful log creation."""
        execution_id = "exec-123"
        content = "Test log content"
        timestamp = datetime(2023, 12, 25, 10, 30)
        
        with patch('src.repositories.execution_logs_repository.ExecutionLog') as mock_log_class:
            created_log = MockExecutionLog(execution_id=execution_id, content=content)
            mock_log_class.return_value = created_log
            
            result = await execution_logs_repository.create(
                mock_async_session, execution_id, content, timestamp
            )
            
            assert result == created_log
            mock_log_class.assert_called_once_with(
                execution_id=execution_id,
                content=content,
                timestamp=timestamp  # Should be normalized
            )
            mock_async_session.add.assert_called_once_with(created_log)
            mock_async_session.commit.assert_called_once()
            mock_async_session.refresh.assert_called_once_with(created_log)
    
    @pytest.mark.asyncio
    async def test_create_without_timestamp(self, execution_logs_repository, mock_async_session):
        """Test log creation without providing timestamp."""
        execution_id = "exec-123"
        content = "Test log content"
        
        with patch('src.repositories.execution_logs_repository.ExecutionLog') as mock_log_class:
            created_log = MockExecutionLog()
            mock_log_class.return_value = created_log
            
            result = await execution_logs_repository.create(
                mock_async_session, execution_id, content
            )
            
            assert result == created_log
            mock_log_class.assert_called_once_with(
                execution_id=execution_id,
                content=content,
                timestamp=None  # Should pass None, let model handle default
            )
    
    @pytest.mark.asyncio
    async def test_create_database_error(self, execution_logs_repository, mock_async_session):
        """Test log creation with database error."""
        mock_async_session.commit.side_effect = Exception("Database error")
        
        with patch('src.repositories.execution_logs_repository.ExecutionLog') as mock_log_class:
            mock_log_class.return_value = MockExecutionLog()
            
            with patch('src.repositories.execution_logs_repository.logger') as mock_logger:
                with pytest.raises(Exception, match="Database error"):
                    await execution_logs_repository.create(
                        mock_async_session, "exec-123", "content"
                    )
                
                mock_async_session.rollback.assert_called_once()
                mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_create_rollback_error(self, execution_logs_repository, mock_async_session):
        """Test log creation with rollback error."""
        mock_async_session.commit.side_effect = Exception("Database error")
        mock_async_session.rollback.side_effect = Exception("Rollback error")
        
        with patch('src.repositories.execution_logs_repository.ExecutionLog') as mock_log_class:
            mock_log_class.return_value = MockExecutionLog()
            
            with patch('src.repositories.execution_logs_repository.logger') as mock_logger:
                with pytest.raises(Exception, match="Database error"):
                    await execution_logs_repository.create(
                        mock_async_session, "exec-123", "content"
                    )
                
                # Should log both the original error and rollback error
                assert mock_logger.error.call_count == 2


class TestExecutionLogsRepositoryCreateWithManagedSession:
    """Test cases for create_with_managed_session method."""
    
    @pytest.mark.asyncio
    async def test_create_with_managed_session_success(self, execution_logs_repository):
        """Test successful log creation with managed session."""
        execution_id = "exec-123"
        content = "Test content"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            created_log = MockExecutionLog()
            with patch.object(execution_logs_repository, 'create', return_value=created_log):
                result = await execution_logs_repository.create_with_managed_session(
                    execution_id, content
                )
                
                assert result == created_log
    
    @pytest.mark.asyncio
    async def test_create_with_managed_session_fallback_to_sql(self, execution_logs_repository):
        """Test fallback to direct SQL when ORM creation fails."""
        execution_id = "exec-123"
        content = "Test content"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock create method to fail
            with patch.object(execution_logs_repository, 'create', side_effect=Exception("ORM failed")):
                # Mock direct SQL execution
                mock_result = MockResult(scalar_value=123)  # Log ID
                mock_session.execute.side_effect = [mock_result, None]  # INSERT then SELECT
                
                # Mock the SELECT query result
                created_log = MockExecutionLog(id=123)
                select_result = MockResult([created_log])
                mock_session.execute.side_effect = [mock_result, select_result]
                
                with patch('src.repositories.execution_logs_repository.logger') as mock_logger:
                    result = await execution_logs_repository.create_with_managed_session(
                        execution_id, content
                    )
                    
                    assert result == created_log
                    mock_logger.error.assert_called()  # Should log the ORM failure
    
    @pytest.mark.asyncio
    async def test_create_with_managed_session_sql_fallback_with_quotes(self, execution_logs_repository):
        """Test SQL fallback with content containing single quotes."""
        execution_id = "exec-123"
        content = "Content with 'single quotes' and more 'quotes'"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_logs_repository, 'create', side_effect=Exception("ORM failed")):
                mock_result = MockResult(scalar_value=123)
                created_log = MockExecutionLog(id=123)
                select_result = MockResult([created_log])
                mock_session.execute.side_effect = [mock_result, select_result]
                
                result = await execution_logs_repository.create_with_managed_session(
                    execution_id, content
                )
                
                assert result == created_log
                # Verify that single quotes were escaped in the SQL
                sql_call = mock_session.execute.call_args_list[0][0][0]
                assert "'Content with ''single quotes'' and more ''quotes''" in str(sql_call)
    
    @pytest.mark.asyncio
    async def test_create_with_managed_session_complete_failure(self, execution_logs_repository):
        """Test when both ORM and SQL fallback fail."""
        execution_id = "exec-123"
        content = "Test content"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_logs_repository, 'create', side_effect=Exception("ORM failed")):
                mock_session.execute.side_effect = Exception("SQL failed")
                
                with patch('src.repositories.execution_logs_repository.logger') as mock_logger:
                    with pytest.raises(Exception, match="SQL failed"):
                        await execution_logs_repository.create_with_managed_session(
                            execution_id, content
                        )
                    
                    # Should log both ORM and SQL failures
                    assert mock_logger.error.call_count >= 2
    
    @pytest.mark.asyncio
    async def test_create_with_managed_session_with_timestamp(self, execution_logs_repository):
        """Test managed session creation with custom timestamp."""
        execution_id = "exec-123"
        content = "Test content"
        timestamp = datetime(2023, 12, 25, 10, 30, 45)
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            created_log = MockExecutionLog()
            with patch.object(execution_logs_repository, 'create', return_value=created_log) as mock_create:
                result = await execution_logs_repository.create_with_managed_session(
                    execution_id, content, timestamp
                )
                
                # Verify timestamp was normalized and passed to create
                mock_create.assert_called_once()
                call_args = mock_create.call_args[1]
                assert call_args['timestamp'] == timestamp


class TestExecutionLogsRepositoryGetByExecutionId:
    """Test cases for get_by_execution_id method."""
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_success(self, execution_logs_repository, mock_async_session, sample_execution_logs):
        """Test successful retrieval of logs by execution ID."""
        execution_id = "exec-1"
        matching_logs = [log for log in sample_execution_logs if log.execution_id == execution_id]
        
        mock_result = MockResult(matching_logs)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.get_by_execution_id(
            mock_async_session, execution_id
        )
        
        assert result == matching_logs
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_with_pagination(self, execution_logs_repository, mock_async_session):
        """Test log retrieval with pagination parameters."""
        execution_id = "exec-1"
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.get_by_execution_id(
            mock_async_session, execution_id, limit=50, offset=10
        )
        
        assert result == []
        mock_async_session.execute.assert_called_once()
        # Verify query was built with limit and offset
        query_call = mock_async_session.execute.call_args[0][0]
        # The query object should have limit and offset applied
        assert hasattr(query_call, '_limit')
        assert hasattr(query_call, '_offset')
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_newest_first(self, execution_logs_repository, mock_async_session):
        """Test log retrieval with newest first ordering."""
        execution_id = "exec-1"
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.get_by_execution_id(
            mock_async_session, execution_id, newest_first=True
        )
        
        assert result == []
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_oldest_first(self, execution_logs_repository, mock_async_session):
        """Test log retrieval with oldest first ordering (default)."""
        execution_id = "exec-1"
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.get_by_execution_id(
            mock_async_session, execution_id, newest_first=False
        )
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestExecutionLogsRepositoryGetById:
    """Test cases for get_by_id method."""
    
    @pytest.mark.asyncio
    async def test_get_by_id_success(self, execution_logs_repository, mock_async_session, sample_execution_logs):
        """Test successful retrieval of log by ID."""
        log_id = 1
        target_log = sample_execution_logs[0]
        
        mock_result = MockResult([target_log])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.get_by_id(mock_async_session, log_id)
        
        assert result == target_log
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, execution_logs_repository, mock_async_session):
        """Test retrieval when log ID not found."""
        log_id = 999
        
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.get_by_id(mock_async_session, log_id)
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestExecutionLogsRepositoryDeleteByExecutionId:
    """Test cases for delete_by_execution_id method."""
    
    @pytest.mark.asyncio
    async def test_delete_by_execution_id_success(self, execution_logs_repository, mock_async_session):
        """Test successful deletion of logs by execution ID."""
        execution_id = "exec-123"
        
        mock_result = MockResult(rowcount=5)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.delete_by_execution_id(
            mock_async_session, execution_id
        )
        
        assert result == 5
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
        
        # Verify SQL query was constructed correctly
        sql_call = mock_async_session.execute.call_args[0][0]
        assert f"DELETE FROM execution_logs WHERE execution_id = '{execution_id}'" in str(sql_call)
    
    @pytest.mark.asyncio
    async def test_delete_by_execution_id_no_matches(self, execution_logs_repository, mock_async_session):
        """Test deletion when no logs match execution ID."""
        execution_id = "nonexistent"
        
        mock_result = MockResult(rowcount=0)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.delete_by_execution_id(
            mock_async_session, execution_id
        )
        
        assert result == 0
        mock_async_session.commit.assert_called_once()


class TestExecutionLogsRepositoryDeleteAll:
    """Test cases for delete_all method."""
    
    @pytest.mark.asyncio
    async def test_delete_all_success(self, execution_logs_repository, mock_async_session):
        """Test successful deletion of all logs."""
        mock_result = MockResult(rowcount=100)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.delete_all(mock_async_session)
        
        assert result == 100
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_all_empty_table(self, execution_logs_repository, mock_async_session):
        """Test deletion when table is empty."""
        mock_result = MockResult(rowcount=0)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.delete_all(mock_async_session)
        
        assert result == 0
        mock_async_session.commit.assert_called_once()


class TestExecutionLogsRepositoryCountByExecutionId:
    """Test cases for count_by_execution_id method."""
    
    @pytest.mark.asyncio
    async def test_count_by_execution_id_success(self, execution_logs_repository, mock_async_session):
        """Test successful counting of logs by execution ID."""
        execution_id = "exec-123"
        
        mock_result = MockResult(scalar_value=10)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.count_by_execution_id(
            mock_async_session, execution_id
        )
        
        assert result == 10
        mock_async_session.execute.assert_called_once()
        
        # Verify SQL query was constructed correctly
        sql_call = mock_async_session.execute.call_args[0][0]
        assert f"SELECT COUNT(*) FROM execution_logs WHERE execution_id = '{execution_id}'" in str(sql_call)
    
    @pytest.mark.asyncio
    async def test_count_by_execution_id_no_matches(self, execution_logs_repository, mock_async_session):
        """Test counting when no logs match execution ID."""
        execution_id = "nonexistent"
        
        mock_result = MockResult(scalar_value=0)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.count_by_execution_id(
            mock_async_session, execution_id
        )
        
        assert result == 0


class TestExecutionLogsRepositoryManagedSessionMethods:
    """Test cases for methods with managed session."""
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_with_managed_session(self, execution_logs_repository, sample_execution_logs):
        """Test get by execution ID with managed session."""
        execution_id = "exec-1"
        matching_logs = [log for log in sample_execution_logs if log.execution_id == execution_id]
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_logs_repository, 'get_by_execution_id', return_value=matching_logs) as mock_get:
                result = await execution_logs_repository.get_by_execution_id_with_managed_session(
                    execution_id, limit=50, offset=10, newest_first=True
                )
                
                assert result == matching_logs
                mock_get.assert_called_once_with(
                    session=mock_session,
                    execution_id=execution_id,
                    limit=50,
                    offset=10,
                    newest_first=True
                )
    
    @pytest.mark.asyncio
    async def test_count_by_execution_id_with_managed_session(self, execution_logs_repository):
        """Test count by execution ID with managed session."""
        execution_id = "exec-123"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_logs_repository, 'count_by_execution_id', return_value=10) as mock_count:
                result = await execution_logs_repository.count_by_execution_id_with_managed_session(execution_id)
                
                assert result == 10
                mock_count.assert_called_once_with(mock_session, execution_id)
    
    @pytest.mark.asyncio
    async def test_delete_by_execution_id_with_managed_session(self, execution_logs_repository):
        """Test delete by execution ID with managed session."""
        execution_id = "exec-123"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_logs_repository, 'delete_by_execution_id', return_value=5) as mock_delete:
                result = await execution_logs_repository.delete_by_execution_id_with_managed_session(execution_id)
                
                assert result == 5
                mock_delete.assert_called_once_with(mock_session, execution_id)
    
    @pytest.mark.asyncio
    async def test_delete_all_with_managed_session(self, execution_logs_repository):
        """Test delete all with managed session."""
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch.object(execution_logs_repository, 'delete_all', return_value=100) as mock_delete:
                result = await execution_logs_repository.delete_all_with_managed_session()
                
                assert result == 100
                mock_delete.assert_called_once_with(mock_session)


class TestExecutionLogsRepositoryGroupMethods:
    """Test cases for group-aware methods."""
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_and_group_with_managed_session(self, execution_logs_repository):
        """Test get by execution ID and group with managed session."""
        execution_id = "exec-123"
        group_id = "group-1"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            sample_logs = [MockExecutionLog(execution_id=execution_id, group_id=group_id)]
            mock_result = MockResult(sample_logs)
            mock_session.execute.return_value = mock_result
            
            result = await execution_logs_repository.get_by_execution_id_and_group_with_managed_session(
                execution_id, group_id, limit=100, offset=0, newest_first=True
            )
            
            assert result == sample_logs
            mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_and_group_include_null_group(self, execution_logs_repository):
        """Test get by execution ID and group including null group IDs."""
        execution_id = "exec-123"
        group_id = "group-1"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            sample_logs = [
                MockExecutionLog(execution_id=execution_id, group_id=group_id),
                MockExecutionLog(execution_id=execution_id, group_id=None)  # Legacy log
            ]
            mock_result = MockResult(sample_logs)
            mock_session.execute.return_value = mock_result
            
            result = await execution_logs_repository.get_by_execution_id_and_group_with_managed_session(
                execution_id, group_id, include_null_group=True
            )
            
            assert result == sample_logs
            mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_with_group_managed_session_success(self, execution_logs_repository, sample_group_context):
        """Test creation with group context and managed session."""
        execution_id = "exec-123"
        content = "Test content"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.execution_logs_repository.ExecutionLog') as mock_log_class:
                created_log = MockExecutionLog(
                    execution_id=execution_id,
                    content=content,
                    group_id=sample_group_context.primary_group_id,
                    group_email=sample_group_context.group_email
                )
                mock_log_class.return_value = created_log
                
                result = await execution_logs_repository.create_with_group_managed_session(
                    execution_id, content, group_context=sample_group_context
                )
                
                assert result == created_log
                mock_log_class.assert_called_once_with(
                    execution_id=execution_id,
                    content=content,
                    timestamp=None,
                    group_id=sample_group_context.primary_group_id,
                    group_email=sample_group_context.group_email
                )
                mock_session.add.assert_called_once_with(created_log)
                mock_session.commit.assert_called_once()
                mock_session.refresh.assert_called_once_with(created_log)
    
    @pytest.mark.asyncio
    async def test_create_with_group_managed_session_no_context(self, execution_logs_repository):
        """Test creation with no group context."""
        execution_id = "exec-123"
        content = "Test content"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.execution_logs_repository.ExecutionLog') as mock_log_class:
                created_log = MockExecutionLog(execution_id=execution_id, content=content)
                mock_log_class.return_value = created_log
                
                result = await execution_logs_repository.create_with_group_managed_session(
                    execution_id, content, group_context=None
                )
                
                assert result == created_log
                mock_log_class.assert_called_once_with(
                    execution_id=execution_id,
                    content=content,
                    timestamp=None,
                    group_id=None,
                    group_email=None
                )
    
    @pytest.mark.asyncio
    async def test_create_with_group_managed_session_error(self, execution_logs_repository, sample_group_context):
        """Test creation with group context when error occurs."""
        execution_id = "exec-123"
        content = "Test content"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            mock_session.commit.side_effect = Exception("Database error")
            
            with patch('src.repositories.execution_logs_repository.ExecutionLog') as mock_log_class:
                mock_log_class.return_value = MockExecutionLog()
                
                with patch('src.repositories.execution_logs_repository.logger') as mock_logger:
                    with pytest.raises(Exception, match="Database error"):
                        await execution_logs_repository.create_with_group_managed_session(
                            execution_id, content, group_context=sample_group_context
                        )
                    
                    mock_logger.error.assert_called()


class TestExecutionLogsRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_create_then_get_workflow(self, execution_logs_repository):
        """Test workflow of creating then retrieving logs."""
        execution_id = "exec-integration"
        content = "Integration test log"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Mock create workflow
            created_log = MockExecutionLog(id=1, execution_id=execution_id, content=content)
            with patch.object(execution_logs_repository, 'create', return_value=created_log):
                create_result = await execution_logs_repository.create_with_managed_session(
                    execution_id, content
                )
                assert create_result == created_log
            
            # Mock get workflow
            with patch.object(execution_logs_repository, 'get_by_execution_id', return_value=[created_log]):
                get_result = await execution_logs_repository.get_by_execution_id_with_managed_session(
                    execution_id
                )
                assert get_result == [created_log]
    
    @pytest.mark.asyncio
    async def test_create_count_delete_workflow(self, execution_logs_repository):
        """Test workflow of creating, counting, then deleting logs."""
        execution_id = "exec-workflow"
        
        with patch('src.repositories.execution_logs_repository.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            # Create logs
            created_log = MockExecutionLog(execution_id=execution_id)
            with patch.object(execution_logs_repository, 'create', return_value=created_log):
                await execution_logs_repository.create_with_managed_session(execution_id, "Log 1")
                await execution_logs_repository.create_with_managed_session(execution_id, "Log 2")
            
            # Count logs
            with patch.object(execution_logs_repository, 'count_by_execution_id', return_value=2):
                count_result = await execution_logs_repository.count_by_execution_id_with_managed_session(
                    execution_id
                )
                assert count_result == 2
            
            # Delete logs
            with patch.object(execution_logs_repository, 'delete_by_execution_id', return_value=2):
                delete_result = await execution_logs_repository.delete_by_execution_id_with_managed_session(
                    execution_id
                )
                assert delete_result == 2


class TestExecutionLogsRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_database_error(self, execution_logs_repository, mock_async_session):
        """Test get by execution ID with database error."""
        mock_async_session.execute.side_effect = Exception("Database connection lost")
        
        with pytest.raises(Exception, match="Database connection lost"):
            await execution_logs_repository.get_by_execution_id(mock_async_session, "exec-123")
    
    @pytest.mark.asyncio
    async def test_delete_all_database_error(self, execution_logs_repository, mock_async_session):
        """Test delete all with database error."""
        mock_async_session.execute.side_effect = Exception("Delete failed")
        
        with pytest.raises(Exception, match="Delete failed"):
            await execution_logs_repository.delete_all(mock_async_session)
    
    @pytest.mark.asyncio
    async def test_count_by_execution_id_database_error(self, execution_logs_repository, mock_async_session):
        """Test count by execution ID with database error."""
        mock_async_session.execute.side_effect = Exception("Count query failed")
        
        with pytest.raises(Exception, match="Count query failed"):
            await execution_logs_repository.count_by_execution_id(mock_async_session, "exec-123")


class TestExecutionLogsRepositorySingleton:
    """Test cases for singleton instance."""
    
    def test_singleton_instance_creation(self):
        """Test that singleton instance is created correctly."""
        from src.repositories.execution_logs_repository import execution_logs_repository
        
        assert execution_logs_repository is not None
        assert isinstance(execution_logs_repository, ExecutionLogsRepository)


class TestExecutionLogsRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_create_with_very_long_content(self, execution_logs_repository, mock_async_session):
        """Test creation with very long log content."""
        execution_id = "exec-123"
        long_content = "x" * 10000  # Very long content
        
        with patch('src.repositories.execution_logs_repository.ExecutionLog') as mock_log_class:
            created_log = MockExecutionLog(content=long_content)
            mock_log_class.return_value = created_log
            
            result = await execution_logs_repository.create(
                mock_async_session, execution_id, long_content
            )
            
            assert result == created_log
            mock_log_class.assert_called_once_with(
                execution_id=execution_id,
                content=long_content,
                timestamp=None
            )
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_with_zero_limit(self, execution_logs_repository, mock_async_session):
        """Test get by execution ID with zero limit."""
        execution_id = "exec-123"
        
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.get_by_execution_id(
            mock_async_session, execution_id, limit=0
        )
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_by_execution_id_with_large_offset(self, execution_logs_repository, mock_async_session):
        """Test get by execution ID with very large offset."""
        execution_id = "exec-123"
        
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.get_by_execution_id(
            mock_async_session, execution_id, offset=10000
        )
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_create_with_empty_content(self, execution_logs_repository, mock_async_session):
        """Test creation with empty content."""
        execution_id = "exec-123"
        empty_content = ""
        
        with patch('src.repositories.execution_logs_repository.ExecutionLog') as mock_log_class:
            created_log = MockExecutionLog(content=empty_content)
            mock_log_class.return_value = created_log
            
            result = await execution_logs_repository.create(
                mock_async_session, execution_id, empty_content
            )
            
            assert result == created_log
    
    @pytest.mark.asyncio
    async def test_delete_by_execution_id_with_special_characters(self, execution_logs_repository, mock_async_session):
        """Test deletion with execution ID containing special characters."""
        execution_id = "exec-with-special-chars-123!@#"
        
        mock_result = MockResult(rowcount=3)
        mock_async_session.execute.return_value = mock_result
        
        result = await execution_logs_repository.delete_by_execution_id(
            mock_async_session, execution_id
        )
        
        assert result == 3
        # SQL should be properly escaped
        sql_call = mock_async_session.execute.call_args[0][0]
        assert execution_id in str(sql_call)