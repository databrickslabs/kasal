"""
Unit tests for DataProcessingRepository.

Tests the functionality of data processing repository including
async/sync operations, record creation, status updates, and table management.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, update, text
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.data_processing_repository import DataProcessingRepository
from src.models.data_processing import DataProcessing


# Mock data processing model
class MockDataProcessing:
    def __init__(self, id=1, che_number="CHE-123456", processed=False, 
                 company_name=None, created_at=None, updated_at=None, **kwargs):
        self.id = id
        self.che_number = che_number
        self.processed = processed
        self.company_name = company_name
        self.created_at = created_at
        self.updated_at = updated_at
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
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_sync_session():
    """Create a mock sync database session."""
    session = MagicMock()
    session.execute = MagicMock()
    session.add = MagicMock()
    session.commit = MagicMock()
    session.refresh = MagicMock()
    session.rollback = MagicMock()
    session.flush = MagicMock()
    return session


@pytest.fixture
def data_processing_repo_async(mock_async_session):
    """Create a data processing repository with async session."""
    return DataProcessingRepository(session=mock_async_session)


@pytest.fixture
def data_processing_repo_sync(mock_sync_session):
    """Create a data processing repository with sync session."""
    return DataProcessingRepository(sync_session=mock_sync_session)


@pytest.fixture
def data_processing_repo_both(mock_async_session, mock_sync_session):
    """Create a data processing repository with both sessions."""
    return DataProcessingRepository(session=mock_async_session, sync_session=mock_sync_session)


@pytest.fixture
def sample_data_processing_records():
    """Create sample data processing records for testing."""
    return [
        MockDataProcessing(id=1, che_number="CHE-123456", processed=False, company_name="Company A", group_id="group-123"),
        MockDataProcessing(id=2, che_number="CHE-789012", processed=True, company_name="Company B", group_id="group-123"),
        MockDataProcessing(id=3, che_number="CHE-345678", processed=False, company_name=None),
        MockDataProcessing(id=4, che_number="CHE-901234", processed=True, company_name="Company C", group_id="group-123")
    ]


class TestDataProcessingRepositoryInit:
    """Test repository initialization."""
    
    def test_init_with_async_session(self, mock_async_session):
        """Test repository initialization with async session."""
        repo = DataProcessingRepository(session=mock_async_session)
        assert repo.session == mock_async_session
        assert repo.sync_session is None
        assert repo.model == DataProcessing
    
    def test_init_with_sync_session(self, mock_sync_session):
        """Test repository initialization with sync session."""
        repo = DataProcessingRepository(sync_session=mock_sync_session)
        assert repo.session is None
        assert repo.sync_session == mock_sync_session
        assert repo.model == DataProcessing
    
    def test_init_with_both_sessions(self, mock_async_session, mock_sync_session):
        """Test repository initialization with both sessions."""
        repo = DataProcessingRepository(session=mock_async_session, sync_session=mock_sync_session)
        assert repo.session == mock_async_session
        assert repo.sync_session == mock_sync_session
        assert repo.model == DataProcessing
    
    def test_init_without_sessions(self):
        """Test repository initialization without sessions."""
        repo = DataProcessingRepository()
        assert repo.session is None
        assert repo.sync_session is None
        assert repo.model == DataProcessing


class TestDataProcessingRepositoryFindByCheNumber:
    """Test find by CHE number functionality."""
    
    @pytest.mark.asyncio
    async def test_find_by_che_number_found(self, data_processing_repo_async, sample_data_processing_records):
        """Test find by CHE number when record is found."""
        target_record = sample_data_processing_records[0]
        mock_result = MockResult([target_record])
        data_processing_repo_async.session.execute.return_value = mock_result
        
        result = await data_processing_repo_async.find_by_che_number("CHE-123456")
        
        assert result == target_record
        assert result.che_number == "CHE-123456"
    
    @pytest.mark.asyncio
    async def test_find_by_che_number_not_found(self, data_processing_repo_async):
        """Test find by CHE number when record is not found."""
        mock_result = MockResult([])
        data_processing_repo_async.session.execute.return_value = mock_result
        
        result = await data_processing_repo_async.find_by_che_number("CHE-NONEXISTENT")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_find_by_che_number_no_session(self):
        """Test find by CHE number without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Session not provided"):
            await repo.find_by_che_number("CHE-123456")
    
    def test_find_by_che_number_sync_found(self, data_processing_repo_sync, sample_data_processing_records):
        """Test sync find by CHE number when record is found."""
        target_record = sample_data_processing_records[0]
        mock_result = MockResult([target_record])
        data_processing_repo_sync.sync_session.execute.return_value = mock_result
        
        result = data_processing_repo_sync.find_by_che_number_sync("CHE-123456")
        
        assert result == target_record
        assert result.che_number == "CHE-123456"
    
    def test_find_by_che_number_sync_not_found(self, data_processing_repo_sync):
        """Test sync find by CHE number when record is not found."""
        mock_result = MockResult([])
        data_processing_repo_sync.sync_session.execute.return_value = mock_result
        
        result = data_processing_repo_sync.find_by_che_number_sync("CHE-NONEXISTENT")
        
        assert result is None
    
    def test_find_by_che_number_sync_no_session(self):
        """Test sync find by CHE number without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Sync session not provided"):
            repo.find_by_che_number_sync("CHE-123456")


class TestDataProcessingRepositoryUpdateProcessedStatus:
    """Test update processed status functionality."""
    
    @pytest.mark.asyncio
    async def test_update_processed_status_success(self, data_processing_repo_async):
        """Test update processed status successfully."""
        mock_result = MockResult(rowcount=1)
        data_processing_repo_async.session.execute.return_value = mock_result
        
        result = await data_processing_repo_async.update_processed_status("CHE-123456", True)
        
        assert result is True
        
        # Verify the update statement was constructed correctly
        call_args = data_processing_repo_async.session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # It's a SQLAlchemy update statement
    
    @pytest.mark.asyncio
    async def test_update_processed_status_no_rows_affected(self, data_processing_repo_async):
        """Test update processed status when no rows are affected."""
        mock_result = MockResult(rowcount=0)
        data_processing_repo_async.session.execute.return_value = mock_result
        
        result = await data_processing_repo_async.update_processed_status("CHE-NONEXISTENT", True)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_update_processed_status_no_session(self):
        """Test update processed status without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Session not provided"):
            await repo.update_processed_status("CHE-123456", True)
    
    def test_update_processed_status_sync_success(self, data_processing_repo_sync):
        """Test sync update processed status successfully."""
        mock_result = MockResult(rowcount=1)
        data_processing_repo_sync.sync_session.execute.return_value = mock_result
        
        result = data_processing_repo_sync.update_processed_status_sync("CHE-123456", True)
        
        assert result is True
    
    def test_update_processed_status_sync_no_rows_affected(self, data_processing_repo_sync):
        """Test sync update processed status when no rows are affected."""
        mock_result = MockResult(rowcount=0)
        data_processing_repo_sync.sync_session.execute.return_value = mock_result
        
        result = data_processing_repo_sync.update_processed_status_sync("CHE-NONEXISTENT", True)
        
        assert result is False
    
    def test_update_processed_status_sync_no_session(self):
        """Test sync update processed status without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Sync session not provided"):
            repo.update_processed_status_sync("CHE-123456", True)


class TestDataProcessingRepositoryCountUnprocessedRecords:
    """Test count unprocessed records functionality."""
    
    @pytest.mark.asyncio
    async def test_count_unprocessed_records_success(self, data_processing_repo_async):
        """Test count unprocessed records successfully."""
        mock_result = MockResult(scalar_value=5)
        data_processing_repo_async.session.execute.return_value = mock_result
        
        result = await data_processing_repo_async.count_unprocessed_records()
        
        assert result == 5
    
    @pytest.mark.asyncio
    async def test_count_unprocessed_records_zero(self, data_processing_repo_async):
        """Test count unprocessed records when zero."""
        mock_result = MockResult(scalar_value=0)
        data_processing_repo_async.session.execute.return_value = mock_result
        
        result = await data_processing_repo_async.count_unprocessed_records()
        
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_count_unprocessed_records_none_fallback(self, data_processing_repo_async):
        """Test count unprocessed records fallback when None returned."""
        mock_result = MockResult(scalar_value=None)
        data_processing_repo_async.session.execute.return_value = mock_result
        
        result = await data_processing_repo_async.count_unprocessed_records()
        
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_count_unprocessed_records_no_session(self):
        """Test count unprocessed records without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Session not provided"):
            await repo.count_unprocessed_records()
    
    def test_count_unprocessed_records_sync_success(self, data_processing_repo_sync):
        """Test sync count unprocessed records successfully."""
        mock_result = MockResult(scalar_value=3)
        data_processing_repo_sync.sync_session.execute.return_value = mock_result
        
        result = data_processing_repo_sync.count_unprocessed_records_sync()
        
        assert result == 3
    
    def test_count_unprocessed_records_sync_no_session(self):
        """Test sync count unprocessed records without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Sync session not provided"):
            repo.count_unprocessed_records_sync()


class TestDataProcessingRepositoryCountingMethods:
    """Test various counting methods."""
    
    def test_count_total_records_sync_success(self, data_processing_repo_sync):
        """Test sync count total records successfully."""
        mock_result = MockResult(scalar_value=10)
        data_processing_repo_sync.sync_session.execute.return_value = mock_result
        
        result = data_processing_repo_sync.count_total_records_sync()
        
        assert result == 10
    
    def test_count_total_records_sync_none_fallback(self, data_processing_repo_sync):
        """Test sync count total records fallback when None returned."""
        mock_result = MockResult(scalar_value=None)
        data_processing_repo_sync.sync_session.execute.return_value = mock_result
        
        result = data_processing_repo_sync.count_total_records_sync()
        
        assert result == 0
    
    def test_count_total_records_sync_no_session(self):
        """Test sync count total records without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Sync session not provided"):
            repo.count_total_records_sync()
    
    def test_count_null_company_name_sync_success(self, data_processing_repo_sync):
        """Test sync count null company name records successfully."""
        mock_result = MockResult(scalar_value=2)
        data_processing_repo_sync.sync_session.execute.return_value = mock_result
        
        result = data_processing_repo_sync.count_null_company_name_sync()
        
        assert result == 2
    
    def test_count_null_company_name_sync_no_session(self):
        """Test sync count null company name without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Sync session not provided"):
            repo.count_null_company_name_sync()


class TestDataProcessingRepositoryCreateRecord:
    """Test create record functionality."""
    
    def test_create_record_sync_basic(self, data_processing_repo_sync):
        """Test sync create record with basic parameters."""
        created_record = MockDataProcessing(
            id=1, che_number="CHE-NEW123", processed=False, company_name=None
        )
        
        with patch('src.repositories.data_processing_repository.DataProcessing') as mock_model:
            mock_model.return_value = created_record
            
            result = data_processing_repo_sync.create_record_sync("CHE-NEW123")
            
            assert result == created_record
            data_processing_repo_sync.sync_session.add.assert_called_once_with(created_record)
            data_processing_repo_sync.sync_session.flush.assert_called_once()
            
            # Verify DataProcessing was created with correct parameters
            call_args = mock_model.call_args[1]
            assert call_args['che_number'] == "CHE-NEW123"
            assert call_args['processed'] is False
            assert call_args['company_name'] is None
    
    def test_create_record_sync_with_all_params(self, data_processing_repo_sync):
        """Test sync create record with all parameters."""
        created_record = MockDataProcessing(
            id=1, che_number="CHE-FULL123", processed=True, company_name="Full Company"
        )
        
        with patch('src.repositories.data_processing_repository.DataProcessing') as mock_model:
            mock_model.return_value = created_record
            
            result = data_processing_repo_sync.create_record_sync(
                che_number="CHE-FULL123",
                processed=True,
                company_name="Full Company"
            )
            
            assert result == created_record
            
            # Verify all parameters were passed correctly
            call_args = mock_model.call_args[1]
            assert call_args['che_number'] == "CHE-FULL123"
            assert call_args['processed'] is True
            assert call_args['company_name'] == "Full Company"
    
    def test_create_record_sync_no_session(self):
        """Test sync create record without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Sync session not provided"):
            repo.create_record_sync("CHE-123456")


class TestDataProcessingRepositoryCreateTable:
    """Test create table functionality."""
    
    def test_create_table_if_not_exists_sync_success(self, data_processing_repo_sync):
        """Test sync create table if not exists successfully."""
        result = data_processing_repo_sync.create_table_if_not_exists_sync()
        
        assert result is True
        data_processing_repo_sync.sync_session.execute.assert_called_once()
        
        # Verify the SQL was executed
        call_args = data_processing_repo_sync.sync_session.execute.call_args[0][0]
        assert hasattr(call_args, 'text')  # It's a SQLAlchemy text statement
    
    def test_create_table_if_not_exists_sync_no_session(self):
        """Test sync create table without session raises error."""
        repo = DataProcessingRepository()
        
        with pytest.raises(ValueError, match="Sync session not provided"):
            repo.create_table_if_not_exists_sync()


class TestDataProcessingRepositoryErrorHandling:
    """Test error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_by_che_number_database_error(self, data_processing_repo_async):
        """Test find by CHE number handles database errors."""
        data_processing_repo_async.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await data_processing_repo_async.find_by_che_number("CHE-123456")
    
    @pytest.mark.asyncio
    async def test_update_processed_status_database_error(self, data_processing_repo_async):
        """Test update processed status handles database errors."""
        data_processing_repo_async.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await data_processing_repo_async.update_processed_status("CHE-123456", True)
    
    @pytest.mark.asyncio
    async def test_count_unprocessed_records_database_error(self, data_processing_repo_async):
        """Test count unprocessed records handles database errors."""
        data_processing_repo_async.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await data_processing_repo_async.count_unprocessed_records()
    
    def test_sync_methods_database_error(self, data_processing_repo_sync):
        """Test sync methods handle database errors."""
        data_processing_repo_sync.sync_session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            data_processing_repo_sync.find_by_che_number_sync("CHE-123456")
        
        with pytest.raises(SQLAlchemyError):
            data_processing_repo_sync.update_processed_status_sync("CHE-123456", True)
        
        with pytest.raises(SQLAlchemyError):
            data_processing_repo_sync.count_unprocessed_records_sync()


class TestDataProcessingRepositoryIntegration:
    """Test integration scenarios and workflows."""
    
    @pytest.mark.asyncio
    async def test_async_workflow(self, data_processing_repo_both, sample_data_processing_records):
        """Test complete async workflow: find, update, count."""
        target_record = sample_data_processing_records[0]  # CHE-123456, processed=False
        
        # 1. Find the record
        data_processing_repo_both.session.execute.return_value = MockResult([target_record])
        found_record = await data_processing_repo_both.find_by_che_number("CHE-123456")
        assert found_record == target_record
        assert found_record.processed is False
        
        # 2. Update processed status
        data_processing_repo_both.session.execute.return_value = MockResult(rowcount=1)
        update_result = await data_processing_repo_both.update_processed_status("CHE-123456", True)
        assert update_result is True
        
        # 3. Count unprocessed records
        data_processing_repo_both.session.execute.return_value = MockResult(scalar_value=2)
        count = await data_processing_repo_both.count_unprocessed_records()
        assert count == 2
    
    def test_sync_workflow(self, data_processing_repo_both, sample_data_processing_records):
        """Test complete sync workflow: create, find, update, count."""
        # 1. Create new record
        new_record = MockDataProcessing(che_number="CHE-SYNC123", processed=False)
        with patch('src.repositories.data_processing_repository.DataProcessing') as mock_model:
            mock_model.return_value = new_record
            
            created = data_processing_repo_both.create_record_sync("CHE-SYNC123")
            assert created == new_record
        
        # 2. Find the created record
        data_processing_repo_both.sync_session.execute.return_value = MockResult([new_record])
        found_record = data_processing_repo_both.find_by_che_number_sync("CHE-SYNC123")
        assert found_record == new_record
        
        # 3. Update processed status
        data_processing_repo_both.sync_session.execute.return_value = MockResult(rowcount=1)
        update_result = data_processing_repo_both.update_processed_status_sync("CHE-SYNC123", True)
        assert update_result is True
        
        # 4. Count various statistics
        data_processing_repo_both.sync_session.execute.return_value = MockResult(scalar_value=5)
        total_count = data_processing_repo_both.count_total_records_sync()
        assert total_count == 5
        
        data_processing_repo_both.sync_session.execute.return_value = MockResult(scalar_value=2)
        unprocessed_count = data_processing_repo_both.count_unprocessed_records_sync()
        assert unprocessed_count == 2
        
        data_processing_repo_both.sync_session.execute.return_value = MockResult(scalar_value=1)
        null_company_count = data_processing_repo_both.count_null_company_name_sync()
        assert null_company_count == 1
    
    def test_table_creation_workflow(self, data_processing_repo_sync):
        """Test table creation workflow."""
        # Create table
        create_result = data_processing_repo_sync.create_table_if_not_exists_sync()
        assert create_result is True
        
        # Verify create record can be called after table creation
        with patch('src.repositories.data_processing_repository.DataProcessing') as mock_model:
            new_record = MockDataProcessing()
            mock_model.return_value = new_record
            
            created = data_processing_repo_sync.create_record_sync("CHE-AFTER-CREATE")
            assert created == new_record
    
    def test_session_isolation(self, mock_async_session, mock_sync_session):
        """Test that async and sync sessions are properly isolated."""
        repo = DataProcessingRepository(session=mock_async_session, sync_session=mock_sync_session)
        
        # Async methods should use async session
        assert repo.session == mock_async_session
        
        # Sync methods should use sync session
        assert repo.sync_session == mock_sync_session
        
        # Verify they're different instances
        assert repo.session != repo.sync_session
    
    def test_mixed_operations_consistency(self, data_processing_repo_both, sample_data_processing_records):
        """Test consistency between async and sync operations."""
        target_record = sample_data_processing_records[0]
        
        # Both async and sync find should work on same CHE number
        # Async find
        data_processing_repo_both.session.execute.return_value = MockResult([target_record])
        
        # Sync find  
        data_processing_repo_both.sync_session.execute.return_value = MockResult([target_record])
        
        # Both should return same logical result
        # (Note: In real usage, they would access the same database)
        async def async_operation():
            return await data_processing_repo_both.find_by_che_number("CHE-123456")
        
        def sync_operation():
            return data_processing_repo_both.find_by_che_number_sync("CHE-123456")
        
        import asyncio
        async_result = asyncio.run(async_operation())
        sync_result = sync_operation()
        
        assert async_result.che_number == sync_result.che_number
        assert async_result.processed == sync_result.processed