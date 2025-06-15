"""
Unit tests for BaseRepository.

Tests the functionality of the base repository including
CRUD operations, error handling, and transaction management.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import UUID

from src.core.base_repository import BaseRepository
from src.db.base import Base


# Mock model for testing
class MockModel(Base):
    __tablename__ = "mock_model"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(String(500))
    
    def __init__(self, **kwargs):
        super().__init__()
        for key, value in kwargs.items():
            setattr(self, key, value)


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    session = AsyncMock(spec=AsyncSession)
    
    # Mock execute method to return mock result
    mock_result = MagicMock()
    mock_scalars = MagicMock()
    mock_result.scalars.return_value = mock_scalars
    session.execute.return_value = mock_result
    
    return session


@pytest.fixture
def mock_model_instance():
    """Create a mock model instance."""
    instance = MagicMock()
    instance.id = str(uuid.uuid4())
    instance.name = "Test Model"
    return instance


@pytest.fixture
def base_repository(mock_session):
    """Create a BaseRepository instance with mock session."""
    return BaseRepository(MockModel, mock_session)


class TestBaseRepository:
    """Test cases for BaseRepository."""
    
    @pytest.mark.asyncio
    async def test_init(self, mock_session):
        """Test repository initialization."""
        repo = BaseRepository(MockModel, mock_session)
        
        assert repo.model == MockModel
        assert repo.session == mock_session
    
    @pytest.mark.asyncio
    async def test_get_success(self, base_repository, mock_session, mock_model_instance):
        """Test successful get operation."""
        # Setup mock return
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = mock_model_instance
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        result = await base_repository.get("test_id")
        
        assert result == mock_model_instance
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, base_repository, mock_session):
        """Test get operation when record not found."""
        # Setup mock to return None
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = None
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        result = await base_repository.get("nonexistent_id")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_with_exception(self, base_repository, mock_session):
        """Test get operation with database exception."""
        mock_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await base_repository.get("test_id")
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_success(self, base_repository, mock_session, mock_model_instance):
        """Test successful list operation."""
        # Setup mock return
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [mock_model_instance]
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        result = await base_repository.list(skip=0, limit=100)
        
        assert len(result) == 1
        assert result[0] == mock_model_instance
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_with_pagination(self, base_repository, mock_session):
        """Test list operation with pagination parameters."""
        # Setup mock return
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = []
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        await base_repository.list(skip=10, limit=50)
        
        # Verify the query was called with pagination
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_with_exception(self, base_repository, mock_session):
        """Test list operation with database exception."""
        mock_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await base_repository.list()
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_success(self, base_repository, mock_session):
        """Test successful create operation."""
        test_data = {"name": "Test Model", "description": "Test description"}
        
        # Mock session methods
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.refresh = AsyncMock()
        
        # Create expected instance
        expected_instance = MockModel(**test_data)
        expected_instance.id = uuid.uuid4()
        
        with patch.object(MockModel, "__new__", return_value=expected_instance):
            result = await base_repository.create(test_data)
            
            mock_session.add.assert_called_once()
            mock_session.flush.assert_called_once()
            mock_session.commit.assert_called_once()
            mock_session.refresh.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_with_exception(self, base_repository, mock_session):
        """Test create operation with database exception."""
        test_data = {"name": "Test Model"}
        
        # Mock model instantiation to work properly
        mock_instance = MagicMock()
        mock_instance.id = str(uuid.uuid4())
        mock_model_class = MagicMock(return_value=mock_instance)
        mock_model_class.__name__ = 'MockModel'
        base_repository.model = mock_model_class
        
        # Mock session.add to raise exception
        mock_session.add.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await base_repository.create(test_data)
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_add_success(self, base_repository, mock_session, mock_model_instance):
        """Test successful add operation."""
        # Mock session methods
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.refresh = AsyncMock()
        
        result = await base_repository.add(mock_model_instance)
        
        assert result == mock_model_instance
        mock_session.add.assert_called_once_with(mock_model_instance)
        mock_session.flush.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_called_once_with(mock_model_instance)
    
    @pytest.mark.asyncio
    async def test_add_with_exception(self, base_repository, mock_session, mock_model_instance):
        """Test add operation with database exception."""
        mock_session.add.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await base_repository.add(mock_model_instance)
        
        mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_success(self, base_repository, mock_session, mock_model_instance):
        """Test successful update operation."""
        test_id = str(uuid.uuid4())
        update_data = {"name": "Updated Model"}
        
        # Mock get method to return existing object
        with patch.object(base_repository, "get", return_value=mock_model_instance):
            # Mock session methods
            mock_session.execute = AsyncMock()
            mock_session.flush = AsyncMock()
            mock_session.commit = AsyncMock()
            
            # Mock updated object
            updated_instance = MagicMock()
            updated_instance.name = "Updated Model"
            
            with patch.object(base_repository, "get", side_effect=[mock_model_instance, updated_instance]):
                result = await base_repository.update(test_id, update_data)
                
                assert result == updated_instance
                mock_session.execute.assert_called_once()
                mock_session.flush.assert_called_once()
                mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, base_repository, mock_session):
        """Test update operation when record not found."""
        test_id = str(uuid.uuid4())
        update_data = {"name": "Updated Model"}
        
        # Mock get method to return None
        with patch.object(base_repository, "get", return_value=None):
            result = await base_repository.update(test_id, update_data)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_with_exception(self, base_repository, mock_session, mock_model_instance):
        """Test update operation with database exception."""
        test_id = str(uuid.uuid4())
        update_data = {"name": "Updated Model"}
        
        # Mock get method to return existing object
        with patch.object(base_repository, "get", return_value=mock_model_instance):
            mock_session.execute.side_effect = Exception("Database error")
            
            with pytest.raises(Exception, match="Database error"):
                await base_repository.update(test_id, update_data)
            
            mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_success(self, base_repository, mock_session, mock_model_instance):
        """Test successful delete operation."""
        test_id = str(uuid.uuid4())
        
        # Mock get method to return existing object
        with patch.object(base_repository, "get", return_value=mock_model_instance):
            # Mock session methods
            mock_session.delete = AsyncMock()
            mock_session.flush = AsyncMock()
            mock_session.commit = AsyncMock()
            
            result = await base_repository.delete(test_id)
            
            assert result is True
            mock_session.delete.assert_called_once_with(mock_model_instance)
            mock_session.flush.assert_called_once()
            mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, base_repository, mock_session):
        """Test delete operation when record not found."""
        test_id = str(uuid.uuid4())
        
        # Mock get method to return None
        with patch.object(base_repository, "get", return_value=None):
            result = await base_repository.delete(test_id)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_with_exception(self, base_repository, mock_session, mock_model_instance):
        """Test delete operation with database exception."""
        test_id = str(uuid.uuid4())
        
        # Mock get method to return existing object
        with patch.object(base_repository, "get", return_value=mock_model_instance):
            # Since session.delete is synchronous, set it to raise immediately
            mock_session.delete = MagicMock(side_effect=Exception("Database error"))
            
            with pytest.raises(Exception, match="Database error"):
                await base_repository.delete(test_id)
            
            mock_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_uuid_id_type(self, base_repository, mock_session):
        """Test that repository works with UUID IDs."""
        test_uuid = uuid.uuid4()
        
        # Setup mock return
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = None
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        await base_repository.get(test_uuid)
        
        # Should execute without error
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_int_id_type(self, base_repository, mock_session):
        """Test that repository works with integer IDs."""
        test_int_id = 123
        
        # Setup mock return
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = None
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute.return_value = mock_result
        
        await base_repository.get(test_int_id)
        
        # Should execute without error
        mock_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_logging_in_operations(self, base_repository, mock_session):
        """Test that operations include proper logging."""
        with patch("logging.getLogger") as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            test_data = {"name": "Test"}
            
            # Test create operation logging
            # Create a proper mock instance
            mock_instance = MagicMock(spec=MockModel)
            mock_instance.id = str(uuid.uuid4())
            
            # Mock session methods to avoid actual database operations
            mock_session.add = MagicMock()
            mock_session.flush = AsyncMock()
            mock_session.commit = AsyncMock()
            mock_session.refresh = AsyncMock()
            
            # Mock the model class to return our instance
            with patch.object(base_repository, "model", MockModel):
                with patch.object(MockModel, "__call__", return_value=mock_instance):
                    try:
                        await base_repository.create(test_data)
                    except:
                        pass  # We're just testing logging, not the full operation
            
            # Verify logging was called
            mock_get_logger.assert_called()