"""
Unit tests for BaseService.

Tests the functionality of the base service including
CRUD operations and business logic.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from typing import Optional

from src.core.base_service import BaseService
from src.core.base_repository import BaseRepository


# Mock model class
class MockModel:
    def __init__(self, id=None, name=None, group_id=None):
        self.id = id
        self.name = name
        self.group_id = group_id


# Mock schema class
class MockSchema:
    def __init__(self, name: str, group_id: str = None):
        self.name = name
        self.group_id = group_id
    
    def model_dump(self, exclude_unset=False):
        result = {"name": self.name}
        if self.group_id is not None:
            result["group_id"] = self.group_id
        return result


# Mock repository class that doesn't use SQLAlchemy
class MockRepository:
    def __init__(self, model_class, session):
        self.model_class = model_class
        self.session = session
    
    async def get(self, id):
        if id == 1:
            return MockModel(id=1, name="Test", group_id="group-123")
        return None
    
    async def list(self, skip, limit):
        return [MockModel(id=1, name="Test1", group_id="group-123"), MockModel(id=2, name="Test2", group_id="group-123")]
    
    async def create(self, obj_in):
        return MockModel(id=1, name=obj_in.get("name"), group_id=obj_in.get("group_id"))
    
    async def update(self, id, obj_in):
        if id == 1:
            return MockModel(id=1, name=obj_in.get("name"), group_id=obj_in.get("group_id"))
        return None
    
    async def delete(self, id):
        return id == 1


# Concrete service implementation for testing
class ConcreteTestService(BaseService[MockModel, MockSchema]):
    model_class = MockModel
    repository_class = MockRepository


@pytest.fixture
def mock_session():
    """Create a mock session."""
    return AsyncMock()


@pytest.fixture
def test_service(mock_session):
    """Create a test service instance."""
    return ConcreteTestService(mock_session)


class TestBaseService:
    """Test cases for BaseService."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, test_service, mock_session):
        """Test successful get operation."""
        # Act
        result = await test_service.get(1)
        
        # Assert
        assert result is not None
        assert result.id == 1
        assert result.name == "Test"
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, test_service, mock_session):
        """Test get operation when record not found."""
        # Act
        result = await test_service.get(999)
        
        # Assert
        assert result is None
    
    @pytest.mark.asyncio
    async def test_list_success(self, test_service, mock_session):
        """Test successful list operation."""
        # Act
        result = await test_service.list(skip=10, limit=20)
        
        # Assert
        assert len(result) == 2
        assert result[0].id == 1
        assert result[0].name == "Test1"
        assert result[1].id == 2
        assert result[1].name == "Test2"
    
    @pytest.mark.asyncio
    async def test_list_with_defaults(self, test_service, mock_session):
        """Test list operation with default parameters."""
        # Act
        result = await test_service.list()
        
        # Assert
        assert len(result) == 2
    
    @pytest.mark.asyncio
    async def test_create_success(self, test_service, mock_session):
        """Test successful create operation."""
        # Arrange
        schema = MockSchema(name="New Item", group_id="group-123")
        
        # Act
        result = await test_service.create(schema)
        
        # Assert
        assert result is not None
        assert result.id == 1
        assert result.name == "New Item"
    
    @pytest.mark.asyncio
    async def test_update_success(self, test_service, mock_session):
        """Test successful update operation."""
        # Arrange
        schema = MockSchema(name="Updated Item", group_id="group-123")
        
        # Act
        result = await test_service.update(1, schema)
        
        # Assert
        assert result is not None
        assert result.id == 1
        assert result.name == "Updated Item"
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, test_service, mock_session):
        """Test update operation when record not found."""
        # Arrange
        schema = MockSchema(name="Updated Item", group_id="group-123")
        
        # Act
        result = await test_service.update(999, schema)
        
        # Assert
        assert result is None
    
    @pytest.mark.asyncio
    async def test_delete_success(self, test_service, mock_session):
        """Test successful delete operation."""
        # Act
        result = await test_service.delete(1)
        
        # Assert
        assert result is True
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, test_service, mock_session):
        """Test delete operation when record not found."""
        # Act
        result = await test_service.delete(999)
        
        # Assert
        assert result is False
    
