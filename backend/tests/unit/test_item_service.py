import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.models.item import Item
from src.repositories.item_repository import ItemRepository
from src.schemas.item import ItemCreate, ItemUpdate
from src.services.item_service import ItemService


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = MagicMock()
    uow.session = AsyncMock()
    return uow


@pytest.fixture
def mock_repository():
    """Create a mock repository with predefined behavior."""
    repo = AsyncMock(spec=ItemRepository)
    
    # Setup mock item for get method
    mock_item = MagicMock(spec=Item)
    mock_item.id = 1
    mock_item.name = "Test Item"
    mock_item.price = 10.0
    mock_item.description = "Test description"
    mock_item.is_active = True
    
    # Setup repository method returns
    repo.get.return_value = mock_item
    repo.list.return_value = [mock_item]
    repo.create.return_value = mock_item
    repo.update.return_value = mock_item
    repo.delete.return_value = True
    repo.find_by_name.return_value = mock_item
    repo.find_active_items.return_value = [mock_item]
    
    return repo


@pytest.mark.asyncio
async def test_get_item(mock_uow, mock_repository):
    """Test getting an item by ID."""
    # Arrange
    with patch("src.services.item_service.ItemRepository", return_value=mock_repository):
        service = ItemService(mock_uow)
        
        # Act
        result = await service.get(1)
        
        # Assert
        assert result is not None
        assert result.id == 1
        assert result.name == "Test Item"
        mock_repository.get.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_create_item(mock_uow, mock_repository):
    """Test creating a new item."""
    # Arrange
    with patch("src.services.item_service.ItemRepository", return_value=mock_repository):
        service = ItemService(mock_uow)
        item_in = ItemCreate(
            name="Test Item",
            price=10.0,
            description="Test description"
        )
        
        # Act
        result = await service.create(item_in)
        
        # Assert
        assert result is not None
        assert result.name == "Test Item"
        mock_repository.create.assert_called_once_with(item_in.model_dump())


@pytest.mark.asyncio
async def test_update_with_partial_data(mock_uow, mock_repository):
    """Test updating an item with partial data."""
    # Arrange
    with patch("src.services.item_service.ItemRepository", return_value=mock_repository):
        service = ItemService(mock_uow)
        item_update = ItemUpdate(price=15.0)
        
        # Act
        result = await service.update_with_partial_data(1, item_update)
        
        # Assert
        assert result is not None
        mock_repository.update.assert_called_once_with(1, {"price": 15.0})


@pytest.mark.asyncio
async def test_find_by_name(mock_uow, mock_repository):
    """Test finding an item by name."""
    # Arrange
    with patch("src.services.item_service.ItemRepository", return_value=mock_repository):
        service = ItemService(mock_uow)
        
        # Act
        result = await service.find_by_name("Test Item")
        
        # Assert
        assert result is not None
        assert result.name == "Test Item"
        mock_repository.find_by_name.assert_called_once_with("Test Item") 