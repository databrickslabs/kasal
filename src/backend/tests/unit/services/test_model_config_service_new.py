"""
Comprehensive tests for ModelConfigService.
"""
import pytest
from unittest.mock import AsyncMock, Mock
from src.services.model_config_service import ModelConfigService
from src.models.model_config import ModelConfig


class TestModelConfigServiceInit:
    """Test ModelConfigService initialization."""

    def test_init_with_group_id(self):
        """Test initialization with group_id."""
        session = AsyncMock()
        service = ModelConfigService(session=session, group_id="test-group")
        
        assert service.group_id == "test-group"
        assert service.repository is not None

    def test_init_without_group_id(self):
        """Test initialization without group_id."""
        session = AsyncMock()
        service = ModelConfigService(session=session)
        
        assert service.group_id is None
        assert service.repository is not None


class TestModelConfigServiceFindAll:
    """Test find_all method."""

    @pytest.mark.asyncio
    async def test_find_all_success(self):
        """Test finding all model configurations."""
        session = AsyncMock()
        service = ModelConfigService(session=session)
        
        mock_models = [
            Mock(id=1, key="gpt-4", name="GPT-4"),
            Mock(id=2, key="claude-3", name="Claude 3")
        ]
        service.repository.find_all = AsyncMock(return_value=mock_models)
        
        result = await service.find_all()
        
        assert len(result) == 2
        assert result[0].key == "gpt-4"
        assert result[1].key == "claude-3"
        service.repository.find_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_all_empty(self):
        """Test finding all when no models exist."""
        session = AsyncMock()
        service = ModelConfigService(session=session)
        
        service.repository.find_all = AsyncMock(return_value=[])
        
        result = await service.find_all()
        
        assert len(result) == 0
        service.repository.find_all.assert_called_once()


class TestModelConfigServiceFindEnabledModels:
    """Test find_enabled_models method."""

    @pytest.mark.asyncio
    async def test_find_enabled_models_success(self):
        """Test finding enabled model configurations."""
        session = AsyncMock()
        service = ModelConfigService(session=session)
        
        mock_models = [
            Mock(id=1, key="gpt-4", enabled=True),
            Mock(id=2, key="claude-3", enabled=True)
        ]
        service.repository.find_enabled_models = AsyncMock(return_value=mock_models)
        
        result = await service.find_enabled_models()
        
        assert len(result) == 2
        assert all(m.enabled for m in result)
        service.repository.find_enabled_models.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_enabled_models_empty(self):
        """Test finding enabled models when none exist."""
        session = AsyncMock()
        service = ModelConfigService(session=session)
        
        service.repository.find_enabled_models = AsyncMock(return_value=[])
        
        result = await service.find_enabled_models()
        
        assert len(result) == 0


class TestModelConfigServiceFindByKey:
    """Test find_by_key method."""

    @pytest.mark.asyncio
    async def test_find_by_key_success(self):
        """Test finding model by key."""
        session = AsyncMock()
        service = ModelConfigService(session=session)

        mock_model = Mock(id=1, key="gpt-4", name="GPT-4")
        service.repository.find_by_key = AsyncMock(return_value=mock_model)

        result = await service.find_by_key("gpt-4")

        assert result is not None
        assert result == mock_model
        service.repository.find_by_key.assert_called_once_with("gpt-4")

    @pytest.mark.asyncio
    async def test_find_by_key_not_found(self):
        """Test finding model by key when not found."""
        session = AsyncMock()
        service = ModelConfigService(session=session)
        
        service.repository.find_by_key = AsyncMock(return_value=None)
        
        result = await service.find_by_key("nonexistent")
        
        assert result is None
        service.repository.find_by_key.assert_called_once_with("nonexistent")


class TestModelConfigServiceCreateModelConfig:
    """Test create_model_config method."""

    @pytest.mark.asyncio
    async def test_create_model_config_success(self):
        """Test creating a new model configuration."""
        session = AsyncMock()
        service = ModelConfigService(session=session)
        
        # Mock model data with model_dump method
        model_data = Mock()
        model_data.key = "new-model"
        model_data.model_dump = Mock(return_value={"key": "new-model", "name": "New Model"})
        
        service.repository.find_by_key = AsyncMock(return_value=None)
        service.repository.create = AsyncMock(return_value=Mock(id=1, key="new-model"))
        
        result = await service.create_model_config(model_data)
        
        assert result.key == "new-model"
        service.repository.find_by_key.assert_called_once_with("new-model")
        service.repository.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_model_config_already_exists(self):
        """Test creating model when key already exists."""
        session = AsyncMock()
        service = ModelConfigService(session=session)
        
        model_data = Mock()
        model_data.key = "existing-model"
        
        service.repository.find_by_key = AsyncMock(return_value=Mock(key="existing-model"))
        
        with pytest.raises(ValueError, match="Model with key existing-model already exists"):
            await service.create_model_config(model_data)

    @pytest.mark.asyncio
    async def test_create_model_config_with_dict_method(self):
        """Test creating model with dict method (legacy Pydantic)."""
        session = AsyncMock()
        service = ModelConfigService(session=session)
        
        model_data = Mock()
        model_data.key = "new-model"
        model_data.dict = Mock(return_value={"key": "new-model", "name": "New Model"})
        # Remove model_dump to test dict fallback
        delattr(model_data, 'model_dump') if hasattr(model_data, 'model_dump') else None
        
        service.repository.find_by_key = AsyncMock(return_value=None)
        service.repository.create = AsyncMock(return_value=Mock(id=1, key="new-model"))
        
        result = await service.create_model_config(model_data)
        
        assert result.key == "new-model"
        model_data.dict.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_model_config_with_dict_object(self):
        """Test creating model with plain dict."""
        session = AsyncMock()
        service = ModelConfigService(session=session)

        # Create a mock object that acts like a dict but has a key attribute
        model_data = Mock()
        model_data.key = "new-model"
        # Make it iterable like a dict
        model_data.__iter__ = Mock(return_value=iter([("key", "new-model"), ("name", "New Model")]))

        service.repository.find_by_key = AsyncMock(return_value=None)
        service.repository.create = AsyncMock(return_value=Mock(id=1, key="new-model"))

        result = await service.create_model_config(model_data)

        assert result.key == "new-model"


class TestModelConfigServiceUpdateModelConfig:
    """Test update_model_config method."""

    @pytest.mark.asyncio
    async def test_update_model_config_success(self):
        """Test updating an existing model configuration."""
        session = AsyncMock()
        service = ModelConfigService(session=session)

        existing_model = Mock(id=1, key="gpt-4", name="GPT-4")
        update_data = Mock()
        update_data.model_dump = Mock(return_value={"name": "GPT-4 Updated"})

        service.repository.find_by_key = AsyncMock(return_value=existing_model)
        service.repository.update = AsyncMock(return_value=Mock(id=1, key="gpt-4", name="GPT-4 Updated"))

        result = await service.update_model_config("gpt-4", update_data)

        assert result is not None
        service.repository.find_by_key.assert_called_once_with("gpt-4")
        service.repository.update.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_model_config_not_found(self):
        """Test updating model that doesn't exist - returns None."""
        session = AsyncMock()
        service = ModelConfigService(session=session)

        update_data = Mock()

        service.repository.find_by_key = AsyncMock(return_value=None)

        result = await service.update_model_config("nonexistent", update_data)

        assert result is None
        service.repository.find_by_key.assert_called_once_with("nonexistent")


class TestModelConfigServiceDeleteModelConfig:
    """Test delete_model_config method."""

    @pytest.mark.asyncio
    async def test_delete_model_config_success(self):
        """Test deleting an existing model configuration."""
        session = AsyncMock()
        service = ModelConfigService(session=session)

        service.repository.delete_by_key = AsyncMock(return_value=True)

        result = await service.delete_model_config("gpt-4")

        assert result is True
        service.repository.delete_by_key.assert_called_once_with("gpt-4")

    @pytest.mark.asyncio
    async def test_delete_model_config_not_found(self):
        """Test deleting model that doesn't exist - returns False."""
        session = AsyncMock()
        service = ModelConfigService(session=session)

        service.repository.delete_by_key = AsyncMock(return_value=False)

        result = await service.delete_model_config("nonexistent")

        assert result is False
        service.repository.delete_by_key.assert_called_once_with("nonexistent")

