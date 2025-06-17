"""
Unit tests for ModelConfigService.

Tests the functionality of model configuration operations including
model retrieval, creation, updates, and management operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from fastapi import HTTPException

from src.services.model_config_service import ModelConfigService
from src.models.model_config import ModelConfig


# Mock models
class MockModelConfig:
    def __init__(self, id=1, key="gpt-4", name="GPT-4", provider="openai",
                 temperature=0.7, context_window=8192, max_output_tokens=4096,
                 extended_thinking=False, enabled=True, created_at=None, updated_at=None):
        self.id = id
        self.key = key
        self.name = name
        self.provider = provider
        self.temperature = temperature
        self.context_window = context_window
        self.max_output_tokens = max_output_tokens
        self.extended_thinking = extended_thinking
        self.enabled = enabled
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


class MockPydanticModelData:
    """Mock Pydantic model with model_dump method."""
    def __init__(self, **kwargs):
        self.data = kwargs
        # Set attributes for direct access
        for k, v in kwargs.items():
            setattr(self, k, v)
    
    def model_dump(self, exclude_unset=False):
        if exclude_unset:
            return {k: v for k, v in self.data.items() if v is not None}
        return self.data


class MockDictModelData:
    """Mock model with dict method."""
    def __init__(self, **kwargs):
        self.data = kwargs
        # Set attributes for direct access
        for k, v in kwargs.items():
            setattr(self, k, v)
    
    def dict(self, exclude_unset=False):
        if exclude_unset:
            return {k: v for k, v in self.data.items() if v is not None}
        return self.data


@pytest.fixture
def mock_repository():
    """Create a mock ModelConfigRepository."""
    return AsyncMock()


@pytest.fixture
def mock_uow():
    """Create a mock Unit of Work."""
    uow = MagicMock()
    uow.model_config_repository = AsyncMock()
    return uow


@pytest.fixture
def model_config_service(mock_repository):
    """Create a ModelConfigService instance with mock repository."""
    return ModelConfigService(mock_repository)


@pytest.fixture
def mock_model_config():
    """Create a mock model configuration."""
    return MockModelConfig()


@pytest.fixture
def mock_model_config_list():
    """Create a list of mock model configurations."""
    return [
        MockModelConfig(id=1, key="gpt-4", name="GPT-4", enabled=True),
        MockModelConfig(id=2, key="claude-3-opus", name="Claude 3 Opus", enabled=False),
        MockModelConfig(id=3, key="gpt-3.5-turbo", name="GPT-3.5 Turbo", enabled=True)
    ]


class TestModelConfigService:
    """Test cases for ModelConfigService."""
    
    @pytest.mark.asyncio
    async def test_from_unit_of_work(self, mock_uow):
        """Test creating service instance from unit of work."""
        service = await ModelConfigService.from_unit_of_work(mock_uow)
        
        assert isinstance(service, ModelConfigService)
        assert service.repository == mock_uow.model_config_repository
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, model_config_service, mock_model_config_list):
        """Test successful retrieval of all model configurations."""
        model_config_service.repository.find_all.return_value = mock_model_config_list
        
        result = await model_config_service.find_all()
        
        assert len(result) == 3
        assert result == mock_model_config_list
        model_config_service.repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_empty(self, model_config_service):
        """Test retrieval when no model configurations exist."""
        model_config_service.repository.find_all.return_value = []
        
        result = await model_config_service.find_all()
        
        assert result == []
        model_config_service.repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_enabled_models_success(self, model_config_service):
        """Test successful retrieval of enabled model configurations."""
        enabled_models = [
            MockModelConfig(id=1, key="gpt-4", enabled=True),
            MockModelConfig(id=3, key="gpt-3.5-turbo", enabled=True)
        ]
        model_config_service.repository.find_enabled_models.return_value = enabled_models
        
        result = await model_config_service.find_enabled_models()
        
        assert len(result) == 2
        assert all(model.enabled for model in result)
        model_config_service.repository.find_enabled_models.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_by_key_success(self, model_config_service, mock_model_config):
        """Test successful model configuration retrieval by key."""
        model_config_service.repository.find_by_key.return_value = mock_model_config
        
        result = await model_config_service.find_by_key("gpt-4")
        
        assert result == mock_model_config
        model_config_service.repository.find_by_key.assert_called_once_with("gpt-4")
    
    @pytest.mark.asyncio
    async def test_find_by_key_not_found(self, model_config_service):
        """Test model configuration retrieval when key not found."""
        model_config_service.repository.find_by_key.return_value = None
        
        result = await model_config_service.find_by_key("nonexistent")
        
        assert result is None
        model_config_service.repository.find_by_key.assert_called_once_with("nonexistent")
    
    @pytest.mark.asyncio
    async def test_create_model_config_success_pydantic(self, model_config_service, mock_model_config):
        """Test successful model configuration creation with Pydantic model."""
        model_data = MockPydanticModelData(
            key="new-model",
            name="New Model",
            provider="openai",
            temperature=0.8
        )
        
        model_config_service.repository.find_by_key = AsyncMock(return_value=None)
        model_config_service.repository.create = AsyncMock(return_value=mock_model_config)
        
        result = await model_config_service.create_model_config(model_data)
        
        assert result == mock_model_config
        model_config_service.repository.find_by_key.assert_called_once_with("new-model")
        model_config_service.repository.create.assert_called_once_with({
            "key": "new-model",
            "name": "New Model", 
            "provider": "openai",
            "temperature": 0.8
        })
    
    @pytest.mark.asyncio
    async def test_create_model_config_success_dict_method(self, model_config_service, mock_model_config):
        """Test successful model configuration creation with object having dict method."""
        model_data = MockDictModelData(
            key="new-model",
            name="New Model",
            provider="anthropic"
        )
        
        model_config_service.repository.find_by_key = AsyncMock(return_value=None)
        model_config_service.repository.create = AsyncMock(return_value=mock_model_config)
        
        result = await model_config_service.create_model_config(model_data)
        
        assert result == mock_model_config
        model_config_service.repository.create.assert_called_once_with({
            "key": "new-model",
            "name": "New Model",
            "provider": "anthropic"
        })
    
    @pytest.mark.asyncio
    async def test_create_model_config_success_plain_dict(self, model_config_service, mock_model_config):
        """Test successful model configuration creation with plain dictionary."""
        # Create a custom dict-like object that has key attribute access
        class DictWithKeyAccess(dict):
            def __getattr__(self, key):
                try:
                    return self[key]
                except KeyError:
                    raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")
        
        model_data = DictWithKeyAccess({
            "key": "new-model",
            "name": "New Model",
            "provider": "google"
        })
        
        model_config_service.repository.find_by_key = AsyncMock(return_value=None)
        model_config_service.repository.create = AsyncMock(return_value=mock_model_config)
        
        result = await model_config_service.create_model_config(model_data)
        
        assert result == mock_model_config
        model_config_service.repository.create.assert_called_once_with(dict(model_data))
    
    @pytest.mark.asyncio
    async def test_create_model_config_success_no_dump_methods(self, model_config_service, mock_model_config):
        """Test successful model configuration creation with object having no dump methods."""
        # Create a dict-like object that works with dict() conversion
        model_data = {
            "key": "simple-model",
            "name": "Simple Model",
            "provider": "openai"
        }
        # Add attributes for direct access like obj.key
        class DictWithAttrs(dict):
            def __getattr__(self, name):
                try:
                    return self[name]
                except KeyError:
                    raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
        
        model_data = DictWithAttrs(model_data)
        
        model_config_service.repository.find_by_key = AsyncMock(return_value=None)
        model_config_service.repository.create = AsyncMock(return_value=mock_model_config)
        
        result = await model_config_service.create_model_config(model_data)
        
        assert result == mock_model_config
        model_config_service.repository.create.assert_called_once_with({
            "key": "simple-model",
            "name": "Simple Model",
            "provider": "openai"
        })
    
    @pytest.mark.asyncio
    async def test_create_model_config_already_exists(self, model_config_service, mock_model_config):
        """Test model configuration creation when key already exists."""
        model_data = MockPydanticModelData(key="existing-model", name="Existing Model")
        
        model_config_service.repository.find_by_key = AsyncMock(return_value=mock_model_config)
        model_config_service.repository.create = AsyncMock()
        
        with pytest.raises(ValueError, match="Model with key existing-model already exists"):
            await model_config_service.create_model_config(model_data)
        
        model_config_service.repository.create.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_update_model_config_success_pydantic(self, model_config_service, mock_model_config):
        """Test successful model configuration update with Pydantic model."""
        model_data = MockPydanticModelData(
            name="Updated Model",
            temperature=0.9
        )
        
        model_config_service.repository.find_by_key.return_value = mock_model_config
        model_config_service.repository.update.return_value = mock_model_config
        
        result = await model_config_service.update_model_config("gpt-4", model_data)
        
        assert result == mock_model_config
        model_config_service.repository.find_by_key.assert_called_once_with("gpt-4")
        model_config_service.repository.update.assert_called_once_with(
            mock_model_config.id,
            {"name": "Updated Model", "temperature": 0.9}
        )
    
    @pytest.mark.asyncio
    async def test_update_model_config_success_dict_method(self, model_config_service, mock_model_config):
        """Test successful model configuration update with dict method."""
        model_data = MockDictModelData(
            name="Updated Model",
            provider="anthropic"
        )
        
        model_config_service.repository.find_by_key.return_value = mock_model_config
        model_config_service.repository.update.return_value = mock_model_config
        
        result = await model_config_service.update_model_config("gpt-4", model_data)
        
        assert result == mock_model_config
        model_config_service.repository.update.assert_called_once_with(
            mock_model_config.id,
            {"name": "Updated Model", "provider": "anthropic"}
        )
    
    @pytest.mark.asyncio
    async def test_update_model_config_success_no_dump_methods(self, model_config_service, mock_model_config):
        """Test successful model configuration update with object having no dump methods."""
        # Create a dict-like object that works with dict() conversion
        model_data = {
            "name": "Updated Simple Model",
            "temperature": 0.8
        }
        # Add attributes for direct access like obj.key
        class DictWithAttrs(dict):
            def __getattr__(self, name):
                try:
                    return self[name]
                except KeyError:
                    raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
        
        model_data = DictWithAttrs(model_data)
        
        model_config_service.repository.find_by_key.return_value = mock_model_config
        model_config_service.repository.update.return_value = mock_model_config
        
        result = await model_config_service.update_model_config("gpt-4", model_data)
        
        assert result == mock_model_config
        model_config_service.repository.update.assert_called_once_with(
            mock_model_config.id,
            {"name": "Updated Simple Model", "temperature": 0.8}
        )
    
    @pytest.mark.asyncio
    async def test_update_model_config_not_found(self, model_config_service):
        """Test model configuration update when model not found."""
        model_data = MockPydanticModelData(name="Updated Model")
        
        model_config_service.repository.find_by_key.return_value = None
        
        result = await model_config_service.update_model_config("nonexistent", model_data)
        
        assert result is None
        model_config_service.repository.update.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_toggle_model_enabled_success(self, model_config_service, mock_model_config):
        """Test successful model enabled status toggle."""
        model_config_service.repository.toggle_enabled.return_value = True
        model_config_service.repository.find_by_key.return_value = mock_model_config
        
        result = await model_config_service.toggle_model_enabled("gpt-4", False)
        
        assert result == mock_model_config
        model_config_service.repository.toggle_enabled.assert_called_once_with("gpt-4", False)
        model_config_service.repository.find_by_key.assert_called_once_with("gpt-4")
    
    @pytest.mark.asyncio
    async def test_toggle_model_enabled_not_found(self, model_config_service):
        """Test model enabled toggle when model not found."""
        model_config_service.repository.toggle_enabled.return_value = False
        
        result = await model_config_service.toggle_model_enabled("nonexistent", True)
        
        assert result is None
        model_config_service.repository.find_by_key.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_toggle_model_enabled_repository_error(self, model_config_service):
        """Test model enabled toggle when repository raises error."""
        model_config_service.repository.toggle_enabled.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await model_config_service.toggle_model_enabled("gpt-4", True)
    
    @pytest.mark.asyncio
    async def test_delete_model_config_success(self, model_config_service):
        """Test successful model configuration deletion."""
        model_config_service.repository.delete_by_key.return_value = True
        
        result = await model_config_service.delete_model_config("gpt-4")
        
        assert result is True
        model_config_service.repository.delete_by_key.assert_called_once_with("gpt-4")
    
    @pytest.mark.asyncio
    async def test_delete_model_config_not_found(self, model_config_service):
        """Test model configuration deletion when model not found."""
        model_config_service.repository.delete_by_key.return_value = False
        
        result = await model_config_service.delete_model_config("nonexistent")
        
        assert result is False
        model_config_service.repository.delete_by_key.assert_called_once_with("nonexistent")
    
    @pytest.mark.asyncio
    async def test_enable_all_models_success(self, model_config_service, mock_model_config_list):
        """Test successful enabling of all model configurations."""
        model_config_service.repository.enable_all_models.return_value = True
        model_config_service.repository.find_all.return_value = mock_model_config_list
        
        result = await model_config_service.enable_all_models()
        
        assert len(result) == 3
        assert result == mock_model_config_list
        model_config_service.repository.enable_all_models.assert_called_once()
        model_config_service.repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_enable_all_models_repository_failure(self, model_config_service, mock_model_config_list):
        """Test enabling all models when repository operation fails."""
        model_config_service.repository.enable_all_models.return_value = False
        model_config_service.repository.find_all.return_value = mock_model_config_list
        
        with patch('src.services.model_config_service.logger') as mock_logger:
            result = await model_config_service.enable_all_models()
            
            assert len(result) == 3
            mock_logger.warning.assert_called_once_with("Failed to enable all models")
    
    @pytest.mark.asyncio
    async def test_enable_all_models_repository_error(self, model_config_service):
        """Test enabling all models when repository raises error."""
        model_config_service.repository.enable_all_models.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await model_config_service.enable_all_models()
    
    @pytest.mark.asyncio
    async def test_disable_all_models_success(self, model_config_service, mock_model_config_list):
        """Test successful disabling of all model configurations."""
        model_config_service.repository.disable_all_models.return_value = True
        model_config_service.repository.find_all.return_value = mock_model_config_list
        
        result = await model_config_service.disable_all_models()
        
        assert len(result) == 3
        assert result == mock_model_config_list
        model_config_service.repository.disable_all_models.assert_called_once()
        model_config_service.repository.find_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_disable_all_models_repository_failure(self, model_config_service, mock_model_config_list):
        """Test disabling all models when repository operation fails."""
        model_config_service.repository.disable_all_models.return_value = False
        model_config_service.repository.find_all.return_value = mock_model_config_list
        
        with patch('src.services.model_config_service.logger') as mock_logger:
            result = await model_config_service.disable_all_models()
            
            assert len(result) == 3
            mock_logger.warning.assert_called_once_with("Failed to disable all models")
    
    @pytest.mark.asyncio
    async def test_disable_all_models_repository_error(self, model_config_service):
        """Test disabling all models when repository raises error."""
        model_config_service.repository.disable_all_models.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await model_config_service.disable_all_models()
    
    @pytest.mark.asyncio
    async def test_get_model_config_from_repository(self, model_config_service, mock_model_config):
        """Test getting model configuration from repository."""
        model_config_service.repository.find_by_key.return_value = mock_model_config
        
        with patch('src.services.model_config_service.ApiKeysService.get_provider_api_key', return_value="test-api-key"):
            result = await model_config_service.get_model_config("gpt-4")
            
            expected_config = {
                "key": "gpt-4",
                "name": "GPT-4",
                "provider": "openai",
                "temperature": 0.7,
                "context_window": 8192,
                "max_output_tokens": 4096,
                "extended_thinking": False,
                "enabled": True,
                "api_key": "test-api-key"
            }
            
            assert result == expected_config
            model_config_service.repository.find_by_key.assert_called_once_with("gpt-4")
    
    @pytest.mark.asyncio
    async def test_get_model_config_fallback_to_utility(self, model_config_service):
        """Test getting model configuration fallback to utility function."""
        model_config_service.repository.find_by_key.return_value = None
        
        mock_config = {
            "key": "gpt-4",
            "name": "GPT-4",
            "provider": "openai",
            "temperature": 0.7,
            "context_window": 8192,
            "max_output_tokens": 4096,
            "extended_thinking": False,
            "enabled": True
        }
        
        with patch('src.services.model_config_service.get_model_config', return_value=mock_config):
            with patch('src.services.model_config_service.ApiKeysService.get_provider_api_key', return_value="test-api-key"):
                result = await model_config_service.get_model_config("gpt-4")
                
                expected_config = mock_config.copy()
                expected_config["api_key"] = "test-api-key"
                
                assert result == expected_config
    
    @pytest.mark.asyncio
    async def test_get_model_config_not_found(self, model_config_service):
        """Test getting model configuration when model not found anywhere."""
        model_config_service.repository.find_by_key.return_value = None
        
        with patch('src.services.model_config_service.get_model_config', return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                await model_config_service.get_model_config("nonexistent")
            
            assert exc_info.value.status_code == 500
            assert "Failed to get model configuration" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_model_config_no_api_key(self, model_config_service, mock_model_config):
        """Test getting model configuration when no API key found."""
        model_config_service.repository.find_by_key.return_value = mock_model_config
        
        with patch('src.services.model_config_service.ApiKeysService.get_provider_api_key', return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                await model_config_service.get_model_config("gpt-4")
            
            assert exc_info.value.status_code == 500
            assert "Failed to get model configuration" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_model_config_databricks_provider_in_apps(self, model_config_service):
        """Test getting Databricks model configuration in Databricks Apps environment."""
        databricks_model = MockModelConfig(
            key="databricks-llama",
            name="Databricks Llama",
            provider="databricks"
        )
        model_config_service.repository.find_by_key = AsyncMock(return_value=databricks_model)
        
        with patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True):
            result = await model_config_service.get_model_config("databricks-llama")
            
            # Should not include API key for Databricks Apps environment
            assert "api_key" not in result
            assert result["provider"] == "databricks"
    
    @pytest.mark.asyncio
    async def test_get_model_config_non_databricks_in_apps_no_api_key(self, model_config_service, mock_model_config):
        """Test getting non-Databricks model in Apps environment without API key."""
        model_config_service.repository.find_by_key = AsyncMock(return_value=mock_model_config)
        
        with patch('src.services.model_config_service.ApiKeysService.get_provider_api_key', return_value=None):
            with patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True):
                with patch('src.services.model_config_service.logger') as mock_logger:
                    result = await model_config_service.get_model_config("gpt-4")
                    
                    # Should allow the request to proceed with warning
                    assert result["provider"] == "openai"
                    assert "api_key" not in result
                    mock_logger.warning.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_model_config_databricks_import_error(self, model_config_service):
        """Test getting model configuration when Databricks auth import fails."""
        databricks_model = MockModelConfig(
            key="databricks-llama",
            provider="databricks"
        )
        model_config_service.repository.find_by_key = AsyncMock(return_value=databricks_model)
        
        with patch('src.utils.databricks_auth.is_databricks_apps_environment', side_effect=ImportError):
            with patch('src.services.model_config_service.ApiKeysService.get_provider_api_key', return_value=None):
                with pytest.raises(HTTPException):
                    await model_config_service.get_model_config("databricks-llama")
    
    @pytest.mark.asyncio
    async def test_get_model_config_general_exception(self, model_config_service):
        """Test getting model configuration when general exception occurs."""
        model_config_service.repository.find_by_key.side_effect = Exception("Database connection error")
        
        with pytest.raises(HTTPException) as exc_info:
            await model_config_service.get_model_config("gpt-4")
        
        assert exc_info.value.status_code == 500
        assert "Failed to get model configuration" in str(exc_info.value.detail)