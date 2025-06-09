"""
Unit tests for ModelConfigService.

Tests the functionality of the model configuration service including
creating, updating, deleting, and managing model configurations.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.services.model_config_service import ModelConfigService
from src.schemas.model_config import ModelConfigCreate, ModelConfigUpdate
from src.models.model_config import ModelConfig
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock unit of work."""
    uow = MagicMock(spec=UnitOfWork)
    uow.session = AsyncMock()
    uow.commit = AsyncMock()
    uow.rollback = AsyncMock()
    return uow


@pytest.fixture
def mock_model_config_repository():
    """Create a mock model config repository."""
    repo = AsyncMock()
    
    # Create mock model config objects
    mock_model_config = MagicMock(spec=ModelConfig)
    mock_model_config.id = uuid.uuid4()
    mock_model_config.name = "GPT-4 Config"
    mock_model_config.provider = "openai"
    mock_model_config.model_name = "gpt-4o-mini"
    mock_model_config.api_key = "encrypted_api_key"
    mock_model_config.base_url = "https://api.openai.com/v1"
    mock_model_config.max_tokens = 4096
    mock_model_config.temperature = 0.7
    mock_model_config.top_p = 1.0
    mock_model_config.frequency_penalty = 0.0
    mock_model_config.presence_penalty = 0.0
    mock_model_config.is_default = False
    mock_model_config.is_active = True
    mock_model_config.created_at = datetime.now(UTC)
    mock_model_config.updated_at = datetime.now(UTC)
    
    # Setup repository method returns
    repo.get.return_value = mock_model_config
    repo.list.return_value = [mock_model_config]
    repo.create.return_value = mock_model_config
    repo.update.return_value = mock_model_config
    repo.delete.return_value = True
    repo.get_by_name.return_value = mock_model_config
    repo.get_default.return_value = mock_model_config
    repo.get_by_provider.return_value = [mock_model_config]
    
    return repo


@pytest.fixture
def model_config_create_data():
    """Create test data for model config creation."""
    return ModelConfigCreate(
        name="Test Model Config",
        provider="openai",
        model_name="gpt-4o-mini",
        api_key="test_api_key_123",
        base_url="https://api.openai.com/v1",
        max_tokens=2048,
        temperature=0.8,
        top_p=0.9,
        frequency_penalty=0.1,
        presence_penalty=0.1,
        is_default=False
    )


@pytest.fixture
def model_config_update_data():
    """Create test data for model config updates."""
    return ModelConfigUpdate(
        name="Updated Model Config",
        max_tokens=4096,
        temperature=0.5
    )


class TestModelConfigService:
    """Test cases for ModelConfigService."""
    
    @pytest.mark.asyncio
    async def test_create_model_config_success(self, mock_uow, mock_model_config_repository, model_config_create_data):
        """Test successful model config creation."""
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository), \
             patch("src.services.model_config_service.encrypt_api_key") as mock_encrypt:
            
            mock_encrypt.return_value = "encrypted_api_key"
            
            service = ModelConfigService(mock_uow)
            
            result = await service.create(model_config_create_data)
            
            assert result is not None
            assert result.name == "GPT-4 Config"
            assert result.provider == "openai"
            mock_model_config_repository.create.assert_called_once()
            mock_uow.commit.assert_called_once()
            mock_encrypt.assert_called_once_with("test_api_key_123")
    
    @pytest.mark.asyncio
    async def test_create_model_config_validation_error(self, mock_uow, mock_model_config_repository):
        """Test model config creation with invalid data."""
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            # Test with invalid data (unsupported provider)
            invalid_data = ModelConfigCreate(
                name="Invalid Config",
                provider="unsupported_provider",
                model_name="test_model",
                api_key="test_key"
            )
            
            with pytest.raises(ValueError, match="Unsupported provider"):
                await service.create(invalid_data)
    
    @pytest.mark.asyncio
    async def test_get_model_config_by_id(self, mock_uow, mock_model_config_repository):
        """Test getting a model config by ID."""
        config_id = uuid.uuid4()
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.get(config_id)
            
            assert result is not None
            assert result.name == "GPT-4 Config"
            mock_model_config_repository.get.assert_called_once_with(config_id)
    
    @pytest.mark.asyncio
    async def test_get_model_config_not_found(self, mock_uow, mock_model_config_repository):
        """Test getting a non-existent model config."""
        config_id = uuid.uuid4()
        mock_model_config_repository.get.return_value = None
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.get(config_id)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_update_model_config_success(self, mock_uow, mock_model_config_repository, model_config_update_data):
        """Test successful model config update."""
        config_id = uuid.uuid4()
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.update(config_id, model_config_update_data)
            
            assert result is not None
            mock_model_config_repository.update.assert_called_once()
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_model_config_success(self, mock_uow, mock_model_config_repository):
        """Test successful model config deletion."""
        config_id = uuid.uuid4()
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.delete(config_id)
            
            assert result is True
            mock_model_config_repository.delete.assert_called_once_with(config_id)
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_model_configs(self, mock_uow, mock_model_config_repository):
        """Test listing all model configs."""
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.list()
            
            assert len(result) == 1
            assert result[0].name == "GPT-4 Config"
            mock_model_config_repository.list.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_default_model_config(self, mock_uow, mock_model_config_repository):
        """Test getting the default model config."""
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.get_default()
            
            assert result is not None
            mock_model_config_repository.get_default.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_set_default_model_config(self, mock_uow, mock_model_config_repository):
        """Test setting a model config as default."""
        config_id = uuid.uuid4()
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.set_default(config_id)
            
            assert result is not None
            # Should update multiple configs (unset previous default, set new default)
            assert mock_model_config_repository.update.call_count >= 1
            mock_uow.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_configs_by_provider(self, mock_uow, mock_model_config_repository):
        """Test getting model configs by provider."""
        provider = "openai"
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.get_by_provider(provider)
            
            assert len(result) == 1
            assert result[0].provider == "openai"
            mock_model_config_repository.get_by_provider.assert_called_once_with(provider)
    
    @pytest.mark.asyncio
    async def test_validate_model_parameters(self, mock_uow):
        """Test validation of model parameters."""
        service = ModelConfigService(mock_uow)
        
        # Test valid parameters
        valid_params = {
            "temperature": 0.7,
            "max_tokens": 2048,
            "top_p": 0.9,
            "frequency_penalty": 0.1,
            "presence_penalty": 0.1
        }
        
        service._validate_parameters(valid_params)  # Should not raise
        
        # Test invalid parameters
        invalid_params = [
            {"temperature": 2.5},  # > 2.0
            {"temperature": -0.5},  # < 0.0
            {"max_tokens": -1},    # Negative
            {"top_p": 1.5},        # > 1.0
            {"frequency_penalty": 2.5},  # > 2.0
            {"presence_penalty": -2.5},  # < -2.0
        ]
        
        for params in invalid_params:
            with pytest.raises(ValueError):
                service._validate_parameters(params)
    
    @pytest.mark.asyncio
    async def test_validate_provider_support(self, mock_uow):
        """Test validation of provider support."""
        service = ModelConfigService(mock_uow)
        
        # Test supported providers
        supported_providers = ["openai", "anthropic", "azure", "databricks"]
        
        for provider in supported_providers:
            assert service._validate_provider(provider) is True
        
        # Test unsupported provider
        assert service._validate_provider("unsupported_provider") is False
    
    @pytest.mark.asyncio
    async def test_test_model_connection(self, mock_uow, mock_model_config_repository):
        """Test testing model connection."""
        config_id = uuid.uuid4()
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository), \
             patch("src.services.model_config_service.test_model_api") as mock_test:
            
            mock_test.return_value = {"status": "success", "latency": 150}
            
            service = ModelConfigService(mock_uow)
            
            result = await service.test_connection(config_id)
            
            assert result["status"] == "success"
            assert "latency" in result
            mock_test.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_api_key_encryption_decryption(self, mock_uow):
        """Test API key encryption and decryption."""
        service = ModelConfigService(mock_uow)
        
        original_key = "test_api_key_123"
        
        with patch("src.services.model_config_service.encrypt_api_key") as mock_encrypt, \
             patch("src.services.model_config_service.decrypt_api_key") as mock_decrypt:
            
            mock_encrypt.return_value = "encrypted_key"
            mock_decrypt.return_value = original_key
            
            # Test encryption
            encrypted = service._encrypt_api_key(original_key)
            assert encrypted == "encrypted_key"
            
            # Test decryption
            decrypted = service._decrypt_api_key(encrypted)
            assert decrypted == original_key
    
    @pytest.mark.asyncio
    async def test_model_config_metrics(self, mock_uow, mock_model_config_repository):
        """Test model config metrics collection."""
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            metrics = await service.get_model_metrics()
            
            assert "total_configs" in metrics
            assert "active_configs" in metrics
            assert "configs_by_provider" in metrics
            assert "default_config" in metrics
            assert metrics["total_configs"] >= 0
    
    @pytest.mark.asyncio
    async def test_model_usage_tracking(self, mock_uow, mock_model_config_repository):
        """Test model usage tracking."""
        config_id = uuid.uuid4()
        usage_data = {
            "requests_count": 100,
            "tokens_used": 50000,
            "average_latency": 250,
            "error_rate": 0.02
        }
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.update_usage_metrics(config_id, usage_data)
            
            assert result is not None
            mock_model_config_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_model_config_clone(self, mock_uow, mock_model_config_repository):
        """Test model config cloning."""
        config_id = uuid.uuid4()
        new_name = "Cloned Config"
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            cloned_config = await service.clone_config(config_id, new_name)
            
            assert cloned_config is not None
            mock_model_config_repository.get.assert_called_once_with(config_id)
            mock_model_config_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_model_config_validation_comprehensive(self, mock_uow):
        """Test comprehensive model config validation."""
        service = ModelConfigService(mock_uow)
        
        # Test complete config validation
        config_data = {
            "name": "Test Config",
            "provider": "openai",
            "model_name": "gpt-4o-mini",
            "api_key": "test_key",
            "max_tokens": 2048,
            "temperature": 0.7,
            "top_p": 0.9
        }
        
        # Should not raise exception
        service._validate_config(config_data)
        
        # Test config with missing required fields
        incomplete_config = {
            "name": "Incomplete Config"
            # Missing provider, model_name, api_key
        }
        
        with pytest.raises(ValueError, match="Missing required field"):
            service._validate_config(incomplete_config)
    
    @pytest.mark.asyncio
    async def test_rate_limit_configuration(self, mock_uow, mock_model_config_repository):
        """Test rate limit configuration for models."""
        config_id = uuid.uuid4()
        rate_limit_config = {
            "requests_per_minute": 60,
            "tokens_per_minute": 60000,
            "concurrent_requests": 5
        }
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            result = await service.configure_rate_limits(config_id, rate_limit_config)
            
            assert result is not None
            mock_model_config_repository.update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_model_config_export_import(self, mock_uow, mock_model_config_repository):
        """Test model config export and import."""
        config_id = uuid.uuid4()
        
        with patch("src.services.model_config_service.ModelConfigRepository", return_value=mock_model_config_repository):
            service = ModelConfigService(mock_uow)
            
            # Test export
            export_data = await service.export_config(config_id)
            
            assert export_data is not None
            assert "name" in export_data
            assert "provider" in export_data
            assert "model_name" in export_data
            # API key should be excluded from export for security
            assert "api_key" not in export_data
            
            # Test import
            import_data = export_data.copy()
            import_data["api_key"] = "new_api_key"
            
            imported_config = await service.import_config(import_data)
            
            assert imported_config is not None
            mock_model_config_repository.create.assert_called()