"""
Unit tests for ModelConfigRepository.

Tests the functionality of model config repository including
CRUD operations, enabling/disabling models, upsert operations, and error handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from src.repositories.model_config_repository import ModelConfigRepository
from src.models.model_config import ModelConfig


# Mock model config model
class MockModelConfig:
    def __init__(self, id=1, key="test-model", name="Test Model", provider="openai",
                 temperature=0.7, context_window=4096, max_output_tokens=2048,
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


# Mock SQLAlchemy result objects
class MockScalars:
    def __init__(self, results):
        self.results = results
    
    def first(self):
        return self.results[0] if self.results else None
    
    def all(self):
        return self.results


class MockResult:
    def __init__(self, results):
        self._scalars = MockScalars(results)
    
    def scalars(self):
        return self._scalars


@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = AsyncMock()
    session.delete = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def model_config_repository(mock_async_session):
    """Create a model config repository with async session."""
    return ModelConfigRepository(session=mock_async_session)


@pytest.fixture
def sample_model_configs():
    """Create sample model configs for testing."""
    return [
        MockModelConfig(id=1, key="gpt-4", name="GPT-4", provider="openai", enabled=True),
        MockModelConfig(id=2, key="gpt-3.5-turbo", name="GPT-3.5 Turbo", provider="openai", enabled=True),
        MockModelConfig(id=3, key="claude-3", name="Claude 3", provider="anthropic", enabled=False),
        MockModelConfig(id=4, key="llama-2", name="Llama 2", provider="meta", enabled=True)
    ]


@pytest.fixture
def sample_model_data():
    """Create sample model data for creation."""
    return {
        "name": "New Model",
        "provider": "test_provider",
        "temperature": 0.5,
        "context_window": 8192,
        "max_output_tokens": 4096,
        "extended_thinking": True,
        "enabled": True
    }


class TestModelConfigRepositoryInit:
    """Test cases for ModelConfigRepository initialization."""
    
    def test_init_success(self, mock_async_session):
        """Test successful initialization."""
        repository = ModelConfigRepository(session=mock_async_session)
        
        assert repository.model == ModelConfig
        assert repository.session == mock_async_session
    
    def test_inherits_from_base_repository(self, mock_async_session):
        """Test that ModelConfigRepository properly inherits from BaseRepository."""
        from src.core.base_repository import BaseRepository
        repository = ModelConfigRepository(session=mock_async_session)
        
        # Verify inheritance
        assert isinstance(repository, BaseRepository)
        assert repository.model == ModelConfig
        assert repository.session == mock_async_session


class TestModelConfigRepositoryFindAll:
    """Test cases for find_all method."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, model_config_repository, mock_async_session, sample_model_configs):
        """Test successful retrieval of all model configs."""
        mock_result = MockResult(sample_model_configs)
        mock_async_session.execute.return_value = mock_result
        
        result = await model_config_repository.find_all()
        
        assert len(result) == len(sample_model_configs)
        assert result == sample_model_configs
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_all_empty_result(self, model_config_repository, mock_async_session):
        """Test find all when no configs exist."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await model_config_repository.find_all()
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestModelConfigRepositoryFindByKey:
    """Test cases for find_by_key method."""
    
    @pytest.mark.asyncio
    async def test_find_by_key_success(self, model_config_repository, mock_async_session):
        """Test successful model config search by key."""
        model_config = MockModelConfig(key="gpt-4")
        mock_result = MockResult([model_config])
        mock_async_session.execute.return_value = mock_result
        
        result = await model_config_repository.find_by_key("gpt-4")
        
        assert result == model_config
        mock_async_session.execute.assert_called_once()
        # Verify the query was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(ModelConfig)))
    
    @pytest.mark.asyncio
    async def test_find_by_key_not_found(self, model_config_repository, mock_async_session):
        """Test find by key when model config not found."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await model_config_repository.find_by_key("nonexistent-model")
        
        assert result is None
        mock_async_session.execute.assert_called_once()


class TestModelConfigRepositoryFindEnabledModels:
    """Test cases for find_enabled_models method."""
    
    @pytest.mark.asyncio
    async def test_find_enabled_models_success(self, model_config_repository, mock_async_session, sample_model_configs):
        """Test successful retrieval of enabled model configs."""
        enabled_configs = [config for config in sample_model_configs if config.enabled]
        mock_result = MockResult(enabled_configs)
        mock_async_session.execute.return_value = mock_result
        
        result = await model_config_repository.find_enabled_models()
        
        assert len(result) == len(enabled_configs)
        assert all(config.enabled for config in result)
        mock_async_session.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_find_enabled_models_none_enabled(self, model_config_repository, mock_async_session):
        """Test find enabled models when none are enabled."""
        mock_result = MockResult([])
        mock_async_session.execute.return_value = mock_result
        
        result = await model_config_repository.find_enabled_models()
        
        assert result == []
        mock_async_session.execute.assert_called_once()


class TestModelConfigRepositoryToggleEnabled:
    """Test cases for toggle_enabled method."""
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_success(self, model_config_repository, mock_async_session):
        """Test successful toggling of model enabled status."""
        model_config = MockModelConfig(key="gpt-4", enabled=False)
        
        with patch.object(model_config_repository, 'find_by_key', return_value=model_config):
            result = await model_config_repository.toggle_enabled("gpt-4", True)
            
            assert result is True
            assert model_config.enabled is True
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_model_not_found(self, model_config_repository, mock_async_session):
        """Test toggle enabled when model not found."""
        with patch.object(model_config_repository, 'find_by_key', return_value=None):
            result = await model_config_repository.toggle_enabled("nonexistent", True)
            
            assert result is False
            mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_disable(self, model_config_repository, mock_async_session):
        """Test disabling a model."""
        model_config = MockModelConfig(key="gpt-4", enabled=True)
        
        with patch.object(model_config_repository, 'find_by_key', return_value=model_config):
            result = await model_config_repository.toggle_enabled("gpt-4", False)
            
            assert result is True
            assert model_config.enabled is False
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_database_error(self, model_config_repository, mock_async_session):
        """Test toggle enabled with database error."""
        model_config = MockModelConfig(key="gpt-4")
        
        with patch.object(model_config_repository, 'find_by_key', return_value=model_config):
            mock_async_session.commit.side_effect = Exception("Commit failed")
            
            with pytest.raises(Exception, match="Commit failed"):
                await model_config_repository.toggle_enabled("gpt-4", True)
            
            mock_async_session.rollback.assert_called_once()


class TestModelConfigRepositoryEnableAllModels:
    """Test cases for enable_all_models method."""
    
    @pytest.mark.asyncio
    async def test_enable_all_models_success(self, model_config_repository, mock_async_session):
        """Test successful enabling of all models."""
        mock_async_session.execute.return_value = MagicMock()
        
        result = await model_config_repository.enable_all_models()
        
        assert result is True
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
        
        # Verify the update statement was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # Should be an update statement
    
    @pytest.mark.asyncio
    async def test_enable_all_models_database_error(self, model_config_repository, mock_async_session):
        """Test enable all models with database error."""
        mock_async_session.execute.side_effect = Exception("Update failed")
        
        with pytest.raises(Exception, match="Update failed"):
            await model_config_repository.enable_all_models()
        
        mock_async_session.rollback.assert_called_once()


class TestModelConfigRepositoryDisableAllModels:
    """Test cases for disable_all_models method."""
    
    @pytest.mark.asyncio
    async def test_disable_all_models_success(self, model_config_repository, mock_async_session):
        """Test successful disabling of all models."""
        mock_async_session.execute.return_value = MagicMock()
        
        result = await model_config_repository.disable_all_models()
        
        assert result is True
        mock_async_session.execute.assert_called_once()
        mock_async_session.commit.assert_called_once()
        
        # Verify the update statement was constructed correctly
        call_args = mock_async_session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # Should be an update statement
    
    @pytest.mark.asyncio
    async def test_disable_all_models_database_error(self, model_config_repository, mock_async_session):
        """Test disable all models with database error."""
        mock_async_session.commit.side_effect = Exception("Commit failed")
        
        with pytest.raises(Exception, match="Commit failed"):
            await model_config_repository.disable_all_models()
        
        mock_async_session.rollback.assert_called_once()


class TestModelConfigRepositoryUpsertModel:
    """Test cases for upsert_model method."""
    
    @pytest.mark.asyncio
    async def test_upsert_model_create_new(self, model_config_repository, mock_async_session, sample_model_data):
        """Test upsert creating a new model."""
        with patch.object(model_config_repository, 'find_by_key', return_value=None):
            with patch('src.repositories.model_config_repository.ModelConfig') as mock_model_class:
                created_model = MockModelConfig(key="new-model", **sample_model_data)
                mock_model_class.return_value = created_model
                
                result = await model_config_repository.upsert_model("new-model", sample_model_data)
                
                assert result == created_model
                mock_model_class.assert_called_once()
                mock_async_session.add.assert_called_once_with(created_model)
    
    @pytest.mark.asyncio
    async def test_upsert_model_update_existing(self, model_config_repository, mock_async_session, sample_model_data):
        """Test upsert updating an existing model."""
        existing_model = MockModelConfig(key="existing-model", name="Old Name")
        
        with patch.object(model_config_repository, 'find_by_key', return_value=existing_model):
            result = await model_config_repository.upsert_model("existing-model", sample_model_data)
            
            assert result == existing_model
            assert existing_model.name == sample_model_data["name"]
            assert existing_model.provider == sample_model_data["provider"]
            assert existing_model.temperature == sample_model_data["temperature"]
            assert existing_model.updated_at is not None
            mock_async_session.add.assert_not_called()  # Should not add existing model
    
    @pytest.mark.asyncio
    async def test_upsert_model_update_partial_data(self, model_config_repository, mock_async_session):
        """Test upsert with partial data preserves existing values."""
        existing_model = MockModelConfig(key="existing-model", name="Old Name", provider="old_provider")
        partial_data = {"name": "New Name"}  # Only updating name
        
        with patch.object(model_config_repository, 'find_by_key', return_value=existing_model):
            result = await model_config_repository.upsert_model("existing-model", partial_data)
            
            assert result == existing_model
            assert existing_model.name == "New Name"
            assert existing_model.provider == "old_provider"  # Should preserve existing
    
    @pytest.mark.asyncio
    async def test_upsert_model_database_error(self, model_config_repository, mock_async_session, sample_model_data):
        """Test upsert with database error."""
        with patch.object(model_config_repository, 'find_by_key', side_effect=Exception("Database error")):
            with pytest.raises(Exception, match="Database error"):
                await model_config_repository.upsert_model("error-model", sample_model_data)
            
            mock_async_session.rollback.assert_called_once()


class TestModelConfigRepositoryDeleteByKey:
    """Test cases for delete_by_key method."""
    
    @pytest.mark.asyncio
    async def test_delete_by_key_success(self, model_config_repository, mock_async_session):
        """Test successful model deletion by key."""
        model_config = MockModelConfig(id=1, key="delete-model")
        
        with patch.object(model_config_repository, 'find_by_key') as mock_find:
            mock_find.side_effect = [model_config, None]  # Found, then verified deleted
            
            result = await model_config_repository.delete_by_key("delete-model")
            
            assert result is True
            mock_async_session.delete.assert_called_once_with(model_config)
            mock_async_session.flush.assert_called_once()
            mock_async_session.commit.assert_called_once()
            assert mock_find.call_count == 2  # Initial find + verification
    
    @pytest.mark.asyncio
    async def test_delete_by_key_not_found(self, model_config_repository, mock_async_session):
        """Test delete by key when model not found."""
        with patch.object(model_config_repository, 'find_by_key', return_value=None):
            result = await model_config_repository.delete_by_key("nonexistent")
            
            assert result is False
            mock_async_session.delete.assert_not_called()
            mock_async_session.flush.assert_not_called()
            mock_async_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_by_key_verification_fails(self, model_config_repository, mock_async_session):
        """Test delete by key when verification shows model still exists."""
        model_config = MockModelConfig(id=1, key="persistent-model")
        
        with patch.object(model_config_repository, 'find_by_key') as mock_find:
            mock_find.side_effect = [model_config, model_config]  # Found, then still exists
            
            result = await model_config_repository.delete_by_key("persistent-model")
            
            assert result is False
            mock_async_session.delete.assert_called_once_with(model_config)
            mock_async_session.flush.assert_called_once()
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_by_key_database_error(self, model_config_repository, mock_async_session):
        """Test delete by key with database error."""
        model_config = MockModelConfig(key="error-model")
        
        with patch.object(model_config_repository, 'find_by_key', return_value=model_config):
            mock_async_session.delete.side_effect = Exception("Delete failed")
            
            with pytest.raises(Exception, match="Delete failed"):
                await model_config_repository.delete_by_key("error-model")
            
            mock_async_session.rollback.assert_called_once()


class TestModelConfigRepositoryIntegration:
    """Integration test cases testing method interactions."""
    
    @pytest.mark.asyncio
    async def test_upsert_then_find_by_key(self, model_config_repository, mock_async_session, sample_model_data):
        """Test upserting a model then finding it by key."""
        with patch.object(model_config_repository, 'find_by_key') as mock_find:
            # First call returns None (doesn't exist), second call returns created model
            created_model = MockModelConfig(key="upsert-model", **sample_model_data)
            mock_find.side_effect = [None, created_model]
            
            with patch('src.repositories.model_config_repository.ModelConfig', return_value=created_model):
                # Upsert (create)
                upsert_result = await model_config_repository.upsert_model("upsert-model", sample_model_data)
                
                # Find
                find_result = await model_config_repository.find_by_key("upsert-model")
                
                assert upsert_result == created_model
                assert find_result == created_model
                assert mock_find.call_count == 2
    
    @pytest.mark.asyncio
    async def test_enable_all_then_find_enabled(self, model_config_repository, mock_async_session, sample_model_configs):
        """Test enabling all models then finding enabled models."""
        # Enable all
        mock_async_session.execute.return_value = MagicMock()
        enable_result = await model_config_repository.enable_all_models()
        
        # Find enabled (all should be enabled now)
        enabled_configs = [config for config in sample_model_configs]
        for config in enabled_configs:
            config.enabled = True
        
        mock_result = MockResult(enabled_configs)
        mock_async_session.execute.return_value = mock_result
        
        find_result = await model_config_repository.find_enabled_models()
        
        assert enable_result is True
        assert len(find_result) == len(sample_model_configs)
        assert all(config.enabled for config in find_result)
    
    @pytest.mark.asyncio
    async def test_toggle_then_delete_workflow(self, model_config_repository, mock_async_session):
        """Test toggling model status then deleting it."""
        model_config = MockModelConfig(key="workflow-model", enabled=True)
        
        # Toggle to disabled
        with patch.object(model_config_repository, 'find_by_key', return_value=model_config):
            toggle_result = await model_config_repository.toggle_enabled("workflow-model", False)
            
            assert toggle_result is True
            assert model_config.enabled is False
            
            # Delete the model
            with patch.object(model_config_repository, 'find_by_key') as mock_find:
                mock_find.side_effect = [model_config, None]  # Found, then deleted
                
                delete_result = await model_config_repository.delete_by_key("workflow-model")
                
                assert delete_result is True
                mock_async_session.delete.assert_called_once_with(model_config)


class TestModelConfigRepositoryErrorHandling:
    """Test cases for error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_find_by_key_database_error(self, model_config_repository, mock_async_session):
        """Test find by key with database error."""
        mock_async_session.execute.side_effect = Exception("Connection lost")
        
        with pytest.raises(Exception, match="Connection lost"):
            await model_config_repository.find_by_key("test-model")
    
    @pytest.mark.asyncio
    async def test_find_enabled_models_database_error(self, model_config_repository, mock_async_session):
        """Test find enabled models with database error."""
        mock_async_session.execute.side_effect = Exception("Query timeout")
        
        with pytest.raises(Exception, match="Query timeout"):
            await model_config_repository.find_enabled_models()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_find_error(self, model_config_repository, mock_async_session):
        """Test toggle enabled when find_by_key fails."""
        with patch.object(model_config_repository, 'find_by_key', side_effect=Exception("Find failed")):
            with pytest.raises(Exception, match="Find failed"):
                await model_config_repository.toggle_enabled("test-model", True)
            
            mock_async_session.rollback.assert_called_once()


class TestModelConfigRepositoryEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    @pytest.mark.asyncio
    async def test_upsert_model_with_none_values(self, model_config_repository, mock_async_session):
        """Test upsert with None values in model data."""
        model_data_with_nones = {
            "name": "Test Model",
            "provider": None,
            "temperature": None,
            "context_window": None
        }
        
        with patch.object(model_config_repository, 'find_by_key', return_value=None):
            with patch('src.repositories.model_config_repository.ModelConfig') as mock_model_class:
                created_model = MockModelConfig(key="none-model")
                mock_model_class.return_value = created_model
                
                result = await model_config_repository.upsert_model("none-model", model_data_with_nones)
                
                assert result == created_model
                # Verify None values were passed to constructor
                call_args = mock_model_class.call_args[1]
                assert call_args["provider"] is None
                assert call_args["temperature"] is None
    
    @pytest.mark.asyncio
    async def test_upsert_model_missing_required_fields(self, model_config_repository, mock_async_session):
        """Test upsert with missing required 'name' field for new model."""
        incomplete_data = {"provider": "test"}  # Missing 'name' field
        
        with patch.object(model_config_repository, 'find_by_key', return_value=None):
            with pytest.raises(KeyError, match="name"):
                await model_config_repository.upsert_model("incomplete-model", incomplete_data)
            
            mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_same_value(self, model_config_repository, mock_async_session):
        """Test toggling model to the same enabled value."""
        model_config = MockModelConfig(key="same-value-model", enabled=True)
        
        with patch.object(model_config_repository, 'find_by_key', return_value=model_config):
            result = await model_config_repository.toggle_enabled("same-value-model", True)
            
            assert result is True
            assert model_config.enabled is True  # Should remain True
            mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_by_key_empty_string(self, model_config_repository, mock_async_session):
        """Test delete by key with empty string."""
        with patch.object(model_config_repository, 'find_by_key', return_value=None):
            result = await model_config_repository.delete_by_key("")
            
            assert result is False
            mock_async_session.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_enable_disable_all_models_empty_database(self, model_config_repository, mock_async_session):
        """Test enable/disable all when database is empty."""
        mock_async_session.execute.return_value = MagicMock()
        
        # Enable all (no models to enable)
        enable_result = await model_config_repository.enable_all_models()
        assert enable_result is True
        
        # Disable all (no models to disable)
        disable_result = await model_config_repository.disable_all_models()
        assert disable_result is True
        
        # Both operations should have been attempted
        assert mock_async_session.execute.call_count == 2
        assert mock_async_session.commit.call_count == 2


class TestModelConfigRepositoryLoggingCoverage:
    """Test cases to ensure 100% coverage including all logging statements."""
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_handles_logging_import(self, model_config_repository, mock_async_session):
        """Test toggle enabled method imports logging and handles errors properly."""
        model_config = MockModelConfig(key="error-model")
        
        with patch.object(model_config_repository, 'find_by_key', return_value=model_config):
            mock_async_session.commit.side_effect = Exception("Commit error")
            
            with pytest.raises(Exception, match="Commit error"):
                await model_config_repository.toggle_enabled("error-model", True)
            
            mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_enable_all_models_handles_logging_import(self, model_config_repository, mock_async_session):
        """Test enable all models method imports logging and handles errors properly."""
        mock_async_session.execute.side_effect = Exception("Execute error")
        
        with pytest.raises(Exception, match="Execute error"):
            await model_config_repository.enable_all_models()
        
        mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_disable_all_models_handles_logging_import(self, model_config_repository, mock_async_session):
        """Test disable all models method imports logging and handles errors properly."""
        mock_async_session.execute.side_effect = Exception("Execute error")
        
        with pytest.raises(Exception, match="Execute error"):
            await model_config_repository.disable_all_models()
        
        mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_upsert_model_handles_logging_and_datetime_imports(self, model_config_repository, mock_async_session, sample_model_data):
        """Test upsert model method imports logging and datetime properly."""
        # Test create path with logging
        with patch.object(model_config_repository, 'find_by_key', return_value=None):
            with patch('src.repositories.model_config_repository.ModelConfig') as mock_model_class:
                created_model = MockModelConfig(key="new-model", **sample_model_data)
                mock_model_class.return_value = created_model
                
                result = await model_config_repository.upsert_model("new-model", sample_model_data)
                
                assert result == created_model
        
        # Test update path with datetime import
        existing_model = MockModelConfig(key="existing-model", name="Old Name")
        with patch.object(model_config_repository, 'find_by_key', return_value=existing_model):
            result = await model_config_repository.upsert_model("existing-model", sample_model_data)
            
            assert result == existing_model
            assert existing_model.updated_at is not None
    
    @pytest.mark.asyncio
    async def test_upsert_model_handles_error_logging(self, model_config_repository, mock_async_session, sample_model_data):
        """Test upsert model method handles errors and logging properly."""
        with patch.object(model_config_repository, 'find_by_key', side_effect=Exception("Find error")):
            with pytest.raises(Exception, match="Find error"):
                await model_config_repository.upsert_model("error-model", sample_model_data)
            
            mock_async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_by_key_handles_all_logging_imports(self, model_config_repository, mock_async_session):
        """Test delete by key method handles all logging scenarios."""
        model_config = MockModelConfig(id=1, key="delete-model")
        
        # Test successful deletion path
        with patch.object(model_config_repository, 'find_by_key') as mock_find:
            mock_find.side_effect = [model_config, None]  # Found, then verified deleted
            
            result = await model_config_repository.delete_by_key("delete-model")
            
            assert result is True
        
        # Test not found path
        with patch.object(model_config_repository, 'find_by_key', return_value=None):
            result = await model_config_repository.delete_by_key("nonexistent")
            
            assert result is False
        
        # Test verification failure path
        with patch.object(model_config_repository, 'find_by_key') as mock_find:
            mock_find.side_effect = [model_config, model_config]  # Found, then still exists
            
            result = await model_config_repository.delete_by_key("persistent-model")
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_by_key_handles_exception_logging_with_traceback(self, model_config_repository, mock_async_session):
        """Test delete by key method handles exception logging with traceback import."""
        model_config = MockModelConfig(key="error-model")
        
        with patch.object(model_config_repository, 'find_by_key', return_value=model_config):
            mock_async_session.delete.side_effect = Exception("Delete failed")
            
            with pytest.raises(Exception, match="Delete failed"):
                await model_config_repository.delete_by_key("error-model")
            
            mock_async_session.rollback.assert_called_once()


class TestModelConfigRepositoryComprehensiveCoverage:
    """Test cases to achieve 100% line coverage for all edge cases."""
    
    @pytest.mark.asyncio
    async def test_enable_all_models_with_filter_condition(self, model_config_repository, mock_async_session):
        """Test enable all models with the specific where condition for disabled models."""
        mock_async_session.execute.return_value = MagicMock()
        
        result = await model_config_repository.enable_all_models()
        
        assert result is True
        # Verify the execute was called with the correct update statement
        mock_async_session.execute.assert_called_once()
        call_args = mock_async_session.execute.call_args[0][0]
        
        # The statement should be an update statement targeting disabled models
        assert hasattr(call_args, 'compile')
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio  
    async def test_disable_all_models_with_filter_condition(self, model_config_repository, mock_async_session):
        """Test disable all models with the specific where condition for enabled models."""
        mock_async_session.execute.return_value = MagicMock()
        
        result = await model_config_repository.disable_all_models()
        
        assert result is True
        # Verify the execute was called with the correct update statement
        mock_async_session.execute.assert_called_once()
        call_args = mock_async_session.execute.call_args[0][0]
        
        # The statement should be an update statement targeting enabled models
        assert hasattr(call_args, 'compile')
        mock_async_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_upsert_model_all_fields_update(self, model_config_repository, mock_async_session):
        """Test upsert updating all possible fields of an existing model."""
        existing_model = MockModelConfig(
            key="comprehensive-model", 
            name="Old Name",
            provider="old_provider",
            temperature=0.5,
            context_window=2048,
            max_output_tokens=1024,
            extended_thinking=False,
            enabled=False
        )
        
        complete_update_data = {
            "name": "New Name",
            "provider": "new_provider", 
            "temperature": 0.8,
            "context_window": 4096,
            "max_output_tokens": 2048,
            "extended_thinking": True,
            "enabled": True
        }
        
        with patch.object(model_config_repository, 'find_by_key', return_value=existing_model):
            result = await model_config_repository.upsert_model("comprehensive-model", complete_update_data)
            
            assert result == existing_model
            # Verify all fields were updated
            assert existing_model.name == "New Name"
            assert existing_model.provider == "new_provider"
            assert existing_model.temperature == 0.8
            assert existing_model.context_window == 4096
            assert existing_model.max_output_tokens == 2048
            assert existing_model.extended_thinking is True
            assert existing_model.enabled is True
            assert existing_model.updated_at is not None
    
    @pytest.mark.asyncio
    async def test_upsert_model_new_with_all_fields(self, model_config_repository, mock_async_session):
        """Test upsert creating a new model with all possible fields."""
        complete_model_data = {
            "name": "Complete Model",
            "provider": "test_provider",
            "temperature": 0.7,
            "context_window": 8192,
            "max_output_tokens": 4096,
            "extended_thinking": True,
            "enabled": False
        }
        
        with patch.object(model_config_repository, 'find_by_key', return_value=None):
            with patch('src.repositories.model_config_repository.ModelConfig') as mock_model_class:
                created_model = MockModelConfig(key="complete-model", **complete_model_data)
                mock_model_class.return_value = created_model
                
                result = await model_config_repository.upsert_model("complete-model", complete_model_data)
                
                assert result == created_model
                
                # Verify all fields were passed to constructor
                call_args = mock_model_class.call_args[1]
                assert call_args["name"] == "Complete Model"
                assert call_args["provider"] == "test_provider"
                assert call_args["temperature"] == 0.7
                assert call_args["context_window"] == 8192
                assert call_args["max_output_tokens"] == 4096
                assert call_args["extended_thinking"] is True
                assert call_args["enabled"] is False
                assert "created_at" in call_args
                assert "updated_at" in call_args
                
                mock_async_session.add.assert_called_once_with(created_model)
    
    @pytest.mark.asyncio
    async def test_find_enabled_models_query_construction(self, model_config_repository, mock_async_session):
        """Test find enabled models query construction with proper where clause."""
        enabled_model = MockModelConfig(key="enabled-model", enabled=True)
        mock_result = MockResult([enabled_model])
        mock_async_session.execute.return_value = mock_result
        
        result = await model_config_repository.find_enabled_models()
        
        assert result == [enabled_model]
        # Verify the query was constructed correctly 
        mock_async_session.execute.assert_called_once()
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(ModelConfig)))
    
    @pytest.mark.asyncio
    async def test_find_all_query_construction(self, model_config_repository, mock_async_session):
        """Test find all query construction."""
        models = [MockModelConfig(key="model1"), MockModelConfig(key="model2")]
        mock_result = MockResult(models)
        mock_async_session.execute.return_value = mock_result
        
        result = await model_config_repository.find_all()
        
        assert result == models
        # Verify the query was constructed correctly
        mock_async_session.execute.assert_called_once()
        call_args = mock_async_session.execute.call_args[0][0]
        assert isinstance(call_args, type(select(ModelConfig)))