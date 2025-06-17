"""
Unit tests for ModelConfigRepository.

Tests the functionality of model config repository including
CRUD operations, enabling/disabling models, upsert functionality, and bulk operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.model_config_repository import ModelConfigRepository
from src.models.model_config import ModelConfig


# Mock model config model
class MockModelConfig:
    def __init__(self, id=1, key="test-model", name="Test Model", provider="openai",
                 temperature=0.7, context_window=4096, max_output_tokens=1024,
                 extended_thinking=False, enabled=True, created_at=None, updated_at=None, **kwargs):
        self.id = id
        self.key = key
        self.name = name
        self.provider = provider
        self.temperature = temperature
        self.context_window = context_window
        self.max_output_tokens = max_output_tokens
        self.extended_thinking = extended_thinking
        self.enabled = enabled
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
        for key_attr, value in kwargs.items():
            setattr(self, key_attr, value)


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
    session.delete = AsyncMock()
    session.flush = AsyncMock()
    return session


@pytest.fixture
def model_config_repository(mock_async_session):
    """Create a model config repository."""
    return ModelConfigRepository(mock_async_session)


@pytest.fixture
def sample_model_configs():
    """Create sample model configs for testing."""
    return [
        MockModelConfig(id=1, key="gpt-4", name="GPT-4", provider="openai", enabled=True),
        MockModelConfig(id=2, key="claude-3", name="Claude-3", provider="anthropic", enabled=True),
        MockModelConfig(id=3, key="llama-2", name="Llama-2", provider="meta", enabled=False),
        MockModelConfig(id=4, key="gemini", name="Gemini", provider="google", enabled=True)
    ]


@pytest.fixture
def sample_model_data():
    """Create sample model data for creation/update testing."""
    return {
        "name": "Test Model",
        "provider": "openai",
        "temperature": 0.8,
        "context_window": 8192,
        "max_output_tokens": 2048,
        "extended_thinking": True,
        "enabled": True
    }


class TestModelConfigRepositoryInit:
    """Test repository initialization."""
    
    def test_init(self, mock_async_session):
        """Test repository initialization."""
        repo = ModelConfigRepository(mock_async_session)
        assert repo.session == mock_async_session
        assert repo.model == ModelConfig


class TestModelConfigRepositoryFindAll:
    """Test find all functionality."""
    
    @pytest.mark.asyncio
    async def test_find_all_success(self, model_config_repository, sample_model_configs):
        """Test find all model configs successfully."""
        mock_result = MockResult(sample_model_configs)
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.find_all()
        
        assert len(result) == 4
        assert result == sample_model_configs
        
        # Verify the query was constructed correctly
        call_args = model_config_repository.session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # It's a SQLAlchemy query
    
    @pytest.mark.asyncio
    async def test_find_all_empty(self, model_config_repository):
        """Test find all when no models exist."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.find_all()
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_find_all_database_error(self, model_config_repository):
        """Test find all handles database errors."""
        model_config_repository.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.find_all()


class TestModelConfigRepositoryFindByKey:
    """Test find by key functionality."""
    
    @pytest.mark.asyncio
    async def test_find_by_key_found(self, model_config_repository, sample_model_configs):
        """Test find model config by key when found."""
        target_model = sample_model_configs[0]  # gpt-4
        mock_result = MockResult([target_model])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.find_by_key("gpt-4")
        
        assert result == target_model
        assert result.key == "gpt-4"
    
    @pytest.mark.asyncio
    async def test_find_by_key_not_found(self, model_config_repository):
        """Test find model config by key when not found."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.find_by_key("nonexistent-model")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_find_by_key_database_error(self, model_config_repository):
        """Test find by key handles database errors."""
        model_config_repository.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.find_by_key("test-key")


class TestModelConfigRepositoryFindEnabledModels:
    """Test find enabled models functionality."""
    
    @pytest.mark.asyncio
    async def test_find_enabled_models_success(self, model_config_repository, sample_model_configs):
        """Test find enabled models successfully."""
        enabled_models = [model for model in sample_model_configs if model.enabled]
        mock_result = MockResult(enabled_models)
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.find_enabled_models()
        
        assert len(result) == 3  # gpt-4, claude-3, gemini are enabled
        assert all(model.enabled for model in result)
        assert not any(model.key == "llama-2" for model in result)  # llama-2 is disabled
    
    @pytest.mark.asyncio
    async def test_find_enabled_models_none_enabled(self, model_config_repository):
        """Test find enabled models when none are enabled."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.find_enabled_models()
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_find_enabled_models_database_error(self, model_config_repository):
        """Test find enabled models handles database errors."""
        model_config_repository.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.find_enabled_models()


class TestModelConfigRepositoryToggleEnabled:
    """Test toggle enabled functionality."""
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_success(self, model_config_repository, sample_model_configs):
        """Test toggle enabled successfully."""
        target_model = sample_model_configs[0]  # gpt-4, currently enabled
        mock_result = MockResult([target_model])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.toggle_enabled("gpt-4", False)
        
        assert result is True
        assert target_model.enabled is False
        model_config_repository.session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_model_not_found(self, model_config_repository):
        """Test toggle enabled when model not found."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.toggle_enabled("nonexistent-model", True)
        
        assert result is False
        model_config_repository.session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_toggle_enabled_database_error(self, model_config_repository, sample_model_configs):
        """Test toggle enabled handles database errors."""
        target_model = sample_model_configs[0]
        mock_result = MockResult([target_model])
        model_config_repository.session.execute.return_value = mock_result
        model_config_repository.session.commit.side_effect = SQLAlchemyError("Commit failed")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.toggle_enabled("gpt-4", False)
        
        model_config_repository.session.rollback.assert_called_once()


class TestModelConfigRepositoryBulkOperations:
    """Test bulk enable/disable operations."""
    
    @pytest.mark.asyncio
    async def test_enable_all_models_success(self, model_config_repository):
        """Test enable all models successfully."""
        result = await model_config_repository.enable_all_models()
        
        assert result is True
        model_config_repository.session.execute.assert_called_once()
        model_config_repository.session.commit.assert_called_once()
        
        # Verify the update statement
        call_args = model_config_repository.session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # It's a SQLAlchemy update statement
    
    @pytest.mark.asyncio
    async def test_enable_all_models_database_error(self, model_config_repository):
        """Test enable all models handles database errors."""
        model_config_repository.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.enable_all_models()
        
        model_config_repository.session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_disable_all_models_success(self, model_config_repository):
        """Test disable all models successfully."""
        result = await model_config_repository.disable_all_models()
        
        assert result is True
        model_config_repository.session.execute.assert_called_once()
        model_config_repository.session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_disable_all_models_database_error(self, model_config_repository):
        """Test disable all models handles database errors."""
        model_config_repository.session.commit.side_effect = SQLAlchemyError("Commit failed")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.disable_all_models()
        
        model_config_repository.session.rollback.assert_called_once()


class TestModelConfigRepositoryUpsert:
    """Test upsert functionality."""
    
    @pytest.mark.asyncio
    async def test_upsert_model_create_new(self, model_config_repository, sample_model_data):
        """Test upsert creates new model when none exists."""
        # Mock no existing model
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        with patch('src.repositories.model_config_repository.ModelConfig') as mock_model:
            created_model = MockModelConfig(key="new-model", **sample_model_data)
            mock_model.return_value = created_model
            
            result = await model_config_repository.upsert_model("new-model", sample_model_data)
            
            assert result == created_model
            model_config_repository.session.add.assert_called_once_with(created_model)
            
            # Verify ModelConfig was created with correct parameters
            call_args = mock_model.call_args[1]
            assert call_args['key'] == "new-model"
            assert call_args['name'] == sample_model_data['name']
            assert call_args['provider'] == sample_model_data['provider']
            assert call_args['temperature'] == sample_model_data['temperature']
            assert call_args['context_window'] == sample_model_data['context_window']
            assert call_args['max_output_tokens'] == sample_model_data['max_output_tokens']
            assert call_args['extended_thinking'] == sample_model_data['extended_thinking']
            assert call_args['enabled'] == sample_model_data['enabled']
    
    @pytest.mark.asyncio
    async def test_upsert_model_update_existing(self, model_config_repository, sample_model_configs, sample_model_data):
        """Test upsert updates existing model."""
        existing_model = sample_model_configs[0]  # gpt-4
        mock_result = MockResult([existing_model])
        model_config_repository.session.execute.return_value = mock_result
        
        original_name = existing_model.name
        sample_model_data['name'] = "Updated GPT-4"
        
        result = await model_config_repository.upsert_model("gpt-4", sample_model_data)
        
        assert result == existing_model
        assert existing_model.name == "Updated GPT-4"
        assert existing_model.provider == sample_model_data['provider']
        assert existing_model.temperature == sample_model_data['temperature']
        # Check that updated_at was set to a datetime
        assert existing_model.updated_at is not None
        assert isinstance(existing_model.updated_at, datetime)
        
        # Verify no new model was added
        model_config_repository.session.add.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_upsert_model_partial_update(self, model_config_repository, sample_model_configs):
        """Test upsert with partial data updates only provided fields."""
        existing_model = sample_model_configs[0]  # gpt-4
        mock_result = MockResult([existing_model])
        model_config_repository.session.execute.return_value = mock_result
        
        original_provider = existing_model.provider
        original_temperature = existing_model.temperature
        
        partial_data = {"name": "Updated Name Only"}
        
        result = await model_config_repository.upsert_model("gpt-4", partial_data)
        
        assert result == existing_model
        assert existing_model.name == "Updated Name Only"
        assert existing_model.provider == original_provider  # Unchanged
        assert existing_model.temperature == original_temperature  # Unchanged
    
    @pytest.mark.asyncio
    async def test_upsert_model_with_defaults(self, model_config_repository):
        """Test upsert creates model with default values."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        minimal_data = {"name": "Minimal Model"}
        
        with patch('src.repositories.model_config_repository.ModelConfig') as mock_model:
            created_model = MockModelConfig(key="minimal-model")
            mock_model.return_value = created_model
            
            result = await model_config_repository.upsert_model("minimal-model", minimal_data)
            
            # Verify defaults were used
            call_args = mock_model.call_args[1]
            assert call_args['extended_thinking'] is False  # Default
            assert call_args['enabled'] is True  # Default
    
    @pytest.mark.asyncio
    async def test_upsert_model_database_error(self, model_config_repository, sample_model_data):
        """Test upsert handles database errors."""
        model_config_repository.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.upsert_model("test-key", sample_model_data)
        
        model_config_repository.session.rollback.assert_called_once()


class TestModelConfigRepositoryDeleteByKey:
    """Test delete by key functionality."""
    
    @pytest.mark.asyncio
    async def test_delete_by_key_success(self, model_config_repository, sample_model_configs):
        """Test delete by key successfully."""
        target_model = sample_model_configs[0]  # gpt-4
        
        # Mock finding the model, then verifying deletion
        model_config_repository.session.execute.side_effect = [
            MockResult([target_model]),  # find_by_key call
            MockResult([])  # verification call (model not found = deleted)
        ]
        
        result = await model_config_repository.delete_by_key("gpt-4")
        
        assert result is True
        model_config_repository.session.delete.assert_called_once_with(target_model)
        model_config_repository.session.flush.assert_called_once()
        model_config_repository.session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_by_key_not_found(self, model_config_repository):
        """Test delete by key when model not found."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.delete_by_key("nonexistent-model")
        
        assert result is False
        model_config_repository.session.delete.assert_not_called()
        model_config_repository.session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_by_key_verification_fails(self, model_config_repository, sample_model_configs):
        """Test delete by key when verification shows model still exists."""
        target_model = sample_model_configs[0]
        
        # Mock finding the model, then verification still finds it (deletion failed)
        model_config_repository.session.execute.side_effect = [
            MockResult([target_model]),  # find_by_key call
            MockResult([target_model])  # verification call (model still exists)
        ]
        
        result = await model_config_repository.delete_by_key("gpt-4")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_by_key_database_error(self, model_config_repository, sample_model_configs):
        """Test delete by key handles database errors."""
        target_model = sample_model_configs[0]
        mock_result = MockResult([target_model])
        model_config_repository.session.execute.return_value = mock_result
        
        model_config_repository.session.delete.side_effect = SQLAlchemyError("Delete failed")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.delete_by_key("gpt-4")
        
        model_config_repository.session.rollback.assert_called_once()


class TestModelConfigRepositoryBaseRepositoryMethods:
    """Test inherited BaseRepository methods."""
    
    @pytest.mark.asyncio
    async def test_get_success(self, model_config_repository, sample_model_configs):
        """Test get method finds model by ID."""
        target_model = sample_model_configs[0]
        mock_result = MockResult([target_model])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.get(1)
        
        assert result == target_model
        # Verify the query was constructed correctly
        call_args = model_config_repository.session.execute.call_args[0][0]
        assert hasattr(call_args, 'compile')  # It's a SQLAlchemy query
    
    @pytest.mark.asyncio
    async def test_get_not_found(self, model_config_repository):
        """Test get method when model not found."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.get(999)
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_database_error(self, model_config_repository):
        """Test get method handles database errors."""
        model_config_repository.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.get(1)
        
        model_config_repository.session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_success(self, model_config_repository, sample_model_configs):
        """Test list method with pagination."""
        mock_result = MockResult(sample_model_configs[:2])  # First 2 models
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.list(skip=0, limit=2)
        
        assert len(result) == 2
        assert result == sample_model_configs[:2]
    
    @pytest.mark.asyncio
    async def test_list_with_defaults(self, model_config_repository, sample_model_configs):
        """Test list method with default parameters."""
        mock_result = MockResult(sample_model_configs)
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.list()
        
        assert len(result) == 4
        assert result == sample_model_configs
    
    @pytest.mark.asyncio
    async def test_list_empty(self, model_config_repository):
        """Test list method when no models exist."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.list()
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_list_database_error(self, model_config_repository):
        """Test list method handles database errors."""
        model_config_repository.session.execute.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.list()
        
        model_config_repository.session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_success(self, model_config_repository, sample_model_data):
        """Test create method successfully creates model."""
        # Mock the ModelConfig class at the module level where it's used
        with patch.object(model_config_repository, 'model') as mock_model:
            created_model = MockModelConfig(id=1, key="created-model", **sample_model_data)
            mock_model.return_value = created_model
            mock_model.__name__ = "ModelConfig"  # Add __name__ attribute for logging
            
            # Add the key to sample_model_data for creation
            sample_model_data['key'] = 'created-model'
            
            # Make session.add non-async since it doesn't need to be
            model_config_repository.session.add = MagicMock()
            
            result = await model_config_repository.create(sample_model_data)
            
            assert result == created_model
            model_config_repository.session.add.assert_called_once_with(created_model)
            model_config_repository.session.flush.assert_called_once()
            model_config_repository.session.commit.assert_called_once()
            model_config_repository.session.refresh.assert_called_once_with(created_model)
    
    @pytest.mark.asyncio
    async def test_create_database_error(self, model_config_repository, sample_model_data):
        """Test create method handles database errors."""
        # Make session.add non-async since it doesn't need to be
        model_config_repository.session.add = MagicMock(side_effect=SQLAlchemyError("Database error"))
        
        sample_model_data['key'] = 'error-model'
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.create(sample_model_data)
        
        model_config_repository.session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_add_success(self, model_config_repository, sample_model_configs):
        """Test add method successfully adds model object."""
        model_to_add = sample_model_configs[0]
        
        # Make session.add non-async since it doesn't need to be
        model_config_repository.session.add = MagicMock()
        
        result = await model_config_repository.add(model_to_add)
        
        assert result == model_to_add
        model_config_repository.session.add.assert_called_once_with(model_to_add)
        model_config_repository.session.flush.assert_called_once()
        model_config_repository.session.commit.assert_called_once()
        model_config_repository.session.refresh.assert_called_once_with(model_to_add)
    
    @pytest.mark.asyncio
    async def test_add_database_error(self, model_config_repository, sample_model_configs):
        """Test add method handles database errors."""
        model_to_add = sample_model_configs[0]
        
        # Make session.add non-async since it doesn't need to be
        model_config_repository.session.add = MagicMock()
        model_config_repository.session.flush.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.add(model_to_add)
        
        model_config_repository.session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_success(self, model_config_repository, sample_model_configs):
        """Test update method successfully updates model."""
        target_model = sample_model_configs[0]
        update_data = {"name": "Updated Model Name"}
        
        # Mock get calls - first for checking existence, second for getting updated data
        model_config_repository.session.execute.side_effect = [
            MockResult([target_model]),  # get() call to check if exists
            MockResult([]),  # execute() call for update statement  
            MockResult([target_model])   # get() call to return updated object
        ]
        
        result = await model_config_repository.update(1, update_data)
        
        assert result == target_model
        model_config_repository.session.flush.assert_called_once()
        model_config_repository.session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_not_found(self, model_config_repository):
        """Test update method when model not found."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.update(999, {"name": "Updated"})
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_database_error(self, model_config_repository, sample_model_configs):
        """Test update method handles database errors."""
        target_model = sample_model_configs[0]
        mock_result = MockResult([target_model])
        model_config_repository.session.execute.return_value = mock_result
        model_config_repository.session.flush.side_effect = SQLAlchemyError("Database error")
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.update(1, {"name": "Updated"})
        
        model_config_repository.session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_success(self, model_config_repository, sample_model_configs):
        """Test delete method successfully deletes model."""
        target_model = sample_model_configs[0]
        mock_result = MockResult([target_model])
        model_config_repository.session.execute.return_value = mock_result
        
        # Make session.delete non-async since it doesn't need to be
        model_config_repository.session.delete = MagicMock()
        
        result = await model_config_repository.delete(1)
        
        assert result is True
        model_config_repository.session.delete.assert_called_once_with(target_model)
        model_config_repository.session.flush.assert_called_once()
        model_config_repository.session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_not_found(self, model_config_repository):
        """Test delete method when model not found."""
        mock_result = MockResult([])
        model_config_repository.session.execute.return_value = mock_result
        
        result = await model_config_repository.delete(999)
        
        assert result is False
        model_config_repository.session.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_delete_database_error(self, model_config_repository, sample_model_configs):
        """Test delete method handles database errors."""
        target_model = sample_model_configs[0]
        mock_result = MockResult([target_model])
        model_config_repository.session.execute.return_value = mock_result
        
        # Make session.delete non-async since it doesn't need to be
        model_config_repository.session.delete = MagicMock(side_effect=SQLAlchemyError("Database error"))
        
        with pytest.raises(SQLAlchemyError):
            await model_config_repository.delete(1)
        
        model_config_repository.session.rollback.assert_called_once()


class TestModelConfigRepositoryIntegration:
    """Test integration scenarios and workflows."""
    
    @pytest.mark.asyncio
    async def test_full_model_lifecycle(self, model_config_repository, sample_model_data):
        """Test complete model lifecycle: create, find, update, toggle, delete."""
        # 1. Create new model via upsert
        model_config_repository.session.execute.return_value = MockResult([])  # No existing model
        
        with patch('src.repositories.model_config_repository.ModelConfig') as mock_model:
            created_model = MockModelConfig(key="lifecycle-model", **sample_model_data)
            mock_model.return_value = created_model
            
            # Create
            result = await model_config_repository.upsert_model("lifecycle-model", sample_model_data)
            assert result == created_model
            
            # 2. Find the created model
            model_config_repository.session.execute.return_value = MockResult([created_model])
            found_model = await model_config_repository.find_by_key("lifecycle-model")
            assert found_model == created_model
            
            # 3. Toggle enabled status
            toggle_result = await model_config_repository.toggle_enabled("lifecycle-model", False)
            assert toggle_result is True
            assert created_model.enabled is False
            
            # 4. Update via upsert
            update_data = {"name": "Updated Lifecycle Model"}
            updated_model = await model_config_repository.upsert_model("lifecycle-model", update_data)
            assert updated_model == created_model
            assert created_model.name == "Updated Lifecycle Model"
            
            # 5. Delete the model
            model_config_repository.session.execute.side_effect = [
                MockResult([created_model]),  # find_by_key
                MockResult([])  # verification (deleted)
            ]
            delete_result = await model_config_repository.delete_by_key("lifecycle-model")
            assert delete_result is True
    
    @pytest.mark.asyncio
    async def test_bulk_operations_workflow(self, model_config_repository, sample_model_configs):
        """Test bulk enable/disable operations workflow."""
        # Enable all models
        enable_result = await model_config_repository.enable_all_models()
        assert enable_result is True
        
        # Disable all models
        disable_result = await model_config_repository.disable_all_models()
        assert disable_result is True
        
        # Verify both operations called session execute and commit
        assert model_config_repository.session.execute.call_count == 2
        assert model_config_repository.session.commit.call_count == 2
    
    @pytest.mark.asyncio
    async def test_find_operations_consistency(self, model_config_repository, sample_model_configs):
        """Test consistency between different find operations."""
        # Mock all models
        model_config_repository.session.execute.return_value = MockResult(sample_model_configs)
        all_models = await model_config_repository.find_all()
        assert len(all_models) == 4
        
        # Mock enabled models only
        enabled_models = [model for model in sample_model_configs if model.enabled]
        model_config_repository.session.execute.return_value = MockResult(enabled_models)
        found_enabled = await model_config_repository.find_enabled_models()
        assert len(found_enabled) == 3
        assert all(model.enabled for model in found_enabled)
        
        # Mock specific model
        target_model = sample_model_configs[0]
        model_config_repository.session.execute.return_value = MockResult([target_model])
        found_by_key = await model_config_repository.find_by_key("gpt-4")
        assert found_by_key == target_model
    
    @pytest.mark.asyncio
    async def test_error_handling_consistency(self, model_config_repository):
        """Test that all methods handle errors consistently."""
        error = SQLAlchemyError("Consistent error")
        
        # All methods should handle SQLAlchemyError and call rollback where appropriate
        methods_with_rollback = [
            ("toggle_enabled", ("test-key", True)),
            ("enable_all_models", ()),
            ("disable_all_models", ()),
            ("upsert_model", ("test-key", {"name": "Test"})),
            ("delete_by_key", ("test-key",))
        ]
        
        for method_name, args in methods_with_rollback:
            # Reset mock
            model_config_repository.session.reset_mock()
            model_config_repository.session.execute.side_effect = error
            
            with pytest.raises(SQLAlchemyError):
                method = getattr(model_config_repository, method_name)
                await method(*args)
            
            model_config_repository.session.rollback.assert_called_once()
        
        # Methods without rollback (read-only operations)
        read_methods = [
            ("find_all", ()),
            ("find_by_key", ("test-key",)),
            ("find_enabled_models", ())
        ]
        
        for method_name, args in read_methods:
            model_config_repository.session.reset_mock()
            model_config_repository.session.execute.side_effect = error
            
            with pytest.raises(SQLAlchemyError):
                method = getattr(model_config_repository, method_name)
                await method(*args)
            
            # Read methods shouldn't call rollback
            model_config_repository.session.rollback.assert_not_called()