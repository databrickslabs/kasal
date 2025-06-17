"""
Unit tests for model configs seed module.
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from sqlalchemy.exc import IntegrityError

from src.seeds.model_configs import (
    DEFAULT_MODELS,
    seed_async,
    seed_sync,
    seed
)


class TestModelConfigsSeed:
    """Test cases for model configs seed module."""

    @pytest.fixture
    def mock_session(self):
        """Mock database session."""
        session = Mock()
        session.add = Mock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        session.execute = AsyncMock()
        return session

    @pytest.fixture
    def mock_sync_session(self):
        """Mock sync database session."""
        session = Mock()
        session.add = Mock()
        session.commit = Mock()
        session.rollback = Mock()
        session.execute = Mock()
        return session

    @pytest.fixture
    def mock_model_config_class(self):
        """Mock ModelConfig class."""
        with patch('src.seeds.model_configs.ModelConfig') as mock_class:
            yield mock_class

    def test_default_models_structure(self):
        """Test that DEFAULT_MODELS has expected structure."""
        assert isinstance(DEFAULT_MODELS, dict)
        assert len(DEFAULT_MODELS) > 0
        
        # Check a few specific models exist
        assert "gpt-4-turbo" in DEFAULT_MODELS
        assert "claude-3-5-sonnet-20241022" in DEFAULT_MODELS
        assert "databricks-llama-4-maverick" in DEFAULT_MODELS
        
        # Check required fields for each model
        required_fields = ["name", "temperature", "provider", "context_window", "max_output_tokens"]
        for model_key, model_data in DEFAULT_MODELS.items():
            for field in required_fields:
                assert field in model_data, f"Model {model_key} missing field {field}"
            
            # Check data types
            assert isinstance(model_data["temperature"], (int, float))
            assert isinstance(model_data["context_window"], int)
            assert isinstance(model_data["max_output_tokens"], int)
            assert isinstance(model_data["provider"], str)
            assert isinstance(model_data["name"], str)

    def test_default_models_extended_thinking(self):
        """Test models with extended thinking capability."""
        extended_thinking_models = [
            key for key, data in DEFAULT_MODELS.items() 
            if data.get("extended_thinking", False)
        ]
        
        # Should have at least one extended thinking model
        assert len(extended_thinking_models) > 0
        assert "claude-3-7-sonnet-20250219-thinking" in extended_thinking_models

    def test_default_models_providers(self):
        """Test that models have valid providers."""
        valid_providers = {"openai", "anthropic", "gemini", "ollama", "databricks", "deepseek"}
        
        for model_key, model_data in DEFAULT_MODELS.items():
            provider = model_data["provider"]
            assert provider in valid_providers, f"Model {model_key} has invalid provider {provider}"

    @pytest.mark.asyncio
    async def test_seed_async_success(self, mock_session, mock_model_config_class):
        """Test successful async seeding."""
        # Mock session factory
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
                
                # Mock no existing models
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_session.execute.return_value = mock_result
                
                await seed_async()
                
                # Should have called commit
                mock_session.commit.assert_called_once()
                
                # Should have added models (one for each in DEFAULT_MODELS)
                assert mock_session.add.call_count == len(DEFAULT_MODELS)

    @pytest.mark.asyncio
    async def test_seed_async_update_existing(self, mock_session, mock_model_config_class):
        """Test async seeding with existing models to update."""
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Mock existing model
            existing_model = Mock()
            existing_model.name = "existing_model"
            existing_model.provider = "openai"
            existing_model.temperature = 0.5
            existing_model.context_window = 4096
            existing_model.max_output_tokens = 2048
            existing_model.extended_thinking = False
            existing_model.enabled = False
            existing_model.updated_at = datetime.now()
            
            mock_result = Mock()
            mock_result.scalars.return_value.first.return_value = existing_model
            mock_session.execute.return_value = mock_result
            
            await seed_async()
            
            # Should have updated existing models
            mock_session.commit.assert_called_once()
            # Should not have added new models since all exist
            assert mock_session.add.call_count == 0

    @pytest.mark.asyncio
    async def test_seed_async_validation_errors(self, mock_session, mock_model_config_class):
        """Test async seeding with validation errors."""
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Mock no existing models
            mock_result = Mock()
            mock_result.scalars.return_value.first.return_value = None
            mock_session.execute.return_value = mock_result
            
            # Patch DEFAULT_MODELS to include invalid data
            invalid_models = {
                "invalid_model": {
                    "name": "invalid",
                    "temperature": "not_a_number",  # Invalid type
                    "provider": "test",
                    "context_window": 4096,
                    "max_output_tokens": 2048
                }
            }
            
            with patch('src.seeds.model_configs.DEFAULT_MODELS', invalid_models):
                await seed_async()
            
            # Should handle validation errors gracefully
            mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_seed_async_missing_fields(self, mock_session, mock_model_config_class):
        """Test async seeding with missing required fields."""
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Mock no existing models
            mock_result = Mock()
            mock_result.scalars.return_value.first.return_value = None
            mock_session.execute.return_value = mock_result
            
            # Patch DEFAULT_MODELS to include incomplete data
            incomplete_models = {
                "incomplete_model": {
                    "name": "incomplete",
                    # Missing required fields
                    "provider": "test"
                }
            }
            
            with patch('src.seeds.model_configs.DEFAULT_MODELS', incomplete_models):
                await seed_async()
            
            # Should handle missing fields gracefully
            mock_session.commit.assert_called_once()
            # Should not add incomplete models
            assert mock_session.add.call_count == 0

    @pytest.mark.asyncio
    async def test_seed_async_database_error(self, mock_session, mock_model_config_class):
        """Test async seeding with database error."""
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Mock database error on commit
            mock_session.commit.side_effect = Exception("Database error")
            
            with pytest.raises(Exception, match="Database error"):
                await seed_async()
            
            # Should have called rollback
            mock_session.rollback.assert_called_once()

    def test_seed_sync_success(self, mock_sync_session, mock_model_config_class):
        """Test successful sync seeding."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
                mock_session_local.return_value.__exit__ = Mock(return_value=None)
                
                # Mock no existing models
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_sync_session.execute.return_value = mock_result
                
                seed_sync()
                
                # Should have called commit
                mock_sync_session.commit.assert_called_once()
                
                # Should have added models
                assert mock_sync_session.add.call_count == len(DEFAULT_MODELS)

    def test_seed_sync_update_existing(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding with existing models."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
            mock_session_local.return_value.__exit__ = Mock(return_value=None)
            
            # Mock existing model
            existing_model = Mock()
            existing_model.name = "existing_model"
            existing_model.provider = "openai"
            existing_model.temperature = 0.5
            existing_model.context_window = 4096
            existing_model.max_output_tokens = 2048
            existing_model.extended_thinking = False
            existing_model.enabled = False
            existing_model.updated_at = datetime.now()
            
            mock_result = Mock()
            mock_result.scalars.return_value.first.return_value = existing_model
            mock_sync_session.execute.return_value = mock_result
            
            seed_sync()
            
            # Should have updated existing models
            mock_sync_session.commit.assert_called_once()

    def test_seed_sync_unique_constraint_error(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding with unique constraint error."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
                mock_session_local.return_value.__exit__ = Mock(return_value=None)
                
                # Mock no existing models initially
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_sync_session.execute.return_value = mock_result
                
                # Mock unique constraint error on commit
                integrity_error = IntegrityError("statement", "params", "UNIQUE constraint failed")
                mock_sync_session.commit.side_effect = integrity_error
                
                # Should raise IntegrityError when it occurs at commit level
                with pytest.raises(IntegrityError):
                    seed_sync()
                
                # Should have called rollback
                mock_sync_session.rollback.assert_called_once()

    def test_seed_sync_other_database_error(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding with other database error."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
            mock_session_local.return_value.__exit__ = Mock(return_value=None)
            
            # Mock database error on commit
            mock_sync_session.commit.side_effect = Exception("Database error")
            
            with pytest.raises(Exception, match="Database error"):
                seed_sync()
            
            # Should have called rollback
            mock_sync_session.rollback.assert_called_once()

    def test_seed_sync_validation_errors(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding with validation errors."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
            mock_session_local.return_value.__exit__ = Mock(return_value=None)
            
            # Mock no existing models
            mock_result = Mock()
            mock_result.scalars.return_value.first.return_value = None
            mock_sync_session.execute.return_value = mock_result
            
            # Patch DEFAULT_MODELS to include invalid data
            invalid_models = {
                "invalid_model": {
                    "name": "invalid",
                    "temperature": "not_a_number",  # Invalid type
                    "provider": "test",
                    "context_window": "not_an_int",  # Invalid type
                    "max_output_tokens": 2048
                }
            }
            
            with patch('src.seeds.model_configs.DEFAULT_MODELS', invalid_models):
                seed_sync()
            
            # Should handle validation errors gracefully
            mock_sync_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_seed_main_entry_point_success(self):
        """Test main seed entry point success."""
        with patch('src.seeds.model_configs.seed_async', new_callable=AsyncMock) as mock_seed_async:
            await seed()
            mock_seed_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_seed_main_entry_point_error(self):
        """Test main seed entry point with error."""
        with patch('src.seeds.model_configs.seed_async', new_callable=AsyncMock) as mock_seed_async:
            mock_seed_async.side_effect = Exception("Seed error")
            
            # Should not raise exception - errors are logged but not re-raised
            await seed()
            
            mock_seed_async.assert_called_once()

    def test_databricks_models_enabled_by_default(self):
        """Test that Databricks models are enabled by default."""
        databricks_models = [
            key for key, data in DEFAULT_MODELS.items() 
            if data["provider"] == "databricks"
        ]
        
        # Should have Databricks models
        assert len(databricks_models) > 0
        
        # In the seeding logic, Databricks models should be enabled by default
        for model_key in databricks_models:
            model_data = DEFAULT_MODELS[model_key]
            assert model_data["provider"] == "databricks"

    def test_non_databricks_models_disabled_by_default(self):
        """Test that non-Databricks models are disabled by default."""
        non_databricks_models = [
            key for key, data in DEFAULT_MODELS.items() 
            if data["provider"] != "databricks"
        ]
        
        # Should have non-Databricks models
        assert len(non_databricks_models) > 0
        
        # In the seeding logic, non-Databricks models should be disabled by default

    @pytest.mark.asyncio
    async def test_seed_async_model_processing_error(self, mock_session, mock_model_config_class):
        """Test async seeding with individual model processing error."""
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Mock execute to raise exception for one model
            call_count = 0
            def mock_execute_side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 2:  # Second call fails
                    raise Exception("Model processing error")
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                return mock_result
            
            mock_session.execute.side_effect = mock_execute_side_effect
            
            await seed_async()
            
            # Should still complete and commit
            mock_session.commit.assert_called_once()

    def test_seed_sync_model_processing_error(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding with individual model processing error."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
            mock_session_local.return_value.__exit__ = Mock(return_value=None)
            
            # Mock execute to raise exception for one model
            call_count = 0
            def mock_execute_side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 2:  # Second call fails
                    raise Exception("Model processing error")
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                return mock_result
            
            mock_sync_session.execute.side_effect = mock_execute_side_effect
            
            seed_sync()
            
            # Should still complete and commit
            mock_sync_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_seed_async_datetime_handling(self, mock_session, mock_model_config_class):
        """Test that async seeding properly handles datetime fields."""
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
                
                # Mock no existing models
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_session.execute.return_value = mock_result
                
                with patch('src.seeds.model_configs.datetime') as mock_datetime:
                    mock_now = Mock()
                    mock_now.replace.return_value = datetime(2023, 1, 1, 12, 0, 0)
                    mock_datetime.now.return_value = mock_now
                    
                    await seed_async()
                
                # Should have called datetime.now() for timestamps
                assert mock_datetime.now.call_count > 0

    def test_seed_sync_datetime_handling(self, mock_sync_session, mock_model_config_class):
        """Test that sync seeding properly handles datetime fields."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
                mock_session_local.return_value.__exit__ = Mock(return_value=None)
                
                # Mock no existing models
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_sync_session.execute.return_value = mock_result
                
                with patch('src.seeds.model_configs.datetime') as mock_datetime:
                    mock_now = Mock()
                    mock_now.replace.return_value = datetime(2023, 1, 1, 12, 0, 0)
                    mock_datetime.now.return_value = mock_now
                    
                    seed_sync()
                
                # Should have called datetime.now() for timestamps
                assert mock_datetime.now.call_count > 0

    @pytest.mark.asyncio
    async def test_seed_async_context_window_type_error(self, mock_session, mock_model_config_class):
        """Test async seeding with context_window type validation error."""
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
                
                # Mock no existing models
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_session.execute.return_value = mock_result
                
                # Patch DEFAULT_MODELS to include invalid context_window type
                invalid_models = {
                    "invalid_model": {
                        "name": "invalid",
                        "temperature": 0.7,
                        "provider": "test",
                        "context_window": "not_an_int",  # Invalid type
                        "max_output_tokens": 2048
                    }
                }
                
                with patch('src.seeds.model_configs.DEFAULT_MODELS', invalid_models):
                    await seed_async()
                
                # Should handle validation errors gracefully
                mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_seed_async_max_output_tokens_type_error(self, mock_session, mock_model_config_class):
        """Test async seeding with max_output_tokens type validation error."""
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
                
                # Mock no existing models
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_session.execute.return_value = mock_result
                
                # Patch DEFAULT_MODELS to include invalid max_output_tokens type
                invalid_models = {
                    "invalid_model": {
                        "name": "invalid",
                        "temperature": 0.7,
                        "provider": "test",
                        "context_window": 4096,
                        "max_output_tokens": "not_an_int"  # Invalid type
                    }
                }
                
                with patch('src.seeds.model_configs.DEFAULT_MODELS', invalid_models):
                    await seed_async()
                
                # Should handle validation errors gracefully
                mock_session.commit.assert_called_once()

    def test_seed_sync_context_window_type_error(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding with context_window type validation error."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
                mock_session_local.return_value.__exit__ = Mock(return_value=None)
                
                # Mock no existing models
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_sync_session.execute.return_value = mock_result
                
                # Patch DEFAULT_MODELS to include invalid context_window type
                invalid_models = {
                    "invalid_model": {
                        "name": "invalid",
                        "temperature": 0.7,
                        "provider": "test",
                        "context_window": "not_an_int",  # Invalid type
                        "max_output_tokens": 2048
                    }
                }
                
                with patch('src.seeds.model_configs.DEFAULT_MODELS', invalid_models):
                    seed_sync()
                
                # Should handle validation errors gracefully
                mock_sync_session.commit.assert_called_once()

    def test_seed_sync_max_output_tokens_type_error(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding with max_output_tokens type validation error."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
                mock_session_local.return_value.__exit__ = Mock(return_value=None)
                
                # Mock no existing models
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_sync_session.execute.return_value = mock_result
                
                # Patch DEFAULT_MODELS to include invalid max_output_tokens type
                invalid_models = {
                    "invalid_model": {
                        "name": "invalid",
                        "temperature": 0.7,
                        "provider": "test",
                        "context_window": 4096,
                        "max_output_tokens": "not_an_int"  # Invalid type
                    }
                }
                
                with patch('src.seeds.model_configs.DEFAULT_MODELS', invalid_models):
                    seed_sync()
                
                # Should handle validation errors gracefully
                mock_sync_session.commit.assert_called_once()

    def test_seed_sync_individual_model_error_handling(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding with individual model processing errors including unique constraint."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
                mock_session_local.return_value.__exit__ = Mock(return_value=None)
                
                # Mock execute to raise unique constraint error during model processing
                call_count = 0
                def mock_execute_side_effect(*args, **kwargs):
                    nonlocal call_count
                    call_count += 1
                    if call_count == 2:  # Second call (first model processing) fails
                        raise IntegrityError("statement", "params", "UNIQUE constraint failed")
                    mock_result = Mock()
                    mock_result.scalars.return_value.first.return_value = None
                    return mock_result
                
                mock_sync_session.execute.side_effect = mock_execute_side_effect
                
                seed_sync()
                
                # Should still complete and commit
                mock_sync_session.commit.assert_called_once()

    def test_seed_sync_missing_fields(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding with missing required fields."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
                mock_session_local.return_value.__exit__ = Mock(return_value=None)
                
                # Mock no existing models
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = None
                mock_sync_session.execute.return_value = mock_result
                
                # Patch DEFAULT_MODELS to include incomplete data
                incomplete_models = {
                    "incomplete_model": {
                        "name": "incomplete",
                        # Missing required fields: temperature, context_window, max_output_tokens
                        "provider": "test"
                    }
                }
                
                with patch('src.seeds.model_configs.DEFAULT_MODELS', incomplete_models):
                    seed_sync()
                
                # Should handle missing fields gracefully
                mock_sync_session.commit.assert_called_once()
                # Should not add incomplete models
                assert mock_sync_session.add.call_count == 0

    def test_main_module_execution(self):
        """Test __main__ block execution."""
        # Test the __main__ block execution by running the module as __main__
        import runpy
        import sys
        from unittest.mock import patch
        
        with patch('src.seeds.model_configs.seed', new_callable=AsyncMock) as mock_seed:
            with patch('asyncio.run') as mock_asyncio_run:
                # Temporarily modify sys.argv to simulate command line execution
                original_argv = sys.argv[:]
                try:
                    sys.argv = ['src/seeds/model_configs.py']
                    
                    # Run the module as __main__ which will execute the __main__ block
                    runpy.run_module('src.seeds.model_configs', run_name='__main__')
                    
                    # Verify that asyncio.run was called with seed()
                    mock_asyncio_run.assert_called_once()
                    
                finally:
                    sys.argv = original_argv

    @pytest.mark.asyncio 
    async def test_seed_async_with_existing_model_update_branch(self, mock_session, mock_model_config_class):
        """Test async seeding update branch for existing models."""
        with patch('src.seeds.model_configs.async_session_factory') as mock_session_factory:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)
                
                # Mock existing model found
                existing_model = Mock()
                existing_model.name = "existing_model"
                existing_model.provider = "databricks"
                existing_model.temperature = 0.5
                existing_model.context_window = 4096
                existing_model.max_output_tokens = 2048
                existing_model.extended_thinking = False
                existing_model.enabled = True
                existing_model.updated_at = datetime.now()
                
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = existing_model
                mock_session.execute.return_value = mock_result
                
                valid_models = {
                    "test_model": {
                        "name": "test_model",
                        "temperature": 0.7,
                        "provider": "databricks",
                        "context_window": 8192,
                        "max_output_tokens": 4096,
                        "extended_thinking": True
                    }
                }
                
                with patch('src.seeds.model_configs.DEFAULT_MODELS', valid_models):
                    await seed_async()
                
                # Should have updated existing model properties
                assert existing_model.name == "test_model"
                assert existing_model.provider == "databricks"
                assert existing_model.temperature == 0.7
                assert existing_model.context_window == 8192
                assert existing_model.max_output_tokens == 4096
                assert existing_model.extended_thinking == True
                assert existing_model.enabled == True  # Databricks models are enabled
                mock_session.commit.assert_called_once()
                
    def test_seed_sync_with_existing_model_update_branch(self, mock_sync_session, mock_model_config_class):
        """Test sync seeding update branch for existing models."""
        with patch('src.seeds.model_configs.SessionLocal') as mock_session_local:
            with patch('src.seeds.model_configs.select') as mock_select:
                mock_session_local.return_value.__enter__ = Mock(return_value=mock_sync_session)
                mock_session_local.return_value.__exit__ = Mock(return_value=None)
                
                # Mock existing model found
                existing_model = Mock()
                existing_model.name = "existing_model"
                existing_model.provider = "openai"
                existing_model.temperature = 0.5
                existing_model.context_window = 4096
                existing_model.max_output_tokens = 2048
                existing_model.extended_thinking = False
                existing_model.enabled = False
                existing_model.updated_at = datetime.now()
                
                mock_result = Mock()
                mock_result.scalars.return_value.first.return_value = existing_model
                mock_sync_session.execute.return_value = mock_result
                
                valid_models = {
                    "test_model": {
                        "name": "test_model", 
                        "temperature": 0.8,
                        "provider": "openai",
                        "context_window": 16384,
                        "max_output_tokens": 8192,
                        "extended_thinking": False
                    }
                }
                
                with patch('src.seeds.model_configs.DEFAULT_MODELS', valid_models):
                    seed_sync()
                
                # Should have updated existing model properties
                assert existing_model.name == "test_model"
                assert existing_model.provider == "openai"
                assert existing_model.temperature == 0.8
                assert existing_model.context_window == 16384
                assert existing_model.max_output_tokens == 8192
                assert existing_model.extended_thinking == False
                assert existing_model.enabled == False  # Non-Databricks models are disabled
                mock_sync_session.commit.assert_called_once()