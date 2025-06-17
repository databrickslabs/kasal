"""
Unit tests for model configuration schemas.

Tests the functionality of Pydantic schemas for model configuration operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.model_config import (
    ModelConfigBase, ModelConfigCreate, ModelConfigUpdate, ModelConfigResponse,
    ModelToggleUpdate, ModelListResponse
)


class TestModelConfigBase:
    """Test cases for ModelConfigBase schema."""
    
    def test_valid_model_config_base_minimal(self):
        """Test ModelConfigBase with minimal required fields."""
        config_data = {
            "key": "gpt-4",
            "name": "GPT-4"
        }
        config = ModelConfigBase(**config_data)
        assert config.key == "gpt-4"
        assert config.name == "GPT-4"
        assert config.provider is None
        assert config.temperature is None
        assert config.context_window is None
        assert config.max_output_tokens is None
        assert config.extended_thinking is False  # Default
        assert config.enabled is True  # Default
    
    def test_valid_model_config_base_complete(self):
        """Test ModelConfigBase with all fields."""
        config_data = {
            "key": "claude-3-opus",
            "name": "Claude 3 Opus",
            "provider": "anthropic",
            "temperature": 0.7,
            "context_window": 200000,
            "max_output_tokens": 4096,
            "extended_thinking": True,
            "enabled": True
        }
        config = ModelConfigBase(**config_data)
        assert config.key == "claude-3-opus"
        assert config.name == "Claude 3 Opus"
        assert config.provider == "anthropic"
        assert config.temperature == 0.7
        assert config.context_window == 200000
        assert config.max_output_tokens == 4096
        assert config.extended_thinking is True
        assert config.enabled is True
    
    def test_model_config_base_missing_required_fields(self):
        """Test ModelConfigBase validation with missing required fields."""
        # Missing key
        with pytest.raises(ValidationError) as exc_info:
            ModelConfigBase(name="Test Model")
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "key" in missing_fields
        
        # Missing name
        with pytest.raises(ValidationError) as exc_info:
            ModelConfigBase(key="test-model")
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "name" in missing_fields
    
    def test_model_config_base_temperature_validation(self):
        """Test ModelConfigBase temperature validation."""
        # Valid temperature values
        valid_temperatures = [0.0, 0.5, 1.0, 1.5, 2.0]
        for temp in valid_temperatures:
            config = ModelConfigBase(
                key="test-model",
                name="Test Model",
                temperature=temp
            )
            assert config.temperature == temp
        
        # Edge case: negative temperature (should be allowed by schema)
        config_negative = ModelConfigBase(
            key="test-model",
            name="Test Model",
            temperature=-0.1
        )
        assert config_negative.temperature == -0.1
        
        # Edge case: very high temperature (should be allowed by schema)
        config_high = ModelConfigBase(
            key="test-model",
            name="Test Model",
            temperature=10.0
        )
        assert config_high.temperature == 10.0
    
    def test_model_config_base_context_window_validation(self):
        """Test ModelConfigBase context window validation."""
        # Valid context window values
        valid_windows = [1000, 4096, 8192, 32768, 128000, 200000]
        for window in valid_windows:
            config = ModelConfigBase(
                key="test-model",
                name="Test Model",
                context_window=window
            )
            assert config.context_window == window
        
        # Zero context window
        config_zero = ModelConfigBase(
            key="test-model",
            name="Test Model",
            context_window=0
        )
        assert config_zero.context_window == 0
    
    def test_model_config_base_max_output_tokens_validation(self):
        """Test ModelConfigBase max output tokens validation."""
        # Valid max output token values
        valid_max_tokens = [256, 512, 1024, 2048, 4096, 8192]
        for max_tokens in valid_max_tokens:
            config = ModelConfigBase(
                key="test-model",
                name="Test Model",
                max_output_tokens=max_tokens
            )
            assert config.max_output_tokens == max_tokens
    
    def test_model_config_base_boolean_fields(self):
        """Test ModelConfigBase boolean fields."""
        # Test extended_thinking combinations
        config_thinking_true = ModelConfigBase(
            key="thinking-model",
            name="Thinking Model",
            extended_thinking=True
        )
        assert config_thinking_true.extended_thinking is True
        
        config_thinking_false = ModelConfigBase(
            key="regular-model",
            name="Regular Model",
            extended_thinking=False
        )
        assert config_thinking_false.extended_thinking is False
        
        # Test enabled combinations
        config_enabled = ModelConfigBase(
            key="active-model",
            name="Active Model",
            enabled=True
        )
        assert config_enabled.enabled is True
        
        config_disabled = ModelConfigBase(
            key="inactive-model",
            name="Inactive Model",
            enabled=False
        )
        assert config_disabled.enabled is False
    
    def test_model_config_base_various_providers(self):
        """Test ModelConfigBase with various providers."""
        providers = ["openai", "anthropic", "google", "databricks", "ollama", "custom_provider"]
        
        for provider in providers:
            config = ModelConfigBase(
                key=f"{provider}-model",
                name=f"{provider.title()} Model",
                provider=provider
            )
            assert config.provider == provider
    
    def test_model_config_base_realistic_examples(self):
        """Test ModelConfigBase with realistic model examples."""
        # GPT-4 configuration
        gpt4_config = ModelConfigBase(
            key="gpt-4-turbo-preview",
            name="GPT-4 Turbo Preview",
            provider="openai",
            temperature=0.7,
            context_window=128000,
            max_output_tokens=4096,
            extended_thinking=False,
            enabled=True
        )
        assert gpt4_config.key == "gpt-4-turbo-preview"
        assert gpt4_config.context_window == 128000
        
        # Claude configuration
        claude_config = ModelConfigBase(
            key="claude-3-5-sonnet-20241022",
            name="Claude 3.5 Sonnet",
            provider="anthropic",
            temperature=0.3,
            context_window=200000,
            max_output_tokens=8192,
            extended_thinking=True,
            enabled=True
        )
        assert claude_config.extended_thinking is True
        assert claude_config.max_output_tokens == 8192
        
        # Local model configuration
        local_config = ModelConfigBase(
            key="llama-3.2-local",
            name="Llama 3.2 (Local)",
            provider="ollama",
            temperature=0.8,
            context_window=8192,
            max_output_tokens=2048,
            extended_thinking=False,
            enabled=False  # Disabled by default
        )
        assert local_config.provider == "ollama"
        assert local_config.enabled is False


class TestModelConfigCreate:
    """Test cases for ModelConfigCreate schema."""
    
    def test_model_config_create_inheritance(self):
        """Test that ModelConfigCreate inherits from ModelConfigBase."""
        create_data = {
            "key": "new-model",
            "name": "New Model",
            "provider": "test_provider",
            "temperature": 0.5
        }
        create_config = ModelConfigCreate(**create_data)
        
        # Should have all base class attributes
        assert hasattr(create_config, 'key')
        assert hasattr(create_config, 'name')
        assert hasattr(create_config, 'provider')
        assert hasattr(create_config, 'temperature')
        assert hasattr(create_config, 'context_window')
        assert hasattr(create_config, 'max_output_tokens')
        assert hasattr(create_config, 'extended_thinking')
        assert hasattr(create_config, 'enabled')
        
        # Values should match
        assert create_config.key == "new-model"
        assert create_config.name == "New Model"
        assert create_config.provider == "test_provider"
        assert create_config.temperature == 0.5
    
    def test_model_config_create_same_validation(self):
        """Test that ModelConfigCreate has same validation as base."""
        # Should fail with missing required fields
        with pytest.raises(ValidationError):
            ModelConfigCreate(name="Test")
        
        # Should succeed with required fields
        create_config = ModelConfigCreate(
            key="valid-model",
            name="Valid Model"
        )
        assert create_config.key == "valid-model"
        assert create_config.name == "Valid Model"


class TestModelConfigUpdate:
    """Test cases for ModelConfigUpdate schema."""
    
    def test_model_config_update_inheritance(self):
        """Test that ModelConfigUpdate inherits from ModelConfigBase."""
        update_data = {
            "key": "updated-model",
            "name": "Updated Model",
            "temperature": 0.9,
            "enabled": False
        }
        update_config = ModelConfigUpdate(**update_data)
        
        # Should have all base class attributes
        assert hasattr(update_config, 'key')
        assert hasattr(update_config, 'name')
        assert hasattr(update_config, 'provider')
        assert hasattr(update_config, 'temperature')
        assert hasattr(update_config, 'enabled')
        
        # Values should match
        assert update_config.key == "updated-model"
        assert update_config.name == "Updated Model"
        assert update_config.temperature == 0.9
        assert update_config.enabled is False
    
    def test_model_config_update_partial_updates(self):
        """Test ModelConfigUpdate with partial field updates."""
        # Update only temperature
        temp_update = ModelConfigUpdate(
            key="existing-model",
            name="Existing Model",
            temperature=0.2
        )
        assert temp_update.temperature == 0.2
        assert temp_update.provider is None  # Other fields use defaults
        
        # Update only enabled status
        status_update = ModelConfigUpdate(
            key="existing-model",
            name="Existing Model",
            enabled=False
        )
        assert status_update.enabled is False
        assert status_update.temperature is None


class TestModelConfigResponse:
    """Test cases for ModelConfigResponse schema."""
    
    def test_valid_model_config_response(self):
        """Test ModelConfigResponse with valid data."""
        now = datetime.now()
        response_data = {
            "key": "response-model",
            "name": "Response Model",
            "provider": "openai",
            "temperature": 0.7,
            "context_window": 8192,
            "max_output_tokens": 2048,
            "extended_thinking": True,
            "enabled": True,
            "id": 123,
            "created_at": now,
            "updated_at": now
        }
        response = ModelConfigResponse(**response_data)
        
        # Should have all base class attributes
        assert response.key == "response-model"
        assert response.name == "Response Model"
        assert response.provider == "openai"
        assert response.temperature == 0.7
        assert response.extended_thinking is True
        
        # Should have response-specific attributes
        assert response.id == 123
        assert response.created_at == now
        assert response.updated_at == now
    
    def test_model_config_response_missing_response_fields(self):
        """Test ModelConfigResponse validation with missing response-specific fields."""
        base_data = {
            "key": "test-model",
            "name": "Test Model"
        }
        
        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            ModelConfigResponse(**base_data, created_at=datetime.now(), updated_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        
        # Missing created_at
        with pytest.raises(ValidationError) as exc_info:
            ModelConfigResponse(**base_data, id=1, updated_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "created_at" in missing_fields
        
        # Missing updated_at
        with pytest.raises(ValidationError) as exc_info:
            ModelConfigResponse(**base_data, id=1, created_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "updated_at" in missing_fields
    
    def test_model_config_response_config(self):
        """Test ModelConfigResponse model configuration."""
        assert hasattr(ModelConfigResponse, 'model_config')
        assert ModelConfigResponse.model_config.get('from_attributes') is True
    
    def test_model_config_response_datetime_handling(self):
        """Test ModelConfigResponse with datetime handling."""
        response_data = {
            "key": "datetime-test",
            "name": "DateTime Test",
            "id": 456,
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:30:00"
        }
        response = ModelConfigResponse(**response_data)
        assert isinstance(response.created_at, datetime)
        assert isinstance(response.updated_at, datetime)


class TestModelToggleUpdate:
    """Test cases for ModelToggleUpdate schema."""
    
    def test_valid_model_toggle_update_enabled(self):
        """Test ModelToggleUpdate enabling a model."""
        toggle_data = {"enabled": True}
        toggle = ModelToggleUpdate(**toggle_data)
        assert toggle.enabled is True
    
    def test_valid_model_toggle_update_disabled(self):
        """Test ModelToggleUpdate disabling a model."""
        toggle_data = {"enabled": False}
        toggle = ModelToggleUpdate(**toggle_data)
        assert toggle.enabled is False
    
    def test_model_toggle_update_missing_enabled(self):
        """Test ModelToggleUpdate validation with missing enabled field."""
        with pytest.raises(ValidationError) as exc_info:
            ModelToggleUpdate()
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "enabled" in missing_fields
    
    def test_model_toggle_update_non_boolean(self):
        """Test ModelToggleUpdate with non-boolean enabled value."""
        # Test with string that can be converted to boolean
        toggle_string_true = ModelToggleUpdate(enabled="true")
        assert toggle_string_true.enabled is True
        
        toggle_string_false = ModelToggleUpdate(enabled="false")
        assert toggle_string_false.enabled is False  # Pydantic parses "false" as False
        
        # Test with other string values
        toggle_string_yes = ModelToggleUpdate(enabled="yes")
        assert toggle_string_yes.enabled is True  # Non-empty string is truthy
        
        # Test with integer that can be converted to boolean
        toggle_int_true = ModelToggleUpdate(enabled=1)
        assert toggle_int_true.enabled is True
        
        toggle_int_false = ModelToggleUpdate(enabled=0)
        assert toggle_int_false.enabled is False


class TestModelListResponse:
    """Test cases for ModelListResponse schema."""
    
    def test_valid_model_list_response_empty(self):
        """Test ModelListResponse with empty models list."""
        list_data = {
            "models": [],
            "count": 0
        }
        model_list = ModelListResponse(**list_data)
        assert model_list.models == []
        assert model_list.count == 0
    
    def test_valid_model_list_response_with_models(self):
        """Test ModelListResponse with model configurations."""
        now = datetime.now()
        models = [
            ModelConfigResponse(
                key="model-1",
                name="Model 1",
                provider="openai",
                id=1,
                created_at=now,
                updated_at=now
            ),
            ModelConfigResponse(
                key="model-2",
                name="Model 2",
                provider="anthropic",
                id=2,
                created_at=now,
                updated_at=now
            ),
            ModelConfigResponse(
                key="model-3",
                name="Model 3",
                provider="google",
                id=3,
                created_at=now,
                updated_at=now
            )
        ]
        
        list_data = {
            "models": models,
            "count": 3
        }
        model_list = ModelListResponse(**list_data)
        assert len(model_list.models) == 3
        assert model_list.count == 3
        assert model_list.models[0].key == "model-1"
        assert model_list.models[1].provider == "anthropic"
        assert model_list.models[2].name == "Model 3"
    
    def test_model_list_response_missing_fields(self):
        """Test ModelListResponse validation with missing fields."""
        # Missing models
        with pytest.raises(ValidationError) as exc_info:
            ModelListResponse(count=5)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "models" in missing_fields
        
        # Missing count
        with pytest.raises(ValidationError) as exc_info:
            ModelListResponse(models=[])
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "count" in missing_fields
    
    def test_model_list_response_count_mismatch(self):
        """Test ModelListResponse with count mismatch."""
        now = datetime.now()
        models = [
            ModelConfigResponse(
                key="single-model",
                name="Single Model",
                id=1,
                created_at=now,
                updated_at=now
            )
        ]
        
        # Count doesn't match actual models length
        list_data = {
            "models": models,
            "count": 5  # Mismatch: actual length is 1
        }
        model_list = ModelListResponse(**list_data)
        assert len(model_list.models) == 1
        assert model_list.count == 5  # Schema allows this inconsistency
    
    def test_model_list_response_large_list(self):
        """Test ModelListResponse with large list of models."""
        now = datetime.now()
        models = []
        for i in range(50):
            models.append(ModelConfigResponse(
                key=f"model-{i}",
                name=f"Model {i}",
                provider=f"provider-{i % 5}",
                temperature=0.1 * (i % 10),
                enabled=i % 2 == 0,  # Alternate enabled/disabled
                id=i + 1,
                created_at=now,
                updated_at=now
            ))
        
        list_data = {
            "models": models,
            "count": 50
        }
        model_list = ModelListResponse(**list_data)
        assert len(model_list.models) == 50
        assert model_list.count == 50
        assert model_list.models[0].key == "model-0"
        assert model_list.models[49].key == "model-49"
        
        # Verify enabled/disabled pattern
        enabled_models = [m for m in model_list.models if m.enabled]
        disabled_models = [m for m in model_list.models if not m.enabled]
        assert len(enabled_models) == 25
        assert len(disabled_models) == 25


class TestModelConfigSchemaIntegration:
    """Integration tests for model configuration schema interactions."""
    
    def test_model_config_lifecycle(self):
        """Test complete model configuration lifecycle."""
        now = datetime.now()
        
        # Create model configuration
        create_config = ModelConfigCreate(
            key="lifecycle-test-model",
            name="Lifecycle Test Model",
            provider="test_provider",
            temperature=0.7,
            context_window=16384,
            max_output_tokens=4096,
            extended_thinking=True,
            enabled=True
        )
        
        # Simulate model creation response
        response_config = ModelConfigResponse(
            **create_config.model_dump(),
            id=100,
            created_at=now,
            updated_at=now
        )
        
        # Update model configuration
        update_config = ModelConfigUpdate(
            key="lifecycle-test-model",
            name="Updated Lifecycle Test Model",
            temperature=0.5,  # Changed temperature
            enabled=False     # Disabled
        )
        
        # Toggle model status
        toggle_update = ModelToggleUpdate(enabled=True)
        
        # Verify lifecycle
        assert create_config.key == response_config.key
        assert create_config.temperature == response_config.temperature
        assert response_config.id == 100
        assert update_config.temperature == 0.5
        assert update_config.enabled is False
        assert toggle_update.enabled is True
    
    def test_model_list_filtering_scenarios(self):
        """Test model list with various filtering scenarios."""
        now = datetime.now()
        
        # Create models with different characteristics
        all_models = [
            ModelConfigResponse(
                key="gpt-4",
                name="GPT-4",
                provider="openai",
                enabled=True,
                id=1,
                created_at=now,
                updated_at=now
            ),
            ModelConfigResponse(
                key="claude-3-opus",
                name="Claude 3 Opus",
                provider="anthropic",
                enabled=True,
                id=2,
                created_at=now,
                updated_at=now
            ),
            ModelConfigResponse(
                key="experimental-model",
                name="Experimental Model",
                provider="research",
                enabled=False,
                id=3,
                created_at=now,
                updated_at=now
            )
        ]
        
        # All models list
        all_models_list = ModelListResponse(
            models=all_models,
            count=3
        )
        
        # Enabled models only
        enabled_models = [m for m in all_models if m.enabled]
        enabled_models_list = ModelListResponse(
            models=enabled_models,
            count=2
        )
        
        # Models by provider
        openai_models = [m for m in all_models if m.provider == "openai"]
        openai_models_list = ModelListResponse(
            models=openai_models,
            count=1
        )
        
        # Verify filtering
        assert all_models_list.count == 3
        assert enabled_models_list.count == 2
        assert all(m.enabled for m in enabled_models_list.models)
        assert openai_models_list.count == 1
        assert openai_models_list.models[0].provider == "openai"
    
    def test_model_configuration_validation_scenarios(self):
        """Test various model configuration validation scenarios."""
        # High-performance model configuration
        high_perf_config = ModelConfigBase(
            key="high-performance-model",
            name="High Performance Model",
            provider="custom",
            temperature=0.1,  # Low temperature for deterministic output
            context_window=1000000,  # Very large context window
            max_output_tokens=32768,  # Large output capacity
            extended_thinking=True,
            enabled=True
        )
        assert high_perf_config.context_window == 1000000
        assert high_perf_config.temperature == 0.1
        
        # Creative model configuration
        creative_config = ModelConfigBase(
            key="creative-model",
            name="Creative Model",
            provider="anthropic",
            temperature=1.5,  # High temperature for creativity
            context_window=8192,
            max_output_tokens=4096,
            extended_thinking=False,
            enabled=True
        )
        assert creative_config.temperature == 1.5
        assert creative_config.extended_thinking is False
        
        # Minimal model configuration
        minimal_config = ModelConfigBase(
            key="minimal-model",
            name="Minimal Model"
        )
        assert minimal_config.temperature is None
        assert minimal_config.enabled is True  # Default
        assert minimal_config.extended_thinking is False  # Default
    
    def test_model_provider_management(self):
        """Test model configuration management by provider."""
        now = datetime.now()
        
        # Create models for different providers
        provider_models = {
            "openai": [
                ModelConfigResponse(
                    key="gpt-4-turbo", name="GPT-4 Turbo", provider="openai",
                    id=1, created_at=now, updated_at=now
                ),
                ModelConfigResponse(
                    key="gpt-3.5-turbo", name="GPT-3.5 Turbo", provider="openai",
                    id=2, created_at=now, updated_at=now
                )
            ],
            "anthropic": [
                ModelConfigResponse(
                    key="claude-3-opus", name="Claude 3 Opus", provider="anthropic",
                    id=3, created_at=now, updated_at=now
                ),
                ModelConfigResponse(
                    key="claude-3-sonnet", name="Claude 3 Sonnet", provider="anthropic",
                    id=4, created_at=now, updated_at=now
                )
            ]
        }
        
        # Create provider-specific lists
        openai_list = ModelListResponse(
            models=provider_models["openai"],
            count=2
        )
        
        anthropic_list = ModelListResponse(
            models=provider_models["anthropic"],
            count=2
        )
        
        # Verify provider management
        assert all(m.provider == "openai" for m in openai_list.models)
        assert all(m.provider == "anthropic" for m in anthropic_list.models)
        assert openai_list.count == 2
        assert anthropic_list.count == 2
    
    def test_model_toggle_workflow(self):
        """Test model enable/disable workflow."""
        now = datetime.now()
        
        # Initial model (enabled)
        initial_model = ModelConfigResponse(
            key="toggle-test-model",
            name="Toggle Test Model",
            enabled=True,
            id=500,
            created_at=now,
            updated_at=now
        )
        assert initial_model.enabled is True
        
        # Disable model
        disable_toggle = ModelToggleUpdate(enabled=False)
        # Simulate updated model after toggle
        disabled_model = ModelConfigResponse(
            **initial_model.model_dump(exclude={"enabled", "updated_at"}),
            enabled=disable_toggle.enabled,
            updated_at=datetime.now()
        )
        assert disabled_model.enabled is False
        
        # Re-enable model
        enable_toggle = ModelToggleUpdate(enabled=True)
        # Simulate updated model after toggle
        enabled_model = ModelConfigResponse(
            **disabled_model.model_dump(exclude={"enabled", "updated_at"}),
            enabled=enable_toggle.enabled,
            updated_at=datetime.now()
        )
        assert enabled_model.enabled is True
        
        # Verify workflow consistency
        assert initial_model.key == disabled_model.key == enabled_model.key
        assert initial_model.id == disabled_model.id == enabled_model.id