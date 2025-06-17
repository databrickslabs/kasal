"""
Unit tests for model provider schemas.

Tests the functionality of model provider enums and constants
including validation of providers and supported models.
"""
import pytest

from src.schemas.model_provider import ModelProvider, SUPPORTED_MODELS


class TestModelProvider:
    """Test cases for ModelProvider enum."""
    
    def test_model_provider_values(self):
        """Test ModelProvider enum values."""
        assert ModelProvider.OPENAI == "openai"
        assert ModelProvider.ANTHROPIC == "anthropic"
        assert ModelProvider.OLLAMA == "ollama"
        assert ModelProvider.DEEPSEEK == "deepseek"
        assert ModelProvider.DATABRICKS == "databricks"
        assert ModelProvider.GEMINI == "gemini"
    
    def test_model_provider_all_values(self):
        """Test that all expected ModelProvider values are present."""
        expected_values = {
            "openai", "anthropic", "ollama", "deepseek", "databricks", "gemini"
        }
        actual_values = {provider.value for provider in ModelProvider}
        assert actual_values == expected_values
    
    def test_model_provider_count(self):
        """Test that ModelProvider has the expected number of providers."""
        assert len(ModelProvider) == 6
    
    def test_model_provider_string_behavior(self):
        """Test ModelProvider string behavior."""
        # Test that enum values can be used as strings
        provider = ModelProvider.OPENAI
        assert provider.value == "openai"
        assert f"Provider: {provider.value}" == "Provider: openai"
    
    def test_model_provider_comparison(self):
        """Test ModelProvider comparison operations."""
        assert ModelProvider.OPENAI == "openai"
        assert ModelProvider.ANTHROPIC != "openai"
        assert ModelProvider.DATABRICKS == ModelProvider.DATABRICKS
    
    def test_model_provider_iteration(self):
        """Test iterating over ModelProvider enum."""
        providers = list(ModelProvider)
        assert len(providers) == 6
        assert ModelProvider.OPENAI in providers
        assert ModelProvider.ANTHROPIC in providers
        assert ModelProvider.OLLAMA in providers
        assert ModelProvider.DEEPSEEK in providers
        assert ModelProvider.DATABRICKS in providers
        assert ModelProvider.GEMINI in providers


class TestSupportedModels:
    """Test cases for SUPPORTED_MODELS dictionary."""
    
    def test_supported_models_structure(self):
        """Test SUPPORTED_MODELS dictionary structure."""
        assert isinstance(SUPPORTED_MODELS, dict)
        assert len(SUPPORTED_MODELS) == 6
        
        # Check that all ModelProvider values are keys
        for provider in ModelProvider:
            assert provider in SUPPORTED_MODELS
            assert isinstance(SUPPORTED_MODELS[provider], list)
    
    def test_openai_models(self):
        """Test OpenAI models list."""
        openai_models = SUPPORTED_MODELS[ModelProvider.OPENAI]
        assert isinstance(openai_models, list)
        assert len(openai_models) > 0
        
        # Check for specific known models
        assert "gpt-4" in openai_models
        assert "gpt-3.5-turbo" in openai_models
        assert "gpt-4o" in openai_models
        assert "o1-mini" in openai_models
        assert "o3-mini" in openai_models
        
        # Verify all models are strings
        for model in openai_models:
            assert isinstance(model, str)
            assert len(model) > 0
    
    def test_anthropic_models(self):
        """Test Anthropic models list."""
        anthropic_models = SUPPORTED_MODELS[ModelProvider.ANTHROPIC]
        assert isinstance(anthropic_models, list)
        assert len(anthropic_models) > 0
        
        # Check for specific known models
        assert "claude-3-opus-20240229" in anthropic_models
        assert "claude-3-5-sonnet-20241022" in anthropic_models
        assert "claude-3-7-sonnet-20250219" in anthropic_models
        assert "claude-2.1" in anthropic_models
        
        # Verify all models are strings
        for model in anthropic_models:
            assert isinstance(model, str)
            assert len(model) > 0
    
    def test_ollama_models(self):
        """Test Ollama models list."""
        ollama_models = SUPPORTED_MODELS[ModelProvider.OLLAMA]
        assert isinstance(ollama_models, list)
        assert len(ollama_models) > 0
        
        # Check for specific known models
        assert "llama2" in ollama_models
        assert "mistral" in ollama_models
        assert "codellama" in ollama_models
        assert "qwen2.5:32b" in ollama_models
        
        # Verify all models are strings
        for model in ollama_models:
            assert isinstance(model, str)
            assert len(model) > 0
    
    def test_deepseek_models(self):
        """Test DeepSeek models list."""
        deepseek_models = SUPPORTED_MODELS[ModelProvider.DEEPSEEK]
        assert isinstance(deepseek_models, list)
        assert len(deepseek_models) > 0
        
        # Check for specific known models
        assert "deepseek-chat" in deepseek_models
        assert "deepseek-reasoner" in deepseek_models
        
        # Verify all models are strings
        for model in deepseek_models:
            assert isinstance(model, str)
            assert len(model) > 0
    
    def test_databricks_models(self):
        """Test Databricks models list."""
        databricks_models = SUPPORTED_MODELS[ModelProvider.DATABRICKS]
        assert isinstance(databricks_models, list)
        assert len(databricks_models) > 0
        
        # Check for specific known models
        assert "databricks-meta-llama-3-3-70b-instruct" in databricks_models
        assert "databricks-meta-llama-3-1-405b-instruct" in databricks_models
        
        # Verify all models are strings
        for model in databricks_models:
            assert isinstance(model, str)
            assert len(model) > 0
    
    def test_gemini_models(self):
        """Test Gemini models list."""
        gemini_models = SUPPORTED_MODELS[ModelProvider.GEMINI]
        assert isinstance(gemini_models, list)
        assert len(gemini_models) > 0
        
        # Check for specific known models
        assert "gemini-2.5-pro" in gemini_models
        assert "gemini-2.0-flash" in gemini_models
        
        # Verify all models are strings
        for model in gemini_models:
            assert isinstance(model, str)
            assert len(model) > 0
    
    def test_all_providers_have_models(self):
        """Test that all providers have at least one model."""
        for provider in ModelProvider:
            models = SUPPORTED_MODELS[provider]
            assert len(models) > 0, f"Provider {provider} has no models"
    
    def test_model_uniqueness_within_provider(self):
        """Test that models are unique within each provider."""
        for provider, models in SUPPORTED_MODELS.items():
            unique_models = set(models)
            assert len(unique_models) == len(models), f"Duplicate models found for {provider}"
    
    def test_model_naming_conventions(self):
        """Test model naming conventions."""
        for provider, models in SUPPORTED_MODELS.items():
            for model in models:
                # All models should be non-empty strings
                assert isinstance(model, str)
                assert len(model) > 0
                assert not model.isspace()
                
                # Check provider-specific naming patterns
                if provider == ModelProvider.OPENAI:
                    assert any(pattern in model for pattern in ["gpt", "o1", "o3"])
                elif provider == ModelProvider.ANTHROPIC:
                    assert "claude" in model
                elif provider == ModelProvider.DATABRICKS:
                    assert "databricks" in model
                elif provider == ModelProvider.DEEPSEEK:
                    assert "deepseek" in model
                elif provider == ModelProvider.GEMINI:
                    assert "gemini" in model
                # Ollama models are more varied, so no strict pattern check


class TestModelProviderIntegration:
    """Integration tests for model provider functionality."""
    
    def test_provider_model_lookup(self):
        """Test looking up models for each provider."""
        for provider in ModelProvider:
            models = SUPPORTED_MODELS[provider]
            assert isinstance(models, list)
            assert len(models) > 0
            
            # Test that we can iterate through models
            for model in models:
                assert isinstance(model, str)
                assert len(model) > 0
    
    def test_model_validation_scenario(self):
        """Test a realistic model validation scenario."""
        def is_model_supported(provider_name: str, model_name: str) -> bool:
            """Check if a model is supported by a provider."""
            try:
                provider = ModelProvider(provider_name)
                return model_name in SUPPORTED_MODELS[provider]
            except ValueError:
                return False
        
        # Test valid combinations
        assert is_model_supported("openai", "gpt-4")
        assert is_model_supported("anthropic", "claude-3-opus-20240229")
        assert is_model_supported("databricks", "databricks-meta-llama-3-3-70b-instruct")
        
        # Test invalid combinations
        assert not is_model_supported("openai", "claude-3-opus-20240229")
        assert not is_model_supported("anthropic", "gpt-4")
        assert not is_model_supported("invalid_provider", "any_model")
        assert not is_model_supported("openai", "non_existent_model")
    
    def test_provider_statistics(self):
        """Test statistics about providers and models."""
        total_models = sum(len(models) for models in SUPPORTED_MODELS.values())
        assert total_models > 20  # Ensure we have a reasonable number of models
        
        # Check provider with most models
        provider_model_counts = {
            provider: len(models) 
            for provider, models in SUPPORTED_MODELS.items()
        }
        
        max_models = max(provider_model_counts.values())
        min_models = min(provider_model_counts.values())
        
        assert max_models >= min_models  # Basic sanity check
        assert min_models > 0  # All providers should have at least one model
    
    def test_supported_models_completeness(self):
        """Test that SUPPORTED_MODELS covers all providers."""
        # Ensure every enum value has a corresponding entry
        for provider in ModelProvider:
            assert provider in SUPPORTED_MODELS
        
        # Ensure no extra entries in SUPPORTED_MODELS
        enum_values = set(ModelProvider)
        dict_keys = set(SUPPORTED_MODELS.keys())
        assert enum_values == dict_keys
    
    def test_model_provider_enum_extensibility(self):
        """Test that the enum can be extended (conceptually)."""
        # Test that we can work with provider values programmatically
        provider_names = [provider.value for provider in ModelProvider]
        assert "openai" in provider_names
        assert "anthropic" in provider_names
        
        # Test enum membership
        assert ModelProvider.OPENAI in ModelProvider
        assert "invalid_provider" not in [p.value for p in ModelProvider]
    
    def test_real_world_usage_patterns(self):
        """Test realistic usage patterns."""
        # Simulate getting models for UI dropdown
        provider_options = []
        for provider in ModelProvider:
            models = SUPPORTED_MODELS[provider]
            provider_options.append({
                "provider": provider.value,
                "models": models,
                "model_count": len(models)
            })
        
        assert len(provider_options) == 6
        
        # Verify each option has the expected structure
        for option in provider_options:
            assert "provider" in option
            assert "models" in option
            assert "model_count" in option
            assert option["model_count"] > 0
            assert len(option["models"]) == option["model_count"]
    
    def test_model_search_functionality(self):
        """Test searching for models across providers."""
        def find_providers_for_model_pattern(pattern: str):
            """Find providers that have models matching a pattern."""
            matching_providers = []
            for provider, models in SUPPORTED_MODELS.items():
                for model in models:
                    if pattern.lower() in model.lower():
                        matching_providers.append(provider)
                        break
            return matching_providers
        
        # Test specific patterns
        gpt_providers = find_providers_for_model_pattern("gpt")
        assert ModelProvider.OPENAI in gpt_providers
        
        claude_providers = find_providers_for_model_pattern("claude")
        assert ModelProvider.ANTHROPIC in claude_providers
        
        llama_providers = find_providers_for_model_pattern("llama")
        assert len(llama_providers) >= 1  # Should find at least Ollama and Databricks