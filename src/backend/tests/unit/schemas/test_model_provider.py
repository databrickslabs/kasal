"""
Unit tests for model provider schemas.

Tests the ModelProvider enum and the SUPPORTED_MODELS mapping.

These tests deliberately assert *invariants* rather than a snapshot of the
catalogue. The previous version hardcoded model names (gpt-4o, gpt-3.5-turbo,
claude-opus-4-20250514, gemini-2.0-flash, deepseek-chat...) and provider counts,
so it failed the moment a provider was added and would have gone on passing
while the catalogue listed models the providers had retired. SUPPORTED_MODELS is
now derived from DEFAULT_MODELS, and what is worth testing is that the
derivation holds — not which models happen to be current today.
"""

import pytest

from src.schemas.model_provider import SUPPORTED_MODELS, ModelProvider
from src.seeds.model_configs import DEFAULT_MODELS, REMOVED_MODEL_KEYS


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
        assert ModelProvider.VLLM == "vllm"
        assert ModelProvider.KIMI == "kimi"

    def test_model_provider_values_are_unique_lowercase(self):
        """Provider values are the keys used in seeds and API payloads."""
        values = [provider.value for provider in ModelProvider]
        assert len(set(values)) == len(values)
        for value in values:
            assert value == value.lower()
            assert value.strip() == value

    def test_model_provider_string_behavior(self):
        """Test ModelProvider string behavior."""
        provider = ModelProvider.OPENAI
        assert provider.value == "openai"
        assert f"Provider: {provider.value}" == "Provider: openai"

    def test_model_provider_comparison(self):
        """Test ModelProvider comparison operations."""
        assert ModelProvider.OPENAI == "openai"
        assert ModelProvider.ANTHROPIC != "openai"
        assert ModelProvider.DATABRICKS == ModelProvider.DATABRICKS

    def test_every_seeded_provider_has_an_enum_member(self):
        """A model cannot be seeded for a provider the enum does not know."""
        seeded = {config["provider"] for config in DEFAULT_MODELS.values()}
        known = {provider.value for provider in ModelProvider}
        assert seeded <= known, f"Seeded providers missing from enum: {seeded - known}"


class TestSupportedModels:
    """Test cases for the SUPPORTED_MODELS mapping."""

    def test_supported_models_structure(self):
        """Every provider maps to a list of model keys."""
        assert isinstance(SUPPORTED_MODELS, dict)
        assert set(SUPPORTED_MODELS.keys()) == set(ModelProvider)
        for provider in ModelProvider:
            assert isinstance(SUPPORTED_MODELS[provider], list)

    def test_derived_from_seed_catalogue(self):
        """SUPPORTED_MODELS is exactly DEFAULT_MODELS grouped by provider.

        This is the property that keeps the two lists from drifting apart.
        """
        expected = {provider: [] for provider in ModelProvider}
        for key, config in DEFAULT_MODELS.items():
            expected[ModelProvider(config["provider"])].append(key)
        assert SUPPORTED_MODELS == expected

    def test_no_retired_models_are_listed(self):
        """Keys queued for pruning must not also be advertised as supported."""
        listed = {model for models in SUPPORTED_MODELS.values() for model in models}
        leaked = listed & set(REMOVED_MODEL_KEYS)
        assert not leaked, f"Retired models still listed as supported: {leaked}"

    def test_all_providers_have_models(self):
        """Test that all providers have at least one model."""
        for provider in ModelProvider:
            assert (
                len(SUPPORTED_MODELS[provider]) > 0
            ), f"Provider {provider} has no models"

    def test_model_uniqueness_within_provider(self):
        """Test that models are unique within each provider."""
        for provider, models in SUPPORTED_MODELS.items():
            assert len(set(models)) == len(
                models
            ), f"Duplicate models found for {provider}"

    def test_model_keys_are_globally_unique(self):
        """A model key belongs to exactly one provider."""
        seen = {}
        for provider, models in SUPPORTED_MODELS.items():
            for model in models:
                assert (
                    model not in seen
                ), f"{model} listed under both {seen.get(model)} and {provider}"
                seen[model] = provider

    def test_model_naming_conventions(self):
        """Provider-specific naming patterns hold for hosted providers."""
        patterns = {
            ModelProvider.OPENAI: ("gpt", "o1", "o3", "o4"),
            ModelProvider.ANTHROPIC: ("claude",),
            ModelProvider.DATABRICKS: ("databricks",),
            ModelProvider.DEEPSEEK: ("deepseek",),
            ModelProvider.GEMINI: ("gemini",),
            ModelProvider.KIMI: ("kimi",),
        }
        for provider, models in SUPPORTED_MODELS.items():
            for model in models:
                assert isinstance(model, str)
                assert model.strip() == model
                assert len(model) > 0
                # Ollama and self-hosted vLLM model names are arbitrary
                # (whatever the serving endpoint was started with), so they
                # carry no pattern constraint.
                if provider in patterns:
                    assert any(
                        p in model.lower() for p in patterns[provider]
                    ), f"{model} does not look like a {provider.value} model"


class TestModelProviderIntegration:
    """Integration tests for model provider functionality."""

    def test_model_validation_scenario(self):
        """Test a realistic model validation scenario."""

        def is_model_supported(provider_name: str, model_name: str) -> bool:
            try:
                provider = ModelProvider(provider_name)
            except ValueError:
                return False
            return model_name in SUPPORTED_MODELS[provider]

        # A model drawn from the catalogue validates against its own provider
        # and only against its own provider.
        for provider in ModelProvider:
            model = SUPPORTED_MODELS[provider][0]
            assert is_model_supported(provider.value, model)
            for other in ModelProvider:
                if other is not provider:
                    assert not is_model_supported(other.value, model)

        assert not is_model_supported("invalid_provider", "any_model")
        assert not is_model_supported("openai", "non_existent_model")

    def test_provider_statistics(self):
        """Every provider ships models and the totals are consistent."""
        total_models = sum(len(models) for models in SUPPORTED_MODELS.values())
        assert total_models == len(DEFAULT_MODELS)
        assert min(len(models) for models in SUPPORTED_MODELS.values()) > 0

    def test_real_world_usage_patterns(self):
        """Simulate building a provider/model dropdown."""
        provider_options = [
            {
                "provider": provider.value,
                "models": SUPPORTED_MODELS[provider],
                "model_count": len(SUPPORTED_MODELS[provider]),
            }
            for provider in ModelProvider
        ]

        assert len(provider_options) == len(ModelProvider)
        for option in provider_options:
            assert option["model_count"] > 0
            assert len(option["models"]) == option["model_count"]

    def test_model_search_functionality(self):
        """Test searching for models across providers."""

        def find_providers_for_model_pattern(pattern: str):
            matching_providers = []
            for provider, models in SUPPORTED_MODELS.items():
                if any(pattern.lower() in model.lower() for model in models):
                    matching_providers.append(provider)
            return matching_providers

        assert ModelProvider.OPENAI in find_providers_for_model_pattern("gpt")
        assert ModelProvider.ANTHROPIC in find_providers_for_model_pattern("claude")
        assert len(find_providers_for_model_pattern("llama")) >= 1
