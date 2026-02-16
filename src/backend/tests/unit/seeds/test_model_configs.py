"""
Unit tests for model configs seed module.

Tests the DEFAULT_MODELS data structure, data integrity, and seed functions.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.seeds.model_configs import (
    DEFAULT_MODELS,
    MODEL_CONFIGS,
    seed_async,
    seed,
)


class TestDefaultModelsDataStructure:
    """Test cases for DEFAULT_MODELS data integrity."""

    def test_default_models_is_dict(self):
        """Test that DEFAULT_MODELS is a dictionary."""
        assert isinstance(DEFAULT_MODELS, dict)

    def test_default_models_not_empty(self):
        """Test that DEFAULT_MODELS contains entries."""
        assert len(DEFAULT_MODELS) > 0

    def test_model_configs_alias(self):
        """Test that MODEL_CONFIGS is an alias for DEFAULT_MODELS."""
        assert MODEL_CONFIGS is DEFAULT_MODELS

    def test_specific_models_exist(self):
        """Test that well-known model keys are present."""
        assert "gpt-4-turbo" in DEFAULT_MODELS
        assert "gpt-4o" in DEFAULT_MODELS
        assert "claude-3-5-sonnet-20241022" in DEFAULT_MODELS
        assert "databricks-llama-4-maverick" in DEFAULT_MODELS
        assert "deepseek-chat" in DEFAULT_MODELS

    def test_required_fields_present(self):
        """Test that every model has the required fields."""
        required_fields = [
            "name", "temperature", "provider",
            "context_window", "max_output_tokens",
        ]
        for model_key, model_data in DEFAULT_MODELS.items():
            for field in required_fields:
                assert field in model_data, (
                    f"Model '{model_key}' missing required field '{field}'"
                )

    def test_temperature_types(self):
        """Test that temperature values are numeric."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(model_data["temperature"], (int, float)), (
                f"Model '{model_key}' has non-numeric temperature"
            )

    def test_temperature_range(self):
        """Test that temperature values are in a reasonable range."""
        for model_key, model_data in DEFAULT_MODELS.items():
            temp = model_data["temperature"]
            assert 0.0 <= temp <= 2.0, (
                f"Model '{model_key}' has temperature {temp} outside [0, 2]"
            )

    def test_context_window_type(self):
        """Test that context_window values are integers."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(model_data["context_window"], int), (
                f"Model '{model_key}' has non-integer context_window"
            )

    def test_context_window_positive(self):
        """Test that context_window values are positive."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert model_data["context_window"] > 0, (
                f"Model '{model_key}' has non-positive context_window"
            )

    def test_max_output_tokens_type(self):
        """Test that max_output_tokens values are integers."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(model_data["max_output_tokens"], int), (
                f"Model '{model_key}' has non-integer max_output_tokens"
            )

    def test_max_output_tokens_positive(self):
        """Test that max_output_tokens values are positive."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert model_data["max_output_tokens"] > 0, (
                f"Model '{model_key}' has non-positive max_output_tokens"
            )

    def test_provider_type(self):
        """Test that provider values are strings."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(model_data["provider"], str), (
                f"Model '{model_key}' has non-string provider"
            )

    def test_valid_providers(self):
        """Test that all providers are known."""
        valid_providers = {
            "openai", "anthropic", "gemini",
            "ollama", "databricks", "deepseek",
        }
        for model_key, model_data in DEFAULT_MODELS.items():
            assert model_data["provider"] in valid_providers, (
                f"Model '{model_key}' has unknown provider '{model_data['provider']}'"
            )

    def test_name_type(self):
        """Test that name values are non-empty strings."""
        for model_key, model_data in DEFAULT_MODELS.items():
            assert isinstance(model_data["name"], str)
            assert len(model_data["name"]) > 0, (
                f"Model '{model_key}' has empty name"
            )

    def test_extended_thinking_models(self):
        """Test that at least one model has extended_thinking enabled."""
        extended_models = [
            k for k, v in DEFAULT_MODELS.items()
            if v.get("extended_thinking", False)
        ]
        assert len(extended_models) > 0
        assert "claude-3-7-sonnet-20250219-thinking" in extended_models

    def test_databricks_models_exist(self):
        """Test that Databricks models are present."""
        databricks_models = [
            k for k, v in DEFAULT_MODELS.items()
            if v["provider"] == "databricks"
        ]
        assert len(databricks_models) > 0

    def test_ollama_models_exist(self):
        """Test that Ollama models are present."""
        ollama_models = [
            k for k, v in DEFAULT_MODELS.items()
            if v["provider"] == "ollama"
        ]
        assert len(ollama_models) > 0


class TestSeedAsyncFunction:
    """Test cases for the seed_async function."""

    @pytest.mark.asyncio
    async def test_seed_async_adds_new_models(self):
        """Test that seed_async adds new models when none exist."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None

        with patch("src.seeds.model_configs.async_session_factory", return_value=mock_context):
            await seed_async()

        mock_session.commit.assert_awaited_once()
        assert mock_session.add.call_count == len(DEFAULT_MODELS)

    @pytest.mark.asyncio
    async def test_seed_async_updates_existing_models(self):
        """Test that seed_async updates existing models."""
        existing_model = MagicMock()
        existing_model.name = "old_name"

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = existing_model
        mock_session.execute.return_value = mock_result

        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None

        with patch("src.seeds.model_configs.async_session_factory", return_value=mock_context):
            await seed_async()

        mock_session.commit.assert_awaited_once()
        # Should not add since all models exist
        mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_seed_async_handles_db_error(self):
        """Test that seed_async rolls back on database error."""
        mock_session = AsyncMock()
        mock_session.commit.side_effect = Exception("DB error")
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result

        mock_context = AsyncMock()
        mock_context.__aenter__.return_value = mock_session
        mock_context.__aexit__.return_value = None

        with patch("src.seeds.model_configs.async_session_factory", return_value=mock_context):
            with pytest.raises(Exception, match="DB error"):
                await seed_async()

        mock_session.rollback.assert_awaited_once()


class TestSeedEntryPoint:
    """Test cases for the main seed() entry point."""

    @pytest.mark.asyncio
    async def test_seed_calls_seed_async(self):
        """Test that seed() delegates to seed_async()."""
        with patch("src.seeds.model_configs.seed_async", new_callable=AsyncMock) as mock:
            await seed()
            mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_seed_does_not_raise_on_error(self):
        """Test that seed() suppresses exceptions and logs them."""
        with patch("src.seeds.model_configs.seed_async", new_callable=AsyncMock) as mock:
            mock.side_effect = Exception("Seed failure")
            # Should not raise
            await seed()
            mock.assert_awaited_once()
