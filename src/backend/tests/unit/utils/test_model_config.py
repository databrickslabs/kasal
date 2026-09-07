"""
Unit tests for model_config module.
"""

from unittest.mock import Mock, patch

import pytest
from sqlalchemy.orm import Session

from src.utils.model_config import (
    get_model_config,
    model_rejects_temperature,
    model_supports_reasoning_effort,
)


class TestModelRejectsTemperature:
    """Endpoints that 400 on `temperature`. There is no drop_params safety net —
    a param set here IS sent, so a miss is a hard request failure, not a warning.
    """

    @pytest.mark.parametrize(
        "model",
        [
            # REGRESSION: the guard enumerated 4-7/4-8 and so missed opus-5 the
            # day it shipped. Every crew run on it failed with "Model
            # global.anthropic.claude-opus-5 does not support the temperature
            # parameter." Both the Kasal key and the served name must match.
            "databricks-claude-opus-5",
            "global.anthropic.claude-opus-5",
            "us.anthropic.claude-opus-5",
            "databricks-claude-opus-4-7",
            "databricks-claude-opus-4-8",
            "databricks-claude-fable-5",
            # REGRESSION 2: the same enumeration then missed claude-sonnet-5.
            # Measured against the live workspace — EVERY Claude 5 rejects
            # temperature, so the family is not the discriminator, the generation
            # is. This one 400'd all 24 LLM calls in a 12-task flow and each fell
            # back to another model, so the run "succeeded" and the only symptom
            # was a trace full of "LLM Error: 400 ... does not support the
            # temperature parameter" beside answers from a model nobody picked.
            "databricks-claude-sonnet-5",
            "global.anthropic.claude-sonnet-5",
            "gpt-5",
            "databricks-gpt-5-1",
            "databricks-gpt-6-astra",
            "databricks-gpt-5-5-pro",
            # Live endpoint regression: the catalogue's 0.7 default made every
            # Gemini 3.8 request fail before generation.
            "databricks-gemini-3-8-flash",
        ],
    )
    def test_rejecting_models(self, model):
        assert model_rejects_temperature(model) is True

    def test_it_reads_the_capability_registry(self):
        """One source of truth, so the UI and the runtime cannot disagree.

        The registry already had claude-sonnet-5 down as refusing temperature
        while this function's own list did not — the UI correctly hid the control
        and the runtime sent the parameter anyway. A second list is the bug.
        """
        from src.core.llm.model_capabilities import model_capability

        for model in (
            "databricks-claude-sonnet-5",
            "databricks-claude-opus-5",
            "databricks-claude-sonnet-4-6",
            "databricks-claude-opus-4-1",
        ):
            capability = model_capability(model)
            assert capability is not None, model
            assert model_rejects_temperature(model) is (
                not capability.accepts("temperature")
            ), model

    @pytest.mark.parametrize(
        "model",
        [
            # Sonnet/Haiku accept temperature — dropping it would silently change
            # sampling for the DEFAULT_ENGINE_MODEL family.
            "databricks-claude-sonnet-4-6",
            # 4-5 is a MINOR version — measured as ACCEPTING temperature, unlike
            # the 5 generation.
            "databricks-claude-sonnet-4-5",
            "databricks-claude-haiku-4-5",
            "databricks-claude-opus-4-1",
            "databricks-claude-opus-4-6",
            "databricks-llama-4-maverick",
            None,
            "",
        ],
    )
    def test_accepting_models(self, model):
        assert model_rejects_temperature(model) is False


class TestModelSupportsReasoningEffort:
    """The capability gate for the model's NATIVE reasoning budget.

    Kasal's reasoning control sets ``reasoning_effort`` (chat completions) /
    ``reasoning.effort`` (Responses API). Sending it to an endpoint that does not
    know the parameter is a 400 on strict gateways, so the gate must be a
    conservative allow-list: anything unproven is dropped silently.
    """

    @pytest.mark.parametrize(
        "model",
        [
            "databricks-gpt-5",
            "databricks-gpt-5-2",
            "databricks-gpt-5-4-mini",
            "databricks-gpt-5-3-codex",
            "gpt-5",
            "openai/gpt-5.2",
            "databricks/gpt-5-2",  # provider-prefixed, as built on the LLM
            "databricks-gpt-oss-120b",
            "o3",
            "o3-mini",
            "o4-mini",
            # Gemini 3.x on Databricks. Probed live 2026-08-05 against
            # gemini-3-1-pro / 3-5-flash / 3-1-flash-lite: WITHOUT the param the
            # response has a text-only block and no thinking; WITH it a populated
            # `reasoning` block comes back (~2,000 chars of "**My Thought
            # Process...**"). The native Gemini `thinking` shape is rejected
            # (400 Invalid JSON payload), so reasoning_effort is the only lever —
            # and excluding it was silently costing us the visible
            # chain-of-thought these models are willing to give.
            "databricks-gemini-3-1-pro",
            "databricks-gemini-3-5-flash",
            "databricks-gemini-3-1-flash-lite",
            "databricks-gemini-3-8-flash",
            "databricks-grok-4-6",
            "databricks-glm-5-3",
            "databricks-kimi-k3",
            "databricks-deepseek-v4-flash-0731",
            "databricks-inkling",
        ],
    )
    def test_supported_models(self, model):
        assert model_supports_reasoning_effort(model) is True

    @pytest.mark.parametrize(
        "model",
        [
            None,
            "",
            # Anthropic uses `thinking: {...}`, not reasoning_effort — sending it
            # is a 400 ("reasoning_effort: Extra inputs are not permitted"). And
            # there is nothing to gain: on Databricks only
            # `thinking:{"type":"adaptive"}` is accepted, and even then the
            # reasoning block comes back with an EMPTY summary (Bedrock returns
            # the opaque `signature` only). Verified live 2026-08-05.
            "databricks-claude-sonnet-4-5",
            "databricks-claude-opus-4-8",
            "databricks-claude-fable-5",
            "databricks-claude-opus-5",
            "claude-opus-4-20250514",
            # No reasoning_effort parameter on these request surfaces. Note
            # kimi-k2-7-code DOES return thinking text — unprompted, in a sibling
            # `reasoning_content` field — so it needs no request-side flag.
            "kimi-k2.7-code",
            "deepseek-reasoner",
            "Qwen3-Coder-30B-A3B-Instruct",
            "databricks-meta-llama-3-3-70b-instruct",
            "gpt-4o",
            # o1 predates reasoning_effort.
            "o1",
            "o1-preview",
            "o1-mini",
            # Deep-research models have a fixed internal budget.
            "o3-deep-research-2025-06-26",
            "o4-mini-deep-research-2025-06-26",
        ],
    )
    def test_unsupported_models(self, model):
        assert model_supports_reasoning_effort(model) is False

    def test_kill_switch_env_disables_everything(self, monkeypatch):
        monkeypatch.setenv("KASAL_REASONING_EFFORT_DISABLED", "true")
        assert model_supports_reasoning_effort("databricks-gpt-5-2") is False

    def test_env_allow_list_extends_the_gate(self, monkeypatch):
        monkeypatch.setenv("KASAL_REASONING_EFFORT_MODELS", "my-endpoint, other")
        assert model_supports_reasoning_effort("prod-my-endpoint-v2") is True
        assert model_supports_reasoning_effort("other") is True
        assert model_supports_reasoning_effort("unrelated") is False

    def test_env_allow_list_beats_the_deep_research_exclusion(self, monkeypatch):
        """An explicit operator override is honored over the built-in exclusions."""
        monkeypatch.setenv("KASAL_REASONING_EFFORT_MODELS", "deep-research")
        assert model_supports_reasoning_effort("o3-deep-research-2025-06-26") is True


class TestGetModelConfig:
    """Test get_model_config function."""

    def test_get_model_config_found_in_database(self):
        """Test successful model config retrieval from database."""
        mock_db = Mock(spec=Session)
        model_key = "gpt-4"

        # Mock model config object
        mock_model_config = Mock()
        mock_model_config.key = "gpt-4"
        mock_model_config.name = "GPT-4"
        mock_model_config.provider = "openai"
        mock_model_config.temperature = 0.7
        mock_model_config.context_window = 8192
        mock_model_config.max_output_tokens = 4096
        mock_model_config.extended_thinking = False
        mock_model_config.enabled = True

        # Mock database query result
        mock_result = Mock()
        mock_result.scalars.return_value.first.return_value = mock_model_config
        mock_db.execute.return_value = mock_result

        with (
            patch("src.utils.model_config.select"),
            patch("src.models.model_config.ModelConfig"),
        ):

            result = get_model_config(model_key, mock_db)

            expected_config = {
                "key": "gpt-4",
                "name": "GPT-4",
                "provider": "openai",
                "temperature": 0.7,
                "context_window": 8192,
                "max_output_tokens": 4096,
                "extended_thinking": False,
                "enabled": True,
            }

            assert result == expected_config
            mock_db.execute.assert_called_once()

    def test_get_model_config_not_found_in_database(self):
        """Test model config retrieval when model not found in database."""
        mock_db = Mock(spec=Session)
        model_key = "non-existent-model"

        # Mock database query result with no model found
        mock_result = Mock()
        mock_result.scalars.return_value.first.return_value = None
        mock_db.execute.return_value = mock_result

        with (
            patch("src.utils.model_config.select"),
            patch("src.models.model_config.ModelConfig"),
        ):

            result = get_model_config(model_key, mock_db)

            assert result is None
            mock_db.execute.assert_called_once()

    def test_get_model_config_database_error(self):
        """Test model config retrieval with database error."""
        mock_db = Mock(spec=Session)
        model_key = "gpt-4"

        # Mock database exception
        mock_db.execute.side_effect = Exception("Database error")

        with (
            patch("src.utils.model_config.select"),
            patch("src.models.model_config.ModelConfig"),
        ):

            result = get_model_config(model_key, mock_db)

            assert result is None

    def test_get_model_config_no_database_session(self):
        """Test model config retrieval without database session."""
        model_key = "gpt-4"

        result = get_model_config(model_key, None)

        assert result is None


class TestModelConfigIntegration:
    """Test integration scenarios for model_config."""

    def test_model_config_from_database(self):
        """Test getting model config from the database."""
        mock_db = Mock(spec=Session)
        model_key = "gpt-4"

        # Mock successful database retrieval
        mock_model_config = Mock()
        mock_model_config.key = "gpt-4"
        mock_model_config.name = "GPT-4"
        mock_model_config.provider = "openai"
        mock_model_config.temperature = 0.7
        mock_model_config.context_window = 8192
        mock_model_config.max_output_tokens = 4096
        mock_model_config.extended_thinking = False
        mock_model_config.enabled = True

        mock_result = Mock()
        mock_result.scalars.return_value.first.return_value = mock_model_config
        mock_db.execute.return_value = mock_result

        with (
            patch("src.utils.model_config.select"),
            patch("src.models.model_config.ModelConfig"),
        ):

            # Get model config
            config = get_model_config(model_key, mock_db)
            assert config is not None
            assert config["key"] == "gpt-4"

    def test_fallback_behavior_for_model_not_in_database(self):
        """Test behavior when a model is not found in the database."""
        mock_db = Mock(spec=Session)
        model_key = "gpt-4-new-variant"

        # Mock database returning None (model not found)
        mock_result = Mock()
        mock_result.scalars.return_value.first.return_value = None
        mock_db.execute.return_value = mock_result

        with (
            patch("src.utils.model_config.select"),
            patch("src.models.model_config.ModelConfig"),
        ):

            # Model config not found in database
            config = get_model_config(model_key, mock_db)
            assert config is None
