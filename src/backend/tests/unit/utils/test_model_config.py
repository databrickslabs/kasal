"""
Unit tests for model_config module.
"""

import pytest
from unittest.mock import Mock, patch
from sqlalchemy.orm import Session

from src.utils.model_config import (
    get_model_config,
    get_max_rpm_for_model,
    model_supports_reasoning_effort,
)


class TestModelSupportsReasoningEffort:
    """The capability gate for the model's NATIVE reasoning budget.

    Kasal's reasoning control sets ``reasoning_effort`` (chat completions) /
    ``reasoning.effort`` (Responses API). Sending it to an endpoint that does not
    know the parameter is a 400 on strict gateways, so the gate must be a
    conservative allow-list: anything unproven is dropped silently.
    """

    @pytest.mark.parametrize("model", [
        "databricks-gpt-5",
        "databricks-gpt-5-2",
        "databricks-gpt-5-4-mini",
        "databricks-gpt-5-3-codex",
        "gpt-5",
        "openai/gpt-5.2",
        "databricks/gpt-5-2",       # provider-prefixed, as built on the LLM
        "databricks-gpt-oss-120b",
        "o3", "o3-mini", "o4-mini",
    ])
    def test_supported_models(self, model):
        assert model_supports_reasoning_effort(model) is True

    @pytest.mark.parametrize("model", [
        None, "",
        # Anthropic uses `thinking: {budget_tokens}`, not reasoning_effort.
        "databricks-claude-sonnet-4-5",
        "databricks-claude-opus-4-8",
        "claude-opus-4-20250514",
        # No reasoning_effort parameter on these request surfaces.
        "databricks-gemini-3-5-flash",
        "kimi-k2.7-code",
        "deepseek-reasoner",
        "Qwen3-Coder-30B-A3B-Instruct",
        "databricks-meta-llama-3-3-70b-instruct",
        "gpt-4o",
        # o1 predates reasoning_effort.
        "o1", "o1-preview", "o1-mini",
        # Deep-research models have a fixed internal budget.
        "o3-deep-research-2025-06-26",
        "o4-mini-deep-research-2025-06-26",
    ])
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
        
        with patch('src.utils.model_config.select'), \
             patch('src.models.model_config.ModelConfig'):
            
            result = get_model_config(model_key, mock_db)
            
            expected_config = {
                "key": "gpt-4",
                "name": "GPT-4",
                "provider": "openai",
                "temperature": 0.7,
                "context_window": 8192,
                "max_output_tokens": 4096,
                "extended_thinking": False,
                "enabled": True
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
        
        with patch('src.utils.model_config.select'), \
             patch('src.models.model_config.ModelConfig'):
            
            result = get_model_config(model_key, mock_db)
            
            assert result is None
            mock_db.execute.assert_called_once()
    
    def test_get_model_config_database_error(self):
        """Test model config retrieval with database error."""
        mock_db = Mock(spec=Session)
        model_key = "gpt-4"
        
        # Mock database exception
        mock_db.execute.side_effect = Exception("Database error")
        
        with patch('src.utils.model_config.select'), \
             patch('src.models.model_config.ModelConfig'):
            
            result = get_model_config(model_key, mock_db)
            
            assert result is None
    
    def test_get_model_config_no_database_session(self):
        """Test model config retrieval without database session."""
        model_key = "gpt-4"
        
        result = get_model_config(model_key, None)
        
        assert result is None


class TestGetMaxRpmForModel:
    """Test get_max_rpm_for_model function."""
    
    def test_get_max_rpm_for_known_openai_models(self):
        """Test RPM limits for the current OpenAI models."""
        assert get_max_rpm_for_model("gpt-5.6-sol") == 50
        assert get_max_rpm_for_model("gpt-5.6-terra") == 100
        assert get_max_rpm_for_model("gpt-5.6-luna") == 100
        assert get_max_rpm_for_model("o3-mini") == 100
        # Retired families still resolve through the generic patterns rather
        # than dropping to the unknown-model floor of 3.
        assert get_max_rpm_for_model("gpt-4") == 50
        assert get_max_rpm_for_model("gpt-3.5-turbo") == 200
    
    def test_get_max_rpm_for_known_anthropic_models(self):
        """Test RPM limits for Anthropic Claude models (Claude 4.x; Claude 3 retired)."""
        # Explicit dict entries.
        assert get_max_rpm_for_model("claude-opus-5") == 5
        assert get_max_rpm_for_model("claude-sonnet-5") == 10
        assert get_max_rpm_for_model("claude-haiku-4-5") == 20
        # Any other Claude model falls through to the generic Claude heuristic.
        assert get_max_rpm_for_model("databricks-claude-opus-4-8") == 10
        assert get_max_rpm_for_model("databricks-claude-sonnet-4-6") == 10
    
    def test_get_max_rpm_for_known_ollama_models(self):
        """Test RPM limits for known Ollama models."""
        assert get_max_rpm_for_model("qwen2.5:32b") == 5
        assert get_max_rpm_for_model("llama2") == 10
        assert get_max_rpm_for_model("llama3.2:latest") == 5
        assert get_max_rpm_for_model("mistral") == 10
        assert get_max_rpm_for_model("mixtral") == 5
        assert get_max_rpm_for_model("llama3.2:3b-text-q8_0") == 20
        assert get_max_rpm_for_model("gemma2:27b") == 5
        assert get_max_rpm_for_model("deepseek-r1:32b") == 5
    
    def test_get_max_rpm_for_known_deepseek_models(self):
        """Test RPM limits for known DeepSeek models."""
        assert get_max_rpm_for_model("deepseek-v4-flash") == 5
        assert get_max_rpm_for_model("deepseek-v4-pro") == 3
    
    def test_get_max_rpm_for_known_databricks_models(self):
        """Test RPM limits for known Databricks models."""
        assert get_max_rpm_for_model("databricks-meta-llama-3-3-70b-instruct") == 5
        assert get_max_rpm_for_model("databricks-meta-llama-3-1-405b-instruct") == 3
        assert get_max_rpm_for_model("databricks-claude-3-7-sonnet") == 10
    
    def test_get_max_rpm_for_known_google_models(self):
        """Test RPM limits for known Google models."""
        assert get_max_rpm_for_model("gemini-3.6-flash") == 10
        assert get_max_rpm_for_model("gemini-3.5-flash") == 10
        assert get_max_rpm_for_model("gemini-3.5-flash-lite") == 20
        # Unlisted Gemini models fall through to the provider heuristic.
        assert get_max_rpm_for_model("gemini-2.5-pro") == 10
    
    def test_get_max_rpm_for_unknown_model_with_gpt4_pattern(self):
        """Test RPM limits for unknown models with GPT-4 pattern."""
        assert get_max_rpm_for_model("gpt-4-custom-model") == 50
        assert get_max_rpm_for_model("gpt4-turbo-custom") == 50
    
    def test_get_max_rpm_for_unknown_model_with_gpt35_pattern(self):
        """Test RPM limits for unknown models with GPT-3.5 pattern."""
        assert get_max_rpm_for_model("gpt-3.5-custom") == 200
        assert get_max_rpm_for_model("gpt3-turbo") == 200
    
    def test_get_max_rpm_for_unknown_model_with_claude_opus_pattern(self):
        """Unknown Claude Opus models use the generic Claude heuristic (10)."""
        assert get_max_rpm_for_model("claude-opus-custom") == 10

    def test_get_max_rpm_for_unknown_model_with_claude_35_pattern(self):
        """Any unknown Claude model uses the generic Claude heuristic (10)."""
        assert get_max_rpm_for_model("claude-sonnet-custom") == 10
        assert get_max_rpm_for_model("claude-haiku-custom") == 10
    
    def test_get_max_rpm_for_unknown_model_with_claude_37_pattern(self):
        """Test RPM limits for unknown models with Claude 3.7 pattern."""
        assert get_max_rpm_for_model("claude-3-7-custom") == 10
    
    def test_get_max_rpm_for_unknown_model_with_llama_3b_pattern(self):
        """Test RPM limits for unknown models with small Llama pattern."""
        assert get_max_rpm_for_model("llama-custom-3b") == 20
    
    def test_get_max_rpm_for_unknown_model_with_llama_pattern(self):
        """Test RPM limits for unknown models with Llama pattern."""
        assert get_max_rpm_for_model("llama-custom-7b") == 5
        assert get_max_rpm_for_model("llama2-custom") == 5
    
    def test_get_max_rpm_for_unknown_model_with_mistral_pattern(self):
        """Test RPM limits for unknown models with Mistral pattern."""
        assert get_max_rpm_for_model("mistral-custom") == 5
        assert get_max_rpm_for_model("mixtral-custom") == 5
    
    def test_get_max_rpm_for_unknown_model_with_deepseek_pattern(self):
        """Test RPM limits for unknown models with DeepSeek pattern."""
        assert get_max_rpm_for_model("deepseek-custom") == 5
    
    def test_get_max_rpm_for_unknown_model_with_databricks_pattern(self):
        """Test RPM limits for unknown models with Databricks pattern."""
        assert get_max_rpm_for_model("databricks-custom-model") == 5
    
    def test_get_max_rpm_for_unknown_model_with_gemini_pattern(self):
        """Test RPM limits for unknown models with Gemini pattern."""
        assert get_max_rpm_for_model("gemini-custom") == 10
    
    def test_get_max_rpm_for_completely_unknown_model(self):
        """Test RPM limits for completely unknown models."""
        assert get_max_rpm_for_model("completely-unknown-model") == 3
        assert get_max_rpm_for_model("random-ai-model") == 3
        assert get_max_rpm_for_model("custom-proprietary-model") == 3
    
    def test_get_max_rpm_for_empty_string(self):
        """Test RPM limits for empty string model key."""
        assert get_max_rpm_for_model("") == 3
    
    def test_get_max_rpm_for_none_model(self):
        """Test RPM limits for None model key."""
        # This might raise an exception in real usage, but test the current behavior
        try:
            result = get_max_rpm_for_model(None)
            assert result == 3  # Conservative default
        except (AttributeError, TypeError):
            # Expected if the function doesn't handle None gracefully
            pass


class TestModelConfigIntegration:
    """Test integration scenarios for model_config."""
    
    def test_model_config_with_database_and_rpm_retrieval(self):
        """Test getting model config and corresponding RPM limit."""
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
        
        with patch('src.utils.model_config.select'), \
             patch('src.models.model_config.ModelConfig'):
            
            # Get model config
            config = get_model_config(model_key, mock_db)
            assert config is not None
            assert config["key"] == "gpt-4"
            
            # Get corresponding RPM limit
            rpm_limit = get_max_rpm_for_model(model_key)
            assert rpm_limit == 50  # Known GPT-4 limit
    
    def test_fallback_behavior_for_model_not_in_database(self):
        """Test behavior when model is not found in database but has known RPM limit."""
        mock_db = Mock(spec=Session)
        model_key = "gpt-4-new-variant"
        
        # Mock database returning None (model not found)
        mock_result = Mock()
        mock_result.scalars.return_value.first.return_value = None
        mock_db.execute.return_value = mock_result
        
        with patch('src.utils.model_config.select'), \
             patch('src.models.model_config.ModelConfig'):
            
            # Model config not found in database
            config = get_model_config(model_key, mock_db)
            assert config is None
            
            # But RPM limit can still be determined by pattern matching
            rpm_limit = get_max_rpm_for_model(model_key)
            assert rpm_limit == 50  # Should match GPT-4 pattern