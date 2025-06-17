"""
Unit tests for model_config module.
"""

import pytest
from unittest.mock import Mock, patch
from sqlalchemy.orm import Session

from src.utils.model_config import (
    get_model_config,
    get_max_rpm_for_model
)


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
        """Test RPM limits for known OpenAI models."""
        assert get_max_rpm_for_model("gpt-4") == 50
        assert get_max_rpm_for_model("gpt-4-0125-preview") == 50
        assert get_max_rpm_for_model("gpt-4o-mini") == 100
        assert get_max_rpm_for_model("gpt-4o") == 100
        assert get_max_rpm_for_model("o1-mini") == 100
        assert get_max_rpm_for_model("o1") == 100
        assert get_max_rpm_for_model("o3-mini") == 100
        assert get_max_rpm_for_model("gpt-3.5-turbo") == 200
    
    def test_get_max_rpm_for_known_anthropic_models(self):
        """Test RPM limits for known Anthropic models."""
        assert get_max_rpm_for_model("claude-3-opus-20240229") == 5
        assert get_max_rpm_for_model("claude-3-5-sonnet-20241022") == 10
        assert get_max_rpm_for_model("claude-3-5-haiku-20241022") == 20
        assert get_max_rpm_for_model("claude-3-7-sonnet-20250219") == 10
        assert get_max_rpm_for_model("claude-3-7-sonnet-20250219-thinking") == 5
    
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
        assert get_max_rpm_for_model("deepseek-chat") == 5
        assert get_max_rpm_for_model("deepseek-reasoner") == 3
    
    def test_get_max_rpm_for_known_databricks_models(self):
        """Test RPM limits for known Databricks models."""
        assert get_max_rpm_for_model("databricks-meta-llama-3-3-70b-instruct") == 5
        assert get_max_rpm_for_model("databricks-meta-llama-3-1-405b-instruct") == 3
        assert get_max_rpm_for_model("databricks-claude-3-7-sonnet") == 10
    
    def test_get_max_rpm_for_known_google_models(self):
        """Test RPM limits for known Google models."""
        assert get_max_rpm_for_model("gemini-2.5-pro") == 10
        assert get_max_rpm_for_model("gemini-2.0-flash") == 10
    
    def test_get_max_rpm_for_unknown_model_with_gpt4_pattern(self):
        """Test RPM limits for unknown models with GPT-4 pattern."""
        assert get_max_rpm_for_model("gpt-4-custom-model") == 50
        assert get_max_rpm_for_model("gpt4-turbo-custom") == 50
    
    def test_get_max_rpm_for_unknown_model_with_gpt35_pattern(self):
        """Test RPM limits for unknown models with GPT-3.5 pattern."""
        assert get_max_rpm_for_model("gpt-3.5-custom") == 200
        assert get_max_rpm_for_model("gpt3-turbo") == 200
    
    def test_get_max_rpm_for_unknown_model_with_claude_opus_pattern(self):
        """Test RPM limits for unknown models with Claude Opus pattern."""
        assert get_max_rpm_for_model("claude-3-opus-custom") == 5
    
    def test_get_max_rpm_for_unknown_model_with_claude_35_pattern(self):
        """Test RPM limits for unknown models with Claude 3.5 pattern."""
        assert get_max_rpm_for_model("claude-3-5-custom") == 20
        assert get_max_rpm_for_model("claude-3-haiku-custom") == 20
    
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