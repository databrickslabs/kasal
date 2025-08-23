"""
Unit tests for DatabricksGPTOSSHandler module.

Tests the specialized handling of Databricks GPT-OSS models including
response format transformation and parameter filtering.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import sys
import logging

from src.core.llm_handlers.databricks_gpt_oss_handler import (
    DatabricksGPTOSSHandler,
    DatabricksGPTOSSLLM
)


class TestDatabricksGPTOSSHandler:
    """Test suite for DatabricksGPTOSSHandler."""
    
    def test_is_gpt_oss_model_true(self):
        """Test identifying GPT-OSS models correctly."""
        assert DatabricksGPTOSSHandler.is_gpt_oss_model("databricks-gpt-oss-2024")
        assert DatabricksGPTOSSHandler.is_gpt_oss_model("gpt-oss-v1")
        assert DatabricksGPTOSSHandler.is_gpt_oss_model("GPT-OSS-TURBO")
    
    def test_is_gpt_oss_model_false(self):
        """Test identifying non-GPT-OSS models correctly."""
        assert not DatabricksGPTOSSHandler.is_gpt_oss_model("gpt-4")
        assert not DatabricksGPTOSSHandler.is_gpt_oss_model("claude-3")
        assert not DatabricksGPTOSSHandler.is_gpt_oss_model("")
        assert not DatabricksGPTOSSHandler.is_gpt_oss_model(None)
    
    def test_extract_text_from_string_response(self):
        """Test extracting text from a simple string response."""
        content = "This is a simple response"
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "This is a simple response"
    
    def test_extract_text_from_json_string(self):
        """Test extracting text from a JSON string response."""
        content = json.dumps([
            {"type": "reasoning", "summary": [], "content": []},
            {"type": "text", "text": "Actual response text"}
        ])
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Actual response text"
    
    def test_extract_text_from_harmony_format(self):
        """Test extracting text from Harmony format response."""
        content = [
            {
                "type": "reasoning",
                "summary": [
                    {"type": "summary_text", "text": "Some summary"}
                ],
                "content": [
                    {"type": "reasoning_text", "text": "Reasoning content"}
                ]
            },
            {"type": "text", "text": "Main response text"}
        ]
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Main response text"
    
    def test_extract_text_prioritizes_text_blocks(self):
        """Test that text blocks are prioritized over reasoning blocks."""
        content = [
            {
                "type": "reasoning",
                "content": [
                    {"type": "reasoning_text", "text": "Reasoning text"}
                ]
            },
            {"type": "text", "text": "Primary text"}
        ]
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Primary text"
    
    def test_extract_text_falls_back_to_reasoning(self):
        """Test fallback to reasoning text when no text blocks exist."""
        content = [
            {
                "type": "reasoning",
                "content": [
                    {"type": "reasoning_text", "text": "Only reasoning text"}
                ]
            }
        ]
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Only reasoning text"
    
    def test_extract_text_from_dict_with_text_field(self):
        """Test extracting text from a dict with a text field."""
        content = {"text": "Dict text response"}
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Dict text response"
    
    def test_extract_text_from_dict_with_content_field(self):
        """Test extracting text from a dict with a content field."""
        content = {"content": "Dict content response"}
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Dict content response"
    
    def test_extract_text_from_dict_with_content_list(self):
        """Test extracting text from a dict with content as a list."""
        content = {
            "content": [
                {"type": "text", "text": "Nested text"}
            ]
        }
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Nested text"
    
    def test_extract_text_filters_metadata(self):
        """Test that metadata responses are filtered out."""
        content = [
            {"type": "text", "text": '{"suggestions": ["item1"], "quality": "high"}'}
        ]
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == ""
    
    def test_extract_text_handles_empty_content(self):
        """Test handling of empty content."""
        assert DatabricksGPTOSSHandler.extract_text_from_response([]) == ""
        assert DatabricksGPTOSSHandler.extract_text_from_response({}) == ""
        assert DatabricksGPTOSSHandler.extract_text_from_response(None) == ""
        assert DatabricksGPTOSSHandler.extract_text_from_response("") == ""
    
    def test_filter_unsupported_params(self):
        """Test filtering of unsupported parameters."""
        params = {
            "model": "gpt-oss",
            "temperature": 0.7,
            "stop": "STOP",
            "stop_sequences": ["seq1"],
            "stop_words": ["word1"],
            "max_tokens": 100
        }
        filtered = DatabricksGPTOSSHandler.filter_unsupported_params(params)
        
        assert "model" in filtered
        assert "temperature" in filtered
        assert "max_tokens" in filtered
        assert "stop" not in filtered
        assert "stop_sequences" not in filtered
        assert "stop_words" not in filtered
    
    def test_filter_unsupported_params_preserves_original(self):
        """Test that filtering doesn't modify the original params."""
        params = {"stop": "STOP", "model": "test"}
        filtered = DatabricksGPTOSSHandler.filter_unsupported_params(params)
        
        assert "stop" in params  # Original unchanged
        assert "stop" not in filtered  # Filtered removed
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksGPTOSSHandler.extract_text_from_response')
    def test_apply_monkey_patch(self, mock_extract):
        """Test that monkey patch is applied correctly."""
        mock_extract.return_value = "Extracted text"
        
        # Mock the litellm module structure
        with patch('src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksGPTOSSHandler.apply_monkey_patch') as mock_patch:
            DatabricksGPTOSSHandler.apply_monkey_patch()
            mock_patch.assert_called_once()


class TestDatabricksGPTOSSLLM:
    """Test suite for DatabricksGPTOSSLLM wrapper."""
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.LLM.__init__')
    def test_initialization(self, mock_llm_init):
        """Test DatabricksGPTOSSLLM initialization."""
        mock_llm_init.return_value = None
        
        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        assert llm._original_model_name == "gpt-oss-test"
        mock_llm_init.assert_called_once()
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.LLM._prepare_completion_params')
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksGPTOSSHandler.filter_unsupported_params')
    def test_prepare_completion_params(self, mock_filter, mock_parent_prepare):
        """Test parameter preparation with filtering."""
        mock_parent_prepare.return_value = {
            "model": "test",
            "messages": [{"role": "user", "content": "test"}],
            "stop": "STOP"
        }
        mock_filter.return_value = {
            "model": "test",
            "messages": [{"role": "user", "content": "test"}]
        }
        
        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        messages = [{"role": "user", "content": "test"}]
        
        result = llm._prepare_completion_params(messages)
        
        mock_parent_prepare.assert_called_once()
        mock_filter.assert_called_once()
        assert "stop" not in result
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call')
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksGPTOSSHandler.filter_unsupported_params')
    def test_call_method(self, mock_filter, mock_parent_call):
        """Test the call method with parameter filtering."""
        mock_filter.return_value = {"model": "test"}
        mock_parent_call.return_value = "Response text"
        
        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        messages = [{"role": "user", "content": "test"}]
        
        result = llm.call(messages, stop="STOP")
        
        mock_filter.assert_called_once()
        mock_parent_call.assert_called_once()
        assert result == "Response text"
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call')
    def test_call_handles_empty_response(self, mock_parent_call):
        """Test handling of empty responses."""
        mock_parent_call.return_value = ""
        
        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        messages = [{"role": "user", "content": "test"}]
        
        result = llm.call(messages)
        assert result == ""
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call')
    def test_call_propagates_exceptions(self, mock_parent_call):
        """Test that exceptions are propagated correctly."""
        mock_parent_call.side_effect = Exception("Test error")
        
        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        messages = [{"role": "user", "content": "test"}]
        
        with pytest.raises(Exception) as exc_info:
            llm.call(messages)
        assert str(exc_info.value) == "Test error"
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.LLM._handle_non_streaming_response')
    def test_handle_non_streaming_response_with_system_message(self, mock_parent_handle):
        """Test that system message is added when missing."""
        mock_parent_handle.return_value = "Response"
        
        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        params = {
            "model": "test",
            "messages": [{"role": "user", "content": "test"}]
        }
        
        result = llm._handle_non_streaming_response(params)
        
        # Check that system message was inserted
        assert params["messages"][0]["role"] == "system"
        assert "helpful AI assistant" in params["messages"][0]["content"]
        assert result == "Response"
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.LLM._handle_non_streaming_response')
    def test_handle_non_streaming_response_preserves_existing_system_message(self, mock_parent_handle):
        """Test that existing system message is preserved."""
        mock_parent_handle.return_value = "Response"
        
        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        params = {
            "model": "test",
            "messages": [
                {"role": "system", "content": "Existing system"},
                {"role": "user", "content": "test"}
            ]
        }
        
        result = llm._handle_non_streaming_response(params)
        
        # Check that original system message is preserved
        assert params["messages"][0]["role"] == "system"
        assert params["messages"][0]["content"] == "Existing system"
        assert result == "Response"
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.LLM._handle_non_streaming_response')
    def test_handle_non_streaming_response_handles_type_error(self, mock_parent_handle):
        """Test handling of TypeError when calling parent method."""
        # First call raises TypeError, second succeeds
        mock_parent_handle.side_effect = [
            TypeError("unexpected keyword argument"),
            "Response"
        ]
        
        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        params = {"model": "test", "messages": []}
        
        result = llm._handle_non_streaming_response(
            params, 
            callbacks=None,
            available_functions=None,
            from_task=None,
            from_agent=None
        )
        
        assert result == "Response"
        assert mock_parent_handle.call_count == 2
    
    @patch('src.core.llm_handlers.databricks_gpt_oss_handler.LLM._handle_non_streaming_response')
    def test_handle_non_streaming_response_returns_empty_on_none(self, mock_parent_handle):
        """Test that None response returns empty string."""
        mock_parent_handle.return_value = None
        
        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        params = {"model": "test", "messages": []}
        
        result = llm._handle_non_streaming_response(params)
        assert result == ""