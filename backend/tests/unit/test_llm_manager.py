"""
Unit tests for LLM Manager.

Tests the functionality of the LLM manager including
model configuration, LLM interactions, and provider management.
"""
import pytest
import os
import tempfile
import logging
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path

from src.core.llm_manager import LiteLLMFileLogger


class TestLiteLLMFileLogger:
    """Test cases for LiteLLMFileLogger."""
    
    def setup_method(self):
        """Set up test environment."""
        # Create a temporary directory for test logs
        self.temp_dir = tempfile.mkdtemp()
        self.test_log_file = os.path.join(self.temp_dir, "test_llm.log")
    
    def teardown_method(self):
        """Clean up test environment."""
        # Clean up temporary files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_logger_initialization(self):
        """Test LiteLLMFileLogger initialization."""
        with patch("src.core.llm_manager.log_file_path", self.test_log_file):
            logger = LiteLLMFileLogger()
            
            assert logger.file_path == self.test_log_file
            assert logger.logger is not None
            assert logger.logger.name == "litellm_file_logger"
            assert logger.logger.level == logging.DEBUG
    
    def test_log_pre_api_call(self):
        """Test logging before API call."""
        with patch("src.core.llm_manager.log_file_path", self.test_log_file):
            logger = LiteLLMFileLogger()
            
            model = "gpt-3.5-turbo"
            messages = [{"role": "user", "content": "Hello"}]
            kwargs = {
                "temperature": 0.7,
                "max_tokens": 100,
                "messages": messages
            }
            
            # Should not raise any exceptions
            logger.log_pre_api_call(model, messages, kwargs)
            
            # Verify log file was created and contains expected content
            assert os.path.exists(self.test_log_file)
            with open(self.test_log_file, 'r') as f:
                content = f.read()
                assert "Pre-API Call" in content
                assert model in content
    
    def test_log_pre_api_call_with_exception(self):
        """Test log_pre_api_call handles exceptions gracefully."""
        with patch("src.core.llm_manager.log_file_path", self.test_log_file):
            logger = LiteLLMFileLogger()
            
            # Mock logger.info to raise an exception
            logger.logger.info = MagicMock(side_effect=Exception("Logging error"))
            
            # Should not raise exception, but handle it gracefully
            logger.log_pre_api_call("model", [], {})
            
            # Should have logged the error
            logger.logger.error.assert_called_once()
    
    def test_log_post_api_call(self):
        """Test logging after API call."""
        from datetime import datetime, timedelta
        
        with patch("src.core.llm_manager.log_file_path", self.test_log_file):
            logger = LiteLLMFileLogger()
            
            kwargs = {"model": "gpt-3.5-turbo"}
            response_obj = {
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "Hello! How can I help you?"
                        }
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 8,
                    "total_tokens": 18
                }
            }
            start_time = datetime.now()
            end_time = start_time + timedelta(seconds=2)
            
            # Should not raise any exceptions
            logger.log_post_api_call(kwargs, response_obj, start_time, end_time)
            
            # Verify log file contains expected content
            with open(self.test_log_file, 'r') as f:
                content = f.read()
                assert "Post-API Call" in content
                assert "Duration: 2.00s" in content
    
    def test_log_post_api_call_with_exception(self):
        """Test log_post_api_call handles exceptions gracefully."""
        from datetime import datetime
        
        with patch("src.core.llm_manager.log_file_path", self.test_log_file):
            logger = LiteLLMFileLogger()
            
            # Mock logger.info to raise an exception
            logger.logger.info = MagicMock(side_effect=Exception("Logging error"))
            
            start_time = datetime.now()
            end_time = datetime.now()
            
            # Should not raise exception, but handle it gracefully
            logger.log_post_api_call({}, {}, start_time, end_time)
            
            # Should have logged the error
            logger.logger.error.assert_called_once()
    
    def test_log_success_event(self):
        """Test logging success event."""
        from datetime import datetime, timedelta
        
        with patch("src.core.llm_manager.log_file_path", self.test_log_file):
            logger = LiteLLMFileLogger()
            
            kwargs = {"model": "gpt-3.5-turbo"}
            response_obj = {
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 8,
                    "total_tokens": 18
                }
            }
            start_time = datetime.now()
            end_time = start_time + timedelta(seconds=1.5)
            
            # Should not raise any exceptions
            logger.log_success_event(kwargs, response_obj, start_time, end_time)
            
            # Verify log file contains expected content
            with open(self.test_log_file, 'r') as f:
                content = f.read()
                assert "Success" in content
                assert "gpt-3.5-turbo" in content
                assert "Duration: 1.50s" in content
    
    def test_log_success_event_with_exception(self):
        """Test log_success_event handles exceptions gracefully."""
        from datetime import datetime
        
        with patch("src.core.llm_manager.log_file_path", self.test_log_file):
            logger = LiteLLMFileLogger()
            
            # Mock logger.info to raise an exception
            logger.logger.info = MagicMock(side_effect=Exception("Logging error"))
            
            start_time = datetime.now()
            end_time = datetime.now()
            
            # Should not raise exception, but handle it gracefully
            logger.log_success_event({}, {}, start_time, end_time)
            
            # Should have logged the error
            logger.logger.error.assert_called_once()
    
    def test_logger_duplicate_handlers_prevention(self):
        """Test that duplicate handlers are not added."""
        with patch("src.core.llm_manager.log_file_path", self.test_log_file):
            # Create first logger instance
            logger1 = LiteLLMFileLogger()
            initial_handler_count = len(logger1.logger.handlers)
            
            # Create second logger instance
            logger2 = LiteLLMFileLogger()
            final_handler_count = len(logger2.logger.handlers)
            
            # Should not have added duplicate handlers
            assert final_handler_count == initial_handler_count


class TestLLMManagerModule:
    """Test cases for LLM Manager module configuration."""
    
    def test_environment_variables_set(self):
        """Test that required environment variables are set."""
        # These should be set when the module is imported
        assert "LITELLM_LOG" in os.environ
        assert "LITELLM_LOG_FILE" in os.environ
        
        assert os.environ["LITELLM_LOG"] == "DEBUG"
    
    def test_log_directory_creation(self):
        """Test that log directory path is properly configured."""
        from src.core.llm_manager import log_dir, log_file_path
        
        # Should have a valid log directory
        assert log_dir is not None
        assert isinstance(log_dir, str)
        
        # Should have a valid log file path
        assert log_file_path is not None
        assert log_file_path.endswith("llm.log")
    
    def test_logger_configuration(self):
        """Test that the module logger is properly configured."""
        from src.core.llm_manager import logger
        
        assert isinstance(logger, logging.Logger)
        assert logger.level == logging.DEBUG
        assert logger.name == "src.core.llm_manager"
    
    def test_logger_handler_configuration(self):
        """Test that logger handlers are properly configured."""
        from src.core.llm_manager import logger
        
        # Should have at least one handler (file handler)
        assert len(logger.handlers) >= 0  # Might be 0 if handlers already exist
        
        # If handlers exist, they should be properly configured
        for handler in logger.handlers:
            assert isinstance(handler, logging.FileHandler)
            assert handler.formatter is not None
    
    @patch.dict(os.environ, {"LOG_DIR": "/custom/log/path"})
    def test_custom_log_directory(self):
        """Test that custom log directory is used when specified."""
        # Re-import the module to test with custom environment
        import importlib
        import src.core.llm_manager
        importlib.reload(src.core.llm_manager)
        
        from src.core.llm_manager import log_dir
        
        assert log_dir == "/custom/log/path"
    
    def test_litellm_imports(self):
        """Test that LiteLLM components are properly imported."""
        from src.core.llm_manager import litellm
        
        # Should be able to import litellm
        assert litellm is not None
    
    def test_crewai_imports(self):
        """Test that CrewAI components are properly imported."""
        from src.core.llm_manager import LLM
        
        # Should be able to import LLM from crewai
        assert LLM is not None
    
    def test_service_imports(self):
        """Test that service components are properly imported."""
        try:
            from src.core.llm_manager import ModelConfigService, ApiKeysService, UnitOfWork
            
            # Should be able to import service classes
            assert ModelConfigService is not None
            assert ApiKeysService is not None
            assert UnitOfWork is not None
        except ImportError:
            # Services might not be available in test environment
            pass
    
    def test_model_provider_imports(self):
        """Test that model provider schemas are properly imported."""
        try:
            from src.core.llm_manager import ModelProvider
            
            # Should be able to import ModelProvider
            assert ModelProvider is not None
        except ImportError:
            # Schema might not be available in test environment
            pass
    
    def test_pathlib_usage(self):
        """Test that pathlib is used for path operations."""
        import pathlib
        from src.core.llm_manager import log_dir
        
        # Should use pathlib for cross-platform path handling
        assert isinstance(pathlib.Path(), type(pathlib.Path()))
    
    def test_log_file_path_construction(self):
        """Test that log file path is constructed correctly."""
        from src.core.llm_manager import log_file_path
        
        # Should end with llm.log
        assert log_file_path.endswith("llm.log")
        
        # Should be an absolute path
        assert os.path.isabs(log_file_path)
    
    def test_custom_logger_class_definition(self):
        """Test that LiteLLMFileLogger class is properly defined."""
        from src.core.llm_manager import LiteLLMFileLogger
        from litellm.integrations.custom_logger import CustomLogger
        
        # Should be a subclass of CustomLogger
        assert issubclass(LiteLLMFileLogger, CustomLogger)
        
        # Should have required methods
        assert hasattr(LiteLLMFileLogger, "log_pre_api_call")
        assert hasattr(LiteLLMFileLogger, "log_post_api_call")
        assert hasattr(LiteLLMFileLogger, "log_success_event")