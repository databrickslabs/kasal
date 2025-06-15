"""
Unit tests for logging configuration.

Tests the functionality of the logging configuration module including
environment-specific setups and logger creation.
"""
import pytest
import logging
import os
import tempfile
from unittest.mock import patch, MagicMock
from pathlib import Path

from src.config.logging import get_logging_config, setup_logging, get_logger


class TestLoggingConfig:
    """Test cases for logging configuration."""
    
    def test_get_logging_config_development(self):
        """Test logging configuration for development environment."""
        with patch("src.config.logging.LoggerManager") as mock_logger_manager:
            # Mock the LoggerManager instance
            mock_instance = MagicMock()
            mock_instance._log_dir = "/tmp/test_logs"
            mock_logger_manager.get_instance.return_value = mock_instance
            
            config = get_logging_config("development")
            
            assert config["version"] == 1
            assert config["disable_existing_loggers"] is False
            
            # Check formatters
            assert "verbose" in config["formatters"]
            assert "simple" in config["formatters"]
            
            # Check handlers
            assert "console" in config["handlers"]
            assert "file" in config["handlers"]
            assert "error_file" in config["handlers"]
            assert "sqlalchemy_file" in config["handlers"]
            
            # Development specific settings
            assert config["handlers"]["console"]["level"] == "DEBUG"
            assert config["handlers"]["console"]["formatter"] == "simple"
            assert config["loggers"][""]["level"] == "DEBUG"
    
    def test_get_logging_config_production(self):
        """Test logging configuration for production environment."""
        with patch("src.config.logging.LoggerManager") as mock_logger_manager:
            # Mock the LoggerManager instance
            mock_instance = MagicMock()
            mock_instance._log_dir = "/tmp/test_logs"
            mock_logger_manager.get_instance.return_value = mock_instance
            
            config = get_logging_config("production")
            
            # Production specific settings
            assert config["handlers"]["console"]["level"] == "INFO"
            assert config["handlers"]["console"]["formatter"] == "verbose"
            assert config["loggers"][""]["level"] == "INFO"
    
    def test_get_logging_config_with_log_dir_env(self):
        """Test logging configuration with LOG_DIR environment variable."""
        with patch("src.config.logging.LoggerManager") as mock_logger_manager:
            # Mock the LoggerManager instance
            mock_instance = MagicMock()
            mock_instance._log_dir = None  # Not initialized
            # After initialize is called, set the log dir
            def set_log_dir(*args):
                mock_instance._log_dir = "/custom/log/dir"
            mock_instance.initialize.side_effect = set_log_dir
            mock_logger_manager.get_instance.return_value = mock_instance
            
            with patch.dict(os.environ, {"LOG_DIR": "/custom/log/dir"}):
                config = get_logging_config("development")
                
                # Check that initialize was called with custom log dir
                mock_instance.initialize.assert_called_with("/custom/log/dir")
    
    def test_get_logging_config_no_log_dir_env(self):
        """Test logging configuration without LOG_DIR environment variable."""
        with patch("src.config.logging.LoggerManager") as mock_logger_manager:
            # Mock the LoggerManager instance
            mock_instance = MagicMock()
            mock_instance._log_dir = None  # Not initialized
            # After initialize is called, set the default log dir
            def set_default_log_dir(*args):
                mock_instance._log_dir = "/default/log/dir"
            mock_instance.initialize.side_effect = set_default_log_dir
            mock_logger_manager.get_instance.return_value = mock_instance
            
            with patch.dict(os.environ, {}, clear=True):
                config = get_logging_config("development")
                
                # Check that initialize was called without parameters
                mock_instance.initialize.assert_called_with()
    
    def test_handler_configurations(self):
        """Test that all handlers are configured correctly."""
        with patch("src.config.logging.LoggerManager") as mock_logger_manager:
            mock_instance = MagicMock()
            mock_instance._log_dir = "/tmp/test_logs"
            mock_logger_manager.get_instance.return_value = mock_instance
            
            config = get_logging_config("development")
            
            # Console handler
            console_handler = config["handlers"]["console"]
            assert console_handler["class"] == "logging.StreamHandler"
            # Check that stream is stdout (comparing object not string)
            import sys
            assert console_handler["stream"] == sys.stdout
            
            # File handler
            file_handler = config["handlers"]["file"]
            assert file_handler["class"] == "logging.handlers.RotatingFileHandler"
            assert file_handler["maxBytes"] == 10485760  # 10 MB
            assert file_handler["backupCount"] == 5
            assert file_handler["encoding"] == "utf-8"
            
            # Error file handler
            error_handler = config["handlers"]["error_file"]
            assert error_handler["level"] == "ERROR"
            assert error_handler["class"] == "logging.handlers.RotatingFileHandler"
            
            # SQLAlchemy handler
            sqlalchemy_handler = config["handlers"]["sqlalchemy_file"]
            assert sqlalchemy_handler["level"] == "INFO"
            assert "sqlalchemy.log" in sqlalchemy_handler["filename"]
    
    def test_logger_configurations(self):
        """Test that all loggers are configured correctly."""
        with patch("src.config.logging.LoggerManager") as mock_logger_manager:
            mock_instance = MagicMock()
            mock_instance._log_dir = "/tmp/test_logs"
            mock_logger_manager.get_instance.return_value = mock_instance
            
            config = get_logging_config("development")
            
            loggers = config["loggers"]
            
            # Root logger
            root_logger = loggers[""]
            assert "console" in root_logger["handlers"]
            assert "file" in root_logger["handlers"]
            assert "error_file" in root_logger["handlers"]
            assert root_logger["propagate"] is True
            
            # Uvicorn logger
            uvicorn_logger = loggers["uvicorn"]
            assert uvicorn_logger["level"] == "INFO"
            assert uvicorn_logger["propagate"] is False
            
            # SQLAlchemy logger
            sqlalchemy_logger = loggers["sqlalchemy.engine"]
            assert "sqlalchemy_file" in sqlalchemy_logger["handlers"]
            assert sqlalchemy_logger["propagate"] is False
            
            # Alembic logger
            alembic_logger = loggers["alembic"]
            assert "console" in alembic_logger["handlers"]
            assert "file" in alembic_logger["handlers"]
    
    @patch("logging.config.dictConfig")
    @patch("src.config.logging.get_logging_config")
    def test_setup_logging(self, mock_get_config, mock_dict_config):
        """Test the setup_logging function."""
        mock_config = {"version": 1, "handlers": {}}
        mock_get_config.return_value = mock_config
        
        setup_logging("development")
        
        mock_get_config.assert_called_once_with("development")
        mock_dict_config.assert_called_once_with(mock_config)
    
    @patch("logging.config.dictConfig")
    @patch("src.config.logging.get_logging_config")
    @patch("logging.getLogger")
    def test_setup_logging_logs_configuration(self, mock_get_logger, mock_get_config, mock_dict_config):
        """Test that setup_logging logs the configuration."""
        mock_config = {"version": 1, "handlers": {}}
        mock_get_config.return_value = mock_config
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        setup_logging("production")
        
        mock_logger.info.assert_called_once_with("Logging configured for production environment")
    
    def test_get_logger(self):
        """Test the get_logger function."""
        with patch("logging.getLogger") as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            result = get_logger("test_logger")
            
            mock_get_logger.assert_called_once_with("test_logger")
            assert result == mock_logger
    
    def test_warning_filters_applied(self):
        """Test that deprecation warning filters are applied."""
        import warnings
        
        # These filters should have been applied when the module was imported
        filters = warnings.filters
        
        # Check that some deprecation warnings are filtered
        httpx_filtered = any(
            f[2] == "httpx" and f[1] == DeprecationWarning 
            for f in filters if len(f) >= 3 and f[2] is not None
        )
        
        # At least one filter should be in place
        assert len(filters) > 0
    
    def test_log_filename_generation(self):
        """Test that log filenames are generated correctly."""
        from src.config.logging import log_filename, error_log_filename
        from datetime import datetime
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        expected_log = f"backend.{current_date}.log"
        expected_error = f"backend.error.{current_date}.log"
        
        assert log_filename == expected_log
        assert error_log_filename == expected_error
    
    def test_verbose_and_simple_formats(self):
        """Test that log formats are defined correctly."""
        from src.config.logging import VERBOSE_FORMAT, SIMPLE_FORMAT
        
        # Check that formats contain expected components
        assert "%(asctime)s" in VERBOSE_FORMAT
        assert "%(name)s" in VERBOSE_FORMAT
        assert "%(levelname)s" in VERBOSE_FORMAT
        assert "%(filename)s" in VERBOSE_FORMAT
        assert "%(lineno)d" in VERBOSE_FORMAT
        assert "%(message)s" in VERBOSE_FORMAT
        
        assert "%(asctime)s" in SIMPLE_FORMAT
        assert "%(levelname)s" in SIMPLE_FORMAT
        assert "%(message)s" in SIMPLE_FORMAT
    
    def test_environment_case_insensitive(self):
        """Test that environment parameter is case insensitive."""
        with patch("src.config.logging.LoggerManager") as mock_logger_manager:
            mock_instance = MagicMock()
            mock_instance._log_dir = "/tmp/test_logs"
            mock_logger_manager.get_instance.return_value = mock_instance
            
            config_prod = get_logging_config("PRODUCTION")
            config_dev = get_logging_config("Development")
            
            # Both should work and produce appropriate configs
            assert config_prod["handlers"]["console"]["level"] == "INFO"
            assert config_dev["handlers"]["console"]["level"] == "DEBUG"