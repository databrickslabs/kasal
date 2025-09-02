"""
Unit tests for CrewLogger.

Tests the functionality of the CrewAI engine logger including
event handling, output capture, and log redirection.
"""
import pytest
import logging
import sys
import io
import threading
import warnings
from unittest.mock import patch, MagicMock, AsyncMock, call, PropertyMock
from contextlib import contextmanager
from datetime import datetime

from src.engines.crewai.crew_logger import CrewLogger, CrewLoggerHandler


@pytest.fixture
def mock_logger_manager():
    """Create a mock LoggerManager instance."""
    manager = MagicMock()
    manager.crew = MagicMock()
    return manager


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.group_id = "group_123"
    context.group_name = "Test Group"
    return context


@pytest.fixture
def reset_singleton():
    """Reset the CrewLogger singleton before each test."""
    CrewLogger._instance = None
    yield
    CrewLogger._instance = None


@pytest.fixture
def crew_logger_instance(mock_logger_manager):
    """Create a CrewLogger instance with mocked dependencies."""
    with patch("src.engines.crewai.crew_logger.LoggerManager") as mock_lm:
        mock_lm.get_instance.return_value = mock_logger_manager
        
        # Reset singleton
        CrewLogger._instance = None
        
        return CrewLogger()


class TestCrewLogger:
    """Test cases for CrewLogger."""
    
    def test_singleton_pattern(self, mock_logger_manager):
        """Test that CrewLogger follows singleton pattern."""
        with patch("src.engines.crewai.crew_logger.LoggerManager") as mock_lm:
            mock_lm.get_instance.return_value = mock_logger_manager
            
            # Reset singleton
            CrewLogger._instance = None
            
            logger1 = CrewLogger()
            logger2 = CrewLogger()
            
            assert logger1 is logger2
    
    def test_initialization(self, crew_logger_instance, mock_logger_manager):
        """Test CrewLogger initialization."""
        assert crew_logger_instance._crew_logger == mock_logger_manager.crew
        assert crew_logger_instance._initialized is True
        assert crew_logger_instance._active_jobs == {}
    
    def test_setup_crewai_logging(self, crew_logger_instance):
        """Test setup of CrewAI logging redirection."""
        with patch("logging.getLogger") as mock_get_logger:
            mock_crewai_logger = MagicMock()
            mock_related_logger = MagicMock()
            
            def get_logger_side_effect(name):
                if name == 'crewai':
                    return mock_crewai_logger
                elif name in ['langchain', 'httpx', 'openai']:
                    return mock_related_logger
                return MagicMock()
            
            mock_get_logger.side_effect = get_logger_side_effect
            
            crew_logger_instance._setup_crewai_logging()
            
            # Should configure CrewAI logger
            assert mock_crewai_logger.handlers == []
            assert mock_crewai_logger.propagate is False
            mock_crewai_logger.addHandler.assert_called()

    def test_module_coverage_verification(self):
        """Verify module constants and coverage."""
        # Simple test to verify module is loaded and accessible
        from src.engines.crewai.crew_logger import logger
        
        # Test the logger exists
        assert logger is not None

    def test_setup_for_job(self, crew_logger_instance, mock_group_context):
        """Test setup for job functionality."""
        job_id = "test_job_123"
        
        with patch("src.engines.crewai.crew_logger.CrewLoggerHandler") as mock_handler_class:
            mock_handler = MagicMock()
            mock_handler_class.return_value = mock_handler
            
            crew_logger_instance.setup_for_job(job_id, mock_group_context)
            
            # Verify handler was created and added
            mock_handler_class.assert_called_once_with(job_id=job_id, group_context=mock_group_context)
            crew_logger_instance._crew_logger.addHandler.assert_called_once_with(mock_handler)
            assert job_id in crew_logger_instance._active_jobs

    def test_cleanup_for_job(self, crew_logger_instance, mock_group_context):
        """Test cleanup for job functionality."""
        job_id = "test_job_123"
        
        # Setup a job first
        with patch("src.engines.crewai.crew_logger.CrewLoggerHandler") as mock_handler_class:
            mock_handler = MagicMock()
            mock_handler_class.return_value = mock_handler
            
            crew_logger_instance.setup_for_job(job_id, mock_group_context)
            
            # Now test cleanup
            crew_logger_instance.cleanup_for_job(job_id)
            
            # Verify handler was removed
            crew_logger_instance._crew_logger.removeHandler.assert_called_with(mock_handler)
            assert job_id not in crew_logger_instance._active_jobs


class TestCrewLoggerHandler:
    """Test cases for CrewLoggerHandler."""
    
    def test_initialization(self, mock_group_context):
        """Test CrewLoggerHandler initialization."""
        job_id = "test_job_123"
        handler = CrewLoggerHandler(job_id, mock_group_context)
        
        assert handler.job_id == job_id
        assert handler.group_context == mock_group_context
    
    def test_emit_log_record(self, mock_group_context):
        """Test CrewLoggerHandler emit method."""
        job_id = "test_job_123"
        handler = CrewLoggerHandler(job_id, mock_group_context)
        
        # Mock the format method
        handler.format = MagicMock(return_value="Formatted log message")
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=100,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        with patch("src.engines.crewai.crew_logger.enqueue_log") as mock_enqueue:
            handler.emit(record)
            mock_enqueue.assert_called_once_with(
                execution_id=job_id,
                content="Formatted log message",
                group_context=mock_group_context
            )
    
    def test_emit_with_exception(self, mock_group_context):
        """Test CrewLoggerHandler emit method with exception handling."""
        job_id = "test_job_123"
        handler = CrewLoggerHandler(job_id, mock_group_context)
        
        handler.format = MagicMock(side_effect=Exception("Format error"))
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=100,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        with patch("src.engines.crewai.crew_logger.enqueue_log"):
            # Should not raise exception even with format error
            handler.emit(record)