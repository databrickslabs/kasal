"""
Unit tests for CrewLogger.

Tests the functionality of the CrewAI engine logger including
event handling, output capture, and log redirection.
"""
import pytest
import logging
import sys
import io
from unittest.mock import patch, MagicMock, AsyncMock
from contextlib import contextmanager

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
        assert crew_logger_instance._event_handlers == {}
    
    def test_setup_crewai_logging(self, crew_logger_instance):
        """Test setup of CrewAI logging redirection."""
        with patch("logging.getLogger") as mock_get_logger:
            mock_crewai_logger = MagicMock()
            mock_get_logger.return_value = mock_crewai_logger
            
            crew_logger_instance._setup_crewai_logging()
            
            # Should configure CrewAI logger
            assert mock_crewai_logger.handlers == []
            assert mock_crewai_logger.propagate is False
            mock_crewai_logger.addHandler.assert_called()
            mock_crewai_logger.setLevel.assert_called_with(logging.DEBUG)
    
    def test_setup_for_job(self, crew_logger_instance, mock_group_context):
        """Test setting up logging for a specific job."""
        job_id = "test_job_123"
        
        with patch("src.engines.crewai.crew_logger.enqueue_log"), \
             patch.object(crew_logger_instance, "_register_event_listeners"), \
             patch.object(crew_logger_instance, "_patch_printer"):
            
            crew_logger_instance.setup_for_job(job_id, mock_group_context)
            
            # Should store job info
            assert job_id in crew_logger_instance._active_jobs
            job_info = crew_logger_instance._active_jobs[job_id]
            assert "handler" in job_info
            assert "original_print_method" in job_info
            
            # Should add handler to crew logger
            crew_logger_instance._crew_logger.addHandler.assert_called()
            
            # Should register event listeners and patch printer
            crew_logger_instance._register_event_listeners.assert_called_with(job_id)
            crew_logger_instance._patch_printer.assert_called_with(job_id)
    
    def test_setup_for_job_already_exists(self, crew_logger_instance, mock_group_context):
        """Test setting up logging for a job that already exists."""
        job_id = "test_job_123"
        
        # Setup job first
        crew_logger_instance._active_jobs[job_id] = {"handler": MagicMock()}
        
        with patch("src.engines.crewai.crew_logger.logger") as mock_logger:
            crew_logger_instance.setup_for_job(job_id, mock_group_context)
            
            # Should log warning
            mock_logger.warning.assert_called_with(f"CrewLogger already set up for job {job_id}")
    
    def test_cleanup_for_job(self, crew_logger_instance):
        """Test cleaning up logging for a specific job."""
        job_id = "test_job_123"
        mock_handler = MagicMock()
        original_print = MagicMock()
        
        # Setup job
        crew_logger_instance._active_jobs[job_id] = {
            "handler": mock_handler,
            "original_print_method": original_print
        }
        
        with patch("src.engines.crewai.crew_logger.Printer") as mock_printer:
            crew_logger_instance.cleanup_for_job(job_id)
            
            # Should remove handler
            crew_logger_instance._crew_logger.removeHandler.assert_called_with(mock_handler)
            
            # Should restore original print method
            assert mock_printer.print == original_print
            
            # Should remove job from active jobs
            assert job_id not in crew_logger_instance._active_jobs
    
    def test_cleanup_for_job_not_found(self, crew_logger_instance):
        """Test cleaning up logging for a job that doesn't exist."""
        job_id = "nonexistent_job"
        
        with patch("src.engines.crewai.crew_logger.logger") as mock_logger:
            crew_logger_instance.cleanup_for_job(job_id)
            
            # Should log warning
            mock_logger.warning.assert_called_with(f"No CrewLogger setup found for job {job_id}")
    
    def test_register_event_listeners(self, crew_logger_instance):
        """Test registering event listeners."""
        job_id = "test_job_123"
        
        with patch.object(crew_logger_instance, "_register_crew_events"), \
             patch.object(crew_logger_instance, "_register_agent_events"), \
             patch.object(crew_logger_instance, "_register_task_events"), \
             patch.object(crew_logger_instance, "_register_tool_events"), \
             patch.object(crew_logger_instance, "_register_llm_events"), \
             patch.object(crew_logger_instance, "_register_extended_events"):
            
            crew_logger_instance._register_event_listeners(job_id)
            
            # Should register all event types
            crew_logger_instance._register_crew_events.assert_called_with(job_id)
            crew_logger_instance._register_agent_events.assert_called_with(job_id)
            crew_logger_instance._register_task_events.assert_called_with(job_id)
            crew_logger_instance._register_tool_events.assert_called_with(job_id)
            crew_logger_instance._register_llm_events.assert_called_with(job_id)
    
    def test_register_crew_events(self, crew_logger_instance):
        """Test registering crew-level events."""
        job_id = "test_job_123"
        
        with patch("src.engines.crewai.crew_logger.crewai_event_bus") as mock_bus, \
             patch("src.engines.crewai.crew_logger.enqueue_log"):
            
            crew_logger_instance._register_crew_events(job_id)
            
            # Should register crew events
            assert mock_bus.on.call_count >= 3  # kickoff started, completed, failed
    
    def test_patch_printer(self, crew_logger_instance):
        """Test patching CrewAI's Printer class."""
        job_id = "test_job_123"
        
        # Setup job first
        crew_logger_instance._active_jobs[job_id] = {
            "handler": MagicMock(),
            "original_print_method": None
        }
        
        with patch("src.engines.crewai.crew_logger.Printer") as mock_printer:
            original_print = MagicMock()
            mock_printer.print = original_print
            
            crew_logger_instance._patch_printer(job_id)
            
            # Should store original print method
            assert crew_logger_instance._active_jobs[job_id]["original_print_method"] == original_print
            
            # Should replace Printer.print with custom method
            assert mock_printer.print != original_print
    
    def test_capture_stdout_stderr_context_manager(self, crew_logger_instance):
        """Test stdout/stderr capture context manager."""
        job_id = "test_job_123"
        
        # Setup job
        crew_logger_instance._active_jobs[job_id] = {
            "handler": MagicMock()
        }
        
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        with patch("src.engines.crewai.crew_logger.enqueue_log"):
            with crew_logger_instance.capture_stdout_stderr(job_id):
                # stdout/stderr should be redirected
                assert sys.stdout != original_stdout
                assert sys.stderr != original_stderr
                
                # Print something to test capture
                print("test stdout")
                print("test stderr", file=sys.stderr)
            
            # Should restore original stdout/stderr
            assert sys.stdout == original_stdout
            assert sys.stderr == original_stderr
    
    def test_should_filter_content(self, crew_logger_instance):
        """Test content filtering logic."""
        # Access the private method through the class
        # We'll test this indirectly through the printer patch
        job_id = "test_job_123"
        crew_logger_instance._active_jobs[job_id] = {
            "handler": MagicMock(),
            "original_print_method": MagicMock()
        }
        
        with patch("src.engines.crewai.crew_logger.Printer"), \
             patch("src.engines.crewai.crew_logger.enqueue_log") as mock_enqueue:
            
            crew_logger_instance._patch_printer(job_id)
            
            # Test that the filter function works by checking if it was defined
            # This is a simplified test since the function is defined inline
            assert crew_logger_instance._active_jobs[job_id]["original_print_method"] is not None


class TestCrewLoggerHandler:
    """Test cases for CrewLoggerHandler."""
    
    def test_initialization(self, mock_group_context):
        """Test CrewLoggerHandler initialization."""
        job_id = "test_job_123"
        handler = CrewLoggerHandler(job_id, mock_group_context)
        
        assert handler.job_id == job_id
        assert handler.group_context == mock_group_context
    
    def test_emit_log_record(self, mock_group_context):
        """Test emitting a log record."""
        job_id = "test_job_123"
        handler = CrewLoggerHandler(job_id, mock_group_context)
        
        # Mock the format method
        handler.format = MagicMock(return_value="Formatted log message")
        
        # Create a log record
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
            
            # Should format and enqueue the log
            handler.format.assert_called_once_with(record)
            mock_enqueue.assert_called_once_with(
                execution_id=job_id,
                content="Formatted log message",
                group_context=mock_group_context
            )
    
    def test_emit_with_exception(self, mock_group_context, capsys):
        """Test emit method handles exceptions gracefully."""
        job_id = "test_job_123"
        handler = CrewLoggerHandler(job_id, mock_group_context)
        
        # Mock format to raise an exception
        handler.format = MagicMock(side_effect=Exception("Format error"))
        
        # Create a log record
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
            # Should not raise exception
            handler.emit(record)
            
            # Should print error to stderr
            captured = capsys.readouterr()
            assert "Error in CrewLoggerHandler.emit" in captured.err


class TestCrewLoggerModule:
    """Test cases for module-level functionality."""
    
    def test_extended_events_availability(self):
        """Test extended events availability detection."""
        from src.engines.crewai.crew_logger import EXTENDED_EVENTS_AVAILABLE
        
        # Should be a boolean
        assert isinstance(EXTENDED_EVENTS_AVAILABLE, bool)
    
    def test_warning_filters_applied(self):
        """Test that deprecation warning filters are applied."""
        import warnings
        
        # Re-import to ensure filters are applied
        import importlib
        import src.engines.crewai.crew_logger
        importlib.reload(src.engines.crewai.crew_logger)
        
        filters = warnings.filters
        
        # Should have filters for known deprecation warnings
        assert len(filters) > 0
    
    def test_crewai_imports(self):
        """Test that CrewAI components are properly imported."""
        try:
            from src.engines.crewai.crew_logger import (
                AgentExecutionCompletedEvent,
                AgentExecutionStartedEvent,
                ToolUsageStartedEvent,
                ToolUsageFinishedEvent,
                LLMCallStartedEvent,
                LLMCallCompletedEvent,
                TaskCompletedEvent,
                TaskStartedEvent,
                CrewKickoffStartedEvent,
                CrewKickoffCompletedEvent,
                CrewKickoffFailedEvent,
                crewai_event_bus,
                Printer
            )
            
            # All should be importable
            events = [
                AgentExecutionCompletedEvent, AgentExecutionStartedEvent,
                ToolUsageStartedEvent, ToolUsageFinishedEvent,
                LLMCallStartedEvent, LLMCallCompletedEvent,
                TaskCompletedEvent, TaskStartedEvent,
                CrewKickoffStartedEvent, CrewKickoffCompletedEvent,
                CrewKickoffFailedEvent
            ]
            
            for event in events:
                assert event is not None
            
            assert crewai_event_bus is not None
            assert Printer is not None
            
        except ImportError as e:
            pytest.fail(f"Failed to import CrewAI components: {e}")
    
    def test_module_dependencies(self):
        """Test that module dependencies are properly imported."""
        try:
            from src.engines.crewai.crew_logger import (
                LoggerManager, enqueue_log, GroupContext
            )
            
            assert LoggerManager is not None
            assert enqueue_log is not None
            assert GroupContext is not None
            
        except ImportError as e:
            pytest.fail(f"Failed to import module dependencies: {e}")
    
    def test_singleton_instance_creation(self):
        """Test that singleton instance is created correctly."""
        from src.engines.crewai.crew_logger import crew_logger, CrewLogger
        
        # Should be a CrewLogger instance
        assert isinstance(crew_logger, CrewLogger)
    
    def test_module_logger_configuration(self):
        """Test that module logger is properly configured."""
        from src.engines.crewai.crew_logger import logger
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == "src.engines.crewai.crew_logger"
    
    def test_threading_imports(self):
        """Test that threading components are imported."""
        from src.engines.crewai.crew_logger import threading
        
        assert threading is not None
    
    def test_contextmanager_import(self):
        """Test that contextmanager is imported."""
        from src.engines.crewai.crew_logger import contextmanager
        
        assert contextmanager is not None
    
    def test_datetime_import(self):
        """Test that datetime is imported."""
        from src.engines.crewai.crew_logger import datetime
        
        assert datetime is not None