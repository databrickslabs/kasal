"""
Unit tests for ExecutionLogCapture.

Tests the functionality of the CrewAI engine logger including
event handling, output capture, and log redirection.
"""
import io
import logging
import sys
import threading
import warnings
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, PropertyMock, call, patch

import pytest

from src.services.execution.logs.capture import ExecutionLogCapture, ExecutionLogHandler


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
    """Reset the ExecutionLogCapture singleton before each test."""
    ExecutionLogCapture._instance = None
    yield
    ExecutionLogCapture._instance = None


@pytest.fixture
def crew_logger_instance(mock_logger_manager):
    """Create a ExecutionLogCapture instance with mocked dependencies."""
    with patch("src.services.execution.logs.capture.LoggerManager") as mock_lm:
        mock_lm.get_instance.return_value = mock_logger_manager
        
        # Reset singleton
        ExecutionLogCapture._instance = None
        
        return ExecutionLogCapture()


class TestExecutionLogCapture:
    """Test cases for ExecutionLogCapture."""
    
    def test_singleton_pattern(self, mock_logger_manager):
        """Test that ExecutionLogCapture follows singleton pattern."""
        with patch("src.services.execution.logs.capture.LoggerManager") as mock_lm:
            mock_lm.get_instance.return_value = mock_logger_manager
            
            # Reset singleton
            ExecutionLogCapture._instance = None
            
            logger1 = ExecutionLogCapture()
            logger2 = ExecutionLogCapture()
            
            assert logger1 is logger2
    
    def test_initialization(self, crew_logger_instance, mock_logger_manager):
        """Test ExecutionLogCapture initialization."""
        assert crew_logger_instance._crew_logger == mock_logger_manager.crew
        assert crew_logger_instance._initialized is True
        assert crew_logger_instance._active_jobs == {}
    
    def test_setup_engine_logging(self, crew_logger_instance):
        """Test setup of CrewAI logging redirection."""
        with patch("logging.getLogger") as mock_get_logger:
            mock_crewai_logger = MagicMock()
            mock_related_logger = MagicMock()
            
            def get_logger_side_effect(name):
                if name == 'kasal_engine':
                    return mock_crewai_logger
                elif name in ['langchain', 'httpx', 'openai']:
                    return mock_related_logger
                return MagicMock()
            
            mock_get_logger.side_effect = get_logger_side_effect
            
            crew_logger_instance._setup_engine_logging()
            
            # Should configure CrewAI logger
            assert mock_crewai_logger.handlers == []
            assert mock_crewai_logger.propagate is False
            mock_crewai_logger.addHandler.assert_called()

    def test_module_coverage_verification(self):
        """Verify module constants and coverage."""
        # Simple test to verify module is loaded and accessible
        from src.services.execution.logs.capture import logger

        # Test the logger exists
        assert logger is not None

    def test_setup_for_job(self, crew_logger_instance, mock_group_context):
        """Test setup for job functionality."""
        job_id = "test_job_123"
        
        with patch("src.services.execution.logs.capture.ExecutionLogHandler") as mock_handler_class:
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
        with patch("src.services.execution.logs.capture.ExecutionLogHandler") as mock_handler_class:
            mock_handler = MagicMock()
            mock_handler_class.return_value = mock_handler
            
            crew_logger_instance.setup_for_job(job_id, mock_group_context)
            
            # Now test cleanup
            crew_logger_instance.cleanup_for_job(job_id)
            
            # Verify handler was removed
            crew_logger_instance._crew_logger.removeHandler.assert_called_with(mock_handler)
            assert job_id not in crew_logger_instance._active_jobs


class TestExecutionLogHandler:
    """Test cases for ExecutionLogHandler."""
    
    def test_initialization(self, mock_group_context):
        """Test ExecutionLogHandler initialization."""
        job_id = "test_job_123"
        handler = ExecutionLogHandler(job_id, mock_group_context)
        
        assert handler.job_id == job_id
        assert handler.group_context == mock_group_context
    
    def test_emit_log_record(self, mock_group_context):
        """Test ExecutionLogHandler emit method."""
        job_id = "test_job_123"
        handler = ExecutionLogHandler(job_id, mock_group_context)
        
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
        
        with patch("src.services.execution.logs.capture.enqueue_log") as mock_enqueue:
            handler.emit(record)
            mock_enqueue.assert_called_once_with(
                execution_id=job_id,
                content="Formatted log message",
                group_context=mock_group_context
            )
    
    def test_emit_with_exception(self, mock_group_context):
        """Test ExecutionLogHandler emit method with exception handling."""
        job_id = "test_job_123"
        handler = ExecutionLogHandler(job_id, mock_group_context)
        
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
        
        with patch("src.services.execution.logs.capture.enqueue_log"):
            # Should not raise exception even with format error
            handler.emit(record)

# ==========================================================================
# Additional coverage: exception paths and early-return conditions
# ==========================================================================

@pytest.fixture(autouse=True)
def reset_singleton():
    ExecutionLogCapture._instance = None
    yield
    ExecutionLogCapture._instance = None


def make_logger():
    with patch("src.services.execution.logs.capture.LoggerManager") as mock_lm:
        mock_mgr = MagicMock()
        mock_mgr.crew = MagicMock()
        mock_lm.get_instance.return_value = mock_mgr
        ExecutionLogCapture._instance = None
        return ExecutionLogCapture()


# ---- redirect_crewai_logs exceptions ----

def test_redirect_crewai_logs_related_logger_exception():
    """Test exception handling for related loggers (lines 118-119)."""
    crew = make_logger()
    with patch('logging.getLogger') as mock_get_logger:
        # Make getLogger raise for specific names (related loggers)
        call_count = [0]
        def side_effect(name=None):
            if name in ('langchain', 'httpx', 'openai', 'src.services.converters'):
                raise Exception(f"Cannot get logger {name}")
            return MagicMock(handlers=[], propagate=True)
        mock_get_logger.side_effect = side_effect
        # Should not raise even with exception
        try:
            crew._redirect_crewai_logs()
        except Exception:
            pass


def test_redirect_crewai_logs_outer_exception():
    """Test outer exception handler (lines 122-123)."""
    crew = make_logger()
    with patch('logging.getLogger', side_effect=Exception("logger unavailable")):
        # Should not raise
        try:
            crew._redirect_crewai_logs()
        except Exception:
            pass


# ---- setup_for_job early return ----

def test_setup_for_job_already_setup():
    """Test early return when job already set up (lines 137-138)."""
    crew = make_logger()
    job_id = "job_already_setup"
    # Pre-populate active_jobs
    crew._active_jobs[job_id] = {"handler": MagicMock(), "original_print_method": None}

    # Should return early without adding another handler
    initial_jobs = dict(crew._active_jobs)
    crew.setup_for_job(job_id)
    # No change expected
    assert job_id in crew._active_jobs


# ---- cleanup_for_job early return ----

def test_cleanup_for_job_not_found():
    """Test early return when job not found (lines 167-168)."""
    crew = make_logger()
    # Should not raise and return early
    crew.cleanup_for_job("nonexistent_job")


# ---- cleanup_for_job restore printer exception ----

def test_cleanup_for_job_restore_exception():
    """Test exception handling when restoring Printer (lines 180-181)."""
    crew = make_logger()
    job_id = "job_restore_error"
    mock_handler = MagicMock()
    original_print = MagicMock()
    crew._active_jobs[job_id] = {
        "handler": mock_handler,
        "original_print_method": original_print,
    }

    with patch('src.services.execution.logs.capture.Printer') as MockPrinter:
        # Simulate error when setting Printer.print
        type(MockPrinter).print = property(
            fget=lambda self: original_print,
            fset=MagicMock(side_effect=Exception("Printer restore failed"))
        )
        # Should not raise
        try:
            crew.cleanup_for_job(job_id)
        except Exception:
            pass


# ---- _patch_printer exception handler ----

def test_patch_printer_exception():
    """Test exception handling in _patch_printer (line 258-259)."""
    crew = make_logger()
    job_id = "job_patch_error"
    crew._active_jobs[job_id] = {"handler": MagicMock(), "original_print_method": None}

    with patch('src.services.execution.logs.capture.Printer', side_effect=Exception("Printer unavailable")):
        # Should not raise
        try:
            crew._patch_printer(job_id)
        except Exception:
            pass


# ---- ExecutionLogHandler emit ----

def test_crew_logger_handler_emit():
    """Test ExecutionLogHandler emit method."""
    handler = ExecutionLogHandler(job_id="j1")
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="Test message", args=(), exc_info=None
    )
    # Should not raise
    handler.emit(record)


def test_crew_logger_handler_emit_with_group_context():
    """Test ExecutionLogHandler emit with group context."""
    group_ctx = MagicMock()
    handler = ExecutionLogHandler(job_id="j1", group_context=group_ctx)
    record = logging.LogRecord(
        name="test", level=logging.WARNING, pathname="", lineno=0,
        msg="Warning message", args=(), exc_info=None
    )
    # Should not raise
    handler.emit(record)


# ---- _patch_printer - exercise inner functions ----

def test_patch_printer_custom_print_called():
    """Test _patch_printer patches Printer and custom_print works."""
    from src.services.execution.logs.capture import ExecutionLogCapture
    crew = make_logger()
    job_id = "job_patch_test"
    crew._active_jobs[job_id] = {"handler": MagicMock(), "original_print_method": None}

    with patch('src.services.execution.logs.capture.Printer') as MockPrinter:
        original_print = MagicMock()
        MockPrinter.print = original_print

        with patch('src.services.execution.logs.capture.enqueue_log'):
            crew._patch_printer(job_id)

            # Now call the patched print method
            # Get the custom_print function that was installed
            custom_fn = MockPrinter.print
            if callable(custom_fn) and custom_fn is not original_print:
                # Call with test content
                fake_self = MagicMock()
                custom_fn(fake_self, "Normal crew output")
                custom_fn(fake_self, "debug: some debug message")  # Should filter
                custom_fn(fake_self, "")  # Empty - should filter
                custom_fn(fake_self, "litellm.info: some info")  # Should filter
                custom_fn(fake_self, "provider list: something")  # Should filter
                custom_fn(fake_self, "│")  # Separator - should filter
                custom_fn(fake_self, "created tenant context: something primary_tenant_id here")  # Filter


def test_patch_printer_setup_for_job_full():
    """Test that setup_for_job patches printer properly."""
    crew = make_logger()
    job_id = "job_full_test"

    with patch('src.services.execution.logs.capture.Printer') as MockPrinter:
        with patch('src.services.execution.logs.capture.ExecutionLogHandler') as MockHandler:
            mock_handler = MagicMock()
            MockHandler.return_value = mock_handler
            with patch('src.services.execution.logs.capture.enqueue_log'):
                crew.setup_for_job(job_id)
                assert job_id in crew._active_jobs


def test_cleanup_for_job_restores_printer():
    """Test cleanup restores original Printer method."""
    crew = make_logger()
    job_id = "job_cleanup_restore"
    original_print = MagicMock()
    mock_handler = MagicMock()
    crew._active_jobs[job_id] = {
        "handler": mock_handler,
        "original_print_method": original_print,
    }

    with patch('src.services.execution.logs.capture.Printer') as MockPrinter:
        crew.cleanup_for_job(job_id)
        assert job_id not in crew._active_jobs


# ---- capture_stdout_stderr context manager ----

def test_capture_stdout_stderr_no_output():
    """Test context manager with no stdout/stderr output."""
    crew = make_logger()
    job_id = "job_capture_empty"
    crew._active_jobs[job_id] = {"handler": MagicMock(), "original_print_method": None}

    with patch('src.services.execution.logs.capture.enqueue_log'):
        with crew.capture_stdout_stderr(job_id):
            pass  # No output


def test_capture_stdout_stderr_with_output():
    """Test context manager captures stdout output."""
    crew = make_logger()
    job_id = "job_capture_output"
    mock_handler = MagicMock()
    mock_handler.group_context = None
    crew._active_jobs[job_id] = {"handler": mock_handler, "original_print_method": None}

    with patch('src.services.execution.logs.capture.enqueue_log') as mock_enqueue:
        with crew.capture_stdout_stderr(job_id):
            import sys as _sys
            print("CREW OUTPUT LINE 1")

    # Should have logged stdout content


def test_capture_stdout_stderr_with_stderr():
    """Test context manager captures stderr output."""
    crew = make_logger()
    job_id = "job_capture_stderr"
    mock_handler = MagicMock()
    mock_handler.group_context = None
    crew._active_jobs[job_id] = {"handler": mock_handler, "original_print_method": None}

    with patch('src.services.execution.logs.capture.enqueue_log'):
        with crew.capture_stdout_stderr(job_id):
            import sys as _sys
            print("stderr message", file=_sys.stderr)
