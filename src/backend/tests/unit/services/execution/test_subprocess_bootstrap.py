"""
Unit tests for src/services/execution/subprocess_bootstrap.py
"""

import logging
import sys
import os
import io
import asyncio
import pytest
from unittest.mock import MagicMock, patch, call, AsyncMock, PropertyMock

from src.services.execution.subprocess_bootstrap import (
    configure_subprocess_logging,
    suppress_stdout_stderr,
    restore_stdout_stderr,
)
from src.services.execution.logs.context import _execution_context


class TestSuppressRestoreStdout:
    """Tests for suppress_stdout_stderr and restore_stdout_stderr."""

    def test_suppress_stdout_stderr(self):
        orig_stdout, orig_stderr = sys.stdout, sys.stderr
        original_stdout, original_stderr, captured = suppress_stdout_stderr()
        assert sys.stdout is captured
        assert sys.stderr is captured
        # Restore before assertions
        restore_stdout_stderr(original_stdout, original_stderr)
        assert sys.stdout is orig_stdout
        assert sys.stderr is orig_stderr

    def test_restore_stdout_stderr(self):
        orig_stdout = sys.stdout
        orig_stderr = sys.stderr
        fake_out = io.StringIO()
        fake_err = io.StringIO()
        sys.stdout = fake_out
        sys.stderr = fake_err
        restore_stdout_stderr(orig_stdout, orig_stderr)
        assert sys.stdout is orig_stdout
        assert sys.stderr is orig_stderr


# ---------------------------------------------------------------------------
# ExecutionLogsDatabaseHandler
# ---------------------------------------------------------------------------



class TestConfigureSubprocessLogging:
    """Tests for configure_subprocess_logging."""

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_crew_process_type(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        mock_exec_logger.handlers = []
        mock_logger_manager.crew = mock_exec_logger
        mock_logger_manager.flow = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        with patch("logging.FileHandler") as mock_fh:
            mock_fh.return_value = MagicMock()
            result = configure_subprocess_logging("exec-sub-crew", "crew")
        assert result == mock_exec_logger

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_flow_process_type(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        mock_exec_logger.handlers = []
        mock_logger_manager.flow = mock_exec_logger
        mock_logger_manager.crew = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        with patch("logging.FileHandler") as mock_fh:
            mock_fh.return_value = MagicMock()
            result = configure_subprocess_logging("exec-sub-flow", "flow")
        assert result == mock_exec_logger

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_debug_level_via_env(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        mock_exec_logger.handlers = []
        mock_logger_manager.crew = mock_exec_logger
        mock_logger_manager.flow = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        with patch.dict(os.environ, {"KASAL_LOG_CREW": "DEBUG"}):
            with patch("logging.FileHandler") as mock_fh:
                mock_fh.return_value = MagicMock()
                configure_subprocess_logging("exec-debug", "crew")

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_log_level_warning_via_env(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        mock_exec_logger.handlers = []
        mock_logger_manager.crew = mock_exec_logger
        mock_logger_manager.flow = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        with patch.dict(os.environ, {"KASAL_LOG_CREW": "WARNING"}):
            with patch("logging.FileHandler") as mock_fh:
                mock_fh.return_value = MagicMock()
                configure_subprocess_logging("exec-warn", "crew")

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_log_level_error_via_env(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        mock_exec_logger.handlers = []
        mock_logger_manager.crew = mock_exec_logger
        mock_logger_manager.flow = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        with patch.dict(os.environ, {"KASAL_LOG_CREW": "ERROR"}):
            with patch("logging.FileHandler") as mock_fh:
                mock_fh.return_value = MagicMock()
                configure_subprocess_logging("exec-error", "crew")

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_log_level_off_via_env(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        mock_exec_logger.handlers = []
        mock_logger_manager.crew = mock_exec_logger
        mock_logger_manager.flow = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        with patch.dict(os.environ, {"KASAL_LOG_CREW": "OFF"}):
            with patch("logging.FileHandler") as mock_fh:
                mock_fh.return_value = MagicMock()
                configure_subprocess_logging("exec-off", "crew")

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_debug_via_kasal_debug_traces(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        mock_exec_logger.handlers = []
        mock_logger_manager.crew = mock_exec_logger
        mock_logger_manager.flow = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        with patch.dict(os.environ, {"KASAL_DEBUG_TRACES": "true"}):
            with patch("logging.FileHandler") as mock_fh:
                mock_fh.return_value = MagicMock()
                configure_subprocess_logging("exec-debug-traces", "crew")

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_global_log_level_info(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        mock_exec_logger.handlers = []
        mock_logger_manager.crew = mock_exec_logger
        mock_logger_manager.flow = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        with patch.dict(os.environ, {"KASAL_LOG_LEVEL": "INFO"}, clear=False):
            os.environ.pop("KASAL_LOG_CREW", None)
            os.environ.pop("KASAL_DEBUG_TRACES", None)
            os.environ.pop("KASAL_DEBUG_ALL", None)
            with patch("logging.FileHandler") as mock_fh:
                mock_fh.return_value = MagicMock()
                configure_subprocess_logging("exec-global-info", "crew")

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_existing_file_handler_gets_formatter_update(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        existing_fh = MagicMock(spec=logging.FileHandler)
        mock_exec_logger.handlers = [existing_fh]
        mock_logger_manager.crew = mock_exec_logger
        mock_logger_manager.flow = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        configure_subprocess_logging("exec-existing-fh", "crew")
        existing_fh.setFormatter.assert_called_once()

    @patch("src.core.logger.LoggerManager")
    @patch("src.core.logger.get_logger")
    def test_log_dir_from_environment(self, mock_get_logger, mock_lm_class):
        mock_logger_manager = MagicMock()
        mock_lm_class.get_instance.return_value = mock_logger_manager
        mock_exec_logger = MagicMock()
        mock_exec_logger.handlers = []
        mock_logger_manager.crew = mock_exec_logger
        mock_logger_manager.flow = MagicMock()

        mock_module_logger = MagicMock()
        mock_module_logger.handlers = []
        mock_get_logger.return_value = mock_module_logger

        with patch.dict(os.environ, {"LOG_DIR": "/tmp/test_logs"}):
            with patch("logging.FileHandler") as mock_fh:
                mock_fh.return_value = MagicMock()
                configure_subprocess_logging("exec-env-logdir", "crew")
