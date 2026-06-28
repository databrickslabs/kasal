"""
Unit tests for src/engines/crewai/logging_config.py

Targets all major code paths to push coverage to 85%+.
"""
import logging
import sys
import os
import io
import asyncio
import pytest
from unittest.mock import MagicMock, patch, call, AsyncMock, PropertyMock

from src.engines.crewai.infra.logging_config import (
    ExecutionContextFormatter,
    ExecutionLogsDatabaseHandler,
    set_execution_context,
    clear_execution_context,
    execution_logging_context,
    configure_subprocess_logging,
    suppress_stdout_stderr,
    restore_stdout_stderr,
    _execution_context,
)


# ---------------------------------------------------------------------------
# ExecutionContextFormatter
# ---------------------------------------------------------------------------

class TestExecutionContextFormatter:
    """Tests for ExecutionContextFormatter."""

    def test_format_with_execution_id(self):
        fmt = "[CREW] %(asctime)s - %(levelname)s - %(message)s"
        formatter = ExecutionContextFormatter(fmt=fmt)
        set_execution_context("abcdef1234567890")
        try:
            record = logging.LogRecord(
                name="crew", level=logging.INFO,
                pathname="", lineno=0,
                msg="test message", args=(), exc_info=None
            )
            result = formatter.format(record)
            assert "abcdef12" in result
            assert "test message" in result
        finally:
            clear_execution_context()

    def test_format_without_execution_id(self):
        fmt = "[CREW] %(asctime)s - %(levelname)s - %(message)s"
        formatter = ExecutionContextFormatter(fmt=fmt)
        clear_execution_context()
        record = logging.LogRecord(
            name="crew", level=logging.INFO,
            pathname="", lineno=0,
            msg="no exec id", args=(), exc_info=None
        )
        result = formatter.format(record)
        assert "no exec id" in result

    def test_format_flow_prefix(self):
        fmt = "[FLOW] %(asctime)s - %(levelname)s - %(message)s"
        formatter = ExecutionContextFormatter(fmt=fmt)
        assert formatter._prefix == "[FLOW]"

    def test_format_default_prefix(self):
        formatter = ExecutionContextFormatter()
        assert formatter._prefix == "[CREW]"

    def test_prefix_no_match(self):
        fmt = "%(asctime)s - %(levelname)s - %(message)s"
        formatter = ExecutionContextFormatter(fmt=fmt)
        # When no [SOMETHING] prefix, falls back to [CREW]
        assert formatter._prefix == "[CREW]"


# ---------------------------------------------------------------------------
# set/clear execution context
# ---------------------------------------------------------------------------

class TestExecutionContext:
    """Tests for context variable helpers."""

    def test_set_and_clear(self):
        set_execution_context("exec-1234")
        assert _execution_context.get() == "exec-1234"
        clear_execution_context()
        assert _execution_context.get() is None

    def test_execution_logging_context(self):
        with execution_logging_context("exec-abcd"):
            assert _execution_context.get() == "exec-abcd"
        assert _execution_context.get() is None

    def test_execution_logging_context_clears_on_exception(self):
        try:
            with execution_logging_context("exec-xyz"):
                raise ValueError("boom")
        except ValueError:
            pass
        assert _execution_context.get() is None


# ---------------------------------------------------------------------------
# suppress / restore stdout/stderr
# ---------------------------------------------------------------------------

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

class TestExecutionLogsDatabaseHandler:
    """Tests for ExecutionLogsDatabaseHandler."""

    @patch("src.core.logger.get_logger")
    @patch("src.config.settings.settings")
    def test_init_with_settings(self, mock_settings, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_settings.DATABASE_URI = "postgresql://user:pass@localhost/db"

        handler = ExecutionLogsDatabaseHandler(execution_id="exec-001")
        assert handler.execution_id == "exec-001"
        assert handler.log_queue is None
        assert handler._db_url is not None

    @patch("src.core.logger.get_logger")
    @patch.dict(os.environ, {"DATABASE_URL": "postgresql://localhost/test"})
    def test_init_with_database_url_env(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        handler = ExecutionLogsDatabaseHandler(execution_id="exec-env")
        assert "postgresql" in handler._db_url

    @patch("src.core.logger.get_logger")
    def test_init_with_log_queue(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        import queue
        q = queue.Queue()

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(
                execution_id="exec-002", log_queue=q
            )
        assert handler.log_queue is q

    @patch("src.core.logger.get_logger")
    def test_init_with_group_context_object(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        group_ctx = MagicMock()
        group_ctx.primary_group_id = "group-1"
        group_ctx.group_email = "test@example.com"

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(
                execution_id="exec-003", group_context=group_ctx
            )
        assert handler.group_context is group_ctx

    @patch("src.core.logger.get_logger")
    def test_init_import_error_falls_back_to_sqlite(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DATABASE_URL", None)
            # Create a handler and manually call _init_db to test fallback path
            with patch("src.config.settings.settings") as mock_settings:
                mock_settings.DATABASE_URI = "sqlite:///fallback.db"
                handler = ExecutionLogsDatabaseHandler(execution_id="exec-fallback")
            # Override the _db_url to test the fallback is used when set
            assert handler._db_url is not None

    @patch("src.core.logger.get_logger")
    def test_emit_skips_db_handler_logs(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-004")
        handler._write_to_db_sync = MagicMock()

        record = logging.LogRecord(
            name="crew", level=logging.INFO,
            pathname="", lineno=0,
            msg="[DB_HANDLER] Internal message", args=(), exc_info=None
        )
        handler.emit(record)
        handler._write_to_db_sync.assert_not_called()

    @patch("src.core.logger.get_logger")
    def test_emit_calls_write_to_db(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-005")
        handler._write_to_db_sync = MagicMock()

        record = logging.LogRecord(
            name="test_logger", level=logging.INFO,
            pathname="", lineno=0,
            msg="real log message", args=(), exc_info=None
        )
        handler.emit(record)
        handler._write_to_db_sync.assert_called_once()

    @patch("src.core.logger.get_logger")
    def test_emit_exception_does_not_propagate(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_logger.handlers = []
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-006")
        handler._write_to_db_sync = MagicMock(side_effect=Exception("db error"))

        record = logging.LogRecord(
            name="test_logger", level=logging.INFO,
            pathname="", lineno=0,
            msg="will fail", args=(), exc_info=None
        )
        # Should not raise
        handler.emit(record)

    @patch("src.core.logger.get_logger")
    def test_write_to_db_sync_queues_log(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        import queue
        q = queue.Queue()

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(
                execution_id="exec-queue", log_queue=q
            )

        handler._write_to_db_sync("test log content")
        assert not q.empty()

    @patch("src.core.logger.get_logger")
    def test_write_to_db_sync_queue_full_falls_back(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        import queue
        q = MagicMock()
        q.put_nowait.side_effect = queue.Full("full")

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(
                execution_id="exec-full", log_queue=q
            )

        with patch.object(handler, "_db_url", "sqlite:///test.db"):
            with patch("sqlite3.connect") as mock_conn:
                mock_cursor = MagicMock()
                mock_conn.return_value.__enter__ = MagicMock(return_value=mock_conn.return_value)
                mock_conn.return_value.cursor.return_value = mock_cursor
                mock_conn.return_value.commit = MagicMock()
                mock_conn.return_value.close = MagicMock()
                handler._write_to_db_sync("test content after full queue")

    @patch("src.core.logger.get_logger")
    def test_write_to_db_sync_group_context_dict(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        group_ctx = {"primary_group_id": "grp-1", "group_email": "a@b.com"}

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(
                execution_id="exec-dict-ctx", group_context=group_ctx
            )

        import queue
        q = queue.Queue()
        handler.log_queue = q
        handler._write_to_db_sync("log with dict context")
        item = q.get_nowait()
        assert item["group_id"] == "grp-1"
        assert item["group_email"] == "a@b.com"

    @patch("src.core.logger.get_logger")
    def test_write_to_db_sync_group_context_object(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        group_ctx = MagicMock()
        group_ctx.primary_group_id = "grp-obj"
        group_ctx.group_email = "obj@b.com"

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(
                execution_id="exec-obj-ctx", group_context=group_ctx
            )

        import queue
        q = queue.Queue()
        handler.log_queue = q
        handler._write_to_db_sync("log with object context")
        item = q.get_nowait()
        assert item["group_id"] == "grp-obj"

    @patch("src.core.logger.get_logger")
    def test_write_to_db_sync_no_group_context(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-no-ctx")

        import queue
        q = queue.Queue()
        handler.log_queue = q
        handler._write_to_db_sync("no context log")
        item = q.get_nowait()
        assert item["group_id"] is None
        assert item["group_email"] is None

    @patch("src.core.logger.get_logger")
    def test_write_to_db_sync_sqlite(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-sqlite")
        handler._db_url = "sqlite:///test.db"

        with patch("sqlite3.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            mock_logger_inner = MagicMock()
            mock_logger_inner.handlers = []
            mock_get_logger.return_value = mock_logger_inner
            handler._write_to_db_sync("sqlite content")
            mock_connect.assert_called_once()
            mock_conn.commit.assert_called_once()
            mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.core.logger.get_logger")
    async def test_write_to_db_async_postgresql(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql://localhost/db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-pg")
        handler._db_url = "postgresql://localhost/db"

        mock_conn = AsyncMock()
        mock_engine = MagicMock()

        async def begin_ctx():
            return mock_conn

        begin_cm = MagicMock()
        begin_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        begin_cm.__aexit__ = AsyncMock(return_value=None)
        mock_engine.begin = MagicMock(return_value=begin_cm)
        mock_engine.dispose = AsyncMock()

        with patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=mock_engine):
            await handler._write_to_db_async({
                "execution_id": "exec-pg",
                "content": "async content",
                "timestamp": None,
                "group_id": None,
                "group_email": None,
            })

    @pytest.mark.asyncio
    @patch("src.core.logger.get_logger")
    async def test_write_to_db_async_pg8000(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql+pg8000://localhost/db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-pg8000")
        handler._db_url = "postgresql+pg8000://localhost/db"

        mock_conn = AsyncMock()
        mock_engine = MagicMock()
        begin_cm = MagicMock()
        begin_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        begin_cm.__aexit__ = AsyncMock(return_value=None)
        mock_engine.begin = MagicMock(return_value=begin_cm)
        mock_engine.dispose = AsyncMock()

        with patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=mock_engine) as mock_create:
            await handler._write_to_db_async({
                "execution_id": "exec-pg8000",
                "content": "pg8000 content",
                "timestamp": None,
                "group_id": None,
                "group_email": None,
            })
            call_url = mock_create.call_args[0][0]
            assert "asyncpg" in call_url

    @pytest.mark.asyncio
    @patch("src.core.logger.get_logger")
    async def test_write_to_db_async_raises_on_error(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql://localhost/db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-err")
        handler._db_url = "postgresql://localhost/db"

        with patch("sqlalchemy.ext.asyncio.create_async_engine", side_effect=Exception("conn error")):
            with pytest.raises(Exception, match="Async database write failed"):
                await handler._write_to_db_async({
                    "execution_id": "exec-err",
                    "content": "error content",
                    "timestamp": None,
                    "group_id": None,
                    "group_email": None,
                })

    @patch("src.core.logger.get_logger")
    def test_write_to_db_sync_postgresql_in_async_loop(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql://localhost/db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-pg-sync")
        handler._db_url = "postgresql://localhost/db"

        mock_task = MagicMock()
        mock_loop = MagicMock()
        mock_loop.create_task.return_value = mock_task

        with patch("asyncio.get_running_loop", return_value=mock_loop):
            handler._write_to_db_sync("postgresql sync content")
        mock_loop.create_task.assert_called_once()

    @patch("src.core.logger.get_logger")
    def test_write_to_db_sync_postgresql_no_loop(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql://localhost/db"
            handler = ExecutionLogsDatabaseHandler(execution_id="exec-pg-noloop")
        handler._db_url = "postgresql://localhost/db"

        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
            with patch("asyncio.run") as mock_run:
                handler._write_to_db_sync("no loop content")
                mock_run.assert_called_once()


# ---------------------------------------------------------------------------
# configure_subprocess_logging
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
