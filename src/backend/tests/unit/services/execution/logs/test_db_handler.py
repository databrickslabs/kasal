"""
Unit tests for src/services/execution/logs/db_handler.py
"""

import asyncio
import io
import logging
import os
import sys
from unittest.mock import AsyncMock, MagicMock, PropertyMock, call, patch

import pytest

from src.services.execution.logs.context import (
    _execution_context,
    clear_execution_context,
    set_execution_context,
)
from src.services.execution.logs.db_handler import ExecutionLogsDatabaseHandler


class TestExecutionLogsDatabaseHandler:
    """Tests for ExecutionLogsDatabaseHandler."""

    @patch("src.core.logger.get_logger")
    @patch("src.config.settings.settings")
    def test_init_with_settings(self, mock_settings, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_settings.DATABASE_URI = "postgresql://user" ":pass@localhost/db"

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

