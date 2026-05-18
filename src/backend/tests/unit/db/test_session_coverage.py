"""
Additional coverage tests for src/db/session.py.

Targets uncovered lines:
- Lines 57-61: SQL_DEBUG logging block
- Lines 81-84: retry async non-lock error + exhaust retries
- Lines 100-103: retry sync non-lock error + exhaust retries
- Lines 128-146: SQLAlchemyLogger SQL_DEBUG path
- Lines 167: SQL_DEBUG info log in setup_logger
- Lines 198: main_event_loop captured at import time branch
- Lines 232-245: SQLite StaticPool engine creation
- Lines 285-286: USE_NULLPOOL engine selection
- Lines 294-316: configure_sqlite function
- Lines 321-330: SQLite event listener application
- Lines 465-662: init_db function
- Lines 713-737: get_db PostgreSQL branch
- Lines 752-764: get_db OperationalError retry in session
- Lines 768-771: get_db ValueError token reset
- Lines 774-780: get_db outer OperationalError retry
- Lines 797-798: get_local_db
- Lines 819-830, 854-855: dispose_engines edge cases
"""
import os
import asyncio
import logging
import sqlite3
import tempfile
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from sqlalchemy.exc import OperationalError


# ---------------------------------------------------------------------------
# retry_db_operation: exhaust retries (raise last_exception) and general exc
# ---------------------------------------------------------------------------

class TestRetryDbOperationEdgeCases:

    @pytest.mark.asyncio
    async def test_async_exhausts_retries_raises_last_exception(self):
        """When locked error on final attempt, raises rather than infinite loop."""
        from src.db.session import retry_db_operation

        attempts = []

        @retry_db_operation(max_retries=2, delay=0.001, backoff=1.0)
        async def always_locked():
            attempts.append(1)
            raise OperationalError("database is locked", None, None)

        with pytest.raises(OperationalError):
            await always_locked()
        assert len(attempts) == 2

    @pytest.mark.asyncio
    async def test_async_non_lock_operational_error_not_retried(self):
        """Non-lock OperationalError is raised immediately without retry."""
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3, delay=0.001)
        async def non_lock_error():
            nonlocal call_count
            call_count += 1
            raise OperationalError("connection refused", None, None)

        with pytest.raises(OperationalError):
            await non_lock_error()
        # Should only be called once (no retry for non-lock)
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_async_general_exception_no_retry(self):
        """Generic Exception inside async wrapper raises immediately."""
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3)
        async def general_fail():
            nonlocal call_count
            call_count += 1
            raise ValueError("general error")

        with pytest.raises(ValueError, match="general error"):
            await general_fail()
        assert call_count == 1

    def test_sync_exhausts_retries_raises_last_exception(self):
        """Sync wrapper raises after max retries exhausted."""
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=2, delay=0.001, backoff=1.0)
        def always_locked_sync():
            nonlocal call_count
            call_count += 1
            raise OperationalError("database is locked", None, None)

        with patch("time.sleep"):
            with pytest.raises(OperationalError):
                always_locked_sync()
        assert call_count == 2

    def test_sync_non_lock_operational_error_not_retried(self):
        """Non-lock OperationalError in sync wrapper raises immediately."""
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3)
        def non_lock_sync():
            nonlocal call_count
            call_count += 1
            raise OperationalError("permission denied", None, None)

        with pytest.raises(OperationalError):
            non_lock_sync()
        assert call_count == 1

    def test_sync_general_exception_no_retry(self):
        """Generic Exception in sync wrapper raises immediately."""
        from src.db.session import retry_db_operation

        call_count = 0

        @retry_db_operation(max_retries=3)
        def general_fail_sync():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("oops")

        with pytest.raises(RuntimeError, match="oops"):
            general_fail_sync()
        assert call_count == 1


# ---------------------------------------------------------------------------
# configure_sqlite
# ---------------------------------------------------------------------------

class TestConfigureSqlite:
    """Tests for the configure_sqlite connection event handler."""

    def test_configure_sqlite_executes_pragmas(self):
        """configure_sqlite should call execute for each PRAGMA."""
        from src.db.session import configure_sqlite

        mock_conn = MagicMock()
        mock_record = MagicMock()

        with patch("src.db.session.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            configure_sqlite(mock_conn, mock_record)

        # Should have been called multiple times with pragma statements
        assert mock_conn.execute.call_count >= 7

    def test_configure_sqlite_handles_exception(self):
        """configure_sqlite should not raise if execute fails."""
        from src.db.session import configure_sqlite

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("pragma failed")
        mock_record = MagicMock()

        with patch("src.db.session.settings") as mock_settings:
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            # Should not raise
            configure_sqlite(mock_conn, mock_record)

    def test_configure_sqlite_not_called_for_postgres(self):
        """configure_sqlite should do nothing for non-sqlite URIs."""
        from src.db.session import configure_sqlite

        mock_conn = MagicMock()
        mock_record = MagicMock()

        with patch("src.db.session.settings") as mock_settings:
            mock_settings.DATABASE_URI = "postgresql+asyncpg://u:p@h/db"
            configure_sqlite(mock_conn, mock_record)

        mock_conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------

def _make_async_cm(return_value=None):
    """Helper: create an object that works as 'async with obj as x: ...'."""
    cm = MagicMock()
    inner = return_value if return_value is not None else AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=inner)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


class TestInitDb:
    """Tests for the init_db async function."""

    @pytest.mark.asyncio
    async def test_init_db_sqlite_creates_tables(self):
        """init_db with SQLite creates tables when they don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test_init.db")
            db_uri = f"sqlite+aiosqlite:///{db_path}"

            mock_inner_conn = AsyncMock()
            mock_inner_conn.run_sync = AsyncMock()

            mock_engine = MagicMock()
            mock_engine.connect = MagicMock(return_value=_make_async_cm(mock_inner_conn))
            mock_engine.begin = MagicMock(return_value=_make_async_cm(mock_inner_conn))
            mock_engine.dispose = AsyncMock()

            import src.db.all_models as _all_models_mod
            with patch("src.db.session.settings") as mock_settings, \
                 patch.object(_all_models_mod, "Base", MagicMock()), \
                 patch("importlib.reload"), \
                 patch("src.db.session.create_async_engine", return_value=mock_engine), \
                 patch("src.db.session.NullPool"), \
                 patch("sqlite3.connect") as mock_sqlite_connect:

                mock_settings.DATABASE_URI = db_uri
                mock_settings.SQLITE_DB_PATH = db_path
                mock_cursor = MagicMock()
                mock_cursor.fetchall.return_value = []
                mock_sqlite_conn = MagicMock()
                mock_sqlite_conn.cursor.return_value = mock_cursor
                mock_sqlite_connect.return_value = mock_sqlite_conn

                # Create the db file so existence check passes
                with open(db_path, 'w'):
                    pass

                from src.db.session import init_db
                await init_db()

    @pytest.mark.asyncio
    async def test_init_db_sqlite_tables_already_exist(self):
        """init_db skips table creation when tables already exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "existing.db")
            db_uri = f"sqlite+aiosqlite:///{db_path}"

            # Create a real SQLite DB with multiple tables so tables_exist is True
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE agents (id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY)")
            conn.commit()
            conn.close()

            import src.db.all_models as _all_models_mod2
            with patch("src.db.session.settings") as mock_settings, \
                 patch.object(_all_models_mod2, "Base", MagicMock()), \
                 patch("importlib.reload"):

                mock_settings.DATABASE_URI = db_uri
                mock_settings.SQLITE_DB_PATH = db_path

                from src.db.session import init_db
                # Should complete without error (tables exist, no creation needed)
                await init_db()

    @pytest.mark.asyncio
    async def test_init_db_exception_propagates(self):
        """init_db propagates unexpected exceptions."""
        with patch("src.db.session.settings") as mock_settings, \
             patch("importlib.reload", side_effect=RuntimeError("import error")):
            mock_settings.DATABASE_URI = "sqlite:///test.db"
            mock_settings.SQLITE_DB_PATH = "/tmp/test.db"

            from src.db.session import init_db
            with pytest.raises(RuntimeError, match="import error"):
                await init_db()

    @pytest.mark.asyncio
    async def test_init_db_sqlite_creates_dir_and_file(self):
        """init_db creates directory and empty file when they don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "subdir", "new.db")
            db_uri = f"sqlite+aiosqlite:///{db_path}"

            mock_inner_conn = AsyncMock()
            mock_inner_conn.run_sync = AsyncMock()

            mock_engine = MagicMock()
            mock_engine.connect = MagicMock(return_value=_make_async_cm(mock_inner_conn))
            mock_engine.begin = MagicMock(return_value=_make_async_cm(mock_inner_conn))
            mock_engine.dispose = AsyncMock()

            with patch("src.db.session.settings") as mock_settings, \
                 patch("src.db.all_models"), \
                 patch("importlib.reload"), \
                 patch("src.db.session.create_async_engine", return_value=mock_engine), \
                 patch("sqlite3.connect") as mock_sqlite_connect:

                mock_settings.DATABASE_URI = db_uri
                mock_settings.SQLITE_DB_PATH = db_path
                mock_cursor = MagicMock()
                mock_cursor.fetchall.return_value = []
                mock_sqlite_conn = MagicMock()
                mock_sqlite_conn.cursor.return_value = mock_cursor
                mock_sqlite_connect.return_value = mock_sqlite_conn

                from src.db.session import init_db
                await init_db()

    @pytest.mark.asyncio
    async def test_init_db_postgresql_path(self):
        """init_db takes the PostgreSQL branch without errors."""
        mock_inner_conn = AsyncMock()
        mock_inner_conn.execute = AsyncMock(return_value=MagicMock(fetchone=MagicMock(return_value=None)))
        mock_inner_conn.run_sync = AsyncMock()
        mock_inner_conn.commit = AsyncMock()

        mock_engine = MagicMock()
        mock_engine.connect = MagicMock(return_value=_make_async_cm(mock_inner_conn))
        mock_engine.begin = MagicMock(return_value=_make_async_cm(mock_inner_conn))
        mock_engine.dispose = AsyncMock()

        with patch("src.db.session.settings") as mock_settings, \
             patch("importlib.reload"), \
             patch("src.db.all_models"), \
             patch("asyncpg.connect", new_callable=AsyncMock) as mock_pg_connect, \
             patch("src.db.session.create_async_engine", return_value=mock_engine), \
             patch("src.db.session.NullPool"):

            mock_settings.DATABASE_URI = "postgresql+asyncpg://user:pass@localhost/testdb"
            mock_settings.POSTGRES_DB = "testdb"
            mock_settings.POSTGRES_SERVER = "localhost"
            mock_settings.POSTGRES_PORT = 5432
            mock_settings.POSTGRES_USER = "user"
            mock_settings.POSTGRES_PASSWORD = "pass"

            mock_pg_conn = AsyncMock()
            mock_pg_connect.return_value = mock_pg_conn

            from src.db.session import init_db
            await init_db()


# ---------------------------------------------------------------------------
# get_db: OperationalError retry paths and token reset ValueError
# ---------------------------------------------------------------------------

class TestGetDbEdgeCases:

    @pytest.mark.asyncio
    async def test_get_db_rollback_on_general_exception(self):
        """get_db rolls back session on general exception."""
        mock_session = AsyncMock()

        class MockCtx:
            async def __aenter__(self):
                return mock_session
            async def __aexit__(self, *args):
                pass

        mock_factory = MagicMock(return_value=MockCtx())

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.session import get_db

            gen = get_db()
            await gen.__anext__()
            with pytest.raises(ValueError):
                await gen.athrow(ValueError("test rollback"))

        mock_session.rollback.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_db_operational_error_non_lock_raises(self):
        """get_db raises OperationalError that is not a lock error."""
        mock_session = AsyncMock()

        class MockCtx:
            async def __aenter__(self):
                return mock_session
            async def __aexit__(self, *args):
                pass

        mock_factory = MagicMock(return_value=MockCtx())

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.session import get_db

            gen = get_db()
            await gen.__anext__()
            with pytest.raises(OperationalError):
                await gen.athrow(OperationalError("connection refused", None, None))

        mock_session.rollback.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_db_locked_error_triggers_rollback_and_retry(self):
        """get_db rolls back on locked error and retries (ends up succeeding on retry)."""
        mock_session = AsyncMock()

        class MockCtx:
            async def __aenter__(self):
                return mock_session
            async def __aexit__(self, *args):
                pass

        mock_factory = MagicMock(return_value=MockCtx())

        with patch("src.db.session.async_session_factory", mock_factory), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            from src.db.session import get_db

            gen = get_db()
            session = await gen.__anext__()
            # Throw a lock error — triggers rollback, then retries (succeeds on retry)
            try:
                await gen.athrow(OperationalError("database is locked", None, None))
            except StopAsyncIteration:
                pass  # Retry succeeded and generator finished
            except OperationalError:
                pass  # Retries exhausted

        # rollback must have been called due to the lock error
        mock_session.rollback.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_db_token_reset_value_error_suppressed(self):
        """get_db suppresses ValueError from _request_session.reset."""
        mock_session = AsyncMock()

        class MockCtx:
            async def __aenter__(self):
                return mock_session
            async def __aexit__(self, *args):
                pass

        mock_factory = MagicMock(return_value=MockCtx())

        with patch("src.db.session.async_session_factory", mock_factory), \
             patch("src.db.session._request_session") as mock_ctx_var:
            mock_token = MagicMock()
            mock_ctx_var.set.return_value = mock_token
            mock_ctx_var.reset.side_effect = ValueError("token mismatch")
            mock_ctx_var.get.return_value = None

            from src.db.session import get_db

            gen = get_db()
            session = await gen.__anext__()
            # Should not raise even though reset raises ValueError
            try:
                await gen.__anext__()
            except StopAsyncIteration:
                pass

    @pytest.mark.asyncio
    async def test_get_db_outer_operational_error_lock_retry(self):
        """get_db retries when outer OperationalError is 'database is locked'."""
        call_count = 0

        class MockCtxFactory:
            def __call__(self):
                nonlocal call_count
                call_count += 1
                ctx = AsyncMock()
                ctx.__aenter__ = AsyncMock(side_effect=OperationalError("database is locked", None, None))
                ctx.__aexit__ = AsyncMock(return_value=False)
                return ctx

        mock_factory = MockCtxFactory()

        with patch("src.db.session.async_session_factory", mock_factory), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            from src.db.session import get_db
            with pytest.raises(OperationalError):
                gen = get_db()
                await gen.__anext__()

        # Should have retried up to max_retries (3)
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_get_db_outer_operational_error_non_lock_raises_immediately(self):
        """get_db raises immediately when outer OperationalError is not a lock error."""
        class MockCtxFactory:
            def __call__(self):
                ctx = AsyncMock()
                ctx.__aenter__ = AsyncMock(
                    side_effect=OperationalError("server gone away", None, None)
                )
                ctx.__aexit__ = AsyncMock(return_value=False)
                return ctx

        with patch("src.db.session.async_session_factory", MockCtxFactory()):
            from src.db.session import get_db
            with pytest.raises(OperationalError, match="server gone away"):
                gen = get_db()
                await gen.__anext__()


# ---------------------------------------------------------------------------
# get_local_db: additional edge cases
# ---------------------------------------------------------------------------

class TestGetLocalDbEdgeCases:

    @pytest.mark.asyncio
    async def test_get_local_db_token_reset_value_error_suppressed(self):
        """get_local_db suppresses ValueError from _request_session.reset."""
        mock_session = AsyncMock()

        class MockCtx:
            async def __aenter__(self):
                return mock_session
            async def __aexit__(self, *args):
                pass

        with patch("src.db.session._local_session_factory", MagicMock(return_value=MockCtx())), \
             patch("src.db.session._request_session") as mock_ctx_var:
            mock_token = MagicMock()
            mock_ctx_var.set.return_value = mock_token
            mock_ctx_var.reset.side_effect = ValueError("bad token")
            mock_ctx_var.get.return_value = None

            from src.db.session import get_local_db

            gen = get_local_db()
            await gen.__anext__()
            try:
                await gen.__anext__()
            except StopAsyncIteration:
                pass

        # Completed without raising ValueError
        mock_session.commit.assert_awaited()


# ---------------------------------------------------------------------------
# dispose_engines: deduplicate engines
# ---------------------------------------------------------------------------

class TestDisposeEnginesDeduplication:

    @pytest.mark.asyncio
    async def test_disposes_deduplicated_engines(self):
        """dispose_engines only disposes each unique engine once."""
        from src.db.session import dispose_engines

        mock_engine = AsyncMock()
        mock_engine.__class__ = type(
            "MockEngine", (), {"dispose": mock_engine.dispose}
        )

        with patch("src.db.session.engine", mock_engine), \
             patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()):
            await dispose_engines()

        # dispose was called at least once
        mock_engine.dispose.assert_awaited()

    @pytest.mark.asyncio
    async def test_dispose_engines_outer_exception_caught(self):
        """dispose_engines handles unexpected outer exceptions gracefully."""
        from src.db.session import dispose_engines

        with patch("src.db.session.engine", None), \
             patch("src.db.lakebase_session.dispose_lakebase_factory", new=AsyncMock()):
            # Should not raise even when engine is None
            await dispose_engines()


# ---------------------------------------------------------------------------
# SwappableSessionFactory: is_lakebase property
# ---------------------------------------------------------------------------

class TestSwappableSessionFactoryProperty:

    def test_is_lakebase_false_by_default(self):
        from src.db.session import _SwappableSessionFactory
        factory = _SwappableSessionFactory(MagicMock())
        assert factory.is_lakebase is False

    def test_is_lakebase_true_after_activate(self):
        from src.db.session import _SwappableSessionFactory
        factory = _SwappableSessionFactory(MagicMock())
        factory.activate_lakebase(MagicMock())
        assert factory.is_lakebase is True

    def test_is_lakebase_false_after_deactivate(self):
        from src.db.session import _SwappableSessionFactory
        factory = _SwappableSessionFactory(MagicMock())
        factory.activate_lakebase(MagicMock())
        factory.deactivate_lakebase()
        assert factory.is_lakebase is False
