"""
Unit tests for database session module.

Tests session factory configuration, engine creation helpers,
identifier validation, safe_async_session, and get_db.
"""
import os
import re
import sqlite3
import tempfile
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path


# ---------------------------------------------------------------------------
# _validate_identifier tests
# ---------------------------------------------------------------------------
from src.db.session import _validate_identifier


class TestValidateIdentifier:
    """Tests for the SQL identifier validation function."""

    @pytest.mark.parametrize(
        "name",
        ["users", "execution_logs", "_private", "Table123", "a", "_", "ALLCAPS"],
    )
    def test_accepts_valid_identifiers(self, name):
        """Test that valid SQL identifiers are accepted."""
        assert _validate_identifier(name) == name

    def test_rejects_empty_string(self):
        """Test that an empty string raises ValueError."""
        with pytest.raises(ValueError):
            _validate_identifier("")

    def test_rejects_none(self):
        """Test that None raises an error."""
        with pytest.raises((ValueError, TypeError, AttributeError)):
            _validate_identifier(None)

    @pytest.mark.parametrize("name", ["123table", "1", "0users"])
    def test_rejects_leading_digit(self, name):
        """Test that identifiers starting with a digit are rejected."""
        with pytest.raises(ValueError):
            _validate_identifier(name)

    @pytest.mark.parametrize(
        "payload",
        [
            "users; DROP TABLE users; --",
            "schema.table",
            "my-table",
            "my table",
            "table'name",
            "users;",
            "table()",
            "table\nname",
            "user@domain",
        ],
    )
    def test_rejects_injection_payloads(self, payload):
        """Test that SQL injection payloads are rejected."""
        with pytest.raises(ValueError):
            _validate_identifier(payload)

    def test_error_contains_default_kind(self):
        """Test that the default kind label appears in the error message."""
        with pytest.raises(ValueError, match="identifier"):
            _validate_identifier("bad-name")

    def test_error_contains_custom_kind(self):
        """Test that a custom kind label appears in the error message."""
        with pytest.raises(ValueError, match="database name"):
            _validate_identifier("bad-name", "database name")

    def test_error_contains_repr_of_value(self):
        """Test that the repr of the invalid value is in the error."""
        try:
            _validate_identifier("bad name")
            pytest.fail("Expected ValueError")
        except ValueError as exc:
            assert "'bad name'" in str(exc)


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------
from src.db.session import get_isolation_level, get_sqlite_connect_args


class TestGetIsolationLevel:
    """Tests for the get_isolation_level helper."""

    def test_sqlite_returns_none(self):
        """Test that SQLite URIs return None isolation level."""
        assert get_isolation_level("sqlite:///test.db") is None
        assert get_isolation_level("sqlite+aiosqlite:///test.db") is None

    def test_postgres_returns_read_committed(self):
        """Test that PostgreSQL URIs return READ COMMITTED."""
        assert get_isolation_level("postgresql+asyncpg://user:pass@host/db") == "READ COMMITTED"

    def test_other_returns_read_committed(self):
        """Test that non-sqlite URIs return READ COMMITTED."""
        assert get_isolation_level("mysql://user:pass@host/db") == "READ COMMITTED"


class TestGetSqliteConnectArgs:
    """Tests for get_sqlite_connect_args helper."""

    def test_sqlite_uri_returns_args(self):
        """Test that SQLite URIs return connection arguments."""
        args = get_sqlite_connect_args("sqlite:///test.db")
        assert "check_same_thread" in args
        assert args["check_same_thread"] is False
        assert "timeout" in args
        assert args["timeout"] == 60

    def test_postgres_uri_returns_empty(self):
        """Test that non-SQLite URIs return empty dict."""
        args = get_sqlite_connect_args("postgresql+asyncpg://user:pass@host/db")
        assert args == {}


# ---------------------------------------------------------------------------
# safe_async_session tests
# ---------------------------------------------------------------------------
class TestSafeAsyncSession:
    """Tests for the safe_async_session context manager."""

    @pytest.mark.asyncio
    async def test_yields_session_and_closes(self):
        """Test that safe_async_session yields a session and closes it."""
        mock_session = AsyncMock()
        mock_factory = MagicMock(return_value=mock_session)

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.session import safe_async_session

            async with safe_async_session() as session:
                assert session is mock_session

            mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_suppresses_close_errors(self):
        """Test that close errors are suppressed (stale connections)."""
        mock_session = AsyncMock()
        mock_session.close.side_effect = Exception("no active connection")
        mock_factory = MagicMock(return_value=mock_session)

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.session import safe_async_session

            async with safe_async_session() as session:
                assert session is mock_session

    @pytest.mark.asyncio
    async def test_propagates_body_exceptions(self):
        """Test that exceptions inside the with-block propagate normally."""
        mock_session = AsyncMock()
        mock_factory = MagicMock(return_value=mock_session)

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.session import safe_async_session

            with pytest.raises(ValueError, match="test error"):
                async with safe_async_session():
                    raise ValueError("test error")

            mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_error_during_body_exception(self):
        """Test body exception propagates even when close also fails."""
        mock_session = AsyncMock()
        mock_session.close.side_effect = Exception("stale connection")
        mock_factory = MagicMock(return_value=mock_session)

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.session import safe_async_session

            with pytest.raises(RuntimeError, match="body error"):
                async with safe_async_session():
                    raise RuntimeError("body error")


# ---------------------------------------------------------------------------
# Database utility / path tests
# ---------------------------------------------------------------------------
class TestDatabaseUtilities:
    """Test database utility functions and path operations."""

    def test_absolute_path_detection(self):
        """Test absolute path detection."""
        assert os.path.isabs("/absolute/path/to/db.sqlite")
        assert not os.path.isabs("relative/path/to/db.sqlite")

    def test_sqlite_uri_parsing(self):
        """Test SQLite URI format."""
        sqlite_uri = "sqlite:///test.db"
        assert "sqlite" in sqlite_uri
        assert "test.db" in sqlite_uri

    def test_postgres_uri_format(self):
        """Test PostgreSQL URI format."""
        pg_uri = "postgresql+asyncpg://user:pass@localhost:5432/db"
        assert "postgresql" in pg_uri
        assert "asyncpg" in pg_uri

    def test_connection_string_formation(self):
        """Test building connection strings."""
        db_path = "tmp/test.db"
        sqlite_uri = f"sqlite:///{db_path}"
        assert sqlite_uri == "sqlite:///tmp/test.db"


class TestSQLiteOperations:
    """Test SQLite-specific operations."""

    def test_sqlite_database_creation(self):
        """Test creating and verifying a SQLite database."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.close()

            assert os.path.exists(db_path)

            conn = sqlite3.connect(db_path)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

            assert "test" in tables
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


# ---------------------------------------------------------------------------
# get_db tests
# ---------------------------------------------------------------------------
class TestGetDbFunction:
    """Tests for the get_db async generator."""

    @pytest.mark.asyncio
    async def test_get_db_success_path(self):
        """Test get_db yields a session and commits on success."""
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
            session = await gen.__anext__()
            assert session is mock_session

            try:
                await gen.__anext__()
            except StopAsyncIteration:
                pass

            mock_session.commit.assert_awaited_once()


# ---------------------------------------------------------------------------
# Module-level configuration tests
# ---------------------------------------------------------------------------
class TestModuleLevelConfiguration:
    """Test module-level session configuration."""

    def test_async_session_factory_exists(self):
        """Test that async_session_factory is defined."""
        from src.db.session import async_session_factory
        assert async_session_factory is not None

    def test_request_scoped_session_exists(self):
        """Test that request_scoped_session is defined."""
        from src.db.session import request_scoped_session
        assert request_scoped_session is not None

    def test_engine_exists(self):
        """Test that the engine is defined."""
        from src.db.session import engine
        assert engine is not None

    def test_sync_session_factory_exists(self):
        """Test that sync_session_factory is defined."""
        from src.db.session import sync_session_factory
        assert sync_session_factory is not None

    def test_set_main_event_loop_callable(self):
        """Test that set_main_event_loop is callable."""
        from src.db.session import set_main_event_loop
        assert callable(set_main_event_loop)

    def test_get_smart_engine_callable(self):
        """Test that get_smart_engine is callable."""
        from src.db.session import get_smart_engine
        assert callable(get_smart_engine)

    def test_init_db_callable(self):
        """Test that init_db is callable."""
        from src.db.session import init_db
        assert callable(init_db)

    def test_dispose_engines_callable(self):
        """Test that dispose_engines is callable."""
        from src.db.session import dispose_engines
        assert callable(dispose_engines)
