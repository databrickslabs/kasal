"""
Unit tests for database_router module.

Tests the routing logic that selects between regular database (PostgreSQL/SQLite)
and Lakebase sessions, including configuration reading, enable/disable checks,
and the get_smart_db_session async generator with all its edge cases.
"""
import json
import os
import sqlite3
import tempfile
import pytest
from contextvars import Token
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_async_ctx(session):
    """Create an async context manager that yields *session*."""
    class _Ctx:
        async def __aenter__(self):
            return session

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

    return _Ctx()


# ---------------------------------------------------------------------------
# get_lakebase_config_from_db
# ---------------------------------------------------------------------------

class TestGetLakebaseConfigFromDb:
    """Tests for get_lakebase_config_from_db (reads directly from SQLite)."""

    @pytest.fixture
    def tmp_db(self, tmp_path):
        """Create a temp SQLite DB with a database_configs table."""
        db_file = tmp_path / "app.db"
        conn = sqlite3.connect(str(db_file))
        conn.execute("CREATE TABLE database_configs (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()
        conn.close()
        return str(db_file)

    @pytest.mark.asyncio
    async def test_returns_config_value_when_found(self, tmp_db):
        """When a lakebase config row exists in SQLite, return its value dict."""
        expected_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "my-instance",
        }
        conn = sqlite3.connect(tmp_db)
        conn.execute(
            "INSERT INTO database_configs (key, value) VALUES (?, ?)",
            ("lakebase", json.dumps(expected_config)),
        )
        conn.commit()
        conn.close()

        mock_settings = MagicMock()
        mock_settings.SQLITE_DB_PATH = tmp_db

        with patch("src.db.database_router.os.path.exists", return_value=True), \
             patch("src.db.session.settings", mock_settings):
            from src.db.database_router import get_lakebase_config_from_db
            result = await get_lakebase_config_from_db()

        assert result == expected_config

    @pytest.mark.asyncio
    async def test_returns_none_when_no_config_row(self, tmp_db):
        """When no lakebase row exists, return None."""
        mock_settings = MagicMock()
        mock_settings.SQLITE_DB_PATH = tmp_db

        with patch("src.db.database_router.os.path.exists", return_value=True), \
             patch("src.db.session.settings", mock_settings):
            from src.db.database_router import get_lakebase_config_from_db
            result = await get_lakebase_config_from_db()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_db_file_missing(self):
        """When the SQLite file does not exist, return None."""
        mock_settings = MagicMock()
        mock_settings.SQLITE_DB_PATH = "/tmp/nonexistent_db_file.db"

        with patch("src.db.session.settings", mock_settings):
            from src.db.database_router import get_lakebase_config_from_db
            result = await get_lakebase_config_from_db()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_value_is_empty_string(self, tmp_db):
        """When config row value is empty string (falsy), return None."""
        conn = sqlite3.connect(tmp_db)
        conn.execute(
            "INSERT INTO database_configs (key, value) VALUES (?, ?)",
            ("lakebase", ""),
        )
        conn.commit()
        conn.close()

        mock_settings = MagicMock()
        mock_settings.SQLITE_DB_PATH = tmp_db

        with patch("src.db.database_router.os.path.exists", return_value=True), \
             patch("src.db.session.settings", mock_settings):
            from src.db.database_router import get_lakebase_config_from_db
            result = await get_lakebase_config_from_db()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        """When a database error occurs (e.g., no table), return None gracefully."""
        # Create a DB file without the expected table
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_path = f.name
        try:
            conn = sqlite3.connect(tmp_path)
            conn.close()

            mock_settings = MagicMock()
            mock_settings.SQLITE_DB_PATH = tmp_path

            with patch("src.db.database_router.os.path.exists", return_value=True), \
                 patch("src.db.session.settings", mock_settings):
                from src.db.database_router import get_lakebase_config_from_db
                result = await get_lakebase_config_from_db()

            assert result is None
        finally:
            os.unlink(tmp_path)

    @pytest.mark.asyncio
    async def test_relative_path_converted_to_absolute(self, tmp_path):
        """When SQLITE_DB_PATH is relative, it is converted to absolute."""
        db_file = tmp_path / "app.db"
        conn = sqlite3.connect(str(db_file))
        conn.execute("CREATE TABLE database_configs (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute(
            "INSERT INTO database_configs (key, value) VALUES (?, ?)",
            ("lakebase", json.dumps({"enabled": True})),
        )
        conn.commit()
        conn.close()

        mock_settings = MagicMock()
        mock_settings.SQLITE_DB_PATH = None  # triggers fallback

        with patch("src.db.database_router.os.path.exists", return_value=True), \
             patch("src.db.database_router.os.path.isabs", return_value=False), \
             patch("src.db.database_router.os.path.abspath", return_value=str(db_file)), \
             patch("src.db.session.settings", mock_settings):
            from src.db.database_router import get_lakebase_config_from_db
            result = await get_lakebase_config_from_db()

        assert result == {"enabled": True}

    @pytest.mark.asyncio
    async def test_returns_non_string_value_directly(self, tmp_db):
        """When the row value is not a string (e.g., already dict), return it directly.

        Note: SQLite stores TEXT so this tests the isinstance check branch.
        Since sqlite3 always returns strings, this tests the non-string branch
        via direct mock.
        """
        mock_settings = MagicMock()
        mock_settings.SQLITE_DB_PATH = tmp_db

        # Patch sqlite3.connect to return a cursor that returns a non-string value
        expected = {"enabled": True, "endpoint": "https://example.com"}
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (expected,)  # value is already a dict
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.db.database_router.os.path.exists", return_value=True), \
             patch("src.db.session.settings", mock_settings), \
             patch("sqlite3.connect", return_value=mock_conn):
            from src.db.database_router import get_lakebase_config_from_db
            result = await get_lakebase_config_from_db()

        assert result == expected
        mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# is_lakebase_enabled
# ---------------------------------------------------------------------------

class TestIsLakebaseEnabled:
    """Tests for is_lakebase_enabled."""

    @pytest.mark.asyncio
    async def test_returns_false_when_no_config(self):
        """When get_lakebase_config_from_db returns None, Lakebase is disabled."""
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=None,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_true_when_fully_configured(self):
        """When enabled, endpoint, and migration_completed are all truthy, return True."""
        config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_enabled_is_false(self):
        """When enabled is False, Lakebase is disabled."""
        config = {
            "enabled": False,
            "endpoint": "https://example.com",
            "migration_completed": True,
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_endpoint_missing(self):
        """When endpoint is empty or missing, Lakebase is disabled."""
        config = {
            "enabled": True,
            "endpoint": "",
            "migration_completed": True,
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert not result

    @pytest.mark.asyncio
    async def test_returns_false_when_endpoint_key_absent(self):
        """When endpoint key is missing from config dict, Lakebase is disabled."""
        config = {
            "enabled": True,
            "migration_completed": True,
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert not result

    @pytest.mark.asyncio
    async def test_returns_false_when_migration_not_completed_and_no_alternatives(self):
        """When migration_completed is False and no database_type or instance_status, disabled."""
        config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": False,
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_true_when_database_type_is_lakebase(self):
        """When migration_completed missing but database_type is 'lakebase', enabled."""
        config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "database_type": "lakebase",
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_true_when_instance_status_is_ready(self):
        """When migration_completed missing but instance_status is 'READY', enabled."""
        config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "instance_status": "READY",
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_instance_status_is_creating(self):
        """When instance_status is not READY and no migration_completed, disabled."""
        config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "instance_status": "CREATING",
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self):
        """When an exception is raised reading the config, return False."""
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            side_effect=Exception("DB table does not exist"),
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is False


# ---------------------------------------------------------------------------
# get_smart_db_session - regular (non-Lakebase) path
# ---------------------------------------------------------------------------

class TestGetSmartDbSessionRegularPath:
    """Tests for get_smart_db_session when Lakebase is disabled."""

    @pytest.mark.asyncio
    async def test_yields_regular_session_when_lakebase_disabled(self):
        """When Lakebase is disabled, yield a regular DB session."""
        mock_session = AsyncMock()

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch("src.db.database_router.async_session_factory", mock_factory),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()

            assert session is mock_session
            mock_request_session.set.assert_called_once_with(mock_session)

            # Finish the generator (simulates successful request)
            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()

            mock_session.commit.assert_awaited_once()
            mock_request_session.reset.assert_called_once_with(mock_token)
            mock_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_regular_session_rollbacks_on_exception(self):
        """When the request handler raises, regular session is rolled back."""
        mock_session = AsyncMock()

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch("src.db.database_router.async_session_factory", mock_factory),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()

            assert session is mock_session

            # Simulate the request handler raising an exception
            with pytest.raises(ValueError, match="handler error"):
                await gen.athrow(ValueError("handler error"))

            mock_session.rollback.assert_awaited_once()
            mock_session.commit.assert_not_awaited()
            mock_session.close.assert_awaited_once()
            mock_request_session.reset.assert_called_once_with(mock_token)

    @pytest.mark.asyncio
    async def test_request_session_context_var_is_set_and_reset(self):
        """The _request_session context var is set before yield and reset after."""
        mock_session = AsyncMock()

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch("src.db.database_router.async_session_factory", mock_factory),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()

            # At this point, set should have been called but reset should not
            mock_request_session.set.assert_called_once_with(mock_session)
            mock_request_session.reset.assert_not_called()

            # Finish the generator
            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()

            # Now reset should have been called
            mock_request_session.reset.assert_called_once_with(mock_token)

    @pytest.mark.asyncio
    async def test_reset_value_error_is_suppressed(self):
        """If _request_session.reset raises ValueError, it is silently suppressed."""
        mock_session = AsyncMock()

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token
        mock_request_session.reset.side_effect = ValueError("wrong context")

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch("src.db.database_router.async_session_factory", mock_factory),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            await gen.__anext__()

            # Should not raise even though reset raises ValueError
            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()


# ---------------------------------------------------------------------------
# get_smart_db_session - Lakebase path (success)
# ---------------------------------------------------------------------------

class TestGetSmartDbSessionLakebasePath:
    """Tests for get_smart_db_session when Lakebase is enabled and succeeds."""

    @pytest.mark.asyncio
    async def test_yields_lakebase_session_when_enabled(self):
        """When Lakebase is enabled and connection succeeds, yield a Lakebase session."""
        lakebase_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                return_value=_make_async_ctx(lakebase_session),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()

            assert session is lakebase_session
            mock_request_session.set.assert_called_once_with(lakebase_session)

            # Finish the generator
            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()

            mock_request_session.reset.assert_called_once_with(mock_token)

    @pytest.mark.asyncio
    async def test_uses_env_var_instance_name_when_config_has_none(self):
        """When config has no instance_name, fall back to LAKEBASE_INSTANCE_NAME env var."""
        lakebase_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            # No instance_name key
        }

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        mock_get_lakebase_session = MagicMock(
            return_value=_make_async_ctx(lakebase_session)
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                mock_get_lakebase_session,
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router._request_session", mock_request_session),
            patch.dict("os.environ", {"LAKEBASE_INSTANCE_NAME": "env-instance"}),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()
            assert session is lakebase_session

            # Verify the instance_name passed to get_lakebase_session
            mock_get_lakebase_session.assert_called_once_with(
                "env-instance", "fake-token", "user@example.com"
            )

            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()

    @pytest.mark.asyncio
    async def test_auth_failure_still_attempts_lakebase(self):
        """When get_auth_context raises, user_token/email are None but Lakebase still tried."""
        lakebase_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        mock_get_lakebase_session = MagicMock(
            return_value=_make_async_ctx(lakebase_session)
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                mock_get_lakebase_session,
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                side_effect=Exception("Auth unavailable"),
            ),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()
            assert session is lakebase_session

            # user_token and user_email should be None
            mock_get_lakebase_session.assert_called_once_with(
                "test-instance", None, None
            )

            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()

    @pytest.mark.asyncio
    async def test_auth_returns_none_user_token_and_email_are_none(self):
        """When get_auth_context returns None, user_token/email remain None."""
        lakebase_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        mock_get_lakebase_session = MagicMock(
            return_value=_make_async_ctx(lakebase_session)
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                mock_get_lakebase_session,
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()
            assert session is lakebase_session

            # user_token and user_email should be None
            mock_get_lakebase_session.assert_called_once_with(
                "test-instance", None, None
            )

            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()


# ---------------------------------------------------------------------------
# get_smart_db_session - Lakebase fallback path (connection failure)
# ---------------------------------------------------------------------------

class TestGetSmartDbSessionLakebaseFallback:
    """Tests for get_smart_db_session when Lakebase connection fails BEFORE yield."""

    @pytest.mark.asyncio
    async def test_falls_back_to_regular_db_on_connection_failure_during_startup(self):
        """When Lakebase fails before yield and fallback is allowed (startup), fall back to regular DB."""
        regular_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        # get_lakebase_session raises an error (connection failure before yield)
        # Must return a new _FailingCtx each time since the retry loop calls it 3 times
        class _FailingCtx:
            async def __aenter__(self):
                raise ConnectionError("Lakebase unreachable")

            async def __aexit__(self, *args):
                return False

        mock_regular_factory = MagicMock(
            return_value=_make_async_ctx(regular_session)
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                side_effect=lambda *a, **kw: _FailingCtx(),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router.async_session_factory", mock_regular_factory),
            patch("src.db.database_router._request_session", mock_request_session),
            patch("src.db.database_router.is_fallback_allowed", return_value=True),
            patch("src.db.database_router.asyncio.sleep", new_callable=AsyncMock),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()

            # Should get the regular session as fallback
            assert session is regular_session

            # Finish the generator
            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()

            regular_session.commit.assert_awaited_once()
            regular_session.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_raises_lakebase_unavailable_when_fallback_disabled(self):
        """When Lakebase fails and fallback is NOT allowed (runtime), raise LakebaseUnavailableError."""
        from src.core.exceptions import LakebaseUnavailableError

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        class _FailingCtx:
            async def __aenter__(self):
                raise ConnectionError("Lakebase unreachable")

            async def __aexit__(self, *args):
                return False

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                side_effect=lambda *a, **kw: _FailingCtx(),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router.is_fallback_allowed", return_value=False),
            patch("src.db.database_router.asyncio.sleep", new_callable=AsyncMock),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            with pytest.raises(LakebaseUnavailableError):
                await gen.__anext__()

    @pytest.mark.asyncio
    async def test_retry_succeeds_on_second_attempt(self):
        """When Lakebase fails once then succeeds, the second attempt works."""
        lakebase_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        call_count = 0

        class _IntermittentCtx:
            async def __aenter__(self):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ConnectionError("first attempt fails")
                return lakebase_session

            async def __aexit__(self, *args):
                return False

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                side_effect=lambda *a, **kw: _IntermittentCtx(),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router._request_session", mock_request_session),
            patch("src.db.database_router.record_successful_connection") as mock_record,
            patch("src.db.database_router.asyncio.sleep", new_callable=AsyncMock),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()

            assert session is lakebase_session
            mock_record.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_uses_exponential_backoff(self):
        """Verify retry loop calls asyncio.sleep with exponential backoff delays."""
        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        class _FailingCtx:
            async def __aenter__(self):
                raise ConnectionError("Lakebase unreachable")

            async def __aexit__(self, *args):
                return False

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                side_effect=lambda *a, **kw: _FailingCtx(),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router.is_fallback_allowed", return_value=False),
            patch("src.db.database_router.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            with pytest.raises(Exception):
                await gen.__anext__()

            # Should have slept twice (before retry 2 and retry 3)
            assert mock_sleep.await_count == 2
            mock_sleep.assert_any_await(0.5)
            mock_sleep.assert_any_await(1.0)

    @pytest.mark.asyncio
    async def test_fallback_regular_db_rollbacks_on_handler_error(self):
        """When Lakebase fails pre-yield (startup) and the handler errors on regular DB, rollback."""
        regular_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        class _FailingCtx:
            async def __aenter__(self):
                raise ConnectionError("Lakebase unreachable")

            async def __aexit__(self, *args):
                return False

        mock_regular_factory = MagicMock(
            return_value=_make_async_ctx(regular_session)
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                side_effect=lambda *a, **kw: _FailingCtx(),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.db.database_router.async_session_factory", mock_regular_factory),
            patch("src.db.database_router._request_session", mock_request_session),
            patch("src.db.database_router.is_fallback_allowed", return_value=True),
            patch("src.db.database_router.asyncio.sleep", new_callable=AsyncMock),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()
            assert session is regular_session

            # Simulate request handler raising an error
            with pytest.raises(RuntimeError, match="handler boom"):
                await gen.athrow(RuntimeError("handler boom"))

            regular_session.rollback.assert_awaited_once()
            regular_session.commit.assert_not_awaited()
            regular_session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_smart_db_session - Lakebase post-yield error (no fallback)
# ---------------------------------------------------------------------------

class TestGetSmartDbSessionLakebasePostYieldError:
    """Tests for get_smart_db_session when Lakebase errors AFTER yield (during request)."""

    @pytest.mark.asyncio
    async def test_re_raises_when_lakebase_errors_after_yield(self):
        """When Lakebase session yields but the handler raises, re-raise without fallback."""
        lakebase_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                return_value=_make_async_ctx(lakebase_session),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()
            assert session is lakebase_session

            # Simulate the request handler raising an error after yield
            with pytest.raises(RuntimeError, match="request processing error"):
                await gen.athrow(RuntimeError("request processing error"))

            # The generator should NOT fall through to regular DB
            mock_request_session.reset.assert_called_once_with(mock_token)


# ---------------------------------------------------------------------------
# get_smart_db_session - GeneratorExit handling
# ---------------------------------------------------------------------------

class TestGetSmartDbSessionGeneratorExit:
    """Tests for get_smart_db_session GeneratorExit behavior."""

    @pytest.mark.asyncio
    async def test_lakebase_generator_exit_returns_cleanly(self):
        """When GeneratorExit is thrown on the Lakebase path, the generator returns cleanly."""
        lakebase_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                return_value=_make_async_ctx(lakebase_session),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()
            assert session is lakebase_session

            # Close the generator (triggers GeneratorExit inside)
            await gen.aclose()

            # Should complete without error
            mock_request_session.reset.assert_called_once_with(mock_token)

    @pytest.mark.asyncio
    async def test_regular_db_generator_exit_returns_cleanly(self):
        """When GeneratorExit is thrown on the regular DB path, cleanup happens."""
        mock_session = AsyncMock()

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch("src.db.database_router.async_session_factory", mock_factory),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()
            assert session is mock_session

            # Close the generator
            await gen.aclose()

            # Session cleanup should still occur
            mock_request_session.reset.assert_called_once_with(mock_token)
            mock_session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_smart_db_session - Lakebase path, context var reset ValueError
# ---------------------------------------------------------------------------

class TestGetSmartDbSessionLakebaseResetError:
    """Tests for _request_session.reset ValueError handling in the Lakebase path."""

    @pytest.mark.asyncio
    async def test_lakebase_reset_value_error_suppressed(self):
        """If _request_session.reset raises ValueError on Lakebase path, it is suppressed."""
        lakebase_session = AsyncMock()

        lakebase_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "test-instance",
        }

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token
        mock_request_session.reset.side_effect = ValueError("wrong context")

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=lakebase_config,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                return_value=_make_async_ctx(lakebase_session),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router._request_session", mock_request_session),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()
            assert session is lakebase_session

            # Finish the generator - should not raise despite ValueError on reset
            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()


# ---------------------------------------------------------------------------
# get_smart_db_session - config is None after is_lakebase_enabled returns True
# ---------------------------------------------------------------------------

class TestGetSmartDbSessionConfigNoneAfterEnabled:
    """Edge case: is_lakebase_enabled returns True but get_lakebase_config_from_db returns None."""

    @pytest.mark.asyncio
    async def test_uses_default_instance_name_when_config_is_none(self):
        """When config is None after Lakebase enabled, instance_name falls back to env."""
        lakebase_session = AsyncMock()

        mock_auth = MagicMock()
        mock_auth.token = "fake-token"
        mock_auth.user_identity = "user@example.com"
        mock_auth.auth_method = "obo"

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        mock_get_lakebase_session = MagicMock(
            return_value=_make_async_ctx(lakebase_session)
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                mock_get_lakebase_session,
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router._request_session", mock_request_session),
            patch.dict("os.environ", {"LAKEBASE_INSTANCE_NAME": "env-fallback"}),
        ):
            from src.db.database_router import get_smart_db_session

            gen = get_smart_db_session()
            session = await gen.__anext__()
            assert session is lakebase_session

            # instance_name should fall back to env
            mock_get_lakebase_session.assert_called_once_with(
                "env-fallback", "fake-token", "user@example.com"
            )

            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()

    @pytest.mark.asyncio
    async def test_uses_kasal_lakebase_default_when_no_env(self):
        """When config is None and no env var, instance_name defaults to 'kasal-lakebase'."""
        lakebase_session = AsyncMock()

        mock_token = MagicMock(spec=Token)
        mock_request_session = MagicMock()
        mock_request_session.set.return_value = mock_token

        mock_get_lakebase_session = MagicMock(
            return_value=_make_async_ctx(lakebase_session)
        )

        with (
            patch(
                "src.db.database_router.is_lakebase_enabled",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "src.db.database_router.get_lakebase_config_from_db",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "src.db.database_router.get_lakebase_session",
                mock_get_lakebase_session,
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.db.database_router._request_session", mock_request_session),
            patch.dict("os.environ", {}, clear=False),
        ):
            # Remove LAKEBASE_INSTANCE_NAME if set
            import os
            env_backup = os.environ.pop("LAKEBASE_INSTANCE_NAME", None)
            try:
                from src.db.database_router import get_smart_db_session

                gen = get_smart_db_session()
                session = await gen.__anext__()
                assert session is lakebase_session

                # Default fallback is "kasal-lakebase"
                mock_get_lakebase_session.assert_called_once_with(
                    "kasal-lakebase", None, None
                )

                with pytest.raises(StopAsyncIteration):
                    await gen.__anext__()
            finally:
                if env_backup is not None:
                    os.environ["LAKEBASE_INSTANCE_NAME"] = env_backup


# ---------------------------------------------------------------------------
# is_lakebase_enabled - edge cases
# ---------------------------------------------------------------------------

class TestIsLakebaseEnabledEdgeCases:
    """Additional edge cases for is_lakebase_enabled."""

    @pytest.mark.asyncio
    async def test_returns_false_when_enabled_key_missing(self):
        """When the config dict has no 'enabled' key, defaults to False."""
        config = {
            "endpoint": "https://example.com",
            "migration_completed": True,
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_all_tertiary_keys_missing(self):
        """When migration_completed, database_type, instance_status are all absent, disabled."""
        config = {
            "enabled": True,
            "endpoint": "https://example.com",
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_true_with_all_truthy_values(self):
        """Verify the three-way AND logic with all truthy values."""
        config = {
            "enabled": True,
            "endpoint": "some-endpoint",
            "migration_completed": True,
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_endpoint_is_none(self):
        """When endpoint is explicitly None, Lakebase is disabled."""
        config = {
            "enabled": True,
            "endpoint": None,
            "migration_completed": True,
        }
        with patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=config,
        ):
            from src.db.database_router import is_lakebase_enabled

            result = await is_lakebase_enabled()

        assert not result


# ---------------------------------------------------------------------------
# get_lakebase_config_from_db - config entry has empty value
# ---------------------------------------------------------------------------

class TestGetLakebaseConfigFromDbEmptyValue:
    """Edge cases for get_lakebase_config_from_db with empty/falsy config values."""

    @pytest.mark.asyncio
    async def test_returns_none_when_value_is_null_row(self):
        """When config row value is NULL in SQLite, return None."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_path = f.name
        try:
            conn = sqlite3.connect(tmp_path)
            conn.execute("CREATE TABLE database_configs (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute(
                "INSERT INTO database_configs (key, value) VALUES (?, ?)",
                ("lakebase", None),
            )
            conn.commit()
            conn.close()

            mock_settings = MagicMock()
            mock_settings.SQLITE_DB_PATH = tmp_path

            with patch("src.db.database_router.os.path.exists", return_value=True), \
                 patch("src.db.session.settings", mock_settings):
                from src.db.database_router import get_lakebase_config_from_db
                result = await get_lakebase_config_from_db()

            assert result is None
        finally:
            os.unlink(tmp_path)

    @pytest.mark.asyncio
    async def test_returns_value_when_non_empty_json(self):
        """When config row has valid JSON, return the parsed dict."""
        config_value = {"enabled": False}

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_path = f.name
        try:
            conn = sqlite3.connect(tmp_path)
            conn.execute("CREATE TABLE database_configs (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute(
                "INSERT INTO database_configs (key, value) VALUES (?, ?)",
                ("lakebase", json.dumps(config_value)),
            )
            conn.commit()
            conn.close()

            mock_settings = MagicMock()
            mock_settings.SQLITE_DB_PATH = tmp_path

            with patch("src.db.database_router.os.path.exists", return_value=True), \
                 patch("src.db.session.settings", mock_settings):
                from src.db.database_router import get_lakebase_config_from_db
                result = await get_lakebase_config_from_db()

            assert result == config_value
        finally:
            os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Module-level imports and exports
# ---------------------------------------------------------------------------

class TestModuleLevelExports:
    """Verify the module exposes the expected public functions."""

    def test_get_lakebase_config_from_db_is_callable(self):
        from src.db.database_router import get_lakebase_config_from_db
        assert callable(get_lakebase_config_from_db)

    def test_is_lakebase_enabled_is_callable(self):
        from src.db.database_router import is_lakebase_enabled
        assert callable(is_lakebase_enabled)

    def test_get_smart_db_session_is_callable(self):
        from src.db.database_router import get_smart_db_session
        assert callable(get_smart_db_session)
