"""
Unit tests for database_router module.

Tests the routing logic that selects between regular database (PostgreSQL/SQLite)
and Lakebase sessions, including configuration reading, enable/disable checks,
and the get_smart_db_session async generator with all its edge cases.
"""
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
    """Tests for get_lakebase_config_from_db."""

    @pytest.mark.asyncio
    async def test_returns_config_value_when_found(self):
        """When a LakebaseConfig row exists, return its value dict."""
        expected_config = {
            "enabled": True,
            "endpoint": "https://example.com",
            "migration_completed": True,
            "instance_name": "my-instance",
        }

        mock_config_entry = MagicMock()
        mock_config_entry.value = expected_config

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_config_entry

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        with (
            patch("src.db.database_router.async_session_factory", mock_factory),
            patch("src.db.database_router.select") as mock_select,
        ):
            # Also patch the inner import of async_session_factory
            with patch.dict(
                "sys.modules",
                {"src.models.database_config": MagicMock(LakebaseConfig=MagicMock())},
            ):
                # Need to re-import to use patched module; easier to just patch the
                # already-imported references inside the function. Since the function
                # does `from src.db.session import async_session_factory` at call time,
                # we patch that path too.
                with patch(
                    "src.db.session.async_session_factory", mock_factory
                ):
                    from src.db.database_router import get_lakebase_config_from_db

                    result = await get_lakebase_config_from_db()

        assert result == expected_config

    @pytest.mark.asyncio
    async def test_returns_none_when_no_config_row(self):
        """When no LakebaseConfig row is found, return None."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        with (
            patch("src.db.session.async_session_factory", mock_factory),
        ):
            from src.db.database_router import get_lakebase_config_from_db

            result = await get_lakebase_config_from_db()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_config_value_is_none(self):
        """When config entry exists but value is None, return None."""
        mock_config_entry = MagicMock()
        mock_config_entry.value = None

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_config_entry

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.database_router import get_lakebase_config_from_db

            result = await get_lakebase_config_from_db()

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        """When a database error occurs, return None gracefully."""
        mock_factory = MagicMock(side_effect=Exception("DB unavailable"))

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.database_router import get_lakebase_config_from_db

            result = await get_lakebase_config_from_db()

        assert result is None


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
    async def test_returns_false_when_migration_not_completed(self):
        """When migration_completed is False, Lakebase is disabled."""
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
    async def test_falls_back_to_regular_db_on_connection_failure(self):
        """When Lakebase connection fails before yield, fall back to regular DB."""
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
                return_value=_FailingCtx(),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=mock_auth,
            ),
            patch("src.db.database_router.async_session_factory", mock_regular_factory),
            patch("src.db.database_router._request_session", mock_request_session),
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
    async def test_fallback_regular_db_rollbacks_on_handler_error(self):
        """When Lakebase fails pre-yield and the handler errors on regular DB, rollback."""
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
                return_value=_FailingCtx(),
            ),
            patch(
                "src.utils.databricks_auth.get_auth_context",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.db.database_router.async_session_factory", mock_regular_factory),
            patch("src.db.database_router._request_session", mock_request_session),
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
    async def test_returns_false_when_migration_completed_key_missing(self):
        """When migration_completed key is missing, defaults to False."""
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
    async def test_returns_none_when_value_is_empty_dict(self):
        """When config entry has an empty dict, it is falsy so return None."""
        mock_config_entry = MagicMock()
        mock_config_entry.value = {}

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_config_entry

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.database_router import get_lakebase_config_from_db

            result = await get_lakebase_config_from_db()

        # Empty dict is falsy in `if config_entry and config_entry.value`
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_value_when_non_empty_dict(self):
        """When config entry has a non-empty dict, return it."""
        config_value = {"enabled": False}

        mock_config_entry = MagicMock()
        mock_config_entry.value = config_value

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_config_entry

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_factory = MagicMock(return_value=_make_async_ctx(mock_session))

        with patch("src.db.session.async_session_factory", mock_factory):
            from src.db.database_router import get_lakebase_config_from_db

            result = await get_lakebase_config_from_db()

        assert result == config_value


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
