"""
Unit tests for LakebaseConnectionService.

Tests all connection management, credential generation, engine creation,
and user identity resolution for Databricks Lakebase (PostgreSQL) instances.
"""
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from typing import Optional


from src.services.lakebase_connection_service import LakebaseConnectionService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def user_token():
    """Provide a fake user token for OBO authentication."""
    return "fake-user-token-abc123"


@pytest.fixture
def user_email():
    """Provide a fake user email for OBO authentication."""
    return "testuser@example.com"


@pytest.fixture
def service(user_token, user_email):
    """Create a LakebaseConnectionService with both token and email."""
    return LakebaseConnectionService(user_token=user_token, user_email=user_email)


@pytest.fixture
def service_no_email(user_token):
    """Create a LakebaseConnectionService without user_email."""
    return LakebaseConnectionService(user_token=user_token, user_email=None)


@pytest.fixture
def mock_credential():
    """Create a mock database credential object."""
    return SimpleNamespace(
        token="fake-db-token-xyz789",
        expiration_time="2099-12-31T23:59:59Z",
    )


@pytest.fixture
def endpoint():
    """Provide a fake Lakebase endpoint."""
    return "lakebase-test.example.com"


@pytest.fixture
def instance_name():
    """Provide a fake Lakebase instance name."""
    return "test-lakebase-instance"


# ---------------------------------------------------------------------------
# Test class: Constructor
# ---------------------------------------------------------------------------

class TestLakebaseConnectionServiceInit:
    """Tests for LakebaseConnectionService constructor."""

    def test_init_stores_user_token(self, user_token, user_email):
        """Constructor should store the user_token attribute."""
        svc = LakebaseConnectionService(user_token=user_token, user_email=user_email)
        assert svc.user_token == user_token

    def test_init_stores_user_email(self, user_token, user_email):
        """Constructor should store the user_email attribute."""
        svc = LakebaseConnectionService(user_token=user_token, user_email=user_email)
        assert svc.user_email == user_email

    def test_init_workspace_client_is_none(self):
        """Constructor should set _workspace_client to None (lazy init)."""
        svc = LakebaseConnectionService()
        assert svc._workspace_client is None

    def test_init_defaults_to_none(self):
        """Constructor with no arguments should default both to None."""
        svc = LakebaseConnectionService()
        assert svc.user_token is None
        assert svc.user_email is None


# ---------------------------------------------------------------------------
# Test class: get_workspace_client
# ---------------------------------------------------------------------------

class TestGetWorkspaceClient:
    """Tests for the get_workspace_client method."""

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_lazy_initialization(self, mock_get_ws, service, user_token):
        """get_workspace_client should create client on first call."""
        mock_ws = MagicMock()
        mock_get_ws.return_value = mock_ws

        result = await service.get_workspace_client()

        mock_get_ws.assert_awaited_once_with(user_token)
        assert result is mock_ws
        assert service._workspace_client is mock_ws

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_caches_result_on_second_call(self, mock_get_ws, service):
        """get_workspace_client should return the cached client on subsequent calls."""
        mock_ws = MagicMock()
        mock_get_ws.return_value = mock_ws

        first = await service.get_workspace_client()
        second = await service.get_workspace_client()

        # Only called once despite two invocations
        mock_get_ws.assert_awaited_once()
        assert first is second

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_raises_value_error_on_none(self, mock_get_ws, service):
        """get_workspace_client should raise ValueError when client creation returns None."""
        mock_get_ws.return_value = None

        with pytest.raises(ValueError, match="Failed to create WorkspaceClient"):
            await service.get_workspace_client()

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_raises_value_error_does_not_cache_none(self, mock_get_ws, service):
        """After ValueError, _workspace_client should remain None (no caching of failure)."""
        mock_get_ws.return_value = None

        with pytest.raises(ValueError):
            await service.get_workspace_client()

        assert service._workspace_client is None


# ---------------------------------------------------------------------------
# Test class: generate_credentials
# ---------------------------------------------------------------------------

class TestGenerateCredentials:
    """Tests for the generate_credentials method."""

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_generates_credentials_successfully(
        self, mock_get_ws, service, instance_name, mock_credential
    ):
        """generate_credentials should call workspace client and return cred object."""
        mock_ws = MagicMock()
        mock_ws.database.generate_database_credential.return_value = mock_credential
        mock_get_ws.return_value = mock_ws

        result = await service.generate_credentials(instance_name)

        assert result is mock_credential
        mock_ws.database.generate_database_credential.assert_called_once()
        call_kwargs = mock_ws.database.generate_database_credential.call_args
        assert call_kwargs.kwargs["instance_names"] == [instance_name]

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_generates_credentials_propagates_exception(
        self, mock_get_ws, service, instance_name
    ):
        """generate_credentials should propagate exceptions from workspace client."""
        mock_ws = MagicMock()
        mock_ws.database.generate_database_credential.side_effect = RuntimeError("API error")
        mock_get_ws.return_value = mock_ws

        with pytest.raises(RuntimeError, match="API error"):
            await service.generate_credentials(instance_name)


# ---------------------------------------------------------------------------
# Test class: test_connections_async
# ---------------------------------------------------------------------------

class TestTestConnectionsAsync:
    """Tests for the test_connections_async method."""

    def _make_async_engine_mock(self, succeed: bool, current_user: str = "admin"):
        """
        Build a mock async engine whose connect() context manager either
        succeeds (returning current_user, version) or raises an exception.
        """
        engine = MagicMock()
        conn = AsyncMock()
        if succeed:
            row = (current_user, "PostgreSQL 15.0 (Databricks)")
            result = MagicMock()
            result.fetchone.return_value = row
            conn.execute = AsyncMock(return_value=result)
        else:
            conn.execute = AsyncMock(side_effect=Exception("connection refused"))

        # async context manager: __aenter__ returns conn, __aexit__ does nothing
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=conn)
        ctx.__aexit__ = AsyncMock(return_value=False)
        engine.connect.return_value = ctx
        engine.dispose = AsyncMock()
        return engine

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_succeeds_on_first_approach(
        self, mock_create_engine, service, endpoint, mock_credential
    ):
        """test_connections_async should return engine+user when first attempt succeeds."""
        good_engine = self._make_async_engine_mock(succeed=True, current_user="token_user")
        mock_create_engine.return_value = good_engine

        engine, user = await service.test_connections_async(endpoint, mock_credential)

        assert engine is good_engine
        assert user == "token_user"
        # Should have been called once (first attempt succeeded, no further attempts)
        assert mock_create_engine.call_count == 1

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_falls_back_to_second_approach(
        self, mock_create_engine, service, endpoint, mock_credential
    ):
        """test_connections_async should try the second approach when the first fails."""
        bad_engine = self._make_async_engine_mock(succeed=False)
        good_engine = self._make_async_engine_mock(succeed=True, current_user="admin")

        mock_create_engine.side_effect = [bad_engine, good_engine]

        engine, user = await service.test_connections_async(endpoint, mock_credential)

        assert engine is good_engine
        assert user == "admin"
        assert mock_create_engine.call_count == 2
        # Verify the first failed engine was disposed
        bad_engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_falls_back_to_third_approach(
        self, mock_create_engine, service, endpoint, mock_credential
    ):
        """test_connections_async should try the third approach when first two fail."""
        bad1 = self._make_async_engine_mock(succeed=False)
        bad2 = self._make_async_engine_mock(succeed=False)
        good = self._make_async_engine_mock(succeed=True, current_user="postgres")

        mock_create_engine.side_effect = [bad1, bad2, good]

        engine, user = await service.test_connections_async(endpoint, mock_credential)

        assert engine is good
        assert user == "postgres"
        assert mock_create_engine.call_count == 3

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_returns_none_none_when_all_fail(
        self, mock_create_engine, service, endpoint, mock_credential
    ):
        """test_connections_async should return (None, None) when all approaches fail."""
        bad1 = self._make_async_engine_mock(succeed=False)
        bad2 = self._make_async_engine_mock(succeed=False)
        bad3 = self._make_async_engine_mock(succeed=False)

        mock_create_engine.side_effect = [bad1, bad2, bad3]

        engine, user = await service.test_connections_async(endpoint, mock_credential)

        assert engine is None
        assert user is None
        # All three failed engines should have been disposed
        bad1.dispose.assert_awaited_once()
        bad2.dispose.assert_awaited_once()
        bad3.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_connection_url_contains_endpoint_and_token(
        self, mock_create_engine, service, endpoint, mock_credential
    ):
        """test_connections_async should build URLs containing the endpoint and token."""
        good_engine = self._make_async_engine_mock(succeed=True, current_user="user1")
        mock_create_engine.return_value = good_engine

        await service.test_connections_async(endpoint, mock_credential)

        call_args = mock_create_engine.call_args
        url = call_args[0][0]
        assert endpoint in url
        assert mock_credential.token in url
        assert "asyncpg" in url


# ---------------------------------------------------------------------------
# Test class: test_connections_sync
# ---------------------------------------------------------------------------

class TestTestConnectionsSync:
    """Tests for the test_connections_sync method."""

    def _make_sync_engine_mock(self, succeed: bool, current_user: str = "admin"):
        """
        Build a mock sync engine whose connect() context manager either
        succeeds (returning current_user) or raises an exception.
        """
        engine = MagicMock()
        conn = MagicMock()
        if succeed:
            result = MagicMock()
            result.scalar.return_value = current_user
            conn.execute.return_value = result
        else:
            conn.execute.side_effect = Exception("connection refused")

        # sync context manager
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = conn
        engine.dispose = MagicMock()
        return engine

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_engine")
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_succeeds_with_workspace_identity(
        self, mock_get_ws_util, mock_create_engine, service, endpoint, mock_credential
    ):
        """test_connections_sync should succeed when workspace identity connects."""
        # Mock the workspace identity resolution inside the method
        mock_ws_inner = MagicMock()
        mock_current_user_obj = MagicMock()
        mock_current_user_obj.user_name = "spn-user@example.com"
        mock_current_user_obj.application_id = None
        mock_current_user_obj.display_name = None
        mock_ws_inner.current_user.me.return_value = mock_current_user_obj
        mock_get_ws_util.return_value = mock_ws_inner

        good_engine = self._make_sync_engine_mock(succeed=True, current_user="spn-user@example.com")
        mock_create_engine.return_value = good_engine

        engine, user = service.test_connections_sync(endpoint, mock_credential)

        assert engine is good_engine
        assert user == "spn-user@example.com"

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_engine")
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_falls_back_to_user_email(
        self, mock_get_ws_util, mock_create_engine, service, endpoint, mock_credential
    ):
        """test_connections_sync should fall back to user_email when workspace identity fails."""
        # Workspace identity resolution fails
        mock_get_ws_util.return_value = None

        # First fallback attempts fail, user_email attempt succeeds
        bad_engine = self._make_sync_engine_mock(succeed=False)
        good_engine = self._make_sync_engine_mock(succeed=True, current_user="testuser@example.com")

        # The order: user_email_obo attempt succeeds
        # When workspace identity returns None, the first attempt is user_email_obo
        mock_create_engine.side_effect = [good_engine]

        engine, user = service.test_connections_sync(endpoint, mock_credential)

        assert engine is good_engine
        assert user == "testuser@example.com"

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_engine")
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_returns_none_none_when_all_fail(
        self, mock_get_ws_util, mock_create_engine, service, endpoint, mock_credential
    ):
        """test_connections_sync should return (None, None) when all approaches fail."""
        # Workspace identity resolution fails
        mock_get_ws_util.return_value = None

        # user_email_obo + 3 fallbacks = 4 attempts all fail
        bad_engines = [self._make_sync_engine_mock(succeed=False) for _ in range(4)]
        mock_create_engine.side_effect = bad_engines

        engine, user = service.test_connections_sync(endpoint, mock_credential)

        assert engine is None
        assert user is None

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_engine")
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_no_user_email_skips_obo_attempt(
        self, mock_get_ws_util, mock_create_engine, service_no_email, endpoint, mock_credential
    ):
        """test_connections_sync should skip the user_email_obo attempt when email is None."""
        mock_get_ws_util.return_value = None

        good_engine = self._make_sync_engine_mock(succeed=True, current_user="databricks_superuser")
        mock_create_engine.return_value = good_engine

        engine, user = service_no_email.test_connections_sync(endpoint, mock_credential)

        assert engine is good_engine
        assert user == "databricks_superuser"
        # Verify that the first call was for databricks_superuser (not user_email_obo)
        first_call_url = mock_create_engine.call_args_list[0][0][0]
        assert "databricks_superuser" in first_call_url

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_engine")
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_connection_url_uses_pg8000_driver(
        self, mock_get_ws_util, mock_create_engine, service, endpoint, mock_credential
    ):
        """test_connections_sync should build URLs using the pg8000 driver."""
        mock_get_ws_util.return_value = None

        good_engine = self._make_sync_engine_mock(succeed=True, current_user="test")
        mock_create_engine.return_value = good_engine

        service.test_connections_sync(endpoint, mock_credential)

        first_call_url = mock_create_engine.call_args_list[0][0][0]
        assert "pg8000" in first_call_url

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_engine")
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_disposes_failed_engines(
        self, mock_get_ws_util, mock_create_engine, service_no_email, endpoint, mock_credential
    ):
        """test_connections_sync should dispose engines that fail connection."""
        mock_get_ws_util.return_value = None

        bad1 = self._make_sync_engine_mock(succeed=False)
        bad2 = self._make_sync_engine_mock(succeed=False)
        good = self._make_sync_engine_mock(succeed=True, current_user="admin")

        mock_create_engine.side_effect = [bad1, bad2, good]

        engine, user = service_no_email.test_connections_sync(endpoint, mock_credential)

        assert engine is good
        bad1.dispose.assert_called_once()
        bad2.dispose.assert_called_once()


# ---------------------------------------------------------------------------
# Test class: create_lakebase_engine_async
# ---------------------------------------------------------------------------

class TestCreateLakebaseEngineAsync:
    """Tests for the create_lakebase_engine_async method."""

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_creates_engine_with_correct_url(
        self, mock_create_engine, service, endpoint
    ):
        """create_lakebase_engine_async should build the correct asyncpg connection URL."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        result = await service.create_lakebase_engine_async(
            endpoint=endpoint,
            username="testuser",
            token="testtoken"
        )

        assert result is mock_engine
        call_args = mock_create_engine.call_args
        url = call_args[0][0]
        assert url == f"postgresql+asyncpg://testuser:testtoken@{endpoint}:5432/databricks_postgres"

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_creates_engine_with_ssl_require(
        self, mock_create_engine, service, endpoint
    ):
        """create_lakebase_engine_async should configure SSL as 'require'."""
        mock_create_engine.return_value = MagicMock()

        await service.create_lakebase_engine_async(
            endpoint=endpoint, username="u", token="t"
        )

        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["connect_args"]["ssl"] == "require"

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_creates_engine_with_jit_off(
        self, mock_create_engine, service, endpoint
    ):
        """create_lakebase_engine_async should disable JIT in server_settings."""
        mock_create_engine.return_value = MagicMock()

        await service.create_lakebase_engine_async(
            endpoint=endpoint, username="u", token="t"
        )

        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["connect_args"]["server_settings"]["jit"] == "off"

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_creates_engine_with_echo_false(
        self, mock_create_engine, service, endpoint
    ):
        """create_lakebase_engine_async should set echo=False."""
        mock_create_engine.return_value = MagicMock()

        await service.create_lakebase_engine_async(
            endpoint=endpoint, username="u", token="t"
        )

        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["echo"] is False


# ---------------------------------------------------------------------------
# Test class: create_lakebase_engine_sync
# ---------------------------------------------------------------------------

class TestCreateLakebaseEngineSync:
    """Tests for the create_lakebase_engine_sync method."""

    @patch("src.services.lakebase_connection_service.create_engine")
    def test_creates_engine_with_correct_url(
        self, mock_create_engine, service, endpoint
    ):
        """create_lakebase_engine_sync should build the correct pg8000 connection URL."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        result = service.create_lakebase_engine_sync(
            endpoint=endpoint,
            username="syncuser",
            token="synctoken"
        )

        assert result is mock_engine
        call_args = mock_create_engine.call_args
        url = call_args[0][0]
        assert url == f"postgresql+pg8000://syncuser:synctoken@{endpoint}:5432/databricks_postgres"

    @patch("src.services.lakebase_connection_service.create_engine")
    def test_creates_engine_with_ssl_context_true(
        self, mock_create_engine, service, endpoint
    ):
        """create_lakebase_engine_sync should set ssl_context=True for pg8000."""
        mock_create_engine.return_value = MagicMock()

        service.create_lakebase_engine_sync(
            endpoint=endpoint, username="u", token="t"
        )

        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["connect_args"]["ssl_context"] is True

    @patch("src.services.lakebase_connection_service.create_engine")
    def test_creates_engine_with_null_pool(
        self, mock_create_engine, service, endpoint
    ):
        """create_lakebase_engine_sync should use NullPool."""
        mock_create_engine.return_value = MagicMock()

        service.create_lakebase_engine_sync(
            endpoint=endpoint, username="u", token="t"
        )

        from sqlalchemy.pool import NullPool
        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["poolclass"] is NullPool

    @patch("src.services.lakebase_connection_service.create_engine")
    def test_creates_engine_with_echo_false(
        self, mock_create_engine, service, endpoint
    ):
        """create_lakebase_engine_sync should set echo=False."""
        mock_create_engine.return_value = MagicMock()

        service.create_lakebase_engine_sync(
            endpoint=endpoint, username="u", token="t"
        )

        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["echo"] is False


# ---------------------------------------------------------------------------
# Test class: get_connected_user_identity
# ---------------------------------------------------------------------------

class TestGetConnectedUserIdentity:
    """Tests for the get_connected_user_identity method."""

    @pytest.mark.asyncio
    async def test_success_path(self, service, instance_name, endpoint, mock_credential):
        """get_connected_user_identity should return (user, engine) on success."""
        mock_engine = MagicMock()
        service.generate_credentials = AsyncMock(return_value=mock_credential)
        service.test_connections_async = AsyncMock(
            return_value=(mock_engine, "connected_admin")
        )

        user, engine = await service.get_connected_user_identity(instance_name, endpoint)

        assert user == "connected_admin"
        assert engine is mock_engine
        service.generate_credentials.assert_awaited_once_with(instance_name)
        service.test_connections_async.assert_awaited_once_with(endpoint, mock_credential)

    @pytest.mark.asyncio
    async def test_raises_when_engine_is_none(self, service, instance_name, endpoint, mock_credential):
        """get_connected_user_identity should raise when engine is None."""
        service.generate_credentials = AsyncMock(return_value=mock_credential)
        service.test_connections_async = AsyncMock(return_value=(None, None))

        with pytest.raises(Exception, match="Failed to connect to Lakebase"):
            await service.get_connected_user_identity(instance_name, endpoint)

    @pytest.mark.asyncio
    async def test_raises_when_connected_user_is_none(
        self, service, instance_name, endpoint, mock_credential
    ):
        """get_connected_user_identity should raise when connected_user is None but engine exists."""
        mock_engine = MagicMock()
        service.generate_credentials = AsyncMock(return_value=mock_credential)
        service.test_connections_async = AsyncMock(return_value=(mock_engine, None))

        with pytest.raises(Exception, match="Failed to connect to Lakebase"):
            await service.get_connected_user_identity(instance_name, endpoint)

    @pytest.mark.asyncio
    async def test_propagates_credential_generation_error(
        self, service, instance_name, endpoint
    ):
        """get_connected_user_identity should propagate errors from generate_credentials."""
        service.generate_credentials = AsyncMock(
            side_effect=RuntimeError("credential API error")
        )

        with pytest.raises(RuntimeError, match="credential API error"):
            await service.get_connected_user_identity(instance_name, endpoint)


# ---------------------------------------------------------------------------
# Test class: resolve_postgresql_user
# ---------------------------------------------------------------------------

class TestResolvePostgresqlUser:
    """Tests for the resolve_postgresql_user method."""

    def test_prefers_connected_user(self, service):
        """resolve_postgresql_user should return connected_user when provided."""
        result = service.resolve_postgresql_user(
            cred_user="cred_admin",
            connected_user="actual_connected_user"
        )
        assert result == "actual_connected_user"

    def test_falls_back_to_cred_user_when_connected_user_is_none(self, service):
        """resolve_postgresql_user should return cred_user when connected_user is None."""
        result = service.resolve_postgresql_user(
            cred_user="cred_admin",
            connected_user=None
        )
        assert result == "cred_admin"

    def test_falls_back_to_cred_user_when_connected_user_is_empty(self, service):
        """resolve_postgresql_user should return cred_user when connected_user is empty string."""
        result = service.resolve_postgresql_user(
            cred_user="cred_admin",
            connected_user=""
        )
        assert result == "cred_admin"

    def test_default_connected_user_is_none(self, service):
        """resolve_postgresql_user should default connected_user to None."""
        result = service.resolve_postgresql_user(cred_user="cred_admin")
        assert result == "cred_admin"

    def test_both_users_equal(self, service):
        """resolve_postgresql_user should work when both users are the same."""
        result = service.resolve_postgresql_user(
            cred_user="same_user",
            connected_user="same_user"
        )
        assert result == "same_user"
