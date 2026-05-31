"""
Unit tests for LakebaseConnectionService.

Tests all connection management, credential generation, engine creation,
and SPN-based username resolution for Databricks Lakebase (PostgreSQL) instances.
"""
import os
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

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

    @pytest.mark.asyncio
    async def test_uses_spn_oauth_when_env_vars_set(self):
        """get_workspace_client should use SPN OAuth when all env vars are present."""
        svc = LakebaseConnectionService()
        env = {
            "DATABRICKS_CLIENT_ID": "spn-id",
            "DATABRICKS_CLIENT_SECRET": "spn-secret",
            "DATABRICKS_HOST": "https://example.com",
        }
        mock_ws = MagicMock()

        with patch.dict("os.environ", env, clear=False), \
             patch("src.services.lakebase_connection_service.WorkspaceClient", return_value=mock_ws) as mock_cls:
            result = await svc.get_workspace_client()

            assert result is mock_ws
            assert svc._workspace_client is mock_ws
            mock_cls.assert_called_once_with(
                host="https://example.com",
                client_id="spn-id",
                client_secret="spn-secret",
            )

    @pytest.mark.asyncio
    async def test_spn_oauth_is_cached(self):
        """SPN workspace client should be cached on subsequent calls."""
        svc = LakebaseConnectionService()
        env = {
            "DATABRICKS_CLIENT_ID": "spn-id",
            "DATABRICKS_CLIENT_SECRET": "spn-secret",
            "DATABRICKS_HOST": "https://example.com",
        }
        mock_ws = MagicMock()

        with patch.dict("os.environ", env, clear=False), \
             patch("src.services.lakebase_connection_service.WorkspaceClient", return_value=mock_ws) as mock_cls:
            first = await svc.get_workspace_client()
            second = await svc.get_workspace_client()

            assert first is second
            mock_cls.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_falls_back_to_pat_when_spn_env_incomplete(self, mock_get_ws):
        """Should fall back to PAT/OBO when SPN env vars are incomplete."""
        mock_ws = MagicMock()
        mock_get_ws.return_value = mock_ws
        svc = LakebaseConnectionService(user_token="tok-abc")

        # Only CLIENT_ID set, no SECRET — should fall back
        with patch.dict("os.environ", {"DATABRICKS_CLIENT_ID": "id"}, clear=True):
            result = await svc.get_workspace_client()

            assert result is mock_ws
            mock_get_ws.assert_awaited_once_with("tok-abc")


# ---------------------------------------------------------------------------
# Test class: get_spn_username
# ---------------------------------------------------------------------------

class TestGetSpnUsername:
    """Tests for the get_spn_username method."""

    def test_returns_client_id_from_env(self, service):
        """get_spn_username should return DATABRICKS_CLIENT_ID from environment."""
        with patch.dict("os.environ", {"DATABRICKS_CLIENT_ID": "84698c5c-6144-44a2-b6d4-4a69c77fc442"}):
            result = service.get_spn_username()
            assert result == "84698c5c-6144-44a2-b6d4-4a69c77fc442"

    def test_returns_none_when_env_not_set(self, service):
        """get_spn_username should return None when DATABRICKS_CLIENT_ID is not set."""
        with patch.dict("os.environ", {}, clear=True):
            result = service.get_spn_username()
            assert result is None


# ---------------------------------------------------------------------------
# Test class: get_username
# ---------------------------------------------------------------------------

class TestGetUsername:
    """Tests for the get_username method."""

    @pytest.mark.asyncio
    async def test_returns_spn_client_id_when_available(self, service):
        """get_username should prefer SPN client_id from environment."""
        with patch.dict("os.environ", {"DATABRICKS_CLIENT_ID": "test-spn-id"}):
            result = await service.get_username()
            assert result == "test-spn-id"

    @pytest.mark.asyncio
    async def test_falls_back_to_user_email(self, service):
        """get_username should fall back to user_email when no SPN client_id."""
        with patch.dict("os.environ", {}, clear=True):
            result = await service.get_username()
            assert result == "testuser@example.com"

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_falls_back_to_workspace_user(self, mock_get_ws, service_no_email):
        """get_username should fall back to workspace current user when no email."""
        with patch.dict("os.environ", {}, clear=True):
            mock_ws = MagicMock()
            mock_user = MagicMock()
            mock_user.user_name = "workspace-user@example.com"
            mock_ws.current_user.me.return_value = mock_user
            mock_get_ws.return_value = mock_ws

            result = await service_no_email.get_username()
            assert result == "workspace-user@example.com"

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.get_workspace_client")
    async def test_raises_when_no_username_available(self, mock_get_ws):
        """get_username should raise ValueError when no username can be determined."""
        svc = LakebaseConnectionService(user_token=None, user_email=None)
        with patch.dict("os.environ", {}, clear=True):
            mock_get_ws.return_value = None

            with pytest.raises(ValueError, match="Cannot determine PostgreSQL username"):
                await svc.get_username()


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
        assert url == f"postgresql+asyncpg://testuser@{endpoint}:5432/databricks_postgres"

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
    async def test_creates_engine_with_pool_pre_ping_false(
        self, mock_create_engine, service, endpoint
    ):
        """create_lakebase_engine_async should set pool_pre_ping=False for do_connect compatibility."""
        mock_create_engine.return_value = MagicMock()

        await service.create_lakebase_engine_async(
            endpoint=endpoint, username="u", token="t"
        )

        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["pool_pre_ping"] is False

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
# Helpers
# ---------------------------------------------------------------------------

def _make_connect_ctx(mock_conn):
    """Async context manager mock for engine.connect()."""
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


def _make_sync_connect_ctx(mock_conn):
    """Sync context manager mock for engine.connect()."""
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=mock_conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx


# ---------------------------------------------------------------------------
# Test class: get_workspace_client SPN PAT backup (line 81)
# ---------------------------------------------------------------------------

class TestGetWorkspaceClientSpnPatBackup:
    """Tests for SPN OAuth path stripping/restoring PAT env vars."""

    @pytest.mark.asyncio
    async def test_spn_strips_and_restores_pat_env_vars(self):
        """SPN path should pop DATABRICKS_TOKEN/API_KEY then restore them."""
        svc = LakebaseConnectionService()
        env = {
            "DATABRICKS_CLIENT_ID": "spn-id",
            "DATABRICKS_CLIENT_SECRET": "spn-secret",
            "DATABRICKS_HOST": "https://example.com",
            "DATABRICKS_TOKEN": "pat-token",
            "DATABRICKS_API_KEY": "api-key",
        }
        captured = {}

        def _fake_ws(**kwargs):
            import os as _os
            # During the SDK call, PAT vars must be removed
            captured["token_present"] = "DATABRICKS_TOKEN" in _os.environ
            captured["api_key_present"] = "DATABRICKS_API_KEY" in _os.environ
            return MagicMock()

        with patch.dict("os.environ", env, clear=True), \
             patch("src.services.lakebase_connection_service.WorkspaceClient", side_effect=_fake_ws):
            result = await svc.get_workspace_client()

            assert result is svc._workspace_client
            assert captured["token_present"] is False
            assert captured["api_key_present"] is False
            # restored afterwards
            assert os.environ["DATABRICKS_TOKEN"] == "pat-token"
            assert os.environ["DATABRICKS_API_KEY"] == "api-key"


# ---------------------------------------------------------------------------
# Test class: test_connection (lines 232-270)
# ---------------------------------------------------------------------------

class TestTestConnection:
    """Tests for the test_connection method."""

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_connection_success(self, mock_create_engine, service, endpoint, instance_name, mock_credential):
        """test_connection returns success dict with connected_user and version."""
        mock_conn = AsyncMock()
        result_row = MagicMock()
        result_row.fetchone.return_value = ("spn-user", "PostgreSQL 15.4")
        mock_conn.execute = AsyncMock(return_value=result_row)

        mock_engine = MagicMock()
        mock_engine.connect = MagicMock(return_value=_make_connect_ctx(mock_conn))
        mock_engine.dispose = AsyncMock()
        mock_create_engine.return_value = mock_engine

        service.get_username = AsyncMock(return_value="spn-user")
        service.generate_credentials = AsyncMock(return_value=mock_credential)

        result = await service.test_connection(endpoint, instance_name)

        assert result["success"] is True
        assert result["connected_user"] == "spn-user"
        assert result["version"] == "PostgreSQL 15.4"
        assert result["username_used"] == "spn-user"
        mock_engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_connection_failure(self, mock_create_engine, service, endpoint, instance_name, mock_credential):
        """test_connection returns failure dict when the connection raises."""
        mock_engine = MagicMock()
        failing_ctx = AsyncMock()
        failing_ctx.__aenter__ = AsyncMock(side_effect=RuntimeError("conn refused"))
        failing_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_engine.connect = MagicMock(return_value=failing_ctx)
        mock_engine.dispose = AsyncMock()
        mock_create_engine.return_value = mock_engine

        service.get_username = AsyncMock(return_value="spn-user")
        service.generate_credentials = AsyncMock(return_value=mock_credential)

        result = await service.test_connection(endpoint, instance_name)

        assert result["success"] is False
        assert "conn refused" in result["error"]
        assert result["username_used"] == "spn-user"
        mock_engine.dispose.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test class: create_engine_with_token_refresh (lines 295-345)
# ---------------------------------------------------------------------------

class TestCreateEngineWithTokenRefresh:
    """Tests for create_engine_with_token_refresh (async + sync)."""

    @pytest.mark.asyncio
    @patch("src.services.lakebase_connection_service.event")
    @patch("src.services.lakebase_connection_service.create_async_engine")
    async def test_async_driver_builds_engine_and_registers_listener(
        self, mock_create_engine, mock_event, service, endpoint
    ):
        """asyncpg driver should build async engine with kasal,public search_path and do_connect listener."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        captured = {}

        def _listens_for(target, name):
            captured["name"] = name

            def _decorator(fn):
                captured["fn"] = fn
                return fn
            return _decorator

        mock_event.listens_for.side_effect = _listens_for

        token_holder = {"token": "tok-123", "refreshed_at": 0.0}
        result = service.create_engine_with_token_refresh(
            endpoint=endpoint, username="u", token_holder=token_holder, driver="asyncpg"
        )

        assert result is mock_engine
        url = mock_create_engine.call_args[0][0]
        assert url == f"postgresql+asyncpg://u@{endpoint}:5432/databricks_postgres"
        ck = mock_create_engine.call_args[1]
        assert ck["connect_args"]["server_settings"]["search_path"] == "kasal, public"
        assert ck["pool_pre_ping"] is False
        # Exercise the registered do_connect listener
        assert captured["name"] == "do_connect"
        cparams = {}
        captured["fn"](None, None, None, cparams)
        assert cparams["password"] == "tok-123"

    @patch("src.services.lakebase_connection_service.event")
    @patch("src.services.lakebase_connection_service.create_engine")
    def test_sync_driver_builds_engine_and_registers_listener(
        self, mock_create_engine, mock_event, service, endpoint
    ):
        """pg8000 driver should build sync engine with do_connect listener."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        captured = {}

        def _listens_for(target, name):
            captured["name"] = name

            def _decorator(fn):
                captured["fn"] = fn
                return fn
            return _decorator

        mock_event.listens_for.side_effect = _listens_for

        token_holder = {"token": "sync-tok", "refreshed_at": 0.0}
        result = service.create_engine_with_token_refresh(
            endpoint=endpoint, username="u", token_holder=token_holder, driver="pg8000"
        )

        assert result is mock_engine
        url = mock_create_engine.call_args[0][0]
        assert url == f"postgresql+pg8000://u:placeholder@{endpoint}:5432/databricks_postgres"
        ck = mock_create_engine.call_args[1]
        assert ck["connect_args"]["ssl_context"] is True
        # Exercise the registered do_connect listener
        assert captured["name"] == "do_connect"
        cparams = {}
        captured["fn"](None, None, None, cparams)
        assert cparams["password"] == "sync-tok"


# ---------------------------------------------------------------------------
# Test class: create_lakebase_engine_sync statement_timeout (lines 424-430)
# ---------------------------------------------------------------------------

class TestCreateLakebaseEngineSyncTimeout:
    """Tests for create_lakebase_engine_sync with statement_timeout_ms."""

    @patch("src.services.lakebase_connection_service.event")
    @patch("src.services.lakebase_connection_service.create_engine")
    def test_statement_timeout_registers_connect_listener(
        self, mock_create_engine, mock_event, service, endpoint
    ):
        """statement_timeout_ms > 0 should register a connect listener that SETs statement_timeout."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        captured = {}

        def _listens_for(target, name):
            captured["name"] = name

            def _decorator(fn):
                captured["fn"] = fn
                return fn
            return _decorator

        mock_event.listens_for.side_effect = _listens_for

        result = service.create_lakebase_engine_sync(
            endpoint=endpoint, username="u", token="t", statement_timeout_ms=5000
        )

        assert result is mock_engine
        assert captured["name"] == "connect"
        # Exercise the listener
        cursor = MagicMock()
        dbapi_conn = MagicMock()
        dbapi_conn.cursor.return_value = cursor
        captured["fn"](dbapi_conn, None)
        cursor.execute.assert_called_once_with("SET statement_timeout = '5000'")
        cursor.close.assert_called_once()


# ---------------------------------------------------------------------------
# Test class: get_connected_engine_async (lines 457-471)
# ---------------------------------------------------------------------------

class TestGetConnectedEngineAsync:
    """Tests for get_connected_engine_async."""

    @pytest.mark.asyncio
    async def test_success(self, service, endpoint, instance_name, mock_credential):
        """get_connected_engine_async returns (username, engine) on success."""
        mock_conn = AsyncMock()
        scalar_result = MagicMock()
        scalar_result.scalar.return_value = "spn-user"
        mock_conn.execute = AsyncMock(return_value=scalar_result)

        mock_engine = MagicMock()
        mock_engine.connect = MagicMock(return_value=_make_connect_ctx(mock_conn))
        mock_engine.dispose = AsyncMock()

        service.get_username = AsyncMock(return_value="spn-user")
        service.generate_credentials = AsyncMock(return_value=mock_credential)
        service.create_lakebase_engine_async = AsyncMock(return_value=mock_engine)

        username, engine = await service.get_connected_engine_async(instance_name, endpoint)

        assert username == "spn-user"
        assert engine is mock_engine
        mock_engine.dispose.assert_not_called()

    @pytest.mark.asyncio
    async def test_failure_disposes_and_raises(self, service, endpoint, instance_name, mock_credential):
        """get_connected_engine_async disposes engine and raises on connect failure."""
        mock_engine = MagicMock()
        failing_ctx = AsyncMock()
        failing_ctx.__aenter__ = AsyncMock(side_effect=RuntimeError("boom"))
        failing_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_engine.connect = MagicMock(return_value=failing_ctx)
        mock_engine.dispose = AsyncMock()

        service.get_username = AsyncMock(return_value="spn-user")
        service.generate_credentials = AsyncMock(return_value=mock_credential)
        service.create_lakebase_engine_async = AsyncMock(return_value=mock_engine)

        with pytest.raises(Exception, match="Failed to connect to Lakebase"):
            await service.get_connected_engine_async(instance_name, endpoint)

        mock_engine.dispose.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test class: get_connected_engine_sync (lines 493-504)
# ---------------------------------------------------------------------------

class TestGetConnectedEngineSync:
    """Tests for get_connected_engine_sync."""

    def test_success(self, service, endpoint):
        """get_connected_engine_sync returns (engine, connected_user) on success."""
        mock_conn = MagicMock()
        scalar_result = MagicMock()
        scalar_result.scalar.return_value = "spn-user"
        mock_conn.execute = MagicMock(return_value=scalar_result)

        mock_engine = MagicMock()
        mock_engine.connect = MagicMock(return_value=_make_sync_connect_ctx(mock_conn))

        service.create_lakebase_engine_sync = MagicMock(return_value=mock_engine)

        engine, connected_user = service.get_connected_engine_sync(endpoint, "u", "t")

        assert engine is mock_engine
        assert connected_user == "spn-user"
        mock_engine.dispose.assert_not_called()

    def test_failure_disposes_and_raises(self, service, endpoint):
        """get_connected_engine_sync disposes engine and raises on connect failure."""
        mock_engine = MagicMock()
        failing_ctx = MagicMock()
        failing_ctx.__enter__ = MagicMock(side_effect=RuntimeError("boom"))
        failing_ctx.__exit__ = MagicMock(return_value=False)
        mock_engine.connect = MagicMock(return_value=failing_ctx)

        service.create_lakebase_engine_sync = MagicMock(return_value=mock_engine)

        with pytest.raises(Exception, match=r"\[SYNC\] Failed to connect to Lakebase"):
            service.get_connected_engine_sync(endpoint, "u", "t")

        mock_engine.dispose.assert_called_once()


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
