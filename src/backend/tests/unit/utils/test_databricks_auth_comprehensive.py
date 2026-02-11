import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock, PropertyMock
from types import SimpleNamespace
import os
import time
import json
import subprocess
import asyncio
import httpx
from typing import Dict, Any, Optional, Tuple
from contextlib import contextmanager

from src.utils.databricks_auth import (
    AuthContext,
    DatabricksAuth,
    extract_user_token_from_request,
    is_scope_error,
    setup_environment_variables,
    _clean_environment,
    get_databricks_auth_headers_sync,
    validate_databricks_connection,
    get_databricks_auth_headers,
    get_auth_context,
    get_workspace_client,
    get_workspace_client_with_fallback,
    get_mcp_access_token,
    get_current_databricks_user,
    get_mcp_auth_headers,
    _databricks_auth,
)


def _make_auth(**kwargs):
    auth = DatabricksAuth()
    for k, v in kwargs.items():
        setattr(auth, f"_{k}", v)
    return auth


# ── AuthContext ────────────────────────────────────────

class TestAuthContext:
    def test_init_basic(self):
        ctx = AuthContext(token="t", workspace_url="https://host.com", auth_method="pat")
        assert ctx.token == "t"
        assert ctx.workspace_url == "https://host.com"
        assert ctx.auth_method == "pat"
        assert ctx.user_identity is None

    def test_init_with_user_identity(self):
        ctx = AuthContext(token="t", workspace_url="https://h.com", auth_method="obo", user_identity="u@x.com")
        assert ctx.user_identity == "u@x.com"

    def test_url_normalization_no_https(self):
        ctx = AuthContext(token="t", workspace_url="host.com/", auth_method="pat")
        assert ctx.workspace_url == "https://host.com"

    def test_url_strips_trailing_slash(self):
        ctx = AuthContext(token="t", workspace_url="https://host.com/", auth_method="pat")
        assert ctx.workspace_url == "https://host.com"

    def test_get_headers(self):
        ctx = AuthContext(token="tok", workspace_url="https://h.com", auth_method="pat")
        h = ctx.get_headers()
        assert h["Authorization"] == "Bearer tok"
        assert h["Content-Type"] == "application/json"

    def test_get_mcp_headers_no_sse(self):
        ctx = AuthContext(token="tok", workspace_url="https://h.com", auth_method="pat")
        h = ctx.get_mcp_headers()
        assert "Accept" not in h

    def test_get_mcp_headers_with_sse(self):
        ctx = AuthContext(token="tok", workspace_url="https://h.com", auth_method="pat")
        h = ctx.get_mcp_headers(include_sse=True)
        assert h["Accept"] == "text/event-stream"
        assert h["Cache-Control"] == "no-cache"
        assert h["Connection"] == "keep-alive"

    def test_get_litellm_params(self):
        ctx = AuthContext(token="tok", workspace_url="https://h.com", auth_method="pat")
        p = ctx.get_litellm_params()
        assert p["api_key"] == "tok"
        assert p["api_base"] == "https://h.com/serving-endpoints"

    @patch("src.utils.databricks_auth.WorkspaceClient")
    def test_get_workspace_client_creates_client(self, mock_wc):
        mock_wc.return_value = MagicMock()
        ctx = AuthContext(token="tok", workspace_url="https://h.com", auth_method="pat")
        client = ctx.get_workspace_client()
        mock_wc.assert_called_once_with(host="https://h.com", token="tok")
        assert client is mock_wc.return_value

    def test_repr_with_identity(self):
        ctx = AuthContext(token="t", workspace_url="https://h.com", auth_method="obo", user_identity="u@x.com")
        r = repr(ctx)
        assert "obo" in r and "u@x.com" in r

    def test_repr_service(self):
        ctx = AuthContext(token="t", workspace_url="https://h.com", auth_method="service_principal")
        assert "service" in repr(ctx)


# ── _clean_environment ─────────────────────────────────

class TestCleanEnvironment:
    def test_cleans_and_restores(self):
        os.environ["DATABRICKS_TOKEN"] = "secret"
        os.environ["DATABRICKS_CLIENT_ID"] = "cid"
        with _clean_environment():
            assert "DATABRICKS_TOKEN" not in os.environ
            assert "DATABRICKS_CLIENT_ID" not in os.environ
        assert os.environ["DATABRICKS_TOKEN"] == "secret"
        assert os.environ["DATABRICKS_CLIENT_ID"] == "cid"
        os.environ.pop("DATABRICKS_TOKEN", None)
        os.environ.pop("DATABRICKS_CLIENT_ID", None)

    def test_restores_on_exception(self):
        os.environ["DATABRICKS_TOKEN"] = "val"
        try:
            with _clean_environment():
                assert "DATABRICKS_TOKEN" not in os.environ
                raise ValueError("boom")
        except ValueError:
            pass
        assert os.environ["DATABRICKS_TOKEN"] == "val"
        os.environ.pop("DATABRICKS_TOKEN", None)

    def test_no_vars_set(self):
        for v in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY", "DATABRICKS_CLIENT_ID",
                   "DATABRICKS_CLIENT_SECRET", "DATABRICKS_CONFIG_FILE", "DATABRICKS_CONFIG_PROFILE"]:
            os.environ.pop(v, None)
        with _clean_environment():
            pass


# ── _load_config ───────────────────────────────────────

class TestLoadConfig:
    @pytest.mark.asyncio
    async def test_already_loaded(self):
        auth = DatabricksAuth()
        auth._config_loaded = True
        assert await auth._load_config() is True

    @pytest.mark.asyncio
    async def test_db_config_sets_host_with_https_prefix(self):
        auth = DatabricksAuth()
        mock_config = MagicMock()
        mock_config.workspace_url = "myhost.databricks.com"
        mock_service = AsyncMock()
        mock_service.get_databricks_config = AsyncMock(return_value=mock_config)
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        with patch.object(auth, "_check_oauth_environment"), \
             patch("src.services.databricks_service.DatabricksService", return_value=mock_service), \
             patch("src.db.session.async_session_factory", return_value=mock_session):
            result = await auth._load_config()
        assert result is True
        assert auth._workspace_host == "https://myhost.databricks.com"

    @pytest.mark.asyncio
    async def test_db_config_get_raises(self):
        auth = DatabricksAuth()
        mock_service = AsyncMock()
        mock_service.get_databricks_config = AsyncMock(side_effect=Exception("db err"))
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        with patch.object(auth, "_check_oauth_environment"), \
             patch("src.services.databricks_service.DatabricksService", return_value=mock_service), \
             patch("src.db.session.async_session_factory", return_value=mock_session):
            result = await auth._load_config()
        assert result is True

    @pytest.mark.asyncio
    async def test_db_import_raises(self):
        auth = DatabricksAuth()
        with patch.object(auth, "_check_oauth_environment"):
            original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__
            def fail_import(name, *args, **kwargs):
                if name == "src.services.databricks_service":
                    raise ImportError("no module")
                return original_import(name, *args, **kwargs)
            with patch("builtins.__import__", side_effect=fail_import):
                result = await auth._load_config()
        assert result is True

    @pytest.mark.asyncio
    async def test_sdk_autodetect_sets_host(self):
        auth = DatabricksAuth()
        mock_sdk_config = MagicMock()
        mock_sdk_config.host = "https://sdk-detected.databricks.com"
        with patch.object(auth, "_check_oauth_environment"), \
             patch("src.utils.databricks_auth.Config", return_value=mock_sdk_config):
            def fail_import(name, *args, **kwargs):
                if "databricks_service" in name or "session" in name:
                    raise ImportError("no module")
                return __import__(name, *args, **kwargs)
            with patch("builtins.__import__", side_effect=fail_import):
                result = await auth._load_config()
        assert result is True
        assert auth._workspace_host == "https://sdk-detected.databricks.com"

    @pytest.mark.asyncio
    async def test_sdk_autodetect_fails(self):
        auth = DatabricksAuth()
        with patch.object(auth, "_check_oauth_environment"), \
             patch("src.utils.databricks_auth.Config", side_effect=Exception("sdk fail")):
            def fail_import(name, *args, **kwargs):
                if "databricks_service" in name or "session" in name:
                    raise ImportError("no module")
                return __import__(name, *args, **kwargs)
            with patch("builtins.__import__", side_effect=fail_import):
                result = await auth._load_config()
        assert result is True

    @pytest.mark.asyncio
    async def test_load_config_outer_exception(self):
        auth = DatabricksAuth()
        with patch.object(auth, "_check_oauth_environment", side_effect=Exception("boom")):
            result = await auth._load_config()
        assert result is False


# ── _check_oauth_environment ───────────────────────────

class TestCheckOauthEnvironment:
    def test_sets_client_credentials(self):
        auth = DatabricksAuth()
        with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "cid", "DATABRICKS_CLIENT_SECRET": "csec"}, clear=False):
            auth._check_oauth_environment()
        assert auth._client_id == "cid"
        assert auth._client_secret == "csec"

    def test_sets_host_from_env_without_https(self):
        auth = DatabricksAuth()
        with patch.dict(os.environ, {"DATABRICKS_HOST": "myhost.com/"}, clear=False):
            auth._check_oauth_environment()
        assert auth._workspace_host == "https://myhost.com"

    def test_sets_host_from_env_with_https(self):
        auth = DatabricksAuth()
        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://myhost.com/"}, clear=False):
            auth._check_oauth_environment()
        assert auth._workspace_host == "https://myhost.com"

    def test_exception_in_check(self):
        auth = DatabricksAuth()
        with patch("os.environ.get", side_effect=Exception("env error")):
            auth._check_oauth_environment()


# ── _is_service_token_expired ──────────────────────────

class TestIsServiceTokenExpired:
    def test_no_token(self):
        assert _make_auth(service_token=None, service_token_fetched_at=None)._is_service_token_expired() is True

    def test_no_fetched_at(self):
        assert _make_auth(service_token="t", service_token_fetched_at=None)._is_service_token_expired() is True

    def test_not_expired(self):
        auth = _make_auth(service_token="t", service_token_fetched_at=time.time(),
                          service_token_expires_in=3600, token_refresh_buffer=300)
        assert auth._is_service_token_expired() is False

    def test_expired(self):
        auth = _make_auth(service_token="t", service_token_fetched_at=time.time() - 4000,
                          service_token_expires_in=3600, token_refresh_buffer=300)
        assert auth._is_service_token_expired() is True


# ── _refresh_service_token ─────────────────────────────

class TestRefreshServiceToken:
    @pytest.mark.asyncio
    async def test_success(self):
        auth = _make_auth(client_id="c", client_secret="s", workspace_host="https://h.com")
        with patch.object(auth, "_get_service_principal_token", new_callable=AsyncMock, return_value="new_tok"):
            result = await auth._refresh_service_token()
        assert result == "new_tok"
        assert auth._service_token == "new_tok"

    @pytest.mark.asyncio
    async def test_returns_none(self):
        auth = _make_auth(client_id="c", client_secret="s")
        with patch.object(auth, "_get_service_principal_token", new_callable=AsyncMock, return_value=None):
            assert await auth._refresh_service_token() is None

    @pytest.mark.asyncio
    async def test_raises(self):
        auth = _make_auth(client_id="c", client_secret="s")
        with patch.object(auth, "_get_service_principal_token", new_callable=AsyncMock, side_effect=Exception("fail")):
            assert await auth._refresh_service_token() is None


# ── get_auth_headers / _get_unified_auth_headers ───────

class TestGetAuthHeaders:
    @pytest.mark.asyncio
    async def test_load_config_fails(self):
        auth = _make_auth(config_loaded=False)
        with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=False):
            headers, err = await auth.get_auth_headers()
        assert headers is None and "Failed to load" in err

    @pytest.mark.asyncio
    async def test_exception(self):
        auth = _make_auth(config_loaded=False)
        with patch.object(auth, "_load_config", new_callable=AsyncMock, side_effect=Exception("boom")):
            headers, err = await auth.get_auth_headers()
        assert headers is None and "boom" in err

    @pytest.mark.asyncio
    async def test_user_token_sets_access_token(self):
        """Line 484: user_token param triggers set_user_access_token."""
        auth = _make_auth(config_loaded=True, user_access_token=None,
                          api_token=None, client_id=None, client_secret=None)
        with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True):
            headers, err = await auth.get_auth_headers(user_token="ut")
        # After set_user_access_token, OBO path returns bearer headers
        assert headers is not None
        assert headers["Authorization"] == "Bearer ut"

    @pytest.mark.asyncio
    async def test_pat_valid(self):
        auth = _make_auth(config_loaded=True, api_token="pat_tok", workspace_host="https://h.com",
                          user_access_token=None, client_id=None, client_secret=None)
        with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True), \
             patch.object(auth, "_validate_token", new_callable=AsyncMock, return_value=True):
            headers, err = await auth.get_auth_headers()
        assert err is None and headers["Authorization"] == "Bearer pat_tok"

    @pytest.mark.asyncio
    async def test_pat_invalid(self):
        auth = _make_auth(config_loaded=True, api_token="bad", workspace_host="https://h.com",
                          user_access_token=None, client_id=None, client_secret=None)
        with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True), \
             patch.object(auth, "_validate_token", new_callable=AsyncMock, return_value=False):
            headers, err = await auth.get_auth_headers()
        assert headers is None

    @pytest.mark.asyncio
    async def test_spn_expired_refresh_success(self):
        auth = _make_auth(config_loaded=True, api_token=None, user_access_token=None,
                          client_id="c", client_secret="s", service_token=None, service_token_fetched_at=None)
        with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True), \
             patch.object(auth, "_is_service_token_expired", return_value=True), \
             patch.object(auth, "_refresh_service_token", new_callable=AsyncMock, return_value="new"):
            headers, err = await auth.get_auth_headers()
        assert err is None and headers["Authorization"] == "Bearer new"

    @pytest.mark.asyncio
    async def test_spn_expired_refresh_fails(self):
        auth = _make_auth(config_loaded=True, api_token=None, user_access_token=None,
                          client_id="c", client_secret="s")
        with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True), \
             patch.object(auth, "_is_service_token_expired", return_value=True), \
             patch.object(auth, "_refresh_service_token", new_callable=AsyncMock, return_value=None):
            headers, err = await auth.get_auth_headers()
        assert headers is None

    @pytest.mark.asyncio
    async def test_spn_cached(self):
        auth = _make_auth(config_loaded=True, api_token=None, user_access_token=None,
                          client_id="c", client_secret="s", service_token="cached",
                          service_token_fetched_at=time.time(), service_token_expires_in=3600,
                          token_refresh_buffer=300)
        with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True), \
             patch.object(auth, "_is_service_token_expired", return_value=False):
            headers, err = await auth.get_auth_headers()
        assert err is None and headers["Authorization"] == "Bearer cached"

    @pytest.mark.asyncio
    async def test_unified_exception(self):
        auth = _make_auth(config_loaded=True, user_access_token="tok",
                          api_token=None, client_id=None, client_secret=None)
        with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True), \
             patch.object(auth, "_create_bearer_headers", side_effect=Exception("unexp")):
            headers, err = await auth.get_auth_headers()
        assert headers is None and "unexp" in err


# ── _get_service_principal_token ───────────────────────

class TestGetServicePrincipalToken:
    @pytest.mark.asyncio
    async def test_no_credentials(self):
        assert await _make_auth(client_id=None, client_secret=None)._get_service_principal_token() is None

    @pytest.mark.asyncio
    async def test_sdk_with_access_token_and_expires(self):
        auth = _make_auth(client_id="c", client_secret="s", workspace_host="https://h.com")
        mock_result = MagicMock()
        mock_result.access_token = "sdk_tok"
        mock_result.expires_in = 7200
        mock_config = MagicMock()
        mock_config.authenticate.return_value = mock_result
        with patch("src.utils.databricks_auth.Config", return_value=mock_config):
            assert await auth._get_service_principal_token() == "sdk_tok"
        assert auth._service_token_expires_in == 7200

    @pytest.mark.asyncio
    async def test_sdk_no_expires_in(self):
        auth = _make_auth(client_id="c", client_secret="s", workspace_host="https://h.com")
        mock_result = MagicMock(spec=["access_token"])
        mock_result.access_token = "tok"
        mock_config = MagicMock()
        mock_config.authenticate.return_value = mock_result
        with patch("src.utils.databricks_auth.Config", return_value=mock_config):
            assert await auth._get_service_principal_token() == "tok"

    @pytest.mark.asyncio
    async def test_sdk_no_access_token(self):
        auth = _make_auth(client_id="c", client_secret="s", workspace_host="https://h.com")
        mock_result = MagicMock(spec=[])
        mock_config = MagicMock()
        mock_config.authenticate.return_value = mock_result
        with patch("src.utils.databricks_auth.Config", return_value=mock_config):
            assert await auth._get_service_principal_token() is None

    @pytest.mark.asyncio
    async def test_sdk_fails_manual_fallback(self):
        auth = _make_auth(client_id="c", client_secret="s", workspace_host="https://h.com")
        with patch("src.utils.databricks_auth.Config", side_effect=Exception("sdk fail")), \
             patch.object(auth, "_manual_oauth_flow", new_callable=AsyncMock, return_value="manual"):
            assert await auth._get_service_principal_token() == "manual"

    @pytest.mark.asyncio
    async def test_outer_exception(self):
        auth = _make_auth(client_id="c", client_secret="s", workspace_host="https://h.com")
        with patch("src.utils.databricks_auth.Config", side_effect=Exception("sdk")), \
             patch.object(auth, "_manual_oauth_flow", new_callable=AsyncMock, side_effect=Exception("manual")):
            assert await auth._get_service_principal_token() is None


# ── Simple accessors ───────────────────────────────────

class TestSimpleAccessors:
    def test_get_workspace_host(self):
        assert _make_auth(workspace_host="https://h.com").get_workspace_host() == "https://h.com"

    def test_get_workspace_host_none(self):
        assert _make_auth(workspace_host=None).get_workspace_host() is None

    @pytest.mark.asyncio
    async def test_get_workspace_url_loads_config(self):
        auth = _make_auth(config_loaded=False, workspace_host="https://h.com")
        with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True):
            assert await auth.get_workspace_url() == "https://h.com"

    @pytest.mark.asyncio
    async def test_get_workspace_url_already_loaded(self):
        assert await _make_auth(config_loaded=True, workspace_host="https://h.com").get_workspace_url() == "https://h.com"

    def test_get_api_token(self):
        assert _make_auth(api_token="tok").get_api_token() == "tok"

    def test_get_api_token_none(self):
        assert _make_auth(api_token=None).get_api_token() is None


# ── validate_databricks_connection ─────────────────────

class TestValidateDatabricksConnection:
    @pytest.mark.asyncio
    async def test_config_load_fails(self):
        with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=False):
            ok, err = await validate_databricks_connection()
        assert ok is False and "Failed to load" in err

    @pytest.mark.asyncio
    async def test_token_valid(self):
        with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
             patch.object(_databricks_auth, "_validate_token", new_callable=AsyncMock, return_value=True):
            ok, err = await validate_databricks_connection()
        assert ok is True and err is None

    @pytest.mark.asyncio
    async def test_token_invalid(self):
        with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
             patch.object(_databricks_auth, "_validate_token", new_callable=AsyncMock, return_value=False):
            ok, err = await validate_databricks_connection()
        assert ok is False and "validation failed" in err

    @pytest.mark.asyncio
    async def test_exception(self):
        with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, side_effect=Exception("conn err")):
            ok, err = await validate_databricks_connection()
        assert ok is False and "conn err" in err


# ── get_databricks_auth_headers_sync ───────────────────

class TestGetDatabricksAuthHeadersSync:
    def test_basic_call(self):
        result = get_databricks_auth_headers_sync()
        assert isinstance(result, tuple) and len(result) == 2

    def test_from_async_context(self):
        async def _inner():
            return get_databricks_auth_headers_sync()
        loop = asyncio.new_event_loop()
        try:
            headers, err = loop.run_until_complete(_inner())
            assert headers is None
            assert "Cannot call sync version from async context" in err
        finally:
            loop.close()

    def test_exception_path(self):
        """Lines 760-762: outer exception."""
        with patch("asyncio.get_running_loop", side_effect=RuntimeError), \
             patch("asyncio.run", side_effect=Exception("async boom")):
            headers, err = get_databricks_auth_headers_sync()
        assert headers is None
        assert "async boom" in err


# ── setup_environment_variables ────────────────────────

class TestSetupEnvironmentVariables:
    def test_config_load_fails(self):
        import warnings
        with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=False):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                assert setup_environment_variables(user_token="tok") is False

    def test_set_access_token_fails(self):
        import warnings
        orig_host = _databricks_auth._workspace_host
        orig_token = _databricks_auth._api_token
        try:
            _databricks_auth._workspace_host = "https://h.com"
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch.object(_databricks_auth, "set_user_access_token", side_effect=Exception("oops")):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    result = setup_environment_variables(user_token="tok")
            assert result is True
            assert os.environ.get("DATABRICKS_TOKEN") == "tok"
        finally:
            _databricks_auth._workspace_host = orig_host
            _databricks_auth._api_token = orig_token
            for v in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY", "DATABRICKS_HOST", "DATABRICKS_API_BASE"]:
                os.environ.pop(v, None)

    def test_without_token_uses_api_token(self):
        import warnings
        orig_token = _databricks_auth._api_token
        orig_host = _databricks_auth._workspace_host
        try:
            _databricks_auth._api_token = "pat_value"
            _databricks_auth._workspace_host = "https://h.com"
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    assert setup_environment_variables(None) is True
            assert os.environ.get("DATABRICKS_TOKEN") == "pat_value"
        finally:
            _databricks_auth._api_token = orig_token
            _databricks_auth._workspace_host = orig_host
            for v in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY", "DATABRICKS_HOST", "DATABRICKS_API_BASE"]:
                os.environ.pop(v, None)

    def test_from_async_context(self):
        import warnings
        orig_host = _databricks_auth._workspace_host
        async def _inner():
            _databricks_auth._workspace_host = "https://h.com"
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    return setup_environment_variables(user_token="tok2")
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(_inner())
            assert isinstance(result, bool)
        finally:
            loop.close()
            _databricks_auth._workspace_host = orig_host
            for v in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY", "DATABRICKS_HOST", "DATABRICKS_API_BASE"]:
                os.environ.pop(v, None)

    def test_outer_exception(self):
        """Lines 868-870."""
        import warnings
        with patch("asyncio.get_running_loop", side_effect=RuntimeError), \
             patch("asyncio.run", side_effect=Exception("run fail")):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                assert setup_environment_variables(user_token="tok") is False


# ── extract_user_token_from_request ────────────────────

class TestExtractUserToken:
    def test_forwarded_header(self):
        req = Mock()
        req.headers = {"X-Forwarded-Access-Token": "fwd"}
        assert extract_user_token_from_request(req) == "fwd"

    def test_bearer_header(self):
        req = Mock()
        req.headers = {"Authorization": "Bearer xyz"}
        assert extract_user_token_from_request(req) == "xyz"

    def test_no_headers_attr(self):
        assert extract_user_token_from_request(Mock(spec=[])) is None

    def test_exception(self):
        req = Mock()
        req.headers = Mock()
        req.headers.get = Mock(side_effect=Exception("hdr err"))
        assert extract_user_token_from_request(req) is None


# ── get_auth_context ───────────────────────────────────

class TestGetAuthContext:
    def _save(self):
        return {k: getattr(_databricks_auth, f"_{k}") for k in
                ["workspace_host", "client_id", "client_secret", "service_token", "config_loaded"]}

    def _restore(self, s):
        for k, v in s.items():
            setattr(_databricks_auth, f"_{k}", v)

    @pytest.mark.asyncio
    async def test_config_load_fails(self):
        with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=False):
            assert await get_auth_context() is None

    @pytest.mark.asyncio
    async def test_no_workspace_host(self):
        s = self._save()
        _databricks_auth._workspace_host = None
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True):
                assert await get_auth_context() is None
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_obo_success(self):
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        mock_user = MagicMock()
        mock_user.user_name = "u@x.com"
        mock_user.application_id = None
        mock_client = MagicMock()
        mock_client.current_user.me.return_value = mock_user
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch("src.utils.databricks_auth._clean_environment") as mc, \
                 patch("src.utils.databricks_auth.WorkspaceClient", return_value=mock_client):
                mc.return_value.__enter__ = Mock(return_value=None)
                mc.return_value.__exit__ = Mock(return_value=False)
                result = await get_auth_context(user_token="user_tok")
            assert result.auth_method == "obo" and result.user_identity == "u@x.com"
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_obo_uses_application_id(self):
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        mock_user = MagicMock()
        mock_user.user_name = None
        mock_user.application_id = "app-123"
        mock_client = MagicMock()
        mock_client.current_user.me.return_value = mock_user
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch("src.utils.databricks_auth._clean_environment") as mc, \
                 patch("src.utils.databricks_auth.WorkspaceClient", return_value=mock_client):
                mc.return_value.__enter__ = Mock(return_value=None)
                mc.return_value.__exit__ = Mock(return_value=False)
                result = await get_auth_context(user_token="tok")
            assert result.user_identity == "app-123"
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_obo_fails_falls_through(self):
        """Lines 988-990: OBO WorkspaceClient raises."""
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = None
        _databricks_auth._client_secret = None
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch("src.utils.databricks_auth._clean_environment") as mc, \
                 patch("src.utils.databricks_auth.WorkspaceClient", side_effect=Exception("obo fail")), \
                 patch("src.services.api_keys_service.ApiKeysService"), \
                 patch("src.db.session.async_session_factory", return_value=mock_session), \
                 patch("src.utils.user_context.UserContext") as mock_uc:
                mc.return_value.__enter__ = Mock(return_value=None)
                mc.return_value.__exit__ = Mock(return_value=False)
                mock_uc.get_group_context.return_value = None
                result = await get_auth_context(user_token="tok")
            # Falls through to PAT (no group_id) then SPN (no creds) -> None
            assert result is None
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_pat_from_db_with_group_id(self):
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = None
        _databricks_auth._client_secret = None
        mock_api_key = MagicMock()
        mock_api_key.encrypted_value = "enc"
        mock_service = MagicMock()
        mock_service.find_by_name = AsyncMock(return_value=mock_api_key)
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch("src.services.api_keys_service.ApiKeysService", return_value=mock_service), \
                 patch("src.db.session.async_session_factory", return_value=mock_session), \
                 patch("src.utils.user_context.UserContext"), \
                 patch("src.utils.encryption_utils.EncryptionUtils") as enc:
                enc.decrypt_value.return_value = "decrypted"
                result = await get_auth_context(group_id="grp1")
            assert result.auth_method == "pat" and result.token == "decrypted"
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_pat_user_context_has_group_id(self):
        """Lines 1012-1013: UserContext returns group_context with primary_group_id."""
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = None
        _databricks_auth._client_secret = None
        mock_api_key = MagicMock()
        mock_api_key.encrypted_value = "enc"
        mock_service = MagicMock()
        mock_service.find_by_name = AsyncMock(return_value=mock_api_key)
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_group_ctx = MagicMock()
        mock_group_ctx.primary_group_id = "ctx_grp"
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch("src.services.api_keys_service.ApiKeysService", return_value=mock_service), \
                 patch("src.db.session.async_session_factory", return_value=mock_session), \
                 patch("src.utils.user_context.UserContext") as mock_uc, \
                 patch("src.utils.encryption_utils.EncryptionUtils") as enc:
                mock_uc.get_group_context.return_value = mock_group_ctx
                enc.decrypt_value.return_value = "pat_from_ctx"
                result = await get_auth_context()  # no group_id param, uses UserContext
            assert result.auth_method == "pat" and result.token == "pat_from_ctx"
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_pat_user_context_exception(self):
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = None
        _databricks_auth._client_secret = None
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch("src.services.api_keys_service.ApiKeysService"), \
                 patch("src.db.session.async_session_factory", return_value=mock_session), \
                 patch("src.utils.user_context.UserContext") as mock_uc:
                mock_uc.get_group_context.side_effect = Exception("no context")
                assert await get_auth_context() is None
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_pat_find_by_name_raises(self):
        """Lines 1038-1039: find_by_name raises for a key."""
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = None
        _databricks_auth._client_secret = None
        mock_service = MagicMock()
        mock_service.find_by_name = AsyncMock(side_effect=Exception("db err"))
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch("src.services.api_keys_service.ApiKeysService", return_value=mock_service), \
                 patch("src.db.session.async_session_factory", return_value=mock_session), \
                 patch("src.utils.user_context.UserContext"):
                assert await get_auth_context(group_id="grp") is None
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_pat_no_encrypted_value(self):
        """Line 1042: keys found but none has encrypted_value."""
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = None
        _databricks_auth._client_secret = None
        mock_api_key = MagicMock()
        mock_api_key.encrypted_value = None  # no encrypted value
        mock_service = MagicMock()
        mock_service.find_by_name = AsyncMock(return_value=mock_api_key)
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch("src.services.api_keys_service.ApiKeysService", return_value=mock_service), \
                 patch("src.db.session.async_session_factory", return_value=mock_session), \
                 patch("src.utils.user_context.UserContext"):
                assert await get_auth_context(group_id="grp") is None
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_pat_lookup_outer_exception(self):
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = None
        _databricks_auth._client_secret = None
        try:
            original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__
            def fail_import(name, *args, **kwargs):
                if name == "src.services.api_keys_service":
                    raise ImportError("no module")
                return original_import(name, *args, **kwargs)
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch("builtins.__import__", side_effect=fail_import):
                assert await get_auth_context(group_id="grp") is None
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_spn_expired_refresh(self):
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = "c"
        _databricks_auth._client_secret = "s"
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch.object(_databricks_auth, "_is_service_token_expired", return_value=True), \
                 patch.object(_databricks_auth, "_refresh_service_token", new_callable=AsyncMock, return_value="spn"), \
                 patch("src.services.api_keys_service.ApiKeysService"), \
                 patch("src.db.session.async_session_factory", return_value=mock_session), \
                 patch("src.utils.user_context.UserContext") as uc:
                uc.get_group_context.return_value = None
                result = await get_auth_context()
            assert result.auth_method == "service_principal" and result.token == "spn"
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_spn_cached(self):
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = "c"
        _databricks_auth._client_secret = "s"
        _databricks_auth._service_token = "cached"
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch.object(_databricks_auth, "_is_service_token_expired", return_value=False), \
                 patch("src.services.api_keys_service.ApiKeysService"), \
                 patch("src.db.session.async_session_factory", return_value=mock_session), \
                 patch("src.utils.user_context.UserContext") as uc:
                uc.get_group_context.return_value = None
                result = await get_auth_context()
            assert result.token == "cached"
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_spn_refresh_fails(self):
        s = self._save()
        _databricks_auth._workspace_host = "https://h.com"
        _databricks_auth._client_id = "c"
        _databricks_auth._client_secret = "s"
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        try:
            with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, return_value=True), \
                 patch.object(_databricks_auth, "_is_service_token_expired", return_value=True), \
                 patch.object(_databricks_auth, "_refresh_service_token", new_callable=AsyncMock, return_value=None), \
                 patch("src.services.api_keys_service.ApiKeysService"), \
                 patch("src.db.session.async_session_factory", return_value=mock_session), \
                 patch("src.utils.user_context.UserContext") as uc:
                uc.get_group_context.return_value = None
                assert await get_auth_context() is None
        finally:
            self._restore(s)

    @pytest.mark.asyncio
    async def test_outer_exception(self):
        with patch.object(_databricks_auth, "_load_config", new_callable=AsyncMock, side_effect=Exception("total")):
            assert await get_auth_context() is None


# ── get_workspace_client ───────────────────────────────

class TestGetWorkspaceClient:
    @pytest.mark.asyncio
    async def test_auth_context_none(self):
        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=None):
            assert await get_workspace_client() is None

    @pytest.mark.asyncio
    async def test_success(self):
        ctx = MagicMock()
        ctx.get_workspace_client.return_value = MagicMock()
        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=ctx):
            assert await get_workspace_client(user_token="t") is ctx.get_workspace_client.return_value

    @pytest.mark.asyncio
    async def test_exception(self):
        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, side_effect=Exception("e")):
            assert await get_workspace_client() is None


# ── get_workspace_client_with_fallback ─────────────────

class TestGetWorkspaceClientWithFallback:
    @pytest.mark.asyncio
    async def test_auth_none(self):
        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=None):
            c, t = await get_workspace_client_with_fallback(operation_name="op")
        assert c is None and t is None

    @pytest.mark.asyncio
    async def test_obo_returns_user_token(self):
        ctx = MagicMock()
        ctx.auth_method = "obo"
        ctx.get_workspace_client.return_value = MagicMock()
        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=ctx):
            c, t = await get_workspace_client_with_fallback(user_token="ut")
        assert c is not None and t == "ut"

    @pytest.mark.asyncio
    async def test_pat_returns_none_token(self):
        ctx = MagicMock()
        ctx.auth_method = "pat"
        ctx.get_workspace_client.return_value = MagicMock()
        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=ctx):
            c, t = await get_workspace_client_with_fallback(user_token="t")
        assert c is not None and t is None

    @pytest.mark.asyncio
    async def test_exception(self):
        with patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, side_effect=Exception("e")):
            c, t = await get_workspace_client_with_fallback()
        assert c is None and t is None


# ── get_mcp_access_token ───────────────────────────────

class TestGetMcpAccessToken:
    @pytest.mark.asyncio
    async def test_jwt_token(self):
        m = MagicMock()
        m.stdout = json.dumps({"access_token": "eyJabc123"})
        with patch("subprocess.run", return_value=m):
            tok, err = await get_mcp_access_token()
        assert tok == "eyJabc123" and err is None

    @pytest.mark.asyncio
    async def test_non_jwt_token(self):
        m = MagicMock()
        m.stdout = json.dumps({"access_token": "pat-token"})
        with patch("subprocess.run", return_value=m):
            tok, err = await get_mcp_access_token()
        assert tok == "pat-token" and err is None

    @pytest.mark.asyncio
    async def test_no_access_token(self):
        m = MagicMock()
        m.stdout = json.dumps({})
        with patch("subprocess.run", return_value=m):
            tok, err = await get_mcp_access_token()
        assert tok is None and "No access token" in err

    @pytest.mark.asyncio
    async def test_called_process_error(self):
        with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd", stderr="cli err")):
            tok, err = await get_mcp_access_token()
        assert tok is None and "CLI command failed" in err

    @pytest.mark.asyncio
    async def test_json_decode_error(self):
        m = MagicMock()
        m.stdout = "not json"
        with patch("subprocess.run", return_value=m):
            tok, err = await get_mcp_access_token()
        assert tok is None and "Failed to parse" in err

    @pytest.mark.asyncio
    async def test_general_exception(self):
        with patch("subprocess.run", side_effect=FileNotFoundError("no file")):
            tok, err = await get_mcp_access_token()
        assert tok is None and "no file" in err


# ── get_current_databricks_user ────────────────────────

class TestGetCurrentDatabricksUser:
    @pytest.mark.asyncio
    async def test_no_client(self):
        with patch("src.utils.databricks_auth.get_workspace_client", new_callable=AsyncMock, return_value=None):
            u, e = await get_current_databricks_user()
        assert u is None and "Failed to create" in e

    @pytest.mark.asyncio
    async def test_user_name(self):
        mu = MagicMock()
        mu.user_name = "u@x.com"
        mc = MagicMock()
        mc.current_user.me.return_value = mu
        with patch("src.utils.databricks_auth.get_workspace_client", new_callable=AsyncMock, return_value=mc):
            u, e = await get_current_databricks_user(user_token="t")
        assert u == "u@x.com" and e is None

    @pytest.mark.asyncio
    async def test_application_id(self):
        mu = MagicMock()
        mu.user_name = None
        mu.applicationId = "app-id"
        mu.display_name = None
        mc = MagicMock()
        mc.current_user.me.return_value = mu
        with patch("src.utils.databricks_auth.get_workspace_client", new_callable=AsyncMock, return_value=mc):
            u, e = await get_current_databricks_user()
        assert u == "app-id"

    @pytest.mark.asyncio
    async def test_display_name(self):
        mu = MagicMock()
        mu.user_name = None
        del mu.applicationId
        mu.display_name = "DN"
        mc = MagicMock()
        mc.current_user.me.return_value = mu
        with patch("src.utils.databricks_auth.get_workspace_client", new_callable=AsyncMock, return_value=mc):
            u, e = await get_current_databricks_user()
        assert u == "DN"

    @pytest.mark.asyncio
    async def test_no_identity(self):
        mu = MagicMock()
        mu.user_name = None
        del mu.applicationId
        mu.display_name = None
        mc = MagicMock()
        mc.current_user.me.return_value = mu
        with patch("src.utils.databricks_auth.get_workspace_client", new_callable=AsyncMock, return_value=mc):
            u, e = await get_current_databricks_user()
        assert u is None and "Could not determine" in e

    @pytest.mark.asyncio
    async def test_me_returns_none(self):
        mc = MagicMock()
        mc.current_user.me.return_value = None
        with patch("src.utils.databricks_auth.get_workspace_client", new_callable=AsyncMock, return_value=mc):
            u, e = await get_current_databricks_user()
        assert u is None and "returned None" in e

    @pytest.mark.asyncio
    async def test_me_raises(self):
        mc = MagicMock()
        mc.current_user.me.side_effect = Exception("api err")
        with patch("src.utils.databricks_auth.get_workspace_client", new_callable=AsyncMock, return_value=mc):
            u, e = await get_current_databricks_user()
        assert u is None and "Failed to get current user" in e

    @pytest.mark.asyncio
    async def test_outer_exception(self):
        with patch("src.utils.databricks_auth.get_workspace_client", new_callable=AsyncMock, side_effect=Exception("x")):
            u, e = await get_current_databricks_user()
        assert u is None and "x" in e


# ── get_mcp_auth_headers ──────────────────────────────

class TestGetMcpAuthHeaders:
    @pytest.mark.asyncio
    async def test_obo_success_with_sse(self):
        ma = MagicMock()
        ma.set_user_access_token = Mock()
        ma.get_auth_headers = AsyncMock(return_value=({"Authorization": "Bearer tok"}, None))
        with patch("src.utils.databricks_auth.DatabricksAuth", return_value=ma):
            h, e = await get_mcp_auth_headers("https://mcp.example.com", user_token="tok", include_sse_headers=True)
        assert e is None and h["Accept"] == "text/event-stream"

    @pytest.mark.asyncio
    async def test_obo_fails_falls_to_api_key(self):
        ma = MagicMock()
        ma.set_user_access_token = Mock()
        ma.get_auth_headers = AsyncMock(return_value=(None, "obo failed"))
        with patch("src.utils.databricks_auth.DatabricksAuth", return_value=ma):
            h, e = await get_mcp_auth_headers("https://mcp.example.com", user_token="tok", api_key="k")
        assert e is None and h["Authorization"] == "Bearer k"

    @pytest.mark.asyncio
    async def test_obo_exception_falls_to_api_key(self):
        ma = MagicMock()
        ma.set_user_access_token = Mock()
        ma.get_auth_headers = AsyncMock(side_effect=Exception("boom"))
        with patch("src.utils.databricks_auth.DatabricksAuth", return_value=ma):
            h, e = await get_mcp_auth_headers("https://mcp.example.com", user_token="tok", api_key="k")
        assert e is None and h["Authorization"] == "Bearer k"

    @pytest.mark.asyncio
    async def test_api_key_with_sse(self):
        h, e = await get_mcp_auth_headers("https://mcp.example.com", api_key="k", include_sse_headers=True)
        assert e is None and h["Accept"] == "text/event-stream"

    @pytest.mark.asyncio
    async def test_api_key_without_sse(self):
        h, e = await get_mcp_auth_headers("https://mcp.example.com", api_key="k")
        assert e is None and "Accept" not in h

    @pytest.mark.asyncio
    async def test_cli_fallback_with_sse(self):
        with patch("src.utils.databricks_auth.get_mcp_access_token", new_callable=AsyncMock, return_value=("ct", None)):
            h, e = await get_mcp_auth_headers("https://mcp.example.com", include_sse_headers=True)
        assert e is None and h["Authorization"] == "Bearer ct"

    @pytest.mark.asyncio
    async def test_cli_fallback_without_sse(self):
        with patch("src.utils.databricks_auth.get_mcp_access_token", new_callable=AsyncMock, return_value=("ct", None)):
            h, e = await get_mcp_auth_headers("https://mcp.example.com")
        assert e is None and "Accept" not in h

    @pytest.mark.asyncio
    async def test_cli_fallback_error(self):
        with patch("src.utils.databricks_auth.get_mcp_access_token", new_callable=AsyncMock, return_value=(None, "cli err")):
            h, e = await get_mcp_auth_headers("https://mcp.example.com")
        assert h is None and e == "cli err"

    @pytest.mark.asyncio
    async def test_outer_exception(self):
        """Lines 1383-1385."""
        with patch("src.utils.databricks_auth.DatabricksAuth", side_effect=Exception("total")), \
             patch("src.utils.databricks_auth.get_mcp_access_token", new_callable=AsyncMock, side_effect=Exception("also")):
            h, e = await get_mcp_auth_headers("https://mcp.example.com", user_token="tok")
        assert h is None


# ── is_scope_error ─────────────────────────────────────

class TestIsScopeError:
    def test_required_scopes(self):
        assert is_scope_error(Exception("does not have required scopes")) is True

    def test_insufficient(self):
        assert is_scope_error(Exception("insufficient scopes")) is True

    def test_missing(self):
        assert is_scope_error(Exception("missing scopes")) is True

    def test_not_scope(self):
        assert is_scope_error(Exception("timeout")) is False

    def test_none(self):
        assert is_scope_error(None) is False


# ── DatabricksAuth httpx methods ───────────────────────

class TestDatabricksAuthHttpxMethods:
    def _mk(self, **kw):
        auth = DatabricksAuth()
        for k, v in kw.items():
            setattr(auth, f"_{k}", v)
        return auth

    @pytest.mark.asyncio
    async def test_manual_oauth_success(self):
        auth = self._mk(workspace_host="https://h.com", client_id="c", client_secret="s")
        mr = MagicMock()
        mr.status_code = 200
        mr.json.return_value = {"access_token": "tok", "expires_in": 7200}
        mc = AsyncMock()
        mc.post.return_value = mr
        with patch("src.utils.databricks_auth.httpx.AsyncClient") as M:
            M.return_value.__aenter__ = AsyncMock(return_value=mc)
            M.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await auth._manual_oauth_flow() == "tok"

    @pytest.mark.asyncio
    async def test_manual_oauth_no_host(self):
        assert await self._mk(workspace_host=None, client_id="c", client_secret="s")._manual_oauth_flow() is None

    @pytest.mark.asyncio
    async def test_manual_oauth_no_token_in_resp(self):
        auth = self._mk(workspace_host="https://h.com", client_id="c", client_secret="s")
        mr = MagicMock()
        mr.status_code = 200
        mr.json.return_value = {}
        mc = AsyncMock()
        mc.post.return_value = mr
        with patch("src.utils.databricks_auth.httpx.AsyncClient") as M:
            M.return_value.__aenter__ = AsyncMock(return_value=mc)
            M.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await auth._manual_oauth_flow() is None

    @pytest.mark.asyncio
    async def test_manual_oauth_non_200(self):
        auth = self._mk(workspace_host="https://h.com", client_id="c", client_secret="s")
        mr = MagicMock()
        mr.status_code = 400
        mr.text = "Bad"
        mc = AsyncMock()
        mc.post.return_value = mr
        with patch("src.utils.databricks_auth.httpx.AsyncClient") as M:
            M.return_value.__aenter__ = AsyncMock(return_value=mc)
            M.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await auth._manual_oauth_flow() is None

    @pytest.mark.asyncio
    async def test_manual_oauth_exception(self):
        auth = self._mk(workspace_host="https://h.com", client_id="c", client_secret="s")
        mc = AsyncMock()
        mc.post.side_effect = Exception("err")
        with patch("src.utils.databricks_auth.httpx.AsyncClient") as M:
            M.return_value.__aenter__ = AsyncMock(return_value=mc)
            M.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await auth._manual_oauth_flow() is None

    @pytest.mark.asyncio
    async def test_validate_success(self):
        auth = self._mk(api_token="t", workspace_host="https://h.com")
        mr = MagicMock()
        mr.status_code = 200
        mr.json.return_value = {"userName": "u@x.com"}
        mc = AsyncMock()
        mc.get.return_value = mr
        with patch("src.utils.databricks_auth.httpx.AsyncClient") as M:
            M.return_value.__aenter__ = AsyncMock(return_value=mc)
            M.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await auth._validate_token() is True

    @pytest.mark.asyncio
    async def test_validate_no_token(self):
        assert await self._mk(api_token=None, workspace_host="https://h.com")._validate_token() is False

    @pytest.mark.asyncio
    async def test_validate_no_host(self):
        assert await self._mk(api_token="t", workspace_host=None)._validate_token() is False

    @pytest.mark.asyncio
    async def test_validate_non_200(self):
        auth = self._mk(api_token="t", workspace_host="https://h.com")
        mr = MagicMock()
        mr.status_code = 401
        mc = AsyncMock()
        mc.get.return_value = mr
        with patch("src.utils.databricks_auth.httpx.AsyncClient") as M:
            M.return_value.__aenter__ = AsyncMock(return_value=mc)
            M.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await auth._validate_token() is False

    @pytest.mark.asyncio
    async def test_validate_exception(self):
        auth = self._mk(api_token="t", workspace_host="https://h.com")
        mc = AsyncMock()
        mc.get.side_effect = Exception("err")
        with patch("src.utils.databricks_auth.httpx.AsyncClient") as M:
            M.return_value.__aenter__ = AsyncMock(return_value=mc)
            M.return_value.__aexit__ = AsyncMock(return_value=False)
            assert await auth._validate_token() is False
