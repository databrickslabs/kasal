"""
Coverage tests for services/lakebase_connection_service.py
Targets uncovered lines: 227-265, 290-338, 417-423, 450-464, 486-497
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from src.services.lakebase_connection_service import LakebaseConnectionService


def make_service(**kwargs):
    return LakebaseConnectionService(**kwargs)


# ─── get_username ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_username_uses_spn_client_id():
    svc = make_service()
    with patch.dict("os.environ", {"DATABRICKS_CLIENT_ID": "my-client-id"}):
        result = await svc.get_username()
    assert result == "my-client-id"


@pytest.mark.asyncio
async def test_get_username_uses_user_email_when_no_spn():
    svc = make_service(user_email="user@example.com")
    with patch.dict("os.environ", {}, clear=True):
        result = await svc.get_username()
    assert result == "user@example.com"


@pytest.mark.asyncio
async def test_get_username_uses_workspace_client_as_fallback():
    svc = make_service()
    mock_user = MagicMock()
    mock_user.user_name = "workspace-user@example.com"
    mock_ws = MagicMock()
    mock_ws.current_user.me.return_value = mock_user
    with patch.dict("os.environ", {}, clear=True):
        svc._workspace_client = mock_ws
        svc.user_email = None
        result = await svc.get_username()
    assert result == "workspace-user@example.com"


@pytest.mark.asyncio
async def test_get_username_raises_when_no_username_available():
    svc = make_service()
    mock_ws = MagicMock()
    mock_ws.current_user.me.side_effect = Exception("no auth")
    with patch.dict("os.environ", {}, clear=True):
        svc._workspace_client = mock_ws
        svc.user_email = None
        with pytest.raises(ValueError, match="Cannot determine PostgreSQL username"):
            await svc.get_username()


@pytest.mark.asyncio
async def test_get_username_workspace_user_has_no_user_name():
    svc = make_service()
    mock_user = MagicMock(spec=[])  # no user_name attr
    mock_ws = MagicMock()
    mock_ws.current_user.me.return_value = mock_user
    with patch.dict("os.environ", {}, clear=True):
        svc._workspace_client = mock_ws
        svc.user_email = None
        with pytest.raises(ValueError):
            await svc.get_username()


# ─── generate_credentials ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_generate_credentials_provisioned_success():
    svc = make_service()
    mock_cred = MagicMock()
    mock_cred.token = "my-token"
    mock_ws = MagicMock()
    mock_ws.database.generate_database_credential.return_value = mock_cred
    svc._workspace_client = mock_ws
    result = await svc.generate_credentials("my-instance")
    assert result.token == "my-token"


@pytest.mark.asyncio
async def test_generate_credentials_falls_back_to_autoscaling():
    svc = make_service()
    mock_cred = MagicMock()
    mock_cred.token = "auto-token"
    mock_ws = MagicMock()
    # First call raises not_found
    mock_ws.database.generate_database_credential.side_effect = Exception("not found")
    mock_endpoint = MagicMock()
    mock_endpoint.name = "projects/proj/branches/production/endpoints/ep"
    mock_ws.postgres.list_endpoints.return_value = iter([mock_endpoint])
    mock_ws.postgres.generate_database_credential.return_value = mock_cred
    svc._workspace_client = mock_ws
    result = await svc.generate_credentials("my-proj")
    assert result.token == "auto-token"


@pytest.mark.asyncio
async def test_generate_credentials_reraises_non_notfound_error():
    svc = make_service()
    mock_ws = MagicMock()
    mock_ws.database.generate_database_credential.side_effect = Exception("internal server error")
    svc._workspace_client = mock_ws
    with pytest.raises(Exception, match="internal server error"):
        await svc.generate_credentials("my-instance")


@pytest.mark.asyncio
async def test_generate_credentials_no_endpoints_raises():
    svc = make_service()
    mock_ws = MagicMock()
    mock_ws.database.generate_database_credential.side_effect = Exception("not found")
    mock_ws.postgres.list_endpoints.return_value = iter([])
    svc._workspace_client = mock_ws
    with pytest.raises(ValueError, match="No endpoints found"):
        await svc.generate_credentials("my-proj")


@pytest.mark.asyncio
async def test_generate_credentials_autoscaling_failure_reraises():
    svc = make_service()
    mock_ws = MagicMock()
    mock_ws.database.generate_database_credential.side_effect = Exception("not_found")
    mock_ws.postgres.list_endpoints.side_effect = Exception("postgres error")
    svc._workspace_client = mock_ws
    with pytest.raises(Exception, match="postgres error"):
        await svc.generate_credentials("my-proj")


# ─── test_connection ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_test_connection_success():
    svc = make_service(user_email="user@example.com")
    mock_cred = MagicMock()
    mock_cred.token = "tok"
    with patch.dict("os.environ", {}, clear=True):
        with patch.object(svc, "generate_credentials", new=AsyncMock(return_value=mock_cred)):
            with patch("src.services.lakebase_connection_service.create_async_engine") as mock_engine_cls:
                mock_conn = MagicMock()
                mock_row = ("user@example.com", "PostgreSQL 14.0")
                mock_result = MagicMock()
                mock_result.fetchone = MagicMock(return_value=mock_row)
                mock_conn.execute = AsyncMock(return_value=mock_result)

                # Properly mock async context manager
                mock_ctx = MagicMock()
                mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_ctx.__aexit__ = AsyncMock(return_value=None)

                mock_engine = MagicMock()
                mock_engine.connect = MagicMock(return_value=mock_ctx)
                mock_engine.dispose = AsyncMock()
                mock_engine_cls.return_value = mock_engine

                result = await svc.test_connection("endpoint.db", "my-instance")
    assert result["success"] is True
    assert result["connected_user"] == "user@example.com"


@pytest.mark.asyncio
async def test_test_connection_failure():
    svc = make_service(user_email="user@example.com")
    mock_cred = MagicMock()
    mock_cred.token = "tok"
    with patch.dict("os.environ", {}, clear=True):
        with patch.object(svc, "generate_credentials", new=AsyncMock(return_value=mock_cred)):
            with patch("src.services.lakebase_connection_service.create_async_engine") as mock_engine_cls:
                mock_ctx = MagicMock()
                mock_ctx.__aenter__ = AsyncMock(side_effect=Exception("connection refused"))
                mock_ctx.__aexit__ = AsyncMock(return_value=None)
                mock_engine = MagicMock()
                mock_engine.connect = MagicMock(return_value=mock_ctx)
                mock_engine.dispose = AsyncMock()
                mock_engine_cls.return_value = mock_engine
                result = await svc.test_connection("endpoint.db", "my-instance")
    assert result["success"] is False
    assert "connection refused" in result["error"]


# ─── create_engine_with_token_refresh ─────────────────────────────────────────

def test_create_engine_with_token_refresh_asyncpg():
    svc = make_service()
    token_holder = {"token": "initial-token", "refreshed_at": 0.0}
    with patch("src.services.lakebase_connection_service.create_async_engine") as mock_create:
        mock_engine = MagicMock()
        mock_engine.sync_engine = MagicMock()
        mock_create.return_value = mock_engine
        with patch("src.services.lakebase_connection_service.event") as mock_event:
            result = svc.create_engine_with_token_refresh(
                endpoint="endpoint.db",
                username="my-user",
                token_holder=token_holder,
                driver="asyncpg"
            )
    assert result is mock_engine


def test_create_engine_with_token_refresh_pg8000():
    svc = make_service()
    token_holder = {"token": "initial-token", "refreshed_at": 0.0}
    with patch("src.services.lakebase_connection_service.create_engine") as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine
        with patch("src.services.lakebase_connection_service.event") as mock_event:
            result = svc.create_engine_with_token_refresh(
                endpoint="endpoint.db",
                username="my-user",
                token_holder=token_holder,
                driver="pg8000"
            )
    assert result is mock_engine


# ─── create_lakebase_engine_async ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_create_lakebase_engine_async():
    svc = make_service()
    with patch("src.services.lakebase_connection_service.create_async_engine") as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine
        result = await svc.create_lakebase_engine_async("ep.db", "user", "token")
    assert result is mock_engine


# ─── create_lakebase_engine_sync ──────────────────────────────────────────────

def test_create_lakebase_engine_sync_no_timeout():
    svc = make_service()
    with patch("src.services.lakebase_connection_service.create_engine") as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine
        result = svc.create_lakebase_engine_sync("ep.db", "user", "token", statement_timeout_ms=0)
    assert result is mock_engine


def test_create_lakebase_engine_sync_with_timeout():
    svc = make_service()
    with patch("src.services.lakebase_connection_service.create_engine") as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine
        with patch("src.services.lakebase_connection_service.event") as mock_event:
            result = svc.create_lakebase_engine_sync("ep.db", "user", "token", statement_timeout_ms=5000)
    assert result is mock_engine


# ─── get_connected_engine_async ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_connected_engine_async_success():
    svc = make_service(user_email="user@example.com")
    mock_cred = MagicMock()
    mock_cred.token = "tok"

    mock_conn = MagicMock()
    mock_scalar_result = MagicMock()
    mock_scalar_result.scalar.return_value = "user@example.com"
    mock_conn.execute = AsyncMock(return_value=mock_scalar_result)

    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_ctx.__aexit__ = AsyncMock(return_value=None)

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(return_value=mock_ctx)
    mock_engine.dispose = AsyncMock()

    with patch.dict("os.environ", {}, clear=True):
        with patch.object(svc, "generate_credentials", new=AsyncMock(return_value=mock_cred)):
            with patch.object(svc, "create_lakebase_engine_async", new=AsyncMock(return_value=mock_engine)):
                username, engine = await svc.get_connected_engine_async("my-instance", "ep.db")
    assert username == "user@example.com"
    assert engine is mock_engine


@pytest.mark.asyncio
async def test_get_connected_engine_async_connection_fails():
    svc = make_service(user_email="user@example.com")
    mock_cred = MagicMock()
    mock_cred.token = "tok"

    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(side_effect=Exception("connect failed"))
    mock_ctx.__aexit__ = AsyncMock(return_value=None)

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(return_value=mock_ctx)
    mock_engine.dispose = AsyncMock()

    with patch.dict("os.environ", {}, clear=True):
        with patch.object(svc, "generate_credentials", new=AsyncMock(return_value=mock_cred)):
            with patch.object(svc, "create_lakebase_engine_async", new=AsyncMock(return_value=mock_engine)):
                with pytest.raises(Exception, match="Failed to connect to Lakebase"):
                    await svc.get_connected_engine_async("my-instance", "ep.db")


# ─── get_connected_engine_sync ────────────────────────────────────────────────

def test_get_connected_engine_sync_success():
    svc = make_service()
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = "sync-user"
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
    with patch.object(svc, "create_lakebase_engine_sync", return_value=mock_engine):
        engine, user = svc.get_connected_engine_sync("ep.db", "user", "tok")
    assert engine is mock_engine
    assert user == "sync-user"


def test_get_connected_engine_sync_fails():
    svc = make_service()
    mock_engine = MagicMock()
    mock_engine.connect.side_effect = Exception("sync connect failed")
    mock_engine.dispose = MagicMock()
    with patch.object(svc, "create_lakebase_engine_sync", return_value=mock_engine):
        with pytest.raises(Exception, match="Failed to connect"):
            svc.get_connected_engine_sync("ep.db", "user", "tok")


# ─── get_workspace_client ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_workspace_client_with_spn():
    svc = make_service()
    with patch.dict("os.environ", {
        "DATABRICKS_CLIENT_ID": "cid",
        "DATABRICKS_CLIENT_SECRET": "csecret",
        "DATABRICKS_HOST": "https://example.com"
    }):
        with patch("src.services.lakebase_connection_service.WorkspaceClient") as mock_ws_cls:
            mock_ws = MagicMock()
            mock_ws_cls.return_value = mock_ws
            result = await svc.get_workspace_client()
    assert result is mock_ws


@pytest.mark.asyncio
async def test_get_workspace_client_local_dev_fallback():
    svc = make_service(user_token="pat-token")
    with patch.dict("os.environ", {}, clear=True):
        mock_ws = MagicMock()
        with patch("src.services.lakebase_connection_service.get_workspace_client", new=AsyncMock(return_value=mock_ws)):
            result = await svc.get_workspace_client()
    assert result is mock_ws


@pytest.mark.asyncio
async def test_get_workspace_client_local_dev_raises_when_none():
    svc = make_service()
    with patch.dict("os.environ", {}, clear=True):
        with patch("src.services.lakebase_connection_service.get_workspace_client", new=AsyncMock(return_value=None)):
            with pytest.raises(ValueError, match="Failed to create WorkspaceClient"):
                await svc.get_workspace_client()


@pytest.mark.asyncio
async def test_get_workspace_client_cached():
    svc = make_service()
    mock_ws = MagicMock()
    svc._workspace_client = mock_ws
    result = await svc.get_workspace_client()
    assert result is mock_ws
