"""
Extended tests for memory_backend_router.py to cover missing lines.
Focuses on: get_memory_backend_service factory (line 44),
set_default_memory_config not-found, one_click_databricks_setup
workspace_url paths, and various endpoints with missing branches.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

from src.api.memory_backend_router import (
    get_memory_backend_service,
    set_default_memory_config,
    get_memory_stats,
    one_click_databricks_setup,
    get_workspace_url,
)
import importlib
_m = importlib.import_module("src.api.memory_backend_router")
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError


class AdminCtx:
    def __init__(self, is_admin=True):
        self.user_role = "admin" if is_admin else "viewer"
        self.current_user = SimpleNamespace(
            is_system_admin=False,
            is_personal_workspace_manager=is_admin,
        )
        # Personal workspace so is_workspace_admin uses is_personal_workspace_manager
        self.primary_group_id = "user_alice_example_com"
        self.group_ids = ["user_alice_example_com"]
        self.group_email = "alice@example.com"
        self.access_token = "tok"


# ── get_memory_backend_service factory (line 44) ──────────────────────────────

def test_get_memory_backend_service_creates_instance():
    """get_memory_backend_service creates MemoryBackendService with session."""
    from src.services.memory_backend_service import MemoryBackendService

    fake_session = MagicMock()
    with patch("src.api.memory_backend_router.MemoryBackendService") as MockSvc:
        MockSvc.return_value = MagicMock(spec=MemoryBackendService)
        svc = get_memory_backend_service(session=fake_session)
        MockSvc.assert_called_once_with(fake_session)


# ── get_workspace_url ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_workspace_url_returns_result():
    """get_workspace_url calls service and returns result."""
    svc = AsyncMock()
    svc.get_workspace_url = AsyncMock(return_value={"workspace_url": "https://db.com"})
    ctx = AdminCtx()

    out = await get_workspace_url(service=svc, group_context=ctx)
    assert out["workspace_url"] == "https://db.com"


# ── test_lakebase_connection ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_lakebase_conn_success():
    """test_lakebase_connection endpoint calls service and returns result."""
    svc = AsyncMock()
    svc.test_lakebase_connection = AsyncMock(
        return_value={"success": True, "message": "Connected"}
    )
    ctx = AdminCtx()

    out = await _m.test_lakebase_connection(
        group_context=ctx, service=svc, request=None
    )
    assert out["success"] is True


@pytest.mark.asyncio
async def test_lakebase_conn_with_instance_name():
    """test_lakebase_connection passes instance_name to service."""
    svc = AsyncMock()
    svc.test_lakebase_connection = AsyncMock(
        return_value={"success": True, "message": "OK"}
    )
    ctx = AdminCtx()

    out = await _m.test_lakebase_connection(
        group_context=ctx, service=svc, request={"instance_name": "my-instance"}
    )
    svc.test_lakebase_connection.assert_called_once_with(instance_name="my-instance")


@pytest.mark.asyncio
async def test_lakebase_conn_exception_returns_error():
    """test_lakebase_connection returns error dict on exception."""
    svc = AsyncMock()
    svc.test_lakebase_connection = AsyncMock(side_effect=Exception("network error"))
    ctx = AdminCtx()

    out = await _m.test_lakebase_connection(
        group_context=ctx, service=svc, request=None
    )
    assert out["success"] is False
    assert "network error" in out["message"]


# ── set_default_memory_config ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_set_default_memory_config_not_found():
    """set_default_memory_config raises NotFoundError when backend not found."""
    svc = AsyncMock()
    svc.set_default_backend = AsyncMock(return_value=False)
    ctx = AdminCtx()

    with pytest.raises(NotFoundError):
        await set_default_memory_config("missing-id", group_context=ctx, service=svc)


@pytest.mark.asyncio
async def test_set_default_memory_config_success():
    """set_default_memory_config returns success dict."""
    svc = AsyncMock()
    svc.set_default_backend = AsyncMock(return_value=True)
    ctx = AdminCtx()

    out = await set_default_memory_config("backend-1", group_context=ctx, service=svc)
    assert out["success"] is True
    svc.set_default_backend.assert_called_once_with("user_alice_example_com", "backend-1")


# ── get_memory_stats ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_memory_stats_returns_stats():
    """get_memory_stats calls service with crew_id and group_id."""
    svc = AsyncMock()
    svc.get_memory_stats = AsyncMock(
        return_value={"total_entries": 10, "storage_mb": 5.2}
    )
    ctx = AdminCtx()

    out = await get_memory_stats("crew-1", group_context=ctx, service=svc)
    assert out["total_entries"] == 10
    svc.get_memory_stats.assert_called_once_with("user_alice_example_com", "crew-1")


# ── one_click_databricks_setup ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_one_click_setup_permission_denied():
    """one_click_databricks_setup raises ForbiddenError for non-admin."""
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=False)
    req = SimpleNamespace(headers={})

    with pytest.raises(ForbiddenError):
        await one_click_databricks_setup(
            request={"workspace_url": "https://w"}, req=req,
            group_context=ctx, service=svc
        )


@pytest.mark.asyncio
async def test_one_click_setup_with_workspace_url_in_request():
    """one_click_databricks_setup uses workspace_url from request."""
    svc = AsyncMock()
    svc.one_click_databricks_setup = AsyncMock(return_value={"success": True})
    ctx = AdminCtx(is_admin=True)
    req = MagicMock()

    with patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"), \
         patch("src.utils.user_context.UserContext.set_group_context"):
        out = await one_click_databricks_setup(
            request={"workspace_url": "https://myws.databricks.com", "catalog": "ml", "schema": "agents"},
            req=req,
            group_context=ctx,
            service=svc,
        )
    assert out["success"] is True


@pytest.mark.asyncio
async def test_one_click_setup_no_workspace_url_raises():
    """one_click_databricks_setup raises BadRequestError when no workspace_url."""
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=True)
    req = MagicMock()

    with patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"), \
         patch("src.utils.user_context.UserContext.set_group_context"), \
         patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)):
        with pytest.raises(BadRequestError):
            await one_click_databricks_setup(
                request={},  # No workspace_url
                req=req,
                group_context=ctx,
                service=svc,
            )


@pytest.mark.asyncio
async def test_one_click_setup_workspace_url_from_auth():
    """one_click_databricks_setup fetches workspace_url from auth when not in request."""
    svc = AsyncMock()
    svc.one_click_databricks_setup = AsyncMock(return_value={"success": True})
    ctx = AdminCtx(is_admin=True)
    req = MagicMock()

    fake_auth = SimpleNamespace(workspace_url="https://auto.databricks.com", auth_method="oauth")
    with patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"), \
         patch("src.utils.user_context.UserContext.set_group_context"), \
         patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=fake_auth)):
        out = await one_click_databricks_setup(
            request={"catalog": "ml", "schema": "agents"},
            req=req,
            group_context=ctx,
            service=svc,
        )
    assert out["success"] is True
    svc.one_click_databricks_setup.assert_called_once()


@pytest.mark.asyncio
async def test_one_click_setup_auth_raises_but_no_url_in_request():
    """one_click_databricks_setup raises BadRequestError when auth fails and no url."""
    svc = AsyncMock()
    ctx = AdminCtx(is_admin=True)
    req = MagicMock()

    with patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"), \
         patch("src.utils.user_context.UserContext.set_group_context"), \
         patch("src.utils.databricks_auth.get_auth_context", AsyncMock(side_effect=Exception("auth error"))):
        with pytest.raises(BadRequestError):
            await one_click_databricks_setup(
                request={},
                req=req,
                group_context=ctx,
                service=svc,
            )
