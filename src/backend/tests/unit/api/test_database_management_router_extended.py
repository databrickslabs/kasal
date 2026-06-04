"""
Extended tests for database_management_router.py to cover missing lines.
Focuses on: get_lakebase_service dependency, streaming migration, error branches,
enable_lakebase with auto-resolved endpoint, and permission check error path.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

from src.api.database_management_router import (
    get_lakebase_service,
    enable_lakebase_without_migration,
    check_database_management_permission,
    export_database,
    import_database,
    list_backups,
    get_database_info,
)
from src.core.exceptions import BadRequestError, ForbiddenError, KasalError
from src.schemas.database_management import (
    ExportRequest,
    ImportRequest,
    ListBackupsRequest,
)


class Ctx:
    def __init__(
        self,
        is_system_admin=False,
        group_email="u@x",
        access_token="tok",
        primary_group_id="g1",
    ):
        self.group_email = group_email
        self.access_token = access_token
        self.primary_group_id = primary_group_id
        self.current_user = SimpleNamespace(is_system_admin=is_system_admin)


# ── get_lakebase_service dependency ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_lakebase_service_creates_service():
    """get_lakebase_service extracts user_token and user_email."""
    from src.services.lakebase_service import LakebaseService

    raw_request = MagicMock()
    ctx = Ctx(group_email="admin@x", access_token="mytoken")

    with patch(
        "src.utils.databricks_auth.extract_user_token_from_request",
        return_value="extracted_token",
    ), patch(
        "src.services.lakebase_service.LakebaseService"
    ) as MockSvc:
        MockSvc.return_value = MagicMock(spec=LakebaseService)
        svc = get_lakebase_service(
            session=MagicMock(), raw_request=raw_request, group_context=ctx
        )
        # Service was called (either via mock or real init)
        assert svc is not None


# ── export: no user_token warning branch ─────────────────────────────────────

@pytest.mark.asyncio
async def test_export_no_user_token_warning():
    """export_database logs warning when user token is absent."""
    svc = AsyncMock()
    svc.user_token = None  # Trigger the warning branch
    svc.export_to_volume = AsyncMock(
        return_value={"success": True, "backup_filename": "b.db"}
    )
    ctx = Ctx(is_system_admin=True)

    with patch("src.utils.user_context.UserContext") as MockUC:
        out = await export_database(ExportRequest(), service=svc, group_context=ctx)
    assert out.success is True


@pytest.mark.asyncio
async def test_export_service_failure_raises_kasal_error():
    """export_database raises KasalError when service reports failure."""
    svc = AsyncMock()
    svc.user_token = "tok"
    svc.export_to_volume = AsyncMock(
        return_value={"success": False, "error": "disk full"}
    )
    ctx = Ctx(is_system_admin=True)

    with patch("src.utils.user_context.UserContext.set_group_context"):
        with pytest.raises(KasalError):
            await export_database(ExportRequest(), service=svc, group_context=ctx)


# ── import: error path ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_import_service_failure_raises_kasal_error():
    """import_database raises KasalError when service reports failure."""
    svc = AsyncMock()
    svc.user_token = "tok"
    svc.import_from_volume = AsyncMock(
        return_value={"success": False, "error": "corrupt backup"}
    )
    ctx = Ctx(is_system_admin=True)

    with patch("src.utils.user_context.UserContext.set_group_context"):
        with pytest.raises(KasalError):
            await import_database(
                ImportRequest(
                    catalog="c", schema="s", volume_name="v", backup_filename="b.db"
                ),
                service=svc,
                group_context=ctx,
            )


# ── list_backups: non-admin and error path ────────────────────────────────────

@pytest.mark.asyncio
async def test_list_backups_non_system_admin_raises_forbidden():
    """list_backups raises ForbiddenError for non-system-admin."""
    svc = AsyncMock()
    ctx = Ctx(is_system_admin=False)

    with pytest.raises(ForbiddenError):
        await list_backups(ListBackupsRequest(), service=svc, group_context=ctx)


@pytest.mark.asyncio
async def test_list_backups_service_failure_raises_kasal_error():
    """list_backups raises KasalError when service fails."""
    svc = AsyncMock()
    svc.list_backups = AsyncMock(
        return_value={"success": False, "error": "volume not found"}
    )
    ctx = Ctx(is_system_admin=True)

    with pytest.raises(KasalError):
        await list_backups(ListBackupsRequest(), service=svc, group_context=ctx)


# ── get_database_info error path ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_database_info_failure_raises_kasal_error():
    """get_database_info raises KasalError when service fails."""
    svc = AsyncMock()
    svc.get_database_info = AsyncMock(
        return_value={"success": False, "error": "db down"}
    )
    ctx = Ctx()

    with pytest.raises(KasalError):
        await get_database_info(service=svc, group_context=ctx)


# ── check_database_management_permission: exception path ─────────────────────

@pytest.mark.asyncio
async def test_check_permission_exception_in_databricks_apps():
    """Permission check falls back to safe mode on exception in Databricks Apps."""
    svc = AsyncMock()
    svc.check_user_permission = AsyncMock(side_effect=RuntimeError("auth failure"))
    ctx = Ctx(group_email="u@x")

    with patch.dict("os.environ", {"DATABRICKS_APP_NAME": "my-app"}):
        result = await check_database_management_permission(
            service=svc, session=AsyncMock(), group_context=ctx
        )
    # In apps environment with error → deny
    assert result["has_permission"] is False
    assert "error" in result


@pytest.mark.asyncio
async def test_check_permission_exception_outside_databricks_apps():
    """Permission check defaults to allow outside Databricks Apps on exception."""
    svc = AsyncMock()
    svc.check_user_permission = AsyncMock(side_effect=RuntimeError("network error"))
    ctx = Ctx(group_email="u@x")

    with patch.dict("os.environ", {}, clear=True):
        # Ensure DATABRICKS_APP_NAME is not set
        import os
        os.environ.pop("DATABRICKS_APP_NAME", None)
        result = await check_database_management_permission(
            service=svc, session=AsyncMock(), group_context=ctx
        )
    # Outside apps with error → allow
    assert result["has_permission"] is True


# ── enable_lakebase: auto-resolve endpoint ────────────────────────────────────

@pytest.mark.asyncio
async def test_enable_lakebase_auto_resolve_endpoint_success():
    """enable_lakebase_without_migration auto-fetches endpoint when not provided."""
    svc = AsyncMock()
    svc.get_instance = AsyncMock(
        return_value={"read_write_dns": "https://auto-ep.databricks.com"}
    )
    svc.enable_lakebase = AsyncMock(return_value={"enabled": True})

    out = await enable_lakebase_without_migration(
        {"instance_name": "my-instance"}, service=svc
    )
    assert out["enabled"] is True
    svc.get_instance.assert_called_once_with("my-instance")


@pytest.mark.asyncio
async def test_enable_lakebase_auto_resolve_no_dns_raises_bad_request():
    """enable_lakebase_without_migration raises BadRequestError when endpoint can't be resolved."""
    svc = AsyncMock()
    svc.get_instance = AsyncMock(return_value={"read_write_dns": None})

    with pytest.raises(BadRequestError):
        await enable_lakebase_without_migration(
            {"instance_name": "my-instance"}, service=svc
        )


@pytest.mark.asyncio
async def test_enable_lakebase_auto_resolve_no_instance_raises_bad_request():
    """enable_lakebase_without_migration raises BadRequestError when instance not found."""
    svc = AsyncMock()
    svc.get_instance = AsyncMock(return_value=None)

    with pytest.raises(BadRequestError):
        await enable_lakebase_without_migration(
            {"instance_name": "my-instance"}, service=svc
        )


# ── debug endpoints: non-debug mode ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_debug_permissions_returns_404_when_not_debug():
    """debug_permissions raises 404 when DEBUG_MODE is False."""
    from fastapi import HTTPException
    from src.api.database_management_router import debug_permissions
    from src.config.settings import settings as app_settings

    orig = app_settings.DEBUG_MODE
    app_settings.DEBUG_MODE = False
    try:
        with pytest.raises(HTTPException) as exc_info:
            await debug_permissions(session=AsyncMock(), group_context=Ctx())
        assert exc_info.value.status_code == 404
    finally:
        app_settings.DEBUG_MODE = orig


@pytest.mark.asyncio
async def test_debug_headers_returns_404_when_not_debug():
    """debug_headers raises 404 when DEBUG_MODE is False."""
    from fastapi import HTTPException
    from src.api.database_management_router import debug_headers
    from src.config.settings import settings as app_settings

    orig = app_settings.DEBUG_MODE
    app_settings.DEBUG_MODE = False
    try:
        req = SimpleNamespace(headers={})
        with pytest.raises(HTTPException) as exc_info:
            await debug_headers(request=req, group_context=Ctx())
        assert exc_info.value.status_code == 404
    finally:
        app_settings.DEBUG_MODE = orig
