"""
Extended tests for engine_config_router.py to cover missing lines.
Focuses on: get_engine_config_service factory (line 47),
not-found paths, engine type lookup, and otel endpoints.
The permission-denied paths are already covered by test_engine_config_router_smoke.py.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

from src.api.engine_config_router import (
    get_engine_config_service,
    get_engine_configs_by_type,
    get_engine_config,
    get_engine_config_by_key,
    create_engine_config,
    update_engine_config,
    toggle_engine_config,
    update_config_value,
    get_crewai_flow_enabled,
    set_crewai_flow_enabled,
    get_otel_app_telemetry_enabled,
    set_otel_app_telemetry_enabled,
    delete_engine_config,
)
from src.schemas.engine_config import (
    EngineConfigCreate,
    EngineConfigUpdate,
    EngineConfigToggleUpdate,
    EngineConfigValueUpdate,
    CrewAIFlowConfigUpdate,
    OtelAppTelemetryConfigUpdate,
)
from src.core.exceptions import ForbiddenError, KasalError, NotFoundError


def make_ctx(user_role="admin", is_system=False):
    """Create a context object with the given role."""
    return type("Ctx", (), {
        "user_role": user_role,
        "current_user": SimpleNamespace(
            is_system_admin=is_system,
            is_personal_workspace_manager=False,
        ),
        "primary_group_id": "team_g1",  # team workspace, not personal
        "group_ids": ["team_g1"],
        "group_email": "admin@x",
    })()


def make_config_obj(engine_name="crewai", engine_type="crew", enabled=True):
    from datetime import datetime
    return SimpleNamespace(
        id=1,
        engine_name=engine_name,
        engine_type=engine_type,
        config_key="key1",
        config_value="val1",
        is_enabled=enabled,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        description=None,
    )


# ── get_engine_config_service factory (line 47) ───────────────────────────────

def test_get_engine_config_service_creates_instance():
    """get_engine_config_service creates EngineConfigService with session."""
    from src.services.engine_config_service import EngineConfigService

    fake_session = MagicMock()
    with patch("src.api.engine_config_router.EngineConfigService") as MockSvc:
        MockSvc.return_value = MagicMock(spec=EngineConfigService)
        svc = get_engine_config_service(session=fake_session)
        MockSvc.assert_called_once_with(fake_session)


# ── get_engine_config_by_key: not found (lines 152-161) ──────────────────────

@pytest.mark.asyncio
async def test_get_engine_config_by_key_not_found_raises():
    """get_engine_config_by_key raises NotFoundError when not found."""
    svc = AsyncMock()
    svc.find_by_engine_and_key = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await get_engine_config_by_key(
            "engine1", "config_key", service=svc, group_context=make_ctx()
        )


@pytest.mark.asyncio
async def test_get_engine_config_by_key_found():
    """get_engine_config_by_key returns config when found."""
    svc = AsyncMock()
    config = make_config_obj()
    svc.find_by_engine_and_key = AsyncMock(return_value=config)

    out = await get_engine_config_by_key(
        "crewai", "key1", service=svc, group_context=make_ctx()
    )
    assert out == config


# ── get_engine_configs_by_type (lines 180-185) ────────────────────────────────

@pytest.mark.asyncio
async def test_get_engine_configs_by_type_returns_list():
    """get_engine_configs_by_type returns list wrapped in EngineConfigListResponse."""
    svc = AsyncMock()
    svc.find_by_engine_type = AsyncMock(return_value=[make_config_obj()])

    out = await get_engine_configs_by_type("crew", service=svc, group_context=make_ctx())
    assert out.count == 1


@pytest.mark.asyncio
async def test_get_engine_configs_by_type_empty():
    """get_engine_configs_by_type returns empty list when no configs."""
    svc = AsyncMock()
    svc.find_by_engine_type = AsyncMock(return_value=[])

    out = await get_engine_configs_by_type("crew", service=svc, group_context=make_ctx())
    assert out.count == 0


# ── update_engine_config not-found path (lines 247-260) ──────────────────────

@pytest.mark.asyncio
async def test_update_engine_config_not_found():
    """update_engine_config raises NotFoundError when config missing."""
    svc = AsyncMock()
    svc.update_engine_config = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await update_engine_config(
            "missing",
            EngineConfigUpdate(config_value="new"),
            service=svc,
            group_context=make_ctx(user_role="admin"),
        )


@pytest.mark.asyncio
async def test_update_engine_config_success():
    """update_engine_config returns updated config."""
    svc = AsyncMock()
    config = make_config_obj()
    svc.update_engine_config = AsyncMock(return_value=config)

    out = await update_engine_config(
        "crewai",
        EngineConfigUpdate(config_value="updated"),
        service=svc,
        group_context=make_ctx(user_role="admin"),
    )
    assert out == config


# ── toggle_engine_config success path (line 287, 302-303) ────────────────────

@pytest.mark.asyncio
async def test_toggle_engine_config_not_found():
    """toggle_engine_config raises NotFoundError when config missing."""
    svc = AsyncMock()
    svc.toggle_engine_enabled = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await toggle_engine_config(
            "missing",
            EngineConfigToggleUpdate(enabled=True),
            service=svc,
            group_context=make_ctx(user_role="admin"),
        )


@pytest.mark.asyncio
async def test_toggle_engine_config_success():
    """toggle_engine_config returns toggled config."""
    svc = AsyncMock()
    config = make_config_obj(enabled=False)
    svc.toggle_engine_enabled = AsyncMock(return_value=config)

    out = await toggle_engine_config(
        "crewai",
        EngineConfigToggleUpdate(enabled=False),
        service=svc,
        group_context=make_ctx(user_role="admin"),
    )
    assert out == config


# ── update_config_value not-found (lines 335, 350-351) ───────────────────────

@pytest.mark.asyncio
async def test_update_config_value_not_found():
    """update_config_value raises NotFoundError when config missing."""
    svc = AsyncMock()
    svc.update_config_value = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await update_config_value(
            "missing",
            "key1",
            EngineConfigValueUpdate(config_value="v"),
            service=svc,
            group_context=make_ctx(user_role="admin"),
        )


@pytest.mark.asyncio
async def test_update_config_value_success():
    """update_config_value returns updated config."""
    svc = AsyncMock()
    config = make_config_obj()
    svc.update_config_value = AsyncMock(return_value=config)

    out = await update_config_value(
        "crewai",
        "key1",
        EngineConfigValueUpdate(config_value="new_value"),
        service=svc,
        group_context=make_ctx(user_role="admin"),
    )
    assert out == config


# ── delete_engine_config not-found (lines 486-498) ───────────────────────────

@pytest.mark.asyncio
async def test_delete_engine_config_not_found():
    """delete_engine_config raises NotFoundError when config missing."""
    svc = AsyncMock()
    svc.delete_engine_config = AsyncMock(return_value=False)

    with pytest.raises(NotFoundError):
        await delete_engine_config("missing", service=svc, group_context=make_ctx(user_role="admin"))


@pytest.mark.asyncio
async def test_delete_engine_config_success():
    """delete_engine_config returns None (204 No Content) on success."""
    svc = AsyncMock()
    svc.delete_engine_config = AsyncMock(return_value=True)

    await delete_engine_config("crewai", service=svc, group_context=make_ctx(user_role="admin"))
    svc.delete_engine_config.assert_called_once_with("crewai")


# ── create_engine_config success (line 218+) ─────────────────────────────────

@pytest.mark.asyncio
async def test_create_engine_config_success():
    """create_engine_config returns created config for admin."""
    svc = AsyncMock()
    config = make_config_obj()
    svc.create_engine_config = AsyncMock(return_value=config)

    out = await create_engine_config(
        EngineConfigCreate(
            engine_name="crewai",
            engine_type="crew",
            config_key="k",
            config_value="v",
            is_enabled=True,
        ),
        service=svc,
        group_context=make_ctx(user_role="admin"),
    )
    assert out == config
