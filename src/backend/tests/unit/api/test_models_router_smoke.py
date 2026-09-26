from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.api.models_router import (
    create_model,
    delete_model,
    disable_all_models,
    enable_all_models,
    get_enabled_models,
    get_global_models,
    get_model,
    get_models,
    toggle_global_model,
    toggle_model,
    update_model,
)
from src.schemas.model_config import (
    ModelConfigCreate,
    ModelConfigUpdate,
    ModelToggleUpdate,
)


class Ctx:
    def __init__(self, user_role=None, primary_group_id="g1"):
        self.user_role = user_role
        self.primary_group_id = primary_group_id
        self.current_user = SimpleNamespace(is_system_admin=False)


@pytest.mark.asyncio
async def test_list_enabled_global_and_get_404():
    svc = AsyncMock()
    ctx = Ctx()

    # list
    now = __import__("datetime").datetime.utcnow()
    models = [
        SimpleNamespace(
            id=1,
            key="m1",
            name="M1",
            provider="openai",
            enabled=True,
            created_at=now,
            updated_at=now,
        )
    ]
    svc.find_all_for_group = AsyncMock(return_value=models)
    out = await get_models(service=svc, group_context=ctx)
    assert out.count == 1

    # enabled
    svc.find_enabled_models_for_group = AsyncMock(return_value=models)
    out2 = await get_enabled_models(service=svc, group_context=ctx)
    assert out2.count == 1

    # global
    svc.find_all_global = AsyncMock(return_value=models)
    out3 = await get_global_models(service=svc)
    assert out3.count == 1

    # get by key 404
    svc.find_by_key = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await get_model("nope", service=svc, group_context=ctx)


@pytest.mark.asyncio
async def test_create_update_delete_and_toggles_permissions_and_success():
    svc = AsyncMock()
    ctx_user = Ctx(user_role="user")
    ctx_admin = Ctx(user_role="admin")  # workspace admin
    ctx_sys = Ctx(user_role="admin")
    ctx_sys.current_user.is_system_admin = True

    # create forbidden
    with pytest.raises(Exception):
        await create_model(
            ModelConfigCreate(key="k", name="n", provider="openai"),
            service=svc,
            group_context=ctx_user,
        )

    # create success
    svc.create_model_config = AsyncMock(
        return_value=SimpleNamespace(key="k", name="n", provider="openai", enabled=True)
    )
    out = await create_model(
        ModelConfigCreate(key="k", name="n", provider="openai"),
        service=svc,
        group_context=ctx_sys,
    )
    assert out.key == "k"

    # update forbidden
    with pytest.raises(Exception):
        await update_model(
            "k",
            ModelConfigUpdate(key="k", name="n2", provider="openai"),
            service=svc,
            group_context=ctx_user,
        )

    # update not found -> 404
    svc.update_model_config = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await update_model(
            "missing",
            ModelConfigUpdate(key="k", name="n2", provider="openai"),
            service=svc,
            group_context=ctx_sys,
        )

    # toggle per-group
    svc.toggle_model_enabled_with_group = AsyncMock(
        return_value=SimpleNamespace(key="k", enabled=False)
    )
    out2 = await toggle_model(
        "k", ModelToggleUpdate(enabled=False), service=svc, group_context=ctx_admin
    )
    assert out2.enabled is False

    # toggle global requires admin/system admin
    with pytest.raises(Exception):
        await toggle_global_model(
            "k",
            ModelToggleUpdate(enabled=True),
            service=svc,
            group_context=Ctx(user_role="user"),
        )

    # A workspace admin's effective role does not reach the GLOBAL row (R2-04)…
    with pytest.raises(Exception):
        await toggle_global_model(
            "k", ModelToggleUpdate(enabled=True), service=svc, group_context=ctx_admin
        )
    # …a system admin's does.
    ctx_system = Ctx(user_role="admin")
    ctx_system.current_user.is_system_admin = True
    svc.toggle_global_enabled = AsyncMock(
        return_value=SimpleNamespace(key="k", enabled=True)
    )
    out3 = await toggle_global_model(
        "k", ModelToggleUpdate(enabled=True), service=svc, group_context=ctx_system
    )
    assert out3.enabled is True

    # delete forbidden for non-admin
    with pytest.raises(Exception):
        await delete_model("k", service=svc, group_context=ctx_user)

    # delete not found -> 404
    svc.delete_model_config = AsyncMock(return_value=False)
    with pytest.raises(Exception):
        await delete_model("missing", service=svc, group_context=ctx_sys)

    # enable-all/disable-all require admin
    with pytest.raises(Exception):
        await enable_all_models(service=svc, group_context=ctx_user)
    with pytest.raises(Exception):
        await disable_all_models(service=svc, group_context=ctx_user)

    # enable/disable all success for admin
    now2 = __import__("datetime").datetime.utcnow()
    models2 = [
        SimpleNamespace(
            id=2,
            key="m2",
            name="M2",
            provider="openai",
            enabled=False,
            created_at=now2,
            updated_at=now2,
        )
    ]
    svc.enable_all_models = AsyncMock(return_value=models2)
    out4 = await enable_all_models(service=svc, group_context=ctx_sys)
    assert out4.count == 1

    svc.disable_all_models = AsyncMock(return_value=models2)
    out5 = await disable_all_models(service=svc, group_context=ctx_sys)
    assert out5.count == 1


@pytest.mark.asyncio
async def test_workspace_admin_cannot_change_the_global_catalog():
    """Audit H2: every key-resolved write and the bulk toggles are system-admin only."""
    from src.core.exceptions import ForbiddenError

    svc = AsyncMock()
    ws_admin = Ctx(user_role="admin")
    calls = [
        create_model(
            ModelConfigCreate(key="k", name="n", provider="openai"),
            service=svc,
            group_context=ws_admin,
        ),
        update_model(
            "k",
            ModelConfigUpdate(key="k", name="n", provider="attacker"),
            service=svc,
            group_context=ws_admin,
        ),
        delete_model("k", service=svc, group_context=ws_admin),
        enable_all_models(service=svc, group_context=ws_admin),
        disable_all_models(service=svc, group_context=ws_admin),
    ]
    for call in calls:
        with pytest.raises(ForbiddenError) as exc:
            await call
        assert "system admins" in exc.value.detail
    svc.create_model_config.assert_not_called()
    svc.update_model_config.assert_not_called()
    svc.delete_model_config.assert_not_called()
    svc.enable_all_models.assert_not_called()
    svc.disable_all_models.assert_not_called()


@pytest.mark.asyncio
async def test_workspace_admin_keeps_the_per_workspace_toggle():
    svc = AsyncMock()
    svc.toggle_model_enabled_with_group = AsyncMock(
        return_value=SimpleNamespace(key="k", enabled=False)
    )
    ws_admin = Ctx(user_role="admin")
    out = await toggle_model(
        "k", ModelToggleUpdate(enabled=False), service=svc, group_context=ws_admin
    )
    assert out.enabled is False
    svc.toggle_model_enabled_with_group.assert_awaited_once_with("k", False, ws_admin)
