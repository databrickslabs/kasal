import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace

from src.api.models_router import (
    get_models,
    get_enabled_models,
    get_global_models,
    get_model,
    create_model,
    update_model,
    toggle_model,
    toggle_global_model,
    delete_model,
    enable_all_models,
    disable_all_models,
)
from src.schemas.model_config import ModelConfigCreate, ModelConfigUpdate, ModelToggleUpdate


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
    now = __import__('datetime').datetime.utcnow()
    models = [SimpleNamespace(id=1, key="m1", name="M1", provider="openai", enabled=True, created_at=now, updated_at=now)]
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
    ctx_admin = Ctx(user_role="admin")

    # create forbidden
    with pytest.raises(Exception):
        await create_model(ModelConfigCreate(key="k", name="n", provider="openai"), service=svc, group_context=ctx_user)

    # create success
    svc.create_model_config = AsyncMock(return_value=SimpleNamespace(key="k", name="n", provider="openai", enabled=True))
    out = await create_model(ModelConfigCreate(key="k", name="n", provider="openai"), service=svc, group_context=ctx_admin)
    assert out.key == "k"

    # update forbidden
    with pytest.raises(Exception):
        await update_model("k", ModelConfigUpdate(name="n2"), service=svc, group_context=ctx_user)

    # update not found -> 404
    svc.update_model_config = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await update_model("missing", ModelConfigUpdate(name="n2"), service=svc, group_context=ctx_admin)

    # toggle per-group
    svc.toggle_model_enabled_with_group = AsyncMock(return_value=SimpleNamespace(key="k", enabled=False))
    out2 = await toggle_model("k", ModelToggleUpdate(enabled=False), service=svc, group_context=ctx_admin)
    assert out2.enabled is False

    # toggle global requires admin/system admin
    with pytest.raises(Exception):
        await toggle_global_model("k", ModelToggleUpdate(enabled=True), service=svc, group_context=Ctx(user_role="user"))

    # global toggle success using admin
    svc.toggle_global_enabled = AsyncMock(return_value=SimpleNamespace(key="k", enabled=True))
    out3 = await toggle_global_model("k", ModelToggleUpdate(enabled=True), service=svc, group_context=ctx_admin)
    assert out3.enabled is True

    # delete forbidden for non-admin
    with pytest.raises(Exception):
        await delete_model("k", service=svc, group_context=ctx_user)

    # delete not found -> 404
    svc.delete_model_config = AsyncMock(return_value=False)
    with pytest.raises(Exception):
        await delete_model("missing", service=svc, group_context=ctx_admin)

    # enable-all/disable-all require admin
    with pytest.raises(Exception):
        await enable_all_models(service=svc, group_context=ctx_user)
    with pytest.raises(Exception):
        await disable_all_models(service=svc, group_context=ctx_user)

    # enable/disable all success for admin
    now2 = __import__('datetime').datetime.utcnow()
    models2 = [SimpleNamespace(id=2, key="m2", name="M2", provider="openai", enabled=False, created_at=now2, updated_at=now2)]
    svc.enable_all_models = AsyncMock(return_value=models2)
    out4 = await enable_all_models(service=svc, group_context=ctx_admin)
    assert out4.count == 1

    svc.disable_all_models = AsyncMock(return_value=models2)
    out5 = await disable_all_models(service=svc, group_context=ctx_admin)
    assert out5.count == 1

