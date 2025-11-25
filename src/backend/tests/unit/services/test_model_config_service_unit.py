import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.services.model_config_service import ModelConfigService


def mk_model(key="k", name="N", provider="openai", enabled=True, group_id=None,
             temperature=0.1, context_window=8192, max_output_tokens=2048, extended_thinking=False):
    return SimpleNamespace(
        key=key, name=name, provider=provider, enabled=enabled, group_id=group_id,
        temperature=temperature, context_window=context_window,
        max_output_tokens=max_output_tokens, extended_thinking=extended_thinking,
        id=1
    )


@pytest.mark.asyncio
async def test_basic_find_and_update_and_delete():
    svc = ModelConfigService(session=SimpleNamespace())
    repo = svc.repository = AsyncMock()

    # find_all / find_enabled_models / find_by_key
    repo.find_all = AsyncMock(return_value=[mk_model("a"), mk_model("b", enabled=False)])
    repo.find_enabled_models = AsyncMock(return_value=[mk_model("a")])
    repo.find_by_key = AsyncMock(side_effect=[mk_model("a"), None])

    out_all = await svc.find_all()
    assert len(out_all) == 2
    out_en = await svc.find_enabled_models()
    assert len(out_en) == 1 and out_en[0].key == "a"
    assert (await svc.find_by_key("a")).key == "a"
    assert await svc.find_by_key("missing") is None

    # create: reject duplicate, then create
    repo.find_by_key = AsyncMock(return_value=mk_model("a"))
    class Dummy:
        def __init__(self): self.key = "a"
        def model_dump(self): return {"key": self.key}
    with pytest.raises(ValueError):
        await svc.create_model_config(Dummy())

    repo.find_by_key = AsyncMock(return_value=None)
    repo.create = AsyncMock(return_value=mk_model("c"))
    created = await svc.create_model_config(Dummy())
    assert created.key == "c"

    # update: missing returns None; then success path updates by id
    repo.find_by_key = AsyncMock(return_value=None)
    assert await svc.update_model_config("nope", {}) is None
    repo.find_by_key = AsyncMock(return_value=mk_model("d"))
    repo.update = AsyncMock(return_value=mk_model("d", name="NN"))
    upd = await svc.update_model_config("d", {"name": "NN"})
    assert upd.name == "NN"

    # toggle_model_enabled: not found -> None; then success -> returns find_by_key
    repo.toggle_enabled = AsyncMock(return_value=None)
    assert await svc.toggle_model_enabled("x", True) is None
    repo.toggle_enabled = AsyncMock(return_value=True)
    repo.find_by_key = AsyncMock(return_value=mk_model("x", enabled=True))
    assert (await svc.toggle_model_enabled("x", True)).enabled is True

    # delete by key pass-through
    repo.delete_by_key = AsyncMock(return_value=True)
    assert await svc.delete_model_config("x") is True


@pytest.mark.asyncio
async def test_enable_disable_all_models_and_global_toggle():
    svc = ModelConfigService(session=SimpleNamespace())
    repo = svc.repository = AsyncMock()

    # enable_all_models: returns find_all list
    repo.enable_all_models = AsyncMock(return_value=True)
    repo.find_all = AsyncMock(return_value=[mk_model("a"), mk_model("b")])
    out = await svc.enable_all_models()
    assert len(out) == 2

    # disable_all_models: returns find_all list
    repo.disable_all_models = AsyncMock(return_value=True)
    repo.find_all = AsyncMock(return_value=[mk_model("a", enabled=False)])
    out2 = await svc.disable_all_models()
    assert len(out2) == 1 and out2[0].enabled is False

    # find_all_global
    repo.find_all_global = AsyncMock(return_value=[mk_model("g")])
    assert len(await svc.find_all_global()) == 1

    # toggle_global_enabled
    repo.toggle_global_enabled = AsyncMock(return_value=None)
    assert await svc.toggle_global_enabled("g", False) is None
    repo.toggle_global_enabled = AsyncMock(return_value=True)
    repo.find_global_by_key = AsyncMock(return_value=mk_model("g", enabled=False))
    out3 = await svc.toggle_global_enabled("g", False)
    assert out3.enabled is False


@pytest.mark.asyncio
async def test_get_model_config_paths_repo_and_fallback_and_auth():
    # repo hit path for databricks provider (no api_key added)
    svc = ModelConfigService(session=SimpleNamespace(), group_id="gid")
    repo = svc.repository = AsyncMock()
    repo.find_by_key = AsyncMock(return_value=mk_model("dbx", provider="databricks"))
    cfg = await svc.get_model_config("databricks/dbx")
    assert cfg["provider"] == "databricks" and "api_key" not in cfg

    # fallback path via utility with non-dbx provider -> adds API key
    from src.services import model_config_service as module
    module.get_model_config = lambda key: {"key": key, "name": "N", "provider": "openai",
                                           "temperature": 0.1, "context_window": 1, "max_output_tokens": 2,
                                           "extended_thinking": False, "enabled": True}
    repo.find_by_key = AsyncMock(return_value=None)
    # Patch ApiKeysService class to control API key returns
    class FakeKeysSvc:
        @staticmethod
        async def get_provider_api_key(provider: str, group_id=None):
            return "KEY"
    module.ApiKeysService = FakeKeysSvc
    cfg2 = await svc.get_model_config("openai/gpt-4o-mini")
    assert cfg2["api_key"] == "KEY" and cfg2["key"].endswith("gpt-4o-mini")

    # unified auth fallback when API key missing: allow pass-through
    class FakeKeysSvc2:
        @staticmethod
        async def get_provider_api_key(provider: str, group_id=None):
            return None
    module.ApiKeysService = FakeKeysSvc2
    # Patch actual function in src.utils.databricks_auth so in-function import sees it
    from src.utils import databricks_auth as auth_mod
    async def fake_auth():
        return SimpleNamespace(auth_method="obo")
    auth_mod.get_auth_context = fake_auth
    cfg3 = await svc.get_model_config("openai/gpt-4o-mini")
    assert cfg3["provider"] == "openai"


@pytest.mark.asyncio
async def test_group_aware_listing_and_toggle_with_group():
    from src.utils.user_context import GroupContext

    svc = ModelConfigService(session=SimpleNamespace())
    repo = svc.repository = AsyncMock()

    # Listing with override
    default = mk_model("k1", group_id=None, enabled=True)
    override = mk_model("k1", group_id="g1", enabled=False)
    other = mk_model("k2", group_id=None, enabled=True)
    repo.find_all = AsyncMock(return_value=[default, override, other])
    gc = GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin")
    eff = await svc.find_all_for_group(gc)
    assert any(m.key == "k1" and m.group_id == "g1" for m in eff)
    eff_en = await svc.find_enabled_models_for_group(gc)
    # Only other remains enabled, override disables k1
    assert [m.key for m in eff_en] == ["k2"]

    # toggle with group: default -> create group copy
    repo.find_all = AsyncMock(return_value=[mk_model("k3", group_id=None, enabled=True)])
    repo.find_by_key_and_group = AsyncMock(return_value=None)
    repo.create = AsyncMock(return_value=mk_model("k3", group_id="g1", enabled=False))
    out = await svc.toggle_model_enabled_with_group("k3", enabled=False, group_context=gc)
    assert out.group_id == "g1" and out.enabled is False

    # toggle with group: default -> toggle existing group override
    repo.find_all = AsyncMock(return_value=[mk_model("k3", group_id=None, enabled=True)])
    repo.find_by_key_and_group = AsyncMock(return_value=mk_model("k3", group_id="g1", enabled=True))
    repo.toggle_enabled_in_group = AsyncMock(return_value=True)
    repo.find_by_key_and_group = AsyncMock(return_value=mk_model("k3", group_id="g1", enabled=False))
    out2 = await svc.toggle_model_enabled_with_group("k3", enabled=False, group_context=gc)
    assert out2.enabled is False

    # toggle with group: other group's model and no default -> returns None
    repo.find_all = AsyncMock(return_value=[mk_model("k4", group_id="g2", enabled=True)])
    assert await svc.toggle_model_enabled_with_group("k4", False, gc) is None

    # toggle with group: group tool toggled within its scope
    repo.find_all = AsyncMock(return_value=[mk_model("k5", group_id="g1", enabled=True)])
    repo.toggle_enabled_in_group = AsyncMock(return_value=True)
    repo.find_by_key_and_group = AsyncMock(return_value=mk_model("k5", group_id="g1", enabled=False))
    out3 = await svc.toggle_model_enabled_with_group("k5", False, gc)
    assert out3.enabled is False

    # toggle with group: requires group context
    repo.find_all = AsyncMock(return_value=[mk_model("k5", group_id=None, enabled=True)])
    with pytest.raises(Exception) as ei2:
        await svc.toggle_model_enabled_with_group("k5", False, None)
    assert getattr(ei2.value, "status_code", None) == 403

