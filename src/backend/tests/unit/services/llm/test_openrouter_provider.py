"""The ``openrouter`` LLM provider and the seeded Jev Router model."""

from unittest.mock import AsyncMock, patch

import pytest

from src.core.llm.model_capabilities import SAMPLING_PARAMS, refused_params
from src.schemas.model_provider import ModelProvider
from src.seeds.model_configs import DEFAULT_MODELS
from src.services.llm.manager import LLMManager
from src.services.settings import engine_settings as es
from src.services.settings.api_keys import ApiKeysService


def _config(name, extra=None):
    return {
        "key": name.rsplit("/", 1)[-1],
        "name": name,
        "provider": "openrouter",
        "temperature": 0.7,
        "context_window": 1000000,
        "max_output_tokens": 32768,
        **(extra or {}),
    }


def _patches(config, key="or-key"):
    session = AsyncMock()
    ctx = AsyncMock()
    ctx.__aenter__.return_value = session
    service = AsyncMock()
    service.get_model_config.return_value = config
    return (
        patch("src.db.session.routed_scoped_session", return_value=ctx),
        patch("src.services.llm.manager.ModelConfigService", return_value=service),
        patch(
            "src.services.llm.manager.ApiKeysService.get_provider_api_key",
            new_callable=AsyncMock,
            return_value=key,
        ),
        patch("src.services.llm.manager.LLM"),
    )


async def _configure(config, temperature=0.3, key="or-key"):
    p_session, p_service, p_key, p_llm = _patches(config, key)
    with p_session, p_service, p_key as get_key, p_llm as llm:
        await LLMManager.configure_kasal_llm(config["key"], "ws-1", temperature)
    return get_key, llm.call_args.kwargs


def test_provider_is_known():
    assert ModelProvider("openrouter") is ModelProvider.OPENROUTER


@pytest.mark.asyncio
async def test_jev_router_goes_bare_to_openrouter_with_the_workspace_key(
    monkeypatch,
):
    monkeypatch.delitem(es._snapshot, es.OPENROUTER_API_BASE, raising=False)
    monkeypatch.delitem(es._snapshot, es.JEV_API_BASE, raising=False)
    get_key, kwargs = await _configure(_config("typesafe/jev-router"))
    get_key.assert_awaited_once_with(ModelProvider.OPENROUTER, group_id="ws-1")
    assert kwargs["model"] == "typesafe/jev-router"
    assert kwargs["provider"] == "openrouter"
    assert kwargs["api_key"] == "or-key"
    assert kwargs["api_base"] == "https://openrouter.ai/api/v1"
    # supported_parameters: [] -- no sampling knob is sent.
    assert not set(kwargs) & set(SAMPLING_PARAMS)


@pytest.mark.asyncio
async def test_api_base_follows_the_configured_openrouter_url(monkeypatch):
    monkeypatch.setitem(
        es._snapshot, es.OPENROUTER_API_BASE, "https://gateway.example.com/v1"
    )
    _, kwargs = await _configure(_config("typesafe/jev-router"))
    assert kwargs["api_base"] == "https://gateway.example.com/v1"
    # A per-model endpoint (Configuration -> Models) wins.
    _, kwargs = await _configure(
        _config(
            "typesafe/jev-router", {"params": {"api_base": "https://m.example.com"}}
        )
    )
    assert kwargs["api_base"] == "https://m.example.com"


@pytest.mark.asyncio
async def test_other_openrouter_models_keep_their_vendor_prefix_and_temperature():
    """ "openai/..." must not be read as the transport's own openai/ prefix."""
    _, kwargs = await _configure(_config("mistralai/mistral-large"), temperature=0.2)
    assert kwargs["model"] == "mistralai/mistral-large"
    assert kwargs["provider"] == "openrouter"
    assert kwargs["temperature"] == 0.2
    assert kwargs["max_tokens"] == 32768


def test_the_transport_keeps_an_openrouter_vendor_prefix():
    from src.core.llm.transport import LLM

    llm = LLM(model="anthropic/claude-x", provider="openrouter", api_key="k")
    assert llm.model == "anthropic/claude-x" and llm.provider == "openrouter"


@pytest.mark.asyncio
async def test_missing_key_is_a_clear_error():
    with pytest.raises(ValueError, match="OPENROUTER_API_KEY"):
        await _configure(_config("typesafe/jev-router"), key=None)


@pytest.mark.asyncio
async def test_key_lookup_uses_openrouter_api_key_for_the_workspace():
    ctx = AsyncMock()
    ctx.__aenter__.return_value = AsyncMock()
    scopes = []
    original = ApiKeysService.__init__

    def spy(self, session, group_id=None):
        scopes.append(group_id)
        original(self, session, group_id=group_id)

    with (
        patch("src.db.session.routed_scoped_session", return_value=ctx),
        patch.object(ApiKeysService, "__init__", spy),
        patch.object(
            ApiKeysService, "find_by_name", new=AsyncMock(return_value=None)
        ) as find,
    ):
        assert (
            await ApiKeysService.get_provider_api_key("openrouter", group_id="ws-9")
            is None
        )
    assert scopes == ["ws-9"]
    find.assert_awaited_once_with("OPENROUTER_API_KEY")


def test_jev_router_is_seeded_disabled_with_no_sampling_params():
    seed = DEFAULT_MODELS["jev-router"]
    assert seed["name"] == "typesafe/jev-router"
    assert seed["provider"] == "openrouter"
    # Only Databricks models are enabled on insert.
    assert seed["provider"] != "databricks"
    assert seed["context_window"] == 1000000
    assert set(refused_params(seed["name"])) == set(SAMPLING_PARAMS)
