"""The last line of defence: "auto" never becomes a model call.

Every entry point resolves Auto before it builds a model (``run_freeze``,
``resolve_dispatch_model``). ``LLMManager.configure_kasal_llm`` is where every
model key becomes an LLM, on both harnesses and for ``completion``, so a leak
is caught there: the workspace default is built instead, and a warning names
the path that leaked it.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.llm.manager import LLMManager
from src.utils.model_config import DEFAULT_ENGINE_MODEL
from tests.unit.services.llm.test_llm_manager import (
    _make_model_config,
    _patch_session_and_config,
)

ENABLED = [SimpleNamespace(key="other"), SimpleNamespace(key=DEFAULT_ENGINE_MODEL)]


async def _configure(model_name):
    p_session, p_service = _patch_session_and_config(
        _make_model_config("served-default", "openai")
    )
    with (
        p_session,
        p_service as service_cls,
        patch(
            "src.services.decisions.model_selection._enabled_models",
            new=AsyncMock(return_value=ENABLED),
        ) as enabled,
        patch(
            "src.services.llm.manager.ApiKeysService.get_provider_api_key",
            new_callable=AsyncMock,
            return_value="a-key",
        ),
        patch("src.services.llm.manager.LLM") as constructor,
    ):
        await LLMManager.configure_kasal_llm(model_name, "group-1", None)
    return service_cls.return_value.get_model_config, constructor, enabled


@pytest.mark.asyncio
async def test_auto_is_built_as_the_workspace_default_and_warns(caplog):
    with caplog.at_level("WARNING"):
        lookup, constructor, enabled = await _configure("auto")
    lookup.assert_awaited_once_with(DEFAULT_ENGINE_MODEL)
    assert enabled.await_args.args[1].group_ids == ["group-1"]
    assert constructor.call_args.kwargs["model"] == "served-default"
    assert "auto" not in str(constructor.call_args).lower()
    warning = next(r for r in caplog.records if "unresolved" in r.getMessage())
    # The stack names the path that leaked it.
    assert "test_auto_safety_net" in (warning.stack_info or "")


@pytest.mark.asyncio
async def test_a_concrete_model_costs_no_extra_lookup(caplog):
    with caplog.at_level("WARNING"):
        lookup, _, enabled = await _configure("served-default")
    lookup.assert_awaited_once_with("served-default")
    enabled.assert_not_called()
    assert not any("unresolved" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_completion_with_auto_never_asks_for_the_auto_model():
    """``completion`` builds through configure_kasal_llm, so the net covers it."""
    built = AsyncMock(side_effect=RuntimeError("stop before any network call"))
    with (
        patch.object(LLMManager, "_get_group_id_from_context", return_value="g"),
        patch.object(LLMManager, "configure_kasal_llm", new=built),
        pytest.raises(RuntimeError),
    ):
        await LLMManager.completion(messages=[], model="auto")
    assert built.await_args.args[:2] == ("auto", "g")
