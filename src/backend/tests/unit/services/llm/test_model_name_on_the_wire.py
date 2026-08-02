"""The model name Kasal sends must be the name the endpoint serves.

Every provider branch in ``configure_kasal_llm`` builds a ``prefixed_model``,
and the prefixes are litellm-era routing hints. litellm is not on this path —
the transport drives endpoints with the OpenAI SDK and an explicit ``api_base``
— so a prefix is either stripped back off or sent verbatim to a server that
never asked for it.

``LLM._split_provider_prefix`` strips one only when it is in ``_KNOWN_PREFIXES``
AND only when it is exactly ``"openai"``. That left two live defects, both
silent at build time:

* ``deepseek/`` is in neither set, so it reached the wire and DeepSeek rejected
  100% of calls — "The supported API model names are deepseek-v4-pro or
  deepseek-v4-flash, but you passed deepseek/deepseek-v4-flash." The models were
  fine; they simply looked discontinued.
* ``openai/`` on the self-hosted vLLM branch WAS stripped, but the stripping is
  what sets ``provider``, so a locally served Qwen reported itself as an OpenAI
  model in every log, repr and trace.

Both are now bare names, exactly like the OpenAI branch that always was.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.llm.manager import LLMManager
from tests.unit.services.llm.test_llm_manager import (
    _make_model_config,
    _patch_session_and_config,
)

#: Which class each provider is actually constructed with. Self-hosted vLLM gets
#: a subclass (it states ``tool_choice`` explicitly), so patching plain ``LLM``
#: for it would assert on a constructor that is never called.
_CONSTRUCTOR = {
    "vllm": "src.services.llm.manager.VLLMFunctionCallingLLM",
}


async def _built(name, provider):
    """The kwargs configure_kasal_llm would hand to the LLM constructor."""
    p_session, p_service = _patch_session_and_config(_make_model_config(name, provider))
    with (
        p_session,
        p_service,
        patch(
            "src.services.llm.manager.ApiKeysService.get_provider_api_key",
            new_callable=AsyncMock,
            return_value="a-key",
        ),
        patch(_CONSTRUCTOR.get(provider, "src.services.llm.manager.LLM")) as MockLLM,
    ):
        await LLMManager.configure_kasal_llm(name, "group-1", None)
        MockLLM.assert_called_once()
        return MockLLM.call_args[1]


class TestDeepSeek:
    @pytest.mark.asyncio
    async def test_the_name_carries_no_provider_prefix(self):
        """The regression that made every DeepSeek call a 400."""
        built = await _built("deepseek-v4-flash", "deepseek")

        assert built["model"] == "deepseek-v4-flash"

    @pytest.mark.asyncio
    async def test_the_pro_model_too(self):
        built = await _built("deepseek-v4-pro", "deepseek")

        assert built["model"] == "deepseek-v4-pro"


class TestSelfHostedVLLM:
    @pytest.mark.asyncio
    async def test_a_self_hosted_model_is_not_labelled_openai(self):
        """vLLM speaks the OpenAI protocol; it is not OpenAI. The prefix existed
        only to be stripped, and setting `provider` was its side effect."""
        built = await _built("Qwen3-Coder-30B-A3B-Instruct", "vllm")

        assert built["model"] == "Qwen3-Coder-30B-A3B-Instruct"
        assert built.get("provider") is None

    @pytest.mark.asyncio
    async def test_the_served_name_must_match_exactly(self):
        """vLLM is launched with --served-model-name and rejects anything else,
        so this is the one branch where a stray prefix is unrecoverable."""
        built = await _built("some-locally-served-model", "vllm")

        assert built["model"] == "some-locally-served-model"
        assert "/" not in built["model"]
