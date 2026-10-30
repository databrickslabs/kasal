"""DatabricksRetryLLM must not shorten a run the harness allowed.

Three defects, each specific to the CrewAI harness (the default), where every
``call()`` is one round of CrewAI's tool loop and carries the whole history:

* a hard-coded limiter stripped ``tools`` once the history held 8 tool results,
  so every task had to answer after 8 tool calls whatever ``max_iter`` said;
* a fallback model was built without ``delegate_tool_calls``, so its tool
  decisions came back as an empty answer;
* a refreshed token was written to ``api_key`` while the cached SDK client kept
  sending the old one.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from src.core.llm.transport.completion import OpenAICompletion
from src.services.llm.handlers.databricks_retry_llm import DatabricksRetryLLM
from src.services.llm.handlers.model_fallback import ModelCandidate

TOOLS = [{"type": "function", "function": {"name": "search", "parameters": {}}}]


def _history(tool_results: int):
    messages = [{"role": "user", "content": "research"}]
    for i in range(tool_results):
        messages.append(
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": f"c{i}",
                        "type": "function",
                        "function": {"name": "search", "arguments": "{}"},
                    }
                ],
            }
        )
        messages.append({"role": "tool", "tool_call_id": f"c{i}", "content": "r"})
    return messages


@pytest.fixture
def llm():
    with patch.object(DatabricksRetryLLM, "_get_crew_logger", return_value=MagicMock()):
        yield DatabricksRetryLLM(model="databricks/databricks-claude-sonnet-4-6")


class TestToolsAreKeptPastEightResults:
    @pytest.mark.parametrize("tool_results", [8, 9, 24])
    def test_tools_reach_the_endpoint(self, llm, tool_results):
        with patch.object(OpenAICompletion, "call", return_value="ok") as parent:
            llm.call(_history(tool_results), tools=list(TOOLS))
        assert parent.call_args.kwargs["tools"] == TOOLS

    def test_nothing_logs_the_old_limit(self, llm):
        log = MagicMock()
        with patch.object(DatabricksRetryLLM, "_get_crew_logger", return_value=log):
            with patch.object(OpenAICompletion, "call", return_value="ok"):
                llm.call(_history(12), tools=list(TOOLS))
        warnings = " ".join(str(c) for c in log.warning.call_args_list)
        assert "tool results" not in warnings


class TestAFallbackKeepsTheCallContract:
    def _configured(self):
        built = OpenAICompletion(model="databricks-fallback", api_key="x")
        assert built.delegate_tool_calls is False  # configure_kasal_llm's default
        return built

    def test_sync_build_inherits_delegate_tool_calls(self, llm):
        llm.delegate_tool_calls = True
        llm._group_id = "g1"
        built = self._configured()
        with (
            patch(
                "src.services.llm.manager.LLMManager.configure_kasal_llm",
                new=MagicMock(return_value=built),
            ),
            patch(
                "src.services.llm.handlers.databricks_retry_llm._run_coro_sync",
                side_effect=lambda value: value,
            ),
        ):
            fallback = llm._build_fallback_llm(ModelCandidate("databricks-fallback"))
        assert fallback is built
        assert fallback.delegate_tool_calls is True

    def test_async_build_inherits_delegate_tool_calls(self, llm):
        llm.delegate_tool_calls = True
        llm._group_id = "g1"
        built = self._configured()

        async def _configure(*args, **kwargs):
            return built

        with patch(
            "src.services.llm.manager.LLMManager.configure_kasal_llm", new=_configure
        ):
            fallback = asyncio.run(
                llm._abuild_fallback_llm(ModelCandidate("databricks-fallback"))
            )
        assert fallback.delegate_tool_calls is True

    def test_a_kasal_harness_fallback_stays_undelegated(self, llm):
        llm._group_id = "g1"
        built = self._configured()
        with (
            patch(
                "src.services.llm.manager.LLMManager.configure_kasal_llm",
                new=MagicMock(return_value=built),
            ),
            patch(
                "src.services.llm.handlers.databricks_retry_llm._run_coro_sync",
                side_effect=lambda value: value,
            ),
        ):
            fallback = llm._build_fallback_llm(ModelCandidate("databricks-fallback"))
        assert fallback.delegate_tool_calls is False


class TestATokenRefreshReachesTheClient:
    def test_the_cached_client_is_rebuilt_with_the_new_token(self, llm):
        llm.api_key = "old-token"
        assert llm.client.api_key == "old-token"  # built and cached

        auth = MagicMock(token="fresh-token", auth_method="pat")
        with patch("asyncio.get_running_loop", side_effect=RuntimeError("no loop")):
            with patch("asyncio.run", return_value=auth):
                assert llm._try_refresh_token() is True

        assert llm.api_key == "fresh-token"
        assert llm.client.api_key == "fresh-token"
