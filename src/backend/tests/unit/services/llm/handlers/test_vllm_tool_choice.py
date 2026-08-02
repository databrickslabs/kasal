"""The vLLM handler declares a tool policy; it does not decide for the model.

This class used to pin ``tool_choice="required"`` while no tool result was in
the history, so the model HAD to call something on the opening turn. That cannot
distinguish one request from another — the endpoint sees the same thing whether
the turn is "gather swiss news from today" or "hello how are you" — and the
greeting called a tool and invented its arguments. Measured against the live
endpoint at 3 samples per cell: ``required`` called a tool 3/3 on a greeting,
``auto`` 0/3 on a greeting and 3/3 on an explicit search request.

It now states ``auto``: the model MAY call its tools and decides each turn. That
is what an OpenAI-compatible server already defaults to when tools are present,
so this changes nothing on the wire by itself — the point is that the policy is
explicit and in one place, rather than inherited from whatever the endpoint
happens to do.
"""

from unittest.mock import patch

from src.services.llm.handlers.vllm import VLLMFunctionCallingLLM

TOOLS = [{"type": "function", "function": {"name": "PerplexityTool"}}]
ASK = [{"role": "user", "content": "gather swiss news from today"}]


def _params(messages=ASK, tools=TOOLS, **env):
    with patch.dict("os.environ", env, clear=False):
        llm = VLLMFunctionCallingLLM(model="Qwen3-Coder-30B-A3B-Instruct")
        return llm._prepare_completion_params(messages, tools=tools)


class TestTheModelDecides:
    def test_auto_is_sent_when_tools_are_offered(self):
        assert _params()["tool_choice"] == "auto"

    def test_a_greeting_gets_the_same_policy_as_a_search(self):
        """The regression that motivated the change: the handler cannot tell
        these apart, so it must not try — it states the same thing for both and
        lets the model choose."""
        greeting = _params(messages=[{"role": "user", "content": "hello how are you"}])

        assert greeting["tool_choice"] == "auto"
        assert greeting["tool_choice"] == _params()["tool_choice"]

    def test_it_does_not_change_across_turns(self):
        """The old rule released after the first tool result, so turn 1 and
        turn 2 disagreed. Nothing about turn N should change turn N+1."""
        after_a_tool = [
            *ASK,
            {"role": "assistant", "tool_calls": [{"id": "1"}]},
            {"role": "tool", "tool_call_id": "1", "content": "three articles"},
        ]

        assert _params(messages=after_a_tool)["tool_choice"] == "auto"


class TestItNeverOverridesTheCaller:
    def test_no_tools_means_no_tool_choice(self):
        """Naming a policy for tools that were not offered is meaningless, and
        some endpoints reject it outright."""
        assert "tool_choice" not in _params(tools=None)

    def test_an_explicit_tool_choice_survives(self):
        """Structured output, guardrails and a caller naming one specific tool
        all pin tool_choice themselves, and each outranks an endpoint default."""
        pinned = {"type": "function", "function": {"name": "PerplexityTool"}}
        llm = VLLMFunctionCallingLLM(model="m")
        llm.additional_params["tool_choice"] = pinned

        params = llm._prepare_completion_params(ASK, tools=TOOLS)

        assert params["tool_choice"] == pinned


class TestPerDeploymentOverride:
    def test_the_value_can_be_changed(self):
        """One deployment restoring the old behaviour must not require a code
        change — and must be visible as a deliberate choice when it happens."""
        assert _params(VLLM_TOOL_CHOICE="required")["tool_choice"] == "required"

    def test_it_can_be_switched_off_entirely(self):
        """Falsy or 'default' sends nothing and lets the server decide."""
        assert "tool_choice" not in _params(VLLM_TOOL_CHOICE="default")
        assert "tool_choice" not in _params(VLLM_TOOL_CHOICE="")
