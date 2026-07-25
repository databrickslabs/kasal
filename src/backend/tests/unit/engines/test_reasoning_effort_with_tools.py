"""OpenAI's GPT-5.6 line rejects `reasoning_effort` alongside function tools.

A crew configured with reasoning and any tool died on a hard 400 from the very
first LLM call::

    Function tools with reasoning_effort are not supported for gpt-5.6-terra in
    /v1/chat/completions. To use function tools, use /v1/responses or set
    reasoning_effort to 'none'.

Because every crew agent that does real work has tools, this made the whole
GPT-5.6 family unusable for crew runs the moment reasoning was switched on. The
fix sends the effort the API asks for ("none") on tool-carrying calls, and only
for the affected models — the Databricks-served gpt-5* endpoints accept effort
alongside tools, and dropping it there would silently disable working reasoning.
"""
import pytest

from kasal_engine.llm.completion import OpenAICompletion


TOOLS = [
    {
        "type": "function",
        "function": {"name": "search", "description": "search", "parameters": {}},
    }
]
MESSAGES = [{"role": "user", "content": "hi"}]


def _llm(model, effort="high"):
    llm = OpenAICompletion(model=model)
    llm.reasoning_effort = effort
    return llm


@pytest.mark.parametrize("model", ["gpt-5.6-terra", "gpt-5.6-sol", "gpt-5.6-luna"])
def test_effort_becomes_none_when_tools_are_present(model):
    params = _llm(model)._prepare_completion_params(MESSAGES, TOOLS)
    assert params["reasoning_effort"] == "none"


@pytest.mark.parametrize("model", ["gpt-5.6-terra", "openai/gpt-5.6-terra"])
def test_provider_prefix_is_tolerated(model):
    params = _llm(model)._prepare_completion_params(MESSAGES, TOOLS)
    assert params["reasoning_effort"] == "none"


@pytest.mark.parametrize("model", ["gpt-5.6-terra", "gpt-5.6-sol"])
def test_effort_is_preserved_when_there_are_no_tools(model):
    """Tool-free calls to the same model keep the configured reasoning budget."""
    for tools in (None, []):
        params = _llm(model)._prepare_completion_params(MESSAGES, tools)
        assert params["reasoning_effort"] == "high"


@pytest.mark.parametrize(
    "model",
    [
        "databricks-gpt-5-3-codex",
        "databricks/gpt-5-2",
        "gpt-5",
        "o3-mini",
    ],
)
def test_other_reasoning_models_keep_their_effort_with_tools(model):
    """The workaround is scoped: it must not disable reasoning that works."""
    params = _llm(model)._prepare_completion_params(MESSAGES, TOOLS)
    assert params["reasoning_effort"] == "high"


def test_unset_effort_stays_absent():
    """A model with no configured effort sends no `reasoning_effort` at all."""
    params = _llm("gpt-5.6-terra", effort=None)._prepare_completion_params(
        MESSAGES, TOOLS
    )
    assert "reasoning_effort" not in params
