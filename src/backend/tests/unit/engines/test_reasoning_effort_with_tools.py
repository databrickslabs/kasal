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

Second pass (execution 68de50f0): the same 400 killed every CHAT-mode run on
gpt-5.6-sol even though kasal had configured NO effort at all — the LLM logged
`reasoning_effort=None`, so the parameter was omitted entirely. Omitting it is
not the same as sending "none": the model applies a reasoning budget server-side
by default, and that default is what the tool-carrying call is refused for. The
value has to be stated explicitly whenever tools are present.
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


def test_unset_effort_is_stated_as_none_when_tools_are_present():
    """Inverted deliberately — this test asserted the behaviour that broke chat.

    It previously required that a gpt-5.6 model with no configured effort send
    NOTHING alongside tools. That is what execution 68de50f0 actually did, and
    the API still refused it: the model applies a default budget server-side, so
    silence is not neutrality. "none" must be explicit whenever tools are sent.
    """
    params = _llm("gpt-5.6-terra", effort=None)._prepare_completion_params(
        MESSAGES, TOOLS
    )
    assert params["reasoning_effort"] == "none"


def test_unset_effort_stays_absent_without_tools():
    """Without tools there is no conflict, so nothing is imposed."""
    params = _llm("gpt-5.6-terra", effort=None)._prepare_completion_params(MESSAGES)
    assert "reasoning_effort" not in params


class TestEffortMustBeStatedNotOmitted:
    """The regression from execution 68de50f0: no configured effort at all."""

    @pytest.mark.parametrize("model", ["gpt-5.6-terra", "gpt-5.6-sol", "gpt-5.6-luna"])
    def test_none_is_sent_even_when_no_effort_is_configured(self, model):
        llm = OpenAICompletion(model=model)  # reasoning_effort stays None
        params = llm._prepare_completion_params(MESSAGES, TOOLS)
        assert params["reasoning_effort"] == "none", (
            "omitting the param lets the model's server-side default apply, "
            "which is exactly what the API refuses alongside function tools"
        )

    @pytest.mark.parametrize("model", ["gpt-5.6-terra", "gpt-5.6-sol"])
    def test_nothing_is_sent_without_tools(self, model):
        """No tools, no conflict — the model keeps its own default."""
        llm = OpenAICompletion(model=model)
        assert "reasoning_effort" not in llm._prepare_completion_params(MESSAGES)

    def test_unaffected_models_still_send_nothing(self):
        """Only the gpt-5.6 line needs this; others must not be touched."""
        llm = OpenAICompletion(model="gpt-4o")
        assert "reasoning_effort" not in llm._prepare_completion_params(MESSAGES, TOOLS)

    def test_databricks_gpt5_keeps_its_configured_effort_with_tools(self):
        """Those endpoints accept effort alongside tools — silently dropping it
        would disable reasoning that works."""
        llm = OpenAICompletion(model="databricks/databricks-gpt-5-4")
        llm.reasoning_effort = "high"
        params = llm._prepare_completion_params(MESSAGES, TOOLS)
        assert params["reasoning_effort"] == "high"
