"""
Unit tests for resolve_guardrail_model — the shared resolver for an LLM
guardrail's model.

Precedence: an explicit per-task choice wins; otherwise the guardrail defaults
to the model its agent runs with (the chat-input selection, top-down), then the
run-level config model, then a hardcoded last resort.
"""
from types import SimpleNamespace

from src.engines.crewai.guardrails.guardrail_model import (
    DEFAULT_GUARDRAIL_MODEL,
    resolve_guardrail_model,
)


def _agent(model=None, model_name=None, has_llm=True):
    """Build a stand-in agent whose .llm exposes .model / .model_name."""
    if not has_llm:
        return SimpleNamespace(llm=None)
    llm = SimpleNamespace()
    if model is not None:
        llm.model = model
    if model_name is not None:
        llm.model_name = model_name
    return SimpleNamespace(llm=llm)


class TestExplicitWins:
    def test_explicit_model_wins_over_agent_and_config(self):
        # A user-picked guardrail model overrides everything.
        assert resolve_guardrail_model(
            "databricks-claude-opus-4",
            _agent(model="agent-model"),
            {"model": "cfg-model"},
        ) == "databricks-claude-opus-4"

    def test_explicit_prefix_stripped(self):
        assert resolve_guardrail_model("databricks/gpt-5", _agent(model="agent-model")) == "gpt-5"

    def test_empty_explicit_falls_through_to_agent(self):
        # "" / whitespace = "use the run model" (the default option in the UI).
        assert resolve_guardrail_model("", _agent(model="agent-model")) == "agent-model"
        assert resolve_guardrail_model("   ", _agent(model="agent-model")) == "agent-model"

    def test_none_explicit_falls_through_to_agent(self):
        assert resolve_guardrail_model(None, _agent(model="agent-model")) == "agent-model"

    def test_non_string_explicit_ignored(self):
        assert resolve_guardrail_model(123, _agent(model="agent-model")) == "agent-model"


class TestDefaultsToRunModel:
    def test_uses_agent_model_when_no_explicit(self):
        assert resolve_guardrail_model(None, _agent(model="databricks-claude-opus-4")) == "databricks-claude-opus-4"

    def test_strips_databricks_provider_prefix(self):
        assert resolve_guardrail_model(None, _agent(model="databricks/databricks-gpt-5")) == "databricks-gpt-5"

    def test_does_not_strip_databricks_name_prefix(self):
        assert resolve_guardrail_model(None, _agent(model="databricks-claude-sonnet-4-5")) == "databricks-claude-sonnet-4-5"

    def test_falls_back_to_model_name_attr(self):
        # DatabricksRetryLLM wrapper exposes model via .model_name.
        assert resolve_guardrail_model(None, _agent(model_name="databricks/run-model")) == "run-model"

    def test_falls_back_to_config_model_when_agent_has_no_llm(self):
        assert resolve_guardrail_model(None, _agent(has_llm=False), {"model": "databricks/cfg-model"}) == "cfg-model"

    def test_falls_back_to_config_model_when_agent_is_none(self):
        assert resolve_guardrail_model(None, None, {"model": "cfg-model"}) == "cfg-model"

    def test_agent_model_preferred_over_config_model(self):
        assert resolve_guardrail_model(None, _agent(model="agent-model"), {"model": "crew-model"}) == "agent-model"

    def test_default_when_nothing_available(self):
        assert resolve_guardrail_model(None, None, None) == DEFAULT_GUARDRAIL_MODEL
        assert resolve_guardrail_model(None, _agent(has_llm=False), {}) == DEFAULT_GUARDRAIL_MODEL

    def test_config_model_non_string_ignored(self):
        assert resolve_guardrail_model(None, None, {"model": 123}) == DEFAULT_GUARDRAIL_MODEL
