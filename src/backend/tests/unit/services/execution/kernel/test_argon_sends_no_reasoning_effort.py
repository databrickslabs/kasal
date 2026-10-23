"""Gemini 4 Argon's Databricks endpoint 400s on ``reasoning_effort`` at any value.

Every crew run on Argon fell back to another model ("model fallback (fatal_4xx)")
because the execution effort set ``reasoning_effort`` on the agent's LLM. Gemini
3.8 still takes it, which is what proves the setter itself still works.
"""

from types import SimpleNamespace

import pytest

from src.services.execution.kernel.agent_effort import apply_execution_effort


def _llm(model):
    return SimpleNamespace(
        model=model,
        max_tokens=65536,
        max_completion_tokens=None,
        reasoning_effort=None,
        thinking_effort=None,
    )


@pytest.mark.parametrize("tier", ["low", "medium", "high"])
def test_argon_gets_no_reasoning_effort(tier):
    llm = _llm("databricks/databricks-gemini-4-argon")
    apply_execution_effort(
        llm, {"execution_effort": {"tier": tier}}, catalog_output_cap=65536
    )
    assert llm.reasoning_effort is None
    assert llm.thinking_effort is None


def test_gemini_38_still_gets_reasoning_effort():
    llm = _llm("databricks/databricks-gemini-3-8-flash")
    apply_execution_effort(
        llm, {"execution_effort": {"tier": "high"}}, catalog_output_cap=65536
    )
    assert llm.reasoning_effort == "high"
