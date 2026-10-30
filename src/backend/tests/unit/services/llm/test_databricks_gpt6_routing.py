"""Databricks GPT-6 endpoints route through the OpenAI Responses API.

Databricks lists databricks-gpt-6-sol, -luna and -astra as Responses API models
(query-openai-responses#supported-models), and says Sol/Luna are not supported
in AI Playground. The list exists twice — in the backend manager and in the
exported app's llm_factory template — and the two must stay in step, or a crew
that works in Kasal breaks once exported.

Kept in its own file because test_llm_manager.py and
test_databricks_app_exporter.py are both over the 1500-line ceiling.
"""

import ast
from pathlib import Path

import pytest

from src.services.llm.manager import (
    _DATABRICKS_OPENAI_RESPONSES_MODELS,
    _uses_databricks_responses_api,
)

GPT6 = ("databricks-gpt-6-sol", "databricks-gpt-6-luna", "databricks-gpt-6-astra")

_TEMPLATE = (
    Path(__file__).resolve().parents[4]
    / "src/services/export/templates/databricks_app/agent_server/llm_factory.py"
)


def _template_models() -> frozenset:
    """Read the template's constant without importing it (it holds {{...}}
    placeholders, so it is not valid Python until export fills them)."""
    source = _TEMPLATE.read_text()
    start = source.index("_DATABRICKS_OPENAI_RESPONSES_MODELS = frozenset(")
    end = source.index("\n)\n", start) + 2
    tree = ast.parse(source[start:end])
    call = tree.body[0].value
    return frozenset(ast.literal_eval(call.args[0]))


@pytest.mark.parametrize("model", GPT6)
def test_backend_routes_gpt6_to_responses(model):
    assert _uses_databricks_responses_api(model)
    assert _uses_databricks_responses_api(f"databricks/{model}")


def test_exported_app_lists_the_same_models_as_the_backend():
    assert _template_models() == _DATABRICKS_OPENAI_RESPONSES_MODELS
    assert set(GPT6) <= _template_models()
