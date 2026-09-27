"""Contract: the mlflow internals the built-in judge bridge relies on.

``builtin_judges/bridge.py`` wraps ``get_adapter``, an mlflow INTERNAL, in the
two places mlflow reads it. These tests pin that shape against the installed
mlflow. When mlflow is upgraded and one fails, re-check the bridge against the
new internals before changing the test — a silent drift would send judge calls
to LiteLLM with environment credentials, or nowhere.

Pinned to mlflow 3.16 (``uv.lock``). ``pyproject.toml`` allows ``>=3.11,<4``,
so the version assertion is what catches an unreviewed minor upgrade.
"""

import inspect
from unittest.mock import MagicMock, patch

import mlflow

from src.services.prompt_optimization.builtin_judges import bridge
from src.services.prompt_optimization.gepa import reflection


def test_installed_mlflow_is_the_reviewed_minor():
    assert mlflow.__version__.startswith("3.16."), (
        f"mlflow {mlflow.__version__}: re-verify builtin_judges/bridge.py "
        "against this version's judge adapters, then update this pin"
    )


def test_get_adapter_signature_and_both_read_sites():
    from mlflow.genai.judges.adapters import utils as adapter_utils
    from mlflow.genai.judges.utils import invocation_utils

    # (After install this follows the wrapper's __wrapped__ to mlflow's own.)
    params = list(inspect.signature(adapter_utils.get_adapter).parameters)
    assert params == ["model_uri", "prompt"]
    # invoke_judge_model reads the name it imported, so both must be wrapped.
    assert "get_adapter" in vars(invocation_utils)
    source = inspect.getsource(invocation_utils.invoke_judge_model)
    assert "get_adapter(model_uri=model_uri, prompt=prompt)" in source

    bridge.install_bridge()
    for module in (adapter_utils, invocation_utils):
        assert getattr(module.get_adapter, bridge._BRIDGE_FLAG, False), module


def test_adapter_base_class_contract():
    from mlflow.genai.judges.adapters.base_adapter import (
        AdapterInvocationInput,
        AdapterInvocationOutput,
        BaseJudgeAdapter,
    )

    assert {"is_applicable", "_invoke"} <= set(BaseJudgeAdapter.__abstractmethods__)
    fields = set(inspect.signature(AdapterInvocationInput).parameters)
    assert {"model_uri", "prompt", "assessment_name", "response_format"} <= fields
    assert "feedback" in inspect.signature(AdapterInvocationOutput).parameters
    # The adapter defined by the bridge is a concrete subclass.
    adapter = bridge.adapter_class()()
    assert isinstance(adapter, BaseJudgeAdapter)


def test_builtin_judges_call_the_factory_synchronously_on_this_thread():
    """The route travels in a ContextVar; that only works while mlflow calls
    the judge on the calling thread (no executor between run() and the
    adapter). End to end with a real Safety scorer."""
    from mlflow.genai.scorers import Safety

    seen = []

    def fake(loop, messages, model, max_tokens, **kwargs):
        seen.append(bridge._ROUTE.get())
        return '{"result": "yes", "rationale": "ok"}'

    route = bridge.JudgeRoute(loop=MagicMock())
    with patch.object(reflection, "_sync_llm_completion", fake):
        with bridge.judge_route(route):
            Safety(model=bridge.placeholder_uri("k")).run(outputs="text")
    assert seen == [route]
