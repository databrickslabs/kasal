"""The LLMManager bridge for MLflow's built-in judges.

A real ``mlflow.genai.scorers`` object is given the placeholder model; its
judge call must land on Kasal's ``_sync_llm_completion`` (LLMManager) with the
Kasal model key, while mlflow's own prompt building and input validation
still run. No network: the LLM is a fake.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.services.prompt_optimization.builtin_judges import bridge
from src.services.prompt_optimization.gepa import reflection
from src.utils.user_context import GroupContext


class _FakeLLM:
    def __init__(self, reply=None):
        self.calls = []
        self.reply = reply or json.dumps({"rationale": "fine", "result": "yes"})

    def __call__(self, loop, messages, model, max_tokens, **kwargs):
        self.calls.append(
            {"loop": loop, "messages": messages, "model": model, **kwargs}
        )
        return self.reply


@pytest.fixture
def llm():
    fake = _FakeLLM()
    with patch.object(reflection, "_sync_llm_completion", fake):
        yield fake


def _route(**kwargs):
    return bridge.JudgeRoute(loop=MagicMock(name="main-loop"), **kwargs)


class TestPlaceholder:
    def test_round_trip(self):
        uri = bridge.placeholder_uri("databricks-claude-sonnet")
        assert uri == "openai:/kasal-judge--databricks-claude-sonnet"
        assert bridge.model_key_of(uri) == "databricks-claude-sonnet"

    @pytest.mark.parametrize("uri", ["openai:/gpt-4.1-mini", "databricks", ""])
    def test_real_models_are_not_placeholders(self, uri):
        assert bridge.model_key_of(uri) is None


class TestRouting:
    def test_safety_goes_through_llm_manager_with_the_kasal_key(self, llm):
        from mlflow.genai.scorers import Safety

        group = GroupContext(group_ids=["g1"], group_email="g@example.com")
        route = _route(group_context=group, user_token="tok")
        scorer = Safety(model=bridge.placeholder_uri("judge-key"))
        with bridge.judge_route(route):
            feedback = scorer.run(outputs="A harmless answer.")

        assert str(getattr(feedback.value, "value", feedback.value)) == "yes"
        assert feedback.rationale == "fine"
        (call,) = llm.calls
        assert call["model"] == "judge-key"
        assert call["loop"] is route.loop
        assert call["group_context"] is group
        assert call["user_token"] == "tok"
        # mlflow built the prompt; the bridge only adds the JSON contract.
        prompt = call["messages"][-1]["content"]
        assert "A harmless answer." in prompt
        assert '"result"' in prompt

    @pytest.mark.parametrize(
        "name,kwargs,call_kwargs",
        [
            ("RelevanceToQuery", {}, {"inputs": {"q": "hi"}, "outputs": "hello"}),
            ("Completeness", {}, {"inputs": {"q": "hi"}, "outputs": "hello"}),
            (
                "Guidelines",
                {"guidelines": ["Be polite"]},
                {"inputs": {"q": "hi"}, "outputs": "hello"},
            ),
        ],
    )
    def test_each_phase_one_judge_routes(self, llm, name, kwargs, call_kwargs):
        import mlflow.genai.scorers as scorers

        scorer = getattr(scorers, name)(model=bridge.placeholder_uri("k"), **kwargs)
        with bridge.judge_route(_route()):
            scorer.run(**call_kwargs)
        assert [c["model"] for c in llm.calls] == ["k"]

    def test_mlflows_own_label_check_still_runs_before_any_call(self, llm):
        from mlflow.exceptions import MlflowException
        from mlflow.genai.scorers import Correctness

        scorer = Correctness(model=bridge.placeholder_uri("k"))
        with bridge.judge_route(_route()), pytest.raises(MlflowException):
            scorer.run(inputs={"q": "hi"}, outputs="hello", expectations={})
        assert llm.calls == []

    def test_placeholder_outside_a_route_fails_instead_of_calling_a_provider(self, llm):
        from mlflow.exceptions import MlflowException
        from mlflow.genai.judges.adapters import utils as adapter_utils
        from mlflow.genai.judges.adapters.base_adapter import AdapterInvocationInput

        bridge.install_bridge()
        adapter = adapter_utils.get_adapter(
            model_uri=bridge.placeholder_uri("k"), prompt="p"
        )
        params = AdapterInvocationInput(
            model_uri=bridge.placeholder_uri("k"), prompt="p", assessment_name="x"
        )
        with pytest.raises(MlflowException, match="placeholder"):
            adapter.invoke(params)
        assert llm.calls == []

    def test_other_models_still_get_mlflows_adapter(self):
        from mlflow.genai.judges.adapters import utils as adapter_utils

        bridge.install_bridge()
        adapter = adapter_utils.get_adapter(model_uri="databricks", prompt="p")
        assert type(adapter).__name__ == "DatabricksManagedJudgeAdapter"

    def test_install_is_idempotent(self):
        from mlflow.genai.judges.adapters import utils as adapter_utils

        bridge.install_bridge()
        first = adapter_utils.get_adapter
        bridge.install_bridge()
        assert adapter_utils.get_adapter is first

    def test_the_route_is_cleared_after_the_block(self):
        with bridge.judge_route(_route()):
            assert bridge._ROUTE.get() is not None
        assert bridge._ROUTE.get() is None


class TestParseVerdict:
    @pytest.mark.parametrize(
        "reply,expected",
        [
            ('{"result": "no", "rationale": "toxic"}', ("no", "toxic")),
            ('```json\n{"result": "yes", "rationale": "ok"}\n```', ("yes", "ok")),
            ('Thinking...\n{"rationale": "r", "result": "yes"} done', ("yes", "r")),
        ],
    )
    def test_json_replies(self, reply, expected):
        assert bridge.parse_verdict(reply) == expected

    @pytest.mark.parametrize(
        "reply,expected",
        [
            ("No issues found.\nYes", "yes"),
            ("Reasoning first.\nresult: no", "no"),
            ("Reasoning.\n**No**.", "no"),
        ],
    )
    def test_a_bare_final_line_verdict_is_accepted(self, reply, expected):
        result, rationale = bridge.parse_verdict(reply)
        assert result == expected
        assert rationale == reply

    @pytest.mark.parametrize(
        "reply",
        ["I cannot decide.", "There is no harmful content here.", "yes and no"],
    )
    def test_prose_is_not_a_verdict(self, reply):
        with pytest.raises(ValueError, match="no verdict"):
            bridge.parse_verdict(reply)
