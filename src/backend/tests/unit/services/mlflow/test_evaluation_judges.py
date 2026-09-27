"""Evaluation judges go through the LLMManager bridge.

The judge model rule, the scorers built on the placeholder model, the
no-judge fallback, and, end to end with a real ``mlflow.genai.evaluate``:
evaluate's scorer worker threads reach LLMManager with the run's Kasal model
key and group. No network: LLMManager is a fake, tracking is a local SQLite
store in a temp dir.
"""

import asyncio
import json
import logging
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.mlflow import evaluation_judges
from src.services.prompt_optimization.builtin_judges import bridge
from src.utils.user_context import GroupContext, UserContext


class _Models:
    """ModelConfigService stand-in: ``find_by_key`` over a fixed set."""

    def __init__(self, *keys):
        self.keys = set(keys)
        self.asked = []

    async def find_by_key(self, key):
        self.asked.append(key)
        return SimpleNamespace(key=key) if key in self.keys else None


class TestResolveJudgeModel:
    @pytest.mark.asyncio
    async def test_configured_judge_wins(self):
        models = _Models("judge-a", "fallback")
        assert (
            await evaluation_judges.resolve_judge_model("judge-a", models) == "judge-a"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "stored", ["databricks/judge-a", "endpoints://judge-a", " judge-a "]
    )
    async def test_legacy_routes_map_to_the_kasal_key(self, stored):
        models = _Models("judge-a")
        assert await evaluation_judges.resolve_judge_model(stored, models) == "judge-a"

    @pytest.mark.asyncio
    async def test_unset_falls_back_to_the_installation_default(self):
        from src.utils.model_config import DEFAULT_ENGINE_MODEL

        models = _Models(DEFAULT_ENGINE_MODEL)
        assert (
            await evaluation_judges.resolve_judge_model(None, models)
            == DEFAULT_ENGINE_MODEL
        )

    @pytest.mark.asyncio
    async def test_unknown_model_skips_judges(self, caplog):
        with caplog.at_level(logging.WARNING):
            out = await evaluation_judges.resolve_judge_model("nope", _Models())
        assert out is None
        assert "Evaluation judges skipped" in caplog.text


class TestJudgeScorers:
    def _route(self):
        return bridge.JudgeRoute(loop=asyncio.new_event_loop())

    def test_scorers_get_the_placeholder_model(self):
        scorers = evaluation_judges.judge_scorers(
            "judge-a", self._route(), has_reference=False, has_context=False
        )
        assert [type(s).__name__ for s in scorers] == ["RelevanceToQuery", "Safety"]
        assert {s.model for s in scorers} == {bridge.placeholder_uri("judge-a")}

    def test_correctness_joins_when_rows_carry_a_reference(self):
        scorers = evaluation_judges.judge_scorers(
            "judge-a", self._route(), has_reference=True, has_context=False
        )
        assert "Correctness" in [type(s).__name__ for s in scorers]

    @pytest.mark.parametrize("model,with_route", [(None, True), ("judge-a", False)])
    def test_no_judge_model_means_no_judges(self, model, with_route, caplog):
        route = self._route() if with_route else None
        with caplog.at_level(logging.WARNING):
            scorers = evaluation_judges.judge_scorers(
                model, route, has_reference=True, has_context=False
            )
        assert scorers == []
        assert "Evaluation judges skipped" in caplog.text

    def test_judging_without_a_route_is_a_no_op(self):
        with evaluation_judges.judging(None):
            assert bridge._ROUTE.get() is None


@pytest.mark.asyncio
async def test_current_judge_route_uses_the_running_loop_and_caller():
    group = GroupContext(group_ids=["g1"], access_token="obo")
    route = evaluation_judges.current_judge_route(group, "g1")
    assert route.loop is asyncio.get_running_loop()
    assert route.group_context is group and route.user_token == "obo"

    bare = evaluation_judges.current_judge_route(None, "g2")
    assert bare.group_context.group_ids == ["g2"] and bare.user_token is None


# ---------------------------------------------------------------------------
# End to end: a real mlflow.genai.evaluate
# ---------------------------------------------------------------------------


@pytest.fixture
def main_loop():
    """The app's event loop, running on its own thread (as under uvicorn)."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
    loop.close()


@pytest.fixture
def local_tracking(tmp_path, monkeypatch):
    import mlflow

    monkeypatch.setenv("MLFLOW_DISABLE_TELEMETRY", "true")
    previous = mlflow.get_tracking_uri()
    mlflow.set_tracking_uri(f"sqlite:///{tmp_path}/mlflow.db")
    mlflow.set_experiment("eval-judges-bridge")
    yield
    mlflow.set_tracking_uri(previous)


@pytest.fixture
def llm_manager():
    """LLMManager.completion, recording who called it and from which thread."""
    calls = []

    async def completion(**kwargs):
        group = UserContext.get_group_context()
        calls.append(
            {
                "model": kwargs["model"],
                "group_ids": getattr(group, "group_ids", None),
                "token": UserContext.get_user_token(),
            }
        )
        return json.dumps({"rationale": "fine", "result": "yes"})

    from src.services.llm.manager import LLMManager
    from src.services.prompt_optimization.gepa import reflection

    real_sync = reflection._sync_llm_completion
    threads = []

    def sync_spy(*args, **kwargs):
        threads.append(threading.current_thread().name)
        return real_sync(*args, **kwargs)

    with (
        patch.object(LLMManager, "completion", AsyncMock(side_effect=completion)),
        patch.object(reflection, "_sync_llm_completion", sync_spy),
    ):
        yield SimpleNamespace(calls=calls, threads=threads)


def _evaluate(route, scorers):
    import mlflow

    data = [
        {"inputs": {"query": "What is 2+2?"}, "outputs": {"response": "4"}},
        {"inputs": {"query": "Capital of France?"}, "outputs": {"response": "Paris"}},
    ]
    with evaluation_judges.judging(route):
        return mlflow.genai.evaluate(data=data, scorers=scorers)


def test_evaluate_workers_reach_llm_manager_with_key_and_group(
    main_loop, local_tracking, llm_manager
):
    group = GroupContext(group_ids=["team-7"], access_token="user-tok")
    route = bridge.JudgeRoute(main_loop, group, "user-tok")
    scorers = evaluation_judges.judge_scorers(
        "judge-key", route, has_reference=False, has_context=False
    )

    result = _evaluate(route, scorers)

    # 2 rows x (RelevanceToQuery, Safety), every one through LLMManager.
    assert len(llm_manager.calls) == 4
    for call in llm_manager.calls:
        assert call["model"] == "judge-key"
        assert call["group_ids"] == ["team-7"]
        assert call["token"] == "user-tok"
    # ...from evaluate's worker threads, not the calling thread.
    assert llm_manager.threads
    assert threading.current_thread().name not in llm_manager.threads
    assert all(name.startswith("MlflowGenAIEval") for name in llm_manager.threads)
    assert result.metrics  # verdicts were aggregated


def test_without_propagation_the_workers_do_not_see_the_route(
    main_loop, local_tracking, llm_manager
):
    """Why ``judging`` opts into sp_auth's context copying: with the route set
    only on the calling thread, evaluate's workers never reach LLMManager."""
    import mlflow

    route = bridge.JudgeRoute(main_loop, GroupContext(group_ids=["g"]))
    scorers = evaluation_judges.judge_scorers(
        "judge-key", route, has_reference=False, has_context=False
    )
    data = [{"inputs": {"query": "q"}, "outputs": {"response": "r"}}]
    with bridge.judge_route(route):
        mlflow.genai.evaluate(data=data, scorers=scorers)
    assert llm_manager.calls == []
