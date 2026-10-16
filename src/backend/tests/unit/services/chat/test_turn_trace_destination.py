"""Where a chat turn's MLflow trace goes when other turns are in flight.

Chat uses the parent-process MLflow setup the dispatcher and builder use, not
the crew subprocess's: memoized per team, its blocking work in a thread, and on
Databricks Apps the team's trace destination bound to the turn's own context.

Outside Apps that binding is process-wide, so a turn opens its root trace only
while the binding is its own team's; otherwise it runs untraced rather than
writing into another team's experiment.
"""

import asyncio
import importlib
import threading
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import mlflow
import pytest

from src.services.chat import turn_trace
from src.services.chat.service import LightAgentService
from src.services.mlflow import mlflow_parent_setup

# Every module the code under test imports lazily, imported HERE, before any
# patch is applied: a module first imported inside a patch window keeps the
# fake it bound with ``from x import y`` for the rest of the worker (the
# hosted ``is_databricks_app`` fake leaked into the database router that way).
for _module in (
    "src.db.session",
    "src.services.databricks.workspace.service",
    "src.services.mlflow.service",
    "src.services.mlflow.tracing",
    "src.services.otel_tracing.mlflow_setup",
):
    importlib.import_module(_module)


def _ctx(group_id):
    return SimpleNamespace(
        primary_group_id=group_id, group_ids=[group_id], group_email=None
    )


def _effective_destination():
    from mlflow.tracing.provider import _MLFLOW_TRACE_USER_DESTINATION

    return _MLFLOW_TRACE_USER_DESTINATION.get()


class _Stack:
    """The parent setup with its I/O faked, and a recording root trace."""

    def __init__(self, hosted):
        self.hosted = hosted
        self.roots: list = []  # (trace name, destination when the root opened)
        self.setup_threads: list = []
        self.hold: dict = {}  # group -> (entered, release) threading events

    def setup_sync(self, exp_name, uc_catalog, uc_schema, warehouse_id, label):
        group = exp_name.removeprefix("exp-")
        self.setup_threads.append(threading.current_thread())
        if group in self.hold:
            entered, release = self.hold[group]
            entered.set()
            assert release.wait(10), "test never released the held setup"
        return SimpleNamespace(team=group) if self.hosted else True

    def mlflow_service(self, session, group_id=None):
        return SimpleNamespace(
            is_enabled=AsyncMock(return_value=True),
            configured_crew_traces_experiment=AsyncMock(return_value=f"exp-{group_id}"),
        )

    @contextmanager
    def start_root_trace(self, name, inputs=None):
        self.roots.append((name, _effective_destination()))
        yield SimpleNamespace(set_outputs=lambda _out: None)


@asynccontextmanager
async def _fake_session():
    yield MagicMock()


@pytest.fixture
def stack(request):
    hosted = request.param
    s = _Stack(hosted)
    subprocess_setup = AsyncMock(
        return_value=SimpleNamespace(
            enabled=True, tracing_ready=True, experiment_name="x", error=None
        )
    )
    databricks = MagicMock()
    databricks.return_value.get_databricks_config = AsyncMock(return_value=None)
    mlflow_parent_setup.invalidate_parent_mlflow_cache()
    with (
        patch.object(mlflow_parent_setup, "_setup_sync", s.setup_sync),
        # Where each module looks the name up — never the global function.
        patch.object(mlflow_parent_setup, "is_databricks_app", lambda: hosted),
        patch.object(turn_trace, "is_databricks_app", lambda: hosted),
        patch("src.services.mlflow.service.MLflowService", s.mlflow_service),
        patch(
            "src.services.databricks.workspace.service.DatabricksService", databricks
        ),
        patch("src.db.session.routed_scoped_session", _fake_session),
        patch("src.services.mlflow.tracing.start_root_trace", s.start_root_trace),
        patch.object(mlflow.tracing, "enable", lambda: None),
        patch.object(mlflow.tracing, "disable", lambda: None),
        # The per-turn subprocess setup chat used to run: asserted unused.
        patch(
            "src.services.otel_tracing.mlflow_setup.configure_mlflow_in_subprocess",
            subprocess_setup,
        ),
        patch(
            "src.services.mlflow.mlflow_setup.configure_mlflow_in_subprocess",
            subprocess_setup,
        ),
    ):
        s.subprocess_setup = subprocess_setup
        yield s
    mlflow_parent_setup.invalidate_parent_mlflow_cache()


class _Agent:
    """Records the destination its (worker-thread) work sees, at given points."""

    def __init__(self, pause=None):
        self.seen: list = []
        self._pause = pause

    async def kickoff_async(self, _prompt):
        self.seen.append(await asyncio.to_thread(_effective_destination))
        if self._pause is not None:
            await self._pause()
            self.seen.append(await asyncio.to_thread(_effective_destination))
        return SimpleNamespace(raw="answer")


async def _turn(agent, label, group):
    config = SimpleNamespace(model="m", inputs={}, session_id=None)
    return await LightAgentService()._kickoff_with_mlflow_trace(
        agent, "prompt", config, f"exec-{label}", label, _ctx(group), group
    )


@pytest.mark.parametrize("stack", [True], indirect=True)
async def test_on_apps_each_concurrent_turn_traces_to_its_own_team(stack):
    a_inside, b_done = asyncio.Event(), asyncio.Event()

    async def a_waits_for_b():
        a_inside.set()
        await asyncio.wait_for(b_done.wait(), timeout=10)

    async def turn_b():
        await asyncio.wait_for(a_inside.wait(), timeout=10)
        try:
            return await _turn(agent_b, "turn-b", "team-b")
        finally:
            b_done.set()

    agent_a, agent_b = _Agent(pause=a_waits_for_b), _Agent()
    await asyncio.gather(_turn(agent_a, "turn-a", "team-a"), turn_b())

    # Each root opened on its own team's destination — B configured (and
    # re-pointed the process-wide experiment) while A's turn was mid-flight.
    roots = dict(stack.roots)
    assert getattr(roots["chat_kickoff:turn-a"], "team", None) == "team-a"
    assert getattr(roots["chat_kickoff:turn-b"], "team", None) == "team-b"
    # A's worker-thread work kept A's destination before and after B's setup.
    assert [getattr(d, "team", None) for d in agent_a.seen] == ["team-a", "team-a"]
    assert [getattr(d, "team", None) for d in agent_b.seen] == ["team-b"]


@pytest.mark.parametrize("stack", [True], indirect=True)
async def test_chat_uses_the_parent_setup_off_the_event_loop(stack):
    await _turn(_Agent(), "turn-a", "team-a")
    await _turn(_Agent(), "turn-a2", "team-a")

    stack.subprocess_setup.assert_not_awaited()
    # One setup for the team (memoized), and not on the event loop's thread.
    assert len(stack.setup_threads) == 1
    assert stack.setup_threads[0] is not threading.main_thread()
    assert [name for name, _ in stack.roots] == [
        "chat_kickoff:turn-a",
        "chat_kickoff:turn-a2",
    ]


@pytest.mark.parametrize("stack", [False], indirect=True)
async def test_off_apps_a_turn_never_traces_into_another_teams_binding(stack):
    """Outside Apps the experiment is process-wide. While another team's setup
    holds it, a turn runs untraced instead of landing in that experiment."""
    await _turn(_Agent(), "a-1", "team-a")  # team A owns the binding

    entered, release = threading.Event(), threading.Event()
    stack.hold["team-b"] = (entered, release)
    turn_b = asyncio.create_task(_turn(_Agent(), "b-1", "team-b"))
    assert await asyncio.to_thread(entered.wait, 10)  # B's setup is running

    out = await _turn(_Agent(), "a-2", "team-a")  # A's memo says "bound"
    assert out.raw == "answer"  # the turn itself still runs

    release.set()
    await asyncio.wait_for(turn_b, timeout=10)
    await _turn(_Agent(), "a-3", "team-a")  # A re-binds, then traces again

    stack.subprocess_setup.assert_not_awaited()
    assert [name for name, _ in stack.roots] == [
        "chat_kickoff:a-1",
        "chat_kickoff:b-1",
        "chat_kickoff:a-3",
    ]
