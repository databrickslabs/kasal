"""Two chat turns in flight at once each keep their tool hooks to themselves.

Chat runs in the server process, and the tool hooks live in one process-wide
list; every turn installs an approval hook. Each turn binds its execution, and
``wrap_tool`` runs only the hooks that belong to it (or to nobody): a gated
call is gated once, by its own turn, its approval is filed under its own run,
and no other run's status is written.

The tool call is exercised the way each harness makes it: on the turn's own
thread, on the Kasal runtime's worker thread, and through CrewAI's two hops
(``ainvoke``, which hands a sync tool to ``loop.run_in_executor``, and the
parallel pool, which copies the context itself).
"""

import asyncio
import contextvars
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.chat.turn_kickoff import kickoff_chat_turn
from src.services.execution.harnesses import binding_for, reset_for_tests
from src.services.execution.kernel import tool_approval
from src.services.execution.runtime.executor import wrap_tool
from src.services.tools.base import BaseTool

_MAIN_THREAD = threading.main_thread()


def _sql_tool():
    """A fresh tool, plus the threads it ran on."""
    threads: list = []

    class _RunSql(BaseTool):
        name: str = "run_sql"
        description: str = "Run a SQL query"

        def _run(self, query: str = "") -> str:
            threads.append(threading.current_thread())
            return f"rows for {query}"

    return _RunSql(), threads


class _Hitl:
    """In-memory stand-in for the approval rows and the run status writes."""

    def __init__(self) -> None:
        self.created: list = []  # (execution_id, group_id, tool_args)
        self.status_writes: list = []  # execution ids whose status a gate wrote
        self._lock = threading.Lock()

    async def create(self, execution_id, group_id, gate_config):
        with self._lock:
            self.created.append(
                (execution_id, group_id, dict(gate_config["tool_args"]))
            )
            # Creating an approval sets that run WAITING_FOR_APPROVAL.
            self.status_writes.append(execution_id)
            return f"approval-{len(self.created)}"

    async def status(self, _approval_id):
        return "approved"  # each run's own reviewer approves

    async def restore(self, execution_id):
        with self._lock:
            self.status_writes.append(execution_id)

    async def prior(self, *_args):
        return None


@pytest.fixture(autouse=True)
def _clean_harnesses():
    reset_for_tests()
    yield
    reset_for_tests()


@pytest.fixture
def hitl():
    store = _Hitl()
    with (
        patch.object(tool_approval, "_create_approval", store.create),
        patch.object(tool_approval, "_approval_status", store.status),
        patch.object(tool_approval, "_restore_running", store.restore),
        patch.object(tool_approval, "_prior_decision", store.prior),
        patch("src.services.hitl.notify.notify_input_needed"),
    ):
        yield store


def _gated(tool: BaseTool) -> BaseTool:
    """Stamp an approval policy the way ToolFactory does."""
    object.__setattr__(tool, "_approval_policy", {"scope": "call"})
    return tool


class _Agent:
    """A chat agent whose kickoff makes one tool call through ``call_tool``."""

    role = "Analyst"

    def __init__(self, call_tool, tool, query):
        self._call_tool = call_tool
        self._tool = tool
        self._query = query

    async def kickoff_async(self, _prompt):
        out = await self._call_tool(self, self._tool, self._query)
        return SimpleNamespace(raw=str(out))


async def _on_turn_thread(agent, tool, query):
    # CrewAI's native single tool call: the tool runs where the turn runs.
    return wrap_tool(tool, agent=agent)(query=query)


async def _crewai_ainvoke(_agent, tool, query):
    # CrewAI's ReAct path: CrewStructuredTool.ainvoke -> loop.run_in_executor.
    adapted = binding_for("crewai").adapt_tools([tool])[0]
    return await adapted.to_structured_tool().ainvoke(input={"query": query})


async def _crewai_parallel_pool(_agent, tool, query):
    # CrewAI's parallel native calls: a pool fed copy_context().run.
    adapted = binding_for("crewai").adapt_tools([tool])[0]
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(contextvars.copy_context().run, adapted.run, query=query)
        return await asyncio.wrap_future(future)


class _ToolCallingLLM:
    model = "fake-model"

    def __init__(self, query):
        self.query = query

    def call(self, messages, tools=None, available_functions=None, **_kwargs):
        return available_functions["run_sql"](query=self.query)


def _kasal_agent(tool, query):
    # The real Kasal runtime: kickoff_async -> asyncio.to_thread -> tool loop.
    return binding_for("kasal").build_agent(
        role="Analyst",
        goal="Answer",
        backstory="You answer.",
        tools=[tool],
        llm=_ToolCallingLLM(query),
    )


def _agent_for(style, tool, query):
    if style == "kasal_worker_thread":
        return _kasal_agent(tool, query)
    call = {
        "turn_thread": _on_turn_thread,
        "crewai_ainvoke": _crewai_ainvoke,
        "crewai_parallel_pool": _crewai_parallel_pool,
    }[style]
    return _Agent(call, tool, query)


async def _turn(execution_id, group_id, agent, started, finished):
    """One chat turn: install the hook as chat does, kick off, then tear down."""
    ctx = SimpleNamespace(primary_group_id=group_id, group_ids=[group_id])
    uninstall = tool_approval.install_tool_approval_hook(execution_id, ctx)
    try:
        # Both turns' hooks are installed before either calls a tool, and stay
        # installed until both have finished: the two turns are in flight.
        await asyncio.wait_for(started.wait(), timeout=10)

        async def kickoff(agent, prompt, *_args):
            return await agent.kickoff_async(prompt)

        service = SimpleNamespace(
            _kickoff_with_mlflow_trace=AsyncMock(side_effect=kickoff)
        )
        config = SimpleNamespace(inputs={}, model="m", output_contract=None)
        result = await kickoff_chat_turn(
            service,
            agent,
            config,
            execution_id,
            "trace",
            ctx,
            group_id,
            "prompt",
            {},
            None,
            None,
            lambda _msg: None,
        )
        await asyncio.wait_for(finished.wait(), timeout=10)
        return result.raw
    finally:
        uninstall()


async def _two_turns(style):
    gated_tool, gated_threads = _sql_tool()
    plain_tool, plain_threads = _sql_tool()
    _gated(gated_tool)
    started, finished = asyncio.Barrier(2), asyncio.Barrier(2)
    out_a, out_b = await asyncio.gather(
        _turn(
            "exec-a",
            "group-a",
            _agent_for(style, gated_tool, "select a"),
            started,
            finished,
        ),
        _turn(
            "exec-b",
            "group-b",
            _agent_for(style, plain_tool, "select b"),
            started,
            finished,
        ),
    )
    return out_a, out_b, gated_threads, plain_threads


STYLES = [
    "turn_thread",
    "kasal_worker_thread",
    "crewai_ainvoke",
    "crewai_parallel_pool",
]


@pytest.mark.parametrize("style", STYLES)
async def test_only_the_owning_turn_gates_its_tool_call(hitl, style):
    out_a, out_b, gated_threads, plain_threads = await _two_turns(style)

    # Both calls ran: A's own reviewer approved A's call, B's was never gated.
    assert out_a == "rows for select a"
    assert out_b == "rows for select b"
    assert len(gated_threads) == 1 and len(plain_threads) == 1

    # Exactly one approval, under A's execution and group, with A's arguments.
    assert hitl.created == [("exec-a", "group-a", {"query": "select a"})]


@pytest.mark.parametrize("style", STYLES)
async def test_neither_turn_writes_the_other_turns_status(hitl, style):
    await _two_turns(style)

    # A's gate wrote A's status (waiting, then running again) and nothing else.
    assert hitl.status_writes == ["exec-a", "exec-a"]
    assert "exec-b" not in hitl.status_writes


@pytest.mark.parametrize(
    "style", ["kasal_worker_thread", "crewai_ainvoke", "crewai_parallel_pool"]
)
async def test_the_worker_thread_styles_really_run_the_tool_off_the_turn(hitl, style):
    """The isolation above has to survive the hop to a tool thread."""
    _, _, gated_threads, plain_threads = await _two_turns(style)

    assert gated_threads and plain_threads
    for thread in gated_threads + plain_threads:
        assert thread is not _MAIN_THREAD
