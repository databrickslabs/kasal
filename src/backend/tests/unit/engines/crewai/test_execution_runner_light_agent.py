"""
Unit tests for the engine-level light-agent ("chat" mode) runner:
``src.engines.crewai.paths.light_agent.light_agent_service.run_light_agent``.

This is the CrewAI-specific counterpart of ``run_crew_in_process`` for the
single-agent chat path. The service layer
(``CrewAIExecutionService.run_light_agent_execution``) only resolves the engine
and delegates here; that delegation is covered separately in
``tests/unit/services/test_crewai_execution_service_coverage.py``.

The runner builds ONE agent, calls ``Agent.kickoff_async`` IN-PROCESS, writes
its own terminal status, and streams tool activity to the chat trace pane via a
per-agent ``step_callback`` that emits ``<tool>_run`` traces through
``ExecutionTraceService.create_trace``.
"""
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.engines.crewai.paths.light_agent.light_agent_service import run_light_agent
from src.models.execution_status import ExecutionStatus
from src.schemas.execution import CrewConfig
from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_config(**kwargs):
    config_kwargs = {
        "model": kwargs.get("model", "gpt-4"),
        "inputs": kwargs.get("inputs", {}),
        "planning": kwargs.get("planning", False),
    }
    if kwargs.get("agents_yaml") is not None:
        config_kwargs["agents_yaml"] = kwargs["agents_yaml"]
    if kwargs.get("tasks_yaml") is not None:
        config_kwargs["tasks_yaml"] = kwargs["tasks_yaml"]
    return CrewConfig(**config_kwargs)


def make_group_context(group_ids=None):
    ctx = MagicMock(spec=GroupContext)
    ctx.group_ids = group_ids or ["g1"]
    ctx.primary_group_id = (group_ids or ["g1"])[0]
    ctx.group_email = "tenant@example.com"
    ctx.access_token = None
    return ctx


def _fake_session():
    """An async-context-manager DB session whose commit() is awaitable."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    return session


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_light_agent_success_writes_completed_with_raw_answer():
    """Runs ONE agent via Agent.kickoff_async and writes COMPLETED with the
    agent's .raw answer through the shared ExecutionStatusService."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Assistant", "goal": "g",
                                  "backstory": "b", "tools": [], "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "Answer: hello",
                                "expected_output": "a reply"}},
    )
    ctx = make_group_context(["g1"])

    mock_agent = AsyncMock()
    mock_agent.kickoff_async = AsyncMock(return_value=SimpleNamespace(raw="Hello there!"))

    update_mock = AsyncMock(return_value=True)
    with patch("src.db.session.request_scoped_session", return_value=_fake_session()), \
         patch("src.utils.user_context.UserContext"), \
         patch("src.services.api_keys_service.ApiKeysService"), \
         patch("src.engines.crewai.tools.tool_factory.ToolFactory.create",
               new_callable=AsyncMock, return_value=MagicMock()), \
         patch("src.engines.crewai.kernel.agent_tools.build_agent_with_tools",
               new_callable=AsyncMock, return_value=mock_agent), \
         patch("src.services.execution_status_service.ExecutionStatusService.update_status",
               update_mock):
        result = await run_light_agent(exec_id, config, group_context=ctx)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    mock_agent.kickoff_async.assert_awaited_once()
    # The grounded task description (+ expected output) is the kickoff prompt.
    prompt_arg = mock_agent.kickoff_async.call_args.args[0]
    assert "Answer: hello" in prompt_arg
    assert "a reply" in prompt_arg
    # Status written COMPLETED with the agent's raw answer as the result.
    completed = [c for c in update_mock.call_args_list
                 if c.kwargs.get("status") == ExecutionStatus.COMPLETED.value]
    assert completed and completed[-1].kwargs.get("result") == "Hello there!"


# ---------------------------------------------------------------------------
# Tool-activity tracing (via the CrewAI event bus)
# ---------------------------------------------------------------------------
#
# Native/function-calling tool calls bypass the agent step_callback, so the
# runner registers ToolUsage{Started,Finished} handlers on the bus, scoped to
# this run's agent id. These tests capture the registered handlers and drive
# them with fake events (a SimpleNamespace is enough — the handler only does
# getattr on the event), exactly as crewai's bus would during kickoff.

import asyncio  # noqa: E402
import crewai.events as _ce  # noqa: E402


async def _run_with_captured_handlers(exec_id, config, ctx, mock_agent, trace_instance, emit_during_kickoff):
    """Run run_light_agent with the bus register_handler/off patched to capture
    handlers, invoking ``emit_during_kickoff(captured)`` while kickoff runs."""
    captured = {}

    def _cap(event_type, handler):
        captured[event_type.__name__] = handler

    async def _kickoff(prompt):
        emit_during_kickoff(captured)
        await asyncio.sleep(0.05)  # let scheduled (run_coroutine_threadsafe) persists run
        return SimpleNamespace(raw="done")
    mock_agent.kickoff_async = AsyncMock(side_effect=_kickoff)

    trace_cls = MagicMock(return_value=trace_instance)
    update_mock = AsyncMock(return_value=True)
    with patch("src.db.session.request_scoped_session", return_value=_fake_session()), \
         patch("src.utils.user_context.UserContext"), \
         patch("src.services.api_keys_service.ApiKeysService"), \
         patch("src.engines.crewai.tools.tool_factory.ToolFactory.create",
               new_callable=AsyncMock, return_value=MagicMock()), \
         patch("src.engines.crewai.kernel.agent_tools.build_agent_with_tools",
               new_callable=AsyncMock, return_value=mock_agent), \
         patch("src.services.execution_trace_service.ExecutionTraceService", trace_cls), \
         patch.object(_ce.crewai_event_bus, "register_handler", side_effect=_cap), \
         patch.object(_ce.crewai_event_bus, "off"), \
         patch("src.services.execution_status_service.ExecutionStatusService.update_status",
               update_mock):
        result = await run_light_agent(exec_id, config, group_context=ctx)
    return result, captured


@pytest.mark.asyncio
async def test_tool_finished_event_emits_tool_run_trace():
    """A ToolUsageFinishedEvent for THIS agent emits a ``<tool>_run`` trace (the
    tool_result the chat pane renders) carrying tool name, input and output."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "Find the top customers"}},
    )
    ctx = make_group_context(["g1"])

    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"  # the runner scopes handlers by str(agent.id)

    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        handler = captured["ToolUsageFinishedEvent"]
        handler(mock_agent, SimpleNamespace(
            agent_id="aid-1", tool_name="Serper Search",
            tool_args={"q": "top customers"}, output="1. Acme  2. Globex",
            started_at=None, finished_at=None,
        ))

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert "ToolUsageStartedEvent" in captured and "ToolUsageFinishedEvent" in captured
    trace_instance.create_trace.assert_awaited_once()
    trace_data = trace_instance.create_trace.await_args.args[0]
    assert trace_data["job_id"] == exec_id
    assert trace_data["event_type"].endswith("_run")
    assert trace_data["event_source"] == "Researcher"
    out = trace_data["output"]
    assert out["tool_name"] == "Serper Search"
    assert "top customers" in out["input"]
    assert "Acme" in out["content"]
    assert trace_data.get("group_id") == "g1"  # tenant isolation carried


@pytest.mark.asyncio
async def test_tool_event_for_other_agent_is_ignored():
    """A tool event whose agent_id doesn't match this run is dropped — no
    cross-talk between concurrent in-process light runs (tenant-safe)."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Assistant", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "hi"}},
    )
    ctx = make_group_context(["g1"])

    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"

    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        # Event belongs to a DIFFERENT agent → must be ignored.
        captured["ToolUsageFinishedEvent"](mock_agent, SimpleNamespace(
            agent_id="other-agent", tool_name="X", tool_args="a",
            output="r", started_at=None, finished_at=None, agent=object(),
        ))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_not_awaited()


@pytest.mark.asyncio
async def test_tool_trace_backend_failure_never_breaks_run():
    """If the trace backend raises, the tool handler swallows it and the run
    still completes."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Assistant", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "hi"}},
    )
    ctx = make_group_context(["g1"])

    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"

    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock(side_effect=RuntimeError("db down"))

    def _emit(captured):
        captured["ToolUsageFinishedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", tool_name="X", tool_args="a",
            output="r", started_at=None, finished_at=None,
        ))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_awaited_once()  # attempted, error swallowed


# ---------------------------------------------------------------------------
# Agent lifecycle tracing (fires even with NO tools)
# ---------------------------------------------------------------------------
#
# ``Agent.kickoff_async`` runs as a CrewAI "LiteAgent" and emits
# LiteAgentExecution{Started,Completed,Error} with the AGENT instance as the bus
# source. These give the chat a trace even when the agent calls no tools — the
# completed event is emitted as a ``response_run`` (tool_result) step. Chat mode
# does not reason/plan; the step represents the agent's answer generation.

@pytest.mark.asyncio
async def test_agent_completed_event_emits_response_run_trace():
    """A LiteAgentExecutionCompletedEvent for THIS agent (source is agent) emits a
    ``response_run`` trace so a tool-less chat answer still shows a step. Chat mode
    does NOT reason/plan, so the step is the agent's answer generation."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Assistant", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "Say hi"}},
    )
    ctx = make_group_context(["g1"])

    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"

    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        # Started records the kickoff time (for duration); completed emits the trace.
        captured["LiteAgentExecutionStartedEvent"](mock_agent, SimpleNamespace())
        captured["LiteAgentExecutionCompletedEvent"](
            mock_agent, SimpleNamespace(output="Hi there, how can I help?"))

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert "LiteAgentExecutionCompletedEvent" in captured
    trace_instance.create_trace.assert_awaited_once()
    trace_data = trace_instance.create_trace.await_args.args[0]
    assert trace_data["job_id"] == exec_id
    assert trace_data["event_type"] == "response_run"
    assert trace_data["event_source"] == "Assistant"
    out = trace_data["output"]
    assert out["tool_name"] == "Response"
    assert "how can I help" in out["content"]
    assert trace_data.get("group_id") == "g1"  # tenant isolation carried


@pytest.mark.asyncio
async def test_agent_event_for_other_agent_is_ignored():
    """A LiteAgent event whose source is NOT this run's agent is dropped — no
    cross-talk between concurrent in-process light runs (tenant-safe)."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Assistant", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "hi"}},
    )
    ctx = make_group_context(["g1"])

    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"

    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        # source is a DIFFERENT agent instance → must be ignored.
        captured["LiteAgentExecutionCompletedEvent"](
            object(), SimpleNamespace(output="leak"))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_not_awaited()


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_light_agent_failure_marks_failed():
    """If the single-agent build/kickoff raises, the run is marked FAILED (never
    left hanging) via ExecutionStatusService."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "r", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "do it"}},
    )
    ctx = make_group_context(["g1"])

    update_mock = AsyncMock(return_value=True)
    with patch("src.db.session.request_scoped_session", return_value=_fake_session()), \
         patch("src.utils.user_context.UserContext"), \
         patch("src.services.api_keys_service.ApiKeysService"), \
         patch("src.engines.crewai.tools.tool_factory.ToolFactory.create",
               new_callable=AsyncMock, return_value=MagicMock()), \
         patch("src.engines.crewai.kernel.agent_tools.build_agent_with_tools",
               new_callable=AsyncMock, side_effect=RuntimeError("boom")), \
         patch("src.services.execution_status_service.ExecutionStatusService.update_status",
               update_mock):
        result = await run_light_agent(exec_id, config, group_context=ctx)

    assert result["status"] == ExecutionStatus.FAILED.value
    assert result["error"] == "boom"
    failed = [c for c in update_mock.call_args_list
              if c.kwargs.get("status") == ExecutionStatus.FAILED.value]
    assert failed, "expected a FAILED status write"


@pytest.mark.asyncio
async def test_run_light_agent_requires_an_agent():
    """A config with no agent can't run a light agent — it fails cleanly."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(tasks_yaml={"task_t1": {"id": "task_t1", "description": "d"}})

    update_mock = AsyncMock(return_value=True)
    with patch("src.utils.user_context.UserContext"), \
         patch("src.services.execution_status_service.ExecutionStatusService.update_status",
               update_mock):
        result = await run_light_agent(exec_id, config, group_context=None)

    assert result["status"] == ExecutionStatus.FAILED.value


@pytest.mark.asyncio
async def test_run_light_agent_requires_a_prompt():
    """An agent but no task description (and no inputs) → fails cleanly rather
    than kicking off an empty prompt."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "r", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
    )

    update_mock = AsyncMock(return_value=True)
    with patch("src.db.session.request_scoped_session", return_value=_fake_session()), \
         patch("src.utils.user_context.UserContext"), \
         patch("src.services.execution_status_service.ExecutionStatusService.update_status",
               update_mock):
        result = await run_light_agent(exec_id, config, group_context=None)

    assert result["status"] == ExecutionStatus.FAILED.value


# ---------------------------------------------------------------------------
# A2UI surface composition (the {"text", "a2ui"} envelope)
# ---------------------------------------------------------------------------
#
# After the agent answers, the runner composes a renderable A2UI surface from
# the prose via the SHARED composer (``a2ui_runner.compose_surface``) and, when
# one is produced, persists a ``{"text", "a2ui"}`` envelope instead of the bare
# string. The compose call is an auxiliary LLM call: it is bounded by
# ``asyncio.wait_for(timeout=60)`` and must NEVER block or fail the terminal
# status — on timeout / error / "nothing to render" the run completes with the
# plain answer. compose_surface is imported lazily inside the function, so we
# patch it at its source module.

def _light_patches(mock_agent, update_mock, compose_mock):
    """The standard happy-path patch stack + a patched A2UI composer."""
    return (
        patch("src.db.session.request_scoped_session", return_value=_fake_session()),
        patch("src.utils.user_context.UserContext"),
        patch("src.services.api_keys_service.ApiKeysService"),
        patch("src.engines.crewai.tools.tool_factory.ToolFactory.create",
              new_callable=AsyncMock, return_value=MagicMock()),
        patch("src.engines.crewai.kernel.agent_tools.build_agent_with_tools",
              new_callable=AsyncMock, return_value=mock_agent),
        patch("src.engines.crewai.kernel.a2ui_runner.compose_surface", compose_mock),
        patch("src.services.execution_status_service.ExecutionStatusService.update_status",
              update_mock),
    )


def _a2ui_config_and_agent(answer="Hello there!"):
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Assistant", "goal": "g",
                                  "backstory": "b", "tools": [], "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "Make a 3-slide deck",
                                "expected_output": "a deck"}},
    )
    mock_agent = AsyncMock()
    mock_agent.kickoff_async = AsyncMock(return_value=SimpleNamespace(raw=answer))
    return config, mock_agent


async def _completed_result(update_mock):
    completed = [c for c in update_mock.call_args_list
                 if c.kwargs.get("status") == ExecutionStatus.COMPLETED.value]
    assert completed, "expected a COMPLETED status write"
    return completed[-1].kwargs.get("result")


@pytest.mark.asyncio
async def test_a2ui_surface_wraps_result_in_envelope():
    """When the composer returns a surface, the persisted result is the
    ``{"text", "a2ui"}`` envelope (not the bare string), carrying both the
    prose answer and the rich surface for the chat to render."""
    exec_id = f"light-{uuid.uuid4()}"
    config, mock_agent = _a2ui_config_and_agent("Hello there!")
    ctx = make_group_context(["g1"])

    surface = {
        "surfaceKind": "presentation",
        "root": "root",
        "components": [{"id": "root", "component": "SlideDeck", "children": []}],
    }
    compose_mock = AsyncMock(return_value=surface)
    update_mock = AsyncMock(return_value=True)

    p = _light_patches(mock_agent, update_mock, compose_mock)
    with p[0], p[1], p[2], p[3], p[4], p[5], p[6]:
        result = await run_light_agent(exec_id, config, group_context=ctx)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    compose_mock.assert_awaited_once()
    # Composed against the agent's prose answer (first positional arg).
    assert compose_mock.await_args.args[0] == "Hello there!"
    assert await _completed_result(update_mock) == {"text": "Hello there!", "a2ui": surface}


@pytest.mark.asyncio
async def test_a2ui_none_keeps_plain_answer():
    """When the composer returns None ("nothing to render" / A2UI disabled), the
    result stays the bare prose string — today's conversational behavior."""
    exec_id = f"light-{uuid.uuid4()}"
    config, mock_agent = _a2ui_config_and_agent("Just a chat reply.")
    ctx = make_group_context(["g1"])

    compose_mock = AsyncMock(return_value=None)
    update_mock = AsyncMock(return_value=True)

    p = _light_patches(mock_agent, update_mock, compose_mock)
    with p[0], p[1], p[2], p[3], p[4], p[5], p[6]:
        result = await run_light_agent(exec_id, config, group_context=ctx)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert await _completed_result(update_mock) == "Just a chat reply."


@pytest.mark.asyncio
async def test_a2ui_timeout_keeps_plain_answer():
    """A slow/hung composer (TimeoutError out of the bounded wait_for) must NOT
    block the terminal status — the run completes with the plain answer."""
    exec_id = f"light-{uuid.uuid4()}"
    config, mock_agent = _a2ui_config_and_agent("Answer despite slow UI.")
    ctx = make_group_context(["g1"])

    compose_mock = AsyncMock(side_effect=asyncio.TimeoutError())
    update_mock = AsyncMock(return_value=True)

    p = _light_patches(mock_agent, update_mock, compose_mock)
    with p[0], p[1], p[2], p[3], p[4], p[5], p[6]:
        result = await run_light_agent(exec_id, config, group_context=ctx)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert await _completed_result(update_mock) == "Answer despite slow UI."


@pytest.mark.asyncio
async def test_a2ui_compose_error_never_breaks_run():
    """Any composer exception is swallowed — the run still completes with the
    plain answer rather than failing on an auxiliary UI call."""
    exec_id = f"light-{uuid.uuid4()}"
    config, mock_agent = _a2ui_config_and_agent("Answer despite UI error.")
    ctx = make_group_context(["g1"])

    compose_mock = AsyncMock(side_effect=RuntimeError("compose blew up"))
    update_mock = AsyncMock(return_value=True)

    p = _light_patches(mock_agent, update_mock, compose_mock)
    with p[0], p[1], p[2], p[3], p[4], p[5], p[6]:
        result = await run_light_agent(exec_id, config, group_context=ctx)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert await _completed_result(update_mock) == "Answer despite UI error."
