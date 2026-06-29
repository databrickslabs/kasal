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
         patch("src.db.session.get_isolated_db_session", side_effect=lambda: _fake_session()), \
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


# ---------------------------------------------------------------------------
# Tool error tracing — ToolUsageErrorEvent
# ---------------------------------------------------------------------------
#
# Kasal wraps MCP tools as CrewAI tools; on success they emit
# ToolUsageFinishedEvent, but on timeout/4xx/error they emit ToolUsageErrorEvent.
# Without a handler a failed tool call showed "using tool" then nothing — these
# tests assert the failure is surfaced as a ``<tool>_error`` trace.

@pytest.mark.asyncio
async def test_tool_error_event_emits_tool_error_trace():
    """A ToolUsageErrorEvent for THIS agent emits a ``<tool>_error`` trace
    carrying the error text, so a failed/timed-out tool call is visible."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["ToolUsageErrorEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", tool_name="Genie Search",
            tool_args={"q": "x"}, error="HTTP 403 Forbidden"))

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert "ToolUsageErrorEvent" in captured
    trace_instance.create_trace.assert_awaited_once()
    td = trace_instance.create_trace.await_args.args[0]
    assert td["job_id"] == exec_id
    assert td["event_type"] == "geniesearch_error"
    assert td["output"]["error"] == "HTTP 403 Forbidden"
    assert "403" in td["output"]["content"]
    assert td.get("group_id") == "g1"


@pytest.mark.asyncio
async def test_tool_error_event_for_other_agent_is_ignored():
    """A tool error whose agent_id doesn't match this run is dropped."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["ToolUsageErrorEvent"](object(), SimpleNamespace(
            agent_id="other", from_agent=None, agent_role="Other",
            tool_name="T", error="boom"))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_not_awaited()


# ---------------------------------------------------------------------------
# LLM call tracing — LLMCall{Started,Completed,Failed}
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_llm_started_event_emits_llm_call_trace_with_prompt():
    """LLMCallStartedEvent → ``llm_call`` trace whose content/extra_data carry the
    request prompt (so the timeline's 'LLM Request → View' shows the request)."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["LLMCallStartedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="databricks-llama-4-maverick",
            messages=[{"role": "system", "content": "You are helpful"},
                      {"role": "user", "content": "find the top customers"}]))

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert "LLMCallStartedEvent" in captured
    trace_instance.create_trace.assert_awaited_once()
    td = trace_instance.create_trace.await_args.args[0]
    assert td["event_type"] == "llm_call"
    assert "top customers" in td["output"]["content"]
    assert td["output"]["extra_data"]["model"] == "databricks-llama-4-maverick"
    assert td["output"]["extra_data"]["prompt_length"] > 0


@pytest.mark.asyncio
async def test_llm_started_event_with_string_messages():
    """_msgs_str handles a plain-string messages payload."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["LLMCallStartedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="m", messages="raw prompt string"))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    td = trace_instance.create_trace.await_args.args[0]
    assert td["output"]["content"] == "raw prompt string"


@pytest.mark.asyncio
async def test_llm_started_event_with_no_messages():
    """_msgs_str returns empty when there are no messages (no crash)."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["LLMCallStartedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="m", messages=None))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    td = trace_instance.create_trace.await_args.args[0]
    assert td["output"]["content"] == ""


@pytest.mark.asyncio
async def test_llm_completed_event_emits_llm_response_trace():
    """LLMCallCompletedEvent → ``llm_response`` trace with the response text and
    output_length stamped in trace_metadata (what the timeline label reads)."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["LLMCallCompletedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="databricks-llama",
            response="The answer is 42", usage={"total_tokens": 10}))

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert "LLMCallCompletedEvent" in captured
    trace_instance.create_trace.assert_awaited_once()
    td = trace_instance.create_trace.await_args.args[0]
    assert td["event_type"] == "llm_response"
    assert td["output"]["content"] == "The answer is 42"
    assert td["output"]["extra_data"]["output_length"] == len("The answer is 42")
    assert td["output"]["extra_data"]["usage"] == {"total_tokens": 10}
    assert td["trace_metadata"]["output_length"] == len("The answer is 42")
    assert td["trace_metadata"]["model"] == "databricks-llama"


@pytest.mark.asyncio
async def test_llm_completed_event_truncates_large_response():
    """A very large LLM response is capped so a trace row can't bloat the run."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    big = "x" * 25000

    def _emit(captured):
        captured["LLMCallCompletedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="m", response=big, usage=None))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    td = trace_instance.create_trace.await_args.args[0]
    assert td["output"]["content"].endswith("…[truncated]")
    assert len(td["output"]["content"]) < len(big)
    # usage absent → no usage key
    assert "usage" not in td["output"]["extra_data"]


@pytest.mark.asyncio
async def test_llm_failed_event_emits_llm_call_failed_trace():
    """LLMCallFailedEvent → ``llm_call_failed`` trace carrying the error."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["LLMCallFailedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="m", error="rate limited"))

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert "LLMCallFailedEvent" in captured
    trace_instance.create_trace.assert_awaited_once()
    td = trace_instance.create_trace.await_args.args[0]
    assert td["event_type"] == "llm_call_failed"
    assert td["output"]["error"] == "rate limited"


# ---------------------------------------------------------------------------
# Broadened event matching (_matches)
# ---------------------------------------------------------------------------
#
# Tool/LLM events arrive from several sources — the agent executor (from_agent →
# agent_id), the LLM inline caller, and MCP wrappers — so the run matches on ANY
# reliable identity signal: agent_id, agent identity, from_agent.id, or agent_role.

@pytest.mark.asyncio
async def test_matches_via_from_agent_when_agent_id_absent():
    """An event with no agent_id but a from_agent whose id matches is captured."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["ToolUsageStartedEvent"](object(), SimpleNamespace(
            agent_id=None, from_agent=SimpleNamespace(id="aid-1"),
            tool_name="T", tool_args={"a": 1}))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_awaited_once()
    td = trace_instance.create_trace.await_args.args[0]
    assert td["event_type"] == "tool_usage"


@pytest.mark.asyncio
async def test_matches_via_agent_role_when_ids_absent():
    """An event with no ids but a matching agent_role is captured."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["ToolUsageStartedEvent"](object(), SimpleNamespace(
            agent_id=None, from_agent=None, agent_role="Researcher",
            tool_name="T", tool_args="raw-args"))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_awaited_once()
    td = trace_instance.create_trace.await_args.args[0]
    assert td["event_type"] == "tool_usage"


# ---------------------------------------------------------------------------
# Defensive paths: handlers never break the run; helpers tolerate odd input
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handlers_swallow_internal_errors():
    """Every trace handler is wrapped so a malformed event can never break the
    run. An event that raises on attribute access is swallowed (logged at debug)
    and no trace is written — the run still completes."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    class _Boom:
        # Any attribute access raises a NON-AttributeError, which getattr(...)
        # propagates — exercising each handler's outer try/except.
        def __getattr__(self, name):
            raise RuntimeError("boom attr")

    def _emit(captured):
        boom = _Boom()
        for name, handler in captured.items():
            # LiteAgent handlers gate on `source is agent` first, so pass the
            # real agent as source to reach the body; the rest match by event.
            handler(mock_agent, boom)

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    # Every registered handler was exercised, and none wrote a trace or raised.
    for ev in ("ToolUsageStartedEvent", "ToolUsageFinishedEvent", "ToolUsageErrorEvent",
               "LLMCallStartedEvent", "LLMCallCompletedEvent", "LLMCallFailedEvent"):
        assert ev in captured
    trace_instance.create_trace.assert_not_awaited()


@pytest.mark.asyncio
async def test_llm_started_truncates_large_prompt():
    """A very large request prompt is capped in the llm_call trace."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    big = "y" * 25000

    def _emit(captured):
        captured["LLMCallStartedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="m",
            messages=[{"role": "user", "content": big}]))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    td = trace_instance.create_trace.await_args.args[0]
    assert td["output"]["content"].endswith("…[truncated]")


@pytest.mark.asyncio
async def test_llm_started_msgs_str_non_dict_and_uniterable():
    """_msgs_str renders non-dict list items via str(), and falls back to
    str(messages) when the payload isn't iterable."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        # list with a non-dict item → str(item); then an uniterable int → fallback.
        captured["LLMCallStartedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="m", messages=["plain line", 7]))
        captured["LLMCallStartedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="m", messages=12345))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    calls = [c.args[0] for c in trace_instance.create_trace.await_args_list]
    contents = [c["output"]["content"] for c in calls if c["event_type"] == "llm_call"]
    assert any("plain line" in c and "7" in c for c in contents)
    assert any(c == "12345" for c in contents)


@pytest.mark.asyncio
async def test_schedule_trace_failure_is_swallowed(monkeypatch):
    """If scheduling the persist onto the loop fails, the handler swallows it and
    the run still completes (no trace written)."""
    import asyncio as _asyncio

    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["ToolUsageStartedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", tool_name="T", tool_args={}))

    # _schedule_trace uses run_coroutine_threadsafe; force it to raise. The flush
    # block uses wrap_future/gather (not this), so only scheduling is affected.
    monkeypatch.setattr(_asyncio, "run_coroutine_threadsafe",
                        MagicMock(side_effect=RuntimeError("loop closed")))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_not_awaited()


@pytest.mark.asyncio
async def test_new_handlers_ignore_events_for_other_agents():
    """tool-error and the three LLM handlers all drop events that don't belong to
    this run (covers each handler's `if not _matches: return`)."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        other = dict(agent_id="zzz", from_agent=None, agent_role="Nope",
                     agent=None, tool_name="T", tool_args={}, error="e",
                     model="m", response="r", messages=None, usage=None)
        captured["ToolUsageErrorEvent"](object(), SimpleNamespace(**other))
        captured["LLMCallStartedEvent"](object(), SimpleNamespace(**other))
        captured["LLMCallCompletedEvent"](object(), SimpleNamespace(**other))
        captured["LLMCallFailedEvent"](object(), SimpleNamespace(**other))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_not_awaited()


@pytest.mark.asyncio
async def test_matches_via_agent_identity():
    """An event carrying the agent INSTANCE (no agent_id) matches by identity."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["ToolUsageStartedEvent"](object(), SimpleNamespace(
            agent_id=None, from_agent=None, agent_role=None,
            agent=mock_agent, tool_name="T", tool_args={"a": 1}))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_awaited_once()
    assert trace_instance.create_trace.await_args.args[0]["event_type"] == "tool_usage"


@pytest.mark.asyncio
async def test_llm_completed_unserializable_usage_is_tolerated():
    """If token-usage can't be JSON-serialized it's dropped, not fatal — the
    llm_response trace is still written with the content."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    circular: dict = {}
    circular["self"] = circular  # json.dumps raises ValueError (circular ref)

    def _emit(captured):
        captured["LLMCallCompletedEvent"](mock_agent, SimpleNamespace(
            agent_id="aid-1", model="m", response="hello", usage=circular))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    td = trace_instance.create_trace.await_args.args[0]
    assert td["event_type"] == "llm_response"
    assert td["output"]["content"] == "hello"
    assert "usage" not in td["output"]["extra_data"]


# ---------------------------------------------------------------------------
# Memory tracing — Memory Read / Context Retrieved / Memory Write
# ---------------------------------------------------------------------------
#
# CrewAI emits MemoryQuery/Retrieval/Save events with source=<the Memory> (no
# agent_id), so the light agent scopes them by the Memory INSTANCE it attached
# (source is agent.memory). This gives the chat trace the same rows the crew/flow
# OTel timeline shows. In these tests the AsyncMock agent's `.memory` attribute IS
# that instance, so we drive the handlers with it as the bus source.

@pytest.mark.asyncio
async def test_memory_query_emits_memory_retrieval_trace():
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    mock_agent.memory = object()  # the Memory instance the run scopes by
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["MemoryQueryCompletedEvent"](mock_agent.memory, SimpleNamespace(
            query="swiss news", results=[1, 2, 3, 4, 5], query_time_ms=12748.6))

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert "MemoryQueryCompletedEvent" in captured
    td = trace_instance.create_trace.await_args.args[0]
    assert td["event_type"] == "memory_retrieval"
    assert td["trace_metadata"]["results_count"] == 5
    assert td["trace_metadata"]["query_time_ms"] == 12748.6
    assert td["output"]["extra_data"]["results_count"] == 5


@pytest.mark.asyncio
async def test_memory_retrieval_completed_emits_context_trace():
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    mock_agent.memory = object()
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["MemoryRetrievalCompletedEvent"](mock_agent.memory, SimpleNamespace(
            memory_content="User is based in Zurich.", retrieval_time_ms=8626.4))

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    td = trace_instance.create_trace.await_args.args[0]
    assert td["event_type"] == "memory_retrieval_completed"
    assert "Zurich" in td["output"]["content"]
    assert td["trace_metadata"]["retrieval_time_ms"] == 8626.4


@pytest.mark.asyncio
async def test_memory_retrieval_completed_empty_shows_placeholder():
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    mock_agent.memory = object()
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["MemoryRetrievalCompletedEvent"](mock_agent.memory, SimpleNamespace(
            memory_content=None, retrieval_time_ms=10.0))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    td = trace_instance.create_trace.await_args.args[0]
    assert td["output"]["content"] == "(no memories matched the query)"


@pytest.mark.asyncio
async def test_memory_save_emits_memory_write_trace():
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    mock_agent.memory = object()
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["MemorySaveCompletedEvent"](mock_agent.memory, SimpleNamespace(
            value="Remember the user likes Switzerland.", metadata={}, save_time_ms=14212.6))

    result, captured = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    assert "MemorySaveCompletedEvent" in captured
    td = trace_instance.create_trace.await_args.args[0]
    assert td["event_type"] == "memory_write"
    assert "Switzerland" in td["output"]["content"]
    assert td["trace_metadata"]["save_time_ms"] == 14212.6


@pytest.mark.asyncio
async def test_memory_events_for_other_run_memory_are_ignored():
    """A memory event whose source is a DIFFERENT Memory instance is dropped — no
    cross-talk between concurrent in-process light runs."""
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Researcher", "goal": "g",
                                  "backstory": "b", "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    ctx = make_group_context(["g1"])
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    mock_agent.memory = object()
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["MemorySaveCompletedEvent"](object(), SimpleNamespace(
            value="leak", metadata={}, save_time_ms=1.0))

    result, _ = await _run_with_captured_handlers(
        exec_id, config, ctx, mock_agent, trace_instance, _emit)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    trace_instance.create_trace.assert_not_awaited()


# ---------------------------------------------------------------------------
# MLflow root trace + chat-only session tagging
# ---------------------------------------------------------------------------
#
# The light agent wraps kickoff in an MLflow root trace (same mechanism as
# crew/flow's execute_with_mlflow_trace → start_root_trace) so its LLM spans land
# in MLflow grouped under one named trace. CHAT-ONLY: it also tags the trace with
# the chat session_id (+ user) via MLflow's session metadata keys so a
# conversation's turns group into one MLflow session.

from contextlib import contextmanager  # noqa: E402
from src.engines.crewai.paths.light_agent.light_agent_service import (  # noqa: E402
    LightAgentService,
)


def _mlflow_result(tracing_ready=True, experiment_name="/Shared/kasal-crew-execution-traces-uc", enabled=True):
    return SimpleNamespace(
        enabled=enabled,
        tracing_ready=tracing_ready,
        experiment_name=experiment_name,
        uc_trace_storage=True,
        error=None,
    )


@contextmanager
def _patch_mlflow_uc_stack(mlflow_result, update_mock):
    """Patch the light-agent MLflow stack: db config lookup, the EXACT crew/flow
    setup function (configure_mlflow_in_subprocess) returning ``mlflow_result``,
    the root-trace context manager, the trace-attribute helpers, and the
    session-tag API. Yields the configure mock for assertions."""
    import mlflow

    @contextmanager
    def _fake_trace(name, inputs=None):
        yield MagicMock()

    svc_cls = MagicMock()
    svc_cls.return_value.get_databricks_config = AsyncMock(return_value=SimpleNamespace(mlflow_enabled=True))
    configure_mock = AsyncMock(return_value=mlflow_result)

    with patch("src.db.session.async_session_factory", side_effect=lambda: _fake_session()), \
         patch("src.services.databricks_service.DatabricksService", svc_cls), \
         patch("src.services.otel_tracing.mlflow_setup.configure_mlflow_in_subprocess", configure_mock), \
         patch("src.services.mlflow_tracing_service.start_root_trace", _fake_trace), \
         patch("src.services.otel_tracing.mlflow_setup.set_trace_attributes"), \
         patch("src.services.otel_tracing.mlflow_setup.extract_trace_outputs", return_value=None), \
         patch.object(mlflow, "update_current_trace", update_mock):
        yield configure_mock


@pytest.mark.asyncio
async def test_kickoff_plain_when_mlflow_tracing_not_ready():
    """MLflow setup reports tracing not ready → kickoff runs plainly, no session."""
    svc = LightAgentService()
    agent = AsyncMock()
    agent.kickoff_async = AsyncMock(return_value=SimpleNamespace(raw="hi"))
    config = SimpleNamespace(model="m", inputs={}, session_id="sess-1")

    update_mock = MagicMock()
    with _patch_mlflow_uc_stack(_mlflow_result(tracing_ready=False), update_mock):
        out = await svc._kickoff_with_mlflow_trace(
            agent, "the prompt", config, "exec-1", "ctx", None, "g1")

    assert out.raw == "hi"
    agent.kickoff_async.assert_awaited_once_with("the prompt")
    update_mock.assert_not_called()


@pytest.mark.asyncio
async def test_kickoff_traces_and_tags_session():
    """Tracing ready → runs the crew/flow MLflow setup for the group and tags the
    chat session id + user; kickoff runs exactly once."""
    svc = LightAgentService()
    agent = AsyncMock()
    agent.kickoff_async = AsyncMock(return_value=SimpleNamespace(raw="hello"))
    ctx = make_group_context(["g1"])
    ctx.group_email = "user@example.com"
    config = SimpleNamespace(model="m", inputs={"run_name": "My Run"}, session_id="sess-42")

    update_mock = MagicMock()
    with _patch_mlflow_uc_stack(_mlflow_result(), update_mock) as configure_mock:
        out = await svc._kickoff_with_mlflow_trace(
            agent, "the prompt", config, "exec-1", "ctx", ctx, "g1")

    assert out.raw == "hello"
    agent.kickoff_async.assert_awaited_once()
    configure_mock.assert_awaited_once()
    assert configure_mock.await_args.kwargs.get("group_id") == "g1"
    # Chat-only session/user metadata.
    update_mock.assert_called_once()
    md = update_mock.call_args.kwargs.get("metadata")
    assert md["mlflow.trace.session"] == "sess-42"
    assert md["mlflow.trace.user"] == "user@example.com"


@pytest.mark.asyncio
async def test_kickoff_session_id_from_inputs_fallback():
    """session_id may ride in config.inputs (not top-level) — it's still tagged."""
    svc = LightAgentService()
    agent = AsyncMock()
    agent.kickoff_async = AsyncMock(return_value=SimpleNamespace(raw="hello"))
    config = SimpleNamespace(model="m", inputs={"session_id": "sess-in-inputs"}, session_id=None)

    update_mock = MagicMock()
    with _patch_mlflow_uc_stack(_mlflow_result(), update_mock):
        out = await svc._kickoff_with_mlflow_trace(
            agent, "the prompt", config, "exec-1", "ctx", None, "g1")

    assert out.raw == "hello"
    update_mock.assert_called_once()
    assert update_mock.call_args.kwargs.get("metadata")["mlflow.trace.session"] == "sess-in-inputs"


@pytest.mark.asyncio
async def test_kickoff_no_session_tag_without_session_id():
    """No session_id anywhere → still traced, but NO session metadata is set."""
    svc = LightAgentService()
    agent = AsyncMock()
    agent.kickoff_async = AsyncMock(return_value=SimpleNamespace(raw="x"))
    config = SimpleNamespace(model="m", inputs={}, session_id=None)

    update_mock = MagicMock()
    with _patch_mlflow_uc_stack(_mlflow_result(), update_mock) as configure_mock:
        out = await svc._kickoff_with_mlflow_trace(
            agent, "the prompt", config, "exec-1", "ctx", None, "g1")

    assert out.raw == "x"
    agent.kickoff_async.assert_awaited_once()
    configure_mock.assert_awaited_once()  # still traced
    update_mock.assert_not_called()  # but no session tag
