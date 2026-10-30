"""The router's served model reaches the execution_trace row, on every path.

Three producers write an ``llm_response`` row: the OTel bridge + DB exporter
(crew/flow subprocess and in-process crews), the event pipe (the live view of a
subprocess run), and the chat light path, which builds its row by hand. Each
must carry ``served_model`` so the Run activity can say "LLM Response —
anthropic/claude-opus-5-5 (628 chars)" instead of only naming jev-router.
"""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.core.events.types import LLMCallCompletedEvent, LLMCallType
from src.services.execution.event_pipe import EventPipeWriter
from src.services.llm.manager import _reply
from src.services.otel_tracing.db_exporter import (
    _extract_output,
    _extract_trace_metadata,
)
from src.services.otel_tracing.event_bridge import OTelEventBridge
from tests.unit.services.chat.test_execution_runner_light_agent import (
    _run_with_captured_handlers,
    make_config,
    make_group_context,
)

SERVED = "anthropic/claude-opus-5-5"


def _event(served_model):
    return LLMCallCompletedEvent(
        model="typesafe/jev-router",
        response="y" * 628,
        call_type=LLMCallType.LLM_CALL,
        served_model=served_model,
    )


def _span_attributes(event) -> dict:
    bridge = object.__new__(OTelEventBridge)
    bridge._current_crew_name = None
    span = MagicMock()
    bridge._set_extra_attributes(span, event)
    return {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}


class TestTheOTelPath:
    def test_the_row_carries_it_in_metadata_and_extra_data(self):
        span = MagicMock()
        span.name = "kasal.llm.call_completed"
        span.attributes = _span_attributes(_event(SERVED))

        assert _extract_trace_metadata(span)["served_model"] == SERVED
        assert _extract_output(span)["extra_data"]["served_model"] == SERVED

    def test_no_served_model_adds_no_attribute(self):
        assert "kasal.extra.served_model" not in _span_attributes(_event(None))


class TestTheLivePipe:
    def _frame(self, event):
        writer = EventPipeWriter(queue=MagicMock(), execution_id="run-1")
        return writer._project_trace_frame("llm_response", event)

    def test_the_live_frame_carries_it(self):
        assert self._frame(_event(SERVED))["trace_metadata"]["served_model"] == SERVED

    def test_and_omits_it_when_absent(self):
        assert "served_model" not in self._frame(_event(None))["trace_metadata"]


@pytest.mark.asyncio
@pytest.mark.parametrize("served", [SERVED, None])
async def test_the_chat_light_path_row_carries_it(served):
    config = make_config(
        agents_yaml={
            "agent_a1": {"id": "agent_a1", "role": "R", "goal": "g", "backstory": "b"}
        },
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "find"}},
    )
    mock_agent = AsyncMock()
    mock_agent.id = "aid-1"
    trace_instance = MagicMock()
    trace_instance.create_trace = AsyncMock()

    def _emit(captured):
        captured["LLMCallCompletedEvent"](
            mock_agent,
            SimpleNamespace(
                agent_id="aid-1",
                model="typesafe/jev-router",
                response="ok",
                served_model=served,
            ),
        )

    await _run_with_captured_handlers(
        f"light-{uuid.uuid4()}",
        config,
        make_group_context(["g1"]),
        mock_agent,
        trace_instance,
        _emit,
    )

    td = trace_instance.create_trace.await_args.args[0]
    assert td["trace_metadata"].get("served_model") == served
    assert td["output"]["extra_data"].get("served_model") == served


class TestCompletionWithServedModel:
    """LLMManager.completion(with_served_model=True) names the router's pick."""

    def test_the_router_pick_wins(self):
        assert (
            _reply("ok", SimpleNamespace(served_model=SERVED), "jev-router", True)[1]
            == SERVED
        )

    @pytest.mark.parametrize("llm", [SimpleNamespace(served_model=None), MagicMock()])
    def test_otherwise_the_resolved_key(self, llm):
        assert _reply("ok", llm, "jev-router", True)[1] == "jev-router"

    def test_without_the_flag_the_bare_text(self):
        assert _reply("ok", SimpleNamespace(served_model=SERVED), "x", False) == "ok"
