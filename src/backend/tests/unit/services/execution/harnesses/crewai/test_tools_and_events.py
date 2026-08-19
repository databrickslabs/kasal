"""The tool wrapper and the event bridge.

Both exist so that switching harnesses changes WHICH runtime executes and nothing
else — the same tool implementation runs, and the same events reach the same
single trace writer.
"""

import importlib

import pytest
from pydantic import BaseModel, Field

import src.core.events as kasal_events
from src.core.events import event_bus
from src.services.execution.harnesses.crewai import events as bridge
from src.services.execution.harnesses.crewai.tools import adapt_tool, adapt_tools
from src.services.tools.base import BaseTool as KasalBaseTool


class _Args(BaseModel):
    q: str = Field(description="the query")


class _Search(KasalBaseTool):
    name: str = "search"
    description: str = "Search for things"
    args_schema: type = _Args

    def _run(self, q: str) -> str:
        return f"results for {q}"


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


class TestToolAdapter:
    def test_a_kasal_tool_becomes_a_crewai_tool(self):
        from crewai.tools import BaseTool as CrewBaseTool

        assert isinstance(adapt_tool(_Search()), CrewBaseTool)

    def test_the_wrapped_tool_is_the_one_that_runs(self):
        """No reimplementation. One tool, one Databricks query, one redaction."""
        assert adapt_tool(_Search()).run(q="x") == "results for x"

    def test_the_argument_schema_is_passed_through_not_rebuilt(self):
        """CrewAI turns this into the function schema the MODEL sees.

        Regenerating it would give the same tool a different signature per
        harness — exactly the difference that makes a comparison meaningless.
        """
        assert adapt_tool(_Search()).args_schema is _Args

    def test_name_and_description_are_carried(self):
        adapted = adapt_tool(_Search())
        assert adapted.name == "search"
        assert "Search for things" in adapted.description

    def test_an_already_crewai_tool_is_not_wrapped_twice(self):
        """A wrapper around a wrapper reports the inner name and loses the
        outer schema. MCP tools arrive already adapted."""
        once = adapt_tool(_Search())
        assert adapt_tool(once) is once

    def test_the_tool_object_is_not_serialized_with_the_adapter(self):
        """It holds sessions, credentials and a group context."""
        assert "_kasal_tool" not in adapt_tool(_Search()).model_dump()

    def test_an_uncallable_tool_is_dropped_with_a_warning_not_raised(self):
        """One malformed tool must not fail a run that has twelve others.

        Dropped rather than passed through: a tool the adapter cannot invoke
        would still reach the model as a usable function and fail at the moment
        the agent commits to it.
        """
        assert len(adapt_tools([_Search(), object()])) == 1

    def test_no_tools_is_an_empty_list_not_none(self):
        assert adapt_tools(None) == []


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


class TestEventBridge:
    def test_every_declared_pair_resolves_in_this_build(self):
        """A name that no longer exists on either side bridges nothing, silently."""
        resolved = {kasal.__name__ for _, kasal in bridge._pairs()}
        assert resolved == set(bridge._BRIDGED)

    def test_a_crewai_event_arrives_on_the_kasal_bus_translated(self):
        from crewai.events.types.agent_events import (
            LiteAgentExecutionCompletedEvent as CrewEvent,
        )

        seen = []
        handler = lambda source, event: seen.append(event)  # noqa: E731
        event_bus.register_handler(
            kasal_events.LiteAgentExecutionCompletedEvent, handler
        )
        crewai_bus = importlib.import_module("crewai.events.event_bus").crewai_event_bus
        try:
            with bridge.bridge_events():
                future = crewai_bus.emit(
                    "source", CrewEvent(agent_info={"role": "Assistant"}, output="42")
                )
                # CrewAI dispatches handlers on a thread pool and returns a
                # Future. Asserting straight after `emit` is a race that passes
                # on an idle machine and fails on a busy one — which is how
                # this test first failed only under xdist.
                if future is not None:
                    future.result(timeout=10)
        finally:
            event_bus.off(kasal_events.LiteAgentExecutionCompletedEvent, handler)

        assert len(seen) == 1
        assert isinstance(seen[0], kasal_events.LiteAgentExecutionCompletedEvent)
        assert seen[0].output == "42"
        assert seen[0].agent_info == {"role": "Assistant"}

    def test_handlers_are_removed_when_the_run_ends(self):
        """The Chat path serves many turns in one process.

        A handler outliving its run keeps firing against a finished execution,
        which is how a trace ends up attached to the wrong job.
        """
        from crewai.events.types.agent_events import (
            LiteAgentExecutionCompletedEvent as CrewEvent,
        )

        seen = []
        handler = lambda source, event: seen.append(event)  # noqa: E731
        event_bus.register_handler(
            kasal_events.LiteAgentExecutionCompletedEvent, handler
        )
        crewai_bus = importlib.import_module("crewai.events.event_bus").crewai_event_bus
        try:
            with bridge.bridge_events():
                pass
            future = crewai_bus.emit("source", CrewEvent(agent_info={}, output="after"))
            if future is not None:
                future.result(timeout=10)
        finally:
            event_bus.off(kasal_events.LiteAgentExecutionCompletedEvent, handler)

        assert seen == []

    def test_handlers_are_removed_even_when_the_run_raises(self):
        from crewai.events.types.agent_events import (
            LiteAgentExecutionCompletedEvent as CrewEvent,
        )

        seen = []
        handler = lambda source, event: seen.append(event)  # noqa: E731
        event_bus.register_handler(
            kasal_events.LiteAgentExecutionCompletedEvent, handler
        )
        crewai_bus = importlib.import_module("crewai.events.event_bus").crewai_event_bus
        try:
            with pytest.raises(RuntimeError):
                with bridge.bridge_events():
                    raise RuntimeError("the run failed")
            future = crewai_bus.emit("source", CrewEvent(agent_info={}, output="after"))
            if future is not None:
                future.result(timeout=10)
        finally:
            event_bus.off(kasal_events.LiteAgentExecutionCompletedEvent, handler)

        assert seen == []

    def test_the_bus_is_flushed_before_handlers_are_removed(self):
        """Otherwise the tail of every run is lost.

        CrewAI's ``emit`` is fire-and-forget, so when a run finishes its LAST
        events are still queued on a worker thread. Unregistering first drops
        exactly the completion events a timeline ends on — a silent hole,
        precisely at the point someone looks to confirm the run finished.
        """
        crewai_bus = importlib.import_module("crewai.events.event_bus").crewai_event_bus
        order = []
        real_flush, real_off = crewai_bus.flush, crewai_bus.off

        def _flush(*a, **k):
            order.append("flush")
            return real_flush(*a, **k)

        def _off(*a, **k):
            order.append("off")
            return real_off(*a, **k)

        crewai_bus.flush, crewai_bus.off = _flush, _off
        try:
            with bridge.bridge_events():
                pass
        finally:
            crewai_bus.flush, crewai_bus.off = real_flush, real_off

        assert "flush" in order, "teardown never flushed the CrewAI bus"
        assert order.index("flush") < order.index("off")

    def test_a_translation_failure_does_not_propagate_into_the_run(self):
        """Telemetry must never fail an execution."""

        class Broken:
            model_fields = {"nope": None}

            def __init__(self, **kwargs):
                raise ValueError("cannot construct")

        # Raises inside the handler; bridge_events swallows and logs.
        with bridge.bridge_events():
            assert bridge._translate.__name__ == "_translate"
        with pytest.raises(ValueError):
            bridge._translate(Broken, object())


class TestEventCoverageIsAccountedFor:
    def test_every_traced_event_is_either_bridged_or_declared_kasal_sourced(self):
        """A silent hole in the trace is the failure this file exists to avoid.

        The OTel bridge is the one trace writer; anything it knows how to map
        must have a stated source under the CrewAI harness — either CrewAI emits
        it and we bridge, or a Kasal subsystem emits it directly.
        """
        from src.services.otel_tracing.event_bridge import _EVENT_SPAN_MAP

        traced = set(_EVENT_SPAN_MAP)
        accounted = set(bridge._BRIDGED) | set(bridge._SOURCED_FROM_KASAL)
        unaccounted = traced - accounted
        assert not unaccounted, (
            "These event types are written to traces but have no stated source "
            f"under the CrewAI harness: {sorted(unaccounted)}. Add each to "
            "_BRIDGED (CrewAI emits it) or _SOURCED_FROM_KASAL (a Kasal "
            "subsystem does)."
        )


class TestCrewAIFlowEventsAreNotBridged:
    """CrewAI's flow events do not describe a flow here.

    In CrewAI 1.15 the agent executor IS a Flow —
    ``class AgentExecutor(Flow[AgentExecutorState], BaseAgentExecutor)`` — so
    every agent TURN emits FlowStartedEvent(flow_name="AgentExecutor").
    Republishing those on Kasal's bus reads as a new flow run, and
    ``flow_started`` opens the OUTERMOST causality scope, so each one re-roots
    every event after it. A measured flow run recorded six flow_started rows
    against two flow_completed: one real "DynamicFlow" and five "AgentExecutor".
    """

    def test_the_flow_lifecycle_is_not_in_the_bridge(self):
        from src.services.execution.harnesses.crewai import events as bridge

        assert "FlowStartedEvent" not in bridge._BRIDGED
        assert "FlowFinishedEvent" not in bridge._BRIDGED

    def test_it_is_recorded_as_kasal_sourced_instead_of_forgotten(self):
        # The completeness test treats an event in neither set as a hole; these
        # are emitted by Kasal's own flow runtime under both harnesses.
        from src.services.execution.harnesses.crewai import events as bridge

        assert "FlowStartedEvent" in bridge._SOURCED_FROM_KASAL
        assert "FlowFinishedEvent" in bridge._SOURCED_FROM_KASAL

    def test_kasals_own_flow_runtime_still_emits_them(self):
        """The flow layer is Kasal's under both harnesses, so the events the
        timeline needs still arrive — from the runtime, not from CrewAI."""
        import inspect

        from src.services.flow_builder.runtime import flow as kasal_flow

        source = inspect.getsource(kasal_flow)
        assert "FlowStartedEvent(" in source
        assert "FlowFinishedEvent(" in source
