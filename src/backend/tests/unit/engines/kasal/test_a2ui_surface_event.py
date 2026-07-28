"""A2UI composition must be visible on the bus and in the trace.

Composing a surface is a gauntlet of quiet gates: A2UI off for the workspace,
no rich intent in the request, the composer falling back to prose, a dashboard
that came back with no data in it. Every one of them returned the answer as
plain text and left NOTHING behind — no event, no span, no trace row — so
"why did I not get a presentation?" could only be answered by reading the logs
of a subprocess, if they still existed.

These pin the event down at both ends: the composer emits one for every outcome,
and the OTel bridge turns it into a row the timeline can render.
"""

import pytest

from kasal_engine.events.bus import crewai_event_bus
from kasal_engine.events.types import A2UISurfaceEvent
from src.engines.kasal.kernel import a2ui_runner


@pytest.fixture
def captured_events():
    events = []
    with crewai_event_bus.scoped_handlers():

        @crewai_event_bus.on(A2UISurfaceEvent)
        def _capture(source, event):  # noqa: ANN001
            events.append(event)

        yield events


class TestEmission:
    def test_an_empty_answer_is_reported_not_silently_dropped(self, captured_events):
        a2ui_runner._emit_surface_event(
            "no_text", reason="the answer was empty, so there was nothing to render"
        )

        (event,) = captured_events
        assert event.outcome == "no_text"
        assert "nothing to render" in event.reason

    def test_a_composed_surface_carries_what_was_built(self, captured_events):
        surface = {
            "surfaceKind": "presentation",
            "components": [{"type": "Slide"}, {"type": "Slide"}, {"type": "Chart"}],
        }

        a2ui_runner._emit_surface_event(
            "composed", surface=surface, query="build me a deck", started_at=None
        )

        (event,) = captured_events
        assert event.outcome == "composed"
        assert event.surface_kind == "presentation"
        assert event.component_count == 3
        assert event.query == "build me a deck"

    def test_labels_are_truncated_because_they_are_not_payloads(self, captured_events):
        a2ui_runner._emit_surface_event(
            "no_rich_intent", query="q" * 500, purpose="p" * 500
        )

        (event,) = captured_events
        assert len(event.query) == 200
        assert len(event.purpose) == 200

    def test_emission_never_raises_into_the_answer_path(self, monkeypatch):
        """This sits on the answer path of every chat turn; observability must
        not be able to fail a run."""

        def _explode(*args, **kwargs):
            raise RuntimeError("bus is down")

        monkeypatch.setattr(crewai_event_bus, "emit", _explode)

        a2ui_runner._emit_surface_event("composed")  # must not raise


class TestReachesTheTrace:
    def test_the_bridge_knows_the_event(self):
        from src.services.otel_tracing.event_bridge import _EVENT_SPAN_MAP

        span_name, event_type = _EVENT_SPAN_MAP["A2UISurfaceEvent"]
        assert (span_name, event_type) == ("kasal.a2ui.compose", "a2ui_surface")

    def test_the_exporter_maps_the_span_to_a_row(self):
        from src.services.otel_tracing.db_exporter import SPAN_NAME_MAP

        assert SPAN_NAME_MAP["kasal.a2ui.compose"] == "a2ui_surface"

    def test_the_bridge_actually_subscribes_to_it(self):
        """The mapping is not the wiring. The bridge only receives what
        register() subscribes to — ContextCompactionEvent was mapped but never
        subscribed, and produced zero trace rows for its entire existence."""
        import inspect

        from src.services.otel_tracing.event_bridge import OTelEventBridge

        assert '"A2UISurfaceEvent"' in inspect.getsource(OTelEventBridge.register)

    def test_the_span_carries_the_fields_the_timeline_reads(self):
        from unittest.mock import MagicMock

        from src.services.otel_tracing.event_bridge import OTelEventBridge

        tracer = MagicMock()
        span = MagicMock()
        tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=span)
        tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
        bridge = OTelEventBridge(tracer, "job-a2ui")

        bridge._emit_span(
            "kasal.a2ui.compose",
            "a2ui_surface",
            A2UISurfaceEvent(
                outcome="composed",
                surface_kind="dashboard",
                component_count=4,
                duration_ms=1234.5,
            ),
        )

        span.set_attribute.assert_any_call("kasal.event_type", "a2ui_surface")
        span.set_attribute.assert_any_call("kasal.extra.outcome", "composed")
        span.set_attribute.assert_any_call("kasal.extra.surface_kind", "dashboard")
        span.set_attribute.assert_any_call("kasal.extra.component_count", 4)
        span.set_attribute.assert_any_call("kasal.extra.duration_ms", 1234.5)


class TestEveryGateReportsItself:
    """The gates are the point: each one used to return plain text silently."""

    @pytest.mark.asyncio
    async def test_empty_answer(self, captured_events):
        assert await a2ui_runner.compose_surface("   ") is None
        assert [e.outcome for e in captured_events] == ["no_text"]

    @pytest.mark.asyncio
    async def test_a2ui_disabled_for_the_workspace(self, captured_events, monkeypatch):
        async def _off(group_id, query):
            return False, None, ""

        monkeypatch.setattr(a2ui_runner, "_resolve_config", _off)

        assert await a2ui_runner.compose_surface("an answer") is None
        assert [e.outcome for e in captured_events] == ["disabled"]

    @pytest.mark.asyncio
    async def test_no_rich_intent(self, captured_events, monkeypatch):
        async def _on(group_id, query):
            return True, {"components": []}, ""

        monkeypatch.setattr(a2ui_runner, "_resolve_config", _on)
        monkeypatch.setattr(a2ui_runner, "wants_rich_surface", lambda *a: False)

        assert await a2ui_runner.compose_surface("hello there", query="hi") is None
        (event,) = captured_events
        assert event.outcome == "no_rich_intent"
        assert "plain text" in event.reason

    @pytest.mark.asyncio
    async def test_a_prose_fallback_is_not_a_surface(self, captured_events, monkeypatch):
        async def _on(group_id, query):
            return True, {"components": []}, ""

        async def _llm(*args, **kwargs):
            return object()

        monkeypatch.setattr(a2ui_runner, "_resolve_config", _on)
        monkeypatch.setattr(a2ui_runner, "wants_rich_surface", lambda *a: True)
        monkeypatch.setattr(
            "src.core.llm_manager.LLMManager.get_llm", staticmethod(_llm)
        )
        monkeypatch.setattr(
            a2ui_runner.asyncio,
            "to_thread",
            lambda *a, **k: _completed({"surfaceKind": "conversation"}),
        )

        assert await a2ui_runner.compose_surface("just prose", query="tell me") is None
        assert [e.outcome for e in captured_events] == ["conversation_fallback"]

    @pytest.mark.asyncio
    async def test_a_dashboard_with_no_data_is_reported_as_such(
        self, captured_events, monkeypatch
    ):
        """A dashboard of Text components would render the answer's own words
        twice; the skip is deliberate and now says so."""

        async def _on(group_id, query):
            return True, {"components": []}, ""

        async def _llm(*args, **kwargs):
            return object()

        monkeypatch.setattr(a2ui_runner, "_resolve_config", _on)
        monkeypatch.setattr(a2ui_runner, "wants_rich_surface", lambda *a: True)
        monkeypatch.setattr(
            "src.core.llm_manager.LLMManager.get_llm", staticmethod(_llm)
        )
        monkeypatch.setattr(
            a2ui_runner.asyncio,
            "to_thread",
            lambda *a, **k: _completed(
                {"surfaceKind": "dashboard", "components": [{"type": "Text"}]}
            ),
        )

        assert await a2ui_runner.compose_surface("prose", query="show me") is None
        (event,) = captured_events
        assert event.outcome == "no_data_component"
        assert event.surface_kind == "dashboard"

    @pytest.mark.asyncio
    async def test_a_real_surface_is_recorded_with_its_shape(
        self, captured_events, monkeypatch
    ):
        async def _on(group_id, query):
            return True, {"components": []}, ""

        async def _llm(*args, **kwargs):
            return object()

        surface = {
            "surfaceKind": "presentation",
            "components": [{"type": "Slide"}, {"type": "Slide"}],
        }
        monkeypatch.setattr(a2ui_runner, "_resolve_config", _on)
        monkeypatch.setattr(a2ui_runner, "wants_rich_surface", lambda *a: True)
        monkeypatch.setattr(
            "src.core.llm_manager.LLMManager.get_llm", staticmethod(_llm)
        )
        monkeypatch.setattr(
            a2ui_runner.asyncio, "to_thread", lambda *a, **k: _completed(surface)
        )

        result = await a2ui_runner.compose_surface("deck please", query="build a deck")

        assert result == surface
        (event,) = captured_events
        assert event.outcome == "composed"
        assert event.surface_kind == "presentation"
        assert event.component_count == 2
        assert event.duration_ms is not None, "how long composition took"


def _completed(value):
    """An already-finished awaitable, standing in for asyncio.to_thread."""
    import asyncio as _asyncio

    future = _asyncio.get_event_loop().create_future()
    future.set_result(value)
    return future


class TestItBelongsToTheRunThatTriggeredIt:
    """Composition ran where neither trace writer was listening.

    The crew path composes in the PARENT, after the subprocess that owns the
    OTel bridge has exited; the light path's writer only handles memory/LLM/tool
    events. So a bus event reached nothing — and because the engine bus is a
    module-global in the parent, a crew's composition could instead be picked up
    by whichever OTHER run had handlers registered at that moment. It looked
    like a separate execution because it belonged to no run at all.
    """

    @pytest.mark.asyncio
    async def test_the_row_is_written_against_the_originating_run(self, monkeypatch):
        written = {}

        async def _write(job_id, rows, **kwargs):
            written["job_id"] = job_id
            written["metadata"] = rows[0][3]

        monkeypatch.setattr(
            "src.services.trace.writer.write_rows", _write
        )

        a2ui_runner._emit_surface_event(
            "composed",
            surface={"surfaceKind": "presentation", "components": [{}, {}]},
            execution_id="job-42",
        )
        await __import__("asyncio").sleep(0)  # let the scheduled task run

        assert written["job_id"] == "job-42"
        assert written["metadata"]["surface_kind"] == "presentation"
        assert written["metadata"]["component_count"] == 2

    @pytest.mark.asyncio
    async def test_an_ordinary_prose_turn_adds_no_row(self, monkeypatch):
        """no_text / no_rich_intent / disabled all mean composition never ran —
        the normal state of a conversation. A row each would put one on EVERY
        chat turn to report that nothing happened. The bus still hears them."""
        rows = []

        async def _write(job_id, written_rows, **kwargs):
            rows.append(written_rows[0][3]["outcome"])

        monkeypatch.setattr(
            "src.services.trace.writer.write_rows", _write
        )

        for outcome in ("no_text", "no_rich_intent", "disabled"):
            a2ui_runner._emit_surface_event(outcome, execution_id="job-42")
        await __import__("asyncio").sleep(0)

        assert rows == []

    @pytest.mark.asyncio
    async def test_a_gate_that_closed_after_composing_does_add_a_row(self, monkeypatch):
        """Once the composer has actually run, every outcome is worth a row —
        these are the surprising ones."""
        rows = []

        async def _write(job_id, written_rows, **kwargs):
            rows.append(written_rows[0][3]["outcome"])

        monkeypatch.setattr(
            "src.services.trace.writer.write_rows", _write
        )

        for outcome in (
            "composed",
            "conversation_fallback",
            "no_data_component",
            "compose_failed",
            "composer_unavailable",
        ):
            a2ui_runner._emit_surface_event(outcome, execution_id="job-42")
        await __import__("asyncio").sleep(0)

        assert len(rows) == 5

    @pytest.mark.asyncio
    async def test_no_run_id_means_no_row_rather_than_a_guessed_one(self, monkeypatch):
        """Filing composition under a run it did not come from is worse than not
        filing it — that is the cross-attribution this replaced."""
        called = []

        async def _write(job_id, rows, **kwargs):
            called.append(job_id)

        monkeypatch.setattr(
            "src.services.trace.writer.write_rows", _write
        )

        a2ui_runner._emit_surface_event("composed", execution_id=None)
        await __import__("asyncio").sleep(0)

        assert called == []

    @pytest.mark.asyncio
    async def test_the_row_hangs_under_the_run_root_not_beside_it(self):
        """Without a parent the row renders at the top of the trace as if it
        were its own run — which is what it looked like."""
        from unittest.mock import AsyncMock, MagicMock

        from src.services.trace import writer

        session = MagicMock()
        session.execute = AsyncMock(
            return_value=MagicMock(
                all=MagicMock(
                    return_value=[
                        ("crew_started", "root-span"),
                        ("task_started", "task-span"),
                    ]
                )
            )
        )

        assert await writer._root_span_id(session, "job-42") == "root-span"

    @pytest.mark.asyncio
    async def test_a_flow_owns_its_crews_so_its_span_is_the_root(self):
        from unittest.mock import AsyncMock, MagicMock

        from src.services.trace import writer

        session = MagicMock()
        session.execute = AsyncMock(
            return_value=MagicMock(
                all=MagicMock(
                    return_value=[
                        ("crew_started", "crew-span"),
                        ("flow_started", "flow-span"),
                    ]
                )
            )
        )

        assert await writer._root_span_id(session, "job-42") == "flow-span"

    @pytest.mark.asyncio
    async def test_a_run_with_no_spans_still_records(self):
        """Parenting is a nicety; losing the row entirely is not acceptable."""
        from unittest.mock import AsyncMock, MagicMock

        from src.services.trace import writer

        session = MagicMock()
        session.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=[])))

        assert await writer._root_span_id(session, "job-42") is None


class TestItFilesUnderTheRunsOwnAgentAndTask:
    """`event_source="A2UI"` made composition its own AGENT lane with a task
    called "composed" — the separate-execution look, one level down. It is not a
    separate agent: it is the last thing this run's agent did with its answer."""

    @pytest.mark.asyncio
    async def test_attribution_comes_from_the_runs_own_rows(self):
        from unittest.mock import AsyncMock, MagicMock

        from src.services.trace import writer

        session = MagicMock()
        session.execute = AsyncMock(
            return_value=MagicMock(
                all=MagicMock(
                    return_value=[
                        # Newest first; crew/System rows are not the agent.
                        ("crew", "crew", {}),
                        ("Quantitative Analyst", "Solve this problem", {"task_id": "T1"}),
                    ]
                )
            )
        )

        attribution = await writer.resolve_attribution(session, "job-42")

        assert attribution["event_source"] == "Quantitative Analyst"
        assert attribution["event_context"] == "Solve this problem"
        assert attribution["task_id"] == "T1", "what nests the row inside the task"

    @pytest.mark.asyncio
    async def test_a_run_with_no_agent_rows_still_records_under_a2ui(self):
        from unittest.mock import AsyncMock, MagicMock

        from src.services.trace import writer

        session = MagicMock()
        session.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=[])))

        assert await writer.resolve_attribution(session, "job-42") == {}
