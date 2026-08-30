"""The bridge stamps memory record identity on memory spans.

A run's memory views resolve "what did this run write / read" on these ids
(the chat path stamps the same keys from its own save hook). Without them the
crew and flow paths fell back to time-window guessing, which attributed other
runs' records — every chat turn, maintenance merge, and (for the oldest run)
everything ever written — to whichever run was selected.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from src.services.otel_tracing.event_bridge import OTelEventBridge


def _bridge_with_span():
    tracer = MagicMock()
    span = MagicMock()
    tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=span)
    tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
    return OTelEventBridge(tracer=tracer, job_id="job-1"), span


def _attrs(span) -> dict:
    return {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}


class TestMemoryRecordIdentity:
    def test_save_completed_carries_the_stored_record_id(self):
        bridge, span = _bridge_with_span()
        event = SimpleNamespace(
            value="the answer",
            save_time_ms=1.5,
            record_id="rec-1",
            metadata={"task_name": "t"},
        )
        bridge._emit_span("kasal.memory.save_completed", "memory_write", event)

        attrs = _attrs(span)
        assert attrs["kasal.extra.record_id"] == "rec-1"
        assert attrs["kasal.extra.save_time_ms"] == 1.5

    def test_query_completed_carries_the_recalled_record_ids(self):
        bridge, span = _bridge_with_span()
        results = [
            SimpleNamespace(id="a"),
            SimpleNamespace(id="b"),
            SimpleNamespace(id=None),  # no id → nothing to resolve on
        ]
        event = SimpleNamespace(
            query="q", results=results, limit=5, score_threshold=0.5, query_time_ms=2.0
        )
        bridge._emit_span("kasal.memory.query_completed", "memory_retrieval", event)

        attrs = _attrs(span)
        assert attrs["kasal.extra.results_count"] == 3
        assert list(attrs["kasal.extra.record_ids"]) == ["a", "b"]

    def test_no_identity_attributes_when_the_event_carries_none(self):
        bridge, span = _bridge_with_span()
        bridge._emit_span(
            "kasal.memory.save_completed",
            "memory_write",
            SimpleNamespace(value="x", save_time_ms=1.0, record_id=None),
        )
        assert "kasal.extra.record_id" not in _attrs(span)

        bridge, span = _bridge_with_span()
        bridge._emit_span(
            "kasal.memory.query_completed",
            "memory_retrieval",
            SimpleNamespace(query="q", results=[], limit=5, query_time_ms=1.0),
        )
        attrs = _attrs(span)
        assert attrs["kasal.extra.results_count"] == 0
        assert "kasal.extra.record_ids" not in attrs


class TestMemoryRecordIdentityEdges:
    """The shapes a real bus can hand the bridge that the happy path does not.

    ``_set_extra_attributes`` is exercised directly: these are about what gets
    stamped, not about span creation.
    """

    def test_record_id_is_stringified(self):
        bridge, span = _bridge_with_span()
        bridge._set_extra_attributes(span, SimpleNamespace(record_id=42))
        assert _attrs(span)["kasal.extra.record_id"] == "42"

    def test_empty_string_record_id_not_stamped(self):
        bridge, span = _bridge_with_span()
        bridge._set_extra_attributes(span, SimpleNamespace(record_id=""))
        assert "kasal.extra.record_id" not in _attrs(span)

    def test_plain_string_results_count_only(self):
        """Older engines / tests hand back bare strings — no id to resolve on."""
        bridge, span = _bridge_with_span()
        bridge._set_extra_attributes(span, SimpleNamespace(results=["r1", "r2"]))
        attrs = _attrs(span)
        assert attrs["kasal.extra.results_count"] == 2
        assert "kasal.extra.record_ids" not in attrs

    def test_results_without_an_id_attribute_are_skipped(self):
        bridge, span = _bridge_with_span()
        results = [SimpleNamespace(id="keep"), SimpleNamespace(content="no id attr")]
        bridge._set_extra_attributes(span, SimpleNamespace(results=results))
        assert list(_attrs(span)["kasal.extra.record_ids"]) == ["keep"]


class TestRecallPlanStamps:
    """What recall planning did rides on the same span as the query."""

    def test_distilled_query_and_rounds_are_stamped(self):
        bridge, span = _bridge_with_span()
        bridge._set_extra_attributes(
            span,
            SimpleNamespace(
                query="x" * 300,
                distilled_query="latest Switzerland news today",
                exploration_rounds=2,
                results=[],
            ),
        )
        attrs = _attrs(span)
        assert attrs["kasal.extra.distilled_query"] == "latest Switzerland news today"
        assert attrs["kasal.extra.exploration_rounds"] == 2

    def test_absent_when_planning_did_nothing(self):
        bridge, span = _bridge_with_span()
        bridge._set_extra_attributes(
            span, SimpleNamespace(query="q", distilled_query=None, exploration_rounds=0)
        )
        attrs = _attrs(span)
        assert "kasal.extra.distilled_query" not in attrs
        assert "kasal.extra.exploration_rounds" not in attrs
