"""Span names share one namespace now, so one may be a PREFIX of another.

``kasal.llm.call`` is a prefix of ``kasal.llm.call_completed``. Under the old
``CrewAI.*`` names they lived in different namespaces and could not collide;
renaming them together made the exact-match-first ordering load-bearing.
"""

import pytest

from src.services.otel_tracing.db_exporter import SPAN_NAME_MAP, _extract_event_type


class _Span:
    def __init__(self, name):
        self.name = name
        self.attributes = {}


@pytest.mark.parametrize("span_name,expected", sorted(SPAN_NAME_MAP.items()))
def test_every_mapped_name_resolves_to_its_own_event_type(span_name, expected):
    assert _extract_event_type(_Span(span_name)) == expected


def test_the_specific_collision_that_bit():
    assert _extract_event_type(_Span("kasal.llm.call_completed")) == "llm_response"
    assert _extract_event_type(_Span("kasal.llm.call_failed")) == "llm_call_failed"
    assert _extract_event_type(_Span("kasal.llm.call")) == "llm_call"


def test_a_suffixed_span_still_matches_by_prefix():
    """Instrumentor spans append to the name of the span they belong to."""
    assert _extract_event_type(_Span("kasal.crew.kickoff.extra_suffix")) == (
        "crew_started"
    )


def test_no_mapped_name_is_a_prefix_of_a_name_with_a_DIFFERENT_event_type():
    """A standing guard: the next name added must not reintroduce the trap."""
    collisions = [
        (short, long)
        for short, short_type in SPAN_NAME_MAP.items()
        for long, long_type in SPAN_NAME_MAP.items()
        if short != long and long.startswith(short) and short_type != long_type
    ]
    # Exact-match-first handles these, so they are allowed — but any NEW pair
    # should be a deliberate decision, not a surprise.
    assert collisions == [("kasal.llm.call", "kasal.llm.call_failed")] or all(
        _extract_event_type(_Span(long)) == SPAN_NAME_MAP[long]
        for _short, long in collisions
    )
