"""The two head starts that put a deck on screen before its answer exists.

These decide what the reader sees in the first ~20 seconds of a presentation
request, so the gating matters as much as the output: a shell shipped for a turn
that answers in prose is an empty deck frame stranded in the transcript.
"""

import asyncio
from typing import Any, Dict, List

import pytest

from src.services.a2ui import early
from src.services.a2ui.stream import SURFACE_ID, apply_messages


def _sink(out: List[Dict[str, Any]]):
    async def _on_delta(msg):
        out.append(msg)

    return _on_delta


@pytest.fixture
def enabled(monkeypatch):
    """A workspace with A2UI on and no per-deliverable guidance."""

    async def _resolve(group_id, query):
        return True, ""

    monkeypatch.setattr(early, "_resolve", _resolve)


# ── what gets a head start ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "query,kind",
    [
        ("make a quiz about python", "quiz"),
        ("build flashcards for spanish verbs", "flashcards"),
        ("draw a mindmap of the org", "mindmap"),
        ("show a map of our stores", "map"),
    ],
)
def test_every_canvas_owning_kind_gets_a_shell(query, kind):
    """A shell is a promise that a surface of this shape is coming, and for
    these kinds the request itself is enough to make it."""
    assert early.shell_kind(query) == kind
    assert early.wants_instant_shell(query) is True


@pytest.mark.parametrize(
    "query,kind",
    [
        ("make me a kanban board for the sprint", "dashboard"),
        ("show me an album of alpine photos", "dashboard"),
        ("build a network graph of services", "dashboard"),
        ("show me a dashboard of sales", "dashboard"),
        ("give me a forecast of revenue", "document"),
        ("write a report on Q3", "document"),
    ],
)
def test_component_deliverables_get_a_provisional_shell(query, kind):
    """Asking for a board or a gallery is just as explicit as asking for a deck.

    These render on dashboard/document, which the prose gate CAN drop, so their
    frame is provisional — the client keeps streaming the answer's text
    underneath one of these, which is what makes a retraction cost nothing.
    """
    assert early.shell_kind(query) == kind


@pytest.mark.parametrize(
    "query",
    [
        "",
        "what is the capital of France?",
        "summarise this document for me",
        "who won the match last night?",
    ],
)
def test_a_request_naming_no_deliverable_gets_no_shell(query):
    """A frame is a promise about SHAPE. With no deliverable named there is no
    shape to promise, and an empty frame over a prose answer is pure noise."""
    assert early.shell_kind(query) is None
    assert early.wants_instant_shell(query) is False


# ── the instant shell ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_shell_is_skipped_for_a_non_deck(enabled):
    sent: List[Dict[str, Any]] = []
    assert await early.emit_instant_shell("what is 2+2?", on_delta=_sink(sent)) is False
    assert sent == []


@pytest.mark.asyncio
async def test_the_shell_is_skipped_when_a2ui_is_off(monkeypatch):
    async def _off(group_id, query):
        return False, ""

    monkeypatch.setattr(early, "_resolve", _off)
    sent: List[Dict[str, Any]] = []
    assert (
        await early.emit_instant_shell("create a presentation on X", on_delta=_sink(sent))
        is False
    )
    assert sent == []


@pytest.mark.asyncio
async def test_a_throwing_sink_cannot_fail_the_run(enabled):
    async def boom(_msg):
        raise RuntimeError("SSE is down")

    assert (
        await early.emit_instant_shell("create a presentation on X", on_delta=boom)
        is False
    )


# ── the outline head start ──────────────────────────────────────────────────


