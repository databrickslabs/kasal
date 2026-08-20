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
        ("create a presentation on how llm works", "presentation"),
        ("make me a deck about Q3", "presentation"),
        ("build a slideshow covering onboarding", "presentation"),
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
async def test_the_shell_ships_a_whole_renderable_deck(enabled):
    sent: List[Dict[str, Any]] = []
    assert await early.emit_instant_shell(
        "create a presentation on how llm works", on_delta=_sink(sent)
    )
    surface = apply_messages(sent)
    assert surface["surfaceKind"] == "presentation"
    assert surface["components"][0]["component"] == "SlideDeck"
    assert surface["components"][1]["title"] == "How LLM Works"
    assert sent[0]["createSurface"]["surfaceId"] == SURFACE_ID


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
async def test_the_kill_switch_disables_both_head_starts(monkeypatch, enabled):
    monkeypatch.setenv("A2UI_EARLY", "false")
    sent: List[Dict[str, Any]] = []
    assert (
        await early.emit_instant_shell("create a presentation on X", on_delta=_sink(sent))
        is False
    )
    assert (
        await early.plan_outline_early(
            "x" * 5000, query="create a presentation on X", on_delta=_sink(sent)
        )
        is None
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


@pytest.mark.asyncio
async def test_the_outline_waits_for_enough_of_the_answer(enabled, monkeypatch):
    """Planned from an introduction, the deck would be an introduction."""
    monkeypatch.setenv("A2UI_OUTLINE_HEADSTART_CHARS", "1500")
    called = False

    def _never(*a, **k):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(early, "plan_presentation_outline", _never)
    assert (
        await early.plan_outline_early(
            "too short", query="create a presentation on X"
        )
        is None
    )
    assert called is False


@pytest.mark.asyncio
async def test_the_outline_fires_once_the_answer_is_long_enough(
    enabled, monkeypatch
):
    plan = [
        {"title": "Why now", "variant": "title", "visual": "none", "focus": ""},
        {"title": "The market", "variant": "content", "visual": "none", "focus": ""},
        {"title": "Takeaways", "variant": "content", "visual": "none", "focus": ""},
    ]
    monkeypatch.setenv("A2UI_OUTLINE_HEADSTART_CHARS", "100")

    class _LLM:
        def call(self, messages):
            return "{}"

    async def _get_llm(*a, **k):
        return _LLM()

    monkeypatch.setattr(early, "plan_presentation_outline", lambda *a, **k: plan)
    import src.services.llm.manager as mgr

    monkeypatch.setattr(mgr.LLMManager, "get_llm", staticmethod(_get_llm))

    sent: List[Dict[str, Any]] = []
    out = await early.plan_outline_early(
        "x" * 500, query="create a presentation on X", on_delta=_sink(sent)
    )
    assert out == plan
    # It ships the skeleton itself, so the reader gets real titles the moment
    # the plan exists rather than when composition later gets round to it.
    surface = apply_messages(sent)
    assert [c.get("title") for c in surface["components"][1:]] == [
        "Why now",
        "The market",
        "Takeaways",
    ]
    assert all(c["pending"] for c in surface["components"][1:])


@pytest.mark.asyncio
async def test_a_failed_outline_degrades_to_no_head_start(enabled, monkeypatch):
    """Composition then plans it itself from the complete answer, as before."""
    monkeypatch.setenv("A2UI_OUTLINE_HEADSTART_CHARS", "10")

    async def _explode(*a, **k):
        raise RuntimeError("no model available")

    import src.services.llm.manager as mgr

    monkeypatch.setattr(mgr.LLMManager, "get_llm", staticmethod(_explode))
    assert (
        await early.plan_outline_early(
            "x" * 500, query="create a presentation on X"
        )
        is None
    )


def test_the_headstart_threshold_survives_a_bad_env(monkeypatch):
    monkeypatch.setenv("A2UI_OUTLINE_HEADSTART_CHARS", "not-a-number")
    assert early.outline_headstart_chars() == 1500
    monkeypatch.setenv("A2UI_OUTLINE_HEADSTART_CHARS", "-5")
    assert early.outline_headstart_chars() == 0


@pytest.mark.asyncio
async def test_the_outline_restores_the_tenant_context(enabled, monkeypatch):
    """The regression that made this head start a no-op on every single run.

    The task is created from the token-flush callback, which is scheduled onto
    the loop from an LLM worker thread, so it inherits the loop's default
    context rather than the request's. ``LLMManager.get_llm`` RAISES without a
    group_id — correctly, it is the isolation guarantee — so the head start died
    silently every time and the outline went back to running after the answer.
    """
    monkeypatch.setenv("A2UI_OUTLINE_HEADSTART_CHARS", "10")
    from src.utils.user_context import UserContext

    seen = {}

    class _Ctx:
        primary_group_id = "grp-1"

    monkeypatch.setattr(UserContext, "get_group_context", staticmethod(lambda: None))
    monkeypatch.setattr(
        UserContext,
        "set_group_context",
        staticmethod(lambda c: seen.__setitem__("ctx", c)),
    )

    class _LLM:
        def call(self, messages):
            return "{}"

    async def _get_llm(*a, **k):
        if "ctx" not in seen:
            raise ValueError("group_id is REQUIRED for get_llm")
        return _LLM()

    import src.services.llm.manager as mgr

    monkeypatch.setattr(mgr.LLMManager, "get_llm", staticmethod(_get_llm))
    monkeypatch.setattr(early, "plan_presentation_outline", lambda *a, **k: [])

    ctx = _Ctx()
    await early.plan_outline_early(
        "x" * 500, query="create a presentation on X", group_context=ctx
    )
    assert seen.get("ctx") is ctx, "the tenant context was never restored"


@pytest.mark.asyncio
async def test_an_existing_context_is_left_alone(enabled, monkeypatch):
    """Never overwrite a context that is already correct for this task."""
    monkeypatch.setenv("A2UI_OUTLINE_HEADSTART_CHARS", "10")
    from src.utils.user_context import UserContext

    overwritten = []

    monkeypatch.setattr(
        UserContext, "get_group_context", staticmethod(lambda: object())
    )
    monkeypatch.setattr(
        UserContext, "set_group_context", staticmethod(overwritten.append)
    )

    async def _get_llm(*a, **k):
        raise RuntimeError("stop here")

    import src.services.llm.manager as mgr

    monkeypatch.setattr(mgr.LLMManager, "get_llm", staticmethod(_get_llm))
    await early.plan_outline_early(
        "x" * 500, query="create a presentation on X", group_context=object()
    )
    assert overwritten == []


@pytest.mark.asyncio
async def test_a_skipped_head_start_leaves_a_trace_row(enabled, monkeypatch):
    """Because the a2ui logger reaches no log file on this deployment.

    Two rounds of "why is the outline STILL running after the answer?" had no
    evidence to read anywhere. The trace is queryable and scoped to the run, so
    a head start that does not happen now says why.
    """
    monkeypatch.setenv("A2UI_OUTLINE_HEADSTART_CHARS", "10")
    rows = []

    async def _write_rows(execution_id, entries, **kw):
        rows.append((execution_id, entries))

    import src.services.trace.writer as writer

    monkeypatch.setattr(writer, "write_rows", _write_rows)

    async def _explode(*a, **k):
        raise ValueError("group_id is REQUIRED for get_llm")

    import src.services.llm.manager as mgr

    monkeypatch.setattr(mgr.LLMManager, "get_llm", staticmethod(_explode))

    await early.plan_outline_early(
        "x" * 500, query="create a presentation on X", execution_id="run-1"
    )
    assert rows, "the skip left no trace row"
    exec_id, entries = rows[0]
    assert exec_id == "run-1"
    assert entries[0][0] == "a2ui_outline_skipped"
    assert "group_id is REQUIRED" in entries[0][2]


@pytest.mark.asyncio
async def test_an_ordinary_prose_turn_traces_nothing(enabled, monkeypatch):
    """The row is for wanted-but-missing head starts, not for every chat turn."""
    rows = []

    async def _write_rows(execution_id, entries, **kw):
        rows.append(entries)

    import src.services.trace.writer as writer

    monkeypatch.setattr(writer, "write_rows", _write_rows)
    await early.plan_outline_early(
        "x" * 5000, query="what is the capital of France?", execution_id="run-1"
    )
    assert rows == []
