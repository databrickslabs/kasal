"""``compose_surface`` end to end: do composer tokens actually reach the sink?

Every other streaming test exercises a piece — the parser, the reducer, the head
starts. None of them proved the WIRING: that opting the composer LLM into
streamed completions, catching its chunk events off the worker thread and
hopping them back to the loop produces deltas at the other end. That gap let a
run ship the instant shell and then stream nothing at all for the deck, which
looked exactly like "composition is slow" in the logs.
"""

import asyncio
import json
from typing import Any, Dict, List

import pytest

from src.core.events import LLMStreamChunkEvent
from src.core.events.bus import event_bus
from src.services.a2ui import runner
from src.services.a2ui.stream import apply_messages

# A deck that survives the composer's own correction passes: every body slide
# carries a real body, or `presentation_needs_body` sends it round the retry loop
# and a fake LLM that keeps returning the same thing ends at the markdown
# fallback — which looks like a streaming failure but is a fixture problem.
DECK = {
    "surfaceKind": "presentation",
    "root": "deck",
    "components": [
        {"id": "deck", "component": "SlideDeck", "children": ["s1", "s2"]},
        {"id": "s1", "component": "Slide", "variant": "title", "title": "Alps"},
        {
            "id": "s2",
            "component": "Slide",
            "variant": "content",
            "title": "Chalets",
            "children": ["md1"],
        },
        {
            "id": "md1",
            "component": "Markdown",
            "content": "- Timber frames\n- Deep eaves\n- Stone footings",
        },
    ],
    "dataModel": {},
}

CATALOG = {
    "components": {
        "SlideDeck": {"summary": "deck"},
        "Slide": {"summary": "slide"},
        "Markdown": {"summary": "md"},
    },
    "surfaceKinds": ["presentation", "conversation"],
}


class FakeStreamingLLM:
    """Emits chunk events the way the real transport does, then returns the whole."""

    def __init__(self, payload: str, chunk: int = 24):
        self.payload = payload
        self.chunk = chunk
        self.model = "fake-model"
        self.stream = False
        self.calls = 0

    def call(self, messages):
        self.calls += 1
        # The composer runs in a worker thread, so these fire OFF the loop —
        # which is the whole reason the bridge exists.
        for i in range(0, len(self.payload), self.chunk):
            event_bus.emit(
                self,
                LLMStreamChunkEvent(
                    model=self.model,
                    chunk=self.payload[i : i + self.chunk],
                    chunk_index=i,
                ),
            )
        return self.payload


@pytest.fixture
def wired(monkeypatch):
    """A2UI on, a catalog the deck validates against, and a fake composer LLM."""
    llm = FakeStreamingLLM(json.dumps(DECK))

    async def _resolve_config(group_id, query):
        return True, CATALOG, ""

    async def _get_llm(*a, **k):
        return llm

    import src.services.llm.manager as mgr

    monkeypatch.setattr(runner, "_resolve_config", _resolve_config)
    monkeypatch.setattr(mgr.LLMManager, "get_llm", staticmethod(_get_llm))
    # The outline pre-pass would consume the same fake LLM and get a deck back
    # instead of a plan; off here so this test is about the wiring only.
    monkeypatch.setenv("A2UI_PRESENTATION_OUTLINE", "0")
    return llm


async def _compose(**kw):
    sent: List[Dict[str, Any]] = []

    async def on_delta(msg):
        sent.append(msg)

    surface = await runner.compose_surface(
        "Some prose about swiss chalets in the alps.",
        query="create a presentation of nice swiss houses in the alps",
        on_delta=on_delta,
        **kw,
    )
    return surface, sent


@pytest.mark.asyncio
async def test_composer_tokens_reach_the_delta_sink(wired):
    surface, sent = await _compose()

    assert surface == DECK
    assert len(sent) > 1, (
        "the deck composed but streamed nothing — the chunk handler never "
        "reached the sink"
    )
    assert apply_messages(sent) == DECK


@pytest.mark.asyncio
async def test_the_llm_is_opted_into_streaming(wired):
    await _compose()
    assert wired.stream is True, "compose_surface must switch the composer to streamed"


@pytest.mark.asyncio
async def test_slides_ship_before_the_surface_is_returned(wired, monkeypatch):
    """The point: components arrive as they are written, not in one lump.

    The rescan throttle has to come off — the fake emits a whole deck in
    microseconds, so at the production cadence every chunk lands inside one
    window and the result would be a single batch no matter how well this works.
    """
    monkeypatch.setenv("A2UI_STREAM_INTERVAL_MS", "0")
    _, sent = await _compose()
    batches = [m for m in sent if "updateComponents" in m]
    assert len(batches) >= 2, "everything arrived in a single batch"
    # ...and still exactly the deck, however it was split up.
    assert apply_messages(sent) == DECK


@pytest.mark.asyncio
async def test_the_handler_is_removed_afterwards(wired):
    before = len(event_bus._handlers.get(LLMStreamChunkEvent, []))
    await _compose()
    after = len(event_bus._handlers.get(LLMStreamChunkEvent, []))
    assert after == before, "a bridge leaked onto the global bus"


@pytest.mark.asyncio
async def test_streaming_can_be_switched_off(wired, monkeypatch):
    monkeypatch.setenv("A2UI_STREAMING", "false")
    surface, sent = await _compose()
    assert surface == DECK  # the answer is unaffected
    assert sent == []


@pytest.mark.asyncio
async def test_a_shell_is_retracted_when_no_surface_is_delivered(wired, monkeypatch):
    """A prose turn must not strand the instant deck frame on screen."""

    async def _off(group_id, query):
        return False, {}, ""

    monkeypatch.setattr(runner, "_resolve_config", _off)
    surface, sent = await _compose(shell_shipped=True)
    assert surface is None
    assert len(sent) == 1 and "deleteSurface" in sent[0]


@pytest.mark.asyncio
async def test_no_retraction_when_no_shell_was_shipped(wired, monkeypatch):
    async def _off(group_id, query):
        return False, {}, ""

    monkeypatch.setattr(runner, "_resolve_config", _off)
    _, sent = await _compose(shell_shipped=False)
    assert sent == []


@pytest.mark.asyncio
async def test_a_precomputed_outline_skips_the_pre_pass(wired, monkeypatch):
    """The head start must REPLACE the outline call, never add to it."""
    monkeypatch.setenv("A2UI_PRESENTATION_OUTLINE", "1")
    plan = [
        {"title": "Alps", "variant": "title", "visual": "none", "focus": ""},
        {"title": "Chalets", "variant": "content", "visual": "none", "focus": ""},
        {"title": "Takeaways", "variant": "content", "visual": "none", "focus": ""},
    ]
    planned = {"n": 0}

    def _spy(*a, **k):
        planned["n"] += 1
        return plan

    import src.services.a2ui.compose as compose_mod

    monkeypatch.setattr(compose_mod, "plan_presentation_outline", _spy)
    await _compose(outline=plan)
    assert planned["n"] == 0, "the outline pre-pass ran again despite a head start"
