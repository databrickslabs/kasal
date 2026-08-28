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

# A dashboard with several components, so the stream has real batches to split.
# (These wiring tests used a deck before presentations left A2UI for the chat
# HTML path; the streaming mechanics under test are surface-agnostic.)
DECK = {
    "surfaceKind": "dashboard",
    "root": "grid",
    "components": [
        {"id": "grid", "component": "Grid", "columns": 2, "children": ["k1", "md1"]},
        {"id": "k1", "component": "KeyValue", "label": "Chalets", "value": "42"},
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
        "Grid": {"summary": "grid"},
        "KeyValue": {"summary": "kv"},
        "Markdown": {"summary": "md"},
    },
    "surfaceKinds": ["dashboard", "conversation"],
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
    """A2UI on, a catalog the surface validates against, and a fake composer LLM."""
    llm = FakeStreamingLLM(json.dumps(DECK))

    async def _resolve_config(group_id, query):
        return True, CATALOG, ""

    async def _get_llm(*a, **k):
        return llm

    import src.services.llm.manager as mgr

    monkeypatch.setattr(runner, "_resolve_config", _resolve_config)
    monkeypatch.setattr(mgr.LLMManager, "get_llm", staticmethod(_get_llm))
    return llm


async def _compose(**kw):
    sent: List[Dict[str, Any]] = []

    async def on_delta(msg):
        sent.append(msg)

    surface = await runner.compose_surface(
        "Some prose about swiss chalets in the alps.",
        query="build a dashboard of nice swiss houses in the alps",
        on_delta=on_delta,
        **kw,
    )
    return surface, sent


@pytest.mark.asyncio
async def test_composer_tokens_reach_the_delta_sink(wired):
    surface, sent = await _compose()

    assert surface == DECK
    assert len(sent) > 1, (
        "the surface composed but streamed nothing — the chunk handler never "
        "reached the sink"
    )
    assert apply_messages(sent) == DECK


@pytest.mark.asyncio
async def test_the_llm_is_opted_into_streaming(wired):
    await _compose()
    assert wired.stream is True, "compose_surface must switch the composer to streamed"


@pytest.mark.asyncio
async def test_components_ship_before_the_surface_is_returned(wired, monkeypatch):
    """The point: components arrive as they are written, not in one lump.

    The rescan throttle has to come off — the fake emits a whole surface in
    microseconds, so at the production cadence every chunk lands inside one
    window and the result would be a single batch no matter how well this works.
    """
    monkeypatch.setenv("A2UI_STREAM_INTERVAL_MS", "0")
    _, sent = await _compose()
    batches = [m for m in sent if "updateComponents" in m]
    assert len(batches) >= 2, "everything arrived in a single batch"
    # ...and still exactly the surface, however it was split up.
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
    """A prose turn must not strand the instant shell frame on screen."""

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


