"""Recall reports what it SELECTED, not only what the store returned.

The store's pool for "gather latest lebanese news" held a Lebanon record at
0.82 and an AI-news record at 0.63; the relevance cliff cut the second and the
prompt never saw it — but the only trace was the pool, so the Run memory pane
showed the turn recalling AI-news concepts."""

from unittest.mock import MagicMock

import pytest

from src.core.events import MemoryQueryCompletedEvent
from src.core.events.bus import event_bus
from src.services.memory.engine import MemoryRecord
from src.services.memory.run.recall import build_memory_preamble


def _record(rid, content, similarity):
    return MemoryRecord(
        id=rid, content=content, scope="/g", metadata={"similarity": similarity}
    )


@pytest.fixture
def reports():
    seen = []

    @event_bus.on(MemoryQueryCompletedEvent)
    def _capture(source, event):
        seen.append((source, event))

    try:
        yield seen
    finally:
        event_bus.off(MemoryQueryCompletedEvent, _capture)


def test_the_selected_report_carries_only_what_survived_the_cliff(reports):
    memory = MagicMock()
    memory.root_scope = "/g"
    lebanon = _record(
        "lebanon", "Israeli strikes kill three in southern Lebanon.", 0.82
    )
    ai_news = _record("ai-news", "Nvidia in talks to acquire Hugging Face.", 0.63)
    memory.recall.return_value = [lebanon, ai_news]

    block = build_memory_preamble(memory, "gather latest lebanese news", limit=6)

    assert "Israeli strikes" in block and "Nvidia" not in block
    selected = [e for s, e in reports if s is memory and e.stage == "selected"]
    assert len(selected) == 1
    assert [r.id for r in selected[0].results] == ["lebanon"]
    assert selected[0].query == "gather latest lebanese news"


def test_an_empty_selection_is_reported_too(reports):
    memory = MagicMock()
    memory.root_scope = "/g"
    memory.recall.return_value = []
    assert build_memory_preamble(memory, "anything", limit=6) == ""
    selected = [e for s, e in reports if s is memory and e.stage == "selected"]
    assert len(selected) == 1 and selected[0].results == []
