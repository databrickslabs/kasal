"""The subprocess live-view lane forwards an AGENT's typing, nothing else.

Memory's recall planner, memory labelling and LLM guardrails stream on the same
LLM object as the agent; their deltas name no agent and used to scroll through
the run view as if an agent had written them.
"""

import queue
from types import SimpleNamespace

from src.services.execution.event_pipe import EventPipeWriter


def _writer():
    q: "queue.Queue[dict]" = queue.Queue()
    return EventPipeWriter(q, "exec-1"), q


def _delivered(writer: EventPipeWriter, q: "queue.Queue[dict]") -> str:
    """What reached the lane: the first delta flushes immediately (the 50 ms
    window starts at zero), later ones sit in the buffer until the next flush."""
    flushed = "" if q.empty() else str(q.get_nowait())
    return flushed + "".join(writer._chunk_buf)


class TestChunkAttributionFilter:
    def test_an_agents_delta_is_forwarded(self):
        w, q = _writer()
        w._on_chunk(object(), SimpleNamespace(chunk="Hello", from_agent=object()))
        assert "Hello" in _delivered(w, q)

    def test_an_unattributed_delta_is_dropped(self):
        w, q = _writer()
        w._on_chunk(object(), SimpleNamespace(chunk='{"query": "…"}', from_agent=None))
        w._on_chunk(object(), SimpleNamespace(chunk="no attribute at all"))
        assert _delivered(w, q) == ""
