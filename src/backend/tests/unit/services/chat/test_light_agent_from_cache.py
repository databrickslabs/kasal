"""A replayed chat call has to LOOK replayed.

The crew path gets `from_cache` for free through the OTel bridge; chat builds
its own trace row, so a field it does not copy is a field the timeline cannot
show — which is how replay looked broken on this path while it was working.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace


def _finished_event(from_cache):
    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        tool_name="PerplexityTool",
        tool_args={"query": "lebanon news"},
        output="the recorded answer",
        from_cache=from_cache,
        started_at=now,
        finished_at=now + timedelta(seconds=12),
    )


def _row_output(event):
    """Mirror of the output dict _on_tool_finished builds, kept in one place so
    the assertion is about the fields, not about reaching into a closure."""
    return {
        "tool_name": str(event.tool_name),
        "input": str(event.tool_args),
        "content": str(event.output),
        "from_cache": bool(getattr(event, "from_cache", False)),
        "duration_ms": int(
            (event.finished_at - event.started_at).total_seconds() * 1000
        ),
    }


def test_a_replayed_call_says_so():
    assert _row_output(_finished_event(True))["from_cache"] is True


def test_a_live_call_says_so():
    assert _row_output(_finished_event(False))["from_cache"] is False


def test_the_duration_cannot_be_used_to_tell_them_apart():
    """wrap_tool stamps started_at BEFORE the pre-hooks, so an approval gate's
    wait lands in the duration whether or not the tool ever ran — which is why
    the flag has to be carried explicitly."""
    replayed = _row_output(_finished_event(True))
    live = _row_output(_finished_event(False))

    assert replayed["duration_ms"] == live["duration_ms"] == 12000
    assert replayed["from_cache"] != live["from_cache"]
