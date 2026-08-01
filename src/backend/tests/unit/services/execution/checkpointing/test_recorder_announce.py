"""A checkpoint write has to be visible — for crews as well as flows.

The flow path announced its STATE saves; the shared recorder, which is where
BOTH paths write their completed units, announced nothing. So a crew run's
timeline showed memory reads, LLM calls and task completions, and claimed no
checkpointing had happened at all.
"""

from unittest.mock import patch

from src.core.events.types import CheckpointUnitSavedEvent
from src.services.execution.checkpointing.recorder import CheckpointRecorder


class _Recorder(CheckpointRecorder):
    kind = "crew"

    def _subscriptions(self):  # pragma: no cover — not exercised here
        return []


def _emitted(unit, persist_raises=None):
    """Run _persist and return the events it put on the bus."""
    seen = []

    def _capture(_source, event):
        seen.append(event)

    def _run(*_a, **_k):
        if persist_raises:
            raise persist_raises

    with (
        patch(
            "src.services.tools.async_bridge.run_async_with_context", side_effect=_run
        ),
        patch("src.core.events.event_bus.emit", side_effect=_capture),
    ):
        _Recorder(job_id="job-1")._persist(unit)
    return seen


class TestTheWriteIsAnnounced:
    def test_a_successful_unit_write_reaches_the_trace(self):
        events = _emitted({"key": "task-1", "index": 0})

        assert len(events) == 1
        assert isinstance(events[0], CheckpointUnitSavedEvent)
        assert events[0].kind == "crew"
        assert events[0].unit_key == "task-1"
        assert events[0].error is None

    def test_a_FAILED_write_is_announced_rather_than_swallowed(self):
        # A checkpoint failure does not fail the run — which is precisely why it
        # has to be visible, or a run that can never be resumed looks like one
        # that can.
        events = _emitted({"key": "task-1"}, persist_raises=RuntimeError("db is gone"))

        assert len(events) == 1
        assert "db is gone" in (events[0].error or "")

    def test_announcing_never_breaks_the_run(self):
        # Telemetry must not be able to fail the thing it reports on.
        def _run(*_a, **_k):
            return None

        with (
            patch(
                "src.services.tools.async_bridge.run_async_with_context",
                side_effect=_run,
            ),
            patch(
                "src.core.events.event_bus.emit", side_effect=RuntimeError("bus down")
            ),
        ):
            _Recorder(job_id="job-1")._persist({"key": "task-1"})  # must not raise
