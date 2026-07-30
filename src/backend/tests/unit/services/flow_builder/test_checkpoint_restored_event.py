"""Unit tests for the restored-crew trace event.

A crew skipped on resume used to emit nothing, so a resumed run's timeline
showed only the part that re-ran and read as a partial job. It now emits its
own event — not a synthetic completion — so the trace is complete without
claiming the crew executed.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.core.events.types import (
    CrewCheckpointRestoredEvent,
    TaskCheckpointRestoredEvent,
)
from src.services.flow_builder.modules.flow_methods import _emit_checkpoint_restored


class TestCrewCheckpointRestoredEvent:
    def test_it_is_not_a_completion_event(self):
        """Its own type, so nothing downstream mistakes it for a run.

        A synthetic CrewKickoffCompletedEvent would also reach the checkpoint
        recorder, which would re-record the crew from a stub that has no tasks
        and overwrite a verified identity with an unverifiable one.
        """
        event = CrewCheckpointRestoredEvent(crew_name="research", output="found it")

        assert event.type == "crew_checkpoint_restored"
        assert event.type != "crew_kickoff_completed"
        assert event.output == "found it"

    @pytest.mark.parametrize(
        "event_name,event_type",
        [
            ("CrewCheckpointRestoredEvent", "crew_checkpoint_restored"),
            ("TaskCheckpointRestoredEvent", "task_checkpoint_restored"),
        ],
        ids=["flow-restores-a-crew", "crew-restores-a-task"],
    )
    def test_the_bridge_maps_and_subscribes_it(self, event_name, event_type):
        """A mapping without a subscription writes nothing, silently."""
        from src.services.otel_tracing.db_exporter import SPAN_NAME_MAP
        from src.services.otel_tracing.event_bridge import (
            _EVENT_CLASSES,
            _EVENT_SPAN_MAP,
        )

        span_name, mapped = _EVENT_SPAN_MAP[event_name]
        assert mapped == event_type
        assert ("src.core.events", event_name) in _EVENT_CLASSES
        # ...and the exporter must resolve the span back to the same type.
        assert SPAN_NAME_MAP[span_name] == event_type


class TestEmitCheckpointRestored:
    def test_emits_for_the_restored_crew(self):
        bus = MagicMock()
        with patch("src.core.events.event_bus", bus):
            _emit_checkpoint_restored("research", "found it")

        bus.emit.assert_called_once()
        event = bus.emit.call_args.args[1]
        assert isinstance(event, CrewCheckpointRestoredEvent)
        assert event.crew_name == "research"
        assert event.output == "found it"

    def test_a_failure_never_breaks_the_run(self):
        bus = MagicMock()
        bus.emit.side_effect = RuntimeError("bus down")
        with patch("src.core.events.event_bus", bus):
            # A trace row is not worth failing a resume for.
            _emit_checkpoint_restored("research", "found it")

    def test_a_restored_crew_with_no_output_still_appears(self):
        """Otherwise the gap in the timeline is exactly what we set out to fix."""
        bus = MagicMock()
        with patch("src.core.events.event_bus", bus):
            _emit_checkpoint_restored("research", None)

        bus.emit.assert_called_once()
        assert bus.emit.call_args.args[1].crew_name == "research"


class TestCrewRestoresTasks:
    """The crew path restores TASKS, and skipping them used to be silent.

    That was correct while a resume reused the original run's record. It is not
    now: a resume creates a NEW execution with its own trace, so the restored
    prefix left no mark and the run read as if it had started midway.
    """

    def test_it_is_not_a_completion_event(self):
        event = TaskCheckpointRestoredEvent(output="restored output")

        assert event.type == "task_checkpoint_restored"
        assert event.type != "task_completed"

    def test_a_restored_task_emits_exactly_one_event(self):
        """One event, not a started/completed pair — the task did not run."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock, patch

        from src.services.execution.runtime.crew import Crew

        emitted = []
        bus = MagicMock()
        bus.emit.side_effect = lambda source, event: emitted.append(event)

        crew = MagicMock(spec=Crew)
        crew._seeded_outputs = {0: SimpleNamespace(raw="restored output")}
        crew.tasks = [SimpleNamespace(description="t0", output=None)]

        with patch("src.services.execution.runtime.crew.event_bus", bus):
            Crew._run_sequential(crew)

        restored = [e for e in emitted if isinstance(e, TaskCheckpointRestoredEvent)]
        assert len(restored) == 1
        assert restored[0].output.raw == "restored output"
        # And the task carries the restored output for downstream context.
        assert crew.tasks[0].output.raw == "restored output"
