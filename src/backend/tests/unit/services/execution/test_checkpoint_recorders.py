"""Unit tests for the crew and flow checkpoint recorders.

Both are thin adapters over CheckpointRecorder; what is worth testing is what
each considers a UNIT, and that the four base guarantees survive — idempotent
keying, bounded output, fail-open handlers, event-driven registration.
"""

from types import SimpleNamespace

from src.core.events.bus import EventsBus
from src.core.events.types import CrewKickoffCompletedEvent, TaskCompletedEvent
from src.services.agent_builder.checkpoint_adapter import CrewTaskCheckpointRecorder
from src.services.execution.checkpointing.record import KIND_CREW, KIND_FLOW
from src.services.execution.runtime.types import Process, TaskOutput
from src.services.flow_builder.checkpoint_adapter import FlowCrewCheckpointRecorder


def make_task(description: str):
    return SimpleNamespace(
        key=f"key-{description}", name=description, description=description
    )


def make_crew(n_tasks: int = 3):
    return SimpleNamespace(
        tasks=[make_task(f"task-{i}") for i in range(n_tasks)],
        process=Process.sequential,
    )


def make_output(raw: str = "the output", agent: str = "worker"):
    return TaskOutput(
        description="task",
        raw=raw,
        agent=agent,
        summary="a summary",
        json_dict={"k": "v"},
    )


def capture(recorder):
    """Replace persistence with an in-memory list, returning it."""
    persisted = []
    recorder._persist = persisted.append
    return persisted


def capture_clear(recorder):
    cleared = []
    recorder._clear = lambda: cleared.append(True)
    return cleared


class TestCrewRecorder:
    def test_a_unit_is_a_task_keyed_by_position(self):
        crew = make_crew(3)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        persisted = capture(recorder)

        event = TaskCompletedEvent(output=make_output(), task=crew.tasks[1])
        recorder._on_task_completed(crew.tasks[1], event)

        assert len(persisted) == 1
        unit = persisted[0]
        assert unit["key"] == "1"
        # The content-addressed task key rides along so the runtime can refuse
        # a checkpoint whose inputs changed.
        assert unit["identity"] == "key-task-1"
        assert unit["output_raw"] == "the output"
        assert unit["output_json"] == {"k": "v"}
        assert unit["agent"] == "worker"
        assert unit["completed_at"]

    def test_declares_crew_kind_and_task_count(self):
        recorder = CrewTaskCheckpointRecorder("job-1", make_crew(4))
        assert recorder.kind == KIND_CREW
        assert recorder._unit_count == 4
        assert recorder._meta["process"] == "sequential"

    def test_unknown_task_is_ignored(self):
        crew = make_crew(2)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        persisted = capture(recorder)

        stranger = make_task("not-in-crew")
        recorder._on_task_completed(
            stranger, TaskCompletedEvent(output=make_output(), task=stranger)
        )
        assert persisted == []

    def test_handler_is_fail_open(self):
        crew = make_crew(1)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)

        def broken(unit):
            raise RuntimeError("db down")

        recorder._persist = broken
        # Must not raise — a checkpoint failure may never fail the run.
        recorder._on_task_completed(
            crew.tasks[0], TaskCompletedEvent(output=make_output(), task=crew.tasks[0])
        )

    def test_long_output_is_truncated_and_flagged(self):
        crew = make_crew(1)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        unit = recorder._build_unit(0, crew.tasks[0], make_output(raw="x" * 600_000))

        assert len(unit["output_raw"]) == 500_000
        assert unit["truncated"] is True

    def test_short_output_carries_no_truncation_claim(self):
        crew = make_crew(1)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        unit = recorder._build_unit(0, crew.tasks[0], make_output(raw="small"))

        # Absence of the flag must never read as an assertion of fidelity.
        assert "truncated" not in unit

    def test_keeps_the_checkpoint_when_its_own_crew_completes(self):
        """A crew checkpoint is a re-run point, not only crash recovery.

        Clearing it on success made a completed run look like it had no
        checkpoint, so you could not re-run from task 3 after editing task 4.
        """
        crew = make_crew(1)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        cleared = capture_clear(recorder)

        recorder._on_crew_completed(
            crew, CrewKickoffCompletedEvent(crew_name="c", output=None, total_tokens=0)
        )
        assert cleared == []

    def test_another_crews_completion_is_ignored(self):
        crew = make_crew(1)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        cleared = capture_clear(recorder)

        recorder._on_crew_completed(
            make_crew(2),
            CrewKickoffCompletedEvent(crew_name="other", output=None, total_tokens=0),
        )
        assert cleared == []

    def test_register_subscribes_on_the_bus(self):
        crew = make_crew(2)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        persisted = capture(recorder)

        bus = EventsBus()
        assert recorder.register(bus) is recorder  # chainable
        bus.emit(
            crew.tasks[0], TaskCompletedEvent(output=make_output(), task=crew.tasks[0])
        )

        assert [u["key"] for u in persisted] == ["0"]


class TestFlowRecorder:
    def test_a_unit_is_a_crew_sequenced_by_completion_order(self):
        recorder = FlowCrewCheckpointRecorder("job-1", crew_count=3)
        persisted = capture(recorder)

        for name in ("research", "write"):
            recorder._on_crew_completed(
                None,
                CrewKickoffCompletedEvent(
                    crew_name=name,
                    output=SimpleNamespace(raw=f"{name} output", json_dict=None),
                    total_tokens=0,
                ),
            )

        assert [u["key"] for u in persisted] == ["1", "2"]
        assert [u["name"] for u in persisted] == ["research", "write"]
        assert persisted[0]["output_raw"] == "research output"

    def test_declares_flow_kind(self):
        recorder = FlowCrewCheckpointRecorder("job-1", crew_count=2)
        assert recorder.kind == KIND_FLOW

    def test_repeated_crew_does_not_renumber_later_sequences(self):
        recorder = FlowCrewCheckpointRecorder("job-1")
        persisted = capture(recorder)

        for name in ("research", "research", "write"):
            recorder._on_crew_completed(
                None,
                CrewKickoffCompletedEvent(
                    crew_name=name,
                    output=SimpleNamespace(raw="o", json_dict=None),
                    total_tokens=0,
                ),
            )

        assert [u["name"] for u in persisted] == ["research", "write"]
        assert [u["key"] for u in persisted] == ["1", "2"]

    def test_unnamed_crew_is_not_checkpointed(self):
        recorder = FlowCrewCheckpointRecorder("job-1")
        persisted = capture(recorder)

        recorder._on_crew_completed(
            SimpleNamespace(name=None),
            CrewKickoffCompletedEvent(crew_name=None, output=None, total_tokens=0),
        )
        # A unit that cannot be matched back to a crew restores into nothing.
        assert persisted == []

    def test_falls_back_to_the_source_crews_name(self):
        recorder = FlowCrewCheckpointRecorder("job-1")
        persisted = capture(recorder)

        recorder._on_crew_completed(
            SimpleNamespace(name="from-source"),
            CrewKickoffCompletedEvent(
                crew_name=None,
                output=SimpleNamespace(raw="o", json_dict=None),
                total_tokens=0,
            ),
        )
        assert [u["name"] for u in persisted] == ["from-source"]

    def test_records_the_flow_state_reference(self):
        recorder = FlowCrewCheckpointRecorder("job-1", flow_uuid="uuid-9")
        # Flow method state stays in its own table; the checkpoint only points at it.
        assert recorder._meta["flow_state_ref"] == {"flow_uuid": "uuid-9"}

    def test_finish_keeps_the_checkpoint(self):
        """A flow checkpoint is an iteration point, not crash recovery.

        Keeping it after a SUCCESSFUL run is what lets a flow be re-run from
        the middle once a downstream crew changes, reusing the upstream
        results. Clearing it would look like it worked and silently re-run
        everything on the next resume.
        """
        recorder = FlowCrewCheckpointRecorder("job-1")
        cleared = capture_clear(recorder)
        recorder.finish()
        assert cleared == []

    def test_handler_is_fail_open(self):
        recorder = FlowCrewCheckpointRecorder("job-1")

        def broken(unit):
            raise RuntimeError("db down")

        recorder._persist = broken
        recorder._on_crew_completed(
            None,
            CrewKickoffCompletedEvent(
                crew_name="c",
                output=SimpleNamespace(raw="o", json_dict=None),
                total_tokens=0,
            ),
        )
