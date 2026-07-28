"""Unit tests for the crew task checkpoint recorder and resume payload builder."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.core.events.bus import EventsBus
from src.core.events.types import CrewKickoffCompletedEvent, TaskCompletedEvent
from src.services.execution.checkpoint import (
    CrewTaskCheckpointRecorder,
    build_resume_checkpoint,
)
from src.services.execution.runtime.types import Process, TaskOutput


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


class TestRecorderTaskCompleted:
    def test_persists_entry_with_task_index(self):
        crew = make_crew(3)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        persisted = []

        async def fake_persist(entry):
            persisted.append(entry)

        recorder._persist_entry = fake_persist
        event = TaskCompletedEvent(output=make_output(), task=crew.tasks[1])
        recorder._on_task_completed(crew.tasks[1], event)

        assert len(persisted) == 1
        entry = persisted[0]
        assert entry["index"] == 1
        assert entry["task_key"] == "key-task-1"
        assert entry["output_raw"] == "the output"
        assert entry["output_json"] == {"k": "v"}
        assert entry["agent"] == "worker"
        assert entry["completed_at"]

    def test_unknown_task_is_ignored(self):
        crew = make_crew(2)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        persisted = []

        async def fake_persist(entry):
            persisted.append(entry)

        recorder._persist_entry = fake_persist
        stranger = make_task("not-in-crew")
        event = TaskCompletedEvent(output=make_output(), task=stranger)
        recorder._on_task_completed(stranger, event)
        assert persisted == []

    def test_persistence_failure_is_swallowed(self):
        crew = make_crew(1)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)

        async def broken_persist(entry):
            raise RuntimeError("db down")

        recorder._persist_entry = broken_persist
        event = TaskCompletedEvent(output=make_output(), task=crew.tasks[0])
        # must not raise — checkpointing is fail-open
        recorder._on_task_completed(crew.tasks[0], event)

    def test_long_output_is_truncated_and_flagged(self):
        crew = make_crew(1)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        big = "x" * 600_000
        entry = recorder._build_entry(0, crew.tasks[0], make_output(raw=big))
        assert len(entry["output_raw"]) == 500_000
        assert entry["truncated"] is True


class TestRecorderCrewCompleted:
    def test_clears_checkpoint_on_success(self):
        crew = make_crew(1)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        cleared = []

        async def fake_clear():
            cleared.append(True)

        recorder._clear_checkpoint = fake_clear
        event = CrewKickoffCompletedEvent(crew_name="c", output=None, total_tokens=0)
        recorder._on_crew_completed(crew, event)
        assert cleared == [True]

    def test_other_crew_completion_is_ignored(self):
        crew = make_crew(1)
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        cleared = []

        async def fake_clear():
            cleared.append(True)

        recorder._clear_checkpoint = fake_clear
        event = CrewKickoffCompletedEvent(
            crew_name="other", output=None, total_tokens=0
        )
        recorder._on_crew_completed(make_crew(2), event)
        assert cleared == []


class TestRecorderRegistration:
    def test_register_hooks_both_events(self):
        crew = make_crew(2)
        bus = EventsBus()
        recorder = CrewTaskCheckpointRecorder("job-1", crew)
        persisted = []

        async def fake_persist(entry):
            persisted.append(entry)

        recorder._persist_entry = fake_persist
        recorder.register(bus)

        bus.emit(
            crew.tasks[0], TaskCompletedEvent(output=make_output(), task=crew.tasks[0])
        )
        assert [e["index"] for e in persisted] == [0]


class TestBuildResumeCheckpoint:
    def test_orders_entries_by_index(self):
        stored = {
            "version": 1,
            "task_count": 3,
            "process": "sequential",
            "completed": {
                "2": {"index": 2, "output_raw": "c"},
                "0": {"index": 0, "output_raw": "a"},
                "10": {"index": 10, "output_raw": "z"},
                "1": {"index": 1, "output_raw": "b"},
            },
        }
        result = build_resume_checkpoint(stored)
        assert [e["index"] for e in result["completed"]] == [0, 1, 2, 10]
        assert result["task_count"] == 3
        assert result["process"] == "sequential"

    def test_empty_or_missing_returns_none(self):
        assert build_resume_checkpoint(None) is None
        assert build_resume_checkpoint({}) is None
        assert build_resume_checkpoint({"completed": {}}) is None
        assert build_resume_checkpoint({"completed": [1, 2]}) is None

    def test_non_numeric_index_returns_none(self):
        assert build_resume_checkpoint({"completed": {"a": {"index": "a"}}}) is None
