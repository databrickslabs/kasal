"""What a crew restores from a checkpoint, and what it insists on re-running.

The rule is a PREFIX: restore tasks from the start while their identities still
match the checkpoint, and stop at the first that does not. Everything from there
re-runs, including tasks whose own text is untouched — a task after an edit has
stale context even when it looks unchanged, and restoring it is the
silent-wrong-answer case.

This used to be all-or-nothing: any mismatch, or any change in the task COUNT,
discarded the whole checkpoint. That was defensible only while a resume replayed
the same frozen snapshot that produced the checkpoint, so nothing could mismatch
except a genuine inconsistency. Now that a resume rebuilds from the current
saved definition, mismatches are the normal case, and "start over" would make
every edit cost a full re-run.
"""

from typing import Any

from src.services.execution.runtime import Task
from src.services.execution.runtime.agent import BaseAgent
from src.services.execution.runtime.crew import Crew
from src.services.execution.runtime.identity import legacy_task_identity, task_identity
from src.services.execution.runtime.types import Process


def make_agent() -> BaseAgent:
    return BaseAgent(role="tester", goal="test", backstory="born in a fixture")


def make_task(description: str) -> Task:
    return Task(
        name=description,
        description=description,
        expected_output="a result",
        agent=make_agent(),
    )


def make_crew(*descriptions: str) -> Crew:
    return Crew(
        name="crew",
        agents=[],
        tasks=[make_task(d) for d in descriptions],
        process=Process.sequential,
    )


def checkpoint_for(crew: Crew, *, identity=task_identity, count=None) -> dict:
    """A checkpoint recording every task of ``crew`` as completed."""
    return {
        "task_count": len(crew.tasks) if count is None else count,
        "completed": [
            {
                "index": i,
                "task_key": identity(task),
                "name": task.name,
                "output_raw": f"output of {task.name}",
            }
            for i, task in enumerate(crew.tasks)
        ],
    }


def restored(crew: Crew, checkpoint: dict) -> list:
    """The indices the crew would restore, in order."""
    seeded = crew._load_checkpoint(checkpoint)
    return sorted(seeded) if seeded else []


class TestThePrefixRule:
    def test_an_untouched_crew_restores_everything(self):
        crew = make_crew("one", "two", "three")
        assert restored(crew, checkpoint_for(crew)) == [0, 1, 2]

    def test_editing_the_last_task_keeps_the_rest(self):
        crew = make_crew("one", "two", "three")
        checkpoint = checkpoint_for(crew)

        crew.tasks[2].description = "three, rewritten"

        assert restored(crew, checkpoint) == [0, 1]

    def test_editing_a_middle_task_re_runs_it_and_everything_after(self):
        crew = make_crew("one", "two", "three", "four")
        checkpoint = checkpoint_for(crew)

        crew.tasks[1].description = "two, rewritten"

        # Task 3 is untouched, and still re-runs: its context is task 2's
        # output, which is about to change.
        assert restored(crew, checkpoint) == [0]

    def test_editing_the_first_task_invalidates_the_whole_checkpoint(self):
        crew = make_crew("one", "two", "three")
        checkpoint = checkpoint_for(crew)

        crew.tasks[0].description = "one, rewritten"

        assert restored(crew, checkpoint) == []

    def test_the_earliest_edit_decides(self):
        crew = make_crew("one", "two", "three", "four")
        checkpoint = checkpoint_for(crew)

        crew.tasks[1].description = "two, rewritten"
        crew.tasks[3].description = "four, rewritten"

        assert restored(crew, checkpoint) == [0]

    def test_expected_output_counts_as_an_edit(self):
        crew = make_crew("one", "two")
        checkpoint = checkpoint_for(crew)

        crew.tasks[1].expected_output = "something else entirely"

        assert restored(crew, checkpoint) == [0]


class TestShapeChanges:
    def test_appending_a_task_keeps_the_existing_prefix(self):
        """The task_count guard used to throw all of this away.

        Adding a fifth task says nothing about whether task 1 is still task 1,
        yet a count mismatch discarded the entire checkpoint.
        """
        crew = make_crew("one", "two", "three")
        checkpoint = checkpoint_for(crew)

        crew.tasks.append(make_task("four"))

        assert restored(crew, checkpoint) == [0, 1, 2]

    def test_inserting_a_task_re_runs_from_the_insertion_point(self):
        crew = make_crew("one", "two", "three")
        checkpoint = checkpoint_for(crew)

        crew.tasks.insert(1, make_task("one and a half"))

        # Position 1 now holds different work, so the prefix stops at 0.
        assert restored(crew, checkpoint) == [0]

    def test_removing_a_task_restores_what_still_lines_up(self):
        crew = make_crew("one", "two", "three")
        checkpoint = checkpoint_for(crew)

        crew.tasks.pop()  # 'three' is gone

        assert restored(crew, checkpoint) == [0, 1]

    def test_a_checkpoint_longer_than_the_crew_does_not_overrun(self):
        crew = make_crew("one", "two", "three")
        checkpoint = checkpoint_for(crew)
        crew.tasks = crew.tasks[:1]

        assert restored(crew, checkpoint) == [0]


class TestCompatibility:
    def test_a_legacy_identity_still_matches(self):
        """Checkpoints written before tools and model joined the hash.

        Rejecting these would invalidate every existing checkpoint the moment
        the wider identity shipped — for exactly the workflow checkpoints are
        for.
        """
        crew = make_crew("one", "two")
        checkpoint = checkpoint_for(crew, identity=legacy_task_identity)

        assert restored(crew, checkpoint) == [0, 1]

    def test_an_entry_with_no_identity_is_accepted(self):
        crew = make_crew("one", "two")
        checkpoint = checkpoint_for(crew, identity=lambda task: None)

        assert restored(crew, checkpoint) == [0, 1]

    def test_a_gap_stops_the_prefix(self):
        crew = make_crew("one", "two", "three")
        checkpoint = checkpoint_for(crew)
        checkpoint["completed"] = [
            e for e in checkpoint["completed"] if e["index"] != 1
        ]

        # Task 2 never completed, so task 3 has no context to be restored with.
        assert restored(crew, checkpoint) == [0]

    def test_a_malformed_entry_costs_only_what_follows_it(self):
        crew = make_crew("one", "two", "three")
        checkpoint = checkpoint_for(crew)
        checkpoint["completed"][1].pop("index")

        assert restored(crew, checkpoint) == [0]

    def test_nothing_restorable_returns_none(self):
        crew = make_crew("one")
        assert crew._load_checkpoint({"completed": []}) is None
        assert crew._load_checkpoint(None) is None

    def test_a_non_sequential_crew_never_restores(self):
        """ "Everything before task N" is not well defined off the sequential
        process, so the whole mechanism declines rather than guessing."""
        crew = make_crew("one", "two")
        checkpoint = checkpoint_for(crew)
        crew.process = Process.hierarchical

        assert crew._load_checkpoint(checkpoint) is None


class TestRestoredContent:
    def test_a_restored_task_carries_its_stored_output(self):
        crew = make_crew("one", "two")
        seeded: Any = crew._load_checkpoint(checkpoint_for(crew))

        assert seeded[0].raw == "output of one"
        # Description comes from the CURRENT task, not the checkpoint: the two
        # match by identity, so either is correct, and the current one is the
        # thing downstream tasks will actually see.
        assert seeded[0].description == crew.tasks[0].description
