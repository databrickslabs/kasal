"""Telling an operator what a resume will actually do, before they press it.

The run decides per unit which stored output survives. Without this the dialog
listed every completed unit identically and said nothing about the three that
were about to be redone, so the new behaviour would only ever be discovered
after the fact.

What can be answered here is narrower than what the run decides, and the
narrowness is the interesting part. ``identity`` hashes built tool objects and a
resolved LLM, which do not exist outside a run; only the TEXT half
(``content_key``) can be recomputed from a saved definition. So this reports a
FLOOR — "at least these will re-run" — and says "unknown" rather than "yes"
whenever it has no basis.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.checkpointing.service import CheckpointService
from src.services.execution.runtime.identity import content_key

TASKS = [("Research", "find things"), ("Write", "write it up"), ("Review", "check it")]


def stored_units():
    return {
        str(i): {
            "key": str(i),
            "name": name,
            "output_raw": f"output of {name}",
            "content_key": content_key(description, "a result"),
        }
        for i, (name, description) in enumerate(TASKS)
    }


def current_definition(**edits):
    """The saved crew's tasks_yaml, with optional per-task description edits."""
    return {
        name: {
            "description": edits.get(name, description),
            "expected_output": "a result",
        }
        for name, description in TASKS
    }


async def summarise(units, projection, *, crew_id="crew-1", kind="crew"):
    service = CheckpointService(MagicMock())
    execution = SimpleNamespace(
        id=1,
        job_id="job-1",
        status="FAILED",
        checkpoint_status="active",
        execution_type=kind,
        run_name="run",
        created_at=None,
        crew_id=crew_id,
    )
    service.repository.get_execution_by_job_id = AsyncMock(return_value=execution)

    record = {"version": 1, "kind": "crew", "unit_count": 3, "units": units}
    with (
        patch(
            "src.services.execution.checkpointing.store.read_record",
            new_callable=AsyncMock,
            return_value=record,
        ),
        patch(
            "src.services.catalog.crew_config.build_crew_execution_config_by_id",
            new_callable=AsyncMock,
            return_value=projection,
        ),
    ):
        return await service.get_checkpoint("job-1")


class TestTheComputedResumePoint:
    @pytest.mark.asyncio
    async def test_an_untouched_crew_reports_nothing_changed(self):
        result = await summarise(stored_units(), ({}, current_definition()))

        assert result["changed_from_index"] is None
        assert result["restorable_count"] == 3
        assert [u["will_restore"] for u in result["units"]] == [True, True, True]

    @pytest.mark.asyncio
    async def test_an_edit_reports_itself_and_everything_after(self):
        result = await summarise(
            stored_units(), ({}, current_definition(Write="write it up BETTER"))
        )

        assert result["changed_from_index"] == 1
        assert result["restorable_count"] == 1
        # 'Review' is untouched and still re-runs: its input is about to change.
        assert [u["will_restore"] for u in result["units"]] == [True, False, False]

    @pytest.mark.asyncio
    async def test_editing_the_first_task_saves_nothing(self):
        result = await summarise(
            stored_units(), ({}, current_definition(Research="find OTHER things"))
        )

        assert result["changed_from_index"] == 0
        assert result["restorable_count"] == 0

    @pytest.mark.asyncio
    async def test_the_earliest_edit_decides(self):
        result = await summarise(
            stored_units(),
            (
                {},
                current_definition(Write="rewritten", Review="also rewritten"),
            ),
        )
        assert result["changed_from_index"] == 1

    @pytest.mark.asyncio
    async def test_a_removed_task_counts_as_a_change(self):
        definition = current_definition()
        del definition["Write"]

        result = await summarise(stored_units(), ({}, definition))

        # The unit no longer describes work that will be done at all.
        assert result["changed_from_index"] == 1


class TestWhatItRefusesToClaim:
    @pytest.mark.asyncio
    async def test_a_run_with_no_saved_crew_reports_unknown(self):
        result = await summarise(stored_units(), None, crew_id=None)

        # Not False, and not True: there is nothing to compare against, and
        # rendering "will be reused" would promise what run time may refuse.
        assert result["changed_from_index"] is None
        assert all(u["will_restore"] is None for u in result["units"])

    @pytest.mark.asyncio
    async def test_a_deleted_crew_reports_unknown(self):
        result = await summarise(stored_units(), None)
        assert all(u["will_restore"] is None for u in result["units"])

    @pytest.mark.asyncio
    async def test_a_failing_read_reports_unknown_rather_than_erroring(self):
        service = CheckpointService(MagicMock())
        with patch(
            "src.services.catalog.crew_config.build_crew_execution_config_by_id",
            new_callable=AsyncMock,
            side_effect=RuntimeError("db down"),
        ):
            keys = await service._current_content_keys(
                SimpleNamespace(crew_id="crew-1", execution_type="crew"), None
            )
        assert keys is None

    @pytest.mark.asyncio
    async def test_a_unit_written_before_content_keys_is_skipped(self):
        units = stored_units()
        units["0"].pop("content_key")

        result = await summarise(
            units, ({}, current_definition(Research="find OTHER things"))
        )

        # Task 0 changed, but its unit predates content keys so it cannot be
        # judged — and an unjudgeable unit must not read as a detected change.
        assert result["changed_from_index"] is None

    @pytest.mark.asyncio
    async def test_a_flow_without_a_config_reports_unknown(self):
        result = await summarise(stored_units(), ({}, {}), kind="flow")
        assert all(u["will_restore"] is None for u in result["units"])


FLOW_CREWS = [("black", "t-black", "write black"), ("white", "t-white", "write white")]


def flow_units():
    """Flow units are keyed by COMPLETION order and carry the crew name."""
    from types import SimpleNamespace

    from src.services.execution.runtime.identity import crew_content_key

    return {
        str(i + 1): {
            "key": str(i + 1),
            "name": name,
            "content_key": crew_content_key(
                name, [SimpleNamespace(key=content_key(desc, "a result"))]
            ),
        }
        for i, (name, _tid, desc) in enumerate(FLOW_CREWS)
    }


def flow_config():
    return {
        "startingPoints": [{"crewName": "black", "taskId": "t-black"}],
        "listeners": [{"name": "white", "tasks": [{"id": "t-white"}]}],
    }


async def summarise_flow(units, task_rows):
    from types import SimpleNamespace as N

    service = CheckpointService(MagicMock())
    execution = N(
        id=1,
        job_id="job-1",
        status="FAILED",
        checkpoint_status="active",
        execution_type="flow",
        run_name="run",
        created_at=None,
        crew_id=None,
        inputs={"flow_config": flow_config()},
    )
    service.repository.get_execution_by_job_id = AsyncMock(return_value=execution)
    task_service = AsyncMock()
    task_service.get = AsyncMock(side_effect=lambda i: task_rows.get(str(i)))
    task_service.get_with_group_check = AsyncMock(
        side_effect=lambda i, _c: task_rows.get(str(i))
    )

    record = {"version": 1, "kind": "flow", "unit_count": 2, "units": units}
    with (
        patch(
            "src.services.execution.checkpointing.store.read_record",
            new_callable=AsyncMock,
            return_value=record,
        ),
        patch("src.services.catalog.tasks.TaskService", return_value=task_service),
    ):
        return await service.get_checkpoint("job-1")


def flow_task_rows(**edits):
    from types import SimpleNamespace as N

    return {
        tid: N(description=edits.get(name, desc), expected_output="a result")
        for name, tid, desc in FLOW_CREWS
    }


class TestFlowPreview:
    @pytest.mark.asyncio
    async def test_an_untouched_flow_reports_nothing_changed(self):
        result = await summarise_flow(flow_units(), flow_task_rows())
        assert result["changed_from_index"] is None
        assert [u["will_restore"] for u in result["units"]] == [True, True]

    @pytest.mark.asyncio
    async def test_an_edited_crew_is_reported(self):
        result = await summarise_flow(
            flow_units(), flow_task_rows(white="write white DIFFERENTLY")
        )
        # Units are keyed by completion order, and matched on the crew NAME
        # they carry — a flow's unit 2 is not the second crew declared.
        assert result["changed_from_index"] == 1
        assert [u["will_restore"] for u in result["units"]] == [True, False]

    @pytest.mark.asyncio
    async def test_a_crew_whose_task_is_gone_is_not_judged(self):
        rows = flow_task_rows()
        del rows["t-white"]
        result = await summarise_flow(flow_units(), rows)
        # Cannot rebuild that crew's key -> no verdict, rather than a wrong one.
        assert result["changed_from_index"] is None


class TestTheContentKeyContract:
    def test_content_key_matches_task_key(self):
        """The whole mechanism rests on these being the same function.

        One is computed inside a run from a built Task, the other outside it
        from stored strings. If they drift, every unit reads as changed.
        """
        from src.services.execution.runtime import Task

        task = Task(description="do the thing", expected_output="a result")
        assert content_key("do the thing", "a result") == task.key
