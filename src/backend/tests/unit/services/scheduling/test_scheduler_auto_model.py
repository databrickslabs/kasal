"""A schedule may ask for "auto": each run resolves it, before it is named,
recorded or started."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.execution import CrewConfig
from src.schemas.schedule import ScheduleCreate
from src.services.decisions.model_selection import ModelSelection
from src.services.execution.config import auto_model
from src.services.scheduling.scheduler import SchedulerService

MODULE = "src.services.scheduling.scheduler"
PICK = ModelSelection("databricks-claude-opus-5-5", "selected", 2.0)


def test_a_schedule_accepts_auto_on_purpose():
    schedule = ScheduleCreate(
        name="nightly",
        cron_expression="0 1 * * *",
        agents_yaml={"a": {"role": "R"}},
        tasks_yaml={"t": {"description": "D"}},
        model="auto",
    )
    assert schedule.model == "auto"


@pytest.mark.asyncio
async def test_each_scheduled_run_gets_one_concrete_model():
    schedule = MagicMock(id=1, group_id="g-1", created_by_email="u@example.com")
    session = MagicMock(commit=AsyncMock())
    repo = MagicMock(
        find_by_id=AsyncMock(return_value=schedule),
        update_after_execution=AsyncMock(),
    )
    config = CrewConfig(
        agents_yaml={"a": {"role": "R", "llm": "auto"}},
        tasks_yaml={"t": {"description": "summarize the day"}},
        model="auto",
        execution_type="crew",
    )

    async def smart_session():
        yield session

    with (
        patch(f"{MODULE}.ScheduleRepository", return_value=repo),
        patch(f"{MODULE}.get_smart_db_session", side_effect=lambda: smart_session()),
        patch(f"{MODULE}.ExecutionService") as executions,
        patch(f"{MODULE}.KasalExecutionService"),
        patch.object(
            auto_model, "select_for_workspace", new=AsyncMock(return_value=PICK)
        ) as select,
        patch.object(auto_model, "_write_trace", new=AsyncMock()) as write,
    ):
        executions.return_value.generate_execution_name = AsyncMock(
            return_value={"name": "Nightly"}
        )
        executions.create_run_record = AsyncMock()
        executions.run_crew_execution = AsyncMock()
        await SchedulerService(MagicMock()).run_schedule_job(
            1, config, datetime.now(timezone.utc)
        )
        await asyncio.gather(*auto_model._pending)

    assert select.await_args.args[1].group_ids == ["g-1"]
    named = executions.return_value.generate_execution_name.await_args.args[0]
    assert named.model == PICK.model
    record = executions.create_run_record.await_args.kwargs
    assert record["inputs"]["model"] == PICK.model
    handed = executions.run_crew_execution.await_args.kwargs["config"]
    assert handed.model == handed.agents_yaml["a"]["llm"] == PICK.model
    assert write.await_args.args[1] == record["job_id"]
