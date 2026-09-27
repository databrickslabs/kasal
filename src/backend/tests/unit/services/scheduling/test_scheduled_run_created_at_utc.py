"""A scheduled run's created_at is naive UTC, like every other run's.

It used to be converted to the server's LOCAL time, so a scheduled run's start
drifted from its UTC completed_at by the machine's offset.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.execution import CrewConfig
from src.services.scheduling.scheduler import SchedulerService

MODULE = "src.services.scheduling.scheduler"


@pytest.mark.asyncio
async def test_scheduled_run_is_recorded_in_utc():
    session = MagicMock(commit=AsyncMock())
    repo = MagicMock(
        find_by_id=AsyncMock(
            return_value=MagicMock(id=1, group_id="g", created_by_email="u@x.io")
        ),
        update_after_execution=AsyncMock(),
    )
    config = CrewConfig(
        agents_yaml={"a": {"role": "R"}},
        tasks_yaml={"t": {"description": "D"}},
        execution_type="crew",
    )

    async def smart_session():
        yield session

    # A tz-aware time off UTC: the row must hold its UTC wall clock.
    fired = datetime(2026, 9, 27, 14, 0, tzinfo=timezone(timedelta(hours=2)))
    with (
        patch(f"{MODULE}.ScheduleRepository", return_value=repo),
        patch(f"{MODULE}.get_smart_db_session", side_effect=lambda: smart_session()),
        patch(f"{MODULE}.ExecutionService") as executions,
        patch(f"{MODULE}.KasalExecutionService"),
    ):
        executions.return_value.generate_execution_name = AsyncMock(
            return_value={"name": "Nightly"}
        )
        executions.create_run_record = AsyncMock()
        executions.run_crew_execution = AsyncMock()
        await SchedulerService(MagicMock()).run_schedule_job(1, config, fired)

    created = executions.create_run_record.await_args.kwargs["created_at"]
    assert created == datetime(2026, 9, 27, 12, 0)
    assert created.tzinfo is None
