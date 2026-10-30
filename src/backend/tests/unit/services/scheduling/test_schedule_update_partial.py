"""Editing a schedule changes only what the edit sends.

The Activity editor's PUT used to replace every field, so a flow schedule's
edit either failed (no execution_type: validated as a crew with empty YAML) or,
with YAML present, cleared flow_id/nodes/edges and became a crew schedule. A
field the edit leaves out (the timezone, the flow) now keeps its stored value.
"""

import uuid
from datetime import datetime

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.schedule import Schedule
from src.schemas.schedule import ScheduleUpdate
from src.services.scheduling.scheduler import SchedulerService
from src.utils.user_context import GroupContext

FLOW_ID = uuid.UUID("0d3f8e1a-7c2b-4d5e-8f9a-1b2c3d4e5f60")
GROUP = "team-a"
CTX = GroupContext(group_ids=[GROUP], group_email="u@example.com")


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync: Schedule.__table__.create(sync, checkfirst=True)
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


async def _flow_schedule(session, **extra):
    schedule = Schedule(
        name="Morning flow",
        cron_expression="0 9 * * *",
        execution_type="flow",
        flow_id=FLOW_ID,
        flow_config={"retries": 1},
        agents_yaml={},
        tasks_yaml={},
        group_id=GROUP,
        next_run_at=datetime(2030, 1, 1, 9, 0),
        **extra,
    )
    session.add(schedule)
    await session.commit()
    return schedule


@pytest.mark.asyncio
async def test_editing_a_flow_schedule_keeps_it_a_flow(session):
    schedule = await _flow_schedule(session)
    # What the Activity editor sends for a flow schedule.
    edit = ScheduleUpdate(
        name="Morning flow (later)",
        cron_expression="30 10 * * *",
        execution_type="flow",
        flow_id=FLOW_ID,
        is_active=True,
        timezone="Europe/Berlin",
    )

    saved = await SchedulerService(session).update_schedule_with_group_check(
        schedule.id, edit, CTX
    )

    assert saved.execution_type == "flow"
    assert saved.flow_id == FLOW_ID
    assert saved.flow_config == {"retries": 1}
    assert saved.timezone == "Europe/Berlin"
    # 10:30 Berlin is 08:30 or 09:30 UTC — the new time, on the user's clock.
    assert (saved.next_run_at.hour, saved.next_run_at.minute) in ((8, 30), (9, 30))


@pytest.mark.asyncio
async def test_an_edit_that_leaves_out_the_timezone_keeps_it(session):
    schedule = await _flow_schedule(session, timezone="Asia/Tokyo")
    edit = ScheduleUpdate(
        name="Renamed",
        cron_expression="0 9 * * *",
        execution_type="flow",
        flow_id=FLOW_ID,
    )

    saved = await SchedulerService(session).update_schedule_with_group_check(
        schedule.id, edit, CTX
    )

    assert saved.name == "Renamed"
    assert saved.timezone == "Asia/Tokyo"
    # 09:00 in Tokyo (no daylight saving) is always 00:00 UTC.
    assert (saved.next_run_at.hour, saved.next_run_at.minute) == (0, 0)
