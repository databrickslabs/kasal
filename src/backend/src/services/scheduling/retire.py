"""Turning off a schedule that can no longer run, and saying why.

A schedule whose saved crew or flow was deleted, or whose cron line or timezone
cannot be read, would otherwise fail on every tick with nothing to show for it:
no run in the history, and the schedule still on. Retiring it records one FAILED
run that carries the reason (so the run history shows what happened and when)
and switches the schedule off. Switching it back on retries it.
"""

import logging
import uuid
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.execution_status import ExecutionStatus
from src.models.schedule import Schedule
from src.repositories.schedule_repository import ScheduleRepository
from src.services.execution.service import ExecutionService
from src.services.execution.status import ExecutionStatusService
from src.services.scheduling.saved_targets import saved_id
from src.utils.cron_utils import ensure_utc

logger = logging.getLogger(__name__)


async def retire_schedule(
    session: AsyncSession, schedule: Schedule, reason: str, when: datetime
) -> None:
    """Record a failed run saying ``reason``, then deactivate ``schedule``.

    ``when`` is the time the schedule was due; the run is stamped with it.
    Commits on ``session``, which the scheduler owns.
    """
    job_id = str(uuid.uuid4())
    execution_type = schedule.execution_type or "crew"
    failed = ExecutionStatus.FAILED.value
    await ExecutionService.create_run_record(
        session,
        job_id=job_id,
        run_name=f"{schedule.name} (schedule turned off)",
        inputs={},
        execution_type=execution_type,
        group_id=schedule.group_id,
        group_email=schedule.created_by_email,
        flow_id=saved_id(schedule.flow_id),
        crew_id=saved_id(schedule.crew_id),
        status=failed,
        trigger_type="scheduled",
        created_at=(ensure_utc(when) or when).replace(tzinfo=None),
    )
    # The status writer is what stores a run's error message.
    await ExecutionStatusService.update_status(job_id, failed, reason, session=session)
    await ScheduleRepository(session).deactivate(schedule.id)
    await session.commit()
    logger.warning(
        "Schedule %s (%s) turned off: %s Run %s records it.",
        schedule.id,
        schedule.name,
        reason,
        job_id,
    )
