from typing import List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.a2a_push_config import A2APushConfig


class A2APushConfigRepository(BaseRepository[A2APushConfig]):
    """Data access for A2A push-notification registrations.

    Exists because the architecture ratchet caught the queries living in the
    service — and it was right to. A push config addresses a run, runs are
    group-scoped, and a query written inline in a service is where a group
    filter silently stops being applied.

    The one read that is deliberately NOT group-filtered is
    ``list_deliverable``, and that is explained on the method.
    """

    def __init__(self, session: AsyncSession):
        super().__init__(A2APushConfig, session)

    async def find_for_task_and_url(
        self, task_id: str, url: str, group_id: str
    ) -> Optional[A2APushConfig]:
        """An existing registration for this exact task and URL.

        Used to make registration idempotent: re-registering the same URL is an
        update, not a second row, or a caller that simply retried would silently
        double every future notification.
        """
        result = await self.session.execute(
            select(self.model).where(
                self.model.task_id == task_id,
                self.model.url == url,
                self.model.group_id == group_id,
            )
        )
        return result.scalars().first()

    async def list_for_task(
        self, task_id: str, group_ids: List[str]
    ) -> List[A2APushConfig]:
        """Registrations on a task that these groups may see."""
        if not group_ids:
            return []
        result = await self.session.execute(
            select(self.model).where(
                self.model.task_id == task_id,
                self.model.group_id.in_(group_ids),
            )
        )
        return list(result.scalars().all())

    async def list_deliverable(
        self, task_id: str, failure_limit: int
    ) -> List[A2APushConfig]:
        """Registrations to actually POST to for this task.

        NOT group-filtered, and that is correct rather than an oversight: this
        runs from the status-update path, which has a job id and no caller. The
        scoping was already enforced when the config was created — registration
        refuses a task the caller cannot see — so every row here is one someone
        in the right workspace deliberately created for THIS run.

        Dead endpoints are excluded in SQL rather than skipped in Python, so a
        webhook pointing at something permanently gone costs nothing per run
        instead of three timed-out requests.
        """
        result = await self.session.execute(
            select(self.model).where(
                self.model.task_id == task_id,
                self.model.consecutive_failures < failure_limit,
            )
        )
        return list(result.scalars().all())

    async def insert(self, config: A2APushConfig) -> A2APushConfig:
        """Persist a new push config and flush so its ``id`` is available."""
        self.session.add(config)
        await self.session.flush()
        return config

    async def save(self) -> None:
        """Flush pending changes — e.g. the delivery counters after a send."""
        await self.session.flush()

    async def delete_for_group(self, config_id: int, group_ids: List[str]) -> int:
        """Remove a registration. Returns the number of rows removed."""
        if not group_ids:
            return 0
        result = await self.session.execute(
            delete(self.model).where(
                self.model.id == config_id,
                self.model.group_id.in_(group_ids),
            )
        )
        return result.rowcount or 0
