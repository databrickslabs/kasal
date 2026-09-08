from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.billing import LLMUsageBilling
from src.models.execution_history import ExecutionHistory


class BillingRepository(BaseRepository[LLMUsageBilling]):
    """Retain legacy usage rows only for execution-history housekeeping.

    Activity billing reads execution traces through TraceUsageRepository and
    ModelBillingRateRepository; this table is no longer the reporting source.
    """

    def __init__(self, session: AsyncSession):
        super().__init__(LLMUsageBilling, session)

    async def delete_older_than(self, cutoff: datetime) -> int:
        """
        Delete billing usage records that reference executions older than the cutoff.

        LLMUsageBilling.execution_id is a FK to executionhistory.job_id WITHOUT
        ON DELETE CASCADE, so these rows must be removed before the parent
        execution history rows during housekeeping, otherwise the DELETE on
        executionhistory raises a FOREIGN KEY constraint failure.

        Deletion is scoped by the parent execution's created_at (not the billing
        row's own timestamp) so it stays in lock-step with the execution rows
        being removed and never leaves orphaned references.

        Args:
            cutoff: Delete records whose execution's created_at is before this datetime

        Returns:
            Number of deleted records
        """
        old_job_ids = select(ExecutionHistory.job_id).where(
            ExecutionHistory.created_at < cutoff
        )
        stmt = delete(LLMUsageBilling).where(
            LLMUsageBilling.execution_id.in_(old_job_ids)
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount
