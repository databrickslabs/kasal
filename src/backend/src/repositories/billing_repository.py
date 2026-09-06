from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, asc, delete, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.billing import LLMUsageBilling
from src.models.execution_history import ExecutionHistory


class BillingRepository(BaseRepository[LLMUsageBilling]):
    """Repository for billing operations"""

    def __init__(self, session: AsyncSession):
        super().__init__(LLMUsageBilling, session)

    async def create_usage_record(self, usage_data: Dict[str, Any]) -> LLMUsageBilling:
        """Create a new LLM usage billing record"""
        usage_record = LLMUsageBilling(**usage_data)
        self.session.add(usage_record)
        await self.session.flush()
        return usage_record

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

    async def get_usage_by_execution(
        self, execution_id: str, group_id: Optional[str] = None
    ) -> List[LLMUsageBilling]:
        """Get all usage records for a specific execution"""
        query = self.session.query(LLMUsageBilling).filter(
            LLMUsageBilling.execution_id == execution_id
        )

        if group_id:
            query = query.filter(LLMUsageBilling.group_id == group_id)

        return query.all()

    async def get_usage_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        group_id: Optional[str] = None,
        user_email: Optional[str] = None,
    ) -> List[LLMUsageBilling]:
        """Get usage records within a date range"""
        query = self.session.query(LLMUsageBilling).filter(
            and_(
                LLMUsageBilling.usage_date >= start_date,
                LLMUsageBilling.usage_date <= end_date,
            )
        )

        if group_id:
            query = query.filter(LLMUsageBilling.group_id == group_id)

        if user_email:
            query = query.filter(LLMUsageBilling.user_email == user_email)

        return query.order_by(desc(LLMUsageBilling.usage_date)).all()

    async def get_cost_summary_by_period(
        self,
        start_date: datetime,
        end_date: datetime,
        group_id: Optional[str] = None,
        group_by: str = "day",  # 'day', 'week', 'month'
    ) -> List[Dict[str, Any]]:
        """Get cost summary grouped by time period"""

        # Choose the appropriate date trunc function based on group_by
        if group_by == "day":
            date_trunc = func.date_trunc("day", LLMUsageBilling.usage_date)
        elif group_by == "week":
            date_trunc = func.date_trunc("week", LLMUsageBilling.usage_date)
        elif group_by == "month":
            date_trunc = func.date_trunc("month", LLMUsageBilling.usage_date)
        else:
            date_trunc = func.date_trunc("day", LLMUsageBilling.usage_date)

        query = self.session.query(
            date_trunc.label("period"),
            func.sum(LLMUsageBilling.cost_usd).label("total_cost"),
            func.sum(LLMUsageBilling.total_tokens).label("total_tokens"),
            func.sum(LLMUsageBilling.prompt_tokens).label("total_prompt_tokens"),
            func.sum(LLMUsageBilling.completion_tokens).label(
                "total_completion_tokens"
            ),
            func.count(LLMUsageBilling.id).label("total_requests"),
        ).filter(
            and_(
                LLMUsageBilling.usage_date >= start_date,
                LLMUsageBilling.usage_date <= end_date,
            )
        )

        if group_id:
            query = query.filter(LLMUsageBilling.group_id == group_id)

        results = query.group_by(date_trunc).order_by(asc(date_trunc)).all()

        return [
            {
                "period": result.period,
                "total_cost": float(result.total_cost or 0),
                "total_tokens": result.total_tokens or 0,
                "total_prompt_tokens": result.total_prompt_tokens or 0,
                "total_completion_tokens": result.total_completion_tokens or 0,
                "total_requests": result.total_requests or 0,
            }
            for result in results
        ]

    async def get_cost_by_model(
        self, start_date: datetime, end_date: datetime, group_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get cost breakdown by model"""
        query = self.session.query(
            LLMUsageBilling.model_name,
            LLMUsageBilling.model_provider,
            func.sum(LLMUsageBilling.cost_usd).label("total_cost"),
            func.sum(LLMUsageBilling.total_tokens).label("total_tokens"),
            func.count(LLMUsageBilling.id).label("total_requests"),
        ).filter(
            and_(
                LLMUsageBilling.usage_date >= start_date,
                LLMUsageBilling.usage_date <= end_date,
            )
        )

        if group_id:
            query = query.filter(LLMUsageBilling.group_id == group_id)

        results = (
            query.group_by(LLMUsageBilling.model_name, LLMUsageBilling.model_provider)
            .order_by(desc("total_cost"))
            .all()
        )

        return [
            {
                "model_name": result.model_name,
                "model_provider": result.model_provider,
                "total_cost": float(result.total_cost or 0),
                "total_tokens": result.total_tokens or 0,
                "total_requests": result.total_requests or 0,
            }
            for result in results
        ]

    async def get_cost_by_user(
        self, start_date: datetime, end_date: datetime, group_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get cost breakdown by user"""
        query = self.session.query(
            LLMUsageBilling.user_email,
            func.sum(LLMUsageBilling.cost_usd).label("total_cost"),
            func.sum(LLMUsageBilling.total_tokens).label("total_tokens"),
            func.count(LLMUsageBilling.id).label("total_requests"),
        ).filter(
            and_(
                LLMUsageBilling.usage_date >= start_date,
                LLMUsageBilling.usage_date <= end_date,
                LLMUsageBilling.user_email.isnot(None),
            )
        )

        if group_id:
            query = query.filter(LLMUsageBilling.group_id == group_id)

        results = (
            query.group_by(LLMUsageBilling.user_email)
            .order_by(desc("total_cost"))
            .all()
        )

        return [
            {
                "user_email": result.user_email,
                "total_cost": float(result.total_cost or 0),
                "total_tokens": result.total_tokens or 0,
                "total_requests": result.total_requests or 0,
            }
            for result in results
        ]

    async def get_monthly_cost_for_group(
        self, group_id: str, year: int, month: int
    ) -> float:
        """Get total cost for a group in a specific month"""
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        result = (
            self.session.query(func.sum(LLMUsageBilling.cost_usd).label("total_cost"))
            .filter(
                and_(
                    LLMUsageBilling.group_id == group_id,
                    LLMUsageBilling.usage_date >= start_date,
                    LLMUsageBilling.usage_date < end_date,
                )
            )
            .scalar()
        )

        return float(result or 0)
