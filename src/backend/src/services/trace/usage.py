"""Numeric usage accessor for billing; trace ownership stays in this domain."""

from datetime import datetime
from typing import Any, AsyncIterator, Collection, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import ForbiddenError
from src.repositories.trace_usage_repository import TraceUsageRepository


class TraceUsageService:
    def __init__(self, session: AsyncSession) -> None:
        self.repository = TraceUsageRepository(session)

    async def iter_calls(
        self,
        context: Any,
        start: datetime,
        end: datetime,
        execution_ids: Optional[Collection[str]] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        if not context or not context.primary_group_id:
            raise ForbiddenError("Select a teamspace to view usage.")
        async for row in self.repository.iter_calls(
            context.primary_group_id, start, end, execution_ids
        ):
            yield row
