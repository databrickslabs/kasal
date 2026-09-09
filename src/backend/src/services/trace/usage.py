"""Numeric usage accessor for billing; trace ownership stays in this domain."""

from src.core.exceptions import ForbiddenError
from src.repositories.trace_usage_repository import TraceUsageRepository


class TraceUsageService:
    def __init__(self, session):
        self.repository = TraceUsageRepository(session)

    async def iter_calls(self, context, start, end, execution_ids=None):
        if not context or not context.primary_group_id:
            raise ForbiddenError("Select a teamspace to view usage.")
        async for row in self.repository.iter_calls(
            context.primary_group_id, start, end, execution_ids
        ):
            yield row
