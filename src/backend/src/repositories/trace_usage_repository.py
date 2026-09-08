"""Read just the attribution and usage fields, without prompt/response bodies."""

from sqlalchemy import and_, or_, select

from src.models.execution_history import ExecutionHistory
from src.models.execution_trace import ExecutionTrace

_FIELDS = (
    "model",
    "usage",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "cached_prompt_tokens",
)


class TraceUsageRepository:
    def __init__(self, session):
        self.session = session

    async def iter_calls(self, group_id, start, end, execution_ids=None):
        trace, run = ExecutionTrace, ExecutionHistory
        query = (
            select(
                trace.id,
                trace.job_id,
                trace.span_id,
                trace.created_at,
                # Reasoning and tool arguments can be large and live alongside
                # usage in these JSON objects. Never load them for billing.
                *(trace.trace_metadata[key].label(f"meta_{key}") for key in _FIELDS),
                *(
                    trace.output["extra_data"][key].label(f"extra_{key}")
                    for key in _FIELDS
                ),
                run.job_id.label("execution_id"),
                run.run_name,
                run.execution_type,
            )
            .join(
                run,
                or_(
                    trace.job_id == run.job_id,
                    and_(trace.job_id.is_(None), trace.run_id == run.id),
                ),
            )
            .where(
                run.group_id == group_id,
                # Legacy rows may have no trace group; parent ownership is required.
                or_(trace.group_id == group_id, trace.group_id.is_(None)),
                trace.event_type == "llm_response",
                trace.created_at >= start,
                trace.created_at < end,
            )
            .order_by(trace.id)
            .execution_options(yield_per=500)
        )
        if execution_ids is not None:
            query = query.where(run.job_id.in_(execution_ids))
        result = await self.session.stream(query)
        try:
            async for row in result.mappings():
                data = dict(row)
                data["trace_metadata"] = {
                    key: data.pop(f"meta_{key}")
                    for key in _FIELDS
                    if data[f"meta_{key}"] is not None
                }
                data["extra"] = {
                    key: data.pop(f"extra_{key}")
                    for key in _FIELDS
                    if data[f"extra_{key}"] is not None
                }
                yield data
        finally:
            await result.close()
