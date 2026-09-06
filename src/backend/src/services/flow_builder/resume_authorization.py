"""Authorize external resume references before state reads or writes."""

from src.core.exceptions import NotFoundError


def require_run_owner(execution, group_ids):
    if (
        execution is None
        or not group_ids
        or not getattr(execution, "group_id", None)
        or execution.group_id not in group_ids
    ):
        raise NotFoundError(detail="Resume execution not found")
    return execution


async def get_owned_resume_source(service, source_id, group_ids):
    if not group_ids:
        raise NotFoundError(detail="Resume execution not found")
    source = await service.get_run_by_job_id(str(source_id), group_ids=group_ids)
    if source is None:
        try:
            numeric_id = int(source_id)
        except (TypeError, ValueError):
            numeric_id = None
        if numeric_id is not None:
            source = await service.get_run_by_id(numeric_id, group_ids=group_ids)
    return require_run_owner(source, group_ids)
