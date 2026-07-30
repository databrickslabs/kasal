"""Checkpoint queries and lifecycle, for any execution type.

The router asks this; this asks the store, the lifecycle and the repository.
Nothing here knows whether it is looking at a crew or a flow beyond the
``kind`` on the record, which is the whole point of unification: the endpoints
that list, inspect, expire and resume a checkpoint are the same endpoints
either way.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.services.execution.checkpointing import lifecycle, store
from src.services.execution.checkpointing.record import (
    is_truncated,
    ordered_units,
    unit_preview,
)
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class CheckpointService:
    """Read and manage the checkpoint on one execution."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = ExecutionHistoryRepository(session)

    async def get_checkpoint(
        self, job_id: str, group_context: Optional[GroupContext] = None
    ) -> Optional[Dict[str, Any]]:
        """Summarise an execution's checkpoint, or None if it has none.

        Returns the units WITHOUT their full outputs — a checkpoint can hold
        half a megabyte per unit, and a list view that dragged all of it
        through the driver would be unusable on a long crew. Full output comes
        from :meth:`get_unit`.
        """
        group_ids = group_context.group_ids if group_context else None

        execution = await self.repository.get_execution_by_job_id(
            job_id, group_ids=group_ids
        )
        if not execution:
            return None

        record = await store.read_record(self.session, job_id, group_ids=group_ids)
        if not record:
            return None

        units = ordered_units(record)
        blocker = lifecycle.resumable_blocker(
            execution.status, execution.checkpoint_status
        )

        return {
            "job_id": job_id,
            "execution_id": execution.id,
            "kind": record.get("kind"),
            "version": record.get("version"),
            "status": execution.checkpoint_status,
            "execution_status": execution.status,
            "run_name": execution.run_name,
            "created_at": execution.created_at,
            "unit_count": record.get("unit_count"),
            "completed_count": len(units),
            "truncated": is_truncated(record),
            # Executions that predate written checkpoints were migrated on
            # read; the UI says so rather than presenting them as equivalent.
            "derived": bool(record.get("migrated_from_version") is not None),
            "resumable": blocker is None,
            "blocked_reason": blocker,
            "units": [self._summarise(unit) for unit in units],
        }

    async def get_unit(
        self,
        job_id: str,
        unit_key: str,
        group_context: Optional[GroupContext] = None,
    ) -> Optional[Dict[str, Any]]:
        """One unit WITH its full output."""
        group_ids = group_context.group_ids if group_context else None
        record = await store.read_record(self.session, job_id, group_ids=group_ids)
        if not record:
            return None

        unit = (record.get("units") or {}).get(str(unit_key))
        if not unit:
            return None

        return {
            **self._summarise(unit),
            "output_raw": unit.get("output_raw") or "",
            "output_json": unit.get("output_json"),
        }

    async def expire(
        self, job_id: str, group_context: Optional[GroupContext] = None
    ) -> bool:
        """Dismiss a checkpoint so it stops offering itself as resumable."""
        group_ids = group_context.group_ids if group_context else None
        return await lifecycle.expire(self.session, job_id, group_ids=group_ids)

    async def list_for_flow(
        self,
        flow_id,
        group_context: Optional[GroupContext] = None,
        status_filter: Optional[str] = "active",
    ) -> List[Dict[str, Any]]:
        """Checkpoints belonging to one saved flow.

        Kept because the flow endpoints are scoped to a flow rather than an
        execution; the per-execution endpoints are the general form.
        """
        group_id = group_context.primary_group_id if group_context else None
        executions = await self.repository.get_checkpoints_for_flow(
            flow_id=flow_id, group_id=group_id, status_filter=status_filter
        )

        summaries = []
        for execution in executions:
            summary = await self.get_checkpoint(execution.job_id, group_context)
            if summary:
                summary["flow_uuid"] = execution.flow_uuid
                summary["checkpoint_method"] = execution.checkpoint_method
                summaries.append(summary)
        return summaries

    @staticmethod
    def _summarise(unit: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "key": unit.get("key"),
            "name": unit.get("name"),
            "agent": unit.get("agent"),
            "output_preview": unit_preview(unit),
            "truncated": bool(unit.get("truncated")),
            "completed_at": unit.get("completed_at"),
        }
