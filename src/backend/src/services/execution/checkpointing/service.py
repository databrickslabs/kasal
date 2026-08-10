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

        current_keys = await self._current_content_keys(execution, group_context)
        changed_at = self._first_changed(units, current_keys)

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
            # Where a resume would actually pick up, given what has been edited
            # since. None when nothing detectable changed, in which case the
            # whole recorded prefix is replayable.
            "changed_from_index": changed_at,
            "restorable_count": len(units) if changed_at is None else changed_at,
            "units": [
                self._summarise(unit, current_keys, changed_at, index)
                for index, unit in enumerate(units)
            ],
        }

    async def _current_content_keys(
        self, execution: Any, group_context: Optional[GroupContext]
    ) -> Optional[List[str]]:
        """Content keys of the saved definition, IN ORDER.

        A list, not a mapping by name: crew units record ``name=None`` (the
        recorder reads it off the runtime Task, which canvas-built tasks do not
        carry), so a by-name lookup missed every unit and reported the whole
        checkpoint as changed. Position is what the run itself matches on
        anyway — ``_load_checkpoint`` walks index by index — so comparing by
        position is both correct here and the same question the run asks.

        None means "cannot tell" — no saved definition to compare against, or
        reading it failed. That is reported as unknown rather than as unchanged:
        claiming a unit will be restored and then re-running it is the mistake
        this whole surface exists to stop.

        Only the TEXT half is comparable here. The full identity hashes built
        tool objects and a resolved LLM, which do not exist outside a run — so a
        re-modelled or re-tooled unit looks unchanged to this and is caught at
        run time instead. Everything downstream of that phrases the answer as a
        floor, never as a promise.
        """
        if (execution.execution_type or "").lower() == "flow":
            return await self._flow_content_keys(execution, group_context)

        crew_id = getattr(execution, "crew_id", None)
        if not crew_id:
            return None

        try:
            from src.services.catalog.crew_config import (
                build_crew_execution_config_by_id,
            )
            from src.services.execution.runtime.identity import content_key

            projected = await build_crew_execution_config_by_id(
                self.session, crew_id, group_context
            )
        except Exception as exc:  # noqa: BLE001 — a preview may never break a read
            logger.warning(
                "Could not read crew %s to compare against its checkpoint: %s",
                crew_id,
                exc,
            )
            return None

        if not projected:
            return None

        _, tasks_yaml = projected
        return [
            content_key(entry.get("description"), entry.get("expected_output"))
            for entry in tasks_yaml.values()
        ]

    async def _flow_content_keys(
        self, execution: Any, group_context: Optional[GroupContext]
    ) -> Optional[Dict[str, str]]:
        """Content keys of a flow's crews, by crew NAME.

        By name rather than by position because a flow records units in
        COMPLETION order (the recorder increments a counter per crew that
        finishes), so unit 2 is not the second crew declared. Flow units do
        carry their crew name, which crew units do not — so each side matches
        on the identifier it actually has.

        Rebuilt from the run's own ``flow_config`` (crew names and their task
        id lists) plus the current task rows, which is exactly what
        ``crew_content_key`` hashes. No crew is built: the text is all this
        needs, and building one here would mean resolving tools and an LLM in
        the middle of a GET.
        """
        from types import SimpleNamespace

        from src.services.catalog.tasks import TaskService
        from src.services.execution.runtime.identity import (
            content_key,
            crew_content_key,
        )

        flow_config = (getattr(execution, "inputs", None) or {}).get("flow_config")
        if not isinstance(flow_config, dict):
            return None

        crews: List[tuple] = []
        for point in flow_config.get("startingPoints") or []:
            name = point.get("crewName") or point.get("name")
            task_id = point.get("taskId")
            if name and task_id:
                crews.append((name, [task_id]))
        for listener in flow_config.get("listeners") or []:
            name = listener.get("name") or listener.get("crewName")
            raw = listener.get("tasks") or listener.get("taskIds") or []
            # Listener tasks arrive either as ids or as objects carrying one.
            ids = [t.get("id") if isinstance(t, dict) else t for t in raw]
            ids = [i for i in ids if i]
            if name and ids:
                crews.append((name, ids))

        if not crews:
            return None

        service = TaskService(self.session)
        keys: Dict[str, str] = {}
        try:
            for name, task_ids in crews:
                stand_ins = []
                for task_id in task_ids:
                    row = (
                        await service.get_with_group_check(str(task_id), group_context)
                        if group_context is not None
                        else await service.get(str(task_id))
                    )
                    if row is None:
                        stand_ins = []
                        break
                    stand_ins.append(
                        SimpleNamespace(
                            key=content_key(row.description, row.expected_output)
                        )
                    )
                if not stand_ins:
                    continue  # a crew we cannot judge, rather than a false verdict
                crew_key = crew_content_key(name, stand_ins)
                if crew_key:
                    keys[name] = crew_key
        except Exception as exc:  # noqa: BLE001 — a preview may never break a read
            logger.warning("Could not rebuild flow crew keys for comparison: %s", exc)
            return None

        return keys or None

    @staticmethod
    def _first_changed(units: List[Dict[str, Any]], current_keys: Any) -> Optional[int]:
        """Index of the earliest unit whose text no longer matches.

        The rule the run applies is a PREFIX — everything from the first change
        re-runs, including units that did not themselves change, because their
        input did. So one index describes the whole answer.
        """
        if not current_keys:
            return None

        by_name = isinstance(current_keys, dict)

        for index, unit in enumerate(units):
            stored = unit.get("content_key")
            if not stored:
                continue  # written before content keys existed

            if by_name:
                # Flow units: matched on the crew name they carry.
                current = current_keys.get(unit.get("name"))
                if current is None:
                    continue  # a crew this reader cannot judge
            else:
                if index >= len(current_keys):
                    # A unit past the end of the definition. The engine appends
                    # synthetic tasks in some configurations (a parallel crew
                    # gets a completion task), so this is not evidence of an
                    # edit and must not be reported as one.
                    continue
                current = current_keys[index]

            if current != stored:
                return index
        return None

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
    def _summarise(
        unit: Dict[str, Any],
        current_keys: Optional[Dict[str, str]] = None,
        changed_at: Optional[int] = None,
        index: Optional[int] = None,
    ) -> Dict[str, Any]:
        """One unit, with what a resume would do to it.

        ``will_restore`` is three-valued on purpose. True and False are
        verdicts; None is "unknown", which is what an old checkpoint or a run
        with no saved definition honestly gets. Collapsing None into True would
        promise a restore that run time may refuse.
        """
        will_restore: Optional[bool] = None
        if current_keys is not None and index is not None:
            will_restore = changed_at is None or index < changed_at

        return {
            "key": unit.get("key"),
            "name": unit.get("name"),
            "agent": unit.get("agent"),
            "output_preview": unit_preview(unit),
            "truncated": bool(unit.get("truncated")),
            "completed_at": unit.get("completed_at"),
            "will_restore": will_restore,
        }
