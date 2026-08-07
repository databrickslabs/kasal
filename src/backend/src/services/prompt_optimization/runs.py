"""The optimization-run registry as seen by callers: read, cancel, apply and
revert. Apply/revert are the write path onto crew nodes and templates.

Mixed into ``PromptOptimizationService`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,031-line file, and the public surface is unchanged.
"""

import asyncio
import hashlib
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.core.exceptions import BadRequestError
from src.schemas.template import PromptTemplateUpdate
from src.services.catalog.templates import TemplateService
from src.services.prompt_optimization import run_state
from src.services.prompt_optimization.run_state import (
    _LIVE_COUNTERS,
    _MAX_KEPT_RUNS,
    _PUBLIC_FIELDS,
    _RUNS,
    RUN_STALE_SECONDS,
    _persist_run_changes,
    _row_to_public,
)
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class RunRegistryMixin:
    async def get_run(
        self, run_id: str, group_context: Optional[GroupContext] = None
    ) -> Optional[Dict[str, Any]]:
        """One run, from its durable row (group-scoped), with live counters."""
        group_id = group_context.primary_group_id if group_context else None
        row = await self.run_repository.get_by_group(run_id, group_id)
        if row is None:
            return None
        await self._settle_orphans(group_id)
        if row.status in ("pending", "running") and run_id not in _RUNS:
            # Re-read once: _settle_orphans may have just failed this row.
            row = await self.run_repository.get_by_group(run_id, group_id)
            if row is None:
                return None
        return self._public(row, group_context)

    async def list_runs(
        self, group_context: Optional[GroupContext] = None
    ) -> List[Dict[str, Any]]:
        """Recent runs for the caller's group, newest first."""
        group_id = group_context.primary_group_id if group_context else None
        await self._settle_orphans(group_id)
        rows = await self.run_repository.list_by_group(group_id, limit=_MAX_KEPT_RUNS)
        return [self._public(row, group_context) for row in rows]

    def _public(
        self, row: Any, group_context: Optional[GroupContext]
    ) -> Dict[str, Any]:
        """Row -> API dict, overlaying a LIVE run's fresher progress counters.

        The worker thread bumps `executions_used`/`candidates_tried` in memory
        on every crew execution and the heartbeat only flushes them every
        RUN_HEARTBEAT_SECONDS, so the row lags for an active run. The overlay is
        group-checked so a cached entry can never leak across groups.
        """
        public = _row_to_public(row)
        run = _RUNS.get(row.id)
        if run is not None and self._visible(run, group_context):
            for key in _LIVE_COUNTERS:
                if run.get(key) is not None:
                    public[key] = run[key]
            # Memory also holds the freshest terminal state (the DB write for a
            # just-finished run may still be in flight).
            if run.get("status"):
                public["status"] = run["status"]
        # Timestamps are stored naive-UTC (every model's convention here) but
        # must serialize with +00:00 so browsers render local time — a naive
        # stamp showed a 01:20 local run as "11:20 PM" (observed live).
        for key in ("created_at", "applied_at"):
            value = public.get(key)
            if isinstance(value, datetime) and value.tzinfo is None:
                public[key] = value.replace(tzinfo=timezone.utc)
        return {k: public.get(k) for k in _PUBLIC_FIELDS}

    async def _settle_orphans(self, group_id: Optional[str]) -> None:
        """Fail runs whose heartbeat died with the backend that owned them.

        A `--reload` (or any restart) kills the asyncio task but leaves the row
        at 'running'. Left alone, that run polls forever and the UI's "run in
        progress" lock never clears, so no new run can be started. Only rows
        with a STALE heartbeat and no LIVE task are touched, which is why a
        legitimately long crew run (hours) is never mistaken for one.
        """
        try:
            stale = await self.run_repository.find_stale_active(
                group_id, RUN_STALE_SECONDS
            )
        except Exception as stale_err:
            logger.debug(f"Could not scan for orphaned runs: {stale_err}")
            return
        for row in stale:
            cached = _RUNS.get(row.id)
            if cached is not None:
                task = cached.get("task")
                # Skip only if the cached entry is genuinely still live: either
                # its task is running, or it carries no task handle at all (a
                # long run with a merely-lagging heartbeat — the conservative
                # default, so hours-long crews are never mistaken for orphans).
                # A cached entry whose task is DONE but whose row is still
                # pending/running is proof the coroutine died before writing a
                # terminal status; the old `row.id in _RUNS` skip treated it as
                # alive forever, wedging the UI's "run in progress" lock until a
                # manual DB delete. Settle exactly that case.
                if task is None or not task.done():
                    continue
                # Keep the cache in step with the DB write below: `_public`
                # overlays a cached entry's status onto the row on read, so a
                # settled row would still surface as 'running' if we left the
                # stale cache entry saying so.
                cached["status"] = "failed"
            logger.info(
                f"Prompt optimization {row.id} orphaned (last heartbeat "
                f"{row.updated_at}, no live task); marking failed"
            )
            await run_state._persist_run_changes(
                row.id,
                {
                    "status": "failed",
                    "error": (
                        "This run was abandoned before it could finish (the "
                        "backend restarted, or the run died in flight). Its "
                        "MLflow record (if any) survives; start a new run to get "
                        "a proposal."
                    ),
                },
            )

    @staticmethod
    def _visible(run: Dict[str, Any], group_context: Optional[GroupContext]) -> bool:
        group_id = group_context.primary_group_id if group_context else None
        return run.get("group_id") == group_id

    @staticmethod
    def _prune_runs() -> None:
        if len(_RUNS) <= _MAX_KEPT_RUNS:
            return
        # Drop the oldest finished runs first; never prune active ones.
        finished = [
            r for r in _RUNS.values() if r.get("status") in ("completed", "failed")
        ]
        _epoch = datetime.min.replace(tzinfo=timezone.utc)
        finished.sort(key=lambda r: r.get("created_at") or _epoch)
        for run in finished[: len(_RUNS) - _MAX_KEPT_RUNS]:
            _RUNS.pop(run["run_id"], None)

    async def cancel_run(
        self, run_id: str, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Request a running optimization to stop. The flag is honored before
        the NEXT crew execution — an in-flight execution finishes first."""
        group_id = group_context.primary_group_id if group_context else None
        row = await self.run_repository.get_by_group(run_id, group_id)
        if row is None:
            raise ValueError(f"Optimization run '{run_id}' not found")
        if row.status not in ("pending", "running"):
            raise ValueError(f"Optimization run '{run_id}' is not active")
        run = _RUNS.get(run_id)
        if run is not None and self._visible(run, group_context):
            run["cancel_requested"] = True
            logger.info(f"Prompt optimization {run_id}: cancellation requested")
            return {"run_id": run_id, "cancelling": True}
        # Active in the DB but not in this process: the worker that owned it is
        # gone (restart), so there is nothing to signal — settle the record
        # instead of leaving a run that can never stop or finish.
        logger.info(
            f"Prompt optimization {run_id}: no in-process task to cancel "
            f"(orphaned by a restart); marking cancelled"
        )
        await run_state._persist_run_changes(
            run_id,
            {
                "status": "cancelled",
                "error": None,
            },
        )
        return {"run_id": run_id, "cancelling": True}

    async def delete_run(
        self, run_id: str, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Delete a run's durable record so it stops blocking new runs.

        Group-scoped. If the run is still active in THIS process, request
        cancellation first so the worker stops touching a row we're removing,
        then drop it from the in-memory cache and delete the row. A run that no
        longer exists is treated as already-deleted (idempotent)."""
        group_id = group_context.primary_group_id if group_context else None
        row = await self.run_repository.get_by_group(run_id, group_id)
        if row is None:
            return {"run_id": run_id, "deleted": False}
        cached = _RUNS.get(run_id)
        if cached is not None and self._visible(cached, group_context):
            cached["cancel_requested"] = True
            _RUNS.pop(run_id, None)
        deleted = await self.run_repository.delete(run_id, group_id)
        logger.info(f"Prompt optimization {run_id}: deleted (was {row.status})")
        return {"run_id": run_id, "deleted": bool(deleted)}

    async def apply_run(
        self, run_id: str, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Apply a completed run's proposal, recording a before-image first.

        The before-image is captured HERE, not from the run's baseline: the
        rows may have been edited between the run and the apply, and what
        `revert_run` must restore is what was actually overwritten.
        """
        group_id = group_context.primary_group_id if group_context else None
        row = await self.run_repository.get_by_group(run_id, group_id)
        if row is None:
            raise ValueError(f"Optimization run '{run_id}' not found")
        if row.status != "completed" or not row.optimized_template:
            raise ValueError(
                f"Optimization run '{run_id}' has no completed proposal to apply"
            )

        if row.kind == "crew":
            result = await self._apply_crew_run(row, group_context)
        else:
            template_service = TemplateService(self.session)
            template_row = await template_service.find_by_name_with_group_check(
                row.target_name, group_context
            )
            if template_row is None:
                raise ValueError(f"Template '{row.target_name}' not found")
            before_image = {
                "template": str(getattr(template_row, "template", "") or "")
            }
            updated = await template_service.update_with_group_check(
                template_row.id,
                PromptTemplateUpdate(template=row.optimized_template),
                group_context,
            )
            if updated is None:
                raise ValueError(f"Failed to update template '{row.target_name}'")
            result = {
                "run_id": run_id,
                "template_name": row.target_name,
                "applied": True,
                "before_image": before_image,
            }

        before_image = result.pop("before_image", None)
        await self.run_repository.update_fields(
            run_id,
            {
                "applied": True,
                "applied_at": datetime.utcnow(),
                "applied_by": getattr(group_context, "group_email", None),
                "before_image": before_image,
            },
        )
        run = _RUNS.get(run_id)
        if run is not None:
            run["applied"] = True
        return result

    async def _apply_crew_run(
        self, row: Any, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Write a crew run's optimized fields onto the agent/task rows.

        Returns the result plus a `before_image` of every field written, read
        from the CURRENT rows immediately before the write — without it an
        apply is irreversible, which matters because crew-level GEPA inverts
        with team size (measured +2.4 at 2 agents, -2.1 at 10, worst -16.0), so
        a good-looking score can permanently degrade a large crew.
        """
        optimized: Dict[str, str] = row.optimized_fields or {}
        baseline: Dict[str, str] = row.baseline_fields or {}

        # Group per entity, skipping unchanged fields.
        changes: Dict[tuple, Dict[str, str]] = {}
        for key, value in optimized.items():
            if not value or value == baseline.get(key):
                continue
            try:
                entity_kind, entity_id, field = key.split(".", 2)
            except ValueError:
                continue
            changes.setdefault((entity_kind, entity_id), {})[field] = value

        before_image = await self._read_crew_fields(changes, group_context)
        applied = await self._write_crew_fields(changes, group_context)
        # The canvas renders crews.nodes, NOT the agent/task rows — the graph is
        # stored twice. Writing only the rows made an apply report success while
        # the canvas and the exported JSON still showed the old prompts, which
        # reads as "apply is broken" even though the database was correct.
        synced = await self._sync_crew_nodes(row.crew_id, changes)
        logger.info(
            f"Crew optimization {row.id} applied to {applied} entities "
            f"({len(before_image)} fields snapshotted for revert; "
            f"{synced} canvas node(s) synced)"
        )
        return {
            "run_id": row.id,
            "template_name": row.target_name,
            "applied": True,
            "before_image": before_image,
        }

    async def _sync_crew_nodes(
        self, crew_id: Optional[str], changes: Dict[tuple, Dict[str, str]]
    ) -> int:
        """Mirror an apply onto the crew's canvas snapshot.

        A crew's graph lives in two places: the ``agents``/``tasks`` rows, and a
        denormalised copy inside ``crews.nodes`` that the canvas renders and the
        JSON export serialises. An apply that writes only the rows leaves the
        snapshot stale, so the user is told "applied" and then sees the old
        prompts — the change is real but invisible where they look for it.

        Best-effort by design: the rows are the system of record and were
        already written, so a failure here must not fail (or half-undo) the
        apply. Returns the number of nodes patched.
        """
        if not crew_id or not changes:
            return 0
        try:
            import copy
            from uuid import UUID

            # Crews are CrewService's domain.
            from src.services.catalog.crews import CrewService

            crew = await CrewService(self.session).get(UUID(str(crew_id)))
            if crew is None or not crew.nodes:
                return 0

            # entity id -> {field: value}, keyed by the node's own id field.
            by_agent = {
                eid: patch for (kind, eid), patch in changes.items() if kind == "agent"
            }
            by_task = {
                eid: patch for (kind, eid), patch in changes.items() if kind == "task"
            }

            patched = 0
            nodes = copy.deepcopy(crew.nodes)
            for node in nodes:
                data = node.get("data")
                if not isinstance(data, dict):
                    continue
                if node.get("type") == "agentNode":
                    patch = by_agent.get(str(data.get("agentId") or ""))
                elif node.get("type") == "taskNode":
                    patch = by_task.get(str(data.get("taskId") or ""))
                else:
                    continue
                if not patch:
                    continue
                data.update(patch)
                patched += 1

            if patched:
                # Reassign rather than mutate so SQLAlchemy marks the JSON dirty.
                crew.nodes = nodes
                await self.session.commit()
            return patched
        except Exception as sync_err:  # noqa: BLE001
            logger.warning(
                f"Applied crew fields but could not sync the canvas snapshot "
                f"for crew {crew_id}: {sync_err}"
            )
            return 0

    async def _read_crew_fields(
        self,
        changes: Dict[tuple, Dict[str, str]],
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, str]:
        """Current values of every field about to be written, as a flat
        'agent.<id>.role' -> text map (the same shape as optimized_fields).

        Group-checked through the owning services, matching the write path: an id
        this caller may not update is one it may not snapshot either.
        """
        from src.services.catalog.agents import AgentService
        from src.services.catalog.tasks import TaskService

        agent_service = AgentService(self.session)
        task_service = TaskService(self.session)
        before: Dict[str, str] = {}
        for (entity_kind, entity_id), patch in changes.items():
            service = agent_service if entity_kind == "agent" else task_service
            try:
                entity = await service.get_with_group_check(entity_id, group_context)
            except Exception as read_err:
                logger.warning(
                    f"Could not snapshot {entity_kind} {entity_id} before apply: "
                    f"{read_err}"
                )
                continue
            if entity is None:
                continue
            for field in patch:
                before[f"{entity_kind}.{entity_id}.{field}"] = str(
                    getattr(entity, field, "") or ""
                )
        return before

    async def _write_crew_fields(
        self,
        changes: Dict[tuple, Dict[str, str]],
        group_context: Optional[GroupContext] = None,
    ) -> int:
        """Write per-entity field patches; returns the number of rows updated.

        Through AgentService/TaskService, which OWN agent and task rows. This used
        to call ``AgentRepository.update()`` / ``TaskRepository.update()`` directly
        and so skipped their group verification — and the entity ids here are parsed
        out of a JSON blob on the run (``optimized_fields``), so that check is what
        stands between a foreign id in that blob and a cross-tenant write.

        The service methods also allowlist which fields may change, so a malformed
        key cannot reach ``enabled``, ``tool_configs`` or ``group_id``.
        """
        from src.services.catalog.agents import AgentService
        from src.services.catalog.tasks import TaskService

        agent_service = AgentService(self.session)
        task_service = TaskService(self.session)
        written = 0
        for (entity_kind, entity_id), patch in changes.items():
            if entity_kind == "agent":
                ok = await agent_service.update_prompt_text_with_group_check(
                    entity_id, patch, group_context
                )
            elif entity_kind == "task":
                ok = await task_service.update_prompt_text_with_group_check(
                    entity_id, patch, group_context
                )
            else:
                continue
            if ok:
                written += 1
        return written

    async def revert_run(
        self, run_id: str, group_context: Optional[GroupContext] = None
    ) -> Dict[str, Any]:
        """Restore the before-image an apply recorded, undoing that apply.

        This is the escape hatch for a proposal that scored well and performed
        worse in practice — the measured inversion of crew-level GEPA with team
        size makes that a realistic outcome, not a hypothetical one.
        """
        group_id = group_context.primary_group_id if group_context else None
        row = await self.run_repository.get_by_group(run_id, group_id)
        if row is None:
            raise ValueError(f"Optimization run '{run_id}' not found")
        if not row.applied:
            raise ValueError(
                f"Optimization run '{run_id}' has not been applied — "
                f"there is nothing to revert"
            )
        before: Dict[str, str] = row.before_image or {}
        if not before:
            raise ValueError(
                f"Optimization run '{run_id}' has no before-image (it was "
                f"applied by an older backend that did not record one), so it "
                f"cannot be reverted automatically"
            )

        if row.kind == "crew":
            changes: Dict[tuple, Dict[str, str]] = {}
            for key, value in before.items():
                try:
                    entity_kind, entity_id, field = key.split(".", 2)
                except ValueError:
                    continue
                changes.setdefault((entity_kind, entity_id), {})[field] = value
            restored = await self._write_crew_fields(changes, group_context)
            # Same two-copy problem as apply, mirrored: restoring only the rows
            # would leave the canvas still showing the reverted-away prompts.
            synced = await self._sync_crew_nodes(row.crew_id, changes)
            logger.info(
                f"Crew optimization {run_id} reverted across {restored} entities "
                f"({synced} canvas node(s) synced)"
            )
        else:
            template_service = TemplateService(self.session)
            template_row = await template_service.find_by_name_with_group_check(
                row.target_name, group_context
            )
            if template_row is None:
                raise ValueError(f"Template '{row.target_name}' not found")
            updated = await template_service.update_with_group_check(
                template_row.id,
                PromptTemplateUpdate(template=before.get("template", "")),
                group_context,
            )
            if updated is None:
                raise ValueError(f"Failed to restore template '{row.target_name}'")
            restored = 1

        # The before-image is CONSUMED: it described the state at apply time,
        # and keeping it would let a second revert overwrite fresh edits with
        # a stale snapshot.
        await self.run_repository.update_fields(
            run_id,
            {
                "applied": False,
                "applied_at": None,
                "applied_by": None,
                "before_image": None,
            },
        )
        run = _RUNS.get(run_id)
        if run is not None:
            run["applied"] = False
        return {
            "run_id": run_id,
            "template_name": row.target_name,
            "applied": False,
            "reverted": True,
            "restored": restored,
        }
