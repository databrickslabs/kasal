"""The event-trigger queue consumer.

Claims due rows from ``triggerqueue`` and dispatches each as a crew/flow run
through the SAME path the scheduler uses (``ExecutionService.create_run_record``
+ ``run_crew_execution``), tagged ``trigger_type="lakebase_queue"``.

Design notes:
- **Non-blocking.** ``claim_and_dispatch`` claims a batch, commits the ``claimed``
  state, then launches one background task per row. It never awaits a whole run
  (runs take minutes), so a slow run can't stall the queue. A crash between
  ``claimed`` and ``dispatched`` is recovered by ``reclaim`` (stuck-row sweep).
- **Session discipline.** Every DB touch goes through ``routed_scoped_session()``
  (reaches Lakebase when enabled); the service never opens a raw session.
- **Tenancy.** Each row's ``group_id`` becomes the run's ``GroupContext``.

Targets: ``kind="flow"`` (saved flow by id), ``kind="crew"`` (saved crew by id,
resolved through the catalog), and ``kind="inline"`` (full config).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.core.logger import LoggerManager
from src.db.session import routed_scoped_session
from src.repositories.trigger_queue_repository import TriggerQueueRepository
from src.schemas.execution import CrewConfig
from src.utils.user_context import GroupContext

logger = LoggerManager.get_instance().system

MAX_ATTEMPTS = 5
TRIGGER_TYPE = "lakebase_queue"


def _backoff_at(attempts: int) -> datetime:
    """Exponential backoff visibility time, capped at 5 minutes."""
    return datetime.utcnow() + timedelta(seconds=min(300, (2 ** max(0, attempts)) * 10))


class TriggerQueueConsumerService:
    """Stateless background worker; acquires a routed session per operation."""

    def __init__(self) -> None:
        # Keep strong refs to in-flight dispatch tasks so they are not GC'd.
        self._tasks: set[asyncio.Task] = set()

    # ------------------------------------------------------------------ claim
    async def claim_and_dispatch(
        self, batch: int = 5, group_ids: Optional[List[str]] = None
    ) -> List[asyncio.Task]:
        """Claim up to ``batch`` due rows and launch a dispatch task for each.

        Returns the launched tasks (the loop ignores them — non-blocking; tests
        await them). The ``claimed`` state is committed before dispatch so other
        replicas skip these rows. ``group_ids`` scopes the claim (the on-demand
        API drain claims only the caller's tenants); None (the background loop)
        drains every tenant.
        """
        snapshots: List[Dict[str, Any]] = []
        async with routed_scoped_session() as session:
            rows = await TriggerQueueRepository(session).claim(
                batch, group_ids=group_ids
            )
            # Snapshot BEFORE commit — commit expires ORM attributes, and a lazy
            # refresh on a closing session is a MissingGreenlet trap.
            for row in rows:
                event = (row.payload or {}).get("event") or {}
                snapshots.append(
                    {
                        "id": row.id,
                        "group_id": row.group_id,
                        "target": row.target or {},
                        "payload": row.payload or {},
                        "event_type": row.event_type,
                        "correlation_id": row.correlation_id,
                        "causation_run_id": row.causation_run_id,
                        "attempts": row.attempts or 0,
                        # Chain depth so far — threaded into the run's inputs and
                        # read back by emit-on-completion to cap runaway loops.
                        "hops": int(event.get("hops") or 0),
                    }
                )
            await session.commit()

        tasks: List[asyncio.Task] = []
        for snap in snapshots:
            task = asyncio.create_task(self._dispatch(snap))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            tasks.append(task)
        return tasks

    async def reclaim(self, stuck_after_seconds: int = 900) -> int:
        """Return rows stuck in ``claimed`` (crashed worker) to ``pending``."""
        cutoff = datetime.utcnow() - timedelta(seconds=stuck_after_seconds)
        async with routed_scoped_session() as session:
            n = await TriggerQueueRepository(session).reclaim_stuck(cutoff)
            await session.commit()
        if n:
            logger.info("[TriggerQueue] reclaimed %d stuck row(s)", n)
        return n

    async def purge(self, older_than_days: int = 7) -> int:
        """Retention sweep: drop finished rows (``dispatched``/``dead``) older
        than the window. Without this the queue table grows forever."""
        cutoff = datetime.utcnow() - timedelta(days=older_than_days)
        async with routed_scoped_session() as session:
            n = await TriggerQueueRepository(session).purge_finished(cutoff)
            await session.commit()
        if n:
            logger.info("[TriggerQueue] purged %d finished row(s)", n)
        return n

    # --------------------------------------------------------------- dispatch
    async def _dispatch(self, snap: Dict[str, Any]) -> None:
        """Turn one claimed row into a launched crew/flow run."""
        row_id = snap["id"]
        try:
            # A webhook target is a DELIVERY, not a run: POST the event to the
            # subscriber's endpoint and mark the row dispatched. Failures ride
            # the queue's normal machinery (backoff retries, dead-letter).
            if ((snap.get("target") or {}).get("kind") or "").lower() == "webhook":
                await self._deliver_webhook(snap)
                return
            job_id = str(uuid.uuid4())
            group_id = snap.get("group_id")
            group_context = GroupContext(
                group_ids=[group_id] if group_id else [],
                group_email=None,
            )
            await self._load_databricks_auth()

            from src.services.execution.service import ExecutionService

            async with routed_scoped_session() as session:
                # Config is built inside the session because a crew target is
                # resolved from its saved id (needs a DB read).
                config, execution_type = await self._build_config(
                    session, snap["target"], snap["payload"], group_context
                )
                self._inject_event_context(config, execution_type)
                run_name = await self._resolve_run_name(
                    session, snap["target"], execution_type, job_id
                )

                config_dict: Dict[str, Any] = {
                    "execution_type": execution_type,
                    "inputs": config.inputs or {},
                    "model": config.model,
                    "trigger": TRIGGER_TYPE,
                    "correlation_id": snap.get("correlation_id"),
                    # Read back by the emit hook when THIS run completes, so a
                    # chain of hand-offs carries its depth and can be capped.
                    "trigger_hops": snap.get("hops", 0),
                }
                if execution_type == "flow" and config.flow_id:
                    config_dict["flow_id"] = str(config.flow_id)
                if config.agents_yaml:
                    config_dict["agents_yaml"] = config.agents_yaml
                if config.tasks_yaml:
                    config_dict["tasks_yaml"] = config.tasks_yaml

                await ExecutionService.create_run_record(
                    session,
                    job_id=job_id,
                    run_name=run_name,
                    inputs=config_dict,
                    execution_type=execution_type,
                    group_id=group_id,
                    flow_id=config.flow_id if execution_type == "flow" else None,
                    crew_id=(
                        getattr(config, "crew_id", None)
                        if execution_type == "crew"
                        else None
                    ),
                    trigger_type=TRIGGER_TYPE,
                    commit=True,
                )
                # Announce the run over SSE. The UI did not start this run, so
                # this event is its FIRST sight of it — without the name here it
                # renders a "Run <id>" placeholder forever.
                from src.services.execution.status import ExecutionStatusService

                await ExecutionStatusService.broadcast_execution_created(
                    {
                        "job_id": job_id,
                        "status": "PENDING",
                        "run_name": run_name,
                        "execution_type": execution_type,
                        "group_id": group_id,
                    }
                )
                await ExecutionService.run_crew_execution(
                    execution_id=job_id,
                    config=config,
                    execution_type=execution_type,
                    group_context=group_context,
                    session=session,
                )
                await TriggerQueueRepository(session).mark_dispatched(row_id)
                await session.commit()

            logger.info(
                "[TriggerQueue] dispatched row %s -> %s run %s",
                row_id,
                execution_type,
                job_id,
            )
        except Exception as exc:  # noqa: BLE001 — one bad row must not kill the loop
            await self._handle_failure(
                row_id,
                snap.get("attempts", 0),
                exc,
                # A validation failure (unknown kind, missing/deleted target) is
                # not transient — retrying it five times over ten minutes of
                # backoff cannot fix it. Straight to the dead letter.
                permanent=isinstance(exc, ValueError),
            )

    async def _handle_failure(
        self, row_id: int, attempts: int, exc: Exception, permanent: bool = False
    ) -> None:
        """Requeue with backoff, or dead-letter (attempts exhausted / permanent)."""
        message = f"{type(exc).__name__}: {exc}"
        dead = permanent or attempts >= MAX_ATTEMPTS
        try:
            async with routed_scoped_session() as session:
                repo = TriggerQueueRepository(session)
                if dead:
                    await repo.mark_failed(row_id, message, dead=True)
                else:
                    await repo.requeue(row_id, _backoff_at(attempts), error=message)
                await session.commit()
        except Exception as book_exc:  # noqa: BLE001
            logger.error(
                "[TriggerQueue] could not record failure for row %s: %s",
                row_id,
                book_exc,
            )
        level = logger.error if dead else logger.warning
        level(
            "[TriggerQueue] row %s failed (attempt %d%s): %s",
            row_id,
            attempts,
            " — dead-lettered" if dead else "",
            message,
        )

    # --------------------------------------------------------------- webhook
    async def _deliver_webhook(self, snap: Dict[str, Any]) -> None:
        """POST one claimed event to its webhook target (server-to-server).

        2xx marks the row dispatched. A non-2xx response or a network error
        raises, so the normal failure path requeues with backoff and
        dead-letters after MAX_ATTEMPTS. A malformed URL is a permanent
        failure (ValueError -> straight to the dead letter).
        """
        target = snap.get("target") or {}
        url = (target.get("url") or "").strip()
        if not url.lower().startswith(("http://", "https://")):
            raise ValueError(f"webhook target requires an http(s) 'url', got {url!r}")
        # SECURITY (SSRF): the URL is tenant-supplied. Same guard as the HITL
        # webhooks and A2A push configs -- https only, no loopback/private/
        # link-local/metadata targets, re-checked after DNS resolution.
        # UnsafeUrlError is a ValueError, so a blocked URL dead-letters
        # immediately instead of burning retries. Local dev can opt out to
        # deliver to a localhost receiver.
        _allow_private = os.getenv(
            "KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS", ""
        ).lower() in ("1", "true", "yes")
        if not _allow_private:
            from src.utils.url_security import assert_safe_outbound_url

            await assert_safe_outbound_url(url)
        payload = snap.get("payload") or {}
        body = {
            "event_type": snap.get("event_type"),
            "event": payload.get("event"),
            "inputs": payload.get("inputs"),
            "correlation_id": snap.get("correlation_id"),
            "causation_run_id": snap.get("causation_run_id"),
        }
        headers = {
            "Content-Type": "application/json",
            # Receivers dedupe on this: at-least-once delivery means a crash
            # between POST and mark_dispatched re-delivers the same row id.
            "X-Kasal-Delivery": str(snap["id"]),
            "X-Kasal-Event": str(snap.get("event_type") or ""),
        }
        status = await self._post_webhook(url, body, headers)
        # 3xx counts as delivered: the endpoint RECEIVED the POST and answered
        # with a redirect we deliberately do not follow (no SSRF re-entry).
        # Google Apps Script web apps -- the common no-code Sheets/Docs
        # receiver -- always respond 302 after running doPost, so treating a
        # redirect as failure would retry (and re-run) successful deliveries.
        if not 200 <= status < 400:
            raise RuntimeError(f"webhook endpoint returned HTTP {status}")

        async with routed_scoped_session() as session:
            await TriggerQueueRepository(session).mark_dispatched(snap["id"])
            await session.commit()
        logger.info("[TriggerQueue] delivered row %s to webhook (%s)", snap["id"], url)

    @staticmethod
    async def _post_webhook(
        url: str, body: Dict[str, Any], headers: Dict[str, str]
    ) -> int:
        """One POST, bounded: 15s total, no redirects (a redirect target was
        not what the subscription vetted). Returns the HTTP status."""
        import aiohttp

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            async with http.post(
                url, json=body, headers=headers, allow_redirects=False
            ) as resp:
                await resp.read()
                return resp.status

    # --------------------------------------------------------------- helpers
    _PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")

    def _inject_event_context(self, config: CrewConfig, execution_type: str) -> None:
        """Make unreferenced event inputs VISIBLE to the subscriber crew.

        Inputs are template variables: ``{key}`` in a task's description or
        expected output is interpolated by the runtime, and anything the
        templates never reference silently vanishes — it reaches the run's
        inputs but no prompt. A subscriber crew is usually authored without
        knowledge of its producer, so the hand-off payload would be dropped on
        the floor. Append the unreferenced keys to the FIRST task's description
        as an explicit context block instead; later tasks see it through normal
        task-output chaining. A task that DOES reference ``{payload}`` (or any
        event key) keeps full control — nothing is appended for that key.
        """
        if execution_type != "crew" or not config.tasks_yaml or not config.inputs:
            return
        referenced: set = set()
        for task in config.tasks_yaml.values():
            if isinstance(task, dict):
                for field in ("description", "expected_output"):
                    referenced.update(
                        self._PLACEHOLDER_RE.findall(str(task.get(field) or ""))
                    )
        leftover = {k: v for k, v in config.inputs.items() if k not in referenced}
        if not leftover:
            return
        first = next(iter(config.tasks_yaml))
        task = config.tasks_yaml[first]
        if not isinstance(task, dict):
            return
        lines = "\n".join(
            f"- {k}: {json.dumps(v) if isinstance(v, (dict, list)) else v}"
            for k, v in leftover.items()
        )
        task["description"] = (
            str(task.get("description") or "")
            + "\n\nContext from the triggering event:\n"
            + lines
        )

    async def _resolve_run_name(
        self,
        session: Any,
        target: Dict[str, Any],
        execution_type: str,
        job_id: str,
    ) -> str:
        """Name the run after the saved crew/flow, so it reads like any other run
        in the history rather than an opaque ``Event crew <id>``.

        Resolved through the owning domain's SERVICE (catalog crew / flow_builder
        flow), never their repositories. Best-effort — falls back to a generic
        label if the definition is gone or the target is inline.
        """
        import uuid

        tid = (target or {}).get("id")
        try:
            if execution_type == "crew" and tid:
                from src.services.catalog.crews import CrewService

                crew = await CrewService(session).get(uuid.UUID(str(tid)))
                if crew and getattr(crew, "name", None):
                    return crew.name
            elif execution_type == "flow" and tid:
                from src.services.flow_builder.flow_service import FlowService

                flow = await FlowService(session).get_flow(uuid.UUID(str(tid)))
                if flow and getattr(flow, "name", None):
                    return flow.name
        except Exception:  # noqa: BLE001 — naming must never block dispatch
            pass
        return f"Event {execution_type} {job_id[:8]}"

    async def _build_config(
        self,
        session: Any,
        target: Dict[str, Any],
        payload: Dict[str, Any],
        group_context: GroupContext,
    ) -> Tuple[CrewConfig, str]:
        """Message target → (CrewConfig, execution_type).

        A ``crew`` target is resolved from its SAVED id via the catalog domain's
        ``build_crew_execution_config_by_id`` — it projects the crew into the
        agents_yaml/tasks_yaml the engine takes (the same shape the canvas posts).
        A ``flow`` runs from its id directly; ``inline`` carries a full config.
        """
        kind = (target or {}).get("kind")
        inputs = (payload or {}).get("inputs") or {}
        harness = (target or {}).get("harness")

        if kind == "flow":
            target_id = target.get("id")
            if not target_id:
                raise ValueError("flow target requires an 'id'")
            return (
                CrewConfig(
                    execution_type="flow",
                    flow_id=str(target_id),
                    inputs=inputs,
                    harness=harness,
                ),
                "flow",
            )

        if kind == "crew":
            crew_id = target.get("id")
            if not crew_id:
                raise ValueError("crew target requires an 'id'")
            from src.services.catalog.crew_config import (
                build_crew_execution_config_by_id,
            )

            resolved = await build_crew_execution_config_by_id(
                session, crew_id, group_context
            )
            if not resolved:
                raise ValueError(f"crew {crew_id!r} not found or unreadable")
            agents_yaml, tasks_yaml = resolved
            return (
                CrewConfig(
                    execution_type="crew",
                    agents_yaml=agents_yaml,
                    tasks_yaml=tasks_yaml,
                    inputs=inputs,
                    harness=harness,
                    crew_id=str(crew_id),
                ),
                "crew",
            )

        if kind == "inline":
            cfg = dict(target.get("config") or {})
            cfg.setdefault("inputs", inputs)
            if harness and not cfg.get("harness"):
                cfg["harness"] = harness
            config = CrewConfig(**cfg)
            return config, (config.execution_type or "crew")

        raise ValueError(f"unknown target kind: {kind!r}")

    async def _load_databricks_auth(self) -> None:
        """Best-effort: put Databricks host/token in env for the launched run.

        Mirrors the scheduler (``run_schedule_job``) so a queue-triggered run
        authenticates the same way a scheduled one does. Best-effort by design —
        the subprocess re-resolves auth; failures here must not abort dispatch.
        """
        import os

        try:
            from src.utils.databricks_auth import get_auth_context

            auth = await get_auth_context()
        except Exception:  # noqa: BLE001
            return
        if not auth:
            return
        if getattr(auth, "workspace_url", None):
            os.environ["DATABRICKS_HOST"] = auth.workspace_url
        if getattr(auth, "token", None):
            os.environ["DATABRICKS_TOKEN"] = auth.token
            os.environ["DATABRICKS_API_KEY"] = auth.token
