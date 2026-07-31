"""Scheduled memory maintenance — coverage by store size, not run frequency.

``run_memory_maintenance`` is triggered at the end of a run, which means a scope
is only ever tidied as often as somebody happens to run something in it. That is
backwards in both directions: a big, stale store on a rarely-run crew needs
consolidating most and gets it least, while a busy chat workspace re-scans the
same recent records every interval.

This sweep asks the opposite question — which scope has gone longest without
maintenance — and works from there, so every scope is eventually reached. It
also gives the passes that do not belong on a teardown path somewhere to live:
supersession costs an LLM call and forgetting deletes rows, and neither is
something to do while a user waits for a run to finish.

**Everything is best-effort, per scope.** One workspace with an unreachable
backend or missing credentials must not stop the sweep for every other
workspace, so a failure is recorded on that scope's watermark and the loop moves
on.

**The known limitation, stated plainly:** building a ``Memory`` here happens
outside any request, so there is no OBO user token — the embedder and analysis
LLM resolve through the app's service-principal/PAT chain instead. Where that
chain is not configured, ``build_group_memory`` returns ``None`` and the scope is
skipped with ``status="unavailable"`` rather than half-maintained. The two
LLM-free passes (exact dedupe, forgetting) would work without an embedder, but
the merge and supersession passes re-save records and therefore need one, so the
sweep treats an embedder as required rather than silently doing half the job.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# How stale a scope must be before the sweep revisits it.
_DEFAULT_INTERVAL_HOURS = 6.0
# Scopes per tick. Bounded so one tick cannot turn into an unbounded amount of
# work (each scope may cost two LLM calls) — the rest are picked up next tick,
# and the oldest-first ordering guarantees they are not starved.
_DEFAULT_BATCH = 5


def sweep_enabled() -> bool:
    return os.environ.get("KASAL_MEMORY_SWEEP", "true").lower() != "false"


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


async def build_group_memory(group_id: str) -> Any:
    """Build the unified ``Memory`` for ``group_id``, or ``None``.

    Composes the same ``CrewMemoryService`` building blocks the chat and crew
    paths use, so the sweep reads and writes exactly what the runtime does —
    same backend, same scoping, same root scope. The one difference is the
    absence of a user token (see the module docstring).
    """
    try:
        from src.schemas.memory_backend import MemoryBackendConfig
        from src.services.execution.config.crew_config_builder import CrewConfigBuilder
        from src.services.execution.config.embedder_config_builder import (
            EmbedderConfigBuilder,
        )
        from src.services.memory.crew_memory import CrewMemoryService

        config: dict[str, Any] = {
            "group_id": group_id,
            "name": "memory-sweep",
            # No agents/tasks: the crew-id hash is only used for write tagging
            # and this sweep does not write new records under its own identity.
            "agents": [],
            "tasks": [],
        }
        memory_service = CrewMemoryService(config)
        config_builder = CrewConfigBuilder(config)

        backend_config = await memory_service.fetch_memory_backend_config()
        if not backend_config:
            return None
        if config_builder.check_memory_disabled_by_backend_config(backend_config):
            return None

        crew_kwargs: dict[str, Any] = {"agents": [], "memory": True}
        embedder_builder = EmbedderConfigBuilder(config, None)
        crew_kwargs, custom_embedder, _ = await embedder_builder.configure_embedder(
            crew_kwargs
        )

        crew_id = memory_service.generate_crew_id()
        memory_service.setup_storage_directory(crew_id, backend_config)
        backend_type = backend_config.get("backend_type")
        embedder = (
            custom_embedder
            if backend_type in ("databricks", "lakebase")
            else crew_kwargs.get("embedder")
        )
        storage = await memory_service.create_unified_storage(
            backend_config, crew_id, embedder
        )
        memory_config = MemoryBackendConfig(**backend_config)
        llm_override = await memory_service.resolve_memory_llm_override(memory_config)
        memory_service.configure_crew_memory_components(
            crew_kwargs,
            memory_config,
            storage,
            crew_id,
            custom_embedder,
            memory_llm_override=llm_override,
        )
        memory = crew_kwargs.get("memory")
        return memory if memory not in (None, True, False) else None
    except Exception as exc:  # noqa: BLE001 — one scope must not break the sweep
        logger.warning(
            "[memory-sweep] could not build memory for %s: %s", group_id, exc
        )
        return None


async def sweep_memory_maintenance() -> dict[str, int]:
    """One tick: maintain the scopes that have gone longest without it.

    Returns ``{"scopes": n, "maintained": n, "skipped": n}``. Never raises — this
    runs on a background loop and a failed tick must not take the loop down.
    """
    result = {"scopes": 0, "maintained": 0, "skipped": 0}
    if not sweep_enabled():
        return result

    interval_hours = _float_env(
        "KASAL_MEMORY_SWEEP_INTERVAL_HOURS", _DEFAULT_INTERVAL_HOURS
    )
    batch = _int_env("KASAL_MEMORY_SWEEP_BATCH", _DEFAULT_BATCH)

    try:
        from src.db.session import get_isolated_db_session
        from src.repositories.memory_maintenance_repository import (
            MemoryMaintenanceRepository,
        )

        async with get_isolated_db_session() as session:
            repository = MemoryMaintenanceRepository(session)
            due = await repository.due_groups(interval_hours, batch)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[memory-sweep] could not read watermarks: %s", exc)
        return result

    result["scopes"] = len(due)
    for group_id in due:
        status, stats, error = await _maintain_group(group_id)
        if status == "ok":
            result["maintained"] += 1
        else:
            result["skipped"] += 1
        try:
            async with get_isolated_db_session() as session:
                repository = MemoryMaintenanceRepository(session)
                await repository.record_result(
                    group_id, status=status, stats=stats, error=error
                )
                await session.commit()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[memory-sweep] could not record watermark for %s: %s", group_id, exc
            )

    if result["maintained"] or result["skipped"]:
        logger.info(
            "[memory-sweep] %d scope(s) maintained, %d skipped",
            result["maintained"],
            result["skipped"],
        )
    return result


async def _maintain_group(group_id: str) -> tuple[str, dict, str | None]:
    """Run the full maintenance pass for one scope. Never raises."""
    import asyncio

    memory = await build_group_memory(group_id)
    if memory is None:
        return "unavailable", {}, "no usable memory backend or embedder"
    try:
        from src.services.memory.maintenance import run_memory_maintenance

        # The passes are synchronous (storage backends bridge to async
        # internally), so keep them off this loop.
        stats = await asyncio.to_thread(run_memory_maintenance, memory)
        return "ok", dict(stats), None
    except Exception as exc:  # noqa: BLE001
        logger.warning("[memory-sweep] maintenance failed for %s: %s", group_id, exc)
        return "error", {}, str(exc)
