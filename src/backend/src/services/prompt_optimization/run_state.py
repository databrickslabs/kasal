"""The optimization-run registry: shared in-process state and its DB mirror.

``_RUNS`` is a PROCESS-WIDE cache of in-flight runs — it holds the asyncio task
handle, the cancel flag, and the live counters the GEPA worker thread mutates
without a DB round trip. It must be ONE object: the service, the run-registry
mixin and the crew runner all reach for it, and a second copy would silently
split live runs from the code that cancels and reports them.

Durable state lives in the ``prompt_optimization_runs`` table; the helpers here
are the mapping between the two."""

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
from src.repositories.log_repository import LLMLogRepository
from src.repositories.model_config_repository import ModelConfigRepository
from src.repositories.prompt_optimization_run_repository import (
    PromptOptimizationRunRepository,
)

logger = logging.getLogger(__name__)


# A pending/running row whose heartbeat is older than this was orphaned by a
# backend restart (a live run bumps updated_at every HEARTBEAT_SECONDS).
RUN_HEARTBEAT_SECONDS = 30


RUN_STALE_SECONDS = 300


# In-process cache for IN-FLIGHT runs only — the durable record is the
# `prompt_optimization_runs` row. This dict holds what cannot live in the DB
# (the asyncio task handle, the cancel flag) plus the progress counters the
# worker thread bumps on every crew execution; a heartbeat flushes those to
# the row. Reads merge the two: the row is the truth, memory is fresher for a
# live run's counters.
_RUNS: Dict[str, Dict[str, Any]] = {}


_MAX_KEPT_RUNS = 50


_PUBLIC_FIELDS = (
    "run_id",
    "template_name",
    "status",
    "dataset_size",
    "model",
    "judge_model",
    "reflection_model",
    "initial_score",
    "final_score",
    "baseline_template",
    "optimized_template",
    "error",
    "applied",
    "applied_at",
    "applied_by",
    "revertible",
    "created_at",
    "kind",
    "crew_id",
    "baseline_fields",
    "optimized_fields",
    "executions_used",
    "execution_cap",
    "human_feedback_count",
    "candidates_tried",
)


# Progress counters the worker thread owns in memory; flushed by the heartbeat
# and read back over the row for live runs.
_LIVE_COUNTERS = (
    "executions_used",
    "candidates_tried",
    "human_feedback_count",
)


# run-dict key -> DB column. The run dict keeps the API's names (`run_id`,
# `template_name`); the row uses `id`/`target_name`.
_RUN_COLUMNS = {
    "template_name": "target_name",
    "kind": "kind",
    "crew_id": "crew_id",
    "status": "status",
    "error": "error",
    "model": "model",
    "judge_model": "judge_model",
    "reflection_model": "reflection_model",
    "budget": "budget",
    "dataset_size": "dataset_size",
    "executions_used": "executions_used",
    "execution_cap": "execution_cap",
    "candidates_tried": "candidates_tried",
    "human_feedback_count": "human_feedback_count",
    "initial_score": "initial_score",
    "final_score": "final_score",
    "baseline_template": "baseline_template",
    "optimized_template": "optimized_template",
    "baseline_fields": "baseline_fields",
    "optimized_fields": "optimized_fields",
    "before_image": "before_image",
    "applied": "applied",
    "applied_at": "applied_at",
    "applied_by": "applied_by",
}


def _row_to_public(row: Any) -> Dict[str, Any]:
    """Project a run row onto the API's field names (see _PUBLIC_FIELDS)."""
    before = getattr(row, "before_image", None)
    return {
        "run_id": row.id,
        "template_name": row.target_name,
        "kind": row.kind,
        "crew_id": row.crew_id,
        "status": row.status,
        "error": row.error,
        "dataset_size": row.dataset_size or 0,
        "model": row.model,
        "judge_model": row.judge_model,
        "reflection_model": row.reflection_model,
        "initial_score": row.initial_score,
        "final_score": row.final_score,
        "baseline_template": row.baseline_template,
        "optimized_template": row.optimized_template,
        "baseline_fields": row.baseline_fields,
        "optimized_fields": row.optimized_fields,
        "executions_used": row.executions_used,
        "execution_cap": row.execution_cap,
        "human_feedback_count": row.human_feedback_count,
        "candidates_tried": row.candidates_tried,
        "applied": bool(row.applied),
        "applied_at": row.applied_at,
        "applied_by": row.applied_by,
        # An apply is undoable only while its before-image is on the row.
        "revertible": bool(row.applied and before),
        "created_at": row.created_at,
    }


def _run_to_columns(run: Dict[str, Any]) -> Dict[str, Any]:
    """Column values for the keys `run` actually carries."""
    return {column: run[key] for key, column in _RUN_COLUMNS.items() if key in run}


async def _persist_run_changes(run_id: str, changes: Dict[str, Any]) -> None:
    """Patch a run row from BACKGROUND work, on its OWN session.

    Background tasks must never touch the request session (it is closed when
    the request returns), so this opens one per write and commits it. Failures
    are logged, never raised: losing a status write must not kill an
    optimization that is otherwise fine.

    Routed through ``get_smart_db_session`` — the SAME router the INSERT takes.
    The row is created on the request session (``SessionDep`` →
    ``get_smart_db_session``), which re-reads ``is_lakebase_enabled()`` per call.
    The raw ``async_session_factory`` used here before is a SNAPSHOT instead: it
    points at Lakebase only if ``activate_lakebase()`` ran in THIS process, which
    happens at BOOT (``main.py`` lifespan) or in a subprocess — never on a
    runtime ``/lakebase/enable``. Enabling Lakebase without a restart therefore
    split this one table: INSERTs landed in Lakebase while these status UPDATEs
    went to the local DB, so a run showed up and then never progressed.
    """
    if not changes:
        return
    try:
        from src.db.database_router import get_smart_db_session

        # get_smart_db_session is an async generator (FastAPI DI shape); driving
        # it with `async for` is how the other non-request callers use it. It
        # commits the local-DB branch itself, but the Lakebase branch commits in
        # get_lakebase_session, so commit here to cover both.
        async for session in get_smart_db_session():
            repo = PromptOptimizationRunRepository(session)
            await repo.update_fields(run_id, changes)
            await session.commit()
    except Exception as persist_err:
        logger.warning(
            f"Could not persist prompt optimization run {run_id}: {persist_err}"
        )
