"""
Crew task checkpoint recorder for crash-resumable crew executions.

Runs inside the crew subprocess: listens on the engine event bus and persists
every completed task's output into ExecutionHistory.checkpoint_data
(["crew_task_checkpoint"]), keyed by task index. If the subprocess crashes or
is killed mid-run, POST /executions/{id}/resume re-launches the crew with this
data threaded through to Crew.kickoff(from_checkpoint=...), which restores the
completed prefix and continues from the first incomplete task.

Every write is fail-open: a checkpoint failure must never fail the run.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.core.events import event_bus
from src.core.events.types import CrewKickoffCompletedEvent, TaskCompletedEvent
from src.services.tools.async_bridge import run_async_with_context

logger = logging.getLogger(__name__)

# Outputs beyond this size are truncated in the checkpoint (a resume with a
# truncated context beats redoing the task, but fidelity is flagged).
_MAX_OUTPUT_CHARS = 500_000


class CrewTaskCheckpointRecorder:
    """Persists per-task completion checkpoints for one crew execution."""

    def __init__(self, job_id: str, crew: Any):
        self._job_id = job_id
        self._crew = crew
        self._task_count = len(crew.tasks)
        self._process = getattr(getattr(crew, "process", None), "value", None)
        # Task identity within the crew = position in the task list.
        self._index_by_task = {id(task): i for i, task in enumerate(crew.tasks)}

    def register(self, event_bus=None) -> "CrewTaskCheckpointRecorder":
        bus = event_bus or event_bus
        bus.register_handler(TaskCompletedEvent, self._on_task_completed)
        bus.register_handler(CrewKickoffCompletedEvent, self._on_crew_completed)
        logger.info(
            f"[CHECKPOINT] Recorder registered for {self._job_id} "
            f"({self._task_count} tasks, process={self._process})"
        )
        return self

    # ------------------------- event handlers -------------------------

    def _on_task_completed(self, source: Any, event: TaskCompletedEvent) -> None:
        try:
            task = event.task if event.task is not None else source
            index = self._index_by_task.get(id(task))
            if index is None:
                return  # not one of this crew's top-level tasks
            entry = self._build_entry(index, task, event.output)
            run_async_with_context(self._persist_entry(entry), timeout=60)
        except Exception as e:  # noqa: BLE001 — checkpointing must never break a run
            logger.warning(
                f"[CHECKPOINT] Failed to record task completion for "
                f"{self._job_id} (non-fatal): {e}"
            )

    def _on_crew_completed(self, source: Any, event: CrewKickoffCompletedEvent) -> None:
        if source is not None and source is not self._crew:
            return
        try:
            run_async_with_context(self._clear_checkpoint(), timeout=60)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"[CHECKPOINT] Failed to clear checkpoint for "
                f"{self._job_id} (non-fatal): {e}"
            )

    # ------------------------- persistence -------------------------

    def _build_entry(self, index: int, task: Any, output: Any) -> Dict[str, Any]:
        raw = getattr(output, "raw", None) or ""
        truncated = len(raw) > _MAX_OUTPUT_CHARS
        json_dict = getattr(output, "json_dict", None)
        if not isinstance(json_dict, dict):
            json_dict = None
        entry: Dict[str, Any] = {
            "index": index,
            "task_key": getattr(task, "key", None),
            "name": getattr(task, "name", None),
            "agent": getattr(output, "agent", None),
            "summary": getattr(output, "summary", None),
            "output_raw": raw[:_MAX_OUTPUT_CHARS],
            "output_json": json_dict,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        if truncated:
            entry["truncated"] = True
        return entry

    async def _persist_entry(self, entry: Dict[str, Any]) -> None:
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )
        from src.utils.asyncio_utils import execute_db_operation_smart

        async def _op(session):
            repo = ExecutionHistoryRepository(session)
            ok = await repo.upsert_crew_task_checkpoint(
                self._job_id,
                entry,
                task_count=self._task_count,
                process=self._process,
            )
            if ok:
                await session.commit()
            return ok

        await execute_db_operation_smart(_op)

    async def _clear_checkpoint(self) -> None:
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )
        from src.utils.asyncio_utils import execute_db_operation_smart

        async def _op(session):
            repo = ExecutionHistoryRepository(session)
            ok = await repo.clear_crew_task_checkpoint(self._job_id)
            if ok:
                await session.commit()
            return ok

        await execute_db_operation_smart(_op)


def build_resume_checkpoint(
    stored: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Convert the stored checkpoint into the engine's from_checkpoint shape.

    Stored shape keys ``completed`` by stringified index (idempotent DB merge);
    the engine expects an ordered list. Returns None when there is nothing to
    resume from.
    """
    if not stored or not isinstance(stored, dict):
        return None
    completed = stored.get("completed")
    if not isinstance(completed, dict) or not completed:
        return None
    try:
        entries = [completed[key] for key in sorted(completed, key=int)]
    except (TypeError, ValueError):
        return None
    return {
        "version": stored.get("version", 1),
        "task_count": stored.get("task_count"),
        "process": stored.get("process"),
        "completed": entries,
    }
