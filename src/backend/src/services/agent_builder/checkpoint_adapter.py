"""Crew checkpointing: a unit is a task.

Runs inside the crew subprocess. Listens on the run's event bus and persists
every completed task's output; if the subprocess crashes or is killed mid-run,
the resume path restores the completed prefix and continues from the first
incomplete task.

Task identity is POSITION in the crew's task list, which is what makes writes
idempotent — recording task 3 twice overwrites rather than appends. The task's
content-addressed ``key`` rides along as ``identity`` so the runtime can refuse
to restore a checkpoint whose inputs have since changed.
"""

import logging
from typing import Any, Dict, Iterable, Tuple

from src.core.events.types import CrewKickoffCompletedEvent, TaskCompletedEvent
from src.services.execution.checkpointing.record import KIND_CREW, build_unit
from src.services.execution.checkpointing.recorder import CheckpointRecorder

logger = logging.getLogger(__name__)


class CrewTaskCheckpointRecorder(CheckpointRecorder):
    """Persists per-task completion checkpoints for one crew execution."""

    kind = KIND_CREW

    def __init__(self, job_id: str, crew: Any):
        process = getattr(getattr(crew, "process", None), "value", None)
        super().__init__(
            job_id,
            unit_count=len(crew.tasks),
            meta={"process": process},
        )
        self._crew = crew
        # Task identity within the crew = position in the task list.
        self._index_by_task = {id(task): i for i, task in enumerate(crew.tasks)}

    def _subscriptions(self) -> Iterable[Tuple[type, Any]]:
        return (
            (TaskCompletedEvent, self._on_task_completed),
            (CrewKickoffCompletedEvent, self._on_crew_completed),
        )

    # ------------------------- event handlers -------------------------

    def _on_task_completed(self, source: Any, event: TaskCompletedEvent) -> None:
        try:
            task = event.task if event.task is not None else source
            index = self._index_by_task.get(id(task))
            if index is None:
                return  # not one of this crew's top-level tasks
            self._persist(self._build_unit(index, task, event.output))
        except Exception as e:  # noqa: BLE001 — checkpointing must never break a run
            logger.warning(
                f"[CHECKPOINT] Failed to record task completion for "
                f"{self._job_id} (non-fatal): {e}"
            )

    def _on_crew_completed(self, source: Any, event: CrewKickoffCompletedEvent) -> None:
        # A flow runs several crews on the same bus; only this crew's
        # completion means this execution is done.
        if source is not None and source is not self._crew:
            return

        # The checkpoint is KEPT, not cleared.
        #
        # It was cleared here originally, on the reasoning that a checkpoint is
        # crash recovery and a run that finished has nothing to recover. That is
        # true of a crash, and wrong about how crews are actually worked on: you
        # run one, like the first three tasks, change the fourth, and want to
        # re-run from there without redoing the first three. Discarding the
        # checkpoint the moment a run succeeded made exactly that impossible,
        # and made a successful run look like it had no checkpoint at all.
        #
        # Task identity (``Task.key``) means a restored task is verified before
        # it is reused, so keeping it cannot silently replay stale work.
        logger.info(
            f"[CHECKPOINT] Crew {self._job_id} completed; checkpoint retained "
            f"for re-runs"
        )

    # ------------------------- unit construction -------------------------

    def _build_unit(self, index: int, task: Any, output: Any) -> Dict[str, Any]:
        return build_unit(
            key=index,
            name=getattr(task, "name", None),
            output_raw=getattr(output, "raw", None) or "",
            output_json=getattr(output, "json_dict", None),
            agent=getattr(output, "agent", None),
            summary=getattr(output, "summary", None),
            # Content-addressed task identity: the runtime refuses to restore a
            # unit whose key no longer matches, so changed inputs re-run rather
            # than resuming against stale context.
            identity=getattr(task, "key", None),
        )
