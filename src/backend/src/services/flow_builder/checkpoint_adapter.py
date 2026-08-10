"""Flow checkpointing: a unit is a crew.

Runs inside the flow subprocess. Listens for each crew finishing and persists
its output, so a flow that dies halfway through its method graph can be resumed
from the last crew that completed.

**This replaces deriving checkpoints from traces.** The flow path used to
reconstruct its checkpoint list by querying ``execution_trace`` for
``task_completed`` events. That was free but fragile: traces are TELEMETRY —
they can be sampled, truncated, retention-pruned or reshaped for reasons that
have nothing to do with resume, and a resume that silently degrades when a
trace row is missing is worse than one that reports "no checkpoint". The
derived reader survives as a read-only fallback for executions that predate
this recorder; nothing writes through it.

Sequence is assigned in COMPLETION order, which is what the trace-derived
reader did and what the flow's ``resume_from_crew_sequence`` already means. It
is 1-based for the same reason.
"""

import logging
from typing import Any, Iterable, Optional, Tuple

from src.core.events.types import CrewKickoffCompletedEvent
from src.services.execution.checkpointing.record import KIND_FLOW, build_unit
from src.services.execution.checkpointing.recorder import CheckpointRecorder
from src.services.execution.runtime.identity import crew_content_key
from src.services.flow_builder.checkpoint_identity import compute_crew_identity

logger = logging.getLogger(__name__)


class FlowCrewCheckpointRecorder(CheckpointRecorder):
    """Persists per-crew completion checkpoints for one flow execution."""

    kind = KIND_FLOW

    def __init__(
        self,
        job_id: str,
        crew_count: Optional[int] = None,
        flow_uuid: Optional[str] = None,
    ):
        # The flow's mid-graph method state stays in its own table with its own
        # lifecycle (flow_states); the checkpoint only REFERENCES it. A crew has
        # no analogue, and that is a property of crews rather than a gap.
        meta = {"flow_state_ref": {"flow_uuid": flow_uuid}} if flow_uuid else None
        super().__init__(job_id, unit_count=crew_count, meta=meta)
        self._sequence = 0
        self._seen_crews = set()

    def _subscriptions(self) -> Iterable[Tuple[type, Any]]:
        return ((CrewKickoffCompletedEvent, self._on_crew_completed),)

    def _on_crew_completed(self, source: Any, event: CrewKickoffCompletedEvent) -> None:
        try:
            crew_name = getattr(event, "crew_name", None) or getattr(
                source, "name", None
            )
            if not crew_name:
                # Without a name the unit cannot be matched back to a crew on
                # resume, so recording it would produce a checkpoint that
                # restores into nothing.
                logger.warning(
                    f"[CHECKPOINT] Crew completed with no name for "
                    f"{self._job_id} — not checkpointed"
                )
                return

            # First completion wins, matching the derived reader it replaces: a
            # crew that runs twice in one flow is one checkpoint boundary, and
            # re-keying it would renumber every later sequence.
            if crew_name in self._seen_crews:
                return
            self._seen_crews.add(crew_name)
            self._sequence += 1

            output = getattr(event, "output", None)
            # A CONTENT hash, not the name: renaming is not the interesting
            # change, rewriting what the crew does is. Computed from the live
            # crew's tasks so resume can refuse to replay this output once the
            # crew has been edited.
            identity = compute_crew_identity(crew_name, getattr(source, "tasks", None))
            if identity is None:
                logger.info(
                    f"[CHECKPOINT] Crew '{crew_name}' has no computable identity; "
                    f"a resume will replay its output without verifying it"
                )
            self._persist(
                build_unit(
                    key=self._sequence,
                    name=crew_name,
                    output_raw=getattr(output, "raw", None) or "",
                    output_json=getattr(output, "json_dict", None),
                    identity=identity,
                    # The text half, recomputable from the saved flow by a
                    # reader that is not running anything.
                    content_key=crew_content_key(
                        crew_name, getattr(source, "tasks", None)
                    ),
                )
            )
        except Exception as e:  # noqa: BLE001 — checkpointing must never break a run
            logger.warning(
                f"[CHECKPOINT] Failed to record crew completion for "
                f"{self._job_id} (non-fatal): {e}"
            )

    def finish(self) -> None:
        """Called once the flow returns. Deliberately KEEPS the checkpoint.

        A crew clears its checkpoint on success, because a crew checkpoint is
        crash recovery and a run that finished has nothing to recover.

        **A flow checkpoint is not crash recovery, it is an iteration point.**
        The whole reason to keep one after a successful run is to re-run the
        flow from the middle after changing a downstream crew, reusing the
        upstream results you were happy with. The flow path has always worked
        this way — ``flow_runner_service`` marks the checkpoint active with
        ``checkpoint_method="flow_complete"`` on a SUCCESSFUL completion, on
        purpose.

        Clearing here would silently delete that, and the failure mode is
        nasty: the flow finishes, its checkpoint vanishes, and the next resume
        re-runs everything while looking like it worked.

        Kept as a hook rather than deleted so the subprocess's completion path
        stays explicit about the choice.
        """
        logger.info(
            f"[CHECKPOINT] Flow {self._job_id} completed with "
            f"{self._sequence} crew checkpoint(s) retained for re-runs"
        )
