"""Loading the outputs a resumed flow restores its completed crews from.

Two sources, in order:

1. The **written checkpoint** on the source execution — recorded as each crew
   completed by ``FlowCrewCheckpointRecorder``.
2. The **trace-derived** reconstruction, for executions that predate that
   recorder. This is LEGACY: it reads ``execution_trace``, which is telemetry
   and can be sampled, truncated or retention-pruned for reasons that have
   nothing to do with resume. It cannot be backfilled — those runs never wrote
   a checkpoint and never will — so it stays until pre-unification executions
   have aged out, and callers are told which source they got.

Extracted from ``modules/flow_builder.py`` rather than edited in place: that
file is well over the size ceiling, and this is a self-contained seam.
"""

import logging
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


async def load_resume_outputs(
    resume_from_execution_id: Optional[str],
    repositories: Optional[Dict[str, Any]],
    from_unit: Optional[Any] = None,
) -> Tuple[Dict[str, Any], bool, Dict[str, Any]]:
    """Load ``{crew_name: output}`` for a flow resuming from an earlier run.

    Args:
        resume_from_execution_id: The SOURCE run's job_id (a UUID string, not
            the integer primary key — the parameter is named for the API field
            it arrives in).
        repositories: The subprocess's repository map.
        from_unit: Optional crew sequence to resume AT; everything before it is
            restored.

    Returns:
        ``(outputs, derived, identities)``.

        ``derived`` is True when the outputs were reconstructed from traces
        rather than read from a written checkpoint, so the caller can say so
        rather than presenting the two as equivalent.

        ``identities`` maps crew name to the content hash of the crew that
        produced that output, so the caller can refuse to replay an output
        whose crew has since been edited. Trace-derived results have none —
        they were never recorded — which reads as "unverified", not "changed".
    """
    if not resume_from_execution_id or not repositories:
        return {}, False, {}

    execution_history_repo = repositories.get("execution_history")
    if not execution_history_repo:
        logger.warning(
            "No execution_history repository — cannot load checkpoint outputs"
        )
        return {}, False, {}

    try:
        execution = await execution_history_repo.get_execution_by_job_id(
            resume_from_execution_id
        )
        if not execution or not execution.job_id:
            logger.warning(f"No execution found for ID: {resume_from_execution_id}")
            return {}, False, {}

        job_id = execution.job_id

        # 1. The written checkpoint.
        from src.services.execution.checkpointing import (
            build_flow_outputs,
            normalize,
            select_prefix,
        )

        record = normalize(execution.checkpoint_data)
        outputs = build_flow_outputs(record, from_unit=from_unit)
        if outputs:
            identities = {
                unit.get("name"): unit.get("identity")
                for unit in select_prefix(record, from_unit)
                if unit.get("name")
            }
            _log_outputs(outputs, job_id, derived=False)
            return outputs, False, identities

        # 2. Legacy: reconstruct from traces.
        execution_trace_repo = repositories.get("execution_trace")
        if not execution_trace_repo:
            logger.warning(
                f"No checkpoint recorded for {job_id} and no execution_trace "
                f"repository to fall back on"
            )
            return {}, False, {}

        outputs = await execution_trace_repo.get_crew_outputs_for_resume(job_id)
        if outputs:
            logger.warning(
                f"Resuming {job_id} from TRACE-DERIVED outputs — this execution "
                f"predates written checkpoints; fidelity is not guaranteed"
            )
            _log_outputs(outputs, job_id, derived=True)
            # Traces carry no crew identity, so nothing here can be verified.
            return outputs, True, {}

        logger.warning(f"No checkpoint outputs available for {job_id}")
        return {}, False, {}

    except Exception as e:
        logger.error(f"Failed to load checkpoint outputs: {e}", exc_info=True)
        return {}, False, {}


def _log_outputs(outputs: Dict[str, Any], job_id: str, derived: bool) -> None:
    source = "trace-derived" if derived else "checkpoint"
    logger.info(
        f"Loaded {source} outputs for {len(outputs)} crew(s) from {job_id}: "
        f"{list(outputs.keys())}"
    )
    for crew_name, output in outputs.items():
        text = str(output)
        preview = f"{text[:200]}..." if len(text) > 200 else text
        logger.info(f"  📦 Output for '{crew_name}': {preview}")
