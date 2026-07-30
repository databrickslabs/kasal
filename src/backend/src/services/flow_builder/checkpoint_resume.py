"""Loading the outputs a resumed flow restores its completed crews from.

One source: the **written checkpoint** on the source execution, recorded as
each crew completed by ``FlowCrewCheckpointRecorder``.

There used to be a second — reconstructing the same mapping from
``execution_trace`` rows. It is gone. Traces are TELEMETRY: they are
retention-pruned on a schedule, and can be sampled, truncated or reshaped for
reasons that have nothing to do with resume, so a crew whose trace row had
aged out simply looked like it never ran. Resuming against that produced a
plausible-looking run built on gaps. A flow executed before checkpoints were
written now re-runs from the start instead, which is slower and honest.

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
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Load ``{crew_name: output}`` for a flow resuming from an earlier run.

    Args:
        resume_from_execution_id: The SOURCE run's job_id (a UUID string, not
            the integer primary key — the parameter is named for the API field
            it arrives in).
        repositories: The subprocess's repository map.
        from_unit: Optional crew sequence to resume AT; everything before it is
            restored.

    Returns:
        ``(outputs, identities)``. ``identities`` maps crew name to the content
        hash of the crew that produced that output, so the caller can refuse to
        replay an output whose crew has since been edited.

        Empty when the source run has no written checkpoint — the flow then
        runs from the start rather than resuming against reconstructed data.
    """
    if not resume_from_execution_id or not repositories:
        return {}, {}

    execution_history_repo = repositories.get("execution_history")
    if not execution_history_repo:
        logger.warning(
            "No execution_history repository — cannot load checkpoint outputs"
        )
        return {}, {}

    try:
        # Accept either form. The flow resume dialog sends the integer row id;
        # this module has always looked up by job_id. Resolving only one of
        # them meant the other quietly found nothing and the flow re-ran in
        # full while reporting a successful resume.
        execution = None
        if isinstance(resume_from_execution_id, int) or (
            isinstance(resume_from_execution_id, str)
            and resume_from_execution_id.isdigit()
        ):
            execution = await execution_history_repo.get_execution_by_id(
                int(resume_from_execution_id)
            )
        if execution is None:
            execution = await execution_history_repo.get_execution_by_job_id(
                str(resume_from_execution_id)
            )

        if not execution or not execution.job_id:
            logger.warning(f"No execution found for ID: {resume_from_execution_id}")
            return {}, {}

        job_id = execution.job_id

        from src.services.execution.checkpointing import (
            build_flow_outputs,
            normalize,
            select_prefix,
        )

        record = normalize(execution.checkpoint_data)
        outputs = build_flow_outputs(record, from_unit=from_unit)
        if not outputs:
            logger.warning(
                f"No checkpoint recorded for {job_id} — the flow will run from "
                f"the start"
            )
            return {}, {}

        identities = {
            unit.get("name"): unit.get("identity")
            for unit in select_prefix(record, from_unit)
            if unit.get("name")
        }
        _log_outputs(outputs, job_id)
        return outputs, identities

    except Exception as e:
        logger.error(f"Failed to load checkpoint outputs: {e}", exc_info=True)
        return {}, {}


def _log_outputs(outputs: Dict[str, Any], job_id: str) -> None:
    logger.info(
        f"Loaded checkpoint outputs for {len(outputs)} crew(s) from {job_id}: "
        f"{list(outputs.keys())}"
    )
    for crew_name, output in outputs.items():
        text = str(output)
        preview = f"{text[:200]}..." if len(text) > 200 else text
        logger.info(f"  📦 Output for '{crew_name}': {preview}")
