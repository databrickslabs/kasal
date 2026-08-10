"""Building the config a resumed run actually executes.

A checkpoint stores OUTPUTS. The definition those outputs came from is not the
checkpoint's to own, and freezing it is how a resume ends up re-running last
week's prompt: ``execution_history.inputs`` is a snapshot taken when the
ORIGINAL run started, so a task edited on the canvas afterwards was invisible
to a resume — including to the guard meant to catch exactly that. Task identity
was being compared against a snapshot of itself, so it could never fire.

So a resume rebuilds from the SAVED DEFINITION where there is one:

- a **crew** from its ``crews`` row (``executionhistory.crew_id``), projected
  by ``catalog/crew_config.py``;
- a **flow** from its ``flows`` row (``executionhistory.flow_uuid``), whose
  nodes/edges/flow_config the flow builder already resolves task-by-task
  against the database.

The stored snapshot is the FALLBACK, for the runs that genuinely have no saved
definition to rebuild from: an unsaved canvas, or one whose crew or flow was
deleted since. Those keep resuming exactly as they did before.

What survives the rebuild is then decided by content identity, not by this
module: the crew runtime restores the longest prefix whose task identities
still match (``runtime/crew.py::_load_checkpoint``), and the flow builder
declines to skip a crew whose identity changed
(``flow_builder/checkpoint_identity.py``). Rebuilding is what makes those
checks able to fire at all.
"""

import logging
from typing import Any, Dict, Optional, Tuple

from src.schemas.execution import CrewConfig
from src.services.execution.checkpointing.record import ordered_units
from src.services.execution.checkpointing.resume import build_crew_payload

logger = logging.getLogger(__name__)


async def _rebuild_crew_definition(
    session: Any,
    source: Any,
    stored_inputs: Dict[str, Any],
    group_context: Any,
) -> Tuple[Dict[str, Any], Dict[str, Any], bool]:
    """The crew's CURRENT agents/tasks, or its stored ones.

    Returns ``(agents_yaml, tasks_yaml, rebuilt)``. ``rebuilt`` says which
    source won, for the log line and so a caller can tell the user whether the
    resume will see their edits.

    Falls back on anything short of a complete rebuild — no crew link, crew
    deleted, or a projection that came back without both agents and tasks. A
    partially rebuilt crew is worse than the snapshot: it would run a smaller
    crew than the one that was checkpointed and call that a resume.
    """
    crew_id = getattr(source, "crew_id", None)
    if not crew_id:
        return (
            stored_inputs.get("agents_yaml") or {},
            stored_inputs.get("tasks_yaml") or {},
            False,
        )

    from src.services.catalog.crew_config import build_crew_execution_config_by_id

    try:
        projected = await build_crew_execution_config_by_id(
            session, crew_id, group_context
        )
    except Exception as exc:  # noqa: BLE001 — the snapshot still works
        logger.warning(
            "[resume] could not rebuild crew %s (%s); resuming from the stored "
            "definition instead",
            crew_id,
            exc,
        )
        projected = None

    if not projected or not projected[0] or not projected[1]:
        logger.info(
            "[resume] crew %s could not be rebuilt (deleted, or it resolved to "
            "no agents/tasks); resuming from the stored definition",
            crew_id,
        )
        return (
            stored_inputs.get("agents_yaml") or {},
            stored_inputs.get("tasks_yaml") or {},
            False,
        )

    agents_yaml, tasks_yaml = projected
    logger.info(
        "[resume] rebuilt crew %s from its saved definition — %d agent(s), "
        "%d task(s); the checkpoint prefix is kept only where task identities "
        "still match",
        crew_id,
        len(agents_yaml),
        len(tasks_yaml),
    )
    return agents_yaml, tasks_yaml, True


async def build_crew_resume_config(
    session: Any,
    source: Any,
    stored_inputs: Dict[str, Any],
    record: Optional[Dict[str, Any]],
    from_unit: Optional[str],
    group_context: Any = None,
) -> Tuple[CrewConfig, int, Dict[str, Any]]:
    """Build the config, restored-unit count and inputs for resuming a CREW.

    Args:
        session: The session the resume is running on.
        source: The execution being resumed from.
        stored_inputs: Its stored inputs (agents_yaml / tasks_yaml / inputs).
        record: Its normalized checkpoint record, if any.
        from_unit: Optional task index to resume AT.
        group_context: Group context, for the rebuild.

    Returns:
        ``(CrewConfig, restored_task_count, resume_inputs)``. ``resume_inputs``
        is what the NEW execution stores — the definition that actually ran, so
        the record is not a lie and a second resume starts from the same place
        the first did.

    ``restored_task_count`` is an UPPER BOUND. The runtime restores the longest
    prefix whose identities still match, so an edit in the middle of the prefix
    restores fewer. Only the runtime holds the built Task objects the
    identities come from, so it reports the true number in its own log line
    rather than this guessing.
    """
    agents_yaml, tasks_yaml, rebuilt = await _rebuild_crew_definition(
        session, source, stored_inputs, group_context
    )

    if not agents_yaml or not tasks_yaml:
        raise ValueError(
            f"Execution {source.job_id} has no stored crew configuration to resume from"
        )

    checkpoint = build_crew_payload(record, from_unit=from_unit)
    restored = len(checkpoint.get("completed") or []) if checkpoint else 0

    config = CrewConfig(
        agents_yaml=agents_yaml,
        tasks_yaml=tasks_yaml,
        inputs=stored_inputs.get("inputs") or {},
        # Legacy executions may still carry a stored "planning" key; it is
        # ignored on resume because CrewConfig no longer models it.
        reasoning=bool(stored_inputs.get("reasoning", False)),
        model=stored_inputs.get("model"),
        execution_type="crew",
        schema_detection_enabled=stored_inputs.get("schema_detection_enabled", True),
        crew_id=str(getattr(source, "crew_id", "") or "") or None,
        resume_checkpoint=checkpoint,
    )

    resume_inputs = dict(stored_inputs)
    if rebuilt:
        resume_inputs["agents_yaml"] = agents_yaml
        resume_inputs["tasks_yaml"] = tasks_yaml

    return config, restored, resume_inputs


async def build_flow_resume_config(
    session: Any,
    source: Any,
    stored_inputs: Dict[str, Any],
    record: Optional[Dict[str, Any]],
    from_unit: Optional[str],
    group_context: Any = None,
) -> Tuple[CrewConfig, int, Dict[str, Any]]:
    """Build the config, restored-unit count and inputs for resuming a FLOW.

    A flow resumes by rebuilding itself from saved nodes and edges and SKIPPING
    the crews it already completed, replaying their stored output. Which crews
    get skipped is driven by ``resume_from_crew_sequence``, which names the
    crew to RUN (crews with a lower sequence are skipped).

    Those nodes and edges come from the ``flows`` row when the run has one, not
    from the stored inputs. Task TEXT was already current either way — the flow
    builder loads each task from the database by id at build time — but the
    stored ``flow_config`` freezes the ``startingPoints``/``listeners`` task-id
    lists, so a task added, removed or rewired since the run was invisible.

    Returns:
        ``(CrewConfig, restored_crew_count, resume_inputs)``.
    """
    units = ordered_units(record)

    if from_unit not in (None, ""):
        try:
            resume_at = int(from_unit)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid resume point '{from_unit}'")
    elif units:
        # Continue after everything recorded: the first crew that did not
        # complete is one past the highest sequence stored.
        resume_at = max(int(u["key"]) for u in units) + 1
    else:
        resume_at = None  # nothing recorded — run the whole flow

    restored = sum(1 for u in units if resume_at is None or int(u["key"]) < resume_at)

    definition = await _rebuild_flow_definition(
        session, source, stored_inputs, group_context
    )

    return (
        CrewConfig(
            agents_yaml={},
            tasks_yaml={},
            inputs=stored_inputs.get("inputs") or {},
            model=stored_inputs.get("model"),
            execution_type="flow",
            flow_id=stored_inputs.get("flow_id"),
            nodes=definition["nodes"],
            edges=definition["edges"],
            flow_config=definition["flow_config"],
            # The flow builder resolves this with get_execution_by_job_id, so
            # it is the SOURCE run's job_id, not the integer row id.
            resume_from_execution_id=source.job_id,
            resume_from_flow_uuid=source.flow_uuid,
            resume_from_crew_sequence=resume_at,
        ),
        restored,
        {**stored_inputs, **definition},
    )


async def _rebuild_flow_definition(
    session: Any,
    source: Any,
    stored_inputs: Dict[str, Any],
    group_context: Any = None,
) -> Dict[str, Any]:
    """The flow's CURRENT nodes/edges/flow_config, or its stored ones.

    Falls back whole rather than field-by-field: nodes, edges and flow_config
    describe one graph, and mixing a current ``flow_config`` with stored
    ``nodes`` would reference node ids that may no longer exist.
    """
    stored = {
        "nodes": stored_inputs.get("nodes"),
        "edges": stored_inputs.get("edges"),
        "flow_config": stored_inputs.get("flow_config"),
    }

    flow_id = getattr(source, "flow_id", None)
    if not flow_id:
        return stored

    try:
        from src.services.flow_builder.flow_service import FlowService

        service = FlowService(session)
        # Group-checked when we have a context. The flow_id already comes off a
        # group-filtered execution row, so this is defence in depth rather than
        # the only guard — but it is the accessor the rest of the codebase uses.
        flow = (
            await service.get_flow_with_group_check(flow_id, group_context)
            if group_context is not None
            else await service.get_flow(flow_id)
        )
    except Exception as exc:  # noqa: BLE001 — the snapshot still works
        logger.warning(
            "[resume] could not load flow %s (%s); resuming from the stored "
            "definition instead",
            flow_id,
            exc,
        )
        return stored

    if flow is None or not getattr(flow, "nodes", None):
        logger.info(
            "[resume] flow %s could not be rebuilt; resuming from the stored "
            "definition",
            flow_id,
        )
        return stored

    logger.info(
        "[resume] rebuilt flow %s from its saved definition — %d node(s); a "
        "completed crew is replayed only where its identity still matches",
        flow_id,
        len(flow.nodes or []),
    )
    return {
        "nodes": flow.nodes,
        "edges": getattr(flow, "edges", None) or [],
        "flow_config": getattr(flow, "flow_config", None) or {},
    }
