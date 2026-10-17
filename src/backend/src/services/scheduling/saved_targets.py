"""What a schedule runs: its saved crew or flow, never a masked copy of a run.

A run's stored inputs are its RECORD, and a record is masked: every credential
string in the agents' and tasks' tool configs and in the user inputs becomes
``***REDACTED***`` (``execution/stored_inputs.mask_run_inputs``). A schedule that
copied them ran with the placeholder as the secret, and failed auth on every run.

So a schedule made from a past run names what the run was built from:

- a crew run of a saved crew stores ``crew_id``, and each run is rebuilt from
  that crew's current definition, where the secrets decrypt on load;
- a flow run of a saved flow stores ``flow_id`` without nodes, so each run loads
  the saved flow (``kasal_service.run_flow_execution`` does so when no nodes are
  passed).

A run with neither (a chat answer, an unsaved canvas) keeps its stored copy when
that copy is complete, and is refused when masking took something out of it.
"""

import logging
import uuid
from typing import Dict, List, Optional, Tuple, TypedDict

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import BadRequestError, ForbiddenError
from src.models.execution_history import ExecutionHistory
from src.schemas.execution import CrewConfig
from src.services.execution.stored_inputs import mask_run_inputs
from src.utils.sensitive_data_utils import REDACTED_PLACEHOLDER
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

MASKED_CONFIG_REFUSAL = (
    "This run cannot be scheduled: its stored configuration has secrets that "
    "were masked when the run was saved, and it was not started from a saved "
    "crew or flow that could supply them. Save the workflow as a crew or flow, "
    "run it, and schedule that run."
)
MASKED_INPUTS_REFUSAL = (
    "This run cannot be scheduled: some of its inputs were masked when the run "
    "was saved (they looked like secrets), so a schedule cannot reuse them."
)

Spec = Dict[str, object]
#: A crew's agents or tasks, keyed by name — the shape CrewConfig takes.
Entries = Dict[str, Dict[str, object]]


class ScheduleTarget(TypedDict, total=False):
    """The fields of a schedule that say what it runs."""

    crew_id: Optional[uuid.UUID]
    flow_id: Optional[uuid.UUID]
    agents_yaml: Spec
    tasks_yaml: Spec
    nodes: Optional[List[object]]
    edges: Optional[List[object]]
    flow_config: Spec


def saved_id(value: object) -> Optional[uuid.UUID]:
    """``value`` as the UUID of a saved crew or flow, or None if it is not one."""
    if isinstance(value, uuid.UUID):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return uuid.UUID(value)
    except ValueError:
        return None


def schedule_crew_id(schedule: object) -> Optional[str]:
    """The saved crew a schedule row runs, as the string CrewConfig takes."""
    crew_id = saved_id(getattr(schedule, "crew_id", None))
    return str(crew_id) if crew_id else None


def schedule_timezone(schedule: object) -> Optional[str]:
    """A schedule row's IANA timezone, or None for the server's clock."""
    tz = getattr(schedule, "timezone", None)
    return tz if isinstance(tz, str) and tz else None


def carries_masked_values(value: object) -> bool:
    """True when some value in ``value`` IS the masking placeholder.

    Judged by value alone: the maskers replace a whole value with
    ``REDACTED_PLACEHOLDER`` and emit nothing else, so a string that merely
    mentions it, or any value under a secret-looking key, is not masked.
    """
    if isinstance(value, str):
        return value == REDACTED_PLACEHOLDER
    if isinstance(value, dict):
        return any(carries_masked_values(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(carries_masked_values(item) for item in value)
    return False


async def project_saved_crew(
    session: AsyncSession, crew_id: uuid.UUID, group_context: Optional[GroupContext]
) -> Optional[Tuple[Entries, Entries]]:
    """The saved crew's current ``(agents_yaml, tasks_yaml)``, or None.

    None covers a deleted crew, another workspace's crew, and a crew none of
    whose agents or tasks the workspace can read. Secrets come back decrypted.
    """
    from src.services.catalog.crew_config import build_crew_execution_config_by_id

    projected = await build_crew_execution_config_by_id(
        session, str(crew_id), group_context
    )
    if not projected or not projected[0] or not projected[1]:
        return None
    return projected[0], projected[1]


class SavedTargetGone(ValueError):
    """The saved crew or flow a schedule runs can no longer be read."""


async def use_saved_target(
    session: AsyncSession, config: CrewConfig, group_context: Optional[GroupContext]
) -> None:
    """Point a schedule's run at its saved crew or flow as it is now.

    A crew schedule with a ``crew_id`` is rebuilt from that crew. A flow
    schedule with a ``flow_id`` and no stored nodes must still find its flow.
    Raises ``SavedTargetGone`` when the crew or flow was deleted (or is no
    longer the workspace's): running the masked display copy instead would
    send ``***REDACTED***`` as the secret, and a flow run would fail later
    with nothing recorded.
    """
    if (config.execution_type or "crew") == "flow":
        await _require_saved_flow(session, config, group_context)
        return
    crew_id = saved_id(config.crew_id)
    if crew_id is None:
        return
    projected = await project_saved_crew(session, crew_id, group_context)
    if projected is None:
        raise SavedTargetGone(
            f"Its saved crew {crew_id} was deleted or is not readable in this "
            "workspace, so the schedule was turned off."
        )
    config.agents_yaml, config.tasks_yaml = projected
    logger.info(
        "Scheduled run rebuilt from saved crew %s: %d agent(s), %d task(s)",
        crew_id,
        len(projected[0]),
        len(projected[1]),
    )


async def _require_saved_flow(
    session: AsyncSession, config: CrewConfig, group_context: Optional[GroupContext]
) -> None:
    flow_id = saved_id(str(config.flow_id) if config.flow_id else None)
    if flow_id is None or config.nodes:
        return  # an ad-hoc flow runs the nodes it stored
    from src.services.flow_builder.flow_service import FlowService

    try:
        flow = await FlowService(session).get_flow_for_execution(
            flow_id, group_context, allow_unsaved=True
        )
    except ForbiddenError:
        flow = None
    if flow is None:
        raise SavedTargetGone(
            f"Its saved flow {flow_id} was deleted or is not readable in this "
            "workspace, so the schedule was turned off."
        )


async def schedule_target_from_run(
    session: AsyncSession,
    execution: ExecutionHistory,
    execution_type: str,
    group_context: Optional[GroupContext],
) -> ScheduleTarget:
    """What a schedule made from the run ``execution`` runs.

    Raises ``BadRequestError`` when the run holds nothing a schedule can run.
    """
    inputs: Spec = execution.inputs if isinstance(execution.inputs, dict) else {}
    if carries_masked_values(inputs.get("inputs")):
        raise BadRequestError(detail=MASKED_INPUTS_REFUSAL)
    if execution_type == "flow":
        return _flow_target(execution, inputs)
    return await _crew_target(session, execution, inputs, group_context)


def _spec(value: object) -> Spec:
    return value if isinstance(value, dict) else {}


async def _crew_target(
    session: AsyncSession,
    execution: ExecutionHistory,
    inputs: Spec,
    group_context: Optional[GroupContext],
) -> ScheduleTarget:
    crew_id = saved_id(getattr(execution, "crew_id", None))
    if crew_id is not None:
        projected = await project_saved_crew(session, crew_id, group_context)
        if projected is not None:
            # Stored for display only, so stored masked; each run re-projects.
            shown = mask_run_inputs(
                {"agents_yaml": projected[0], "tasks_yaml": projected[1]}
            )
            return {
                "crew_id": crew_id,
                "agents_yaml": _spec(shown["agents_yaml"]),
                "tasks_yaml": _spec(shown["tasks_yaml"]),
            }
        logger.info(
            "Saved crew %s of run %s is gone; scheduling the run's stored copy",
            crew_id,
            execution.id,
        )

    agents_yaml = _spec(inputs.get("agents_yaml"))
    tasks_yaml = _spec(inputs.get("tasks_yaml"))
    if not agents_yaml or not tasks_yaml:
        raise BadRequestError(
            detail=f"Execution {execution.id} does not contain valid agents_yaml "
            "or tasks_yaml configuration"
        )
    if carries_masked_values([agents_yaml, tasks_yaml]):
        raise BadRequestError(detail=MASKED_CONFIG_REFUSAL)
    return {"agents_yaml": agents_yaml, "tasks_yaml": tasks_yaml}


def _flow_target(execution: ExecutionHistory, inputs: Spec) -> ScheduleTarget:
    flow_config = _spec(inputs.get("flow_config"))
    flow_id = saved_id(getattr(execution, "flow_id", None)) or saved_id(
        inputs.get("flow_id")
    )
    if flow_id is not None:
        # No nodes: each run loads the saved flow as it is then.
        return {
            "flow_id": flow_id,
            "nodes": None,
            "edges": None,
            "flow_config": flow_config,
            "agents_yaml": {},
            "tasks_yaml": {},
        }

    nodes = inputs.get("nodes")
    edges = inputs.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        nodes, edges = [], []
    if not (nodes and edges):
        raise BadRequestError(
            detail=f"Execution {execution.id} is a flow execution but does not "
            "contain flow_id or nodes/edges configuration"
        )
    if carries_masked_values([nodes, edges, flow_config]):
        raise BadRequestError(detail=MASKED_CONFIG_REFUSAL)
    return {
        "flow_id": None,
        "nodes": nodes,
        "edges": edges,
        "flow_config": flow_config,
        "agents_yaml": _spec(inputs.get("agents_yaml")),
        "tasks_yaml": _spec(inputs.get("tasks_yaml")),
    }
