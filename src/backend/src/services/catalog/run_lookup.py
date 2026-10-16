"""Catalog reads a RUN makes by id: scoped to the run's workspace, closed by default.

A run names agents, tasks and crews by ids that arrive with it: the canvas
payload, the Runs API, a flow_config, a crew or flow imported from another
workspace, a stored run being re-run. None of that proves the run's workspace
owns the row. So a run-time read by id goes through the owning service's
group-checked method with the run's ``GroupContext``, and only through that.

Closed by default. ``get_with_group_check`` lets any row through when the context
has no group ids (a router always has them; a background run might not), so a
run with no group ids reads nothing here.

A miss, whether the row is absent or belongs elsewhere, is a ``None`` and one
WARNING line naming the id, never the row; the two cases read the same, so the
log cannot tell a caller which ids exist in other workspaces. Each caller decides
what "not found" means for its run: skip the stored settings, or fall back to
data the run already owns.
"""

import logging
import uuid
from typing import Awaitable, Callable, List, Optional, TypeVar, Union

from src.models.agent import Agent
from src.models.crew import Crew
from src.models.task import Task
from src.services.catalog.agents import AgentService
from src.services.catalog.crews import CrewService
from src.services.catalog.tasks import TaskService
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

_Id = TypeVar("_Id")
_Row = TypeVar("_Row")


def run_group_ids(group_context: Optional[GroupContext]) -> List[str]:
    """The run's group ids; empty means the run may read no catalog row."""
    return [g for g in (getattr(group_context, "group_ids", None) or []) if g]


async def _scoped_read(
    kind: str,
    entity_id: Optional[_Id],
    read: Callable[[_Id, Optional[GroupContext]], Awaitable[Optional[_Row]]],
    group_context: Optional[GroupContext],
    log: logging.Logger,
) -> Optional[_Row]:
    if not entity_id:
        return None
    if not run_group_ids(group_context):
        log.warning("Run has no workspace: %s %s not loaded", kind, entity_id)
        return None
    row = await read(entity_id, group_context)
    if row is None:
        log.warning("%s %s not found in this run's workspace", kind, entity_id)
    return row


async def agent_for_run(
    service: AgentService,
    agent_id: Optional[str],
    group_context: Optional[GroupContext],
    log: logging.Logger = logger,
) -> Optional[Agent]:
    """The agent, if it is in one of the run's groups; else None."""
    return await _scoped_read(
        "Agent", agent_id, service.get_with_group_check, group_context, log
    )


async def task_for_run(
    service: TaskService,
    task_id: Optional[str],
    group_context: Optional[GroupContext],
    log: logging.Logger = logger,
) -> Optional[Task]:
    """The task, if it is in one of the run's groups; else None."""
    return await _scoped_read(
        "Task", task_id, service.get_with_group_check, group_context, log
    )


async def crew_for_run(
    service: CrewService,
    crew_id: Union[uuid.UUID, str, None],
    group_context: Optional[GroupContext],
    log: logging.Logger = logger,
) -> Optional[Crew]:
    """The crew, if it is in the run's current workspace; else None.

    ``CrewService.get_by_group`` checks the primary group, which is the run's
    workspace. A crew id that is not a UUID is a miss, not an error.
    """
    if not crew_id:
        return None
    try:
        key = crew_id if isinstance(crew_id, uuid.UUID) else uuid.UUID(str(crew_id))
    except ValueError:
        log.warning("Crew %s not found in this run's workspace", crew_id)
        return None
    return await _scoped_read("Crew", key, service.get_by_group, group_context, log)
