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

import dataclasses
import logging
import uuid
from typing import Awaitable, Callable, List, Optional, TypeVar, Union

from sqlalchemy.ext.asyncio import AsyncSession

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


def require_run_workspace(group_context: Optional[GroupContext]) -> List[str]:
    """The run's group ids, or ``ValueError``: a run with none is not started.

    Every entry point resolves a workspace before it starts a run (the API from
    the caller, schedules and triggers from their row, MCP/A2A from the
    publication). A run without one would read no catalog row, and so run
    without the tool configs and approval gates its agents and tasks add.
    """
    group_ids = run_group_ids(group_context)
    if not group_ids:
        raise ValueError("This run has no workspace (no group ids); not started")
    return group_ids


def scope_to_run(
    group_context: Optional[GroupContext], run: object
) -> Optional[GroupContext]:
    """The context narrowed to an existing run's own group.

    With no workspace selected a context spans every workspace the user is in.
    Work done FOR one run (a resume, its checkpoint preview, the resumed run's
    record) belongs to that run's workspace, so its crew, flow, agents and
    tasks are read there and nowhere else. Only a group already in the
    context's scope is taken; otherwise the context is returned unchanged.
    """
    group_id = getattr(run, "group_id", None)
    if (
        not isinstance(group_context, GroupContext)
        or not isinstance(group_id, str)
        or group_id not in run_group_ids(group_context)
    ):
        return group_context
    return dataclasses.replace(group_context, group_ids=[group_id])


def _miss_level(entity_id: object) -> int:
    """WARNING when a catalog id missed; DEBUG for a key that is not one.

    Catalog ids are UUIDs. A spec keyed by name with no id (or a generated
    chat agent) cannot name a row, so its miss is routine and stays out of
    every run's log at warning level.
    """
    if isinstance(entity_id, uuid.UUID):
        return logging.WARNING
    try:
        uuid.UUID(str(entity_id))
    except ValueError:
        return logging.DEBUG
    return logging.WARNING


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
        log.log(
            _miss_level(entity_id),
            "%s %s not found in this run's workspace",
            kind,
            entity_id,
        )
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
        log.debug("Crew %s not found in this run's workspace", crew_id)
        return None
    return await _scoped_read("Crew", key, service.get_by_group, group_context, log)


async def crew_id_to_record(
    session: Optional[AsyncSession],
    crew_id: Union[uuid.UUID, str, None],
    group_context: Optional[GroupContext],
) -> Optional[uuid.UUID]:
    """The saved-crew link a new run records: its crew's id, or None.

    The id comes with the request; the run is filed in the context's current
    workspace, and a resume later rebuilds from this link. So it is recorded
    only for a crew of that workspace. Anything else (another workspace's crew,
    a deleted one, an id that is not a UUID) records None, with a warning.
    Without a session the read is routed.
    """
    if session is None:
        from src.db.session import routed_scoped_session

        async with routed_scoped_session() as routed:
            return await crew_id_to_record(routed, crew_id, group_context)
    crew = await crew_for_run(CrewService(session), crew_id, group_context)
    if crew is None:
        logger.warning(
            "Run records no crew link: crew %s is not in its workspace", crew_id
        )
        return None
    return crew.id
