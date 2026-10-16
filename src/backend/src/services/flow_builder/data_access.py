"""The data-access bundle a dynamic flow run carries into ``BackendFlow``.

A dynamic flow (nodes passed inline, no saved ``flow_id``) has to resolve the agents,
tasks and crews its graph names, and a resumed run has to read the checkpoint off its
previous execution. ``BackendFlow`` and its modules run in the flow SUBPROCESS with no
router above them, so the runner builds this bundle once and injects it.

It used to hold seven REPOSITORIES — ``task``, ``agent``, ``crew``,
``execution_history`` and ``execution_trace`` all belonging to other domains. That was
the last cross-domain repository access in the codebase, and the reason given for
keeping it (the subprocess is risky to change) was an argument for care, not for a
different standard: a flow run reading an agent skips ``AgentService`` exactly as any
other caller would, and loses the same group check.

So the bundle holds SERVICES now, and ``task``/``agent``/``crew`` are BOUND TO THE
RUN'S WORKSPACE. Their ids come from the flow_config, the client, or a flow or crew
imported from another workspace, so the bundle's ``get`` reads through the
catalog's run-scoped, group-checked lookups (``catalog/run_lookup.py``): a row
outside the run's groups, or any row when the run has no groups, is ``None``, which
every reader already handles as "not found". The 17 read sites downstream only call
``get``, so they are unchanged.

* ``tool`` and ``execution_trace`` were injected and NEVER read. They are gone
  rather than translated.

``flow`` stays as a repository: flows are flow_builder's own domain, and
``FlowService`` would be a circular import here.
"""

import logging
from typing import Any, Awaitable, Callable, Dict, Generic, Optional, TypeVar

from src.core.logger import LoggerManager
from src.utils.user_context import GroupContext

_Service = TypeVar("_Service")
_Id = TypeVar("_Id")
_Row = TypeVar("_Row")

#: ``catalog/run_lookup``'s ``agent_for_run`` / ``task_for_run`` / ``crew_for_run``.
_ScopedRead = Callable[
    [_Service, _Id, Optional[GroupContext], logging.Logger], Awaitable[Optional[_Row]]
]


class _RunScopedReads(Generic[_Service, _Id, _Row]):
    """``get(id)`` over one catalog kind, scoped to the run's workspace."""

    def __init__(
        self,
        read: _ScopedRead[_Service, _Id, _Row],
        service: _Service,
        group_context: Optional[GroupContext],
    ) -> None:
        self._read = read
        self._service = service
        self._group_context = group_context

    async def get(self, entity_id: _Id) -> Optional[_Row]:
        # The flow log, so a refused id shows up beside the run's other lines.
        log = LoggerManager.get_instance().flow
        return await self._read(self._service, entity_id, self._group_context, log)


def build_flow_data_access(
    session: Any, group_context: Optional[GroupContext]
) -> Dict[str, Any]:
    """The ``repositories`` bundle for a ``BackendFlow``, keyed as before.

    The keys are unchanged (``flow``/``task``/``agent``/``crew``/
    ``execution_history``) so the 17 read sites downstream keep working; what they
    receive is the owning SERVICE rather than that domain's repository, and for
    the catalog kinds a reader bound to ``group_context``.

    Args:
        session: the session this run owns — the subprocess's own, already routed.
        group_context: the run's group context (``config["group_context"]``).
            Required: with none, or none with group ids, no catalog row is read.

    Returns:
        Mapping of key -> service (or, for ``flow``, this domain's repository).
    """
    from src.repositories.flow_repository import FlowRepository
    from src.services.catalog.agents import AgentService
    from src.services.catalog.crews import CrewService
    from src.services.catalog.run_lookup import (
        agent_for_run,
        crew_for_run,
        task_for_run,
    )
    from src.services.catalog.tasks import TaskService
    from src.services.execution.service import ExecutionService

    return {
        # flow_builder's own data — no cross-domain hop, and FlowService would be a
        # circular import from here.
        "flow": FlowRepository(session),
        # Other domains: through their services, scoped to the run's workspace.
        "task": _RunScopedReads(task_for_run, TaskService(session), group_context),
        "agent": _RunScopedReads(agent_for_run, AgentService(session), group_context),
        "crew": _RunScopedReads(crew_for_run, CrewService(session), group_context),
        "execution_history": ExecutionService(session),
    }
