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
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Optional,
    TypeVar,
)

from sqlalchemy.ext.asyncio import AsyncSession

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


@asynccontextmanager
async def _short_lived_session() -> AsyncIterator[AsyncSession]:
    """A fresh, routed, auto-committing session for ONE data-access call.

    The flow subprocess runs for many minutes doing LLM work with no DB traffic.
    A session HELD across that idle stretch has its connection dropped by the
    Lakebase/network idle timeout (pool_pre_ping is off for the do_connect token
    pattern), so the next operation on it fails and a successful run is mislabelled
    FAILED. Giving every data-access call its OWN short-lived session means the run
    holds no idle connection — under NullPool (the subprocess default, main.py) each
    call gets a fresh connection, so an idle timeout has nothing to drop.
    """
    from src.db.database_router import get_smart_db_session

    gen = get_smart_db_session()
    session = await gen.__anext__()
    try:
        yield session
    except BaseException:
        await gen.aclose()
        raise
    else:
        try:
            await gen.__anext__()  # commit-on-success + close
        except StopAsyncIteration:
            pass


class _ShortLivedDataAccess:
    """A data-access handle whose every async method call runs on its OWN fresh,
    short-lived session (see ``_short_lived_session``). ``build(session)``
    constructs the real service/repository for that single call.

    The consumers only ever ``await <handle>.<method>(...)`` (``get`` / by-id
    lookups — verified across flow_processors + backend_flow), so a generic method
    proxy preserves the bundle's interface exactly while removing the long-held
    session that the idle timeout was killing.
    """

    def __init__(self, build: Callable[[AsyncSession], object]) -> None:
        self._build = build

    def __getattr__(self, name: str) -> Callable[..., Awaitable[object]]:
        if name.startswith("__"):
            # Don't synthesize dunders (repr/copy/pickle/awaitable checks).
            raise AttributeError(name)
        build = self._build

        async def _call(*args: object, **kwargs: object) -> object:
            async with _short_lived_session() as session:
                return await getattr(build(session), name)(*args, **kwargs)

        _call.__name__ = name
        return _call


def build_flow_data_access(
    session: Any = None, group_context: Optional[GroupContext] = None
) -> Dict[str, Any]:
    """The ``repositories`` bundle for a ``BackendFlow``, keyed as before.

    The keys are unchanged (``flow``/``task``/``agent``/``crew``/
    ``execution_history``) so the 17 read sites downstream keep working; what they
    receive is the owning SERVICE rather than that domain's repository, and for
    the catalog kinds a reader bound to ``group_context``.

    Every entry also runs each call on its OWN short-lived session
    (``_ShortLivedDataAccess``) instead of one held across the whole run, so the
    flow never holds a DB connection idle while the crews do their minutes-long,
    DB-idle LLM work — the Lakebase idle timeout has nothing to drop.

    Args:
        session: accepted for call-site compatibility but intentionally NOT held
            (that is the point of the short-lived handles).
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
        "flow": _ShortLivedDataAccess(lambda s: FlowRepository(s)),
        # Other domains: through their services, scoped to the run's workspace.
        "task": _RunScopedReads(
            task_for_run, _ShortLivedDataAccess(lambda s: TaskService(s)), group_context
        ),
        "agent": _RunScopedReads(
            agent_for_run,
            _ShortLivedDataAccess(lambda s: AgentService(s)),
            group_context,
        ),
        "crew": _RunScopedReads(
            crew_for_run, _ShortLivedDataAccess(lambda s: CrewService(s)), group_context
        ),
        "execution_history": _ShortLivedDataAccess(lambda s: ExecutionService(s)),
    }
