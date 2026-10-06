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

So the bundle holds SERVICES now. Two things made that safe to do:

* the consumers only ever called four methods, three of them a plain ``get`` —
  ``task.get``, ``agent.get``, ``crew.get``, plus ``execution_history``'s two
  by-id lookups. Nothing needed a repository-only API.
* ``tool`` and ``execution_trace`` were injected and NEVER read. They are gone
  rather than translated.

``flow`` stays as a repository: flows are flow_builder's own domain, and
``FlowService`` would be a circular import here.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _short_lived_session():
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

    gen = get_smart_db_session().__aiter__()
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

    def __init__(self, build: Callable[[Any], Any]):
        self._build = build

    def __getattr__(self, name: str):
        if name.startswith("__"):
            # Don't synthesize dunders (repr/copy/pickle/awaitable checks).
            raise AttributeError(name)
        build = self._build

        async def _call(*args, **kwargs):
            async with _short_lived_session() as session:
                return await getattr(build(session), name)(*args, **kwargs)

        _call.__name__ = name
        return _call


def build_flow_data_access(session: Any = None) -> Dict[str, Any]:
    """The ``repositories`` bundle for a ``BackendFlow``, keyed as before
    (``flow``/``task``/``agent``/``crew``/``execution_history``).

    Each entry is a SHORT-LIVED-session handle: every call opens its own fresh
    session rather than sharing one held across the whole run. This is the durable
    fix for the long-run connection death — the flow never holds a DB connection
    idle while the crews do their (minutes-long, DB-idle) LLM work, so the idle
    timeout has nothing to drop and the mid-run reads + terminal writes succeed.

    ``session`` is accepted for call-site compatibility but intentionally NOT held
    (that is the whole point); callers may stop passing it.
    """
    from src.repositories.flow_repository import FlowRepository
    from src.services.catalog.agents import AgentService
    from src.services.catalog.crews import CrewService
    from src.services.catalog.tasks import TaskService
    from src.services.execution.service import ExecutionService

    return {
        # flow_builder's own data — no cross-domain hop, and FlowService would be a
        # circular import from here.
        "flow": _ShortLivedDataAccess(lambda s: FlowRepository(s)),
        # Other domains: through their services.
        "task": _ShortLivedDataAccess(lambda s: TaskService(s)),
        "agent": _ShortLivedDataAccess(lambda s: AgentService(s)),
        "crew": _ShortLivedDataAccess(lambda s: CrewService(s)),
        "execution_history": _ShortLivedDataAccess(lambda s: ExecutionService(s)),
    }
