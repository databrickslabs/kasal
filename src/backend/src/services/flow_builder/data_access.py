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
from typing import Any, Dict

logger = logging.getLogger(__name__)


def build_flow_data_access(session) -> Dict[str, Any]:
    """The ``repositories`` bundle for a ``BackendFlow``, keyed as before.

    The keys are unchanged (``flow``/``task``/``agent``/``crew``/
    ``execution_history``) so the 17 read sites downstream keep working; what they
    receive is the owning SERVICE rather than that domain's repository.

    Args:
        session: the session this run owns — the subprocess's own, already routed.

    Returns:
        Mapping of key -> service (or, for ``flow``, this domain's repository).
    """
    from src.repositories.flow_repository import FlowRepository
    from src.services.catalog.agents import AgentService
    from src.services.catalog.crews import CrewService
    from src.services.catalog.tasks import TaskService
    from src.services.execution.service import ExecutionService

    return {
        # flow_builder's own data — no cross-domain hop, and FlowService would be a
        # circular import from here.
        "flow": FlowRepository(session),
        # Other domains: through their services.
        "task": TaskService(session),
        "agent": AgentService(session),
        "crew": CrewService(session),
        "execution_history": ExecutionService(session),
    }
