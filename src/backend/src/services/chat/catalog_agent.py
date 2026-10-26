"""Fill a chat turn's agent spec from the catalog agent it names.

A chat turn's single agent may name a saved (catalog) agent by its
``agent_<id>`` key. When the spec carries no ``tool_configs`` the stored
agent's fill them in, approval gates included. The id comes from the request,
so the read is scoped to the turn's workspace (``catalog/run_lookup``), and a
turn with no workspace is refused outright rather than run without them.
"""

import logging
from typing import Dict, Optional

from src.services.catalog.agents import AgentService
from src.services.catalog.run_lookup import agent_for_run, require_run_workspace
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


async def fill_tool_configs_from_catalog(
    agent_spec: Dict[str, object], group_context: Optional[GroupContext]
) -> None:
    """Refuse a turn with no workspace; else fill missing tool_configs in place.

    Raises:
        ValueError: the turn has no group ids, so it is not a legitimate run.
    """
    require_run_workspace(group_context)
    if agent_spec.get("tool_configs"):
        return  # chat-generated agents already carry their own
    # Imported here, not at module level: tests route the session by patching
    # src.db.session.routed_scoped_session, as for the rest of the turn.
    from src.db.session import routed_scoped_session

    agent_id = str(agent_spec.get("id", "")).replace("agent_", "").replace("agent-", "")
    try:
        async with routed_scoped_session() as session:
            row = await agent_for_run(AgentService(session), agent_id, group_context)
        if row is not None and row.tool_configs:
            agent_spec["tool_configs"] = row.tool_configs
    except Exception:  # noqa: BLE001 — best-effort: the turn runs on the spec alone
        logger.debug("[light_agent] tool_configs enrichment skipped", exc_info=True)
