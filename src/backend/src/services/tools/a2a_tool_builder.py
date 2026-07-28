"""Building an A2AAgentTool from the remote-agent registry.

A sibling module rather than another branch inside ``tool_factory.py``, which is
already well past the size ceiling. The factory calls one function; everything
this tool needs to know about the registry lives here.

The tool is bound to ONE remote at construction. That is what lets its
description name the remote's actual skills — which is what the calling model
selects on — instead of asking it to guess an agent name it has never seen.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


async def build_a2a_tools(
    tool_config: Optional[Dict[str, Any]] = None,
    user_token: Optional[str] = None,
    group_ids: Optional[List[str]] = None,
    result_as_answer: bool = False,
) -> List[Any]:
    """The A2AAgentTool instances this agent should get.

    Named in the config, or every enabled remote when none is named. The
    fall-through is deliberate: an operator who attached one remote agent and
    ticked the tool means "let the agent use it", and making them also type its
    name is a configuration step that can only be got wrong.

    Returns a LIST because one tool entry can produce several tools, the same
    shape MCP uses. An empty list is the honest answer when nothing is
    configured — it leaves the agent without a capability it was never actually
    given, rather than with one that errors on first use.
    """
    from src.db.session import get_isolated_db_session
    from src.services.a2a.agent_service import A2AAgentService
    from src.services.tools.a2a_agent_tool import A2AAgentTool

    config = tool_config or {}
    wanted = config.get("agent_name") or config.get("agent_names")
    if isinstance(wanted, str):
        wanted = [wanted]

    if not group_ids:
        logger.warning("[A2A] No group context; not building remote-agent tools.")
        return []

    tools: List[Any] = []
    try:
        async with get_isolated_db_session() as session:
            service = A2AAgentService(session)
            repository = service.repository
            rows = await repository.list_enabled_for_group(group_ids)
            if wanted:
                names = {str(n) for n in wanted}
                rows = [r for r in rows if r.name in names]

            for row in rows:
                resolved = await service.resolve_for_call(row.name, group_ids)
                if not resolved:
                    continue
                tools.append(
                    A2AAgentTool(
                        agent_name=resolved["name"],
                        interface_url=resolved["interface_url"],
                        api_key=resolved["api_key"],
                        # Only forwarded when the remote is configured for OBO —
                        # sending a user's Databricks token to a remote that
                        # authenticates its own way would leak it for nothing.
                        user_token=(
                            user_token if resolved["auth_type"] == "obo" else None
                        ),
                        timeout_seconds=resolved["timeout_seconds"],
                        skills=resolved["skills"],
                        result_as_answer=result_as_answer,
                    )
                )
    except Exception as exc:  # noqa: BLE001
        logger.error("[A2A] Could not build remote-agent tools: %s", exc)
        return []

    logger.info("[A2A] Built %d remote-agent tool(s).", len(tools))
    return tools
