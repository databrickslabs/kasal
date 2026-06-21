"""Shared agent tool-sourcing + agent assembly used by BOTH the crew path
(``agent_adapter.create_agent``) and the flow path
(``flow.modules.agent_adapter.configure_agent_and_tools``).

The two paths differ only in WHERE tool ids come from (crew: a config dict +
a DB ``tool_service``; flow: ORM objects + the flow graph + a per-agent
``ToolFactory``). That difference is expressed through the parameters here —
the resolution + MCP wiring + agent construction all live in one place.
"""
from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager
from src.engines.crewai.common.agent_builder import build_agent

logger = LoggerManager.get_instance().crew


def resolve_tool_override(tool_factory, tool_id, tool_configs):
    """Resolve the per-tool config override for ``tool_id`` from ``tool_configs``.

    Tries a direct lookup by id (string form) first, then resolves a numeric id
    to the tool's title via the factory and looks that up. Returns the override
    dict or None.
    """
    if not tool_configs:
        return None
    override = tool_configs.get(str(tool_id))
    if override:
        return override
    try:
        tool_info = tool_factory.get_tool_info(tool_id)
    except Exception:
        return None
    if tool_info:
        title = getattr(tool_info, 'title', None)
        if title:
            return tool_configs.get(title)
    return None


async def add_mcp_tools(mcp_config: Dict[str, Any], label: str, call_config: Any) -> List[Any]:
    """Resolve MCP tools for an agent from ``mcp_config['tool_configs']``.

    Cheap dict check first: with no explicit MCP servers the integration returns
    [] without ever opening a DB session (the common case). Shared by both paths.
    Never raises — MCP wiring is best-effort.
    """
    tools: List[Any] = []
    try:
        from src.engines.crewai.tools.mcp_integration import MCPIntegration

        if MCPIntegration._extract_mcp_servers_from_config((mcp_config or {}).get('tool_configs', {})):
            from src.services.mcp_service import MCPService
            from src.db.session import request_scoped_session
            async with request_scoped_session() as session:
                mcp_service = MCPService(session)
                mcp_tools = await MCPIntegration.create_mcp_tools_for_agent(
                    mcp_config, label, mcp_service, call_config
                )
                tools.extend(mcp_tools)
                logger.info(f"Added {len(mcp_tools)} MCP tools to agent {label}")
    except Exception as e:
        import traceback
        logger.error(f"Error adding MCP tools to agent {label}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    return tools


async def resolve_agent_tools(
    tool_ids: List[Any],
    tool_factory,
    *,
    tool_configs: Optional[Dict[str, Any]] = None,
    tool_service: Any = None,
    label: str = "",
) -> List[Any]:
    """Create tool instances from ``tool_ids``.

    - Crew (``tool_service`` provided): ids are mapped to names via the service,
      ``result_as_answer`` comes from ``tool_service.get_tool_config_by_name``,
      and the override comes from ``tool_configs[name]``.
    - Flow (``tool_service`` is None): ids are used directly and the override is
      resolved via ``resolve_tool_override`` (id → title fallback).

    Tools that come back as an MCP ``(True, [tools])`` tuple are expanded. Mirrors
    the prior per-path behavior exactly.
    """
    tools: List[Any] = []
    tool_configs = tool_configs or {}
    if not tool_ids:
        return tools

    # Build the list of (identifier_for_create_tool, config_key) pairs. A failure
    # in id→name resolution must not fail agent creation (legacy crew behavior).
    try:
        if tool_service is not None:
            from src.engines.crewai.helpers.tool_helpers import resolve_tool_ids_to_names
            names = await resolve_tool_ids_to_names(tool_ids, tool_service)
            logger.info(f"Resolved tool names for agent {label}: {names}")
            identifiers = [(n, n) for n in names if n]
        else:
            identifiers = [(tid, tid) for tid in tool_ids]
    except Exception as e:
        logger.error(f"Error resolving tool ids for agent {label}: {e}")
        return tools

    if not tool_factory:
        # Legacy crew fallback: without a factory, surface the resolved names
        # (may not work with CrewAI, but preserves prior behavior).
        if tool_service is not None:
            tools.extend([ident for ident, _ in identifiers])
            logger.warning("No tool_factory provided, using tool names which may not work with CrewAI")
        else:
            logger.warning("No tool_factory provided; cannot create tool instances")
        return tools

    for identifier, config_key in identifiers:
        try:
            result_as_answer = False
            if tool_service is not None and hasattr(tool_service, 'get_tool_config_by_name'):
                tool_config = await tool_service.get_tool_config_by_name(config_key) or {}
                result_as_answer = tool_config.get('result_as_answer', False)
                override = tool_configs.get(config_key, {})
            else:
                override = resolve_tool_override(tool_factory, identifier, tool_configs) or {}

            tool_instance = tool_factory.create_tool(
                identifier,
                result_as_answer=result_as_answer,
                tool_config_override=override,
            )
            if not tool_instance:
                logger.error(f"Could not create tool instance for {identifier} (agent {label})")
                continue

            # An MCP tool factory result is a (True, [tools]) tuple — expand it.
            if isinstance(tool_instance, tuple) and len(tool_instance) == 2 and tool_instance[0] is True:
                mcp_tools = tool_instance[1]
                if mcp_tools == 'mcp_service_adapter':
                    logger.info("MCP service adapter requested but not supported anymore")
                elif isinstance(mcp_tools, list):
                    tools.extend(mcp_tools)
                    logger.info(f"Added {len(mcp_tools)} MCP tools from {identifier} to agent {label}")
                else:
                    logger.warning(f"Unexpected MCP tools format: {mcp_tools}")
            else:
                tools.append(tool_instance)
                logger.info(f"Added tool instance {identifier} to agent {label}")
        except Exception as e:
            logger.error(f"Error creating tool {identifier} for agent {label}: {e}")
    return tools


async def build_agent_with_tools(
    spec: Dict[str, Any],
    *,
    group_id: Optional[str],
    default_model: str = "gpt-4o",
    label: str = "",
    base_tools: Optional[List[Any]] = None,
    tool_ids: Optional[List[Any]] = None,
    tool_factory: Any = None,
    tool_configs: Optional[Dict[str, Any]] = None,
    tool_service: Any = None,
    mcp_config: Optional[Dict[str, Any]] = None,
    mcp_call_config: Any = None,
    extra_kwargs: Optional[Dict[str, Any]] = None,
    custom_attrs: Optional[Dict[str, Any]] = None,
) -> Any:
    """End-to-end agent assembly shared by crew and flow: gather tools (already
    supplied ``base_tools`` + MCP tools + tools resolved from ``tool_ids``) then
    construct the agent via the shared ``build_agent``.

    Each path supplies its own ``tool_factory`` (and optional ``tool_service``)
    and the ``tool_ids`` it gathered from its native source (config dict / flow
    graph); everything downstream is identical.
    """
    tools: List[Any] = list(base_tools or [])

    if mcp_config is not None:
        tools.extend(await add_mcp_tools(mcp_config, label, mcp_call_config))

    tools.extend(
        await resolve_agent_tools(
            tool_ids or [],
            tool_factory,
            tool_configs=tool_configs,
            tool_service=tool_service,
            label=label,
        )
    )

    if tools:
        logger.info(f"Agent {label} will have access to {len(tools)} tool(s)")
    else:
        logger.info(f"Agent {label} will not have any tools")

    return await build_agent(
        spec,
        tools,
        group_id=group_id,
        default_model=default_model,
        label=label,
        extra_kwargs=extra_kwargs,
        custom_attrs=custom_attrs,
    )
