"""
Utilities for Agent configuration, validation, and setup.

This module provides helper functions for working with CrewAI agents.
"""
import os
import re
from typing import Dict, Any, Optional, Tuple, List

from crewai import Agent
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.engines.crewai.helpers.tool_helpers import resolve_tool_ids_to_names
from src.utils.model_config import model_rejects_temperature

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().crew

# NOTE: Knowledge sources are now implemented as tools (DatabricksKnowledgeSearchTool)
# Agents should have the DatabricksKnowledgeSearchTool in their tools list instead of knowledge_sources
# The tool provides direct control over when and how knowledge is searched


# Security: prompt hardening / spotlighting now lives in the shared common
# package so the crew and flow agent builders inject the identical preamble.
# Re-exported here for backward compatibility with existing call sites/tests.
from src.engines.crewai.common.agent_security import (  # noqa: E402,F401
    _SECURITY_PREAMBLE,
    _build_security_preamble,
    inject_security_preamble,
)


# redact_llm_repr now lives in the shared common package (used by both the crew
# and flow agent builders); re-exported here for existing call sites.
from src.engines.crewai.common.agent_builder import redact_llm_repr  # noqa: E402,F401


async def create_agent(
    agent_key: str,
    agent_config: Dict,
    tools: List[Any] = None,
    config: Dict = None,
    tool_service = None,
    tool_factory = None,
    agent_id: Optional[str] = None
) -> Agent:
    """
    Creates an Agent instance from the provided configuration.

    Args:
        agent_key: The unique identifier for the agent
        agent_config: Dictionary containing agent configuration
        tools: List of tools to be available to the agent
        config: Global configuration dictionary containing API keys
        tool_service: Optional tool service for resolving tool IDs to names
        tool_factory: Optional tool factory for creating tools
        agent_id: Optional Kasal agent UUID for knowledge source access control

    Returns:
        Agent: A configured CrewAI Agent instance

    Raises:
        ValueError: If required fields are missing
    """
    logger.info(f"Creating agent {agent_key} with config: {agent_config}")
    
    # Validate required fields
    required_fields = ['role', 'goal', 'backstory']
    for field in required_fields:
        if field not in agent_config:
            raise ValueError(f"Missing required field '{field}' in agent configuration")
        if not agent_config[field]:  # Check if field is empty
            raise ValueError(f"Field '{field}' cannot be empty in agent configuration")
    
    # NOTE: Knowledge sources removed - use DatabricksKnowledgeSearchTool instead
    if 'knowledge_sources' in agent_config:
        logger.warning(f"[CREW] Agent {agent_key} has knowledge_sources configured, but this is deprecated. Use DatabricksKnowledgeSearchTool in the agent's tools list instead.")
        # Remove knowledge_sources from config to avoid confusion
        agent_config = agent_config.copy()
        del agent_config['knowledge_sources']
    
    from src.engines.crewai.common.agent_tools import build_agent_with_tools

    group_id_param = config.get('group_id') if config else None

    # Resolve tools (MCP + tool_service id→name resolution) and build the agent.
    # ALL of this is shared with the flow path via build_agent_with_tools; the
    # crew supplies its tool_service + config-dict tool ids, flow supplies its
    # graph-sourced ids + factory.
    agent = await build_agent_with_tools(
        agent_config,
        group_id=group_id_param,
        default_model="gpt-4o",
        label=agent_key,
        base_tools=tools or [],
        tool_ids=agent_config.get('tools') if tool_service else None,
        tool_factory=tool_factory,
        tool_configs=agent_config.get('tool_configs', {}),
        tool_service=tool_service,
        mcp_config=agent_config,
        mcp_call_config=config,
        custom_attrs={'_agent_key': agent_key},
    )

    # Explicitly check if the llm attribute was set correctly.
    # NOTE: log only the model identifier — never the LLM object itself. Its
    # repr exposes api_key, which holds the live Databricks OBO/PAT token.
    if hasattr(agent, 'llm'):
        llm_model = getattr(agent.llm, 'model', None)
        logger.info(f"Confirmed agent {agent_key} has llm attribute set, model={llm_model}")
    else:
        logger.warning(f"Agent {agent_key} does not have llm attribute after creation!")

    logger.info(f"Successfully created agent {agent_key} with role '{agent_config['role']}'")
    return agent