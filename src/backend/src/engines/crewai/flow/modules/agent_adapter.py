"""
Agent configuration module for CrewAI flow execution.

This module handles the configuration of agents for CrewAI flows.
"""
import json

from src.core.logger import LoggerManager
from src.engines.crewai.tools.tool_factory import ToolFactory

# Initialize logger
logger = LoggerManager.get_instance().flow

class AgentConfig:
    """
    Helper class for configuring agents in CrewAI flows.
    """
    
    @staticmethod
    async def configure_agent_and_tools(agent_data, flow_data=None, repositories=None, group_context=None, crew_tool_configs=None):
        """
        Configure an agent with its associated tools.

        Args:
            agent_data: Agent data from the database
            flow_data: Flow data for context (optional)
            repositories: Dictionary of repositories (optional)
            group_context: Group context for multi-tenant isolation (optional)
            crew_tool_configs: Tool configs from crew node data to override agent_data.tool_configs (optional)

        Returns:
            Agent: A properly configured CrewAI Agent instance
        """
        if not agent_data:
            logger.warning("No agent data provided for configuration")
            return None
            
        try:
            logger.info(f"Configuring agent: {agent_data.name}")
            
            # Build the per-agent ToolFactory (with api_keys_service for DB-backed
            # API keys). Flow-specific: flow has no injected tool_service.
            factory_config = {}
            if group_context and getattr(group_context, 'primary_group_id', None):
                factory_config['group_id'] = group_context.primary_group_id
                logger.info(f"Creating ToolFactory with group_id: {group_context.primary_group_id}")

            tool_factory = None
            try:
                from src.db.session import request_scoped_session
                from src.services.api_keys_service import ApiKeysService
                async with request_scoped_session() as session:
                    api_keys_service = ApiKeysService(session, group_id=factory_config.get('group_id'))
                    tool_factory = await ToolFactory.create(
                        config=factory_config, api_keys_service=api_keys_service
                    )
                    logger.info(f"Created ToolFactory with api_keys_service for agent {agent_data.name}")
            except Exception as e:
                logger.warning(f"Error creating ToolFactory with api_keys_service: {e}, falling back to basic factory")
                tool_factory = ToolFactory(factory_config)
                try:
                    await tool_factory.initialize()
                except Exception as init_error:
                    logger.warning(f"Error initializing ToolFactory: {init_error}")

            # Use crew_tool_configs if provided, otherwise agent_data.tool_configs.
            effective_tool_configs = crew_tool_configs if crew_tool_configs is not None else (
                agent_data.tool_configs if hasattr(agent_data, 'tool_configs') else None
            )

            # Gather tool IDS from the agent's own list, else from the flow graph
            # (flow-specific sources). The shared builder creates the instances.
            tool_ids = []
            if hasattr(agent_data, 'tools') and agent_data.tools:
                tool_ids = AgentConfig._normalize_tools_list(agent_data.tools)
            if not tool_ids and flow_data and hasattr(flow_data, 'nodes'):
                tool_ids = AgentConfig._get_tool_ids_from_flow_nodes(agent_data, flow_data)

            # Build the MCP config dict (flow constructs it with group attribution).
            mcp_config = None
            if effective_tool_configs:
                mcp_config = {
                    'tool_configs': effective_tool_configs,
                    'name': agent_data.name,
                    'role': agent_data.role,
                }
                if group_context:
                    if getattr(group_context, 'primary_group_id', None):
                        mcp_config['group_id'] = group_context.primary_group_id
                    elif getattr(group_context, 'group_ids', None):
                        mcp_config['group_id'] = group_context.group_ids[0]
                    else:
                        mcp_config['group_id'] = group_context
                elif getattr(agent_data, 'group_id', None):
                    mcp_config['group_id'] = agent_data.group_id

            # Keep UserContext in sync for any downstream consumers.
            if group_context:
                try:
                    from src.utils.user_context import UserContext
                    UserContext.set_group_context(group_context)
                except Exception as e:
                    logger.warning(f"Could not set group context: {e}")

            # Resolve tools + build the agent via the SINGLE shared orchestrator
            # (the same one the crew path uses). Flow supplies its factory + the
            # graph-sourced ids; everything downstream is shared.
            from src.engines.crewai.common.agent_tools import build_agent_with_tools
            spec = AgentConfig._agent_data_to_spec(agent_data)
            group_id = (getattr(group_context, 'primary_group_id', None) if group_context else None) or 'default'
            agent = await build_agent_with_tools(
                spec,
                group_id=group_id,
                default_model='databricks-llama-4-maverick',
                label=getattr(agent_data, 'name', '?'),
                tool_ids=tool_ids,
                tool_factory=tool_factory,
                tool_configs=effective_tool_configs,
                tool_service=None,
                mcp_config=mcp_config,
                mcp_call_config=mcp_config,
                extra_kwargs={'config': AgentConfig._resolve_agent_config(agent_data)},
                custom_attrs={'_kasal_memory_disabled': getattr(agent_data, 'memory', None) is False},
            )
            logger.info(f"Successfully configured agent: {agent_data.name} with {len(tool_ids)} sourced tool id(s)")
            logger.info(f"Agent {agent_data.name} memory disabled: {agent._kasal_memory_disabled}")

            # We no longer add default tools - respect the agent configuration.
            return agent
            
        except Exception as e:
            logger.error(f"Error configuring agent {getattr(agent_data, 'name', 'unknown')}: {e}", exc_info=True)
            return None
    
    @staticmethod
    def _normalize_tools_list(tools_data):
        """Convert tools data to a normalized list of tool IDs"""
        agent_tools = []
        
        if isinstance(tools_data, list):
            agent_tools = [str(tool_id) for tool_id in tools_data]
        elif isinstance(tools_data, str):
            # Try to convert to list if it's a string (e.g., JSON)
            try:
                agent_tools = [str(tool_id) for tool_id in json.loads(tools_data)]
            except Exception as e:
                logger.error(f"Error parsing tools string: {e}")
        
        return agent_tools
    
    @staticmethod
    def _get_tool_ids_from_flow_nodes(agent_data, flow_data):
        """Extract tool IDS for an agent from the flow graph nodes (flow-specific
        source). The shared builder turns these ids into tool instances."""
        try:
            nodes = flow_data.nodes
            # Check if nodes is a string and try to parse it
            if isinstance(nodes, str):
                nodes = json.loads(nodes)

            agent_id = str(getattr(agent_data, 'id', ''))
            if agent_id:
                agent_node_id = f"agent-{agent_id}"
                for node in nodes:
                    if node.get('id') == agent_node_id and 'data' in node:
                        node_tools = node.get('data', {}).get('tools', [])
                        if node_tools:
                            logger.info(f"Found tools in node data for agent {agent_id}: {node_tools}")
                            return AgentConfig._normalize_tools_list(node_tools)
                        break
        except Exception as e:
            logger.error(f"Error looking for tools in flow nodes: {e}", exc_info=True)
        return []
    
    @staticmethod
    def _agent_data_to_spec(agent_data):
        """Map a flow agent ORM object to the crew-style spec dict the shared
        agent builder consumes, so flow agents get the SAME LLM build + kwargs
        assembly (and defaults) as crew agents. Only fields present and non-None
        are included — the builder applies the crew defaults otherwise."""
        spec = {
            'role': agent_data.role,
            'goal': agent_data.goal,
            'backstory': agent_data.backstory,
            'allow_delegation': getattr(agent_data, 'allow_delegation', False),
        }
        # LLM: explicit agent_data.llm (str/dict), else fall back to agent_data.model.
        if getattr(agent_data, 'llm', None):
            spec['llm'] = agent_data.llm
        else:
            model_name = getattr(agent_data, 'model', None)
            if isinstance(model_name, str) and model_name:
                spec['llm'] = model_name
        temperature = getattr(agent_data, 'temperature', None)
        if temperature is not None:
            spec['temperature'] = temperature
        # Crew-parity scalar fields (flow now honors these when set).
        for key in (
            'verbose', 'cache', 'max_retry_limit',
            'max_iter', 'max_rpm', 'code_execution_mode',
            'max_context_window_size', 'max_tokens',
            'reasoning', 'max_reasoning_attempts',
            'inject_date', 'date_format',
        ):
            val = getattr(agent_data, key, None)
            if val is not None:
                spec[key] = val
        for key in ('system_template', 'prompt_template', 'response_template'):
            val = getattr(agent_data, key, None)
            if val:
                spec[key] = val
        return spec

    @staticmethod
    def _resolve_agent_config(agent_data):
        """Flow-only Agent ``config`` dict (crew never sets this), parsed from
        ``agent_data.config`` (dict or JSON string); empty dict by default."""
        cfg = {}
        raw = getattr(agent_data, 'config', None)
        if raw is None:
            return cfg
        try:
            if isinstance(raw, dict):
                cfg = raw
            elif isinstance(raw, str) and raw.strip():
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    cfg = parsed
                else:
                    logger.warning(f"Parsed config is not a dictionary for agent: {agent_data.name}")
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse config string for agent {agent_data.name}")
        except Exception as e:
            logger.warning(f"Error processing config for agent {agent_data.name}: {e}")
        return cfg 