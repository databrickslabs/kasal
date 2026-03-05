"""
MCP Integration Module

This module handles the high-level business logic for the three-tier MCP configuration system.
It provides clean interfaces for crew preparation, agent helpers, and task helpers while
centralizing all MCP-related configuration logic.

Three-Tier MCP System:
1. Global MCP Servers - Available to all agents/tasks (highest coverage)
2. Agent-Level MCP Servers - Specific to individual agents  
3. Task-Level MCP Servers - Most specific (highest priority)

Priority Order: Task-level > Agent-level > Global
Effective servers = Global ∪ Agent-specific ∪ Task-specific (deduplicated)
"""

import logging
import os
from typing import List, Dict, Any, Optional, Set
from src.core.exceptions import MCPConnectionError
from src.core.logger import LoggerManager
from src.engines.crewai.tools.mcp_handler import create_crewai_tool_from_mcp

# Get logger from the centralized logging system
# Use flow logger if running in flow subprocess mode, otherwise use crew logger
logger_manager = LoggerManager.get_instance()
if os.environ.get('FLOW_SUBPROCESS_MODE', 'false').lower() == 'true':
    logger = logger_manager.flow
else:
    logger = logger_manager.crew


class MCPIntegration:
    """
    High-level MCP integration for the three-tier configuration system.

    This class handles:
    - Resolving effective MCP servers based on priority rules
    - Creating MCP tools for agents and tasks
    - Managing global vs explicit server configurations
    - Providing clean interfaces for crew components

    Class-level warnings list collects MCP connection errors so they can
    be surfaced in the execution trace/UI rather than silently swallowed.
    """

    # Collect warnings during MCP tool creation for UI visibility
    _warnings: List[str] = []

    @classmethod
    def reset_warnings(cls) -> None:
        """Reset the warnings list (call at the start of each execution)."""
        cls._warnings = []

    @classmethod
    def get_warnings(cls) -> List[str]:
        """Get collected MCP warnings."""
        return list(cls._warnings)

    @classmethod
    def add_warning(cls, warning: str) -> None:
        """Add a warning message."""
        cls._warnings.append(warning)
        logger.warning(f"[MCP WARNING] {warning}")
    
    @staticmethod
    async def resolve_effective_mcp_servers(
        explicit_servers: List[str],
        mcp_service,
        include_global: bool = True,
        group_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Resolve effective MCP servers: all enabled servers + any explicitly selected ones.

        All enabled servers are automatically available to all agents/tasks.
        Explicit selections add servers that may not be enabled globally.

        Args:
            explicit_servers: List of explicitly selected server names
            mcp_service: MCPService instance for fetching server details
            include_global: Whether to include all enabled servers (kept for API compat)
            group_id: Optional group_id for workspace scoping

        Returns:
            List of effective MCP server configurations (deduplicated)
        """
        try:
            by_name: Dict[str, Dict[str, Any]] = {}

            # 1. Add all enabled servers (they are all available to all agents/tasks)
            if include_global:
                enabled_response = await mcp_service.get_enabled_servers()
                for server in enabled_response.servers:
                    payload = server.model_dump()
                    name = payload.get('name')
                    if name is None:
                        continue
                    if name not in by_name:
                        by_name[name] = payload
                    else:
                        # Prefer group-scoped over base
                        existing = by_name[name]
                        if (existing.get('group_id') is None) and (payload.get('group_id') == group_id):
                            by_name[name] = payload
                logger.info(f"Added {len(enabled_response.servers)} enabled MCP servers")

            # 2. Add explicit servers (group-aware, deduplicated)
            if explicit_servers:
                explicit_server_configs = await mcp_service.get_servers_by_names_group_aware(explicit_servers, group_id)
                for server in explicit_server_configs:
                    payload = server.model_dump()
                    name = payload.get('name')
                    if name is None:
                        continue
                    if name not in by_name:
                        by_name[name] = payload
                    else:
                        existing = by_name[name]
                        if (existing.get('group_id') is None) and (payload.get('group_id') == group_id):
                            by_name[name] = payload
                logger.info(f"Added {len(explicit_server_configs)} explicit MCP servers")

            # 3. Finalize list
            effective_servers = list(by_name.values())
            server_list = [server['name'] for server in effective_servers]
            logger.info(f"Effective MCP servers: {server_list}")
            return effective_servers

        except Exception as e:
            logger.error(f"Error resolving effective MCP servers: {str(e)}")
            return []
    
    @staticmethod  
    async def collect_agent_mcp_requirements(
        config: Dict[str, Any]
    ) -> Dict[str, List[str]]:
        """
        Collect MCP server requirements for each agent based on their assigned tasks.
        
        Args:
            config: Complete crew configuration
            
        Returns:
            Dict mapping agent_id -> list of required MCP server names
        """
        try:
            agent_requirements = {}
            
            # Process each task to collect MCP requirements
            for task_config in config.get('tasks', []):
                task_mcp_servers = MCPIntegration._extract_mcp_servers_from_config(
                    task_config.get('tool_configs', {})
                )
                
                if task_mcp_servers:
                    agent_ref = task_config.get('agent')
                    if agent_ref:
                        # Find the actual agent ID for this reference
                        agent_id = MCPIntegration._resolve_agent_reference(agent_ref, config)
                        if agent_id:
                            if agent_id not in agent_requirements:
                                agent_requirements[agent_id] = []
                            
                            # Add task MCP servers to agent requirements (deduplicated)
                            for server in task_mcp_servers:
                                if server not in agent_requirements[agent_id]:
                                    agent_requirements[agent_id].append(server)
            
            logger.info(f"Collected MCP requirements for {len(agent_requirements)} agents")
            for agent_id, servers in agent_requirements.items():
                logger.info(f"Agent {agent_id} requires MCP servers: {servers}")
                
            return agent_requirements
            
        except Exception as e:
            logger.error(f"Error collecting agent MCP requirements: {str(e)}")
            return {}
    
    @staticmethod
    async def create_mcp_tools_for_agent(
        agent_config: Dict[str, Any],
        agent_key: str,
        mcp_service,
        config: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """
        Create MCP tools for a specific agent based on their configuration.
        
        Args:
            agent_config: Agent configuration dictionary
            agent_key: Agent identifier
            mcp_service: MCPService instance
            
        Returns:
            List of CrewAI-compatible MCP tools
        """
        try:
            # Extract MCP server names from agent configuration
            explicit_servers = MCPIntegration._extract_mcp_servers_from_config(
                agent_config.get('tool_configs', {})
            )
            
            logger.info(f"Creating MCP tools for agent {agent_key} with explicit servers: {explicit_servers}")
            
            # Resolve effective servers (global + explicit)
            effective_servers = await MCPIntegration.resolve_effective_mcp_servers(
                explicit_servers, mcp_service, include_global=True, group_id=(config.get('group_id') if isinstance(config, dict) else None)
            )

            if not effective_servers:
                logger.info(f"No effective MCP servers for agent {agent_key}")
                return []

            # Extract user_token from config for OBO authentication
            user_token = config.get('user_token') if isinstance(config, dict) else None

            # Create tools for each effective server
            mcp_tools = []
            for server in effective_servers:
                try:
                    group_id = config.get('group_id') if isinstance(config, dict) else None
                    server_tools = await MCPIntegration._create_tools_for_server(
                        server, agent_key, mcp_service, user_token=user_token, group_id=group_id
                    )
                    mcp_tools.extend(server_tools)

                except Exception as e:
                    logger.error(f"Error creating tools for server {server.get('name', 'unknown')}: {str(e)}")
                    continue

            logger.info(f"Created {len(mcp_tools)} MCP tools for agent {agent_key}")
            return mcp_tools

        except Exception as e:
            logger.error(f"Error creating MCP tools for agent {agent_key}: {str(e)}")
            return []

    @staticmethod
    async def create_mcp_tools_for_task(
        task_config: Dict[str, Any],
        task_key: str,
        mcp_service,
        config: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """
        Create MCP tools for a specific task based on their configuration.
        
        Args:
            task_config: Task configuration dictionary
            task_key: Task identifier
            mcp_service: MCPService instance
            
        Returns:
            List of CrewAI-compatible MCP tools
        """
        try:
            # Extract MCP server names from task configuration
            explicit_servers = MCPIntegration._extract_mcp_servers_from_config(
                task_config.get('tool_configs', {})
            )
            
            logger.info(f"Creating MCP tools for task {task_key} with explicit servers: {explicit_servers}")
            
            # For tasks, we include both global and explicit servers
            effective_servers = await MCPIntegration.resolve_effective_mcp_servers(
                explicit_servers, mcp_service, include_global=True, group_id=(config.get('group_id') if isinstance(config, dict) else None)
            )

            if not effective_servers:
                logger.info(f"No effective MCP servers for task {task_key}")
                return []

            # Extract user_token from config for OBO authentication
            user_token = config.get('user_token') if isinstance(config, dict) else None

            # Create tools for each effective server
            mcp_tools = []
            for server in effective_servers:
                try:
                    group_id = config.get('group_id') if isinstance(config, dict) else None
                    server_tools = await MCPIntegration._create_tools_for_server(
                        server, f"task_{task_key}", mcp_service, user_token=user_token, group_id=group_id
                    )
                    mcp_tools.extend(server_tools)
                    
                except Exception as e:
                    logger.error(f"Error creating tools for server {server.get('name', 'unknown')}: {str(e)}")
                    continue
            
            logger.info(f"Created {len(mcp_tools)} MCP tools for task {task_key}")
            return mcp_tools
            
        except Exception as e:
            logger.error(f"Error creating MCP tools for task {task_key}: {str(e)}")
            return []
    
    @staticmethod
    def _extract_mcp_servers_from_config(tool_configs: Dict[str, Any]) -> List[str]:
        """
        Extract MCP server names from tool_configs.
        
        Args:
            tool_configs: Tool configurations dictionary
            
        Returns:
            List of MCP server names
        """
        try:
            mcp_config = tool_configs.get('MCP_SERVERS')
            if not mcp_config:
                return []
            
            # Handle both new dict format and legacy array format
            if isinstance(mcp_config, dict):
                servers = mcp_config.get('servers', [])
            elif isinstance(mcp_config, list):
                servers = mcp_config
            else:
                return []
            
            # Ensure we return a list of strings, stripping whitespace
            return [str(server).strip() for server in servers if server]
            
        except Exception as e:
            logger.error(f"Error extracting MCP servers from config: {str(e)}")
            return []
    
    @staticmethod
    def _resolve_agent_reference(agent_ref: str, config: Dict[str, Any]) -> Optional[str]:
        """
        Resolve an agent reference to the actual agent ID.
        
        Args:
            agent_ref: Agent reference (could be name, id, or role)
            config: Complete crew configuration
            
        Returns:
            Resolved agent ID or None if not found
        """
        try:
            # Try to find agent by exact match on various fields
            for agent_config in config.get('agents', []):
                agent_id = agent_config.get('id')
                agent_name = agent_config.get('name')
                agent_role = agent_config.get('role')
                
                if agent_ref in [agent_id, agent_name, agent_role]:
                    return agent_id or agent_name or agent_role
            
            # If no exact match, return the reference as-is
            return agent_ref
            
        except Exception as e:
            logger.error(f"Error resolving agent reference {agent_ref}: {str(e)}")
            return None
    
    @staticmethod
    async def _create_tools_for_server(
        server: Dict[str, Any],
        context_key: str,
        mcp_service,
        user_token: Optional[str] = None,
        group_id: Optional[str] = None
    ) -> List[Any]:
        """
        Create tools for a specific MCP server.

        Args:
            server: MCP server configuration
            context_key: Context identifier for logging (agent/task key)
            mcp_service: MCPService instance
            user_token: Optional user access token for OBO authentication
            group_id: Optional group_id for PAT/SPN fallback authentication

        Returns:
            List of CrewAI-compatible tools
        """
        tools = []
        server_name = server.get('name', 'unknown')

        try:
            logger.info(f"Creating tools for MCP server '{server_name}' (context: {context_key})")

            # Create server connection parameters
            server_params = {
                "url": server.get('server_url'),
                "timeout_seconds": server.get('timeout_seconds', 30),
                "max_retries": server.get('max_retries', 3),
                "rate_limit": server.get('rate_limit', 60),
                "auth_type": server.get('auth_type', 'api_key'),
                "user_token": user_token,
                "group_id": group_id,
                "headers": {}
            }

            # Detect Databricks-hosted MCP URLs (external MCP proxy)
            # These always require Databricks auth, regardless of auth_type setting
            server_url = server.get('server_url', '')
            is_databricks_mcp = '/api/2.0/mcp/' in server_url

            # Add authentication headers
            auth_type = server.get('auth_type', 'api_key')

            if is_databricks_mcp or auth_type in ('databricks_spn', 'databricks_obo'):
                # Databricks MCP proxy endpoints require Databricks auth (OBO → PAT → SPN)
                from src.utils.databricks_auth import get_auth_context
                auth_context = await get_auth_context(user_token=user_token, group_id=group_id)
                if auth_context and auth_context.token:
                    server_params["headers"]["Authorization"] = f"Bearer {auth_context.token}"
                    # Override auth_type so the adapter knows SPN fallback is available
                    server_params["auth_type"] = "databricks_spn"
                    logger.info(f"MCP server '{server_name}': Using {auth_context.auth_method} authentication (databricks MCP)")
                else:
                    mcp_err = MCPConnectionError(
                        server_name=server_name,
                        server_url=server_url,
                        detail=f"MCP server '{server_name}': No authentication available (tried OBO, PAT, SPN). On deployed apps, check DATABRICKS_CLIENT_ID/SECRET env vars.",
                    )
                    logger.error(mcp_err.detail)
                    MCPIntegration.add_warning(mcp_err.detail)
                    return []
            elif auth_type == 'api_key' and server.get('api_key'):
                # Non-Databricks MCP servers with their own API key
                server_params["headers"]["Authorization"] = f"Bearer {server['api_key']}"
                logger.info(f"MCP server '{server_name}': Using api_key authentication")
            
            # Get or create MCP adapter
            from src.engines.crewai.tools.mcp_handler import get_or_create_mcp_adapter
            adapter_id = f"{context_key}_server_{server.get('id', server_name)}"
            mcp_adapter = await get_or_create_mcp_adapter(server_params, adapter_id)
            
            if not mcp_adapter or not hasattr(mcp_adapter, 'tools'):
                logger.warning(f"No tools available from MCP server '{server_name}'")
                MCPIntegration.add_warning(f"MCP server '{server_name}': adapter not available")
                return []

            # Check for initialization errors on the adapter (MCPConnectionError)
            if hasattr(mcp_adapter, 'initialization_error') and mcp_adapter.initialization_error:
                err = mcp_adapter.initialization_error
                detail = err.detail if isinstance(err, MCPConnectionError) else str(err)
                MCPIntegration.add_warning(detail)

            # Get tools from the adapter
            server_tools = mcp_adapter.tools
            logger.info(f"Got {len(server_tools)} tools from MCP server '{server_name}'")
            
            # Create CrewAI tools from MCP tool dictionaries
            for tool in server_tools:
                try:
                    wrapped_tool = create_crewai_tool_from_mcp(tool)
                    
                    # Add server name prefix to tool name for identification
                    tool_name = tool.get('name', 'unknown') if isinstance(tool, dict) else getattr(tool, 'name', 'unknown')
                    if not tool_name.startswith(f"{server_name}_"):
                        tool_name = f"{server_name}_{tool_name}"
                        if hasattr(wrapped_tool, 'name'):
                            wrapped_tool.name = tool_name

                    tools.append(wrapped_tool)
                    logger.debug(f"Created tool '{tool_name}' from server '{server_name}'")
                    
                except Exception as e:
                    logger.error(f"Error wrapping tool from server '{server_name}': {str(e)}")
                    continue
            
            logger.info(f"Successfully created {len(tools)} tools for server '{server_name}'")
            return tools
            
        except Exception as e:
            mcp_err = MCPConnectionError(
                server_name=server_name,
                server_url=server.get('server_url', ''),
                detail=f"MCP server '{server_name}': {str(e)}",
                cause=e,
            )
            logger.error(f"Error creating tools for server '{server_name}': {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            MCPIntegration.add_warning(mcp_err.detail)
            return []
    
    @staticmethod
    async def get_mcp_settings(mcp_service) -> Dict[str, bool]:
        """
        Get MCP global settings.
        
        Args:
            mcp_service: MCPService instance
            
        Returns:
            Dict with global MCP settings
        """
        try:
            settings = await mcp_service.get_settings()
            return {
                'global_enabled': settings.global_enabled,
                'individual_enabled': getattr(settings, 'individual_enabled', True)
            }
        except Exception as e:
            logger.error(f"Error getting MCP settings: {str(e)}")
            return {'global_enabled': False, 'individual_enabled': True}
    
    @staticmethod
    def validate_mcp_configuration(config: Dict[str, Any]) -> bool:
        """
        Validate MCP configuration structure.
        
        Args:
            config: Configuration to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Basic structure validation
            if not isinstance(config, dict):
                return False
            
            # Validate agent configurations
            for agent_config in config.get('agents', []):
                if not isinstance(agent_config, dict):
                    return False
                    
                tool_configs = agent_config.get('tool_configs', {})
                if tool_configs and not isinstance(tool_configs, dict):
                    return False
            
            # Validate task configurations
            for task_config in config.get('tasks', []):
                if not isinstance(task_config, dict):
                    return False
                    
                tool_configs = task_config.get('tool_configs', {})
                if tool_configs and not isinstance(tool_configs, dict):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating MCP configuration: {str(e)}")
            return False