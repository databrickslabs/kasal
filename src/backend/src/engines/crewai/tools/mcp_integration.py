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
from typing import List, Dict, Any, Optional, Set
from src.core.logger import LoggerManager
from src.engines.crewai.tools.mcp_handler import create_crewai_tool_from_mcp

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().crew


class MCPIntegration:
    """
    High-level MCP integration for the three-tier configuration system.
    
    This class handles:
    - Resolving effective MCP servers based on priority rules
    - Creating MCP tools for agents and tasks
    - Managing global vs explicit server configurations
    - Providing clean interfaces for crew components
    """
    
    @staticmethod
    async def resolve_effective_mcp_servers(
        explicit_servers: List[str],
        mcp_service,
        include_global: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Resolve effective MCP servers combining global and explicit selections.
        
        Args:
            explicit_servers: List of explicitly selected server names
            mcp_service: MCPService instance for fetching server details
            include_global: Whether to include globally enabled servers
            
        Returns:
            List of effective MCP server configurations (deduplicated)
        """
        try:
            effective_servers = []
            server_names = set()
            
            # 1. Add global servers first (if enabled)
            if include_global:
                global_response = await mcp_service.get_global_servers()
                for server in global_response.servers:
                    if server.name not in server_names:
                        effective_servers.append(server.model_dump())
                        server_names.add(server.name)
                        
                logger.info(f"Added {len(global_response.servers)} global MCP servers")
            
            # 2. Add explicit servers (deduplicated)
            if explicit_servers:
                explicit_server_configs = await mcp_service.get_servers_by_names(explicit_servers)
                for server in explicit_server_configs:
                    if server.name not in server_names:
                        effective_servers.append(server.model_dump())
                        server_names.add(server.name)
                        
                logger.info(f"Added {len(explicit_server_configs)} explicit MCP servers")
            
            # 3. Log final effective servers
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
        mcp_service
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
                explicit_servers, mcp_service, include_global=True
            )
            
            if not effective_servers:
                logger.info(f"No effective MCP servers for agent {agent_key}")
                return []
            
            # Create tools for each effective server
            mcp_tools = []
            for server in effective_servers:
                try:
                    server_tools = await MCPIntegration._create_tools_for_server(
                        server, agent_key, mcp_service
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
        mcp_service
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
                explicit_servers, mcp_service, include_global=True
            )
            
            if not effective_servers:
                logger.info(f"No effective MCP servers for task {task_key}")
                return []
            
            # Create tools for each effective server
            mcp_tools = []
            for server in effective_servers:
                try:
                    server_tools = await MCPIntegration._create_tools_for_server(
                        server, f"task_{task_key}", mcp_service
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
            
            # Ensure we return a list of strings
            return [str(server) for server in servers if server]
            
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
        mcp_service
    ) -> List[Any]:
        """
        Create tools for a specific MCP server.
        
        Args:
            server: MCP server configuration
            context_key: Context identifier for logging (agent/task key)
            mcp_service: MCPService instance
            
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
                "headers": {}
            }
            
            # Add authentication headers if needed
            if server.get('auth_type') == 'api_key' and server.get('api_key'):
                server_params["headers"]["Authorization"] = f"Bearer {server['api_key']}"
            elif server.get('auth_type') == 'databricks_obo':
                # Handle Databricks OBO authentication
                from src.utils.databricks_auth import get_mcp_auth_headers
                oauth_headers, error = await get_mcp_auth_headers(
                    server.get('server_url'),
                    api_key=server.get('api_key')
                )
                if oauth_headers:
                    server_params["headers"].update(oauth_headers)
                elif error:
                    logger.error(f"Authentication error for server {server_name}: {error}")
                    return []
            
            # Get or create MCP adapter
            from src.engines.crewai.tools.mcp_handler import get_or_create_mcp_adapter
            adapter_id = f"{context_key}_server_{server.get('id', server_name)}"
            mcp_adapter = await get_or_create_mcp_adapter(server_params, adapter_id)
            
            if not mcp_adapter or not hasattr(mcp_adapter, 'tools'):
                logger.warning(f"No tools available from MCP server '{server_name}'")
                return []
            
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
                        prefixed_name = f"{server_name}_{tool_name}"
                        if hasattr(wrapped_tool, 'name'):
                            wrapped_tool.name = prefixed_name
                    
                    tools.append(wrapped_tool)
                    logger.debug(f"Created tool '{prefixed_name}' from server '{server_name}'")
                    
                except Exception as e:
                    logger.error(f"Error wrapping tool from server '{server_name}': {str(e)}")
                    continue
            
            logger.info(f"Successfully created {len(tools)} tools for server '{server_name}'")
            return tools
            
        except Exception as e:
            logger.error(f"Error creating tools for server '{server_name}': {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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