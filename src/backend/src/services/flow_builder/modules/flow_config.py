"""
Flow configuration module for CrewAI flow execution.

This module handles MCP requirements collection and flow configuration parsing.
"""
import logging
from typing import Dict, Optional

from src.core.logger import LoggerManager

# Initialize logger - use flow logger for flow execution
logger = LoggerManager.get_instance().flow


class FlowConfigManager:
    """
    Manager for flow configuration parsing and MCP requirements collection.
    """

    @staticmethod
    async def collect_agent_mcp_requirements(flow_config, repositories, group_context=None):
        """
        Collect MCP server requirements for each agent based on their assigned tasks in the flow.
        This follows the same pattern as MCPIntegration.collect_agent_mcp_requirements() but for flow structure.

        Args:
            flow_config: Flow configuration with starting points and listeners
            repositories: Dictionary of repositories to load task data
            group_context: Group context for multi-tenant isolation

        Returns:
            Dict mapping agent_id -> list of required MCP server names
        """
        logger.info("Collecting agent MCP requirements from flow tasks")
        agent_requirements = {}
        task_to_mcp = {}  # Map task_id -> list of MCP server names

        try:
            # Get repositories
            task_repo = repositories.get('task') if repositories else None
            crew_repo = repositories.get('crew') if repositories else None

            if not task_repo:
                logger.warning("No task repository provided, cannot collect MCP requirements")
                return agent_requirements

            # Collect task IDs and crew IDs from starting points and listeners
            task_info = {}  # task_id -> {crew_id, task_name}

            # From starting points
            for start_point in flow_config.get('startingPoints', []):
                task_id = start_point.get('taskId')
                crew_id = start_point.get('crewId')
                if task_id:
                    task_info[str(task_id)] = {'crew_id': crew_id, 'task_name': start_point.get('taskName')}

            # From listeners
            for listener in flow_config.get('listeners', []):
                crew_id = listener.get('crewId')
                for task in listener.get('tasks', []):
                    task_id = task.get('id')
                    if task_id:
                        task_info[str(task_id)] = {'crew_id': crew_id, 'task_name': task.get('name')}

            logger.info(f"Found {len(task_info)} unique tasks in flow to check for MCP requirements")

            # Step 1: Collect MCP servers for each task
            for task_id, info in task_info.items():
                try:
                    task_data = await task_repo.get(task_id)
                    if not task_data:
                        logger.warning(f"Task {task_id} not found in database")
                        continue

                    # Extract MCP servers from task's tool_configs
                    task_mcp_servers = []
                    if hasattr(task_data, 'tool_configs') and task_data.tool_configs:
                        tool_configs = task_data.tool_configs
                        if isinstance(tool_configs, dict):
                            mcp_config = tool_configs.get('MCP_SERVERS')
                            if mcp_config:
                                if isinstance(mcp_config, dict):
                                    task_mcp_servers = mcp_config.get('servers', [])
                                elif isinstance(mcp_config, list):
                                    task_mcp_servers = mcp_config

                    if task_mcp_servers:
                        logger.info(f"Task {task_id} ({task_data.name}) requires MCP servers: {task_mcp_servers}")
                        task_to_mcp[task_id] = task_mcp_servers

                        # Try to get agent_id from database first
                        agent_id = task_data.agent_id
                        if agent_id:
                            agent_id = str(agent_id)
                            if agent_id not in agent_requirements:
                                agent_requirements[agent_id] = []
                            for server in task_mcp_servers:
                                if server and server not in agent_requirements[agent_id]:
                                    agent_requirements[agent_id].append(server)
                            logger.info(f"Mapped MCP servers to agent {agent_id} from task.agent_id")
                        else:
                            # Store for later resolution from crew structure
                            info['mcp_servers'] = task_mcp_servers
                            logger.info(f"Task {task_id} has MCP servers but no agent_id in database, will resolve from crew structure")

                except Exception as e:
                    logger.error(f"Error processing task {task_id} for MCP requirements: {e}", exc_info=True)

            # Step 2: For tasks without agent_id, resolve from crew structure
            if crew_repo:
                for task_id, info in task_info.items():
                    if 'mcp_servers' not in info:
                        continue  # Already resolved

                    crew_id = info.get('crew_id')
                    if not crew_id:
                        logger.warning(f"Task {task_id} has MCP servers but no crew_id to resolve agent")
                        continue

                    try:
                        crew_data = await crew_repo.get(crew_id)
                        if not crew_data or not crew_data.nodes:
                            logger.warning(f"Crew {crew_id} not found or has no nodes")
                            continue

                        # Find agent for this task in crew structure
                        agent_id = None
                        for node in crew_data.nodes:
                            node_id = node.get('id', '')
                            node_uuid = node_id.split('-', 1)[1] if '-' in node_id else node_id

                            if node.get('type') == 'taskNode' and node_uuid == task_id:
                                # Find edge connecting agent to this task
                                if crew_data.edges:
                                    for edge in crew_data.edges:
                                        if edge.get('target') == node.get('id'):
                                            agent_node_id = edge.get('source')
                                            # Find the agent node
                                            for agent_node in crew_data.nodes:
                                                if agent_node.get('id') == agent_node_id and agent_node.get('type') == 'agentNode':
                                                    agent_full_id = agent_node.get('id', '')
                                                    agent_id = agent_full_id.split('-', 1)[1] if '-' in agent_full_id else agent_full_id
                                                    if not agent_id:
                                                        agent_id = agent_node.get('data', {}).get('id') or agent_node.get('data', {}).get('agentId')
                                                    break
                                            if agent_id:
                                                break
                                break

                        if agent_id:
                            agent_id = str(agent_id)
                            if agent_id not in agent_requirements:
                                agent_requirements[agent_id] = []

                            for server in info['mcp_servers']:
                                if server and server not in agent_requirements[agent_id]:
                                    agent_requirements[agent_id].append(server)
                            logger.info(f"Resolved agent {agent_id} for task {task_id} from crew structure, mapped MCP servers: {info['mcp_servers']}")
                        else:
                            logger.warning(f"Could not resolve agent for task {task_id} from crew structure")

                    except Exception as e:
                        logger.error(f"Error resolving agent for task {task_id} from crew {crew_id}: {e}", exc_info=True)

            # Log collected requirements
            for agent_id, servers in agent_requirements.items():
                logger.info(f"Agent {agent_id} requires MCP servers from tasks: {servers}")

            return agent_requirements

        except Exception as e:
            logger.error(f"Error collecting agent MCP requirements from flow: {e}", exc_info=True)
            return {}
