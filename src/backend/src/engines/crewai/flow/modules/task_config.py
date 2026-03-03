"""
Task configuration module for CrewAI flow execution.

This module handles the configuration of tasks for CrewAI flows.
"""
import logging
import json
from typing import Dict, List, Optional, Any, Union

from src.core.logger import LoggerManager
from src.utils.user_context import GroupContext
from crewai import Task

from src.engines.crewai.tools.tool_factory import ToolFactory

# Initialize logger
logger = LoggerManager.get_instance().flow


def _resolve_tool_override(tool_factory, tool_id, tool_configs):
    """Resolve tool_config_override for a tool by looking up its name in tool_configs.

    Tries a direct lookup by tool_id first, then resolves the numeric ID to a
    tool title and looks that up.

    Args:
        tool_factory: ToolFactory instance used to resolve tool IDs to titles.
        tool_id: The tool identifier (may be a numeric string like '35').
        tool_configs: Dict mapping tool names/IDs to their config overrides.

    Returns:
        The matching override dict, or None.
    """
    if not tool_configs:
        return None
    # Direct lookup by tool_id (string form)
    override = tool_configs.get(str(tool_id))
    if override:
        return override
    # Resolve numeric ID to title and look up by name
    tool_info = tool_factory.get_tool_info(tool_id)
    if tool_info:
        title = getattr(tool_info, 'title', None)
        if title:
            return tool_configs.get(title)
    return None

class TaskConfig:
    """
    Helper class for configuring tasks in CrewAI flows.
    """
    
    @staticmethod
    async def configure_task(task_data, agent=None, task_output_callback=None, flow_data=None, repositories=None, group_context: Optional[GroupContext] = None):
        """
        Configure a task with its associated agent and callbacks.

        Args:
            task_data: Task data from the database
            agent: Pre-configured agent instance (optional)
            task_output_callback: Callback for task output (optional)
            flow_data: Flow data for context (optional)
            repositories: Dictionary of repositories (optional)
            group_context: Group context for multi-tenant tool access (optional)

        Returns:
            Task: A properly configured CrewAI Task instance
        """
        if not task_data:
            logger.warning("No task data provided for configuration")
            return None
            
        try:
            logger.info(f"Configuring task: {task_data.name}")
            
            # Debug logging for task_data
            logger.info(f"Task data type: {type(task_data)}")
            
            # First resolve the agent if not provided
            if agent is None:
                agent = await TaskConfig._resolve_agent_for_task(task_data, flow_data, repositories, group_context)

                if not agent:
                    logger.error(f"No agent provided or configured for task: {task_data.name}")
                    return None
            
            # Check if task has specific tools and add them to the agent
            await TaskConfig._configure_task_tools(task_data, agent, flow_data, group_context)
            
            # Create basic required parameters - this avoids all the complex validation issues
            description = str(task_data.description)
            expected_output = str(task_data.expected_output) if hasattr(task_data, 'expected_output') and task_data.expected_output else ""
            
            # Add markdown instructions if enabled
            if getattr(task_data, 'markdown', False):
                description += "\n\nPlease format the output using Markdown syntax."
                expected_output += "\n\nThe output should be formatted using Markdown."
            
            # Create the task using basic parameters only
            from crewai import Task
            task = Task(
                description=description,
                expected_output=expected_output,
                agent=agent,
                markdown=getattr(task_data, 'markdown', False)
            )
            
            # If task output callback provided, configure it
            if task_output_callback:
                # Use callback instead of output_callback in newer versions of crewAI
                task.callback = task_output_callback

            # Add other parameters one by one if they exist
            if hasattr(task_data, 'async_execution'):
                task.async_execution = bool(task_data.async_execution)
                
            if hasattr(task_data, 'human_input'):
                task.human_input = bool(task_data.human_input)
                
            # Note: We're deliberately NOT setting context here because that's causing problems

            # Handle code-based guardrail if present in task configuration
            # This uses custom guardrails from GuardrailFactory (e.g., CompanyCountGuardrail)
            try:
                guardrail_config = getattr(task_data, 'guardrail', None)

                if guardrail_config:
                    logger.info(f"Task {task_data.name} has code-based guardrail configuration: {guardrail_config}")

                    from src.engines.crewai.guardrails.guardrail_factory import GuardrailFactory
                    from src.engines.crewai.guardrails.guardrail_wrapper import GuardrailWrapper

                    # Convert guardrail config to JSON string if it's a dictionary
                    if isinstance(guardrail_config, dict):
                        guardrail_config = json.dumps(guardrail_config)

                    # Create the guardrail instance
                    guardrail = GuardrailFactory.create_guardrail(guardrail_config)
                    if guardrail:
                        # Create a callable wrapper for this guardrail
                        # Using GuardrailWrapper class instead of a closure because:
                        # CrewAI's LLMGuardrailStartedEvent.__init__ calls inspect.getsource()
                        # on the guardrail function, which fails for closures with
                        # "OSError: could not get source code". The wrapper class is defined
                        # in a source file, so getsource() can inspect it properly.
                        guardrail_wrapper = GuardrailWrapper(guardrail, task_data.name)

                        task.guardrail = guardrail_wrapper
                        task.retry_on_fail = True
                        logger.info(f"Applied code-based guardrail to task {task_data.name}")
                    else:
                        logger.warning(f"Failed to create guardrail for task {task_data.name}, guardrail will not be applied")
            except Exception as guardrail_error:
                logger.warning(f"Error setting up code-based guardrail for task {task_data.name}: {guardrail_error}")
                # Continue without guardrail - task is still valid

            # Handle LLM guardrail if explicitly enabled by the user in config
            # NOTE: LLM guardrail takes priority over code-based guardrail if both exist
            # Read from config (user's explicit choice), not from the column (which
            # stores the LLM-generated suggestion and may not reflect the toggle state)
            try:
                task_config_dict = getattr(task_data, 'config', None) or {}
                llm_guardrail_config = task_config_dict.get('llm_guardrail') if isinstance(task_config_dict, dict) else None

                if llm_guardrail_config:
                    logger.info(f"Task {task_data.name} has LLM guardrail configuration: {llm_guardrail_config}")

                    from crewai.tasks.llm_guardrail import LLMGuardrail
                    from crewai import LLM

                    # Extract configuration
                    if isinstance(llm_guardrail_config, dict):
                        guardrail_description = llm_guardrail_config.get('description', 'Validate the task output')
                        llm_model = llm_guardrail_config.get('llm_model', 'databricks-claude-sonnet-4-5')
                    else:
                        guardrail_description = getattr(llm_guardrail_config, 'description', 'Validate the task output')
                        llm_model = getattr(llm_guardrail_config, 'llm_model', 'databricks-claude-sonnet-4-5')

                    # PROACTIVE GUARDRAIL AUGMENTATION: Inject validation criteria into task description
                    if guardrail_description and guardrail_description != 'Validate the task output':
                        validation_augmentation = (
                            f"\n\n=== VALIDATION REQUIREMENTS ===\n"
                            f"Your output will be validated against these criteria: {guardrail_description}\n"
                            f"Ensure your response meets these requirements to pass validation."
                        )
                        task.description = task.description + validation_augmentation
                        logger.info(f"Augmented task {task_data.name} description with guardrail criteria")

                    # Ensure model has provider prefix for LiteLLM
                    if llm_model and not llm_model.startswith('databricks/'):
                        if llm_model.startswith('databricks-'):
                            llm_model = f"databricks/{llm_model}"

                    # Create and apply LLM guardrail
                    guardrail_llm = LLM(model=llm_model)
                    llm_guardrail = LLMGuardrail(description=guardrail_description, llm=guardrail_llm)
                    task.guardrail = llm_guardrail
                    task.retry_on_fail = True
                    logger.info(f"Applied LLM guardrail to task {task_data.name} using model {llm_model}")

            except Exception as guardrail_error:
                logger.warning(f"Error setting up guardrail for task {task_data.name}: {guardrail_error}")
                # Continue without guardrail - task is still valid

            logger.info(f"Successfully configured task: {task_data.name} with agent role: {agent.role}")

            # No longer adding default tools - we respect the task configuration
            # If a task has no tools assigned, we don't add any by default

            return task
        except Exception as e:
            logger.error(f"Error configuring task {getattr(task_data, 'name', 'unknown')}: {e}", exc_info=True)
            return None

    @staticmethod
    async def _resolve_agent_for_task(task_data, flow_data, repositories, group_context: Optional[GroupContext] = None):
        """
        Resolve the agent for a task from either the task data or flow connections.

        Args:
            task_data: Task data from the database
            flow_data: Optional flow data for looking up connections
            repositories: Optional dictionary of repositories
            group_context: Group context for multi-tenant tool access (optional)

        Returns:
            Agent: The resolved agent for the task
        """
        from src.engines.crewai.flow.modules.agent_config import AgentConfig
        
        # If task has an agent_id, try to get that agent
        if hasattr(task_data, 'agent_id') and task_data.agent_id:
            # Try to get agent from repository first
            agent_data = None
            agent_repo = None if repositories is None else repositories.get('agent')
            
            if agent_repo:
                agent_data = await agent_repo.get(task_data.agent_id)
            
            # Fallback to direct database query if repository not available or agent not found
            if not agent_data:
                try:
                    from src.db.session import request_scoped_session
                    from sqlalchemy import select
                    from src.models.agent import Agent as AgentModel

                    async with request_scoped_session() as db:
                        stmt = select(AgentModel).filter(AgentModel.id == task_data.agent_id)
                        result = await db.execute(stmt)
                        agent_data = result.scalar_one_or_none()
                except Exception as db_error:
                    logger.error(f"Error querying database for agent: {db_error}", exc_info=True)
            
            if agent_data:
                # Configure the agent
                agent = await AgentConfig.configure_agent_and_tools(agent_data, flow_data, repositories, group_context)
                if agent:
                    return agent

                logger.error(f"Failed to configure agent for task: {task_data.name}")
                return None

            logger.error(f"Agent with ID {task_data.agent_id} not found for task: {task_data.name}")
        
        # If no agent_id or agent not found, try to infer from flow edges
        if flow_data and hasattr(flow_data, 'edges'):
            logger.info(f"No agent_id in task data or agent not found, attempting to infer from edges for task {task_data.id}")
            try:
                edges = flow_data.edges
                # Check if edges is a string and try to parse it
                if isinstance(edges, str):
                    edges = json.loads(edges)
                
                # Look for edges where target is this task
                task_node_id = f"task-{task_data.id}"
                for edge in edges:
                    if edge.get('target') == task_node_id and edge.get('source', '').startswith('agent-'):
                        # Extract agent ID from source
                        agent_node_id = edge.get('source')
                        inferred_agent_id = agent_node_id.replace('agent-', '')
                        logger.info(f"Inferred agent_id {inferred_agent_id} for task {task_data.id} from edge {edge.get('id')}")
                        
                        # Get the agent with this ID
                        agent_data = None
                        agent_repo = None if repositories is None else repositories.get('agent')
                        
                        if agent_repo:
                            agent_data = await agent_repo.get(inferred_agent_id)
                        
                        # Fallback to direct database query
                        if not agent_data:
                            try:
                                from src.db.session import request_scoped_session
                                from sqlalchemy import select
                                from src.models.agent import Agent as AgentModel

                                async with request_scoped_session() as db:
                                    stmt = select(AgentModel).filter(AgentModel.id == inferred_agent_id)
                                    result = await db.execute(stmt)
                                    agent_data = result.scalar_one_or_none()
                            except Exception as db_error:
                                logger.error(f"Error querying database for inferred agent: {db_error}", exc_info=True)
                        
                        if agent_data:
                            # Configure the agent
                            agent = await AgentConfig.configure_agent_and_tools(agent_data, flow_data, repositories, group_context)
                            if agent:
                                return agent
                        break
            except Exception as e:
                logger.error(f"Error inferring agent from edges for task {task_data.id}: {e}", exc_info=True)
        
        return None
    
    @staticmethod
    async def _configure_task_tools(task_data, agent, flow_data, group_context: Optional[GroupContext] = None):
        """
        Configure tools for a task and add them to the agent.
        Only assign tools if explicitly defined in the task configuration.

        Args:
            task_data: Task data from the database
            agent: The agent to add tools to
            flow_data: Optional flow data for looking up tools
            group_context: Group context for multi-tenant tool access (optional)
        """
        # Initialize the ToolFactory with proper context for API key access
        from src.engines.crewai.tools.tool_factory import ToolFactory

        # Build config with group_id for multi-tenant isolation
        factory_config = {}
        if group_context and hasattr(group_context, 'primary_group_id') and group_context.primary_group_id:
            factory_config['group_id'] = group_context.primary_group_id
            logger.info(f"Creating ToolFactory with group_id: {group_context.primary_group_id}")

        # Create api_keys_service for tool factory to access API keys from database
        api_keys_service = None
        try:
            from src.db.session import request_scoped_session
            from src.services.api_keys_service import ApiKeysService

            async with request_scoped_session() as session:
                group_id = factory_config.get('group_id')
                api_keys_service = ApiKeysService(session, group_id=group_id)
                # Create tool factory with proper context
                tool_factory = await ToolFactory.create(
                    config=factory_config,
                    api_keys_service=api_keys_service
                )
        except Exception as e:
            logger.warning(f"Error creating ToolFactory with api_keys_service: {e}, falling back to basic factory")
            tool_factory = ToolFactory(factory_config)
            try:
                await tool_factory.initialize()
            except Exception as init_error:
                logger.warning(f"Error initializing ToolFactory: {init_error}")
            
        # Check if task has specific tools
        if hasattr(task_data, 'tools') and task_data.tools:
            # Ensure tools is a list of strings
            task_tools = []
            if isinstance(task_data.tools, list):
                task_tools = [str(tool_id) for tool_id in task_data.tools]
            else:
                try:
                    # Try to convert to list if it's a string (e.g., JSON)
                    if isinstance(task_data.tools, str):
                        task_tools = [str(tool_id) for tool_id in json.loads(task_data.tools)]
                except Exception as e:
                    logger.error(f"Error parsing tools for task {task_data.name}: {e}")
            
            if task_tools:
                logger.info(f"Task {task_data.name} has specific tools: {task_tools}")

                # Extract tool_configs for per-tool overrides
                task_tool_configs = getattr(task_data, 'tool_configs', {}) or {}

                # We only replace agent tools if task has tools specifically defined
                tools = []
                for tool_id in task_tools:
                    try:
                        # Resolve per-tool config override (e.g. GenieTool spaceId)
                        tool_override = _resolve_tool_override(tool_factory, tool_id, task_tool_configs)
                        # Try to create the tool using the factory
                        tool = tool_factory.create_tool(tool_id, tool_config_override=tool_override)
                        if tool:
                            tools.append(tool)
                            logger.info(f"Added tool {tool_id} for task: {task_data.name}")
                        else:
                            logger.warning(f"Tool factory couldn't create tool with ID: {tool_id} for task: {task_data.name}")
                    except Exception as tool_error:
                        logger.warning(f"Error creating tool {tool_id}: {tool_error}")
                
                # Assign tools to the agent if found
                if tools:
                    agent.tools = tools
                    logger.info(f"Assigned {len(tools)} tools from task data to agent for task {task_data.name}")
                else:
                    logger.warning(f"No valid tools could be created for task {task_data.name}, using agent's existing tools")
            else:
                logger.info(f"Task {task_data.name} has empty tools list, using agent's existing tools")
        
        # Also check if tools are defined in node data
        elif flow_data and hasattr(flow_data, 'nodes'):
            logger.info(f"Checking flow nodes for tools in task {task_data.name}")
            try:
                nodes = flow_data.nodes
                # Check if nodes is a string and try to parse it
                if isinstance(nodes, str):
                    nodes = json.loads(nodes)
                
                # Get the task ID to look for
                task_id = str(getattr(task_data, 'id', ''))
                
                if task_id:
                    # Find the task node
                    task_node_id = f"task-{task_id}"
                    for node in nodes:
                        if node.get('id') == task_node_id and 'data' in node:
                            node_data = node.get('data', {})
                            node_tools = node_data.get('tools', [])
                            
                            if node_tools:
                                logger.info(f"Found tools in node data for task {task_id}: {node_tools}")
                                tools = []

                                # Extract tool_configs for per-tool overrides
                                task_tool_configs = getattr(task_data, 'tool_configs', {}) or {}

                                for tool_id in node_tools:
                                    try:
                                        # Resolve per-tool config override (e.g. GenieTool spaceId)
                                        tool_override = _resolve_tool_override(tool_factory, tool_id, task_tool_configs)
                                        # Try to create the tool using the factory
                                        tool = tool_factory.create_tool(tool_id, tool_config_override=tool_override)
                                        if tool:
                                            tools.append(tool)
                                            logger.info(f"Added tool {tool_id} from node data for task: {task_data.name}")
                                        else:
                                            logger.warning(f"Tool factory couldn't create tool with ID: {tool_id} from node data for task: {task_data.name}")
                                    except Exception as tool_error:
                                        logger.warning(f"Error creating tool {tool_id} from node data: {tool_error}")
                                
                                # Assign tools to the agent if found
                                if tools:
                                    agent.tools = tools
                                    logger.info(f"Assigned {len(tools)} tools from node data to agent for task {task_data.name}")
                                else:
                                    logger.warning(f"No valid tools could be created from node data for task {task_data.name}, using agent's existing tools")
                            else:
                                logger.info(f"No tools found in node data for task {task_data.name}, using agent's existing tools")
                            break
            except Exception as e:
                logger.error(f"Error looking for tools in flow nodes for task {task_data.name}: {e}", exc_info=True)
        else:
            logger.info(f"No tools explicitly assigned to task {task_data.name}, using agent's existing tools") 