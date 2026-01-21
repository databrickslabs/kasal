"""
Crew preparation module for CrewAI engine.

This module handles the preparation and configuration of CrewAI agents and tasks.
"""

from typing import Dict, Any, List, Optional
import logging
import re
import os
from datetime import datetime
from crewai import Agent, Crew, Task, Process
from src.core.logger import LoggerManager
from src.engines.crewai.helpers.task_helpers import create_task, is_data_missing
from src.engines.crewai.helpers.agent_helpers import create_agent
from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType
from src.engines.crewai.memory.memory_backend_factory import MemoryBackendFactory
from src.utils.databricks_url_utils import DatabricksURLUtils
# Import new service classes
from src.engines.crewai.services.crew_memory_service import CrewMemoryService
from src.engines.crewai.config.embedder_config_builder import EmbedderConfigBuilder
from src.engines.crewai.config.manager_config_builder import ManagerConfigBuilder
from src.engines.crewai.config.crew_config_builder import CrewConfigBuilder



logger = LoggerManager.get_instance().crew

def validate_crew_config(config: Dict[str, Any]) -> bool:
    """
    Validate crew configuration

    Args:
        config: Crew configuration dictionary

    Returns:
        True if configuration is valid
    """
    # Simple validation - check required sections
    required_sections = ['agents', 'tasks']
    for section in required_sections:
        if section not in config or not config[section]:
            logger.error(f"Missing or empty required section: {section}")
            return False

    return True

def handle_crew_error(e: Exception, message: str) -> None:
    """
    Handle crew-related errors

    Args:
        e: Exception that occurred
        message: Base error message
    """
    error_msg = f"{message}: {str(e)}"
    logger.error(error_msg, exc_info=True)

async def process_crew_output(result: Any) -> Dict[str, Any]:
    """
    Process crew execution output

    Args:
        result: Raw output from crew execution

    Returns:
        Processed output dictionary
    """
    try:
        if isinstance(result, dict):
            return result
        elif hasattr(result, 'raw'):
            # CrewAI result object with raw attribute
            return {"result": result.raw, "type": "crew_result"}
        else:
            # Convert any other result to string
            return {"result": str(result), "type": "processed"}
    except Exception as e:
        logger.error(f"Error processing crew output: {e}")
        return {"error": f"Failed to process output: {str(e)}"}

class CrewPreparation:
    """Handles the preparation of CrewAI agents and tasks"""

    def __init__(self, config: Dict[str, Any], tool_service=None, tool_factory=None, user_token: Optional[str] = None):
        """
        Initialize the CrewPreparation class

        Args:
            config: Configuration dictionary containing crew setup
            tool_service: Tool service instance for resolving tool IDs
            tool_factory: Tool factory for creating tool instances
            user_token: Optional user access token for OBO authentication
        """
        self.config = config
        self.agents: Dict[str, Agent] = {}
        self.tasks: List[Task] = []
        self.crew: Optional[Crew] = None
        self.tool_service = tool_service
        self.tool_factory = tool_factory
        self.user_token = user_token  # Store user token for OBO auth
        self.custom_embedder = None
        self.embedder_config = None

        # Log the configuration to debug memory backend
        logger.info(f"[CrewPreparation.__init__] Config keys: {list(config.keys())}")
        if 'memory_backend_config' in config:
            logger.info(f"[CrewPreparation.__init__] Memory backend config found: {config['memory_backend_config']}")

    def _needs_entity_extraction_fallback(self, model_name: str) -> bool:
        """
        Check if a model needs fallback for entity extraction.

        Args:
            model_name: The model name to check

        Returns:
            True if the model needs fallback for entity extraction
        """
        if not model_name:
            return False

        model_lower = str(model_name).lower()

        # Models that have issues with entity extraction via Instructor/tool calling
        problematic_patterns = [
            'databricks-claude',     # Databricks Claude has strict JSON schema requirements
            'gpt-oss',              # GPT-OSS returns empty responses for complex schemas
            'databricks-gpt-oss',   # Full name for GPT-OSS
        ]

        for pattern in problematic_patterns:
            if pattern in model_lower:
                logger.info(f"Model {model_name} identified as needing entity extraction fallback")
                return True

        return False

    def _should_disable_memory_for_agent(self, agent_config: Dict[str, Any]) -> bool:
        """
        Check if memory is explicitly disabled for an agent.

        Args:
            agent_config: Agent configuration dictionary

        Returns:
            True if memory is explicitly set to False in the agent config
        """
        # Only check if memory is explicitly disabled in agent config
        if 'memory' in agent_config and agent_config['memory'] is False:
            logger.info(f"Memory explicitly disabled for agent {agent_config.get('name', agent_config.get('role', 'Unknown'))}")
            return True
        return False

    async def _apply_entity_extraction_fallback_patch(self):
        """
        Apply runtime patch to CrewAI's Converter to use fallback model for entity extraction.
        This is needed because EntityMemory doesn't accept a custom LLM parameter.
        """
        try:
            from crewai.utilities.converter import Converter
            from crewai.utilities.evaluators.task_evaluator import TaskEvaluator
            from crewai.llm import LLM
            from src.core.llm_manager import LLMManager

            # Store the original methods
            if not hasattr(Converter, '_original_to_pydantic'):
                Converter._original_to_pydantic = Converter.to_pydantic
            if not hasattr(TaskEvaluator, '_original_evaluate'):
                TaskEvaluator._original_evaluate = TaskEvaluator.evaluate

            # Create the fallback LLM instance
            # SECURITY: Pass group_id for multi-tenant isolation
            group_id = self.config.get('group_id') if self.config else None
            if not group_id:
                raise ValueError("group_id is REQUIRED for LLM configuration")
            fallback_llm = await LLMManager.configure_crewai_llm("databricks-llama-4-maverick", group_id)

            # Ensure fallback LLM has all necessary attributes
            if not hasattr(fallback_llm, 'function_calling_llm'):
                fallback_llm.function_calling_llm = fallback_llm

            def patched_to_pydantic(self, current_attempt=1):
                """Patched version that uses fallback LLM for problematic models."""
                # Always use fallback for problematic models - simpler and more reliable
                if hasattr(self, 'llm') and hasattr(self.llm, 'model'):
                    model_name = str(self.llm.model).lower()
                    # Check if this is a problematic model
                    if 'databricks-claude' in model_name or 'gpt-oss' in model_name:
                        # Store original LLM and its attributes
                        original_llm = self.llm
                        original_func_calling = getattr(self.llm, 'function_calling_llm', None)

                        # Use fallback and ensure it has function_calling_llm
                        self.llm = fallback_llm
                        if not hasattr(self.llm, 'function_calling_llm'):
                            self.llm.function_calling_llm = fallback_llm

                        logger.info(f"Using databricks-llama-4-maverick fallback for {model_name}")
                        try:
                            result = Converter._original_to_pydantic(self, current_attempt)
                            return result
                        finally:
                            # Restore original LLM and attributes
                            self.llm = original_llm
                            if original_func_calling is not None:
                                self.llm.function_calling_llm = original_func_calling

                # Use original method for non-problematic models
                return Converter._original_to_pydantic(self, current_attempt)

            def patched_evaluate(self, task, output):
                """Patched TaskEvaluator that uses fallback LLM for problematic models."""
                # Check if the original agent has a problematic LLM
                if hasattr(self, 'original_agent') and hasattr(self.original_agent, 'llm'):
                    model_name = str(self.original_agent.llm.model).lower() if hasattr(self.original_agent.llm, 'model') else ""
                    if 'databricks-claude' in model_name or 'gpt-oss' in model_name:
                        # Temporarily replace the LLM with fallback
                        original_llm = self.llm
                        self.llm = fallback_llm
                        logger.info(f"Using fallback LLM for TaskEvaluator with {model_name}")
                        try:
                            return TaskEvaluator._original_evaluate(self, task, output)
                        finally:
                            self.llm = original_llm

                # Use original method for non-problematic models
                return TaskEvaluator._original_evaluate(self, task, output)

            # Apply the patches
            Converter.to_pydantic = patched_to_pydantic
            TaskEvaluator.evaluate = patched_evaluate
            logger.info("Applied entity extraction and task evaluation fallback patches")

        except Exception as e:
            logger.error(f"Failed to apply entity extraction fallback patch: {e}")
            # Continue without patch - will fail on entity extraction but rest will work

    async def prepare(self) -> bool:
        """
        Prepare the crew by creating agents and tasks

        Returns:
            bool: True if preparation was successful
        """
        try:
            # Validate configuration
            if not validate_crew_config(self.config):
                logger.error("Invalid crew configuration")
                return False

            # Create agents
            if not await self._create_agents():
                logger.error("Failed to create agents")
                return False

            # Create tasks
            if not await self._create_tasks():
                logger.error("Failed to create tasks")
                return False

            # Create crew
            if not await self._create_crew():
                logger.error("Failed to create crew")
                return False

            logger.info("Crew preparation completed successfully")
            return True

        except Exception as e:
            handle_crew_error(e, "Error during crew preparation")
            return False

    def _find_agent_by_reference(self, agent_reference: str) -> Optional[Agent]:
        """
        Find an agent by various reference formats.

        Args:
            agent_reference: Agent reference (name, ID, role, or prefixed ID)

        Returns:
            Agent instance or None if not found
        """
        logger.info(f"[_find_agent_by_reference] Looking for agent: '{agent_reference}'")
        logger.info(f"[_find_agent_by_reference] Available agents: {list(self.agents.keys())}")

        if not agent_reference or agent_reference == 'unknown':
            logger.info(f"[_find_agent_by_reference] Returning None for unknown/empty reference")
            return None

        # Try direct lookup first
        agent = self.agents.get(agent_reference)
        if agent:
            logger.info(f"[_find_agent_by_reference] Found by direct lookup: {agent_reference}")
            return agent

        # If reference starts with 'agent_agent-', extract the UUID part and try lookup
        if agent_reference.startswith('agent_agent-'):
            # Extract UUID: 'agent_agent-47b50da8-bfa2-41c9-8d0f-19c063f5c9c0' -> '47b50da8-bfa2-41c9-8d0f-19c063f5c9c0'
            uuid_part = agent_reference[12:]  # Remove 'agent_agent-' prefix

            # Try to find agent by matching the UUID in the stored agent keys
            for agent_key, stored_agent in self.agents.items():
                # Check if the stored agent key contains this UUID
                if uuid_part in agent_key:
                    logger.info(f"Found agent by UUID match: {agent_reference} -> {agent_key}")
                    return stored_agent

        # If still not found, try to match by agent role in the original config
        # This handles cases where the task references an agent ID but we stored by role
        for agent_config in self.config.get('agents', []):
            agent_id = agent_config.get('id', '')
            if agent_id == agent_reference:
                # Found the config, now find the corresponding stored agent
                agent_name = agent_config.get('name', agent_config.get('id', agent_config.get('role', '')))
                stored_agent = self.agents.get(agent_name)
                if stored_agent:
                    logger.info(f"Found agent by config ID match: {agent_reference} -> {agent_name}")
                    return stored_agent

        logger.warning(f"Could not find agent for reference: {agent_reference}")
        return None

    async def _create_agents(self) -> bool:
        """
        Create all agents defined in the configuration, with MCP tools based on their assigned tasks

        Returns:
            bool: True if all agents were created successfully
        """
        try:
            # Use MCP integration to collect agent MCP requirements
            from src.engines.crewai.tools.mcp_integration import MCPIntegration
            agent_mcp_requirements = await MCPIntegration.collect_agent_mcp_requirements(self.config)

            for i, agent_config in enumerate(self.config.get('agents', [])):
                # Use the agent's 'name' if present, then 'id', then 'role', or generate a name if none exist
                agent_name = agent_config.get('name', agent_config.get('id', agent_config.get('role', f'agent_{i}')))
                config_agent_id = agent_config.get('id', agent_name)

                # CRITICAL FIX: Look up the actual Kasal agent UUID from database using AgentService
                # The config agent_id is the CrewAI agent name, but we need the Kasal agent UUID for vector search
                kasal_agent_id = await self._lookup_kasal_agent_uuid_via_service(agent_config, config_agent_id)
                agent_id = kasal_agent_id if kasal_agent_id else config_agent_id

                # Debug logging to track agent ID resolution for knowledge source access control
                logger.info(f"[CrewPreparation] Agent {i}: name='{agent_name}', config_id='{config_agent_id}'")
                logger.info(f"[CrewPreparation] Agent {i}: kasal_agent_id='{kasal_agent_id}', final_agent_id='{agent_id}'")
                logger.info(f"[CrewPreparation] Agent {i}: Will use agent_id '{agent_id}' for knowledge source access control")

                # Log agent configuration to debug knowledge_sources
                logger.info(f"[CrewPreparation] Processing agent {agent_name} with config keys: {list(agent_config.keys())}")
                if 'knowledge_sources' in agent_config:
                    ks = agent_config['knowledge_sources']
                    logger.info(f"[CrewPreparation] Agent {agent_name} has {len(ks)} knowledge_sources: {ks}")
                else:
                    logger.info(f"[CrewPreparation] Agent {agent_name} has NO knowledge_sources")

                # Add MCP requirements from assigned tasks to agent config
                agent_mcp_servers = agent_mcp_requirements.get(agent_id, [])
                if agent_mcp_servers:
                    logger.info(f"Agent {agent_name} will have MCP servers from tasks: {agent_mcp_servers}")
                    if 'tool_configs' not in agent_config:
                        agent_config['tool_configs'] = {}
                    agent_config['tool_configs']['MCP_SERVERS'] = {'servers': agent_mcp_servers}

                agent = await create_agent(
                    agent_key=agent_name,
                    agent_config=agent_config,
                    tool_service=self.tool_service,
                    tool_factory=self.tool_factory,
                    config=self.config,  # Pass the full config for execution_id and group_id
                    agent_id=agent_id   # Pass Kasal agent UUID for knowledge source access control
                )
                if not agent:
                    logger.error(f"Failed to create agent: {agent_name}")
                    return False

                # NOTE: knowledge_sources handling removed - we now use DatabricksKnowledgeSearchTool directly
                # The tool is configured via agent.tools, not agent.knowledge_sources

                # Store the agent with the agent_name as key
                self.agents[agent_name] = agent
                logger.info(f"Created agent: {agent_name}")
            return True
        except Exception as e:
            handle_crew_error(e, "Error creating agents")
            return False


    async def _create_tasks(self) -> bool:
        """
        Create all tasks defined in the configuration

        Returns:
            bool: True if all tasks were created successfully
        """
        try:
            from src.engines.crewai.helpers.task_helpers import create_task

            tasks = self.config.get('tasks', [])
            total_tasks = len(tasks)

            # Create a dictionary to store tasks by ID for reference
            task_dict = {}

            # First pass: create all tasks without setting context
            for i, task_config in enumerate(tasks):
                # Get the agent for this task, default to first agent if not specified
                agent_name = task_config.get('agent', 'unknown')
                agent = self._find_agent_by_reference(agent_name)

                # Handle missing agent
                if not agent:
                    if not self.agents:
                        logger.error("No agents available for tasks")
                        return False

                    # Use the first available agent as fallback
                    fallback_agent_name, agent = next(iter(self.agents.items()))
                    logger.warning(f"Invalid agent '{agent_name}' specified for task. Using '{fallback_agent_name}' instead.")

                # Define task_name first so it can be used in logging
                # If this is the first task and it has the Databricks knowledge tool, add context
                try:
                    if i == 0 and agent is not None:
                        # Check if DatabricksKnowledgeSearchTool is in the task's tools list
                        task_tools = task_config.get('tools', [])
                        has_db_knowledge_tool = (
                            'DatabricksKnowledgeSearchTool' in task_tools or
                            '36' in [str(t) for t in task_tools]
                        )

                        if has_db_knowledge_tool:
                            # Extract available file information from the task's assigned agent's knowledge_sources
                            available_files = []

                            # Find the agent config that matches the current task's agent
                            task_agent_ref = task_config.get('agent', '')
                            agent_config_for_files = None

                            for agent_cfg in self.config.get('agents', []):
                                agent_id = agent_cfg.get('id', '')
                                agent_role = agent_cfg.get('role', '')
                                # Match by ID or role
                                if agent_id == task_agent_ref or agent_role == task_agent_ref:
                                    agent_config_for_files = agent_cfg
                                    break

                            if not agent_config_for_files:
                                # Fallback to first agent if no match found
                                agent_config_for_files = self.config.get('agents', [{}])[0]

                            knowledge_sources = agent_config_for_files.get('knowledge_sources', [])

                            for ks in knowledge_sources:
                                if isinstance(ks, dict):
                                    # Extract filename from fileInfo or metadata
                                    file_info = ks.get('fileInfo', {})
                                    metadata = ks.get('metadata', {})
                                    filename = file_info.get('filename') or metadata.get('filename')

                                    if filename:
                                        available_files.append(filename)

                            # Build file list string
                            files_info = ""
                            if available_files:
                                files_list = "\n".join([f"  - {fname}" for fname in available_files])
                                files_info = f"\n\n**Available Knowledge Files:**\n{files_list}\n"

                            # Prepend a STRONG instruction to REQUIRE tool use with file context
                            nudge = (
                                "**CRITICAL INSTRUCTION**: You MUST use the DatabricksKnowledgeSearchTool BEFORE answering. "
                                f"{files_info}"
                                "Search the uploaded documents for relevant information first, then use ONLY the retrieved content "
                                "to formulate your answer. DO NOT proceed without calling this tool. "
                                "Formulate your search query to find specific information from these files. "
                                "Cite the specific passages you found.\n\n"
                            )
                            # Only inject once, and keep original description intact after the nudge
                            original_desc = task_config.get('description', '') or ''
                            if nudge.strip() not in original_desc:
                                task_config['description'] = f"{nudge}{original_desc}"
                                t_name_for_log = task_config.get('name', 'first_task')
                                logger.info(f"[CrewPreparation] Injected STRONG knowledge-search requirement with {len(available_files)} files into first task '{t_name_for_log}' for agent '{agent_name}'")

                            # Also ensure DatabricksKnowledgeSearchTool is in the task's tools list
                            task_tools = task_config.get('tools', [])
                            if 'DatabricksKnowledgeSearchTool' not in task_tools and '36' not in [str(t) for t in task_tools]:
                                task_tools.append('DatabricksKnowledgeSearchTool')
                                task_config['tools'] = task_tools
                                logger.info(f"[CrewPreparation] Added DatabricksKnowledgeSearchTool to task '{t_name_for_log}' tools list")
                except Exception as _nudge_err:
                    logger.warning(f"[CrewPreparation] Failed to inject knowledge-search nudge: {_nudge_err}")

                task_name = task_config.get('name', f"task_{len(self.tasks)}")
                task_id = task_config.get('id', task_name)

                # Store any context IDs for second pass resolution (only if multiple tasks)
                if len(tasks) > 1 and "context" in task_config:
                    context_value = task_config.pop("context")
                    # Only process non-empty context values
                    if context_value:  # Skip empty lists, empty strings, etc.
                        logger.info(f"Saved context references for task {task_name}: {context_value}")
                        # Store the references for resolution in second pass
                        if isinstance(context_value, list) and context_value:
                            task_config['_context_refs'] = [str(item) for item in context_value]
                        elif isinstance(context_value, str) and context_value.strip():
                            task_config['_context_refs'] = [context_value]
                        elif isinstance(context_value, dict) and "task_ids" in context_value and context_value["task_ids"]:
                            task_config['_context_refs'] = context_value["task_ids"]
                elif "context" in task_config:
                    # Remove context from single-task configurations to avoid issues
                    task_config.pop("context")

                # Get the async execution setting
                # Tasks with async_execution=True will run in parallel (if they have no context dependencies)
                # CrewAI validation requires that a crew ends with at most one async task
                # We handle this by auto-creating a completion task after the loop if needed
                is_async = task_config.get('async_execution', False)

                if is_async:
                    # Mark that this task wants async execution for later processing
                    task_config['_wanted_async'] = True
                    has_context = '_context_refs' in task_config or task_config.get('context')
                    if has_context:
                        logger.info(f"Task '{task_name}' has async_execution=True with context - will wait for dependencies")
                    else:
                        logger.info(f"Task '{task_name}' has async_execution=True - will run in parallel")

                logger.info(f"Task '{task_name}' async_execution setting: {is_async}")

                # Create the task
                # Get execution_name from config (can be run_name or execution_id)
                execution_name = self.config.get('run_name') or self.config.get('inputs', {}).get('run_name') or self.config.get('execution_id')

                task = await create_task(
                    task_key=task_name,
                    task_config=task_config,
                    agent=agent,
                    output_dir=self.config.get('output_dir'),
                    config=self.config,
                    tool_service=self.tool_service,
                    tool_factory=self.tool_factory,
                    execution_name=execution_name
                )

                self.tasks.append(task)
                # Store in our dictionary for context resolution
                task_dict[task_id] = task
                logger.info(f"Created task: {task_name} for agent: {agent_name}")

            # Second pass: Resolve context references to actual Task objects
            for task_config in tasks:
                task_id = task_config.get('id', task_config.get('name'))
                task = task_dict.get(task_id)

                if not task:
                    logger.warning(f"Could not find task for ID {task_id} during context resolution")
                    continue

                # If this task has context references, resolve them
                if '_context_refs' in task_config:
                    context_refs = task_config['_context_refs']
                    context_tasks = []

                    for ref in context_refs:
                        if ref in task_dict:
                            context_tasks.append(task_dict[ref])
                        else:
                            logger.warning(f"Could not resolve context reference '{ref}' for task {task_id}")

                    if context_tasks:
                        logger.info(f"Setting context for task {task_id} to {len(context_tasks)} Task objects")
                        task.context = context_tasks
                    else:
                        logger.warning(f"No context tasks could be resolved for task {task_id}")

            # Handle parallel execution for multiple async tasks
            # CrewAI validation: "A crew must end with at most one async task"
            # Solution: Keep ALL async tasks as async (they run in parallel), add a minimal
            # completion task with context=[all_async_tasks] to satisfy CrewAI validation
            #
            # IMPORTANT: Tasks with async_execution=True must NOT have context set,
            # otherwise they will wait for that context and not run in parallel!
            if self.tasks:
                async_tasks = [t for t in self.tasks if getattr(t, 'async_execution', False)]

                if len(async_tasks) > 1:
                    logger.info(f"Found {len(async_tasks)} async tasks - configuring for parallel execution")

                    # Remove any context from async tasks so they can run truly in parallel
                    for async_task in async_tasks:
                        if getattr(async_task, 'context', None):
                            logger.info(f"Removing context from async task to enable parallel execution")
                            async_task.context = None

                    # Add a minimal completion task that waits for all async tasks
                    # This satisfies CrewAI's validation while enabling true parallel execution
                    from crewai import Task as CrewAITask

                    # Use the last async task's agent for the completion task
                    completion_agent = async_tasks[-1].agent

                    # Create a minimal completion task
                    completion_task = CrewAITask(
                        description="Return the combined outputs from the parallel tasks.",
                        expected_output="The outputs from all parallel tasks.",
                        agent=completion_agent,
                        context=async_tasks,  # Wait for ALL async tasks to complete
                        async_execution=False  # Sync task to satisfy CrewAI validation
                    )

                    # Add completion task to the crew's task list
                    self.tasks.append(completion_task)

                    # Log the parallel execution setup
                    parallel_descriptions = [
                        getattr(t, 'description', 'unknown')[:40] + '...'
                        if len(getattr(t, 'description', '')) > 40
                        else getattr(t, 'description', 'unknown')
                        for t in async_tasks
                    ]

                    logger.info(f"  {len(async_tasks)} tasks will run in PARALLEL: {parallel_descriptions}")
                    logger.info(f"  Added completion task to collect results and satisfy CrewAI validation")

            return True
        except Exception as e:
            handle_crew_error(e, "Error creating tasks")
            return False

    async def _create_crew(self) -> bool:
        """
        Create the crew with all prepared agents and tasks.
        Refactored to use specialized service classes.

        Returns:
            bool: True if crew was created successfully
        """
        try:
            # Initialize builders and services
            config_builder = CrewConfigBuilder(self.config)
            memory_service = CrewMemoryService(self.config, self.user_token)
            embedder_builder = EmbedderConfigBuilder(self.config, self.user_token)
            manager_builder = ManagerConfigBuilder(
                self.config,
                self.tool_service,
                self.tool_factory,
                self.user_token
            )

            # 1. Determine crew memory and process type
            default_crew_memory = config_builder.determine_crew_memory_setting()
            process_type = config_builder.determine_process_type()

            # 2. Build base crew kwargs
            crew_kwargs = config_builder.build_base_crew_kwargs(
                agents=list(self.agents.values()),
                tasks=self.tasks,
                process_type=process_type,
                default_crew_memory=default_crew_memory
            )

            # 3. Configure manager (hierarchical/sequential)
            crew_kwargs = await manager_builder.configure_manager(crew_kwargs, process_type)

            # 4. Configure embedder
            crew_kwargs, custom_embedder, embedder_config = await embedder_builder.configure_embedder(crew_kwargs)
            self.custom_embedder = custom_embedder
            self.embedder_config = embedder_config

            # 5. Fetch and setup memory backend
            should_disable_memory = not crew_kwargs.get('memory', True)

            memory_backend_config = None
            if crew_kwargs.get('memory', False) and not should_disable_memory:
                memory_backend_config = await memory_service.fetch_memory_backend_config()

            # If no config found, create default
            if not memory_backend_config and crew_kwargs.get('memory', False):
                memory_backend_config = {
                    'backend_type': 'default',
                    'enable_short_term': True,
                    'enable_long_term': True,
                    'enable_entity': True,
                    'enable_relationship_retrieval': False,
                }
                logger.info("Created default memory backend configuration (ChromaDB + SQLite)")

            # 6. Generate crew ID and setup storage
            crew_id = memory_service.generate_crew_id()
            memory_service.setup_storage_directory(crew_id, memory_backend_config)

            # 7. Check if all memory types disabled
            if config_builder.check_memory_disabled_by_backend_config(memory_backend_config):
                crew_kwargs['memory'] = False
                should_disable_memory = True
                logger.info("Found 'Disabled Configuration' - ignoring database config and using default memory")

            # 8. Configure memory components
            if crew_kwargs.get('memory', False) and not should_disable_memory and memory_backend_config:
                # Determine which embedder to use
                embedder_for_backends = custom_embedder if memory_backend_config.get('backend_type') == 'databricks' else crew_kwargs.get('embedder')

                # Create memory backends
                memory_backends = await memory_service.create_memory_backends(
                    memory_backend_config,
                    crew_id,
                    embedder_for_backends
                )

                # Configure CrewAI memory components
                from src.schemas.memory_backend import MemoryBackendConfig as MemBackConfig
                memory_config = MemBackConfig(**memory_backend_config)

                crew_kwargs = memory_service.configure_crew_memory_components(
                    crew_kwargs,
                    memory_config,
                    memory_backends,
                    crew_id,
                    custom_embedder
                )

            # 9. Add optional parameters
            crew_kwargs = config_builder.add_optional_parameters(crew_kwargs)
            crew_kwargs = await config_builder.add_llm_parameters(crew_kwargs)

            # 10. Handle memory disabling
            if should_disable_memory:
                crew_kwargs = config_builder.disable_memory_completely(crew_kwargs)

            # 11. Check if custom memory backends override default
            if 'short_term_memory' in crew_kwargs or 'long_term_memory' in crew_kwargs or 'entity_memory' in crew_kwargs:
                if memory_backend_config and memory_backend_config.get('backend_type') == 'databricks':
                    crew_kwargs['memory'] = False
                    logger.info("Set memory=False to prevent CrewAI default memory initialization for Databricks backend")

            # 12. Log configuration
            config_builder.log_memory_configuration(crew_kwargs, memory_backend_config)

            # 13. Handle OpenAI API key
            await self._handle_openai_api_key()

            # 14. Create crew instance with error handling
            # NOTE: knowledge_sources no longer used - we use DatabricksKnowledgeSearchTool instead
            logger.info(f"Creating Crew with kwargs: {list(crew_kwargs.keys())}")
            try:
                self.crew = Crew(**crew_kwargs)
            except (TypeError, Exception) as e:
                # Handle both TypeError and Pydantic ValidationError
                error_msg = str(e)
                logger.warning(f"Crew creation failed with error: {error_msg}")

                # Try to extract the problematic field from the error message
                if "unexpected keyword argument" in error_msg or "validation error" in error_msg.lower():
                    # Common problematic fields that might not be supported in all CrewAI versions
                    problematic_keys = ['tracing', 'embedder', 'reasoning_llm', 'planning_llm']

                    # Try removing each problematic key one at a time
                    for key in problematic_keys:
                        if key in crew_kwargs:
                            logger.warning(f"Removing potentially unsupported Crew kwarg '{key}' and retrying")
                            crew_kwargs.pop(key, None)
                            try:
                                self.crew = Crew(**crew_kwargs)
                                logger.info(f"Successfully created crew after removing '{key}'")
                                break
                            except Exception:
                                continue

                    if not self.crew:
                        # If still failing, try with minimal kwargs
                        logger.warning("Trying crew creation with minimal kwargs")
                        minimal_kwargs = {
                            'agents': crew_kwargs.get('agents', []),
                            'tasks': crew_kwargs.get('tasks', []),
                            'process': crew_kwargs.get('process'),
                            'verbose': crew_kwargs.get('verbose', True),
                            'memory': crew_kwargs.get('memory', False)
                        }
                        # For hierarchical process, manager_llm or manager_agent is required
                        if 'manager_llm' in crew_kwargs:
                            minimal_kwargs['manager_llm'] = crew_kwargs['manager_llm']
                        elif 'manager_agent' in crew_kwargs:
                            minimal_kwargs['manager_agent'] = crew_kwargs['manager_agent']
                        self.crew = Crew(**minimal_kwargs)
                else:
                    raise

            if not self.crew:
                logger.error("Failed to create crew")
                return False

            # 16. Set crew references and attach trace context
            memory_service.set_crew_reference_on_memory(self.crew)
            memory_service.attach_memory_trace_context(self.crew, memory_backend_config, crew_kwargs)

            # 17. Initialize knowledge for agents
            await self._initialize_agent_knowledge(crew_kwargs)

            logger.info("Created crew successfully")
            return True

        except Exception as e:
            handle_crew_error(e, "Error creating crew")
            return False

    async def _handle_openai_api_key(self) -> None:
        """Handle OpenAI API key configuration"""
        try:
            from src.services.api_keys_service import ApiKeysService

            # SECURITY: Get group_id from config for multi-tenant isolation
            group_id = self.config.get('group_id')
            openai_key = await ApiKeysService.get_provider_api_key("openai", group_id=group_id)
            if openai_key:
                os.environ["OPENAI_API_KEY"] = openai_key
                logger.info("OpenAI API key is configured, keeping it for CrewAI")
            else:
                os.environ["OPENAI_API_KEY"] = "sk-dummy-validation-key"
                logger.info("No OpenAI API key configured, set dummy key for CrewAI validation")
        except Exception as e:
            logger.warning(f"Error handling OpenAI API key: {e}")

    async def _attach_knowledge_sources(self) -> None:
        """
        DEPRECATED: This method is deprecated and no longer functional.
        
        Knowledge sources should now be configured via:
        1. Agent's knowledge_sources configuration (processed in _create_agents)
        2. DatabricksKnowledgeSearchTool in agent's tools list
        
        This method is kept as a no-op to prevent breaking existing code paths.
        """
        logger.info("[DEPRECATED] _attach_knowledge_sources called - this method is deprecated")
        logger.info("Knowledge sources should be configured via agent.knowledge_sources or DatabricksKnowledgeSearchTool")
        # No-op: Knowledge sources are now handled via the KnowledgeSourceFactory in _create_agents

    async def _initialize_agent_knowledge(self, crew_kwargs: Dict[str, Any]) -> None:
        """
        DEPRECATED: This method is no longer needed.
        Knowledge is now accessed via DatabricksKnowledgeSearchTool in agent's tools list.
        Kept as no-op to prevent breaking existing code paths.
        """
        logger.info("[CrewPreparation] _initialize_agent_knowledge called (deprecated - no-op)")
        logger.info("[CrewPreparation] Knowledge access is now via DatabricksKnowledgeSearchTool")


    async def execute(self) -> Dict[str, Any]:
        """
        Execute the prepared crew

        Returns:
            Dict[str, Any]: Results from crew execution
        """
        if not self.crew:
            logger.error("Cannot execute crew: crew not prepared")
            return {"error": "Crew not prepared"}

        try:
            # Execute the crew
            result = await self.crew.kickoff()

            # Process the output
            processed_output = await process_crew_output(result)

            # Check if data is missing
            if is_data_missing(processed_output):
                logger.warning("Crew execution completed but data may be missing")

            return processed_output

        except Exception as e:
            handle_crew_error(e, "Error during crew execution")
            return {"error": str(e)}

    async def _lookup_kasal_agent_uuid_via_service(self, agent_config: Dict[str, Any], config_agent_id: str) -> Optional[str]:
        """
        Look up the actual Kasal agent UUID from the database using AgentService.

        This is critical for knowledge source access control - we need to use the same
        agent ID that the task search will use, which is the Kasal agent UUID from the database,
        not the CrewAI agent name from the configuration.

        Args:
            agent_config: Agent configuration from CrewAI config
            config_agent_id: Agent ID from config (usually CrewAI agent name)

        Returns:
            Kasal agent UUID from database, or None if not found
        """
        try:
            from src.core.unit_of_work import UnitOfWork
            from src.services.agent_service import AgentService
            from src.utils.user_context import GroupContext

            # Get the group_id from config
            group_id = self.config.get('group_id', 'default')

            from src.db.session import async_session_factory

            async with async_session_factory() as session:
                agent_service = AgentService(session)
                group_context = GroupContext(group_ids=[group_id])

                # Get all agents for the group
                agents = await agent_service.find_by_group(group_context)

                # Try to match by various criteria
                agent_role = agent_config.get('role', '')
                agent_name = agent_config.get('name', '')

                logger.info(f"[CrewPreparation] Looking for agent in {len(agents)} agents in group '{group_id}'")
                logger.info(f"[CrewPreparation] Searching for role='{agent_role}', name='{agent_name}', config_id='{config_agent_id}'")

                for db_agent in agents:
                    # Try exact matches first
                    if (db_agent.role == agent_role or
                        db_agent.name == agent_name or
                        str(db_agent.id) == config_agent_id):
                        logger.info(f"[CrewPreparation] Found matching agent: UUID={db_agent.id}, role='{db_agent.role}', name='{db_agent.name}'")
                        return str(db_agent.id)

                # If no exact match, log available agents for debugging
                logger.warning(f"[CrewPreparation] No matching agent found for config_id='{config_agent_id}'")
                logger.info(f"[CrewPreparation] Available agents:")
                for db_agent in agents:
                    logger.info(f"[CrewPreparation]   - UUID={db_agent.id}, role='{db_agent.role}', name='{db_agent.name}'")

                return None

        except Exception as e:
            logger.error(f"[CrewPreparation] Error looking up Kasal agent UUID: {e}")
            return None

    def cleanup(self):
        """
        Cleanup method to restore original environment settings.
        This should be called when done with the crew to restore the original storage directory.
        """
        if hasattr(self, '_original_storage_dir'):
            import os
            if self._original_storage_dir is not None:
                os.environ["CREWAI_STORAGE_DIR"] = self._original_storage_dir
                logger.info(f"Restored original CREWAI_STORAGE_DIR: {self._original_storage_dir}")
            elif "CREWAI_STORAGE_DIR" in os.environ:
                # If there was no original value, remove the environment variable
                del os.environ["CREWAI_STORAGE_DIR"]
                logger.info("Removed CREWAI_STORAGE_DIR environment variable")