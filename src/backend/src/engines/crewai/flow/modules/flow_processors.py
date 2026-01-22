"""
Flow processors module for CrewAI flow execution.

This module handles processing of starting points, listeners, and routers in flow configuration.
"""
import logging
from typing import Dict, List, Any, Optional, Callable
from uuid import UUID
from crewai.flow.flow import listen, router

from src.core.logger import LoggerManager


def _to_uuid(value) -> UUID:
    """Convert a string or UUID to UUID object."""
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        return UUID(value)
    raise ValueError(f"Cannot convert {type(value)} to UUID")

# Initialize logger - use flow logger for flow execution
logger = LoggerManager.get_instance().flow


class FlowProcessorManager:
    """
    Manager for processing flow configuration elements (starting points, listeners, routers).
    """

    @staticmethod
    async def process_starting_points(
        flow_config,
        all_tasks,
        repositories,
        group_context=None,
        callbacks=None
    ):
        """
        Process starting points from flow configuration.

        IMPORTANT: Groups tasks by crew_id. If multiple tasks have the same crew_id,
        they are executed sequentially within a single crew using task.context for dependencies.

        Args:
            flow_config: Flow configuration with starting points
            all_tasks: Dictionary to populate with task objects
            repositories: Dictionary of repositories to load data
            group_context: Group context for multi-tenant isolation
            callbacks: Callbacks for execution monitoring

        Returns:
            List of starting point method names
        """
        logger.info("Processing starting points")
        starting_point_methods = []

        task_repo = repositories.get('task') if repositories else None
        crew_repo = repositories.get('crew') if repositories else None

        if not task_repo:
            logger.warning("No task repository provided for starting points")
            return starting_point_methods

        # Group starting points by crew_id
        crews_map = {}  # crew_id -> list of task_ids

        for idx, start_point in enumerate(flow_config.get('startingPoints', [])):
            task_id = start_point.get('taskId')
            crew_id = start_point.get('crewId')

            logger.info(f"Starting point {idx}: task_id={task_id}, crew_id={crew_id}")

            if not task_id or not crew_id:
                logger.warning(f"Starting point {idx} missing task_id or crew_id")
                continue

            # Group by crew_id
            if crew_id not in crews_map:
                crews_map[crew_id] = []
            crews_map[crew_id].append(task_id)

        logger.info(f"Grouped starting points into {len(crews_map)} crews")

        # Process each crew (which may have multiple tasks)
        for crew_idx, (crew_id, task_ids) in enumerate(crews_map.items()):
            method_name = f"starting_point_{crew_idx}"
            logger.info(f"Processing crew {crew_id} with {len(task_ids)} tasks: {task_ids}")

            if not task_ids:
                continue

            try:
                # Load crew data (shared by all tasks)
                # Convert crew_id to UUID since the Crew model uses UUID(as_uuid=True)
                crew_uuid = _to_uuid(crew_id)
                crew_data = await crew_repo.get(crew_uuid) if crew_repo else None
                if not crew_data:
                    logger.warning(f"Crew {crew_id} not found")
                    continue

                # Build Task objects for all tasks in this crew
                crew_task_objects = []

                for task_idx, task_id in enumerate(task_ids):
                    task_data = await task_repo.get(task_id) if task_repo else None

                    # Variables for embedded node data fallback
                    task_node_data = None
                    agent_node_data = None
                    agent_id = None

                    # Always scan crew structure for task/agent data (needed for both DB and embedded cases)
                    if crew_data.nodes:
                        for node in crew_data.nodes:
                            node_id = node.get('id', '')
                            node_uuid = node_id.split('-', 1)[1] if '-' in node_id else node_id

                            if node.get('type') == 'taskNode' and node_uuid == str(task_id):
                                # Found the task node - save its data for fallback
                                task_node_data = node.get('data', {})

                                # Find the agent connected to this task
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
                                                    # Save agent node data for fallback
                                                    agent_node_data = agent_node.get('data', {})
                                                    logger.info(f"  Resolved agent {agent_id} from crew structure for task {task_id}")
                                                    break
                                            if agent_id:
                                                break
                                break

                    # If task not in DB, try to create from embedded node data
                    if not task_data:
                        if task_node_data:
                            logger.info(f"Task {task_id} not in database, using embedded node data from crew")
                            # Create a mock task data object from node data
                            from types import SimpleNamespace
                            task_data = SimpleNamespace(
                                id=task_id,
                                name=task_node_data.get('label', f'Task {task_id}'),
                                description=task_node_data.get('description', ''),
                                expected_output=task_node_data.get('expected_output', ''),
                                agent_id=agent_id,
                                tools=task_node_data.get('tools', []),
                                tool_configs=task_node_data.get('tool_configs', {}),
                                async_execution=task_node_data.get('async_execution', False),
                                context=task_node_data.get('context', []),
                                config=task_node_data.get('config', {}),
                                memory=task_node_data.get('memory', False),
                                markdown=task_node_data.get('markdown', False),
                                output_file=task_node_data.get('config', {}).get('output_file'),
                                output_json=task_node_data.get('config', {}).get('output_json'),
                                output_pydantic=task_node_data.get('config', {}).get('output_pydantic'),
                                human_input=task_node_data.get('config', {}).get('human_input', False),
                                converter_cls=None
                            )
                        else:
                            logger.warning(f"Task {task_id} not found in database or crew nodes, skipping")
                            continue

                    # Fallback to task.agent_id if crew structure lookup failed
                    if not agent_id:
                        agent_id = getattr(task_data, 'agent_id', None)
                        if agent_id:
                            logger.info(f"  Using agent {agent_id} from task.agent_id for task {task_id}")
                        else:
                            logger.warning(f"  Could not resolve agent for task {task_id}")
                            continue

                    # Import necessary classes for building CrewAI objects
                    from src.engines.crewai.flow.modules.task_config import TaskConfig
                    from src.engines.crewai.flow.modules.agent_config import AgentConfig

                    # Build the CrewAI Task object with agent
                    agent_repo = repositories.get('agent') if repositories else None
                    agent_data = await agent_repo.get(agent_id) if agent_repo else None

                    # If agent not in DB, try to create from embedded node data
                    if not agent_data and agent_node_data:
                        logger.info(f"Agent {agent_id} not in database, using embedded node data from crew")
                        from types import SimpleNamespace
                        # Helper to handle None values with defaults
                        def _get_val(d, key, default):
                            val = d.get(key)
                            return default if val is None else val

                        agent_data = SimpleNamespace(
                            id=agent_id,
                            name=agent_node_data.get('label', f'Agent {agent_id}'),
                            role=agent_node_data.get('role', ''),
                            goal=agent_node_data.get('goal', ''),
                            backstory=agent_node_data.get('backstory', ''),
                            llm=agent_node_data.get('llm'),
                            function_calling_llm=agent_node_data.get('function_calling_llm'),
                            tools=agent_node_data.get('tools') or [],
                            tool_configs=agent_node_data.get('tool_configs') or {},
                            max_iter=agent_node_data.get('max_iter'),
                            max_rpm=agent_node_data.get('max_rpm'),
                            max_execution_time=agent_node_data.get('max_execution_time'),
                            verbose=_get_val(agent_node_data, 'verbose', True),
                            allow_delegation=_get_val(agent_node_data, 'allow_delegation', False),
                            cache=_get_val(agent_node_data, 'cache', True),
                            memory=_get_val(agent_node_data, 'memory', False),
                            embedder_config=agent_node_data.get('embedder_config'),
                            system_template=agent_node_data.get('system_template'),
                            prompt_template=agent_node_data.get('prompt_template'),
                            response_template=agent_node_data.get('response_template'),
                            allow_code_execution=_get_val(agent_node_data, 'allow_code_execution', False),
                            code_execution_mode=agent_node_data.get('code_execution_mode'),
                            max_retry_limit=_get_val(agent_node_data, 'max_retry_limit', 2),
                            use_system_prompt=_get_val(agent_node_data, 'use_system_prompt', True),
                            respect_context_window=_get_val(agent_node_data, 'respect_context_window', True),
                            knowledge_sources=agent_node_data.get('knowledge_sources'),
                            reasoning_mode=agent_node_data.get('reasoning_mode'),
                            reasoning_config=agent_node_data.get('reasoning_config')
                        )

                    if agent_data:
                        # Merge tool_configs: task-level takes priority over crew-level
                        # MCP servers are typically configured at the task level
                        effective_tool_configs = {}
                        if crew_data and hasattr(crew_data, 'tool_configs') and crew_data.tool_configs:
                            if isinstance(crew_data.tool_configs, dict):
                                effective_tool_configs.update(crew_data.tool_configs)
                        if hasattr(task_data, 'tool_configs') and task_data.tool_configs:
                            if isinstance(task_data.tool_configs, dict):
                                effective_tool_configs.update(task_data.tool_configs)

                        logger.info(f"Task {task_id} effective tool_configs: {effective_tool_configs}")

                        # Build CrewAI Agent object using configure_agent_and_tools
                        agent_obj = await AgentConfig.configure_agent_and_tools(
                            agent_data=agent_data,
                            flow_data=flow_config,
                            repositories=repositories,
                            group_context=group_context,
                            crew_tool_configs=effective_tool_configs if effective_tool_configs else None
                        )

                        # Build CrewAI Task object with the agent using configure_task
                        task_obj = await TaskConfig.configure_task(
                            task_data=task_data,
                            agent=agent_obj,
                            task_output_callback=callbacks.get('task_callback') if callbacks else None,
                            flow_data=flow_config,
                            repositories=repositories,
                            group_context=group_context
                        )

                        # Set up task.context for sequential dependencies
                        # Only set context if task is NOT marked for async execution
                        is_async = getattr(task_data, 'async_execution', False)

                        if is_async:
                            # Parallel execution - don't set context
                            logger.info(f"Task {task_id} marked for async execution (will run in parallel)")
                        elif task_idx > 0 and len(crew_task_objects) > 0:
                            # Sequential execution - set context to wait for previous task
                            task_obj.context = [crew_task_objects[-1]]  # Wait for previous task
                            logger.info(f"Task {task_id} will wait for previous task {task_ids[task_idx-1]} (sequential)")

                        # Store the actual task object by task_id
                        all_tasks[str(task_id)] = task_obj
                        crew_task_objects.append(task_obj)
                        logger.info(f"Added task {task_id} (index {task_idx}) to crew {crew_id}, agent {agent_id}")
                    else:
                        logger.warning(f"Agent {agent_id} not found in database or crew nodes for task {task_id}")

                # CrewAI validation: A crew cannot end with more than one async task
                # If we have multiple async tasks, auto-create a completion task to enable parallel execution
                if crew_task_objects:
                    # Count async tasks
                    async_tasks = [t for t in crew_task_objects if getattr(t, 'async_execution', False)]

                    if len(async_tasks) > 1:
                        # Auto-create a lightweight completion task that waits for all async tasks
                        # This allows all async tasks to run in parallel while satisfying CrewAI validation
                        from crewai import Task

                        # Use the last async task's agent for the completion task
                        completion_agent = async_tasks[-1].agent

                        # Create completion task that aggregates results
                        completion_task = Task(
                            description="Aggregate and return results from parallel task executions",
                            expected_output="Combined results from all parallel tasks",
                            agent=completion_agent,
                            context=async_tasks,  # Wait for ALL async tasks
                            async_execution=False  # This must be sync to satisfy CrewAI validation
                        )

                        # Add to the crew's task list
                        crew_task_objects.append(completion_task)

                        # Generate a unique ID for the auto-created task
                        auto_task_id = f"auto_completion_{crew_id[:8]}"
                        task_ids.append(auto_task_id)

                        logger.info(f"Auto-created completion task for crew {crew_id} to enable parallel execution")
                        logger.info(f"  {len(async_tasks)} tasks will run in PARALLEL, completion task aggregates results")
                        logger.info(f"  Parallel tasks: {[t.description[:50] + '...' if len(t.description) > 50 else t.description for t in async_tasks]}")

                # If we successfully built tasks for this crew, add it as a starting point
                if crew_task_objects:
                    # Store metadata about this starting point crew
                    # Format: (method_name, task_ids_list, task_objects_list, crew_name, crew_data)
                    # Include both IDs (for listener matching), objects (for crew creation), and crew_data (for configuration)
                    starting_point_info = (method_name, task_ids, crew_task_objects, crew_data.name if hasattr(crew_data, 'name') else f"Crew {crew_idx}", crew_data)
                    starting_point_methods.append(starting_point_info)
                    logger.info(f"Added starting point {method_name} for crew {crew_id} with {len(crew_task_objects)} tasks (including {len(async_tasks)} parallel)")

            except Exception as e:
                logger.error(f"Error processing crew {crew_id}: {e}", exc_info=True)
                continue

        logger.info(f"Processed {len(starting_point_methods)} starting points")
        return starting_point_methods

    @staticmethod
    async def process_listeners(
        flow_config,
        all_tasks,
        repositories,
        group_context=None,
        callbacks=None
    ):
        """
        Process listeners from flow configuration.

        IMPORTANT: Groups listener tasks by crew_id. If multiple listener entries have the same crew_id,
        they are merged into a single crew that executes all tasks sequentially using task.context.
        This enables crew-level listener orchestration instead of task-level.

        Args:
            flow_config: Flow configuration with listeners
            all_tasks: Dictionary to populate with task objects
            repositories: Dictionary of repositories to load data
            group_context: Group context for multi-tenant isolation
            callbacks: Callbacks for execution monitoring

        Returns:
            List of tuples: (method_name, crew_id, task_ids, task_objects, crew_name, listen_to_task_ids, condition_type)
            This rich structure allows flow_builder to create proper crew-level listeners.
        """
        logger.info("Processing listeners with crew grouping")
        listener_methods = []

        task_repo = repositories.get('task') if repositories else None
        crew_repo = repositories.get('crew') if repositories else None

        if not task_repo:
            logger.warning("No task repository provided for listeners")
            return listener_methods

        # STEP 1: Group listeners by crew_id
        # Multiple listener configs with the same crew_id should be merged into one crew
        crews_map = {}  # crew_id -> {task_ids: [], listen_to_task_ids: set(), condition_type: str, listener_config: dict}

        for idx, listener in enumerate(flow_config.get('listeners', [])):
            crew_id = listener.get('crewId')
            listen_to_task_ids = listener.get('listenToTaskIds', [])
            condition_type = listener.get('conditionType', 'NONE')

            logger.info(f"Listener {idx}: crew_id={crew_id}, listenToTaskIds={listen_to_task_ids}, conditionType={condition_type}")

            if not crew_id:
                logger.warning(f"Listener {idx} missing crew_id, skipping")
                continue

            # Skip ROUTER listeners - handled separately
            if condition_type == 'ROUTER':
                logger.info(f"Listener {idx} is ROUTER type, skipping (handled by routers)")
                continue

            # Initialize crew entry if not exists
            if crew_id not in crews_map:
                crews_map[crew_id] = {
                    'task_ids': [],
                    'listen_to_task_ids': set(),
                    'condition_type': condition_type,
                    'listener_configs': []
                }

            # Add tasks from this listener to the crew
            for task_config in listener.get('tasks', []):
                task_id = task_config.get('id')
                if task_id and task_id not in crews_map[crew_id]['task_ids']:
                    crews_map[crew_id]['task_ids'].append(task_id)

            # Add listen_to targets
            for listen_id in listen_to_task_ids:
                crews_map[crew_id]['listen_to_task_ids'].add(listen_id)

            # Store original listener config for reference
            crews_map[crew_id]['listener_configs'].append(listener)

            # Use AND if multiple listen targets, otherwise keep existing type
            if len(crews_map[crew_id]['listen_to_task_ids']) > 1 and condition_type == 'NONE':
                crews_map[crew_id]['condition_type'] = 'AND'

        logger.info(f"Grouped listeners into {len(crews_map)} crews")

        # STEP 2: Process each crew (building task objects with task.context for sequential execution)
        for crew_idx, (crew_id, crew_info) in enumerate(crews_map.items()):
            method_name = f"listener_{crew_idx}"
            task_ids = crew_info['task_ids']
            listen_to_task_ids = list(crew_info['listen_to_task_ids'])
            condition_type = crew_info['condition_type']

            logger.info(f"Processing listener crew {crew_id} with {len(task_ids)} tasks")
            logger.info(f"  Tasks to execute: {task_ids}")
            logger.info(f"  Listening to: {listen_to_task_ids}")
            logger.info(f"  Condition type: {condition_type}")

            if not task_ids:
                logger.warning(f"No tasks for listener crew {crew_id}, skipping")
                continue

            try:
                # Load crew data
                # Convert crew_id to UUID since the Crew model uses UUID(as_uuid=True)
                crew_uuid = _to_uuid(crew_id)
                crew_data = await crew_repo.get(crew_uuid) if crew_repo else None
                if not crew_data:
                    logger.warning(f"Crew {crew_id} not found for listener")
                    continue

                crew_name = crew_data.name if hasattr(crew_data, 'name') else f"Listener Crew {crew_idx}"

                # Build Task objects for all tasks in this crew
                crew_task_objects = []

                for task_idx, task_id in enumerate(task_ids):
                    task_data = await task_repo.get(task_id)

                    # Variables for embedded node data fallback
                    task_node_data = None
                    agent_node_data = None
                    agent_id = None

                    # Always scan crew structure for task/agent data (needed for both DB and embedded cases)
                    if crew_data.nodes:
                        for node in crew_data.nodes:
                            node_id = node.get('id', '')
                            node_uuid = node_id.split('-', 1)[1] if '-' in node_id else node_id

                            if node.get('type') == 'taskNode' and node_uuid == str(task_id):
                                # Found the task node - save its data for fallback
                                task_node_data = node.get('data', {})

                                # Find the agent connected to this task
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
                                                    # Save agent node data for fallback
                                                    agent_node_data = agent_node.get('data', {})
                                                    logger.info(f"  Resolved agent {agent_id} from crew structure for listener task {task_id}")
                                                    break
                                            if agent_id:
                                                break
                                break

                    # If task not in DB, try to create from embedded node data
                    if not task_data:
                        if task_node_data:
                            logger.info(f"Listener task {task_id} not in database, using embedded node data from crew")
                            # Create a mock task data object from node data
                            from types import SimpleNamespace
                            task_data = SimpleNamespace(
                                id=task_id,
                                name=task_node_data.get('label', f'Task {task_id}'),
                                description=task_node_data.get('description', ''),
                                expected_output=task_node_data.get('expected_output', ''),
                                agent_id=agent_id,
                                tools=task_node_data.get('tools', []),
                                tool_configs=task_node_data.get('tool_configs', {}),
                                async_execution=task_node_data.get('async_execution', False),
                                context=task_node_data.get('context', []),
                                config=task_node_data.get('config', {}),
                                memory=task_node_data.get('memory', False),
                                markdown=task_node_data.get('markdown', False),
                                output_file=task_node_data.get('config', {}).get('output_file'),
                                output_json=task_node_data.get('config', {}).get('output_json'),
                                output_pydantic=task_node_data.get('config', {}).get('output_pydantic'),
                                human_input=task_node_data.get('config', {}).get('human_input', False),
                                converter_cls=None
                            )
                        else:
                            logger.warning(f"Listener task {task_id} not found in database or crew nodes, skipping")
                            continue

                    # Fallback to task.agent_id if crew structure lookup failed
                    if not agent_id:
                        agent_id = getattr(task_data, 'agent_id', None)
                        if agent_id:
                            logger.info(f"  Using agent {agent_id} from task.agent_id for listener task {task_id}")

                    if not agent_id:
                        logger.warning(f"Could not resolve agent for listener task {task_id}")
                        continue

                    # Build CrewAI Task and Agent objects
                    from src.engines.crewai.flow.modules.task_config import TaskConfig
                    from src.engines.crewai.flow.modules.agent_config import AgentConfig

                    agent_repo = repositories.get('agent') if repositories else None
                    agent_data = await agent_repo.get(agent_id) if agent_repo else None

                    # If agent not in DB, try to create from embedded node data
                    if not agent_data and agent_node_data:
                        logger.info(f"Listener agent {agent_id} not in database, using embedded node data from crew")
                        from types import SimpleNamespace
                        # Helper to handle None values with defaults
                        def _get_val(d, key, default):
                            val = d.get(key)
                            return default if val is None else val

                        agent_data = SimpleNamespace(
                            id=agent_id,
                            name=agent_node_data.get('label', f'Agent {agent_id}'),
                            role=agent_node_data.get('role', ''),
                            goal=agent_node_data.get('goal', ''),
                            backstory=agent_node_data.get('backstory', ''),
                            llm=agent_node_data.get('llm'),
                            function_calling_llm=agent_node_data.get('function_calling_llm'),
                            tools=agent_node_data.get('tools') or [],
                            tool_configs=agent_node_data.get('tool_configs') or {},
                            max_iter=agent_node_data.get('max_iter'),
                            max_rpm=agent_node_data.get('max_rpm'),
                            max_execution_time=agent_node_data.get('max_execution_time'),
                            verbose=_get_val(agent_node_data, 'verbose', True),
                            allow_delegation=_get_val(agent_node_data, 'allow_delegation', False),
                            cache=_get_val(agent_node_data, 'cache', True),
                            memory=_get_val(agent_node_data, 'memory', False),
                            embedder_config=agent_node_data.get('embedder_config'),
                            system_template=agent_node_data.get('system_template'),
                            prompt_template=agent_node_data.get('prompt_template'),
                            response_template=agent_node_data.get('response_template'),
                            allow_code_execution=_get_val(agent_node_data, 'allow_code_execution', False),
                            code_execution_mode=agent_node_data.get('code_execution_mode'),
                            max_retry_limit=_get_val(agent_node_data, 'max_retry_limit', 2),
                            use_system_prompt=_get_val(agent_node_data, 'use_system_prompt', True),
                            respect_context_window=_get_val(agent_node_data, 'respect_context_window', True),
                            knowledge_sources=agent_node_data.get('knowledge_sources'),
                            reasoning_mode=agent_node_data.get('reasoning_mode'),
                            reasoning_config=agent_node_data.get('reasoning_config')
                        )

                    if agent_data:
                        # Merge tool_configs: task-level takes priority over crew-level
                        # MCP servers are typically configured at the task level
                        effective_tool_configs = {}
                        if crew_data and hasattr(crew_data, 'tool_configs') and crew_data.tool_configs:
                            if isinstance(crew_data.tool_configs, dict):
                                effective_tool_configs.update(crew_data.tool_configs)
                        if hasattr(task_data, 'tool_configs') and task_data.tool_configs:
                            if isinstance(task_data.tool_configs, dict):
                                effective_tool_configs.update(task_data.tool_configs)

                        logger.info(f"Listener task {task_id} effective tool_configs: {effective_tool_configs}")

                        # Build CrewAI Agent object
                        agent_obj = await AgentConfig.configure_agent_and_tools(
                            agent_data=agent_data,
                            flow_data=flow_config,
                            repositories=repositories,
                            group_context=group_context,
                            crew_tool_configs=effective_tool_configs if effective_tool_configs else None
                        )

                        # Build CrewAI Task object with the agent
                        task_obj = await TaskConfig.configure_task(
                            task_data=task_data,
                            agent=agent_obj,
                            task_output_callback=callbacks.get('task_callback') if callbacks else None,
                            flow_data=flow_config,
                            repositories=repositories,
                            group_context=group_context
                        )

                        # Set up task.context for sequential dependencies within crew
                        # Only set context if task is NOT marked for async execution
                        is_async = getattr(task_data, 'async_execution', False)

                        if is_async:
                            logger.info(f"  Task {task_id} marked for async execution (parallel)")
                        elif task_idx > 0 and len(crew_task_objects) > 0:
                            # Sequential execution - set context to wait for previous task
                            task_obj.context = [crew_task_objects[-1]]
                            logger.info(f"  Task {task_id} will wait for previous task (sequential via task.context)")

                        # Store the task object
                        all_tasks[str(task_id)] = task_obj
                        crew_task_objects.append(task_obj)
                        logger.info(f"  Added task {task_id} (index {task_idx}) to listener crew")
                    else:
                        logger.warning(f"Agent {agent_id} not found in database or crew nodes for listener task {task_id}")

                # If we successfully built tasks for this listener crew, add it
                if crew_task_objects:
                    # Return rich structure for flow_builder to create proper crew-level listeners
                    # Format: (method_name, crew_id, task_ids, task_objects, crew_name, listen_to_task_ids, condition_type, crew_data)
                    listener_info = (
                        method_name,
                        crew_id,
                        task_ids,
                        crew_task_objects,
                        crew_name,
                        listen_to_task_ids,
                        condition_type,
                        crew_data
                    )
                    listener_methods.append(listener_info)
                    logger.info(f"Added listener crew {method_name} for crew {crew_id} with {len(crew_task_objects)} sequential tasks")
                else:
                    logger.warning(f"No valid tasks found for listener crew {crew_id}")

            except Exception as e:
                logger.error(f"Error processing listener crew {crew_id}: {e}", exc_info=True)
                continue

        logger.info(f"Processed {len(listener_methods)} listener crews")
        return listener_methods

    @staticmethod
    async def process_routers(
        flow_config,
        all_tasks,
        repositories,
        group_context=None,
        callbacks=None
    ):
        """
        Process routers from flow configuration.

        Args:
            flow_config: Flow configuration with routers
            all_tasks: Dictionary to populate with task objects
            repositories: Dictionary of repositories to load data
            group_context: Group context for multi-tenant isolation
            callbacks: Callbacks for execution monitoring

        Returns:
            List of tuples (router_method_name, routes_config)
        """
        logger.info("Processing routers")
        router_configs = []

        task_repo = repositories.get('task') if repositories else None
        crew_repo = repositories.get('crew') if repositories else None

        if not task_repo:
            logger.warning("No task repository provided for routers")
            return router_configs

        for idx, router_config in enumerate(flow_config.get('routers', [])):
            router_method_name = f"router_{idx}"
            listen_to = router_config.get('listenTo')
            routes = router_config.get('routes', {})  # Dict format: {route_name: [task_configs]}
            route_conditions = router_config.get('routeConditions', {})  # Dict format: {route_name: condition_expr}

            if not listen_to:
                logger.warning(f"Router {idx} missing listenTo")
                continue

            # Debug: Log the routes and conditions structure
            logger.info(f"Router {idx} routes type: {type(routes)}, value: {routes}")
            logger.info(f"Router {idx} route_conditions: {route_conditions}")

            try:
                # Process each route - routes is a dict: {route_name: [task_configs]}
                processed_routes = []
                for route_name, route_task_configs in routes.items():
                    # Debug: Log each route structure
                    logger.info(f"  Route '{route_name}' task_configs: {route_task_configs}")

                    # Get condition for this route from routeConditions dict
                    condition = route_conditions.get(route_name, '')

                    # Get crew_id from the first task config (all tasks in a route are from the same crew)
                    if not route_task_configs:
                        logger.warning(f"Route '{route_name}' has no task configs")
                        continue

                    first_task_config = route_task_configs[0] if isinstance(route_task_configs, list) else route_task_configs
                    crew_id = first_task_config.get('crewId')

                    if not crew_id:
                        logger.warning(f"Route '{route_name}' missing crew_id in router {idx}")
                        continue

                    # Load crew data
                    # Convert crew_id to UUID since the Crew model uses UUID(as_uuid=True)
                    crew_uuid = _to_uuid(crew_id)
                    crew_data = await crew_repo.get(crew_uuid) if crew_repo else None
                    if not crew_data:
                        logger.warning(f"Crew {crew_id} not found for route {route_name}")
                        continue

                    # Process tasks for this route - route_task_configs is already the list of task configs
                    route_tasks = []
                    for task_config in route_task_configs:
                        task_id = task_config.get('id')
                        if not task_id:
                            continue

                        task_data = await task_repo.get(task_id)

                        # Variables for embedded node data fallback
                        task_node_data = None
                        agent_node_data = None
                        agent_id = None

                        # Always scan crew structure for task/agent data (needed for both DB and embedded cases)
                        if crew_data.nodes:
                            for node in crew_data.nodes:
                                node_id = node.get('id', '')
                                node_uuid = node_id.split('-', 1)[1] if '-' in node_id else node_id

                                if node.get('type') == 'taskNode' and node_uuid == str(task_id):
                                    # Found the task node - save its data for fallback
                                    task_node_data = node.get('data', {})

                                    # Find the agent connected to this task
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
                                                        # Save agent node data for fallback
                                                        agent_node_data = agent_node.get('data', {})
                                                        logger.info(f"  Resolved agent {agent_id} from crew structure for router task {task_id}")
                                                        break
                                                if agent_id:
                                                    break
                                    break

                        # If task not in DB, try to create from embedded node data
                        if not task_data:
                            if task_node_data:
                                logger.info(f"Router task {task_id} not in database, using embedded node data from crew")
                                # Create a mock task data object from node data
                                from types import SimpleNamespace
                                task_data = SimpleNamespace(
                                    id=task_id,
                                    name=task_node_data.get('label', f'Task {task_id}'),
                                    description=task_node_data.get('description', ''),
                                    expected_output=task_node_data.get('expected_output', ''),
                                    agent_id=agent_id,
                                    tools=task_node_data.get('tools', []),
                                    tool_configs=task_node_data.get('tool_configs', {}),
                                    async_execution=task_node_data.get('async_execution', False),
                                    context=task_node_data.get('context', []),
                                    config=task_node_data.get('config', {}),
                                    memory=task_node_data.get('memory', False),
                                    markdown=task_node_data.get('markdown', False),
                                    output_file=task_node_data.get('config', {}).get('output_file'),
                                    output_json=task_node_data.get('config', {}).get('output_json'),
                                    output_pydantic=task_node_data.get('config', {}).get('output_pydantic'),
                                    human_input=task_node_data.get('config', {}).get('human_input', False),
                                    converter_cls=None
                                )
                            else:
                                logger.warning(f"Router task {task_id} not found in database or crew nodes for route {route_name}")
                                continue

                        # Fallback to task.agent_id if crew structure lookup failed
                        if not agent_id:
                            agent_id = getattr(task_data, 'agent_id', None)
                            if agent_id:
                                logger.info(f"  Using agent {agent_id} from task.agent_id for router task {task_id}")

                        if not agent_id:
                            logger.warning(f"Could not resolve agent for router task {task_id}")
                            continue

                        # Build CrewAI Task and Agent objects
                        from src.engines.crewai.flow.modules.task_config import TaskConfig
                        from src.engines.crewai.flow.modules.agent_config import AgentConfig

                        agent_repo = repositories.get('agent') if repositories else None
                        agent_data = await agent_repo.get(agent_id) if agent_repo else None

                        # If agent not in DB, try to create from embedded node data
                        if not agent_data and agent_node_data:
                            logger.info(f"Router agent {agent_id} not in database, using embedded node data from crew")
                            from types import SimpleNamespace

                            # Helper to handle None values with defaults
                            def _get_val(d, key, default):
                                val = d.get(key)
                                return default if val is None else val

                            agent_data = SimpleNamespace(
                                id=agent_id,
                                name=agent_node_data.get('label', f'Agent {agent_id}'),
                                role=agent_node_data.get('role', ''),
                                goal=agent_node_data.get('goal', ''),
                                backstory=agent_node_data.get('backstory', ''),
                                llm=agent_node_data.get('llm'),
                                function_calling_llm=agent_node_data.get('function_calling_llm'),
                                tools=agent_node_data.get('tools') or [],
                                tool_configs=agent_node_data.get('tool_configs') or {},
                                max_iter=agent_node_data.get('max_iter'),
                                max_rpm=agent_node_data.get('max_rpm'),
                                max_execution_time=agent_node_data.get('max_execution_time'),
                                verbose=_get_val(agent_node_data, 'verbose', True),
                                allow_delegation=_get_val(agent_node_data, 'allow_delegation', False),
                                cache=_get_val(agent_node_data, 'cache', True),
                                memory=_get_val(agent_node_data, 'memory', False),
                                embedder_config=agent_node_data.get('embedder_config'),
                                system_template=agent_node_data.get('system_template'),
                                prompt_template=agent_node_data.get('prompt_template'),
                                response_template=agent_node_data.get('response_template'),
                                allow_code_execution=_get_val(agent_node_data, 'allow_code_execution', False),
                                code_execution_mode=agent_node_data.get('code_execution_mode'),
                                max_retry_limit=_get_val(agent_node_data, 'max_retry_limit', 2),
                                use_system_prompt=_get_val(agent_node_data, 'use_system_prompt', True),
                                respect_context_window=_get_val(agent_node_data, 'respect_context_window', True),
                                knowledge_sources=agent_node_data.get('knowledge_sources'),
                                reasoning_mode=agent_node_data.get('reasoning_mode'),
                                reasoning_config=agent_node_data.get('reasoning_config')
                            )

                        if agent_data:
                            # Merge tool_configs: task-level takes priority over crew-level
                            # MCP servers are typically configured at the task level
                            effective_tool_configs = {}
                            if crew_data and hasattr(crew_data, 'tool_configs') and crew_data.tool_configs:
                                if isinstance(crew_data.tool_configs, dict):
                                    effective_tool_configs.update(crew_data.tool_configs)
                            if hasattr(task_data, 'tool_configs') and task_data.tool_configs:
                                if isinstance(task_data.tool_configs, dict):
                                    effective_tool_configs.update(task_data.tool_configs)

                            logger.info(f"Router task {task_id} effective tool_configs: {effective_tool_configs}")

                            # Build CrewAI Agent object using configure_agent_and_tools
                            agent_obj = await AgentConfig.configure_agent_and_tools(
                                agent_data=agent_data,
                                flow_data=flow_config,
                                repositories=repositories,
                                group_context=group_context,
                                crew_tool_configs=effective_tool_configs if effective_tool_configs else None
                            )

                            # Build CrewAI Task object with the agent using configure_task
                            task_obj = await TaskConfig.configure_task(
                                task_data=task_data,
                                agent=agent_obj,
                                task_output_callback=callbacks.get('task_callback') if callbacks else None,
                                flow_data=flow_config,
                                repositories=repositories,
                                group_context=group_context
                            )

                            # Store the actual task object by task_id
                            all_tasks[str(task_id)] = task_obj
                            route_tasks.append(task_obj)
                        else:
                            logger.warning(f"Agent {agent_id} not found in database or crew nodes for router task {task_id}")

                    if route_tasks:
                        processed_routes.append({
                            'name': route_name,
                            'condition': condition,
                            'tasks': route_tasks,
                            'crew_id': crew_id
                        })
                        logger.info(f"Added route '{route_name}' with {len(route_tasks)} tasks")
                    else:
                        logger.warning(f"No valid tasks found for route {route_name}")

                if processed_routes:
                    router_configs.append((router_method_name, processed_routes))
                    logger.info(f"Added router {router_method_name} with {len(processed_routes)} routes, listening to '{listen_to}'")
                else:
                    logger.warning(f"No valid routes found for router {idx}")

            except Exception as e:
                logger.error(f"Error processing router {idx}: {e}", exc_info=True)
                continue

        logger.info(f"Processed {len(router_configs)} routers")
        return router_configs
