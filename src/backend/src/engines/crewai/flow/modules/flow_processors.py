"""
Flow processors module for CrewAI flow execution.

This module handles processing of starting points, listeners, and routers in flow configuration.
"""
import logging
from typing import Dict, List, Any, Optional, Callable
from crewai.flow.flow import listen, router

from src.core.logger import LoggerManager

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
                crew_data = await crew_repo.get(crew_id) if crew_repo else None
                if not crew_data:
                    logger.warning(f"Crew {crew_id} not found")
                    continue

                # Build Task objects for all tasks in this crew
                crew_task_objects = []

                for task_idx, task_id in enumerate(task_ids):
                    task_data = await task_repo.get(task_id)
                    if not task_data:
                        logger.warning(f"Task {task_id} not found, skipping")
                        continue

                    # Get agent_id - prioritize crew structure as source of truth
                    agent_id = None

                    # Try to get agent from crew structure (nodes/edges)
                    if crew_data.nodes:
                        for node in crew_data.nodes:
                            node_id = node.get('id', '')
                            node_uuid = node_id.split('-', 1)[1] if '-' in node_id else node_id

                            if node.get('type') == 'taskNode' and node_uuid == str(task_id):
                                # Found the task node, now find the agent connected to it
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
                                                    logger.info(f"  Resolved agent {agent_id} from crew structure for task {task_id}")
                                                    break
                                            if agent_id:
                                                break
                                break

                    # Fallback to task.agent_id if crew structure lookup failed
                    if not agent_id:
                        agent_id = task_data.agent_id
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
                    if agent_repo:
                        agent_data = await agent_repo.get(agent_id)
                        if agent_data:
                            # Build CrewAI Agent object using configure_agent_and_tools
                            agent_obj = await AgentConfig.configure_agent_and_tools(
                                agent_data=agent_data,
                                flow_data=flow_config,
                                repositories=repositories,
                                group_context=group_context,
                                crew_tool_configs=crew_data.tool_configs if crew_data and hasattr(crew_data, 'tool_configs') else None
                            )

                            # Build CrewAI Task object with the agent using configure_task
                            task_obj = await TaskConfig.configure_task(
                                task_data=task_data,
                                agent=agent_obj,
                                task_output_callback=callbacks.get('task_callback') if callbacks else None,
                                flow_data=flow_config,
                                repositories=repositories
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
                            logger.warning(f"Agent {agent_id} not found for task {task_id}")
                    else:
                        logger.warning(f"No agent repository provided for task {task_id}")

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
                    # Format: (method_name, task_ids_list, task_objects_list, crew_name)
                    # Include both IDs (for listener matching) and objects (for crew creation)
                    starting_point_info = (method_name, task_ids, crew_task_objects, crew_data.name if hasattr(crew_data, 'name') else f"Crew {crew_idx}")
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

        Args:
            flow_config: Flow configuration with listeners
            all_tasks: Dictionary to populate with task objects
            repositories: Dictionary of repositories to load data
            group_context: Group context for multi-tenant isolation
            callbacks: Callbacks for execution monitoring

        Returns:
            List of listener method names
        """
        logger.info("Processing listeners")
        listener_methods = []

        task_repo = repositories.get('task') if repositories else None
        crew_repo = repositories.get('crew') if repositories else None

        if not task_repo:
            logger.warning("No task repository provided for listeners")
            return listener_methods

        for idx, listener in enumerate(flow_config.get('listeners', [])):
            method_name = f"listener_{idx}"
            crew_id = listener.get('crewId')

            # Note: We don't need listenTo here - that's handled in flow_builder.py
            # We just need to load the listener tasks into all_tasks so flow_builder can find them
            if not crew_id:
                logger.warning(f"Listener {idx} missing crew_id")
                continue

            try:
                # Load crew data
                crew_data = await crew_repo.get(crew_id) if crew_repo else None
                if not crew_data:
                    logger.warning(f"Crew {crew_id} not found for listener {idx}")
                    continue

                # Process tasks for this listener
                listener_tasks = []
                for task_config in listener.get('tasks', []):
                    task_id = task_config.get('id')
                    if not task_id:
                        continue

                    task_data = await task_repo.get(task_id)
                    if not task_data:
                        logger.warning(f"Task {task_id} not found for listener {idx}")
                        continue

                    # Get agent_id from crew structure
                    agent_id = None
                    if crew_data.nodes:
                        for node in crew_data.nodes:
                            node_id = node.get('id', '')
                            node_uuid = node_id.split('-', 1)[1] if '-' in node_id else node_id

                            if node.get('type') == 'taskNode' and node_uuid == str(task_id):
                                if crew_data.edges:
                                    for edge in crew_data.edges:
                                        if edge.get('target') == node.get('id'):
                                            agent_node_id = edge.get('source')
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

                    # Fallback to task.agent_id
                    if not agent_id:
                        agent_id = task_data.agent_id

                    if not agent_id:
                        logger.warning(f"Could not resolve agent for task {task_id}")
                        continue

                    # Build CrewAI Task and Agent objects
                    from src.engines.crewai.flow.modules.task_config import TaskConfig
                    from src.engines.crewai.flow.modules.agent_config import AgentConfig

                    agent_repo = repositories.get('agent') if repositories else None
                    if agent_repo:
                        agent_data = await agent_repo.get(agent_id)
                        if agent_data:
                            # Build CrewAI Agent object using configure_agent_and_tools
                            agent_obj = await AgentConfig.configure_agent_and_tools(
                                agent_data=agent_data,
                                flow_data=flow_config,
                                repositories=repositories,
                                group_context=group_context,
                                crew_tool_configs=crew_data.tool_configs if crew_data and hasattr(crew_data, 'tool_configs') else None
                            )

                            # Build CrewAI Task object with the agent using configure_task
                            task_obj = await TaskConfig.configure_task(
                                task_data=task_data,
                                agent=agent_obj,
                                task_output_callback=callbacks.get('task_callback') if callbacks else None,
                                flow_data=flow_config,
                                repositories=repositories
                            )

                            # Store the actual task object by task_id
                            all_tasks[str(task_id)] = task_obj
                            listener_tasks.append(task_obj)
                        else:
                            logger.warning(f"Agent {agent_id} not found for listener task {task_id}")
                    else:
                        logger.warning(f"No agent repository for listener task {task_id}")

                if listener_tasks:
                    listener_methods.append(method_name)
                    logger.info(f"Added listener {method_name} with {len(listener_tasks)} tasks")
                else:
                    logger.warning(f"No valid tasks found for listener {idx}")

            except Exception as e:
                logger.error(f"Error processing listener {idx}: {e}", exc_info=True)
                continue

        logger.info(f"Processed {len(listener_methods)} listeners")
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
            routes = router_config.get('routes', [])

            if not listen_to:
                logger.warning(f"Router {idx} missing listenTo")
                continue

            # Debug: Log the routes structure
            logger.info(f"Router {idx} routes type: {type(routes)}, value: {routes}")

            try:
                # Process each route
                processed_routes = []
                for route in routes:
                    # Debug: Log each route structure
                    logger.info(f"  Route type: {type(route)}, value: {route}")

                    # Handle case where route might be a string instead of dict
                    if isinstance(route, str):
                        logger.warning(f"Router {idx} has route as string instead of dict: {route}")
                        continue

                    route_name = route.get('name')
                    condition = route.get('condition')
                    crew_id = route.get('crewId')

                    if not route_name or not crew_id:
                        logger.warning(f"Route missing name or crew_id in router {idx}")
                        continue

                    # Load crew data
                    crew_data = await crew_repo.get(crew_id) if crew_repo else None
                    if not crew_data:
                        logger.warning(f"Crew {crew_id} not found for route {route_name}")
                        continue

                    # Process tasks for this route
                    route_tasks = []
                    for task_config in route.get('tasks', []):
                        task_id = task_config.get('id')
                        if not task_id:
                            continue

                        task_data = await task_repo.get(task_id)
                        if not task_data:
                            logger.warning(f"Task {task_id} not found for route {route_name}")
                            continue

                        # Get agent_id from crew structure
                        agent_id = None
                        if crew_data.nodes:
                            for node in crew_data.nodes:
                                node_id = node.get('id', '')
                                node_uuid = node_id.split('-', 1)[1] if '-' in node_id else node_id

                                if node.get('type') == 'taskNode' and node_uuid == str(task_id):
                                    if crew_data.edges:
                                        for edge in crew_data.edges:
                                            if edge.get('target') == node.get('id'):
                                                agent_node_id = edge.get('source')
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

                        # Fallback to task.agent_id
                        if not agent_id:
                            agent_id = task_data.agent_id

                        if not agent_id:
                            logger.warning(f"Could not resolve agent for task {task_id}")
                            continue

                        # Build CrewAI Task and Agent objects
                        from src.engines.crewai.flow.modules.task_config import TaskConfig
                        from src.engines.crewai.flow.modules.agent_config import AgentConfig

                        agent_repo = repositories.get('agent') if repositories else None
                        if agent_repo:
                            agent_data = await agent_repo.get(agent_id)
                            if agent_data:
                                # Build CrewAI Agent object using configure_agent_and_tools
                                agent_obj = await AgentConfig.configure_agent_and_tools(
                                    agent_data=agent_data,
                                    flow_data=flow_config,
                                    repositories=repositories,
                                    group_context=group_context,
                                    crew_tool_configs=crew_data.tool_configs if crew_data and hasattr(crew_data, 'tool_configs') else None
                                )

                                # Build CrewAI Task object with the agent using configure_task
                                task_obj = await TaskConfig.configure_task(
                                    task_data=task_data,
                                    agent=agent_obj,
                                    task_output_callback=callbacks.get('task_callback') if callbacks else None,
                                    flow_data=flow_config,
                                    repositories=repositories
                                )

                                # Store the actual task object by task_id
                                all_tasks[str(task_id)] = task_obj
                                route_tasks.append(task_obj)
                            else:
                                logger.warning(f"Agent {agent_id} not found for route task {task_id}")
                        else:
                            logger.warning(f"No agent repository for route task {task_id}")

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
