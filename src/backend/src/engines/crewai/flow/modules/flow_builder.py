"""
Flow builder module for CrewAI flow execution.

This module handles the building of CrewAI flows from configuration.

ARCHITECTURE:
This module orchestrates flow building using several specialized sub-modules:
- flow_config.py: MCP requirements collection from flow tasks
- flow_processors.py: Processing of starting points, listeners, and routers
- flow_state.py: State management and crew output parsing
- flow_methods.py: Dynamic method creation for flow execution

The FlowBuilder class coordinates these modules to construct complete CrewAI flows.
"""
import logging
from typing import Dict, List, Optional, Any, Union
from crewai.flow.flow import Flow as CrewAIFlow
from crewai.flow.flow import start, listen, router, and_, or_
from crewai import Crew, Agent, Task, Process
from pydantic import BaseModel

# Note: persist decorator is not available in CrewAI 0.203.1
# Persistence will be handled through FlowPersistence class when available

from src.core.logger import LoggerManager
from src.engines.crewai.flow.modules.agent_config import AgentConfig
from src.engines.crewai.flow.modules.task_config import TaskConfig
from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

# Import new modular components
from src.engines.crewai.flow.modules.flow_config import FlowConfigManager
from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager
from src.engines.crewai.flow.modules.flow_state import FlowStateManager
from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

# Initialize logger - use flow logger for flow execution
logger = LoggerManager.get_instance().flow

class FlowBuilder:
    """
    Helper class for building CrewAI flows.
    """
    
    @staticmethod
    async def build_flow(flow_data, repositories=None, callbacks=None, group_context=None):
        """
        Build a CrewAI flow from flow data.

        Args:
            flow_data: Flow data from the database
            repositories: Dictionary of repositories (optional)
            callbacks: Dictionary of callbacks (optional)
            group_context: Group context for multi-tenant isolation (optional)

        Returns:
            CrewAIFlow: A configured CrewAI Flow instance
        """
        logger.info("Building CrewAI Flow")

        if not flow_data:
            logger.error("No flow data provided")
            raise ValueError("No flow data provided")

        try:
            # Parse flow configuration
            flow_config = flow_data.get('flow_config', {})

            if not flow_config:
                logger.warning("No flow_config found in flow data")
                # Try to parse flow_config from a string if needed
                if isinstance(flow_data.get('flow_config'), str):
                    try:
                        import json
                        flow_config = json.loads(flow_data.get('flow_config'))
                        logger.info("Successfully parsed flow_config from string")
                    except Exception as e:
                        logger.error(f"Failed to parse flow_config string: {e}")

            # Log the flow configuration for debugging
            logger.info(f"Flow configuration for processing: {flow_config}")

            # Check for starting points
            starting_points = flow_config.get('startingPoints', [])
            if not starting_points:
                logger.error("No starting points defined in flow configuration")
                raise ValueError("No starting points defined in flow configuration")

            # Check for listeners
            listeners = flow_config.get('listeners', [])
            logger.info(f"Found {len(listeners)} listeners in flow config")

            # Check for routers (conditional routing)
            routers = flow_config.get('routers', [])
            logger.info(f"Found {len(routers)} routers in flow config")

            # CRITICAL: Collect MCP requirements from tasks before creating agents
            # This follows the same pattern as regular crew execution in crew_preparation.py
            agent_mcp_requirements = await FlowConfigManager.collect_agent_mcp_requirements(
                flow_config, repositories, group_context
            )
            logger.info(f"Collected MCP requirements for {len(agent_mcp_requirements)} agents from flow tasks")

            # Parse all tasks, agents, and tools
            all_agents = {}
            all_tasks = {}

            # Process all starting points first to collect tasks and agents using FlowProcessorManager
            # Note: FlowProcessorManager returns method names, but we need the actual task objects
            # So we'll still populate all_tasks dict here
            starting_point_methods = await FlowProcessorManager.process_starting_points(
                flow_config, all_tasks, repositories, group_context, callbacks
            )
            logger.info(f"FlowProcessorManager returned {len(starting_point_methods)} starting point methods")

            # Process all listener tasks using FlowProcessorManager
            listener_methods = await FlowProcessorManager.process_listeners(
                flow_config, all_tasks, repositories, group_context, callbacks
            )
            logger.info(f"FlowProcessorManager returned {len(listener_methods)} listener methods")

            # Process router tasks using FlowProcessorManager
            router_configs = await FlowProcessorManager.process_routers(
                flow_config, all_tasks, repositories, group_context, callbacks
            )
            logger.info(f"FlowProcessorManager returned {len(router_configs)} router configs")

            # Build all_agents from all_tasks
            for task_id, task_obj in all_tasks.items():
                if hasattr(task_obj, 'agent') and task_obj.agent:
                    agent = task_obj.agent
                    if hasattr(agent, 'role'):
                        all_agents[agent.role] = agent

            # Now build the flow class with proper structure
            # Create a dynamic flow class
            logger.info("="*100)
            logger.info("ABOUT TO CREATE DYNAMIC FLOW CLASS")
            logger.info("="*100)
            logger.info(f"Starting points count: {len(starting_points)}")
            logger.info(f"Listeners count: {len(listeners)}")
            logger.info(f"Routers count: {len(routers)}")
            logger.info(f"All agents count: {len(all_agents)}")
            logger.info(f"All tasks count: {len(all_tasks)}")

            # Log details of starting points
            logger.info(f"All tasks available: {list(all_tasks.keys())}")
            logger.info(f"Starting points structure: {starting_points}")

            # Extract task IDs from starting points based on their structure
            # Starting points can be:
            # 1. Simple format: {"taskId": "...", "crewId": "..."}
            # 2. Node format: {"nodeType": "crewNode", "nodeData": {"allTasks": [...]}}
            # 3. Node format: {"nodeType": "agentNode", "nodeData": {"taskId": "..."}}

            extracted_task_ids = []
            for i, sp in enumerate(starting_points):
                logger.info(f"Starting point {i} type: {sp.get('nodeType', 'simple')}")

                node_type = sp.get('nodeType')
                if node_type == 'crewNode':
                    # For crew nodes, extract tasks from allTasks
                    node_data = sp.get('nodeData', {})
                    all_tasks_in_node = node_data.get('allTasks', [])
                    for task in all_tasks_in_node:
                        task_id = task.get('id')
                        if task_id:
                            extracted_task_ids.append(task_id)
                            logger.info(f"  ✅ Extracted task ID from crewNode: {task_id}")
                elif node_type == 'agentNode':
                    # Agent nodes don't have tasks, skip them
                    logger.info(f"  ⏭️  Skipping agentNode (no tasks)")
                    continue
                else:
                    # Simple format - try to get taskId directly
                    task_id = sp.get('taskId') or sp.get('task_id')
                    if task_id:
                        extracted_task_ids.append(task_id)
                        logger.info(f"  ✅ Extracted task ID from simple format: {task_id}")

            logger.info(f"Extracted {len(extracted_task_ids)} task IDs from starting points: {extracted_task_ids}")

            # Validate extracted task IDs
            for task_id in extracted_task_ids:
                if task_id in all_tasks:
                    logger.info(f"  ✅ Task {task_id} found in all_tasks")
                else:
                    logger.error(f"  ❌ Task {task_id} NOT found in all_tasks!")
                    logger.error(f"  Available task IDs: {list(all_tasks.keys())}")

            dynamic_flow = await FlowBuilder._create_dynamic_flow(
                starting_point_methods, listeners, routers, all_agents, all_tasks, flow_config, callbacks, group_context
            )

            logger.info("="*100)
            logger.info(f"✅ Dynamic flow created successfully, type: {type(dynamic_flow)}")
            logger.info("="*100)

            # Log the methods we've added to help diagnose issues
            flow_methods = [method for method in dir(dynamic_flow) if callable(getattr(dynamic_flow, method)) and not method.startswith('_')]
            logger.info(f"Flow has {len(flow_methods)} public methods: {flow_methods}")

            # Specifically check for start methods
            start_methods = [m for m in flow_methods if m.startswith('starting_point_')]
            if start_methods:
                logger.info(f"✅ Found {len(start_methods)} start methods: {start_methods}")
            else:
                logger.error("❌ NO START METHODS FOUND in created flow!")
                logger.error("This is a critical error - flow cannot execute without start methods")
            
            return dynamic_flow
            
        except Exception as e:
            logger.error(f"Error building flow: {e}", exc_info=True)
            raise ValueError(f"Failed to build flow: {str(e)}")
    
    # Removed: _collect_agent_mcp_requirements_from_flow
    # Now using FlowConfigManager.collect_agent_mcp_requirements from flow_config.py module

    @staticmethod
    def _apply_state_operations(flow_instance, state_operations):
        """
        Apply state operations (reads, writes, conditions) to the flow state.

        Args:
            flow_instance: The flow instance with state
            state_operations: Dictionary containing reads, writes, and conditions
        """
        if not state_operations:
            return

        # Handle state reads (just log for now, actual reads happen in expressions)
        reads = state_operations.get('reads', [])
        if reads:
            logger.info(f"Reading state variables: {reads}")
            for var in reads:
                value = flow_instance.state.get(var) if hasattr(flow_instance.state, 'get') else getattr(flow_instance.state, var, None)
                logger.info(f"  {var} = {value}")

        # Handle state writes
        writes = state_operations.get('writes', [])
        if writes:
            logger.info(f"Writing state variables: {[w.get('variable') for w in writes]}")
            for write in writes:
                variable = write.get('variable')
                expression = write.get('expression')
                value = write.get('value')

                if expression:
                    # Evaluate the expression
                    try:
                        # Create a safe evaluation context
                        eval_context = {
                            'state': flow_instance.state,
                        }
                        computed_value = eval(expression, {"__builtins__": {}}, eval_context)
                        if hasattr(flow_instance.state, 'get'):
                            flow_instance.state[variable] = computed_value
                        else:
                            setattr(flow_instance.state, variable, computed_value)
                        logger.info(f"  {variable} = {computed_value} (from expression: {expression})")
                    except Exception as e:
                        logger.error(f"Failed to evaluate expression '{expression}': {e}")
                elif value is not None:
                    if hasattr(flow_instance.state, 'get'):
                        flow_instance.state[variable] = value
                    else:
                        setattr(flow_instance.state, variable, value)
                    logger.info(f"  {variable} = {value}")

    @staticmethod
    async def _create_dynamic_flow(starting_points, listeners, routers, all_agents, all_tasks, flow_config=None, callbacks=None, group_context=None):
        """
        Create a dynamic flow class with all start, listener, and router methods.

        Args:
            starting_points: List of tuples (method_name, task_ids, crew_name) for starting point crews
            listeners: List of original listener configuration dictionaries from flow_config
            routers: List of original router configuration dictionaries from flow_config
            all_agents: Dictionary of configured agents
            all_tasks: Dictionary of configured tasks (with task.context dependencies set)
            flow_config: Flow configuration including state and persistence settings
            callbacks: Dictionary of callback handlers for execution logging
            group_context: Group context for multi-tenant isolation

        Returns:
            CrewAIFlow: An instance of the dynamically created flow class
        """
        # Extract state and persistence configuration
        state_config = flow_config.get('state', {}) if flow_config else {}
        persistence_config = flow_config.get('persistence', {}) if flow_config else {}

        state_enabled = state_config.get('enabled', False)
        state_type = state_config.get('type', 'unstructured')
        state_model = state_config.get('model')
        state_initial_values = state_config.get('initialValues', {})

        persistence_enabled = persistence_config.get('enabled', False)
        persistence_level = persistence_config.get('level', 'none')

        logger.info(f"State enabled: {state_enabled}, type: {state_type}")
        logger.info(f"Persistence enabled: {persistence_enabled}, level: {persistence_level}")

        # CRITICAL: Build all methods FIRST, then create class WITH them
        # The FlowMeta metaclass processes @start() and @listen() decorators during class creation
        # Adding methods via setattr() after class creation doesn't work!

        # Dictionary to collect all class methods
        class_methods = {}

        # Create __init__ method for state management
        def create_init_method():
            if state_enabled:
                def __init__(self):
                    super(type(self), self).__init__()
                    if state_initial_values:
                        self.state.update(state_initial_values)
            else:
                def __init__(self):
                    super(type(self), self).__init__()
            return __init__

        class_methods['__init__'] = create_init_method()

        # Note: Class-level persistence decorator not available in CrewAI 0.203.1
        # Persistence configuration is stored but not applied
        # Will be implemented when CrewAI adds @persist decorator support
        if persistence_enabled and persistence_level == 'class':
            logger.info("Class-level persistence requested (not yet supported in CrewAI 0.203.1)")
            logger.info("Persistence configuration will be available in future CrewAI versions")
        
        # Add start methods for each starting point (now receiving tuples with crew info)
        logger.info("="*100)
        logger.info(f"ADDING START METHODS - Processing {len(starting_points)} starting point crews")
        logger.info("="*100)

        # starting_points now contains tuples: (method_name, task_ids_list, task_objects_list, crew_name)
        for i, starting_point_info in enumerate(starting_points):
            # Unpack the tuple
            method_name, task_ids, crew_tasks, crew_name = starting_point_info

            logger.info(f"Creating start method for crew: {crew_name}")
            logger.info(f"  Method name: {method_name}")
            logger.info(f"  Task IDs: {task_ids}")
            logger.info(f"  Number of tasks: {len(crew_tasks)}")

            # crew_tasks already contains task objects (no lookup needed)
            if crew_tasks:
                logger.info(f"  Creating method: {method_name} with {len(crew_tasks)} tasks")

                # Use FlowMethodFactory to create the starting point method with ALL tasks
                class_methods[method_name] = FlowMethodFactory.create_starting_point_crew_method(
                    method_name=method_name,
                    task_list=crew_tasks,
                    crew_name=crew_name,
                    callbacks=callbacks,
                    group_context=group_context,
                    create_execution_callbacks=create_execution_callbacks
                )
                logger.info(f"  ✅ Created start method '{method_name}' for crew '{crew_name}' with {len(crew_tasks)} sequential tasks")
            else:
                logger.error(f"  ❌ No tasks found for crew '{crew_name}', skipping start method creation")
        
        # Add listener methods for each listener
        logger.info("="*100)
        logger.info(f"ADDING LISTENER METHODS - Processing {len(listeners)} listeners")
        logger.info("="*100)

        for i, listener_config in enumerate(listeners):
            listen_to_task_ids = listener_config.get('listenToTaskIds', [])
            condition_type = listener_config.get('conditionType', 'NONE')

            logger.info(f"Listener {i}: conditionType={condition_type}, listenToTaskIds={listen_to_task_ids}")

            # Skip ROUTER listeners - they should be handled by routers, not listeners
            if condition_type == 'ROUTER':
                logger.warning(f"  ⏭️  Skipping listener {i} - ROUTER type should be handled by router methods")
                continue

            # Skip if no tasks to listen to
            if not listen_to_task_ids:
                logger.warning(f"  ⏭️  Skipping listener {i} - no listenToTaskIds")
                continue

            # Get the listener tasks
            listener_tasks = []
            for task_config in listener_config.get('tasks', []):
                task_id = task_config.get('id')
                if task_id in all_tasks:
                    listener_tasks.append(all_tasks[task_id])

            logger.info(f"  Found {len(listener_tasks)} listener tasks to execute")

            # Skip if no listener tasks
            if not listener_tasks:
                logger.warning(f"  ⏭️  Skipping listener {i} - no valid listener tasks")
                continue

            # Create the proper decorator based on condition_type
            listener_created = False  # Track if we've created the listener for AND/OR
            for j, listen_task_id in enumerate(listen_to_task_ids):
                # Skip if the task to listen to is not in our collection
                if listen_task_id not in all_tasks:
                    continue

                method_name = f"listen_task_{i}_{j}"

                # Handle different condition types
                if condition_type in ["AND", "OR"]:
                    # For AND/OR conditions, we only need one listener for all tasks
                    if not listener_created:  # Only create once (first valid task)
                        # Create method condition for AND/OR
                        # Need to find which start methods correspond to the tasks we're listening to
                        # starting_points is now a list of tuples: (method_name, task_ids, task_objects, crew_name)
                        method_names = []
                        for starting_point_info in starting_points:
                            method_name_sp, task_ids_sp, task_objects_sp, crew_name_sp = starting_point_info
                            # Check if any task in this starting point's tasks matches what we're listening to
                            for task_id in task_ids_sp:
                                if str(task_id) in [str(tid) for tid in listen_to_task_ids]:
                                    method_names.append(method_name_sp)
                                    break  # Found a match, don't add this method multiple times

                        logger.info(f"Creating {condition_type} listener for tasks {listen_to_task_ids}")
                        logger.info(f"Found {len(method_names)} matching start methods: {method_names}")

                        # Default to first starting point if no matches found
                        default_start_method = starting_points[0][0] if starting_points else "starting_point_0"

                        if condition_type == "AND":
                            method_condition = and_(*method_names) if len(method_names) > 1 else method_names[0] if method_names else default_start_method
                        else:  # OR
                            method_condition = or_(*method_names) if len(method_names) > 1 else method_names[0] if method_names else default_start_method

                        # Use FlowMethodFactory to create the listener method
                        class_methods[method_name] = FlowMethodFactory.create_listener_method(
                            method_name=method_name,
                            listener_tasks=listener_tasks,
                            method_condition=method_condition,
                            condition_type=condition_type,
                            callbacks=callbacks,
                            group_context=group_context,
                            create_execution_callbacks=create_execution_callbacks
                        )
                        logger.info(f"✅ Created {condition_type} listener {method_name} for class dictionary, listening to: {method_condition}")
                        listener_created = True  # Mark as created
                    break  # Skip other iterations
                else:
                    # For NONE/other conditions, create individual listeners
                    # Find the corresponding start method name by matching task ID
                    # starting_points is now a list of tuples: (method_name, task_ids, task_objects, crew_name)
                    method_condition = "starting_point_0"  # Default fallback

                    # Find which start method corresponds to this task
                    for starting_point_info in starting_points:
                        method_name_sp, task_ids_sp, task_objects_sp, crew_name_sp = starting_point_info
                        # Check if the listen_task_id is in this starting point's tasks
                        if str(listen_task_id) in [str(tid) for tid in task_ids_sp]:
                            method_condition = method_name_sp
                            logger.info(f"Found start method {method_condition} for task {listen_task_id}")
                            break

                    # Use FlowMethodFactory to create the listener method
                    class_methods[method_name] = FlowMethodFactory.create_listener_method(
                        method_name=method_name,
                        listener_tasks=listener_tasks,
                        method_condition=method_condition,
                        condition_type="NONE",
                        callbacks=callbacks,
                        group_context=group_context,
                        create_execution_callbacks=create_execution_callbacks
                    )
                    logger.info(f"✅ Created simple listener {method_name} for class dictionary, listening to: {method_condition}")

        # Add router methods for conditional routing
        for i, router_config in enumerate(routers):
            router_name = router_config.get('name', f'router_{i}')
            listen_to = router_config.get('listenTo')  # Method name to listen to
            routes = router_config.get('routes', {})  # Dict of route_name -> task configs
            condition_expr = router_config.get('condition')  # Python condition expression to evaluate
            condition_field = router_config.get('conditionField', 'success')

            # Find the method to listen to
            # Default to the first starting point method if not specified
            default_method = starting_points[0][0] if starting_points else "starting_point_0"
            listen_to_method = listen_to or default_method

            # Create router method
            def router_factory(router_routes, router_condition_expr, router_condition_field, router_method_name):
                @router(listen_to_method)
                def route_method(self, *args, **kwargs):
                    logger.info(f"Router {router_method_name} evaluating condition")

                    # If we have a condition expression, evaluate it
                    if router_condition_expr:
                        try:
                            import json

                            # Build evaluation context
                            eval_context = {}

                            # Add state to context if available
                            if hasattr(self, 'state'):
                                eval_context['state'] = self.state
                            else:
                                # Create empty state dict if not available
                                eval_context['state'] = {}

                            # Add result from args
                            if args:
                                eval_context['result'] = args[0]

                                # Try to extract values from CrewOutput
                                result_obj = args[0]

                                # If result has a 'raw' attribute (CrewOutput), try to parse it as JSON
                                if hasattr(result_obj, 'raw'):
                                    try:
                                        raw_str = str(result_obj.raw).strip()
                                        # Try to parse as JSON
                                        if raw_str.startswith('{') and raw_str.endswith('}'):
                                            parsed_data = json.loads(raw_str)
                                            # Merge parsed data into state for easy access
                                            eval_context['state'].update(parsed_data)
                                            logger.info(f"Parsed crew output JSON and merged into state: {parsed_data}")
                                    except (json.JSONDecodeError, Exception) as parse_err:
                                        logger.debug(f"Could not parse crew output as JSON: {parse_err}")

                                # Also add common fields from result
                                if isinstance(args[0], dict):
                                    eval_context.update(args[0])
                                elif hasattr(args[0], '__dict__'):
                                    eval_context.update(vars(args[0]))

                            # Add kwargs
                            eval_context.update(kwargs)

                            logger.info(f"Evaluating condition: {router_condition_expr}")
                            logger.info(f"Context keys: {list(eval_context.keys())}")
                            logger.info(f"State contents: {eval_context.get('state', {})}")

                            # Evaluate the condition
                            condition_result = eval(router_condition_expr, {"__builtins__": {}}, eval_context)
                            logger.info(f"Condition evaluated to: {condition_result}")

                            # If condition is True, take the default route; otherwise don't route
                            if condition_result:
                                default_route = list(router_routes.keys())[0] if router_routes else "default"
                                logger.info(f"Router {router_method_name} condition True, taking route: {default_route}")
                                return default_route
                            else:
                                logger.info(f"Router {router_method_name} condition False, no route taken")
                                return None  # No route - flow ends here

                        except Exception as e:
                            logger.error(f"Error evaluating router condition: {e}", exc_info=True)
                            # Don't route on error - let the flow stop
                            logger.warning(f"Router condition evaluation failed - no route taken (flow stops)")
                            return None
                    else:
                        # No condition expression - use simple value matching (legacy behavior)
                        condition_value = None

                        # Try to get condition from kwargs (result from previous method)
                        if kwargs and router_condition_field in kwargs:
                            condition_value = kwargs.get(router_condition_field)
                        # Try to get from self.state if it exists
                        elif hasattr(self, 'state') and hasattr(self.state, router_condition_field):
                            condition_value = getattr(self.state, router_condition_field)
                        elif hasattr(self, 'state') and isinstance(self.state, dict) and router_condition_field in self.state:
                            condition_value = self.state.get(router_condition_field)
                        # Default routing based on args
                        elif args:
                            # If we have a result, check if it's successful
                            result = args[0]
                            if hasattr(result, router_condition_field):
                                condition_value = getattr(result, router_condition_field)
                            elif isinstance(result, dict) and router_condition_field in result:
                                condition_value = result.get(router_condition_field)

                        # Determine which route to take
                        for route_name in router_routes.keys():
                            # Simple matching: if condition_value matches route_name, take that route
                            if condition_value == route_name:
                                logger.info(f"Router {router_method_name} taking route: {route_name}")
                                return route_name
                            # Boolean routing
                            if condition_value is True and route_name in ['success', 'true']:
                                logger.info(f"Router {router_method_name} taking success route")
                                return route_name
                            if condition_value is False and route_name in ['failed', 'false', 'failure']:
                                logger.info(f"Router {router_method_name} taking failure route")
                                return route_name

                        # Default to first route if no match
                        default_route = list(router_routes.keys())[0] if router_routes else "default"
                        logger.info(f"Router {router_method_name} taking default route: {default_route}")
                        return default_route

                route_method.__name__ = router_method_name
                return route_method

            # Add router method to flow
            router_method_name = f"router_{router_name}_{i}"
            bound_router = router_factory(routes, condition_expr, condition_field, router_method_name)
            class_methods[router_method_name] = bound_router
            logger.info(f"Created router method {router_method_name} for class dictionary with routes: {list(routes.keys())}")

            # Add listener methods for each route
            for route_name, route_tasks in routes.items():
                logger.info(f"Creating route listener for route '{route_name}' with {len(route_tasks)} tasks")
                logger.info(f"  Route tasks: {route_tasks}")
                logger.info(f"  All tasks available: {list(all_tasks.keys())}")

                route_task_objs = [all_tasks[t.get('id')] for t in route_tasks if t.get('id') in all_tasks]
                logger.info(f"  Found {len(route_task_objs)} task objects for route listener")

                if route_task_objs:
                    route_listener_name = f"route_{router_name}_{route_name}_{i}"

                    def route_listener_factory(route_task_list, route_listener_method_name, callbacks_param, group_ctx, expected_route):
                        @listen(expected_route)
                        async def route_listener_method(self, previous_output):
                            logger.info("="*80)
                            logger.info(f"ROUTE LISTENER METHOD CALLED - {route_listener_method_name}")
                            logger.info(f"Executing route listener for route: {expected_route}")

                            # Log and store previous output from router
                            if previous_output:
                                logger.info(f"📥 RECEIVED PREVIOUS OUTPUT FROM ROUTER:")
                                logger.info(f"  Output: {str(previous_output)[:200]}...")
                                self.state['previous_output'] = previous_output
                            else:
                                logger.info("📭 No previous output received from router")

                            logger.info("="*80)

                            # Get agents for these tasks
                            agents = list(set(task.agent for task in route_task_list))
                            logger.info(f"Number of agents in route listener: {len(agents)}")

                            # Create crew with route tasks
                            logger.info("Creating Crew instance for route listener")

                            # Set crew name based on first agent role or route name
                            route_crew_name = agents[0].role if agents and hasattr(agents[0], 'role') and agents[0].role else "Route Crew"
                            logger.info(f"Creating route crew with name: {route_crew_name}")

                            crew = Crew(
                                name=route_crew_name,  # Set crew name for proper event tracing
                                agents=agents,
                                tasks=route_task_list,
                                verbose=True,
                                process=Process.sequential
                            )
                            logger.info(f"Crew instance '{route_crew_name}' created for route")

                            # CRITICAL: Set up execution callbacks like regular crew execution
                            # Extract job_id directly from callbacks dict
                            job_id = None
                            if callbacks_param:
                                # Get job_id directly from callbacks dict (no longer using JobOutputCallback)
                                job_id = callbacks_param.get('job_id')
                                if job_id:
                                    logger.info(f"Extracted job_id from callbacks for route listener: {job_id}")

                            # Create and set synchronous step and task callbacks
                            if job_id:
                                try:
                                    step_callback, task_callback = create_execution_callbacks(
                                        job_id=job_id,
                                        config={},
                                        group_context=group_ctx,
                                        crew=crew
                                    )
                                    crew.step_callback = step_callback
                                    crew.task_callback = task_callback
                                    logger.info(f"✅ Set synchronous execution callbacks on route listener crew for job {job_id}")
                                except Exception as callback_error:
                                    logger.warning(f"Failed to set execution callbacks on route listener: {callback_error}")
                            else:
                                logger.warning("No job_id available for route listener, skipping execution callbacks setup")

                            result = await crew.kickoff_async()
                            logger.info(f"Route listener kickoff_async completed, result type: {type(result)}")
                            return result

                        route_listener_method.__name__ = route_listener_method_name
                        return route_listener_method

                    bound_route_listener = route_listener_factory(route_task_objs, route_listener_name, callbacks, group_context, route_name)
                    class_methods[route_listener_name] = bound_route_listener
                    logger.info(f"Created route listener {route_listener_name} for class dictionary listening to router '{router_method_name}' for route '{route_name}'")

        # CRITICAL: Create the DynamicFlow class WITH all methods using type()
        # This allows the FlowMeta metaclass to process @start() and @listen() decorators
        logger.info("="*100)
        logger.info(f"CREATING DYNAMICFLOW CLASS with {len(class_methods)} methods")
        logger.info("="*100)

        DynamicFlow = type(
            'DynamicFlow',      # Class name
            (CrewAIFlow,),      # Base classes (includes FlowMeta metaclass)
            class_methods       # All methods in dictionary
        )
        logger.info("✅ DynamicFlow class created with FlowMeta metaclass processing")

        # Create an instance of our properly defined flow
        flow_instance = DynamicFlow()

        # Log summary of created methods
        logger.info("="*100)
        logger.info("FLOW CREATION SUMMARY")
        logger.info("="*100)

        # Count and list all flow methods
        start_methods = [m for m in dir(DynamicFlow) if m.startswith('start_flow_')]
        listener_methods = [m for m in dir(DynamicFlow) if m.startswith('listen_task_')]
        router_methods = [m for m in dir(DynamicFlow) if m.startswith('router_') or m.startswith('route_')]

        logger.info(f"✅ Created {len(start_methods)} start methods: {start_methods}")
        logger.info(f"✅ Created {len(listener_methods)} listener methods: {listener_methods}")
        logger.info(f"✅ Created {len(router_methods)} router methods: {router_methods}")
        logger.info(f"Total flow methods: {len(start_methods) + len(listener_methods) + len(router_methods)}")
        logger.info("="*100)

        logger.info("Flow configured successfully with proper flow structure")

        return flow_instance