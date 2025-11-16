"""
Flow builder module for CrewAI flow execution.

This module handles the building of CrewAI flows from configuration.
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

            # Parse all tasks, agents, and tools
            all_agents = {}
            all_tasks = {}

            # Process all starting points first to collect tasks and agents
            await FlowBuilder._process_starting_points(
                starting_points, all_agents, all_tasks, flow_data, repositories, callbacks, group_context
            )

            # Process all listener tasks
            await FlowBuilder._process_listeners(
                listeners, all_agents, all_tasks, flow_data, repositories, callbacks, group_context
            )

            # Process router tasks
            await FlowBuilder._process_routers(
                routers, all_agents, all_tasks, flow_data, repositories, callbacks, group_context
            )

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
                starting_points, listeners, routers, all_agents, all_tasks, flow_config
            )

            logger.info("="*100)
            logger.info(f"✅ Dynamic flow created successfully, type: {type(dynamic_flow)}")
            logger.info("="*100)

            # Log the methods we've added to help diagnose issues
            flow_methods = [method for method in dir(dynamic_flow) if callable(getattr(dynamic_flow, method)) and not method.startswith('_')]
            logger.info(f"Flow has {len(flow_methods)} public methods: {flow_methods}")

            # Specifically check for start methods
            start_methods = [m for m in flow_methods if m.startswith('start_flow_')]
            if start_methods:
                logger.info(f"✅ Found {len(start_methods)} start methods: {start_methods}")
            else:
                logger.error("❌ NO START METHODS FOUND in created flow!")
                logger.error("This is a critical error - flow cannot execute without start methods")
            
            return dynamic_flow
            
        except Exception as e:
            logger.error(f"Error building flow: {e}", exc_info=True)
            raise ValueError(f"Failed to build flow: {str(e)}")
    
    @staticmethod
    async def _process_starting_points(starting_points, all_agents, all_tasks, flow_data, repositories, callbacks, group_context=None):
        """
        Process starting points and configure their tasks and agents.

        Args:
            starting_points: List of starting point configurations
            all_agents: Dictionary to store configured agents
            all_tasks: Dictionary to store configured tasks
            flow_data: Flow data for context
            repositories: Dictionary of repositories
            callbacks: Dictionary of callbacks
            group_context: Group context for multi-tenant isolation
        """
        logger.info(f"Processing {len(starting_points)} starting points")

        for start_point in starting_points:
            # Starting points now have nodeType and nodeData structure
            node_type = start_point.get('nodeType')
            node_data = start_point.get('nodeData', {})

            logger.info(f"Processing start point with nodeType: {node_type}")
            logger.info(f"Node data keys: {list(node_data.keys())}")

            # Extract crew/task information from nodeData based on nodeType
            crew_name = node_data.get('crewName')
            crew_id = node_data.get('crewId')
            task_name = node_data.get('label') if node_type == 'taskNode' else None
            task_id = node_data.get('taskId') if node_type == 'taskNode' else None

            # For agentNode, we might have a different structure
            agent_id = node_data.get('agentId') if node_type == 'agentNode' else None

            # Ensure all IDs are strings
            if crew_id:
                crew_id = str(crew_id)
            if task_id:
                task_id = str(task_id)
            if agent_id:
                agent_id = str(agent_id)

            logger.info(f"Extracted: nodeType={node_type}, crew={crew_name}({crew_id}), task={task_name}({task_id}), agent={agent_id}")

            # Handle different node types
            if node_type == 'crewNode':
                # CrewNode contains allTasks array with task objects
                all_crew_tasks = node_data.get('allTasks', [])
                logger.info(f"CrewNode has {len(all_crew_tasks)} tasks")

                for crew_task in all_crew_tasks:
                    task_id = crew_task.get('id')
                    if task_id:
                        task_id = str(task_id)
                        logger.info(f"Processing task {task_id} from crew {crew_name}")

                        # Load and configure this task
                        task_repo = None if repositories is None else repositories.get('task')
                        if task_repo:
                            task_data = await task_repo.get(task_id)
                            if task_data:
                                # Configure agent for this task
                                task_agent_id = task_data.agent_id
                                if task_agent_id is None:
                                    # Use crew_id from starting point as agent_id
                                    task_agent_id = crew_id
                                    logger.info(f"Using crew_id {crew_id} as agent_id for task {task_id}")

                                # Configure agent if not already done
                                if task_agent_id and task_agent_id not in all_agents:
                                    agent_repo = None if repositories is None else repositories.get('agent')
                                    if agent_repo:
                                        agent_data = await agent_repo.get(task_agent_id)
                                        if agent_data:
                                            agent = await AgentConfig.configure_agent_and_tools(agent_data, flow_data, repositories, group_context)
                                            all_agents[task_agent_id] = agent
                                            logger.info(f"Added agent {agent_data.name} to flow agents")

                                # Configure task with agent if available
                                agent = all_agents.get(task_agent_id)
                                task_output_callback = None
                                if callbacks and callbacks.get('streaming'):
                                    task_output_callback = callbacks['streaming'].execute

                                task = await TaskConfig.configure_task(task_data, agent, task_output_callback, flow_data, repositories)
                                if task:
                                    all_tasks[task_id] = task
                                    logger.info(f"Added task {task_data.name} to flow tasks")

            elif node_type == 'agentNode':
                # AgentNode contains agent information directly
                if agent_id and agent_id not in all_agents:
                    logger.info(f"Processing agentNode with agent_id {agent_id}")
                    agent_repo = None if repositories is None else repositories.get('agent')
                    if agent_repo:
                        agent_data = await agent_repo.get(agent_id)
                        if agent_data:
                            agent = await AgentConfig.configure_agent_and_tools(agent_data, flow_data, repositories, group_context)
                            all_agents[agent_id] = agent
                            logger.info(f"Added agent {agent_data.name} from agentNode to flow agents")
                        else:
                            logger.warning(f"Agent with ID {agent_id} not found for agentNode")

            elif node_type == 'taskNode':
                # TaskNode - process the task directly
                # Load task data
                task_data = None
                if task_id:
                    task_repo = None if repositories is None else repositories.get('task')
                    if task_repo:
                        task_data = await task_repo.get(task_id)
                    else:
                        # Log warning if repositories not provided
                        logger.warning(f"No task repository provided for task_id {task_id}")
                        task_data = None

                    if task_data:
                        # Configure agent for this task
                        task_agent_id = task_data.agent_id
                        if task_agent_id is None:
                            # Use crew_id from starting point as agent_id
                            task_agent_id = crew_id
                            logger.info(f"Using crew_id {crew_id} as agent_id for task {task_id}")

                        # Configure agent if not already done
                        if task_agent_id and task_agent_id not in all_agents:
                            agent_repo = None if repositories is None else repositories.get('agent')
                            agent_data = None

                            if agent_repo:
                                agent_data = await agent_repo.get(task_agent_id)
                            else:
                                # Log warning if repositories not provided
                                logger.warning(f"No agent repository provided for agent_id {task_agent_id}")
                                agent_data = None

                            if agent_data:
                                agent = await AgentConfig.configure_agent_and_tools(agent_data, flow_data, repositories, group_context)
                                all_agents[task_agent_id] = agent
                                logger.info(f"Added agent {agent_data.name} to flow agents")
                            else:
                                logger.warning(f"Agent with ID {task_agent_id} not found")

                        # Configure task with agent if available
                        agent = all_agents.get(task_agent_id)
                        task_output_callback = None
                        if callbacks and callbacks.get('streaming'):
                            task_output_callback = callbacks['streaming'].execute

                        task = await TaskConfig.configure_task(task_data, agent, task_output_callback, flow_data, repositories)
                        if task:
                            all_tasks[task_id] = task
                            logger.info(f"Added task {task_data.name} to flow tasks")
    
    @staticmethod
    async def _process_listeners(listeners, all_agents, all_tasks, flow_data, repositories, callbacks, group_context=None):
        """
        Process listeners and configure their tasks and agents.

        Args:
            listeners: List of listener configurations
            all_agents: Dictionary to store configured agents
            all_tasks: Dictionary to store configured tasks
            flow_data: Flow data for context
            repositories: Dictionary of repositories
            callbacks: Dictionary of callbacks
            group_context: Group context for multi-tenant isolation
        """
        logger.info(f"Processing {len(listeners)} listeners")
        
        for listener_config in listeners:
            listener_name = listener_config.get('name')
            crew_id = listener_config.get('crewId')
            listen_to_task_ids = listener_config.get('listenToTaskIds', [])
            condition_type = listener_config.get('conditionType', 'NONE')
            
            logger.info(f"Processing listener: {listener_name}, ConditionType: {condition_type}")
            
            # Prepare agent for listener tasks if needed
            if crew_id and crew_id not in all_agents:
                agent_repo = None if repositories is None else repositories.get('agent')
                agent_data = None
                
                if agent_repo:
                    agent_data = await agent_repo.get(crew_id)
                else:
                    # Log warning if repositories not provided
                    logger.warning(f"No agent repository provided for crew_id {crew_id}")
                    agent_data = None
                
                if agent_data:
                    agent = await AgentConfig.configure_agent_and_tools(agent_data, flow_data, repositories)
                    all_agents[crew_id] = agent
                    logger.info(f"Added agent {agent_data.name} to flow agents for listener")
            
            # Process listener tasks
            for task_config in listener_config.get('tasks', []):
                task_id = task_config.get('id')
                if task_id not in all_tasks:
                    task_repo = None if repositories is None else repositories.get('task')
                    task_data = None

                    # CRITICAL: Try to load from database first
                    if task_repo:
                        task_data = await task_repo.get(task_id)

                    # CRITICAL: If not found in DB, use the task_config data directly
                    # This handles "inline" tasks that are embedded in the flow configuration
                    if not task_data:
                        logger.info(f"Task {task_id} not found in database, using inline task config from flow")
                        # Convert task_config dict to a TaskModel-like object that configure_task expects
                        from src.models.task import Task as TaskModel
                        from datetime import datetime

                        # Create a minimal TaskModel instance from task_config
                        task_data = TaskModel(
                            id=task_config.get('id'),
                            name=task_config.get('name', ''),
                            description=task_config.get('description', ''),
                            expected_output=task_config.get('expected_output', ''),
                            agent_id=crew_id,  # Use crew_id as agent_id for inline tasks
                            tools=task_config.get('tools', []),
                            context=task_config.get('context', []),
                            async_execution=task_config.get('async_execution', False),
                            output_json=task_config.get('output_json'),
                            output_pydantic=task_config.get('output_pydantic'),
                            output_file=task_config.get('output_file'),
                            callback=task_config.get('callback'),
                            human_input=task_config.get('human_input', False),
                            group_id='',  # Will be set from group_context if needed
                            created_at=datetime.utcnow(),
                            updated_at=datetime.utcnow()
                        )

                    if task_data:
                        # Use agent_id from task config or fall back to crew_id
                        agent_id = task_data.agent_id or crew_id

                        # CRITICAL: Load the agent if not already loaded
                        if agent_id and agent_id not in all_agents:
                            agent_repo = None if repositories is None else repositories.get('agent')
                            agent_data = None

                            if agent_repo:
                                agent_data = await agent_repo.get(agent_id)
                                if agent_data:
                                    agent = await AgentConfig.configure_agent_and_tools(agent_data, flow_data, repositories, group_context)
                                    all_agents[agent_id] = agent
                                    logger.info(f"Added agent {agent_data.name} (id={agent_id}) to flow agents for listener task")
                                else:
                                    logger.warning(f"Agent with ID {agent_id} not found in database for task {task_id}")
                            else:
                                logger.warning(f"No agent repository provided to load agent {agent_id} for task {task_id}")

                        agent = all_agents.get(agent_id)

                        if agent:
                            task_output_callback = None
                            if callbacks and callbacks.get('streaming'):
                                task_output_callback = callbacks['streaming'].execute

                            task = await TaskConfig.configure_task(task_data, agent, task_output_callback, flow_data, repositories)
                            if task:
                                all_tasks[task_id] = task
                                logger.info(f"Added listener task {task_data.name} to flow tasks")
                        else:
                            logger.warning(f"No agent found for task {task_id}, agent_id={agent_id}")

    @staticmethod
    async def _process_routers(routers, all_agents, all_tasks, flow_data, repositories, callbacks, group_context=None):
        """
        Process routers for conditional flow routing.

        Routers allow dynamic routing based on conditions. Each router listens to a
        previous method and returns a route name (string) that determines which
        @listen("route_name") method gets called next.

        Args:
            routers: List of router configurations
            all_agents: Dictionary to store configured agents
            group_context: Group context for multi-tenant isolation
            all_tasks: Dictionary to store configured tasks
            flow_data: Flow data for context
            repositories: Dictionary of repositories
            callbacks: Dictionary of callbacks
        """
        logger.info(f"Processing {len(routers)} routers")

        for router_config in routers:
            router_name = router_config.get('name')
            listen_to = router_config.get('listenTo')  # Method or task to listen to
            routes = router_config.get('routes', {})  # Dict of route_name -> task_ids
            condition_field = router_config.get('conditionField')  # Field to check for routing

            logger.info(f"Processing router: {router_name}, Routes: {list(routes.keys())}")

            # Process tasks for each route
            for route_name, route_tasks in routes.items():
                for task_config in route_tasks:
                    task_id = task_config.get('id')
                    crew_id = task_config.get('crewId')

                    # Configure agent if needed
                    if crew_id and crew_id not in all_agents:
                        agent_repo = None if repositories is None else repositories.get('agent')
                        agent_data = None

                        if agent_repo:
                            agent_data = await agent_repo.get(crew_id)

                        if agent_data:
                            agent = await AgentConfig.configure_agent_and_tools(agent_data, flow_data, repositories)
                            all_agents[crew_id] = agent
                            logger.info(f"Added agent {agent_data.name} for router route {route_name}")

                    # Configure task if not already configured
                    if task_id and task_id not in all_tasks:
                        task_repo = None if repositories is None else repositories.get('task')
                        task_data = None

                        if task_repo:
                            task_data = await task_repo.get(task_id)

                        if task_data:
                            agent_id = task_data.agent_id or crew_id
                            agent = all_agents.get(agent_id)

                            if agent:
                                task_output_callback = None
                                if callbacks and callbacks.get('streaming'):
                                    task_output_callback = callbacks['streaming'].execute

                                task = await TaskConfig.configure_task(task_data, agent, task_output_callback, flow_data, repositories)
                                if task:
                                    all_tasks[task_id] = task
                                    logger.info(f"Added router task {task_data.name} for route {route_name}")

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
    async def _create_dynamic_flow(starting_points, listeners, routers, all_agents, all_tasks, flow_config=None):
        """
        Create a dynamic flow class with all start, listener, and router methods.

        Args:
            starting_points: List of starting point configurations
            listeners: List of listener configurations
            routers: List of router configurations
            all_agents: Dictionary of configured agents
            all_tasks: Dictionary of configured tasks
            flow_config: Flow configuration including state and persistence settings

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
        
        # Add start methods for each starting point
        logger.info("="*100)
        logger.info(f"ADDING START METHODS - Processing {len(starting_points)} starting points")
        logger.info("="*100)

        # Extract task IDs from starting points based on their structure
        starting_point_tasks = []
        for i, start_point in enumerate(starting_points):
            node_type = start_point.get('nodeType')

            if node_type == 'crewNode':
                # For crew nodes, extract tasks from allTasks
                node_data = start_point.get('nodeData', {})
                all_tasks_in_node = node_data.get('allTasks', [])
                for task in all_tasks_in_node:
                    task_id = task.get('id')
                    if task_id and task_id in all_tasks:
                        starting_point_tasks.append(task_id)
                        logger.info(f"  ✅ Added task {task_id} from crewNode")
            elif node_type == 'agentNode':
                # Agent nodes don't have tasks, skip
                logger.info(f"  ⏭️  Skipping agentNode")
                continue
            else:
                # Simple format - try to get taskId directly
                task_id = start_point.get('taskId') or start_point.get('task_id')
                if task_id and task_id in all_tasks:
                    starting_point_tasks.append(task_id)
                    logger.info(f"  ✅ Added task {task_id} from simple format")

        logger.info(f"Total starting point tasks to create methods for: {len(starting_point_tasks)}")

        # Create start methods for each task
        for i, task_id in enumerate(starting_point_tasks):
            task = all_tasks.get(task_id)

            logger.info(f"Creating start method {i} for task: {task_id}")

            if task:
                # Define a proper method name
                method_name = f"start_flow_{i}"
                logger.info(f"  ✅ Task found, creating method: {method_name}")
                
                # Define the method directly on the class using a function factory
                def method_factory(task_obj):
                    # Create the actual method as async
                    @start()
                    async def start_method(self):
                        logger.info("="*80)
                        logger.info(f"START METHOD CALLED - Task: {task_obj.description}")
                        logger.info("="*80)

                        # Get the agent for this task
                        agent = task_obj.agent
                        logger.info(f"Agent role: {agent.role}")

                        # We no longer add default tools - respect the existing configuration
                        # Only log if agent has no tools
                        if not hasattr(agent, 'tools') or not agent.tools:
                            logger.info(f"Agent {agent.role} has no tools assigned but will continue with execution")

                        # Create a single-task crew
                        logger.info("Creating Crew instance for start method")
                        logger.info(f"Agent details - role: {agent.role}, has_llm: {hasattr(agent, 'llm') and agent.llm is not None}")
                        logger.info(f"Task details - description length: {len(task_obj.description)}, has_agent: {task_obj.agent is not None}")

                        crew = Crew(
                            agents=[agent],
                            tasks=[task_obj],
                            verbose=True,
                            process=Process.sequential
                        )
                        logger.info("Crew instance created successfully, about to call kickoff_async")

                        try:
                            import asyncio
                            logger.info("Calling crew.kickoff_async() with 10 minute timeout...")
                            result = await asyncio.wait_for(crew.kickoff_async(), timeout=600.0)
                            logger.info(f"✅ kickoff_async completed successfully, result type: {type(result)}")
                            return result
                        except asyncio.TimeoutError:
                            logger.error("❌ Crew execution timed out after 10 minutes")
                            raise TimeoutError("Crew execution timed out after 10 minutes")
                        except Exception as e:
                            logger.error(f"❌ Error during crew kickoff: {e}", exc_info=True)
                            raise
                    
                    # Need to use the name that matches method_name for proper binding
                    start_method.__name__ = method_name
                    return start_method
                
                # Add method to class dictionary (will be processed by FlowMeta metaclass)
                class_methods[method_name] = method_factory(task)
                logger.info(f"  ✅ Created start method '{method_name}' for class dictionary")
            else:
                logger.error(f"  ❌ Task {task_id} not found in all_tasks, skipping start method creation")
        
        # Add listener methods for each listener
        logger.info("="*100)
        logger.info(f"ADDING LISTENER METHODS - Processing {len(listeners)} listeners")
        logger.info("="*100)

        for i, listener_config in enumerate(listeners):
            listen_to_task_ids = listener_config.get('listenToTaskIds', [])
            condition_type = listener_config.get('conditionType', 'NONE')

            logger.info(f"Listener {i}: conditionType={condition_type}, listenToTaskIds={listen_to_task_ids}")

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
            for j, listen_task_id in enumerate(listen_to_task_ids):
                # Skip if the task to listen to is not in our collection
                if listen_task_id not in all_tasks:
                    continue
                
                method_name = f"listen_task_{i}_{j}"
                
                # Define the listener method using a factory function to properly capture variables
                def listener_factory(listener_tasks_obj, listen_task_array, condition_type_str, method_condition):
                    # Apply the listen decorator with the appropriate condition
                    decorator = listen(method_condition)

                    @decorator
                    async def create_method(self, *results):
                        logger.info("="*80)
                        condition_desc = f"{condition_type_str} conditional " if condition_type_str in ["AND", "OR"] else ""
                        logger.info(f"LISTENER METHOD CALLED - Executing {condition_desc}listener with {len(listener_tasks_obj)} tasks")
                        logger.info("="*80)

                        # Create a crew with all listener tasks
                        agents = list(set(task.agent for task in listener_tasks_obj))
                        logger.info(f"Number of agents in listener: {len(agents)}")

                        # We no longer add default tools - respect the existing configuration
                        # Only log if agents have no tools
                        for agent in agents:
                            if not hasattr(agent, 'tools') or not agent.tools:
                                logger.info(f"Agent {agent.role} has no tools assigned but will continue with execution")

                        logger.info("Creating Crew instance for listener method")
                        logger.info(f"Listener has {len(agents)} agents and {len(listener_tasks_obj)} tasks")

                        crew = Crew(
                            agents=agents,
                            tasks=listener_tasks_obj,
                            verbose=True,
                            process=Process.sequential
                        )
                        logger.info("Crew instance created for listener, about to call kickoff_async")

                        try:
                            import asyncio
                            logger.info("Calling listener crew.kickoff_async() with 10 minute timeout...")
                            result = await asyncio.wait_for(crew.kickoff_async(), timeout=600.0)
                            logger.info(f"✅ Listener kickoff_async completed, result type: {type(result)}")
                            return result
                        except asyncio.TimeoutError:
                            logger.error("❌ Listener crew execution timed out after 10 minutes")
                            raise TimeoutError("Listener crew execution timed out")
                        except Exception as e:
                            logger.error(f"❌ Error during listener crew kickoff: {e}", exc_info=True)
                            raise
                    
                    # Set the method name to match the assigned name
                    create_method.__name__ = method_name
                    return create_method
                
                # Handle different condition types
                if condition_type in ["AND", "OR"]:
                    # For AND/OR conditions, we only need one listener for all tasks
                    if j == 0:  # Only create once
                        listen_tasks = [all_tasks[tid] for tid in listen_to_task_ids if tid in all_tasks]

                        # Create method condition for AND/OR
                        # Need to find which start methods correspond to the tasks we're listening to
                        method_names = []
                        for idx, task_id in enumerate(starting_point_tasks):
                            if task_id in listen_to_task_ids:
                                method_names.append(f"start_flow_{idx}")

                        logger.info(f"Creating {condition_type} listener for tasks {listen_to_task_ids}")
                        logger.info(f"Found {len(method_names)} matching start methods: {method_names}")

                        if condition_type == "AND":
                            method_condition = and_(*method_names) if len(method_names) > 1 else method_names[0] if method_names else "start_flow_0"
                        else:  # OR
                            method_condition = or_(*method_names) if len(method_names) > 1 else method_names[0] if method_names else "start_flow_0"

                        bound_method = listener_factory(listener_tasks, listen_tasks, condition_type, method_condition)
                        class_methods[method_name] = bound_method
                        logger.info(f"✅ Created {condition_type} listener {method_name} for class dictionary, listening to: {method_condition}")
                    break  # Skip other iterations
                else:
                    # For NONE/other conditions, create individual listeners
                    # Find the corresponding start method name by matching task ID
                    method_condition = "start_flow_0"  # Default fallback

                    # Find which start method corresponds to this task
                    for idx, task_id in enumerate(starting_point_tasks):
                        if task_id == listen_task_id:
                            method_condition = f"start_flow_{idx}"
                            logger.info(f"Found start method {method_condition} for task {listen_task_id}")
                            break

                    bound_method = listener_factory(listener_tasks, [all_tasks[listen_task_id]], "NONE", method_condition)
                    class_methods[method_name] = bound_method
                    logger.info(f"✅ Created simple listener {method_name} for class dictionary, listening to: {method_condition}")

        # Add router methods for conditional routing
        for i, router_config in enumerate(routers):
            router_name = router_config.get('name', f'router_{i}')
            listen_to = router_config.get('listenTo')  # Method name to listen to
            routes = router_config.get('routes', {})  # Dict of route_name -> task configs
            condition_field = router_config.get('conditionField', 'success')

            # Find the method to listen to
            listen_to_method = listen_to or "start_flow_0"

            # Create router method
            def router_factory(router_routes, router_condition_field, router_method_name):
                @router(listen_to_method)
                def route_method(self, *args, **kwargs):
                    logger.info(f"Router {router_method_name} evaluating condition")

                    # Simple condition evaluation
                    # Check if we have state or result to route on
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
            bound_router = router_factory(routes, condition_field, router_method_name)
            class_methods[router_method_name] = bound_router
            logger.info(f"Created router method {router_method_name} for class dictionary with routes: {list(routes.keys())}")

            # Add listener methods for each route
            for route_name, route_tasks in routes.items():
                route_task_objs = [all_tasks[t.get('id')] for t in route_tasks if t.get('id') in all_tasks]

                if route_task_objs:
                    route_listener_name = f"route_{router_name}_{route_name}_{i}"

                    def route_listener_factory(route_task_list, route_listener_method_name):
                        @listen(route_name)
                        async def route_listener_method(self, *args, **kwargs):
                            logger.info("="*80)
                            logger.info(f"ROUTE LISTENER METHOD CALLED - {route_listener_method_name}")
                            logger.info("="*80)

                            # Get agents for these tasks
                            agents = list(set(task.agent for task in route_task_list))
                            logger.info(f"Number of agents in route listener: {len(agents)}")

                            # Create crew with route tasks
                            logger.info("Creating Crew instance for route listener")
                            crew = Crew(
                                agents=agents,
                                tasks=route_task_list,
                                verbose=True,
                                process=Process.sequential
                            )
                            logger.info("Crew instance created for route, about to call kickoff_async")
                            result = await crew.kickoff_async()
                            logger.info(f"Route listener kickoff_async completed, result type: {type(result)}")
                            return result

                        route_listener_method.__name__ = route_listener_method_name
                        return route_listener_method

                    bound_route_listener = route_listener_factory(route_task_objs, route_listener_name)
                    class_methods[route_listener_name] = bound_route_listener
                    logger.info(f"Created route listener {route_listener_name} for class dictionary for route '{route_name}'")

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