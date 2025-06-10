"""
Flow builder module for CrewAI flow execution.

This module handles the building of CrewAI flows from configuration.
"""
import logging
from typing import Dict, List, Optional, Any, Union
from crewai.flow.flow import Flow as CrewAIFlow
from crewai.flow.flow import start, listen, and_, or_
from crewai import Crew, Agent, Task, Process

from src.core.logger import LoggerManager
from src.engines.crewai.flow.modules.agent_config import AgentConfig
from src.engines.crewai.flow.modules.task_config import TaskConfig

# Initialize logger
logger = LoggerManager.get_instance().crew

class FlowBuilder:
    """
    Helper class for building CrewAI flows.
    """
    
    @staticmethod
    async def build_flow(flow_data, repositories=None, callbacks=None):
        """
        Build a CrewAI flow from flow data.
        
        Args:
            flow_data: Flow data from the database
            repositories: Dictionary of repositories (optional)
            callbacks: Dictionary of callbacks (optional)
            
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
            
            # Parse all tasks, agents, and tools
            all_agents = {}
            all_tasks = {}
            
            # Process all starting points first to collect tasks and agents
            await FlowBuilder._process_starting_points(
                starting_points, all_agents, all_tasks, flow_data, repositories, callbacks
            )
            
            # Process all listener tasks
            await FlowBuilder._process_listeners(
                listeners, all_agents, all_tasks, flow_data, repositories, callbacks
            )
            
            # Now build the flow class with proper structure
            # Create a dynamic flow class
            dynamic_flow = await FlowBuilder._create_dynamic_flow(
                starting_points, listeners, all_agents, all_tasks
            )
            
            # Log the methods we've added to help diagnose issues
            flow_methods = [method for method in dir(dynamic_flow) if callable(getattr(dynamic_flow, method)) and not method.startswith('_')]
            logger.info(f"Flow has methods: {flow_methods}")
            
            return dynamic_flow
            
        except Exception as e:
            logger.error(f"Error building flow: {e}", exc_info=True)
            raise ValueError(f"Failed to build flow: {str(e)}")
    
    @staticmethod
    async def _process_starting_points(starting_points, all_agents, all_tasks, flow_data, repositories, callbacks):
        """
        Process starting points and configure their tasks and agents.
        
        Args:
            starting_points: List of starting point configurations
            all_agents: Dictionary to store configured agents
            all_tasks: Dictionary to store configured tasks
            flow_data: Flow data for context
            repositories: Dictionary of repositories
            callbacks: Dictionary of callbacks
        """
        logger.info(f"Processing {len(starting_points)} starting points")
        
        for start_point in starting_points:
            crew_name = start_point.get('crewName')
            crew_id = start_point.get('crewId')
            task_name = start_point.get('taskName')
            task_id = start_point.get('taskId')
            
            # Ensure all IDs are strings
            if crew_id:
                crew_id = str(crew_id)
            if task_id:
                task_id = str(task_id)
            
            logger.info(f"Processing start point: Crew={crew_name}({crew_id}), Task={task_name}({task_id})")
            
            # Load task data
            task_data = None
            if task_id:
                task_repo = None if repositories is None else repositories.get('task')
                if task_repo:
                    task_data = task_repo.find_by_id(task_id)
                else:
                    # Get task repository from factory
                    from src.repositories.task_repository import get_sync_task_repository
                    task_repo = get_sync_task_repository()
                    task_data = task_repo.find_by_id(task_id)
                
                if task_data:
                    # Configure agent for this task
                    agent_id = task_data.agent_id
                    if agent_id is None:
                        # Use crew_id from starting point as agent_id
                        agent_id = crew_id
                        logger.info(f"Using crew_id {crew_id} as agent_id for task {task_id}")
                    
                    # Configure agent if not already done
                    if agent_id not in all_agents:
                        agent_repo = None if repositories is None else repositories.get('agent')
                        agent_data = None
                        
                        if agent_repo:
                            agent_data = agent_repo.find_by_id(agent_id)
                        else:
                            # Get agent repository from factory
                            from src.repositories.agent_repository import get_sync_agent_repository
                            agent_repo = get_sync_agent_repository()
                            agent_data = agent_repo.find_by_id(agent_id)
                        
                        if agent_data:
                            agent = await AgentConfig.configure_agent_and_tools(agent_data, flow_data, repositories)
                            all_agents[agent_id] = agent
                            logger.info(f"Added agent {agent_data.name} to flow agents")
                        else:
                            logger.warning(f"Agent with ID {agent_id} not found")
                    
                    # Configure task with agent if available
                    agent = all_agents.get(agent_id)
                    task_output_callback = None
                    if callbacks and callbacks.get('streaming'):
                        task_output_callback = callbacks['streaming'].execute
                    
                    task = await TaskConfig.configure_task(task_data, agent, task_output_callback, flow_data, repositories)
                    if task:
                        all_tasks[task_id] = task
                        logger.info(f"Added task {task_data.name} to flow tasks")
    
    @staticmethod
    async def _process_listeners(listeners, all_agents, all_tasks, flow_data, repositories, callbacks):
        """
        Process listeners and configure their tasks and agents.
        
        Args:
            listeners: List of listener configurations
            all_agents: Dictionary to store configured agents
            all_tasks: Dictionary to store configured tasks
            flow_data: Flow data for context
            repositories: Dictionary of repositories
            callbacks: Dictionary of callbacks
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
                    agent_data = agent_repo.find_by_id(crew_id)
                else:
                    # Get agent repository from factory
                    from src.repositories.agent_repository import get_sync_agent_repository
                    agent_repo = get_sync_agent_repository()
                    agent_data = agent_repo.find_by_id(crew_id)
                
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
                    
                    if task_repo:
                        task_data = task_repo.find_by_id(task_id)
                    else:
                        # Get task repository from factory
                        from src.repositories.task_repository import get_sync_task_repository
                        task_repo = get_sync_task_repository()
                        task_data = task_repo.find_by_id(task_id)
                    
                    if task_data:
                        # Use agent_id from task config or fall back to crew_id
                        agent_id = task_data.agent_id or crew_id
                        agent = all_agents.get(agent_id)
                        
                        if agent:
                            task_output_callback = None
                            if callbacks and callbacks.get('streaming'):
                                task_output_callback = callbacks['streaming'].execute
                            
                            task = await TaskConfig.configure_task(task_data, agent, task_output_callback, flow_data, repositories)
                            if task:
                                all_tasks[task_id] = task
                                logger.info(f"Added listener task {task_data.name} to flow tasks")
    
    @staticmethod
    async def _create_dynamic_flow(starting_points, listeners, all_agents, all_tasks):
        """
        Create a dynamic flow class with all start and listener methods.
        
        Args:
            starting_points: List of starting point configurations
            listeners: List of listener configurations
            all_agents: Dictionary of configured agents
            all_tasks: Dictionary of configured tasks
            
        Returns:
            CrewAIFlow: An instance of the dynamically created flow class
        """
        # Create a dynamic flow class
        class DynamicFlow(CrewAIFlow):
            pass
        
        # Add start methods for each starting point
        for i, start_point in enumerate(starting_points):
            task_id = start_point.get('taskId')
            task = all_tasks.get(task_id)
            
            if task:
                # Define a proper method name
                method_name = f"start_flow_{i}"
                
                # Define the method directly on the class using a function factory
                def method_factory(task_obj):
                    # Create the actual method
                    @start()
                    def start_method(self):
                        logger.info(f"Starting flow with task: {task_obj.description}")
                        # Get the agent for this task
                        agent = task_obj.agent
                        
                        # We no longer add default tools - respect the existing configuration
                        # Only log if agent has no tools
                        if not hasattr(agent, 'tools') or not agent.tools:
                            logger.info(f"Agent {agent.role} has no tools assigned but will continue with execution")
                        
                        # Create a single-task crew
                        crew = Crew(
                            agents=[agent],
                            tasks=[task_obj],
                            verbose=True,
                            process=Process.sequential
                        )
                        return crew.kickoff()
                    
                    # Need to use the name that matches method_name for proper binding
                    start_method.__name__ = method_name
                    return start_method
                
                # Create and bind the method to the class
                bound_method = method_factory(task)
                setattr(DynamicFlow, method_name, bound_method)
                logger.info(f"Added start method {method_name} for task {task_id}")
        
        # Add listener methods for each listener
        for i, listener_config in enumerate(listeners):
            listen_to_task_ids = listener_config.get('listenToTaskIds', [])
            condition_type = listener_config.get('conditionType', 'NONE')
            
            # Skip if no tasks to listen to
            if not listen_to_task_ids:
                continue
            
            # Get the listener tasks
            listener_tasks = []
            for task_config in listener_config.get('tasks', []):
                task_id = task_config.get('id')
                if task_id in all_tasks:
                    listener_tasks.append(all_tasks[task_id])
            
            # Skip if no listener tasks
            if not listener_tasks:
                continue
            
            # Create the proper decorator based on condition_type
            for j, listen_task_id in enumerate(listen_to_task_ids):
                # Skip if the task to listen to is not in our collection
                if listen_task_id not in all_tasks:
                    continue
                
                method_name = f"listen_task_{i}_{j}"
                
                # Define the listener method using a factory function to properly capture variables
                def listener_factory(listener_tasks_obj, listen_task_array, condition_type_str):
                    # Apply the appropriate decorator based on condition type
                    if condition_type_str == "AND":
                        decorator = listen(and_(*listen_task_array))
                    elif condition_type_str == "OR":
                        decorator = listen(or_(*listen_task_array))
                    else:
                        # Must be a specific task
                        listen_task = all_tasks[listen_task_id]
                        decorator = listen(listen_task)
                    
                    @decorator
                    def create_method(self, *results):
                        condition_desc = f"{condition_type_str} conditional " if condition_type_str in ["AND", "OR"] else ""
                        logger.info(f"Executing {condition_desc}listener with {len(listener_tasks_obj)} tasks")
                        
                        # Create a crew with all listener tasks
                        agents = list(set(task.agent for task in listener_tasks_obj))
                        
                        # We no longer add default tools - respect the existing configuration
                        # Only log if agents have no tools
                        for agent in agents:
                            if not hasattr(agent, 'tools') or not agent.tools:
                                logger.info(f"Agent {agent.role} has no tools assigned but will continue with execution")
                        
                        crew = Crew(
                            agents=agents,
                            tasks=listener_tasks_obj,
                            verbose=True,
                            process=Process.sequential
                        )
                        return crew.kickoff()
                    
                    # Set the method name to match the assigned name
                    create_method.__name__ = method_name
                    return create_method
                
                # Handle different condition types
                if condition_type in ["AND", "OR"]:
                    # For AND/OR conditions, we only need one listener for all tasks
                    if j == 0:  # Only create once
                        listen_tasks = [all_tasks[tid] for tid in listen_to_task_ids if tid in all_tasks]
                        bound_method = listener_factory(listener_tasks, listen_tasks, condition_type)
                        setattr(DynamicFlow, method_name, bound_method)
                        logger.info(f"Added {condition_type} listener {method_name} for {len(listen_tasks)} tasks")
                    break  # Skip other iterations
                else:
                    # For NONE/other conditions, create individual listeners
                    bound_method = listener_factory(listener_tasks, [all_tasks[listen_task_id]], "NONE")
                    setattr(DynamicFlow, method_name, bound_method)
                    logger.info(f"Added simple listener {method_name} for task {listen_task_id}")
        
        # Create an instance of our properly defined flow
        flow_instance = DynamicFlow()
        logger.info("Flow configured successfully with proper flow structure")
        
        return flow_instance 