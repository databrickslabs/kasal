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
from crewai import Crew, Task, Process
from pydantic import BaseModel

# The @persist decorator is available since CrewAI 0.98.0
# Import: from crewai.flow.persistence import persist
# Usage: @persist() at class level or method level for state checkpointing

from src.core.logger import LoggerManager
from src.engines.crewai.flow.modules.agent_config import AgentConfig
from src.engines.crewai.flow.modules.task_config import TaskConfig
from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

# Import new modular components
from src.engines.crewai.flow.modules.flow_config import FlowConfigManager
from src.engines.crewai.flow.modules.flow_processors import FlowProcessorManager
from src.engines.crewai.flow.modules.flow_state import FlowStateManager
from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory, extract_final_answer, get_model_context_limits
from src.engines.crewai.flow.exceptions import FlowPausedForApprovalException

# Initialize logger - use flow logger for flow execution
logger = LoggerManager.get_instance().flow

class FlowBuilder:
    """
    Helper class for building CrewAI flows.
    """
    
    @staticmethod
    async def build_flow(flow_data, repositories=None, callbacks=None, group_context=None, restore_uuid=None, resume_from_crew_sequence=None, resume_from_execution_id=None):
        """
        Build a CrewAI flow from flow data.

        Args:
            flow_data: Flow data from the database
            repositories: Dictionary of repositories (optional)
            callbacks: Dictionary of callbacks (optional)
            group_context: Group context for multi-tenant isolation (optional)
            restore_uuid: UUID of a previous flow execution to resume from (optional)
            resume_from_crew_sequence: Crew sequence number to resume from (optional).
                When provided, crews with sequence <= this value will be skipped.
            resume_from_execution_id: Execution ID of checkpoint to resume from (optional).
                Used to query execution traces for previous crew outputs.

        Returns:
            CrewAIFlow: A configured CrewAI Flow instance
        """
        logger.info("Building CrewAI Flow")
        if resume_from_crew_sequence is not None:
            logger.info(f"Resume from crew sequence: {resume_from_crew_sequence} (will skip crews 1-{resume_from_crew_sequence})")
        if resume_from_execution_id is not None:
            logger.info(f"Resume from execution ID: {resume_from_execution_id} (will query traces for checkpoint data)")

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

            # Check edges for checkpoint flag and enable persistence if any edge has checkpoint=true
            # This allows checkpoint/resume functionality when users enable checkpoints on edges
            edges = flow_data.get('edges', [])
            has_checkpoint_edge = any(
                edge.get('data', {}).get('checkpoint', False) for edge in edges
            )
            if has_checkpoint_edge:
                logger.info("Found edge(s) with checkpoint=true, enabling flow persistence")
                # Ensure persistence config exists and is enabled
                if 'persistence' not in flow_config:
                    flow_config['persistence'] = {}
                flow_config['persistence']['enabled'] = True
                flow_config['persistence']['level'] = 'flow'

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

            # Load checkpoint outputs from execution traces if resuming from a specific execution
            checkpoint_outputs = {}
            if resume_from_execution_id and repositories:
                try:
                    # resume_from_execution_id is the job_id (UUID string), not the integer database ID
                    execution_history_repo = repositories.get('execution_history')
                    execution_trace_repo = repositories.get('execution_trace')

                    if execution_history_repo and execution_trace_repo:
                        logger.info(f"Looking up execution for job_id: {resume_from_execution_id}")
                        # Use get_execution_by_job_id since we're passing the job_id (UUID)
                        execution = await execution_history_repo.get_execution_by_job_id(resume_from_execution_id)

                        if execution and execution.job_id:
                            job_id = execution.job_id
                            logger.info(f"Found execution with job_id: {job_id}")

                            # Now query traces using the job_id
                            checkpoint_outputs = await execution_trace_repo.get_crew_outputs_for_resume(job_id)
                            logger.info(f"Loaded checkpoint outputs for {len(checkpoint_outputs)} crews: {list(checkpoint_outputs.keys())}")
                            for crew_name, output in checkpoint_outputs.items():
                                output_preview = str(output)[:200] + "..." if len(str(output)) > 200 else str(output)
                                logger.info(f"  📦 Checkpoint output for '{crew_name}': {output_preview}")
                        else:
                            logger.warning(f"No execution found for ID: {resume_from_execution_id}")
                    else:
                        logger.warning("Missing repositories for loading checkpoint outputs (need execution_history and execution_trace)")
                except Exception as e:
                    logger.error(f"Failed to load checkpoint outputs: {e}", exc_info=True)

            # Pass the processed listener_methods (with crew grouping) instead of raw listeners
            dynamic_flow = await FlowBuilder._create_dynamic_flow(
                starting_point_methods, listener_methods, routers, all_agents, all_tasks, flow_config, callbacks, group_context, restore_uuid, resume_from_crew_sequence, checkpoint_outputs
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
    async def _create_dynamic_flow(starting_points, listener_crews, routers, all_agents, all_tasks, flow_config=None, callbacks=None, group_context=None, restore_uuid=None, resume_from_crew_sequence=None, checkpoint_outputs=None):
        """
        Create a dynamic flow class with all start, listener, and router methods.

        IMPORTANT: Creates ONE listener per crew, not per task. Tasks within a crew execute
        sequentially using task.context (set up by FlowProcessorManager.process_listeners).

        Args:
            starting_points: List of tuples (method_name, task_ids, task_objects, crew_name) for starting point crews
            listener_crews: List of tuples from process_listeners:
                (method_name, crew_id, task_ids, task_objects, crew_name, listen_to_task_ids, condition_type)
                Each tuple represents ONE listener crew (with sequential tasks inside)
            routers: List of original router configuration dictionaries from flow_config
            all_agents: Dictionary of configured agents
            all_tasks: Dictionary of configured tasks (with task.context dependencies set)
            flow_config: Flow configuration including state and persistence settings
            callbacks: Dictionary of callback handlers for execution logging
            group_context: Group context for multi-tenant isolation
            restore_uuid: UUID of a previous flow execution to resume from (optional)
            resume_from_crew_sequence: Crew sequence number to resume from (optional).
                When provided, crews with sequence <= this value will be skipped.

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
        # IMPORTANT: Must accept **kwargs to support @persist decorator which passes 'persistence' kwarg
        def create_init_method():
            if state_enabled:
                def __init__(self, **kwargs):
                    super(type(self), self).__init__(**kwargs)
                    if state_initial_values:
                        self.state.update(state_initial_values)
            else:
                def __init__(self, **kwargs):
                    super(type(self), self).__init__(**kwargs)
            return __init__

        class_methods['__init__'] = create_init_method()

        # Log persistence and resume configuration
        if persistence_enabled:
            logger.info(f"Persistence enabled at level: {persistence_level}")
            if restore_uuid:
                logger.info(f"Flow will resume from checkpoint: {restore_uuid}")

        # Track crew sequence for checkpoint resume support
        # When resume_from_crew_sequence is provided, crews with sequence < that value will be skipped
        # (The resume_from value is the sequence of the crew TO RUN, not the last completed)
        crew_sequence_counter = 0  # Will be incremented to 1 for first crew
        if resume_from_crew_sequence is not None:
            logger.info("="*100)
            logger.info(f"CHECKPOINT RESUME MODE - Will skip crews with sequence < {resume_from_crew_sequence} (will run sequence {resume_from_crew_sequence} and beyond)")
            logger.info("="*100)

        # Add start methods for each starting point (now receiving tuples with crew info)
        logger.info("="*100)
        logger.info(f"ADDING START METHODS - Processing {len(starting_points)} starting point crews")
        logger.info("="*100)

        # Get frontend starting points config for crew name lookup
        frontend_starting_points = flow_config.get('startingPoints', []) if flow_config else []

        # starting_points now contains tuples: (method_name, task_ids_list, task_objects_list, crew_name, crew_data)
        for i, starting_point_info in enumerate(starting_points):
            # Unpack the tuple
            method_name, task_ids, crew_tasks, db_crew_name, crew_data = starting_point_info

            # Increment sequence counter for each crew (1-indexed to match frontend)
            crew_sequence_counter += 1
            current_crew_sequence = crew_sequence_counter

            # CRITICAL: Use crew name from flow config (frontend), not from database
            # The flow config has the user-facing crew name, database might have agent role
            crew_name = db_crew_name  # Default to database name
            for sp_config in frontend_starting_points:
                # Match by taskId to find the corresponding frontend config
                sp_task_id = sp_config.get('taskId')
                if sp_task_id and str(sp_task_id) in [str(tid) for tid in task_ids]:
                    # Use crewName from frontend config
                    frontend_crew_name = sp_config.get('crewName')
                    if frontend_crew_name:
                        crew_name = frontend_crew_name
                        logger.info(f"  Using crew name from flow config: '{crew_name}' (database had: '{db_crew_name}')")
                    break

            logger.info(f"Creating start method for crew: {crew_name} (sequence: {current_crew_sequence})")
            logger.info(f"  Method name: {method_name}")
            logger.info(f"  Task IDs: {task_ids}")
            logger.info(f"  Number of tasks: {len(crew_tasks)}")

            # Check if this crew should be skipped for checkpoint resume
            # Use < (not <=) because resume_from is the sequence of the crew TO RUN, not the last completed
            should_skip_crew = (
                resume_from_crew_sequence is not None and
                current_crew_sequence < resume_from_crew_sequence
            )

            if should_skip_crew:
                logger.info(f"  ⏭️  SKIPPING crew '{crew_name}' (sequence {current_crew_sequence} < resume_from {resume_from_crew_sequence})")
                # Get checkpoint output for this crew if available
                crew_checkpoint_output = None
                if checkpoint_outputs:
                    crew_checkpoint_output = checkpoint_outputs.get(crew_name)
                    if crew_checkpoint_output:
                        logger.info(f"  📦 Found checkpoint output for '{crew_name}'")
                    else:
                        logger.warning(f"  ⚠️ No checkpoint output found for '{crew_name}' in checkpoint_outputs")
                # Create a stub method that returns the checkpoint output to allow flow to continue
                class_methods[method_name] = FlowMethodFactory.create_skipped_crew_method(
                    method_name=method_name,
                    crew_name=crew_name,
                    crew_sequence=current_crew_sequence,
                    is_starting_point=True,
                    checkpoint_output=crew_checkpoint_output
                )
                logger.info(f"  ✅ Created SKIP method '{method_name}' for crew '{crew_name}' (checkpoint resume)")
            elif crew_tasks:
                logger.info(f"  Creating method: {method_name} with {len(crew_tasks)} tasks")

                # Use FlowMethodFactory to create the starting point method with ALL tasks
                class_methods[method_name] = FlowMethodFactory.create_starting_point_crew_method(
                    method_name=method_name,
                    task_list=crew_tasks,
                    crew_name=crew_name,
                    callbacks=callbacks,
                    group_context=group_context,
                    create_execution_callbacks=create_execution_callbacks,
                    crew_data=crew_data
                )
                logger.info(f"  ✅ Created start method '{method_name}' for crew '{crew_name}' with {len(crew_tasks)} sequential tasks")
            else:
                logger.error(f"  ❌ No tasks found for crew '{crew_name}', skipping start method creation")
        
        # Add listener methods for each listener CREW (not per task!)
        # listener_crews contains processed tuples with crew grouping from FlowProcessorManager
        logger.info("="*100)
        logger.info(f"ADDING LISTENER METHODS - Processing {len(listener_crews)} listener crews (crew-level orchestration)")
        logger.info("="*100)

        # Get frontend listeners config for crew name lookup
        listeners = flow_config.get('listeners', []) if flow_config else []

        # Build a mapping from method name to crew sequence for HITL gates
        # This allows us to pass the SOURCE crew sequence (not the target crew sequence)
        method_to_sequence = {}
        for starting_point_info in starting_points:
            sp_method_name, sp_task_ids, sp_task_objects, sp_crew_name, sp_crew_data = starting_point_info
            # Find the sequence number for this starting point from the counter used above
            # Starting points are numbered 1, 2, 3, ... in order
            sp_sequence = starting_points.index(starting_point_info) + 1
            method_to_sequence[sp_method_name] = sp_sequence
            logger.info(f"  Mapped method '{sp_method_name}' to sequence {sp_sequence}")

        # listener_crews is a list of tuples:
        # (method_name, crew_id, task_ids, task_objects, crew_name, listen_to_task_ids, condition_type, crew_data)
        for listener_info in listener_crews:
            # Unpack the tuple
            method_name, crew_id, task_ids, listener_tasks, db_crew_name, listen_to_task_ids, condition_type, crew_data = listener_info

            # Increment sequence counter for each crew (1-indexed to match frontend)
            crew_sequence_counter += 1
            current_crew_sequence = crew_sequence_counter

            # CRITICAL: Use crew name from flow config (frontend), not from database
            # The flow config has the user-facing crew name, database might have agent role
            crew_name = db_crew_name  # Default to database name
            for listener_config in listeners:
                # Match by crew_id to find the corresponding listener config
                if listener_config.get('crewId') == crew_id:
                    # Use name or crewName from frontend config (these are the actual crew names)
                    frontend_crew_name = listener_config.get('name') or listener_config.get('crewName')
                    if frontend_crew_name:
                        crew_name = frontend_crew_name
                        logger.info(f"  Using crew name from flow config: '{crew_name}' (database had: '{db_crew_name}')")
                    break

            logger.info(f"Creating listener for crew: {crew_name} (crew_id: {crew_id}, sequence: {current_crew_sequence})")
            logger.info(f"  Method name: {method_name}")
            logger.info(f"  Task IDs: {task_ids}")
            logger.info(f"  Number of tasks: {len(listener_tasks)}")
            logger.info(f"  Listening to task IDs: {listen_to_task_ids}")
            logger.info(f"  Condition type: {condition_type}")

            # Skip if no tasks
            if not listener_tasks:
                logger.warning(f"  ⏭️  Skipping listener crew {crew_name} - no tasks")
                continue

            # Skip if no listen_to targets
            if not listen_to_task_ids:
                logger.warning(f"  ⏭️  Skipping listener crew {crew_name} - no listen targets")
                continue

            # Find the starting point method(s) to listen to
            # Match listen_to_task_ids to starting point methods
            method_names = []
            for starting_point_info in starting_points:
                sp_method_name, sp_task_ids, sp_task_objects, sp_crew_name, sp_crew_data = starting_point_info
                # Check if any task in this starting point matches our listen targets
                for sp_task_id in sp_task_ids:
                    if str(sp_task_id) in [str(tid) for tid in listen_to_task_ids]:
                        if sp_method_name not in method_names:
                            method_names.append(sp_method_name)
                            logger.info(f"  Found starting point {sp_method_name} matches listen target {sp_task_id}")
                        break

            logger.info(f"  Matched {len(method_names)} starting point methods: {method_names}")

            # Default to first starting point if no matches found
            default_start_method = starting_points[0][0] if starting_points else "starting_point_0"

            # Check if incoming edge has HITL enabled
            # The edge data contains hitl config when HITL is configured on the connection
            edges_for_hitl = flow_config.get('edges', []) if flow_config else []
            nodes_for_hitl = flow_config.get('nodes', []) if flow_config else []

            # Build a mapping from node ID to crew ID (node.data.crewId)
            node_to_crew_map = {}
            for node in nodes_for_hitl:
                node_id = node.get('id', '')
                node_crew_id = node.get('data', {}).get('crewId', '')
                if node_id and node_crew_id:
                    node_to_crew_map[node_id] = node_crew_id

            hitl_edge = None
            for edge in edges_for_hitl:
                edge_data = edge.get('data', {})
                hitl_config = edge_data.get('hitl', {})
                # Check if this edge targets our listener crew
                # Edge target can be:
                # - A node ID like "crew-{crewId}-{timestamp}"
                # - A UUID node ID that maps to a crewId in node.data.crewId
                # - A direct crew_id or task_id
                target_node_id = edge.get('target', '')

                # Resolve the crew ID from the target node
                target_crew_id = node_to_crew_map.get(target_node_id, '')

                # Match by:
                # 1. Direct crew_id match (edge target == crew_id)
                # 2. Node ID contains crew_id (e.g., "crew-abc123-timestamp" contains "abc123")
                # 3. Resolved crew ID from node mapping matches our crew_id
                # 4. Task ID match (edge targets a specific task)
                is_match = (
                    target_node_id == crew_id or
                    crew_id in target_node_id or
                    target_crew_id == crew_id or
                    target_node_id in [str(tid) for tid in task_ids]
                )

                if is_match and hitl_config.get('enabled', False):
                    hitl_edge = edge
                    logger.info(f"  🚦 Found HITL-enabled edge to this listener: {edge.get('id', 'unknown')}")
                    logger.info(f"     Edge target: {target_node_id}, Resolved crew: {target_crew_id}, Listener crew_id: {crew_id}")
                    logger.info(f"     HITL config: {hitl_config}")
                    break

            # If HITL is enabled on the incoming edge, create an HITL gate method
            hitl_gate_method_name = None
            if hitl_edge:
                edge_id = hitl_edge.get('id', f'edge_{crew_id}')
                hitl_config = hitl_edge.get('data', {}).get('hitl', {})

                # Build gate config from edge HITL data
                gate_config = {
                    'message': hitl_config.get('message', 'Please review and approve to continue'),
                    'timeout_seconds': hitl_config.get('timeout_seconds', 86400),
                    'timeout_action': hitl_config.get('timeout_action', 'auto_reject'),
                    'require_comment': hitl_config.get('require_comment', False),
                    'allowed_approvers': hitl_config.get('allowed_approvers', [])
                }

                # The gate should listen to the source crew's method
                source_method = method_names[0] if method_names else default_start_method

                # Get the SOURCE crew's sequence (the crew that completed BEFORE the gate)
                # NOT the current (target) crew's sequence
                source_crew_sequence = method_to_sequence.get(source_method, 1)

                # Create HITL gate method name
                hitl_gate_method_name = f"hitl_gate_edge_{edge_id}"
                logger.info(f"  🚦 Creating HITL gate method: {hitl_gate_method_name}")
                logger.info(f"     Gate listens to: {source_method} (sequence: {source_crew_sequence})")
                logger.info(f"     Gate config: {gate_config}")
                logger.info(f"     ⚠️  IMPORTANT: Using SOURCE crew sequence ({source_crew_sequence}), NOT target crew sequence ({current_crew_sequence})")

                # Create the HITL gate method using the factory
                class_methods[hitl_gate_method_name] = FlowMethodFactory.create_hitl_gate_method(
                    method_name=hitl_gate_method_name,
                    gate_node_id=edge_id,
                    gate_config=gate_config,
                    previous_method_name=source_method,
                    crew_sequence=source_crew_sequence,  # FIXED: Use SOURCE crew sequence, not target
                    callbacks=callbacks,
                    group_context=group_context
                )
                logger.info(f"  ✅ Created HITL gate method '{hitl_gate_method_name}' for edge")

            # Build the method condition based on condition_type
            # If HITL gate was created, listener should listen to the gate instead
            if hitl_gate_method_name:
                # Listener now listens to the HITL gate, not the source crew
                method_condition = hitl_gate_method_name
                logger.info(f"  Using HITL gate condition: {method_condition}")
            elif condition_type == "AND" and len(method_names) > 1:
                method_condition = and_(*method_names)
                logger.info(f"  Using AND condition for {len(method_names)} methods")
            elif condition_type == "OR" and len(method_names) > 1:
                method_condition = or_(*method_names)
                logger.info(f"  Using OR condition for {len(method_names)} methods")
            else:
                # Single method or NONE condition
                method_condition = method_names[0] if method_names else default_start_method
                logger.info(f"  Using simple condition: {method_condition}")

            # Check if this crew should be skipped for checkpoint resume
            # Use < (not <=) because resume_from is the sequence of the crew TO RUN, not the last completed
            should_skip_crew = (
                resume_from_crew_sequence is not None and
                current_crew_sequence < resume_from_crew_sequence
            )

            if should_skip_crew:
                logger.info(f"  ⏭️  SKIPPING listener crew '{crew_name}' (sequence {current_crew_sequence} < resume_from {resume_from_crew_sequence})")
                # Get checkpoint output for this crew if available
                crew_checkpoint_output = None
                if checkpoint_outputs:
                    crew_checkpoint_output = checkpoint_outputs.get(crew_name)
                    if crew_checkpoint_output:
                        logger.info(f"  📦 Found checkpoint output for '{crew_name}'")
                    else:
                        logger.warning(f"  ⚠️ No checkpoint output found for '{crew_name}' in checkpoint_outputs")
                # Create a stub listener method that returns the checkpoint output to allow flow to continue
                class_methods[method_name] = FlowMethodFactory.create_skipped_crew_method(
                    method_name=method_name,
                    crew_name=crew_name,
                    crew_sequence=current_crew_sequence,
                    is_starting_point=False,
                    method_condition=method_condition,
                    condition_type=condition_type if condition_type in ["AND", "OR"] else "NONE",
                    checkpoint_output=crew_checkpoint_output
                )
                logger.info(f"  ✅ Created SKIP listener '{method_name}' for crew '{crew_name}' (checkpoint resume)")
            else:
                # Create ONE listener method for the entire crew
                # Tasks within the crew will execute sequentially (task.context was set by process_listeners)
                class_methods[method_name] = FlowMethodFactory.create_listener_method(
                    method_name=method_name,
                    listener_tasks=listener_tasks,  # All tasks in this crew (with task.context for ordering)
                    method_condition=method_condition,
                    condition_type=condition_type if condition_type in ["AND", "OR"] else "NONE",
                    callbacks=callbacks,
                    group_context=group_context,
                    create_execution_callbacks=create_execution_callbacks,
                    crew_name=crew_name,
                    crew_data=crew_data
                )
                logger.info(f"  ✅ Created listener '{method_name}' for crew '{crew_name}' with {len(listener_tasks)} sequential tasks, listening to: {method_condition}")

            # Track this listener method's sequence for potential downstream HITL gates
            method_to_sequence[method_name] = current_crew_sequence

        # Process HITL gate nodes and create gate methods
        # HITL gates pause the flow for human approval before continuing
        hitl_gates = flow_config.get('hitlGates', []) if flow_config else []
        nodes = flow_config.get('nodes', []) if flow_config else []
        edges = flow_config.get('edges', []) if flow_config else []

        # Also check for HITL gate nodes in the nodes array by type
        # This handles cases where HITL gates are defined as regular nodes
        for node in nodes:
            node_type = node.get('type', '')
            if node_type == 'hitlGateNode':
                node_id = node.get('id', '')
                node_data = node.get('data', {})

                # Build gate config from node data
                gate_config = {
                    'message': node_data.get('message', 'Approval required to proceed'),
                    'timeout_seconds': node_data.get('timeout_seconds', 86400),  # 24 hours default
                    'timeout_action': node_data.get('timeout_action', 'auto_reject'),
                    'require_comment': node_data.get('require_comment', False),
                    'allowed_approvers': node_data.get('allowed_approvers', [])
                }

                # Find the previous node this gate is connected to (incoming edge)
                previous_method_name = None
                incoming_edges = [e for e in edges if e.get('target') == node_id]
                if incoming_edges:
                    source_node_id = incoming_edges[0].get('source', '')
                    # Map source node to method name
                    # Check starting points first
                    for sp_info in starting_points:
                        sp_method_name, sp_task_ids, sp_tasks, sp_crew_name, sp_crew_data = sp_info
                        # Check if source matches any task ID in this starting point's crew
                        if source_node_id in [str(tid) for tid in sp_task_ids]:
                            previous_method_name = sp_method_name
                            break
                        # Also check node_id pattern
                        if source_node_id == sp_crew_data.get('id') if sp_crew_data else None:
                            previous_method_name = sp_method_name
                            break

                    # Check listeners if not found in starting points
                    if not previous_method_name:
                        for listener_info in listener_crews:
                            l_method_name, l_crew_id, l_task_ids, l_tasks, l_crew_name, l_listen_to, l_condition, l_crew_data = listener_info
                            if source_node_id in [str(tid) for tid in l_task_ids]:
                                previous_method_name = l_method_name
                                break
                            if source_node_id == l_crew_id:
                                previous_method_name = l_method_name
                                break

                if not previous_method_name:
                    logger.warning(f"HITL gate {node_id} has no incoming connection - using default start method")
                    previous_method_name = starting_points[0][0] if starting_points else "starting_point_0"

                # Create the HITL gate method
                gate_method_name = f"hitl_gate_{node_id}"
                logger.info(f"Creating HITL gate method: {gate_method_name} listening to {previous_method_name}")

                class_methods[gate_method_name] = FlowMethodFactory.create_hitl_gate_method(
                    method_name=gate_method_name,
                    gate_node_id=node_id,
                    gate_config=gate_config,
                    previous_method_name=previous_method_name,
                    crew_sequence=crew_sequence_counter,
                    callbacks=callbacks,
                    group_context=group_context
                )
                logger.info(f"  ✅ Created HITL gate method '{gate_method_name}'")

                # Store mapping so listeners can listen to this gate instead of the previous crew
                # This is stored in gate_config with the node_id as key
                hitl_gates.append({
                    'id': node_id,
                    'method_name': gate_method_name,
                    'listens_to': previous_method_name
                })

        logger.info(f"Processed {len(hitl_gates)} HITL gate nodes")

        # Add router methods for conditional routing
        for i, router_config in enumerate(routers):
            router_name = router_config.get('name', f'router_{i}')
            listen_to = router_config.get('listenTo')  # Method name to listen to
            routes = router_config.get('routes', {})  # Dict of route_name -> task configs
            condition_expr = router_config.get('condition')  # Legacy: single Python condition expression
            route_conditions = router_config.get('routeConditions', {})  # New: per-route conditions
            condition_field = router_config.get('conditionField', 'success')

            # Find the method to listen to
            # Default to the first starting point method if not specified
            default_method = starting_points[0][0] if starting_points else "starting_point_0"
            listen_to_method = listen_to or default_method

            # Create router method
            def router_factory(router_routes, router_condition_expr, router_route_conditions, router_condition_field, router_method_name):
                @router(listen_to_method)
                def route_method(self, *args, **kwargs):
                    logger.info(f"Router {router_method_name} evaluating condition")

                    # Build evaluation context for condition evaluation
                    def build_eval_context():
                        import json
                        eval_context = {}

                        # Helper function to convert string values to appropriate types
                        def auto_convert_value(val):
                            """Convert string numeric values to int/float."""
                            if isinstance(val, str):
                                # Try int first
                                try:
                                    return int(val)
                                except ValueError:
                                    pass
                                # Try float
                                try:
                                    return float(val)
                                except ValueError:
                                    pass
                            return val

                        # Helper function to convert all string numerics in a dict
                        def auto_convert_dict(d):
                            """Recursively convert string numerics in a dict."""
                            if not isinstance(d, dict):
                                return d
                            return {k: auto_convert_value(v) if not isinstance(v, dict) else auto_convert_dict(v) for k, v in d.items()}

                        # Safe helper functions for condition evaluation
                        def safe_int(val, default=0):
                            """Safely convert value to int."""
                            try:
                                return int(val)
                            except (ValueError, TypeError):
                                return default

                        def safe_float(val, default=0.0):
                            """Safely convert value to float."""
                            try:
                                return float(val)
                            except (ValueError, TypeError):
                                return default

                        # Add helper functions to context for use in conditions
                        eval_context['int'] = safe_int
                        eval_context['float'] = safe_float
                        eval_context['str'] = str
                        eval_context['len'] = len
                        eval_context['bool'] = bool
                        eval_context['abs'] = abs
                        eval_context['min'] = min
                        eval_context['max'] = max

                        # Add state to context if available
                        if hasattr(self, 'state'):
                            eval_context['state'] = self.state
                        else:
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
                                    if raw_str.startswith('{') and raw_str.endswith('}'):
                                        parsed_data = json.loads(raw_str)
                                        # Auto-convert numeric strings to actual numbers
                                        parsed_data = auto_convert_dict(parsed_data)
                                        eval_context['state'].update(parsed_data)
                                        eval_context.update(parsed_data)  # Also add to top-level for easy access
                                        logger.info(f"Parsed crew output JSON and merged into state: {parsed_data}")
                                except (json.JSONDecodeError, Exception) as parse_err:
                                    logger.debug(f"Could not parse crew output as JSON: {parse_err}")

                            # If result is a string that looks like JSON, parse it
                            elif isinstance(result_obj, str):
                                try:
                                    raw_str = result_obj.strip()
                                    if raw_str.startswith('{') and raw_str.endswith('}'):
                                        parsed_data = json.loads(raw_str)
                                        # Auto-convert numeric strings to actual numbers
                                        parsed_data = auto_convert_dict(parsed_data)
                                        eval_context['state'].update(parsed_data)
                                        eval_context.update(parsed_data)  # Also add to top-level for easy access
                                        logger.info(f"Parsed string result as JSON and merged into state: {parsed_data}")
                                except (json.JSONDecodeError, Exception) as parse_err:
                                    logger.debug(f"Could not parse string result as JSON: {parse_err}")

                            # Also add common fields from result
                            if isinstance(args[0], dict):
                                eval_context.update(args[0])
                            elif hasattr(args[0], '__dict__'):
                                eval_context.update(vars(args[0]))

                        # Parse JSON strings in state values and add them to top-level context
                        # This makes values like state["Random Number"] = '{"number": 43}' accessible as eval_context["number"] = 43
                        if eval_context.get('state'):
                            for key, value in list(eval_context['state'].items()):
                                if isinstance(value, str):
                                    # Strip markdown code fences if present (e.g., ```json\n...\n```)
                                    json_value = value.strip()
                                    if json_value.startswith('```'):
                                        # Remove opening code fence (```json or ```)
                                        first_newline = json_value.find('\n')
                                        if first_newline != -1:
                                            json_value = json_value[first_newline + 1:]
                                        # Remove closing code fence
                                        if json_value.rstrip().endswith('```'):
                                            json_value = json_value.rstrip()[:-3].rstrip()

                                    # Now check if it looks like JSON
                                    if json_value.strip().startswith('{') and json_value.strip().endswith('}'):
                                        try:
                                            parsed_value = json.loads(json_value)
                                            if isinstance(parsed_value, dict):
                                                # Auto-convert numeric strings to actual numbers
                                                parsed_value = auto_convert_dict(parsed_value)
                                                # Add parsed values to both state and top-level for easy access
                                                eval_context['state'].update(parsed_value)
                                                eval_context.update(parsed_value)
                                                logger.info(f"Parsed state['{key}'] JSON and added to context: {list(parsed_value.keys())}")
                                        except (json.JSONDecodeError, Exception) as e:
                                            logger.debug(f"Could not parse state['{key}'] as JSON: {e}")
                                            pass  # Not JSON, leave as-is

                        # Add kwargs
                        eval_context.update(kwargs)
                        return eval_context

                    # If we have per-route conditions (routeConditions), evaluate each route's condition
                    if router_route_conditions:
                        try:
                            eval_context = build_eval_context()
                            logger.info(f"Evaluating per-route conditions for routes: {list(router_route_conditions.keys())}")
                            logger.info(f"Context keys: {list(eval_context.keys())}")
                            logger.info(f"State contents: {eval_context.get('state', {})}")

                            # Evaluate each route's condition and return the first matching route
                            for route_name, route_condition in router_route_conditions.items():
                                if route_condition:
                                    logger.info(f"Evaluating condition for route '{route_name}': {route_condition}")
                                    try:
                                        condition_result = eval(route_condition, {"__builtins__": {}}, eval_context)
                                        logger.info(f"Route '{route_name}' condition evaluated to: {condition_result}")
                                        if condition_result:
                                            logger.info(f"Router {router_method_name} taking route: {route_name}")
                                            return route_name
                                    except Exception as route_err:
                                        logger.warning(f"Error evaluating condition for route '{route_name}': {route_err}")
                                        continue

                            # No route matched - take 'default' route if exists, otherwise None
                            if 'default' in router_routes:
                                logger.info(f"Router {router_method_name} no condition matched, taking 'default' route")
                                return 'default'
                            else:
                                logger.info(f"Router {router_method_name} no condition matched and no 'default' route, flow stops")
                                return None

                        except Exception as e:
                            logger.error(f"Error evaluating router conditions: {e}", exc_info=True)
                            return None

                    # Legacy: single condition expression (deprecated but still supported)
                    elif router_condition_expr:
                        try:
                            eval_context = build_eval_context()

                            logger.info(f"Evaluating legacy condition: {router_condition_expr}")
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
                                return None

                        except Exception as e:
                            logger.error(f"Error evaluating router condition: {e}", exc_info=True)
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
            logger.info(f"[ROUTER DEBUG] Creating router {router_method_name}")
            logger.info(f"[ROUTER DEBUG]   routes: {routes}")
            logger.info(f"[ROUTER DEBUG]   route_conditions (full dict): {route_conditions}")
            logger.info(f"[ROUTER DEBUG]   condition_expr (legacy): {condition_expr}")
            logger.info(f"[ROUTER DEBUG]   listen_to_method: {listen_to_method}")
            bound_router = router_factory(routes, condition_expr, route_conditions, condition_field, router_method_name)
            class_methods[router_method_name] = bound_router
            logger.info(f"Created router method {router_method_name} with routes: {list(routes.keys())}")

            # Add listener methods for each route
            for route_name, route_tasks in routes.items():
                logger.info(f"Creating route listener for route '{route_name}' with {len(route_tasks)} tasks")
                logger.info(f"  Route tasks: {route_tasks}")
                logger.info(f"  All tasks available: {list(all_tasks.keys())}")

                route_task_objs = [all_tasks[t.get('id')] for t in route_tasks if t.get('id') in all_tasks]
                logger.info(f"  Found {len(route_task_objs)} task objects for route listener")

                if route_task_objs:
                    route_listener_name = f"route_{router_name}_{route_name}_{i}"

                    def route_listener_factory(route_task_list, route_listener_method_name, callbacks_param, group_ctx, expected_route, route_crew_name_param):
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

                            # CRITICAL FIX: Inject previous output context into task descriptions
                            # This ensures the agent has access to the data from the previous crew
                            # Same pattern as create_listener_method in flow_methods.py
                            runtime_tasks = []
                            previous_output_context = ""

                            if previous_output:
                                # Get the first agent to determine context limits
                                first_agent = route_task_list[0].agent if route_task_list else None

                                # Get model's context window and max output tokens using ModelConfigService
                                context_window_tokens, max_output_tokens = await get_model_context_limits(first_agent, group_ctx) if first_agent else (128000, 16000)

                                # Calculate available input budget (subtract output reservation)
                                available_input_tokens = context_window_tokens - max_output_tokens

                                # Allocate 60% of available input for previous output
                                # This leaves 40% for system prompts, tools, conversation history, and safety buffer
                                max_context_tokens = int(available_input_tokens * 0.6)

                                # Convert tokens to characters (using 3.5 chars/token for safety)
                                max_context_length = int(max_context_tokens * 3.5)

                                logger.info(f"Model limits: context={context_window_tokens} tokens, max_output={max_output_tokens} tokens")
                                logger.info(f"Available input: {available_input_tokens} tokens, allocating {max_context_tokens} tokens ({max_context_length} chars) for previous output")

                                # Create a concise context string to inject into task descriptions
                                # Use extract_final_answer to get only the final answer, not the full thinking process
                                previous_output_str = extract_final_answer([previous_output])
                                if len(previous_output_str) > max_context_length:
                                    previous_output_context = f"\n\nContext from previous step:\n{previous_output_str[:max_context_length]}...\n(Output truncated for brevity)"
                                else:
                                    previous_output_context = f"\n\nContext from previous step:\n{previous_output_str}"
                                logger.info(f"📤 Injecting previous output context into task descriptions ({len(previous_output_context)} chars)")

                            # Create new Task objects with modified descriptions
                            for task in route_task_list:
                                # Create new task with injected context
                                runtime_task = Task(
                                    description=f"{task.description}{previous_output_context}",
                                    agent=task.agent,
                                    expected_output=task.expected_output if hasattr(task, 'expected_output') else "Task completed successfully"
                                )
                                runtime_tasks.append(runtime_task)
                                logger.info(f"Created runtime task with injected context for agent: {task.agent.role}")

                            # Use runtime_tasks (with context) instead of original route_task_list
                            tasks_to_use = runtime_tasks

                            # CrewAI validation: A crew cannot end with more than one async task
                            # If we have multiple async tasks, auto-create a completion task
                            async_tasks = [t for t in tasks_to_use if getattr(t, 'async_execution', False)]

                            if len(async_tasks) > 1:
                                # Auto-create a lightweight completion task that waits for all async tasks
                                from crewai import Task as CrewTask

                                completion_agent = async_tasks[-1].agent
                                completion_task = CrewTask(
                                    description="Aggregate and return results from parallel task executions",
                                    expected_output="Combined results from all parallel tasks",
                                    agent=completion_agent,
                                    context=async_tasks,
                                    async_execution=False
                                )
                                tasks_to_use.append(completion_task)
                                logger.info(f"Auto-created completion task for route listener to handle {len(async_tasks)} async tasks")

                            # Create crew with runtime tasks (with injected context)
                            logger.info("Creating Crew instance for route listener")

                            # Use provided crew name, fallback to first agent role
                            route_crew_name = route_crew_name_param if route_crew_name_param else (agents[0].role if agents and hasattr(agents[0], 'role') and agents[0].role else "Route Crew")
                            logger.info(f"Creating route crew with name: {route_crew_name}")

                            crew = Crew(
                                name=route_crew_name,  # Set crew name for proper event tracing
                                agents=agents,
                                tasks=tasks_to_use,  # Use validated task list
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

                    # Try to get crew name from route tasks configuration
                    route_crew_name = None
                    if route_tasks and len(route_tasks) > 0:
                        route_crew_name = route_tasks[0].get('crewName') or route_tasks[0].get('crew_name')

                    bound_route_listener = route_listener_factory(route_task_objs, route_listener_name, callbacks, group_context, route_name, route_crew_name)
                    class_methods[route_listener_name] = bound_route_listener
                    logger.info(f"Created route listener {route_listener_name} for crew '{route_crew_name}' listening to router '{router_method_name}' for route '{route_name}'")

        # CRITICAL: Create the DynamicFlow class WITH all methods using type()
        # This allows the FlowMeta metaclass to process @start() and @listen() decorators
        logger.info("="*100)
        logger.info(f"CREATING DYNAMICFLOW CLASS with {len(class_methods)} methods")
        logger.info(f"  persistence_enabled: {persistence_enabled}")
        logger.info(f"  restore_uuid: {restore_uuid}")
        logger.info("="*100)

        DynamicFlow = type(
            'DynamicFlow',      # Class name
            (CrewAIFlow,),      # Base classes (includes FlowMeta metaclass)
            class_methods       # All methods in dictionary
        )
        logger.info("✅ DynamicFlow class created with FlowMeta metaclass processing")

        # Apply @persist decorator if persistence is enabled
        # This enables checkpoint/resume functionality via CrewAI's flow persistence
        if persistence_enabled:
            try:
                from crewai.flow.persistence import persist
                DynamicFlow = persist()(DynamicFlow)
                logger.info("✅ Applied @persist decorator for flow state checkpointing")
            except ImportError as e:
                logger.warning(f"Could not import persist decorator (may require CrewAI 0.98.0+): {e}")
            except Exception as e:
                logger.warning(f"Error applying @persist decorator: {e}")

        # Create an instance of our properly defined flow
        # Note: State restoration happens at kickoff time when 'id' is passed in inputs
        # (see backend_flow.py kickoff_async - passes inputs={"id": restore_uuid})
        # The @persist decorator handles loading state from persistence based on that id
        if restore_uuid and persistence_enabled:
            logger.info(f"Flow will resume from checkpoint {restore_uuid} when kickoff is called with id in inputs")
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