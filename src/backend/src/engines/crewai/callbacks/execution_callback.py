"""
Execution-scoped callback system for CrewAI.

This module provides callback functions that are scoped to specific executions,
replacing global event listeners to prevent cross-contamination between concurrent crews.
"""

import logging
from typing import Any, Optional, Dict
from datetime import datetime, timezone

# Import queue services
from src.services.execution_logs_queue import enqueue_log
from src.services.trace_queue import get_trace_queue

# Import group context
from src.utils.user_context import GroupContext, UserContext

logger = logging.getLogger(__name__)


def create_execution_callbacks(job_id: str, config: Dict[str, Any] = None, group_context: GroupContext = None, crew: Any = None):
    """
    Create execution-scoped callback functions for a specific CrewAI execution.
    
    These callbacks are passed directly to crew.kickoff() and are automatically
    scoped to the specific execution, preventing cross-contamination.
    
    Args:
        job_id: Unique identifier for the execution
        config: Optional configuration dictionary
        group_context: Group context for multi-tenant isolation
        crew: The CrewAI crew instance for context extraction
        
    Returns:
        Tuple of (step_callback, task_callback) functions
    """
    log_prefix = f"[ExecutionCallback][{job_id}]"
    trace_queue = get_trace_queue()
    
    # Enhanced context tracking
    execution_context = {
        "current_agent": None,
        "current_task": None,
        "agent_lookup": {},  # id -> role mapping
        "task_to_agent": {},  # task index -> agent role
        "last_known_agent": None
    }
    
    # Build agent lookup from crew if available
    if crew and hasattr(crew, 'agents'):
        for agent in crew.agents:
            if hasattr(agent, 'role'):
                agent_role = agent.role
                execution_context["agent_lookup"][id(agent)] = agent_role
                execution_context["agent_lookup"][agent_role] = agent
                logger.debug(f"{log_prefix} Registered agent: {agent_role}")
    
    # Build task-to-agent mapping from crew
    if crew and hasattr(crew, 'tasks'):
        for idx, task in enumerate(crew.tasks):
            if hasattr(task, 'agent') and task.agent:
                if hasattr(task.agent, 'role'):
                    agent_role = task.agent.role
                    execution_context["task_to_agent"][idx] = agent_role
                    execution_context["task_to_agent"][task] = agent_role
                    logger.debug(f"{log_prefix} Task {idx} mapped to agent: {agent_role}")
    
    logger.info(f"{log_prefix} Creating execution-scoped callbacks with {len(execution_context['agent_lookup'])} agents")
    
    def step_callback(step_output):
        """
        Called after each agent step during execution.
        
        Args:
            step_output: The output from an agent step
        """
        try:
            logger.debug(f"{log_prefix} Step callback triggered - type: {type(step_output).__name__}")
            
            # Extract information from step output
            timestamp = datetime.now(timezone.utc)
            
            # First, identify the type of step output
            step_type = type(step_output).__name__
            is_agent_action = hasattr(step_output, '__class__') and 'AgentAction' in step_output.__class__.__name__
            is_agent_finish = hasattr(step_output, '__class__') and 'AgentFinish' in step_output.__class__.__name__
            is_tool_result = hasattr(step_output, '__class__') and 'ToolResult' in step_output.__class__.__name__
            
            # Handle AgentAction objects specially
            if is_agent_action:
                # Extract meaningful information from AgentAction
                content_parts = []
                if hasattr(step_output, 'tool'):
                    content_parts.append(f"Tool: {step_output.tool}")
                if hasattr(step_output, 'tool_input'):
                    tool_input_str = str(step_output.tool_input)
                    if len(tool_input_str) > 200:
                        tool_input_str = tool_input_str[:200] + "..."
                    content_parts.append(f"Input: {tool_input_str}")
                if hasattr(step_output, 'thought'):
                    content_parts.append(f"Thought: {step_output.thought}")
                if hasattr(step_output, 'log'):
                    content_parts.append(f"Log: {step_output.log}")
                content = " | ".join(content_parts) if content_parts else str(step_output)
            elif hasattr(step_output, 'output'):
                content = str(step_output.output)
            elif hasattr(step_output, 'raw'):
                content = str(step_output.raw)
            else:
                content = str(step_output)
            
            # Enhanced agent extraction with multiple strategies
            agent_name = "Unknown Agent"
            
            # Strategy 1: Direct agent attribute (rare but check first)
            if hasattr(step_output, 'agent') and step_output.agent:
                if hasattr(step_output.agent, 'role'):
                    agent_name = step_output.agent.role
                    execution_context["current_agent"] = agent_name
                    execution_context["last_known_agent"] = agent_name
                elif id(step_output.agent) in execution_context["agent_lookup"]:
                    agent_name = execution_context["agent_lookup"][id(step_output.agent)]
                    execution_context["current_agent"] = agent_name
                    execution_context["last_known_agent"] = agent_name
            
            # Strategy 2: Handle specific object types using context
            elif is_agent_action or is_agent_finish or is_tool_result or isinstance(step_output, str):
                # These types don't have direct agent reference, use context
                
                # For AgentAction, we're about to use a tool, so this is the current agent acting
                # For AgentFinish, the agent is finishing their task
                # For ToolResult, it's the result of the current agent's tool usage
                # For strings, it's output from an agent without tools
                
                if execution_context["current_agent"]:
                    agent_name = execution_context["current_agent"]
                elif execution_context["last_known_agent"]:
                    agent_name = execution_context["last_known_agent"]
                elif execution_context["current_task"] and execution_context["current_task"] in execution_context["task_to_agent"]:
                    agent_name = execution_context["task_to_agent"][execution_context["current_task"]]
                    execution_context["current_agent"] = agent_name
                # Try to infer from the first task if we haven't started yet
                elif not execution_context["current_agent"] and execution_context["task_to_agent"]:
                    # If we have task mappings but no current agent, we're likely at the start
                    # Use the agent from the first task
                    for task_idx in sorted([k for k in execution_context["task_to_agent"].keys() if isinstance(k, int)]):
                        agent_name = execution_context["task_to_agent"][task_idx]
                        execution_context["current_agent"] = agent_name
                        logger.debug(f"{log_prefix} Using first task's agent: {agent_name}")
                        break
                # For single-agent crews
                elif len(execution_context["agent_lookup"]) > 0:
                    # Count actual agents (not including reverse lookups)
                    agent_roles = [v for k, v in execution_context["agent_lookup"].items() if isinstance(v, str)]
                    if len(agent_roles) == 1:
                        agent_name = agent_roles[0]
                        execution_context["current_agent"] = agent_name
                    elif len(agent_roles) > 0:
                        # Multiple agents, try to use the first one as fallback
                        agent_name = agent_roles[0]
                        execution_context["current_agent"] = agent_name
                        logger.debug(f"{log_prefix} Multiple agents found, using first: {agent_name}")
            
            # Special handling for ToolResult - it comes after AgentAction, so preserve the agent
            if is_tool_result and agent_name != "Unknown Agent":
                # ToolResult means the current agent just used a tool
                execution_context["current_agent"] = agent_name
                execution_context["last_known_agent"] = agent_name
            
            # If still unknown, log for debugging
            if agent_name == "Unknown Agent":
                logger.warning(f"{log_prefix} Could not determine agent for {step_type}. Context: current={execution_context.get('current_agent')}, last={execution_context.get('last_known_agent')}")
            
            logger.debug(f"{log_prefix} Extracted agent: {agent_name} from {type(step_output).__name__}")
            
            # Limit content length for logging
            content_preview = content[:500] + "..." if len(content) > 500 else content
            
            # Log the step
            log_message = f"[STEP] Agent: {agent_name} - Output: {content_preview}"
            
            # Enqueue to execution logs with group context
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            # Enqueue to trace queue for detailed analysis
            trace_data = {
                "job_id": job_id,
                "event_source": agent_name,
                "event_context": "agent_step",
                "event_type": "agent_execution",  # Use important event type
                "timestamp": timestamp.isoformat(),
                "output_content": content,
                "extra_data": {
                    "type": "step_callback",
                    "agent_role": agent_name if agent_name != "Unknown Agent" else None,
                    "step_type": type(step_output).__name__
                }
            }
            
            # Add group context if available
            if group_context:
                trace_data["group_id"] = group_context.primary_group_id
                trace_data["group_email"] = group_context.group_email
            
            try:
                trace_queue.put_nowait(trace_data)
                logger.debug(f"{log_prefix} Step trace enqueued successfully")
            except Exception as trace_error:
                logger.error(f"{log_prefix} Failed to enqueue step trace: {trace_error}")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in step_callback: {e}", exc_info=True)
    
    def task_callback(task_output):
        """
        Called after each task completion during execution.
        
        Args:
            task_output: The output from a completed task
        """
        try:
            logger.debug(f"{log_prefix} Task callback triggered - type: {type(task_output).__name__}")
            
            # Extract information from task output
            timestamp = datetime.now(timezone.utc)
            
            # Get task information
            task_description = "Unknown Task"
            task_obj = None
            if hasattr(task_output, 'description'):
                task_description = task_output.description
            elif hasattr(task_output, 'task') and hasattr(task_output.task, 'description'):
                task_description = task_output.task.description
                task_obj = task_output.task
            
            # Update current task in context
            if task_obj:
                execution_context["current_task"] = task_obj
            
            # Get output content
            if hasattr(task_output, 'raw'):
                content = str(task_output.raw)
            elif hasattr(task_output, 'output'):
                content = str(task_output.output)
            else:
                content = str(task_output)
            
            # Enhanced agent extraction from task
            agent_name = "Unknown Agent"
            
            # Strategy 1: Direct agent attribute on task_output
            if hasattr(task_output, 'agent') and task_output.agent:
                if hasattr(task_output.agent, 'role'):
                    agent_name = task_output.agent.role
                    execution_context["current_agent"] = agent_name
                    execution_context["last_known_agent"] = agent_name
            
            # Strategy 2: Agent from task object
            elif hasattr(task_output, 'task') and hasattr(task_output.task, 'agent'):
                if hasattr(task_output.task.agent, 'role'):
                    agent_name = task_output.task.agent.role
                    execution_context["current_agent"] = agent_name
                    execution_context["last_known_agent"] = agent_name
            
            # Strategy 3: Use task-to-agent mapping
            elif task_obj and task_obj in execution_context["task_to_agent"]:
                agent_name = execution_context["task_to_agent"][task_obj]
                execution_context["current_agent"] = agent_name
                execution_context["last_known_agent"] = agent_name
            
            # Strategy 4: Use current context
            elif execution_context["current_agent"]:
                agent_name = execution_context["current_agent"]
            
            logger.info(f"{log_prefix} Task completed by agent: {agent_name}")
            
            # Limit content length for logging
            task_preview = task_description[:100] + "..." if len(task_description) > 100 else task_description
            content_preview = content[:500] + "..." if len(content) > 500 else content
            
            # Log the task completion
            log_message = f"[TASK COMPLETED] Task: {task_preview} - Agent: {agent_name} - Output: {content_preview}"
            
            # Enqueue to execution logs with group context
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            # Enqueue to trace queue for detailed analysis
            trace_data = {
                "job_id": job_id,
                "event_source": "task",
                "event_context": task_description,
                "event_type": "task_completed",
                "timestamp": timestamp.isoformat(),
                "output_content": content,
                "extra_data": {
                    "type": "task_callback",
                    "agent_role": agent_name,
                    "task_description": task_description
                }
            }
            
            # Add group context if available
            if group_context:
                trace_data["group_id"] = group_context.primary_group_id
                trace_data["group_email"] = group_context.group_email
            
            try:
                trace_queue.put_nowait(trace_data)
                logger.debug(f"{log_prefix} Task trace enqueued successfully")
            except Exception as trace_error:
                logger.error(f"{log_prefix} Failed to enqueue task trace: {trace_error}")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in task_callback: {e}", exc_info=True)
    
    logger.info(f"{log_prefix} Execution-scoped callbacks created successfully")
    return step_callback, task_callback


def create_crew_callbacks(job_id: str, config: Dict[str, Any] = None, group_context: GroupContext = None):
    """
    Create crew-level callback functions for logging crew lifecycle events.
    
    Args:
        job_id: Unique identifier for the execution
        config: Optional configuration dictionary
        group_context: Group context for multi-tenant isolation
        
    Returns:
        Dictionary of crew callback functions
    """
    log_prefix = f"[CrewCallback][{job_id}]"
    
    def on_crew_start():
        """Called when crew execution starts."""
        try:
            timestamp = datetime.now(timezone.utc)
            log_message = f"[CREW STARTED] Execution {job_id} started"
            
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            # Also create trace for crew start
            from src.services.trace_queue import get_trace_queue
            trace_queue = get_trace_queue()
            trace_data = {
                "job_id": job_id,
                "event_source": "crew",
                "event_context": f"execution-{job_id}",
                "event_type": "crew_started",
                "timestamp": timestamp.isoformat(),
                "output_content": f"Crew execution {job_id} started",
                "extra_data": {"type": "crew_callback"}
            }
            
            if group_context:
                trace_data["group_id"] = group_context.primary_group_id
                trace_data["group_email"] = group_context.group_email
            
            try:
                trace_queue.put_nowait(trace_data)
            except Exception as trace_error:
                logger.error(f"{log_prefix} Failed to enqueue crew start trace: {trace_error}")
            
            logger.info(f"{log_prefix} Crew execution started")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in on_crew_start: {e}", exc_info=True)
    
    def on_crew_complete(result):
        """Called when crew execution completes."""
        try:
            timestamp = datetime.now(timezone.utc)
            
            # Format result for logging
            result_preview = str(result)[:500] + "..." if len(str(result)) > 500 else str(result)
            log_message = f"[CREW COMPLETED] Execution {job_id} completed - Result: {result_preview}"
            
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            # Also create trace for crew completion
            from src.services.trace_queue import get_trace_queue
            trace_queue = get_trace_queue()
            trace_data = {
                "job_id": job_id,
                "event_source": "crew",
                "event_context": f"execution-{job_id}",
                "event_type": "crew_completed",
                "timestamp": timestamp.isoformat(),
                "output_content": result_preview,
                "extra_data": {"type": "crew_callback"}
            }
            
            if group_context:
                trace_data["group_id"] = group_context.primary_group_id
                trace_data["group_email"] = group_context.group_email
            
            try:
                trace_queue.put_nowait(trace_data)
            except Exception as trace_error:
                logger.error(f"{log_prefix} Failed to enqueue crew completion trace: {trace_error}")
            
            logger.info(f"{log_prefix} Crew execution completed")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in on_crew_complete: {e}", exc_info=True)
    
    def on_crew_error(error):
        """Called when crew execution fails."""
        try:
            timestamp = datetime.now(timezone.utc)
            log_message = f"[CREW FAILED] Execution {job_id} failed - Error: {str(error)}"
            
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            logger.error(f"{log_prefix} Crew execution failed: {error}")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in on_crew_error: {e}", exc_info=True)
    
    return {
        'on_start': on_crew_start,
        'on_complete': on_crew_complete,
        'on_error': on_crew_error
    }


def log_crew_initialization(job_id: str, config: Dict[str, Any] = None, group_context: GroupContext = None):
    """
    Log crew initialization with configuration details.
    
    Args:
        job_id: Unique identifier for the execution
        config: Configuration dictionary
        group_context: Group context for multi-tenant isolation
    """
    try:
        timestamp = datetime.now(timezone.utc)
        
        # Create sanitized config for logging
        sanitized_config = {}
        if config:
            # Extract safe configuration elements
            for key, value in config.items():
                if key not in ['api_keys', 'tokens', 'passwords']:
                    sanitized_config[key] = value
        
        log_message = f"[CREW INITIALIZED] Job {job_id} - Config: {sanitized_config}"
        
        enqueue_log(
            execution_id=job_id,
            content=log_message,
            timestamp=timestamp,
            group_context=group_context
        )
        
        logger.info(f"[ExecutionCallback][{job_id}] Crew initialization logged")
        
    except Exception as e:
        logger.error(f"[ExecutionCallback][{job_id}] Error logging crew initialization: {e}", exc_info=True)