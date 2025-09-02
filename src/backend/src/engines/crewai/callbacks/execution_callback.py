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
    
    # Store crew reference for use in callbacks
    crew_ref = crew
    
    # Enhanced context tracking
    execution_context = {
        "current_agent": None,
        "current_task": None,
        "agent_lookup": {},  # id -> role mapping
        "task_to_agent": {},  # task index -> agent role
        "last_known_agent": None,
        "task_index": 0,  # Track which task we're on
        "agent_tools": {},  # agent role -> list of tool names
        "tool_to_agent": {}  # tool name -> agent role
    }
    
    # Build agent lookup from crew if available
    if crew and hasattr(crew, 'agents'):
        logger.info(f"{log_prefix} Building agent lookup for {len(crew.agents)} agents")
        for idx, agent in enumerate(crew.agents):
            if hasattr(agent, 'role'):
                agent_role = agent.role
                execution_context["agent_lookup"][id(agent)] = agent_role
                execution_context["agent_lookup"][agent_role] = agent
                logger.info(f"{log_prefix} Registered agent #{idx}: {agent_role}")
                
                # Track tools for each agent
                if hasattr(agent, 'tools') and agent.tools:
                    tool_names = []
                    logger.info(f"{log_prefix} Agent '{agent_role}' has {len(agent.tools)} tools")
                    for tool in agent.tools:
                        tool_name = None
                        if hasattr(tool, 'name'):
                            tool_name = tool.name
                        elif hasattr(tool, '__class__'):
                            tool_name = tool.__class__.__name__
                        
                        if tool_name:
                            tool_names.append(tool_name)
                            execution_context["tool_to_agent"][tool_name] = agent_role
                            logger.info(f"{log_prefix} Mapped tool '{tool_name}' to agent '{agent_role}'")
                            
                            # Also map tool without prefix for MCP tools
                            # MCP tools have format like "ServerName_tool_name"
                            if '_' in tool_name:
                                # Extract the part after the first underscore
                                unprefixed_name = '_'.join(tool_name.split('_')[1:])
                                if unprefixed_name:
                                    execution_context["tool_to_agent"][unprefixed_name] = agent_role
                                    logger.info(f"{log_prefix} Also mapped unprefixed '{unprefixed_name}' to agent '{agent_role}'")
                            
                            # Also try without any prefix (just the last part)
                            parts = tool_name.split('_')
                            if len(parts) > 1:
                                last_parts = ['_'.join(parts[-2:]), parts[-1]]  # Try last 2 parts and last part
                                for variant in last_parts:
                                    if variant and variant != tool_name:
                                        execution_context["tool_to_agent"][variant] = agent_role
                                        logger.info(f"{log_prefix} Also mapped variant '{variant}' to agent '{agent_role}'")
                    
                    execution_context["agent_tools"][agent_role] = tool_names
                else:
                    logger.info(f"{log_prefix} Agent '{agent_role}' has no tools")
    
    # Build task-to-agent mapping from crew
    if crew and hasattr(crew, 'tasks'):
        logger.info(f"{log_prefix} Building task-to-agent mapping for {len(crew.tasks)} tasks")
        for idx, task in enumerate(crew.tasks):
            if hasattr(task, 'agent') and task.agent:
                if hasattr(task.agent, 'role'):
                    agent_role = task.agent.role
                    execution_context["task_to_agent"][idx] = agent_role
                    execution_context["task_to_agent"][task] = agent_role
                    task_desc = task.description if hasattr(task, 'description') else 'unknown'
                    logger.info(f"{log_prefix} Task #{idx} '{task_desc[:50]}...' mapped to agent: {agent_role}")
    
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
            
            logger.info(f"{log_prefix} Step output type: {step_type}, is_agent_action: {is_agent_action}, is_tool_result: {is_tool_result}")
            logger.info(f"{log_prefix} Current context - agent: {execution_context.get('current_agent')}, task_index: {execution_context.get('task_index')}")
            
            # Check if we're working on a task that's different from what we expect
            # This helps detect task switches
            if crew_ref and hasattr(crew_ref, 'tasks') and execution_context["task_index"] < len(crew_ref.tasks):
                expected_task = crew_ref.tasks[execution_context["task_index"]]
                if expected_task != execution_context["current_task"]:
                    # We've switched to a new task
                    execution_context["current_task"] = expected_task
                    if hasattr(expected_task, 'agent') and hasattr(expected_task.agent, 'role'):
                        new_agent = expected_task.agent.role
                        old_agent = execution_context.get('current_agent', 'None')
                        execution_context["current_agent"] = new_agent
                        execution_context["last_known_agent"] = new_agent
                        logger.info(f"{log_prefix} TASK SWITCH DETECTED - old agent: {old_agent}, new agent: {new_agent}")
            
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
                
                # Special handling for AgentAction - check which agent owns the tool being used
                if is_agent_action and hasattr(step_output, 'tool'):
                    tool_name = step_output.tool
                    tool_matched = False
                    
                    logger.info(f"{log_prefix} AgentAction using tool: '{tool_name}'")
                    logger.info(f"{log_prefix} Available tool mappings: {list(execution_context['tool_to_agent'].keys())}")
                    
                    # Direct match first
                    if tool_name in execution_context["tool_to_agent"]:
                        # Found the agent that owns this tool
                        agent_name = execution_context["tool_to_agent"][tool_name]
                        execution_context["current_agent"] = agent_name
                        execution_context["last_known_agent"] = agent_name
                        tool_matched = True
                        logger.info(f"{log_prefix} TOOL MATCH: '{tool_name}' directly belongs to agent: {agent_name}")
                    
                    # If no direct match, try more flexible matching
                    if not tool_matched:
                        # Try to find tool by various matching strategies
                        for known_tool, tool_agent in execution_context["tool_to_agent"].items():
                            # Check if the tool names match (case insensitive)
                            if tool_name.lower() == known_tool.lower():
                                agent_name = tool_agent
                                execution_context["current_agent"] = agent_name
                                execution_context["last_known_agent"] = agent_name
                                tool_matched = True
                                logger.info(f"{log_prefix} Tool '{tool_name}' matched to agent: {agent_name} (case-insensitive)")
                                break
                            # Check if tool_name is contained in known_tool or vice versa
                            elif tool_name in known_tool or known_tool in tool_name:
                                agent_name = tool_agent
                                execution_context["current_agent"] = agent_name
                                execution_context["last_known_agent"] = agent_name
                                tool_matched = True
                                logger.info(f"{log_prefix} Tool '{tool_name}' matched to agent: {agent_name} (partial match with '{known_tool}')")
                                break
                            # Check if known_tool ends with tool_name (e.g., "Gmail_send_email_tool" ends with "send_email_tool")
                            elif known_tool.endswith(tool_name):
                                agent_name = tool_agent
                                execution_context["current_agent"] = agent_name
                                execution_context["last_known_agent"] = agent_name
                                tool_matched = True
                                logger.info(f"{log_prefix} Tool '{tool_name}' matched to agent: {agent_name} (suffix match with '{known_tool}')")
                                break
                            # Check if they have similar endings (e.g., "send_email_tool" vs "Gmail_send_email_tool")
                            elif tool_name.endswith(known_tool.split('_', 1)[-1] if '_' in known_tool else known_tool):
                                agent_name = tool_agent
                                execution_context["current_agent"] = agent_name
                                execution_context["last_known_agent"] = agent_name
                                tool_matched = True
                                logger.info(f"{log_prefix} Tool '{tool_name}' matched to agent: {agent_name} (partial suffix match)")
                                break
                    
                    # Log if we couldn't match the tool
                    if not tool_matched:
                        logger.warning(f"{log_prefix} TOOL NOT MATCHED: '{tool_name}' could not be matched to any agent")
                        logger.warning(f"{log_prefix} Available tool mappings:")
                        for tool, agent in execution_context['tool_to_agent'].items():
                            logger.warning(f"{log_prefix}   - '{tool}' -> '{agent}'")
                        # Fall back to current context
                        if execution_context.get('current_agent'):
                            logger.warning(f"{log_prefix} Using current agent context: {execution_context['current_agent']}")
                            agent_name = execution_context['current_agent']
                            execution_context["last_known_agent"] = agent_name
                
                # If we still don't have an agent, use other strategies
                if agent_name == "Unknown Agent":
                    # For AgentAction, we're about to use a tool, so this is the current agent acting
                    # For AgentFinish, the agent is finishing their task
                    # For ToolResult, it's the result of the current agent's tool usage
                    # For strings, it's output from an agent without tools
                    
                    # First check if we have a current task and its agent mapping
                    # This is most reliable for multi-task crews
                    if execution_context["current_task"] and execution_context["current_task"] in execution_context["task_to_agent"]:
                        agent_name = execution_context["task_to_agent"][execution_context["current_task"]]
                        execution_context["current_agent"] = agent_name
                        execution_context["last_known_agent"] = agent_name
                    elif execution_context["current_agent"]:
                        agent_name = execution_context["current_agent"]
                    elif execution_context["last_known_agent"]:
                        agent_name = execution_context["last_known_agent"]
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
            
            # If still unknown, log for debugging with more context
            if agent_name == "Unknown Agent":
                logger.warning(f"{log_prefix} Could not determine agent for {step_type}. Context: current={execution_context.get('current_agent')}, last={execution_context.get('last_known_agent')}, task_index={execution_context.get('task_index')}")
                # Log available tools for debugging
                if execution_context.get('agent_tools'):
                    for agent, tools in execution_context['agent_tools'].items():
                        logger.debug(f"{log_prefix}   Agent '{agent}' has tools: {tools}")
            
            logger.info(f"{log_prefix} FINAL: Extracted agent: '{agent_name}' from {type(step_output).__name__}")
            
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
            
            # Strategy 0: Use crew task list to find agent
            # This is the most reliable method when task_output doesn't have agent
            if crew_ref and hasattr(crew_ref, 'tasks'):
                try:
                    for idx, crew_task in enumerate(crew_ref.tasks):
                        # Match by description or object reference
                        task_matches = False
                        if hasattr(crew_task, 'description') and crew_task.description == task_description:
                            task_matches = True
                        elif task_obj and crew_task == task_obj:
                            task_matches = True
                        
                        if task_matches:
                            # Found the task that completed
                            if hasattr(crew_task, 'agent') and hasattr(crew_task.agent, 'role'):
                                agent_name = crew_task.agent.role
                                execution_context["current_agent"] = agent_name
                                execution_context["last_known_agent"] = agent_name
                                logger.info(f"{log_prefix} Identified task {idx} agent from crew: {agent_name}")
                                break
                except Exception as e:
                    logger.debug(f"{log_prefix} Error finding task in crew: {e}")
            
            # Strategy 1: Direct agent attribute on task_output
            if agent_name == "Unknown Agent" and hasattr(task_output, 'agent') and task_output.agent:
                if hasattr(task_output.agent, 'role'):
                    agent_name = task_output.agent.role
                    execution_context["current_agent"] = agent_name
                    execution_context["last_known_agent"] = agent_name
            
            # Strategy 2: Agent from task object
            if agent_name == "Unknown Agent" and hasattr(task_output, 'task') and hasattr(task_output.task, 'agent'):
                if hasattr(task_output.task.agent, 'role'):
                    agent_name = task_output.task.agent.role
                    execution_context["current_agent"] = agent_name
                    execution_context["last_known_agent"] = agent_name
            
            # Strategy 3: Use task-to-agent mapping
            if agent_name == "Unknown Agent" and task_obj and task_obj in execution_context["task_to_agent"]:
                agent_name = execution_context["task_to_agent"][task_obj]
                execution_context["current_agent"] = agent_name
                execution_context["last_known_agent"] = agent_name
            
            # Strategy 4: Use current context
            if agent_name == "Unknown Agent" and execution_context["current_agent"]:
                agent_name = execution_context["current_agent"]
            
            logger.info(f"{log_prefix} TASK COMPLETED by agent: {agent_name}, task: {task_description[:50]}")
            
            # CRITICAL FIX: After task completion, immediately prepare for the next task
            # This ensures the next agent's actions are correctly attributed
            if crew_ref and hasattr(crew_ref, 'tasks'):
                # Try multiple methods to find which task just completed
                current_task_idx = -1
                
                # Method 1: Try using task_obj if available
                if task_obj:
                    try:
                        current_task_idx = crew_ref.tasks.index(task_obj)
                        logger.info(f"{log_prefix} Found task index {current_task_idx} using task_obj")
                    except (ValueError, IndexError):
                        pass
                
                # Method 2: Match by task description
                if current_task_idx == -1:
                    for idx, crew_task in enumerate(crew_ref.tasks):
                        if hasattr(crew_task, 'description') and crew_task.description == task_description:
                            current_task_idx = idx
                            logger.info(f"{log_prefix} Found task index {current_task_idx} by description match")
                            break
                
                # Method 3: Use the task_index from context (least reliable but better than nothing)
                if current_task_idx == -1 and execution_context.get("task_index", 0) < len(crew_ref.tasks):
                    current_task_idx = execution_context.get("task_index", 0)
                    logger.info(f"{log_prefix} Using context task_index: {current_task_idx}")
                
                # Now switch to the next task's agent
                if current_task_idx >= 0:
                    next_task_idx = current_task_idx + 1
                    execution_context["task_index"] = next_task_idx
                    
                    if next_task_idx < len(crew_ref.tasks):
                        next_task = crew_ref.tasks[next_task_idx]
                        if next_task and hasattr(next_task, 'agent') and hasattr(next_task.agent, 'role'):
                            next_agent_name = next_task.agent.role
                            # IMMEDIATELY update context to the next agent
                            execution_context["current_agent"] = next_agent_name
                            execution_context["current_task"] = next_task
                            execution_context["last_known_agent"] = next_agent_name
                            logger.info(f"{log_prefix} *** CONTEXT SWITCH: Task {current_task_idx} ({agent_name}) completed → Task {next_task_idx} ({next_agent_name}) starting ***")
                        else:
                            logger.warning(f"{log_prefix} Next task exists but has no agent")
                    else:
                        logger.info(f"{log_prefix} This was the last task (index {current_task_idx})")
                else:
                    logger.error(f"{log_prefix} Could not determine which task completed")
            
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