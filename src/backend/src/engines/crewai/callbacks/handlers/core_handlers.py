"""Core event handlers: agent execution, crew kickoff, and flow events.

Handles AgentExecutionStarted/Completed, CrewKickoffStarted/Completed,
and FlowStarted/Finished events.
"""

import logging
import os
from datetime import datetime, timezone

from src.engines.crewai.callbacks.event_imports import (
    AgentExecutionStartedEvent,
    AgentExecutionCompletedEvent,
    CrewKickoffStartedEvent,
    CrewKickoffCompletedEvent,
    FLOW_EVENTS_AVAILABLE,
)
from src.services.execution_logs_queue import enqueue_log

logger = logging.getLogger(__name__)


def register_core_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register agent execution, crew kickoff, and flow event handlers.

    Args:
        listener: AgentTraceEventListener instance
        crewai_event_bus: CrewAI event bus to register handlers on
        log_prefix: Logging prefix string
    """
    _register_agent_handlers(listener, crewai_event_bus, log_prefix)
    _register_crew_handlers(listener, crewai_event_bus, log_prefix)
    if FLOW_EVENTS_AVAILABLE:
        _register_flow_handlers(listener, crewai_event_bus, log_prefix)


def _register_agent_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register agent execution started/completed handlers."""

    @crewai_event_bus.on(AgentExecutionStartedEvent)
    def on_agent_execution_started(source, event):
        """Handle agent execution start events to capture LLM request."""
        logger.debug(
            f"[TRACE_DEBUG] AgentExecutionStartedEvent received for job {listener.job_id}"
        )
        try:
            agent_name, agent_id = listener._extract_agent_info(event)
            task_name, task_id, task_description = listener._extract_task_info(event)

            task_prompt = (
                event.task_prompt if hasattr(event, "task_prompt") else None
            )

            if task_prompt:
                agent = event.agent if hasattr(event, "agent") else None
                complete_prompt_parts = []

                if agent:
                    system_prompt_parts = []
                    if hasattr(agent, "role") and agent.role:
                        system_prompt_parts.append(f"Role: {agent.role}")
                    if hasattr(agent, "goal") and agent.goal:
                        system_prompt_parts.append(f"Goal: {agent.goal}")
                    if hasattr(agent, "backstory") and agent.backstory:
                        system_prompt_parts.append(f"Backstory: {agent.backstory}")

                    if system_prompt_parts:
                        system_prompt = "\n\n".join(system_prompt_parts)
                        complete_prompt_parts.append(
                            "=== SYSTEM PROMPT (Agent Instructions) ===\n"
                            + system_prompt
                        )

                complete_prompt_parts.append(
                    "=== TASK PROMPT (User Request) ===\n" + task_prompt
                )
                complete_prompt = "\n\n".join(complete_prompt_parts)

                logger.info(
                    f"{log_prefix} Event: LLM Request | Agent: {agent_name} | Complete prompt length: {len(complete_prompt)} chars (system: {len(complete_prompt_parts[0]) if len(complete_prompt_parts) > 1 else 0}, task: {len(task_prompt)})"
                )

                extra_data = {
                    "agent_role": agent_name,
                    "agent_id": agent_id,
                    "task": task_name,
                    "task_id": task_id,
                    "task_description": task_description,
                    "prompt_length": len(complete_prompt),
                    "system_prompt_length": (
                        len(complete_prompt_parts[0])
                        if len(complete_prompt_parts) > 1
                        else 0
                    ),
                    "task_prompt_length": len(task_prompt),
                    "timestamp": (
                        event.timestamp.isoformat()
                        if hasattr(event, "timestamp") and event.timestamp
                        else None
                    ),
                }

                active_crew = listener._active_crew_name.get(listener.job_id)
                if active_crew:
                    extra_data["crew_name"] = active_crew

                listener._enqueue_trace(
                    event_source=agent_name,
                    event_context="llm_request",
                    event_type="llm_request",
                    output_content=complete_prompt,
                    extra_data=extra_data,
                )

                logger.info(
                    f"{log_prefix} LLM Request trace enqueued with complete prompt"
                )

        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_agent_execution_started: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(AgentExecutionCompletedEvent)
    def on_agent_execution_completed(source, event):
        """Handle agent execution completion events.

        NOTE: This event fires for every agent step, including tool usage.
        We skip creating traces for tool-related steps since we already have
        dedicated ToolExecutionStarted/Finished event handlers.
        """
        logger.debug(
            f"[TRACE_DEBUG] *** HANDLER CALLED *** AgentExecutionCompletedEvent received for job {listener.job_id}!"
        )
        logger.info(
            f"{log_prefix} *** HANDLER CALLED *** AgentExecutionCompletedEvent received!"
        )
        try:
            agent_name, agent_id = listener._extract_agent_info(event)
            logger.debug(
                f"{log_prefix} Extracted agent: {agent_name} (ID: {agent_id})"
            )

            task_name, task_id, task_description = listener._extract_task_info(event)

            # Get output - prefer structured JSON for planning outputs
            output_content = ""
            if event.output is not None:
                task_output = getattr(event.task, "output", None)
                pydantic_output = getattr(task_output, "pydantic", None) if task_output else None
                if pydantic_output and hasattr(pydantic_output, "model_dump_json"):
                    try:
                        output_content = pydantic_output.model_dump_json()
                    except Exception:
                        output_content = str(event.output)
                else:
                    output_content = str(event.output)

            # Check for duplicate agent_execution events
            task_key = task_id or task_name
            output_hash = hash(output_content[:500])
            execution_key = (agent_name, task_key, output_hash)

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            if listener.job_id not in AgentTraceEventListener._processed_agent_executions:
                AgentTraceEventListener._processed_agent_executions[listener.job_id] = set()

            if execution_key in AgentTraceEventListener._processed_agent_executions[listener.job_id]:
                logger.debug(
                    f"{log_prefix} Skipping duplicate agent_execution - already processed: {agent_name} -> {task_name}"
                )
                return

            AgentTraceEventListener._processed_agent_executions[listener.job_id].add(
                execution_key
            )

            # Skip if this is a tool-related step
            tool_patterns = [
                "ToolResult(",
                "Tool: ",
                "Using tool:",
                "Action:",
                "Tool Output:",
                "Action Output:",
            ]

            if any(pattern in output_content for pattern in tool_patterns):
                logger.debug(
                    f"{log_prefix} Skipping agent_execution - tool event already handled by dedicated handler"
                )
                return

            # Enhanced detection for different types of agent executions
            event_type = "agent_execution"
            event_context = task_name
            tool_info = None

            if (
                "Final Answer:" in output_content
                or "Task Complete:" in output_content
            ):
                event_type = "task_completed"
                event_context = "task_completion"
            elif any(
                pattern in output_content
                for pattern in [
                    "Thought:",
                    "Thinking:",
                    "Reasoning:",
                    "Analysis:",
                    "Plan:",
                ]
            ):
                event_type = "agent_reasoning"
                event_context = "reasoning"
            else:
                event_type = "llm_response"
                event_context = "llm_response"

            logger.info(
                f"{log_prefix} Event: {event_type} | Agent: {agent_name} | Task: {task_name[:50]}..."
            )

            listener._update_active_context(agent_name, task_name, log_prefix, task_id)

            full_extra_data = {
                "agent_role": agent_name,
                "agent_id": agent_id,
                "task": task_name,
                "task_id": task_id,
                "task_description": task_description,
                "timestamp": (
                    event.timestamp.isoformat()
                    if hasattr(event, "timestamp") and event.timestamp
                    else None
                ),
            }

            active_crew = listener._active_crew_name.get(listener.job_id)
            if active_crew:
                full_extra_data["crew_name"] = active_crew

            if tool_info:
                full_extra_data.update(tool_info)

            if event_type == "llm_response":
                full_extra_data["output_length"] = len(output_content)

            if hasattr(event, "task") and hasattr(event.task, "markdown"):
                full_extra_data["markdown"] = event.task.markdown

            if event_type == "task_completed":
                listener._handle_task_completion(task_name, task_id, log_prefix)

            # Track that this task has an execution completion trace
            if event_type in ("llm_response", "agent_execution", "task_completed"):
                if listener.job_id not in AgentTraceEventListener._tasks_with_execution_trace:
                    AgentTraceEventListener._tasks_with_execution_trace[listener.job_id] = set()
                task_key = task_id or task_name
                AgentTraceEventListener._tasks_with_execution_trace[listener.job_id].add(task_key)
                logger.debug(
                    f"{log_prefix} Marked task {task_key} as having execution trace (type: {event_type})"
                )

            listener._enqueue_trace(
                event_source=agent_name,
                event_context=event_context,
                event_type=event_type,
                output_content=output_content,
                extra_data=full_extra_data,
            )

            logger.info(
                f"{log_prefix} *** HANDLER SUCCESS *** Event processed and enqueued"
            )

        except Exception as e:
            logger.error(
                f"{log_prefix} *** HANDLER ERROR *** Error in on_agent_execution_completed: {e}",
                exc_info=True,
            )


def _register_crew_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register crew kickoff started/completed handlers."""

    @crewai_event_bus.on(CrewKickoffStartedEvent)
    def on_crew_kickoff_started(source, event):
        """Handle crew kickoff start events."""
        try:
            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            crew_name = event.crew_name if hasattr(event, "crew_name") else "crew"
            inputs = event.inputs if hasattr(event, "inputs") else {}

            logger.info(
                f"{log_prefix} Event: CrewKickoffStarted | Crew: {crew_name}"
            )

            AgentTraceEventListener._task_registry[listener.job_id] = {}
            AgentTraceEventListener._task_start_times[listener.job_id] = {}
            AgentTraceEventListener._active_crew_name[listener.job_id] = crew_name
            logger.debug(f"{log_prefix} Stored active crew name: {crew_name}")

            is_flow_mode = (
                os.environ.get("FLOW_SUBPROCESS_MODE", "false").lower() == "true"
            )

            if not is_flow_mode:
                listener._enqueue_trace(
                    event_source="crew",
                    event_context=crew_name,
                    event_type="crew_started",
                    output_content=f"Crew '{crew_name}' execution started",
                    extra_data={"inputs": inputs} if inputs else None,
                )
            else:
                logger.debug(
                    f"{log_prefix} Suppressing crew_started trace in flow mode for crew: {crew_name}"
                )

        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_crew_kickoff_started: {e}", exc_info=True
            )

    @crewai_event_bus.on(CrewKickoffCompletedEvent)
    def on_crew_kickoff_completed(source, event):
        """Handle crew kickoff completion events."""
        try:
            crew_name = event.crew_name if hasattr(event, "crew_name") else "crew"
            output_content = (
                str(event.output) if hasattr(event, "output") else "Crew completed"
            )
            total_tokens = (
                event.total_tokens if hasattr(event, "total_tokens") else None
            )

            logger.info(
                f"{log_prefix} Event: CrewKickoffCompleted | Crew: {crew_name}"
            )

            extra_data = {}
            if total_tokens is not None:
                extra_data["total_tokens"] = total_tokens

            if listener._init_time:
                execution_time = (
                    datetime.now(timezone.utc) - listener._init_time
                ).total_seconds()
                extra_data["execution_time_seconds"] = execution_time

            if extra_data:
                logger.info(
                    f"{log_prefix} Crew Completed - Total Tokens: {extra_data.get('total_tokens', 'N/A')}, Execution Time: {extra_data.get('execution_time_seconds', 'N/A')} seconds"
                )
                enqueue_log(
                    execution_id=listener.job_id,
                    content=f"Crew Statistics: Total Tokens: {extra_data.get('total_tokens', 'N/A')}, Execution Time: {extra_data.get('execution_time_seconds', 'N/A')} seconds",
                    group_context=listener.group_context,
                )

            is_flow_mode = (
                os.environ.get("FLOW_SUBPROCESS_MODE", "false").lower() == "true"
            )

            if not is_flow_mode:
                listener._enqueue_trace(
                    event_source="crew",
                    event_context=crew_name,
                    event_type="crew_completed",
                    output_content=output_content,
                    extra_data=extra_data if extra_data else None,
                )
            else:
                logger.debug(
                    f"{log_prefix} Suppressing crew_completed trace in flow mode for crew: {crew_name}"
                )

            listener._cleanup_job_tracking()

        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_crew_kickoff_completed: {e}",
                exc_info=True,
            )


def _register_flow_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register flow started/finished handlers."""
    from src.engines.crewai.callbacks.event_imports import (
        FlowStartedEvent,
        FlowFinishedEvent,
    )

    @crewai_event_bus.on(FlowStartedEvent)
    def on_flow_started(source, event):
        """Handle flow start events."""
        try:
            flow_name = (
                event.flow_name if hasattr(event, "flow_name") else "flow"
            )
            inputs = event.inputs if hasattr(event, "inputs") else {}

            logger.info(f"{log_prefix} Event: FlowStarted | Flow: {flow_name}")

            listener._enqueue_trace(
                event_source="flow",
                event_context=flow_name,
                event_type="flow_started",
                output_content=f"Flow '{flow_name}' execution started",
                extra_data={"inputs": inputs} if inputs else None,
            )

        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_flow_started: {e}", exc_info=True
            )

    @crewai_event_bus.on(FlowFinishedEvent)
    def on_flow_finished(source, event):
        """Handle flow completion events."""
        try:
            flow_name = (
                event.flow_name if hasattr(event, "flow_name") else "flow"
            )
            result = event.result if hasattr(event, "result") else None
            output_content = str(result) if result else "Flow completed"

            logger.info(f"{log_prefix} Event: FlowFinished | Flow: {flow_name}")

            extra_data = {}
            if listener._init_time:
                execution_time = (
                    datetime.now(timezone.utc) - listener._init_time
                ).total_seconds()
                extra_data["execution_time_seconds"] = execution_time
                logger.info(
                    f"{log_prefix} Flow Completed - Execution Time: {execution_time} seconds"
                )

            listener._enqueue_trace(
                event_source="flow",
                event_context=flow_name,
                event_type="flow_completed",
                output_content=output_content,
                extra_data=extra_data if extra_data else None,
            )

        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_flow_finished: {e}", exc_info=True
            )
