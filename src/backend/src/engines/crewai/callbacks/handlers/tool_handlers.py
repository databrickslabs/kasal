"""Tool usage event handlers.

Handles ToolUsageStarted, ToolUsageFinished, and ToolUsageError events.
"""

import logging

logger = logging.getLogger(__name__)


def register_tool_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register tool usage event handlers.

    Args:
        listener: AgentTraceEventListener instance
        crewai_event_bus: CrewAI event bus to register handlers on
        log_prefix: Logging prefix string
    """
    from src.engines.crewai.callbacks.event_imports import (
        ToolUsageStartedEvent,
        ToolUsageFinishedEvent,
        ToolUsageErrorEvent,
    )

    logger.info(f"{log_prefix} Registering tool event handlers for CrewAI 0.177")

    @crewai_event_bus.on(ToolUsageStartedEvent)
    def on_tool_usage_started(source, event):
        """Handle tool usage start events."""
        try:
            agent_name = event.agent_role or (
                event.agent.role
                if event.agent and hasattr(event.agent, "role")
                else "Unknown Agent"
            )
            tool_name = event.tool_name
            tool_args = event.tool_args

            # Get task context - first from event, then from active context
            task_name = event.task_name if hasattr(event, "task_name") and event.task_name else None
            task_id = event.task_id if hasattr(event, "task_id") and event.task_id else None

            if (not task_name or not task_id) and listener.job_id in listener._active_context:
                agent_ctx = listener._active_context[listener.job_id].get(agent_name, {})
                if not task_name and agent_ctx.get("task"):
                    task_name = agent_ctx["task"]
                if not task_id and agent_ctx.get("task_id"):
                    task_id = agent_ctx["task_id"]

            logger.info(
                f"{log_prefix} Event: ToolUsageStarted | Agent: {agent_name} | Tool: {tool_name} | Task: {task_name}"
            )

            extra_data = {
                "tool_name": tool_name,
                "tool_args": str(tool_args) if tool_args else None,
                "tool_class": (
                    event.tool_class if hasattr(event, "tool_class") else None
                ),
                "agent_role": agent_name,
                "task_name": task_name,
                "task_id": task_id,
                "operation": "tool_started",
            }

            listener._enqueue_trace(
                event_source=agent_name,
                event_context=f"tool:{tool_name}",
                event_type="tool_usage",
                output_content=str(tool_args) if tool_args else "",
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_tool_usage_started: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(ToolUsageFinishedEvent)
    def on_tool_usage_finished(source, event):
        """Handle tool usage completion events."""
        try:
            agent_name = event.agent_role or (
                event.agent.role
                if event.agent and hasattr(event.agent, "role")
                else "Unknown Agent"
            )
            tool_name = event.tool_name
            tool_output = (
                str(event.output) if hasattr(event, "output") else "No output"
            )

            task_name = event.task_name if hasattr(event, "task_name") and event.task_name else None
            task_id = event.task_id if hasattr(event, "task_id") and event.task_id else None

            if (not task_name or not task_id) and listener.job_id in listener._active_context:
                agent_ctx = listener._active_context[listener.job_id].get(agent_name, {})
                if not task_name and agent_ctx.get("task"):
                    task_name = agent_ctx["task"]
                if not task_id and agent_ctx.get("task_id"):
                    task_id = agent_ctx["task_id"]

            logger.info(
                f"{log_prefix} Event: ToolUsageFinished | Agent: {agent_name} | Tool: {tool_name} | Task: {task_name}"
            )

            duration = None
            if hasattr(event, "started_at") and hasattr(event, "finished_at"):
                duration = (event.finished_at - event.started_at).total_seconds()

            extra_data = {
                "tool_name": tool_name,
                "tool_class": (
                    event.tool_class if hasattr(event, "tool_class") else None
                ),
                "agent_role": agent_name,
                "task_name": task_name,
                "task_id": task_id,
                "from_cache": (
                    event.from_cache if hasattr(event, "from_cache") else False
                ),
                "duration_seconds": duration,
                "operation": "tool_finished",
            }

            listener._enqueue_trace(
                event_source=agent_name,
                event_context=f"tool:{tool_name}",
                event_type="tool_usage",
                output_content=tool_output,
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_tool_usage_finished: {e}",
                exc_info=True,
            )

    @crewai_event_bus.on(ToolUsageErrorEvent)
    def on_tool_usage_error(source, event):
        """Handle tool usage error events."""
        try:
            agent_name = event.agent_role or (
                event.agent.role
                if event.agent and hasattr(event.agent, "role")
                else "Unknown Agent"
            )
            tool_name = event.tool_name
            error = (
                str(event.error) if hasattr(event, "error") else "Unknown error"
            )

            logger.error(
                f"{log_prefix} Event: ToolUsageError | Agent: {agent_name} | Tool: {tool_name} | Error: {error}"
            )

            extra_data = {
                "tool_name": tool_name,
                "tool_args": (
                    str(event.tool_args) if hasattr(event, "tool_args") else None
                ),
                "tool_class": (
                    event.tool_class if hasattr(event, "tool_class") else None
                ),
                "agent_role": agent_name,
                "task_name": (
                    event.task_name if hasattr(event, "task_name") else None
                ),
                "error": error,
                "operation": "tool_error",
            }

            listener._enqueue_trace(
                event_source=agent_name,
                event_context=f"tool:{tool_name}",
                event_type="tool_error",
                output_content=f"Tool error: {error}",
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_tool_usage_error: {e}", exc_info=True
            )
