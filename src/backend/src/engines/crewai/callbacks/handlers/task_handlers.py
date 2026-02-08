"""Task lifecycle event handlers.

Handles TaskStarted, TaskCompleted, and TaskFailed events.
"""

import logging

logger = logging.getLogger(__name__)


def register_task_handlers(listener, crewai_event_bus, log_prefix: str) -> None:
    """Register task lifecycle event handlers.

    Args:
        listener: AgentTraceEventListener instance
        crewai_event_bus: CrewAI event bus to register handlers on
        log_prefix: Logging prefix string
    """
    from src.engines.crewai.callbacks.event_imports import (
        TaskStartedEvent,
        TaskCompletedEvent,
        TaskFailedEvent,
    )
    from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

    logger.info(f"{log_prefix} Registering task event handlers for CrewAI 0.177")

    @crewai_event_bus.on(TaskStartedEvent)
    def on_task_started(source, event):
        """Handle task start events."""
        try:
            task = event.task if hasattr(event, "task") else None
            task_name = (
                task.description
                if task and hasattr(task, "description")
                else "Unknown Task"
            )
            task_id = task.id if task and hasattr(task, "id") else None
            context = str(event.context) if hasattr(event, "context") else None

            # Extract the frontend task ID if available
            frontend_task_id = None
            if task and hasattr(task, "_kasal_task_id"):
                frontend_task_id = task._kasal_task_id
                logger.info(
                    f"{log_prefix} [DEBUG] Found _kasal_task_id on task: {frontend_task_id}"
                )
                logger.debug(
                    f"{log_prefix} Found frontend task ID: {frontend_task_id}"
                )
            else:
                logger.info(
                    f"{log_prefix} [DEBUG] No _kasal_task_id attribute on task object"
                )

            agent_name = (
                task.agent.role
                if task and hasattr(task, "agent") and task.agent
                else "System"
            )

            logger.info(
                f"{log_prefix} Event: TaskStarted | Task: {task_name[:50]} | Agent: {agent_name} | Frontend ID: {frontend_task_id}"
            )

            # Update active context so memory events can use correct task info
            listener._update_active_context(
                agent_name, task_name, log_prefix, task_id
            )

            extra_data = {
                "task_name": task_name,
                "task_id": task_id,
                "agent_role": agent_name,
                "context": context,
                "operation": "task_started",
                "agent_task": f"{agent_name} \u2192 {task_name}",
                "frontend_task_id": frontend_task_id,
            }

            active_crew = AgentTraceEventListener._active_crew_name.get(listener.job_id)
            if active_crew:
                extra_data["crew_name"] = active_crew

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="starting_task",
                event_type="task_started",
                output_content=task_name,
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_task_started: {e}", exc_info=True
            )

    @crewai_event_bus.on(TaskCompletedEvent)
    def on_task_completed(source, event):
        """Handle task completion events."""
        try:
            task = event.task if hasattr(event, "task") else None
            task_name = (
                task.description
                if task and hasattr(task, "description")
                else "Unknown Task"
            )
            task_id = task.id if task and hasattr(task, "id") else None

            # Prefer structured JSON for pydantic outputs
            output = "Task completed"
            if hasattr(event, "output") and event.output is not None:
                task_output = getattr(task, "output", None) if task else None
                pydantic_output = getattr(task_output, "pydantic", None) if task_output else None
                if pydantic_output and hasattr(pydantic_output, "model_dump_json"):
                    try:
                        output = pydantic_output.model_dump_json()
                    except Exception:
                        output = str(event.output)
                else:
                    output = str(event.output)

            frontend_task_id = None
            if task and hasattr(task, "_kasal_task_id"):
                frontend_task_id = task._kasal_task_id
                logger.info(
                    f"{log_prefix} [DEBUG] Found _kasal_task_id on task: {frontend_task_id}"
                )
                logger.debug(
                    f"{log_prefix} Found frontend task ID: {frontend_task_id}"
                )
            else:
                logger.info(
                    f"{log_prefix} [DEBUG] No _kasal_task_id attribute on task object"
                )

            agent_name = (
                task.agent.role
                if task and hasattr(task, "agent") and task.agent
                else "System"
            )

            logger.info(
                f"{log_prefix} Event: TaskCompleted | Task: {task_name[:50]} | Agent: {agent_name} | Frontend ID: {frontend_task_id}"
            )

            extra_data = {
                "task_name": task_name,
                "task_id": task_id,
                "agent_role": agent_name,
                "operation": "task_completed",
                "agent_task": f"{agent_name} \u2192 {task_name}",
                "frontend_task_id": frontend_task_id,
            }

            active_crew = AgentTraceEventListener._active_crew_name.get(listener.job_id)
            if active_crew:
                extra_data["crew_name"] = active_crew

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="completing_task",
                event_type="task_completed",
                output_content=output,
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_task_completed: {e}", exc_info=True
            )

    @crewai_event_bus.on(TaskFailedEvent)
    def on_task_failed(source, event):
        """Handle task failure events."""
        try:
            task = event.task if hasattr(event, "task") else None
            task_name = (
                task.description
                if task and hasattr(task, "description")
                else "Unknown Task"
            )
            task_id = task.id if task and hasattr(task, "id") else None
            error = (
                str(event.error) if hasattr(event, "error") else "Unknown error"
            )

            frontend_task_id = None
            if task and hasattr(task, "_kasal_task_id"):
                frontend_task_id = task._kasal_task_id
                logger.debug(
                    f"{log_prefix} Found frontend task ID: {frontend_task_id}"
                )

            agent_name = (
                task.agent.role
                if task and hasattr(task, "agent") and task.agent
                else "System"
            )

            logger.error(
                f"{log_prefix} Event: TaskFailed | Task: {task_name[:50]} | Agent: {agent_name} | Error: {error} | Frontend ID: {frontend_task_id}"
            )

            extra_data = {
                "task_name": task_name,
                "task_id": task_id,
                "agent_role": agent_name,
                "error": error,
                "operation": "task_failed",
                "agent_task": f"{agent_name} \u2192 {task_name}",
                "frontend_task_id": frontend_task_id,
            }

            active_crew = AgentTraceEventListener._active_crew_name.get(listener.job_id)
            if active_crew:
                extra_data["crew_name"] = active_crew

            listener._enqueue_trace(
                event_source=agent_name,
                event_context="task_error",
                event_type="task_failed",
                output_content=f"Task failed: {error}",
                extra_data=extra_data,
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_task_failed: {e}", exc_info=True
            )
