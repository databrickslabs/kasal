"""
Execution-scoped callback system for CrewAI.

Provides lightweight callback functions scoped to specific executions.
Trace creation is handled by the event bus handlers (logging_callbacks.py)
and the OTel pipeline (KasalDBSpanExporter) when enabled.

Agent attribution heuristics have been removed — the OTel-instrumented event
bus carries correct agent/task context on every event, eliminating the need
for the ~350-line tool-to-agent mapping and context-switching logic that
previously lived here.
"""

import logging
from typing import Any, Dict
from datetime import datetime, timezone

from src.services.execution_logs_queue import enqueue_log
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


def create_execution_callbacks(
    job_id: str,
    config: Dict[str, Any] = None,
    group_context: GroupContext = None,
    crew: Any = None,
):
    """Create execution-scoped callback functions for a specific CrewAI execution.

    These callbacks handle execution log streaming only.  Trace creation is
    delegated to the event bus handlers (``logging_callbacks.py``) and the
    OTel pipeline (``KasalDBSpanExporter``).

    Args:
        job_id: Unique identifier for the execution.
        config: Optional configuration dictionary.
        group_context: Group context for multi-tenant isolation.
        crew: The CrewAI crew instance (kept for API compatibility).

    Returns:
        Tuple of (step_callback, task_callback) functions.
    """
    log_prefix = f"[ExecutionCallback][{job_id}]"
    logger.info(f"{log_prefix} Creating execution-scoped callbacks")

    def step_callback(step_output):
        """Called after each agent step.  Enqueues execution logs for live streaming."""
        try:
            timestamp = datetime.now(timezone.utc)

            # Extract content from step output
            if hasattr(step_output, "output"):
                content = str(step_output.output)
            elif hasattr(step_output, "raw"):
                content = str(step_output.raw)
            elif hasattr(step_output, "log"):
                content = str(step_output.log)
            else:
                content = str(step_output)

            # SECURITY: Scan tool output for injection patterns.
            # Intentionally log-only (fail-open by design) — blocking here would halt
            # live streaming on false positives.  Detection feeds into audit logs;
            # the LLM injection guardrail is the blocking layer when enabled by the user.
            try:
                from src.engines.crewai.security.scanner_pipeline import security_scanner
                _scan = security_scanner.scan(content, context=f"step_callback:{job_id}")
            except Exception as _sec_err:
                logger.debug("%s [SECURITY] Tool output scan skipped: %s", log_prefix, _sec_err)

            content_preview = content[:500] + "..." if len(content) > 500 else content
            log_message = f"[STEP] {content_preview}"

            enqueue_log(
                execution_id=job_id,
                content=log_message,
                timestamp=timestamp,
                group_context=group_context,
            )
        except Exception as e:
            logger.error(f"{log_prefix} Error in step_callback: {e}", exc_info=True)

    def task_callback(task_output):
        """Called after each task completion.  Enqueues execution logs for live streaming."""
        try:
            timestamp = datetime.now(timezone.utc)

            # Extract task description
            task_description = "Unknown Task"
            if hasattr(task_output, "description"):
                task_description = task_output.description
            elif hasattr(task_output, "task") and hasattr(
                task_output.task, "description"
            ):
                task_description = task_output.task.description

            # Extract output content
            if hasattr(task_output, "raw"):
                content = str(task_output.raw)
            elif hasattr(task_output, "output"):
                content = str(task_output.output)
            else:
                content = str(task_output)

            # SECURITY: Scan task output for injection + secret leakage.
            # Intentionally log-only (fail-open by design) — blocking here would break
            # task chaining on false positives.  Detection feeds into audit logs;
            # the LLM injection guardrail is the blocking layer when enabled by the user.
            try:
                from src.engines.crewai.security.scanner_pipeline import security_scanner
                _scan = security_scanner.scan(content, context=f"task_callback:{job_id}")
            except Exception as _sec_err:
                logger.debug("%s [SECURITY] Task output scan skipped: %s", log_prefix, _sec_err)

            task_preview = (
                task_description[:100] + "..."
                if len(task_description) > 100
                else task_description
            )
            content_preview = content[:500] + "..." if len(content) > 500 else content
            log_message = (
                f"[TASK COMPLETED] Task: {task_preview} - Output: {content_preview}"
            )

            enqueue_log(
                execution_id=job_id,
                content=log_message,
                timestamp=timestamp,
                group_context=group_context,
            )
        except Exception as e:
            logger.error(f"{log_prefix} Error in task_callback: {e}", exc_info=True)

    logger.info(f"{log_prefix} Execution-scoped callbacks created successfully")
    return step_callback, task_callback


def create_crew_callbacks(
    job_id: str,
    config: Dict[str, Any] = None,
    group_context: GroupContext = None,
):
    """Create crew-level callback functions for logging crew lifecycle events.

    Args:
        job_id: Unique identifier for the execution.
        config: Optional configuration dictionary.
        group_context: Group context for multi-tenant isolation.

    Returns:
        Dictionary of crew callback functions.
    """
    log_prefix = f"[CrewCallback][{job_id}]"

    def on_crew_start():
        """Called when crew execution starts."""
        try:
            timestamp = datetime.now(timezone.utc)
            log_message = f"[CREW STARTED] Execution {job_id} started"

            enqueue_log(
                execution_id=job_id,
                content=log_message,
                timestamp=timestamp,
                group_context=group_context,
            )
            logger.info(
                f"{log_prefix} Crew execution started (trace created by event bus)"
            )
        except Exception as e:
            logger.error(f"{log_prefix} Error in on_crew_start: {e}", exc_info=True)

    def on_crew_complete(result):
        """Called when crew execution completes."""
        try:
            timestamp = datetime.now(timezone.utc)
            result_preview = (
                str(result)[:500] + "..." if len(str(result)) > 500 else str(result)
            )
            log_message = f"[CREW COMPLETED] Execution {job_id} completed - Result: {result_preview}"

            enqueue_log(
                execution_id=job_id,
                content=log_message,
                timestamp=timestamp,
                group_context=group_context,
            )
            logger.info(
                f"{log_prefix} Crew execution completed (trace created by event bus)"
            )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error in on_crew_complete: {e}", exc_info=True
            )

    def on_crew_error(error):
        """Called when crew execution fails."""
        try:
            timestamp = datetime.now(timezone.utc)
            log_message = (
                f"[CREW FAILED] Execution {job_id} failed - Error: {str(error)}"
            )

            enqueue_log(
                execution_id=job_id,
                content=log_message,
                timestamp=timestamp,
                group_context=group_context,
            )
            logger.error(f"{log_prefix} Crew execution failed: {error}")
        except Exception as e:
            logger.error(f"{log_prefix} Error in on_crew_error: {e}", exc_info=True)

    return {
        "on_start": on_crew_start,
        "on_complete": on_crew_complete,
        "on_error": on_crew_error,
    }


def log_crew_initialization(
    job_id: str,
    config: Dict[str, Any] = None,
    group_context: GroupContext = None,
):
    """Log crew initialization with configuration details.

    Args:
        job_id: Unique identifier for the execution.
        config: Configuration dictionary.
        group_context: Group context for multi-tenant isolation.
    """
    try:
        timestamp = datetime.now(timezone.utc)

        sanitized_config = {}
        if config:
            for key, value in config.items():
                if key not in ["api_keys", "tokens", "passwords"]:
                    sanitized_config[key] = value

        log_message = f"[CREW INITIALIZED] Job {job_id} - Config: {sanitized_config}"

        enqueue_log(
            execution_id=job_id,
            content=log_message,
            timestamp=timestamp,
            group_context=group_context,
        )
        logger.info(f"[ExecutionCallback][{job_id}] Crew initialization logged")
    except Exception as e:
        logger.error(
            f"[ExecutionCallback][{job_id}] Error logging crew initialization: {e}",
            exc_info=True,
        )
