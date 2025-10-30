"""CrewAI-specific MLflow integration.

This module provides MLflow autolog configuration and tracing setup specifically
for CrewAI workflows. It handles CrewAI autolog, LiteLLM autolog, and crew-specific
trace management.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional


logger = logging.getLogger(__name__)


def _get_mlflow():
    """Get mlflow module if available."""
    try:
        import mlflow  # type: ignore
        return mlflow
    except Exception as e:
        logger.info(f"[crewai_mlflow] MLflow not available: {e}")
        return None


def enable_autologs(
    *,
    global_autolog: bool = True,
    global_log_traces: bool = True,
    crewai_autolog: bool = True,
    litellm_spans_only: bool = True
) -> None:
    """Enable MLflow autolog integrations with configurable root-trace behavior.

    This is CrewAI-specific configuration that enables autologging for CrewAI
    workflows and LiteLLM calls.

    Args:
        global_autolog: Enable global MLflow autolog
        global_log_traces: Whether global autolog creates root traces
        crewai_autolog: Enable CrewAI-specific autolog
        litellm_spans_only: Enable LiteLLM autolog (spans only, no root traces)
    """
    mlflow = _get_mlflow()
    if not mlflow:
        return

    # Global autolog (may create root traces when global_log_traces=True)
    if global_autolog:
        try:
            mlflow.autolog(log_traces=bool(global_log_traces), disable=False, silent=True)
            logger.info(
                f"[crewai_mlflow] Global autolog enabled (log_traces={bool(global_log_traces)})"
            )
        except Exception as e:
            logger.info(f"[crewai_mlflow] Global autolog not available: {e}")

    # LiteLLM autolog with spans only (no root traces)
    if litellm_spans_only:
        try:
            if hasattr(mlflow, "litellm"):
                mlflow.litellm.autolog(log_traces=False, disable=False, silent=False)  # type: ignore[attr-defined]
                logger.info("[crewai_mlflow] LiteLLM autolog enabled (spans only)")
        except Exception as e:
            logger.info(f"[crewai_mlflow] LiteLLM autolog not available: {e}")

    # CrewAI autolog (this typically creates root traces around CrewAI flows)
    if crewai_autolog:
        try:
            if hasattr(mlflow, "crewai"):
                mlflow.crewai.autolog()  # type: ignore[attr-defined]
                logger.info("[crewai_mlflow] CrewAI autolog enabled")
        except Exception as e:
            logger.info(f"[crewai_mlflow] CrewAI autolog not available: {e}")


async def update_execution_trace_id(
    execution_id: str,
    trace_id: Optional[str],
    experiment_name: str,
    group_id: Optional[str]
) -> None:
    """Update execution record with MLflow trace ID.

    This is CrewAI-specific as it updates the execution history with trace information.

    Args:
        execution_id: The execution ID to update
        trace_id: The MLflow trace ID
        experiment_name: The MLflow experiment name
        group_id: Optional group ID for multi-tenant isolation (currently unused)
    """
    if not trace_id:
        return
    try:
        from src.services.execution_status_service import ExecutionStatusService
        await ExecutionStatusService.update_mlflow_trace_id(
            job_id=execution_id,
            trace_id=trace_id,
            experiment_name=experiment_name,
        )
        logger.info(f"[crewai_mlflow] Updated execution {execution_id} with trace {trace_id}")
    except Exception as e:
        logger.info(f"[crewai_mlflow] Could not update execution with trace id: {e}")


async def flush_and_stop_writers(async_logger: Optional[logging.Logger] = None) -> None:
    """Flush MLflow async logging and stop TraceManager writer if present.

    This is CrewAI-specific as it also handles the custom TraceManager used by CrewAI.

    Args:
        async_logger: Optional logger for debug output
    """
    alog = async_logger or logger

    # Flush MLflow async traces (use generic service)
    from src.services.mlflow_tracing_service import flush_async_logging
    await flush_async_logging(async_logger=alog)

    # Drain any custom trace queue and stop TraceManager if available (CrewAI-specific)
    try:
        from src.services.trace_queue import get_trace_queue
        trace_queue = get_trace_queue()
        max_wait = 10
        waited = 0.0
        while trace_queue.qsize() > 0 and waited < max_wait:
            await asyncio.sleep(0.1)
            waited += 0.1
        try:
            from src.engines.crewai.trace_management import TraceManager
            await TraceManager.stop_writer()
        except Exception:
            pass
    except Exception:
        pass
