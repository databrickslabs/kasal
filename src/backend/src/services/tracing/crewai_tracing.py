from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager, nullcontext
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


def _get_mlflow():
    try:
        import mlflow  # type: ignore
        return mlflow
    except Exception as e:
        logger.info(f"[crewai_tracing] MLflow not available: {e}")
        return None


def configure_tracing(experiment_name: str) -> Optional[object]:
    """Configure MLflow tracking + tracing destination if MLflow is available.

    Returns the experiment object when possible; otherwise None.
    """
    mlflow = _get_mlflow()
    if not mlflow:
        return None

    try:
        # Route to Databricks when available; otherwise use current tracking URI
        try:
            mlflow.set_tracking_uri("databricks")
        except Exception:
            pass

        exp = mlflow.set_experiment(experiment_name)

        # Configure tracing destination when supported
        try:
            dest_mod = getattr(mlflow, "tracing", None)
            if dest_mod and hasattr(dest_mod, "set_destination"):
                from mlflow.tracing.destination import Databricks as _Dest  # type: ignore

                dest_mod.set_destination(_Dest(experiment_id=str(getattr(exp, "experiment_id", ""))))
                if hasattr(dest_mod, "enable"):
                    dest_mod.enable()
        except Exception as e:
            logger.info(f"[crewai_tracing] Could not set tracing destination: {e}")

        # Enable async logging when supported
        try:
            mlflow.config.enable_async_logging()
        except Exception:
            pass

        return exp
    except Exception as e:
        logger.info(f"[crewai_tracing] configure_tracing failed: {e}")
        return None


def enable_autologs(*, global_autolog: bool = True, global_log_traces: bool = True,
                    crewai_autolog: bool = True, litellm_spans_only: bool = True) -> None:
    """Enable MLflow autolog integrations with configurable root-trace behavior.

    - global_autolog + global_log_traces controls mlflow.autolog(log_traces=...)
    - crewai_autolog toggles mlflow.crewai.autolog()
    - litellm_spans_only enables mlflow.litellm.autolog(log_traces=False)
    """
    mlflow = _get_mlflow()
    if not mlflow:
        return

    # Global autolog (may create root traces when global_log_traces=True)
    if global_autolog:
        try:
            mlflow.autolog(log_traces=bool(global_log_traces), disable=False, silent=True)
            logger.info(
                f"[crewai_tracing] Global autolog enabled (log_traces={bool(global_log_traces)})"
            )
        except Exception as e:
            logger.info(f"[crewai_tracing] Global autolog not available: {e}")

    # LiteLLM autolog with spans only (no root traces)
    if litellm_spans_only:
        try:
            if hasattr(mlflow, "litellm"):
                mlflow.litellm.autolog(log_traces=False, disable=False, silent=False)  # type: ignore[attr-defined]
                logger.info("[crewai_tracing] LiteLLM autolog enabled (spans only)")
        except Exception as e:
            logger.info(f"[crewai_tracing] LiteLLM autolog not available: {e}")

    # CrewAI autolog (this typically creates root traces around CrewAI flows)
    if crewai_autolog:
        try:
            if hasattr(mlflow, "crewai"):
                mlflow.crewai.autolog()  # type: ignore[attr-defined]
                logger.info("[crewai_tracing] CrewAI autolog enabled")
        except Exception as e:
            logger.info(f"[crewai_tracing] CrewAI autolog not available: {e}")


@contextmanager
def start_root_trace(trace_name: str, inputs: Optional[Dict[str, Any]] = None):
    """Start a root trace using fluent API if available; otherwise no-op context.

    Returns the context manager yielding the root_trace-like object when possible.
    """
    mlflow = _get_mlflow()
    if not mlflow:
        yield None
        return

    inputs = inputs or {}

    # Prefer mlflow.start_trace, fallback to mlflow.tracing.start_trace
    start_trace_fn = getattr(mlflow, "start_trace", None)
    if not callable(start_trace_fn):
        tracing_mod = getattr(mlflow, "tracing", None)
        start_trace_fn = getattr(tracing_mod, "start_trace", None)

    if callable(start_trace_fn):
        try:
            with start_trace_fn(name=trace_name, inputs=inputs) as rt:  # type: ignore[misc]
                yield rt
                return
        except Exception as e:
            logger.info(f"[crewai_tracing] start_root_trace failed, continuing without root: {e}")

    # Fallback to nullcontext if start_trace is not available
    with nullcontext() as _nc:
        yield _nc


def get_last_active_trace_id() -> Optional[str]:
    mlflow = _get_mlflow()
    if not mlflow:
        return None
    try:
        get_last = getattr(getattr(mlflow, "tracing", None), "get_last_active_trace_id", None)
        if callable(get_last):
            return get_last()
        alt = getattr(mlflow, "get_last_active_trace_id", None)
        if callable(alt):
            return alt()
    except Exception:
        return None
    return None


async def update_execution_trace_id(execution_id: str, trace_id: Optional[str], experiment_name: str, group_id: Optional[str]):
    if not trace_id:
        return
    try:
        from src.services.execution_status_service import ExecutionStatusService
        await ExecutionStatusService.update_mlflow_trace_id(
            job_id=execution_id,
            trace_id=trace_id,
            experiment_name=experiment_name,
        )
        logger.info(f"[crewai_tracing] Updated execution {execution_id} with trace {trace_id}")
    except Exception as e:
        logger.info(f"[crewai_tracing] Could not update execution with trace id: {e}")


async def flush_and_stop_writers(async_logger: Optional[logging.Logger] = None) -> None:
    """Flush MLflow async logging and stop TraceManager writer if present."""
    mlflow = _get_mlflow()
    alog = async_logger or logger

    # Flush MLflow async traces
    try:
        if mlflow and hasattr(mlflow, "flush_trace_async_logging"):
            mlflow.flush_trace_async_logging()
    except Exception as e:
        alog.warning(f"[crewai_tracing] Error flushing MLflow async logging: {e}")

    # Drain any custom trace queue and stop TraceManager if available
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


def cleanup_async_db_connections(async_logger: Optional[logging.Logger] = None) -> None:
    """Dispose AsyncEngine via sync_engine before event loop is closed to avoid loop errors."""
    alog = async_logger or logger
    try:
        from sqlalchemy.ext.asyncio import AsyncEngine
        import gc

        for obj in gc.get_objects():
            if isinstance(obj, AsyncEngine):
                try:
                    obj.sync_engine.dispose()
                except Exception as e:
                    alog.warning(f"[crewai_tracing] Error disposing AsyncEngine: {e}")
        gc.collect()
    except Exception as e:
        alog.warning(f"[crewai_tracing] DB cleanup encountered an error: {e}")

