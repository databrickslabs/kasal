"""MLflow tracing utilities service.

This module provides generic MLflow tracing utilities that can be used by any part
of the application (dispatcher, crew executor, etc.). It handles trace lifecycle,
cleanup, and utility functions independent of any specific engine.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager, nullcontext
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


def _get_mlflow():
    """Get mlflow module if available."""
    try:
        import mlflow  # type: ignore
        return mlflow
    except Exception as e:
        logger.info(f"[mlflow_tracing] MLflow not available: {e}")
        return None


@contextmanager
def start_root_trace(trace_name: str, inputs: Optional[Dict[str, Any]] = None):
    """Start a root trace using fluent API if available; otherwise no-op context.

    In MLflow 3.x, mlflow.start_span() without a parent creates a root trace.
    Returns the context manager yielding the LiveSpan object when possible.

    Args:
        trace_name: Name for the trace
        inputs: Optional input dictionary for the trace

    Yields:
        Trace/span object or None if MLflow unavailable
    """
    mlflow = _get_mlflow()
    if not mlflow:
        yield None
        return

    inputs = inputs or {}

    # Try multiple approaches for creating a root trace:
    # 1. mlflow.start_trace (MLflow 2.14+)
    # 2. mlflow.tracing.start_trace (alternative location)
    # 3. mlflow.start_span with no parent (MLflow 3.x - creates root trace)
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
            logger.info(f"[mlflow_tracing] start_trace failed, trying start_span: {e}")

    # In MLflow 3.x, start_span without parent creates a root trace
    start_span_fn = getattr(mlflow, "start_span", None)
    if callable(start_span_fn):
        try:
            # start_span uses 'attributes' parameter, not 'inputs'
            with start_span_fn(name=trace_name, span_type="CHAIN", attributes=inputs) as span:  # type: ignore[misc]
                # Explicitly set inputs on the span if method exists
                # This ensures inputs appear in the MLflow UI
                if inputs and hasattr(span, 'set_inputs'):
                    try:
                        span.set_inputs(inputs)
                    except Exception as input_e:
                        logger.debug(f"[mlflow_tracing] Could not set span inputs: {input_e}")
                yield span
                return
        except Exception as e:
            logger.info(f"[mlflow_tracing] start_span failed, continuing without root: {e}")

    # Fallback to nullcontext if no trace API is available
    with nullcontext() as _nc:
        yield _nc


def get_last_active_trace_id() -> Optional[str]:
    """Get the ID of the last active MLflow trace.

    Returns:
        Trace ID string or None if unavailable
    """
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


async def flush_async_logging(async_logger: Optional[logging.Logger] = None) -> None:
    """Flush MLflow async logging if enabled.

    This should be called after MLflow operations to ensure traces are written.

    Args:
        async_logger: Optional logger for debug output
    """
    mlflow = _get_mlflow()
    alog = async_logger or logger

    try:
        alog.info(f"[mlflow_tracing] [DEBUG] Starting MLflow flush operation")
        if mlflow and hasattr(mlflow, "flush_trace_async_logging"):
            alog.info(f"[mlflow_tracing] [DEBUG] About to call mlflow.flush_trace_async_logging()")
            mlflow.flush_trace_async_logging()
            alog.info(f"[mlflow_tracing] [DEBUG] MLflow flush_trace_async_logging() completed successfully")
        else:
            alog.info(f"[mlflow_tracing] [DEBUG] MLflow not available or flush_trace_async_logging not found")
    except Exception as e:
        alog.warning(f"[mlflow_tracing] [DEBUG] Error flushing MLflow async logging: {e}")
        alog.warning(f"[mlflow_tracing] Error flushing MLflow async logging: {e}")


def cleanup_async_db_connections(async_logger: Optional[logging.Logger] = None) -> None:
    """Dispose AsyncEngine via sync_engine before event loop is closed to avoid loop errors.

    This is a utility function to help clean up database connections that might
    interfere with async event loops, particularly useful in subprocess contexts.

    Args:
        async_logger: Optional logger for debug output
    """
    alog = async_logger or logger
    try:
        from sqlalchemy.ext.asyncio import AsyncEngine
        import gc

        for obj in gc.get_objects():
            if isinstance(obj, AsyncEngine):
                try:
                    obj.sync_engine.dispose()
                except Exception as e:
                    alog.warning(f"[mlflow_tracing] Error disposing AsyncEngine: {e}")
        gc.collect()
    except Exception as e:
        alog.warning(f"[mlflow_tracing] DB cleanup encountered an error: {e}")
