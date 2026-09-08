"""An MLflow trace for a builder turn, alongside its durable activity trace."""

import logging
from contextlib import asynccontextmanager

from src.services.mlflow.mlflow_parent_setup import configure_parent_mlflow_tracing
from src.services.mlflow.session import tag_session
from src.services.mlflow.tracing import start_root_trace

logger = logging.getLogger(__name__)


@asynccontextmanager
async def builder_mlflow_trace(session, mode, request, group_context, job_id):
    enabled = await configure_parent_mlflow_tracing(
        session, group_context, label="Builder"
    )
    if not enabled:
        yield None
        return
    inputs = {
        "prompt": request.prompt if mode == "flow" else request.message,
        "model": request.model,
        "session_id": request.session_id,
        "job_id": job_id,
    }
    with start_root_trace(f"{mode}_plan", inputs) as span:
        tag_session(
            span, request.session_id, getattr(group_context, "group_email", None)
        )
        try:
            yield span
        finally:
            # Capture THIS root, never the process-global 'last active trace'
            # which could belong to a concurrent conversation.
            trace_id = getattr(span, "trace_id", None)
            if isinstance(trace_id, str) and trace_id:
                from src.services.execution.status import ExecutionStatusService

                try:
                    await ExecutionStatusService.update_mlflow_trace_id(
                        job_id, trace_id, session=session
                    )
                except Exception:
                    logger.warning(
                        "Could not save the builder MLflow trace link", exc_info=True
                    )
