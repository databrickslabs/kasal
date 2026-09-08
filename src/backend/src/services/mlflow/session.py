"""Conversation identity shared by native and event-exported MLflow traces."""

import logging

logger = logging.getLogger(__name__)


def session_id_from(config, inputs=None):
    value = config.get("session_id") or (inputs or {}).get("session_id")
    value = value or (config.get("inputs") or {}).get("session_id")
    return value if isinstance(value, str) and value.strip() else None


def tag_session(span, session_id, user=None):
    """Update trace metadata while the trace is live, before either exporter flushes.

    The client API used by the event exporter does not activate its root span.
    Bind it temporarily; the context manager restores the caller's active trace.
    """
    if span is None or not session_id:
        return
    try:
        import mlflow
        from mlflow.tracing.provider import safe_set_span_in_context

        metadata = {"mlflow.trace.session": str(session_id)}
        if user:
            metadata["mlflow.trace.user"] = str(user)
        with safe_set_span_in_context(span):
            mlflow.update_current_trace(metadata=metadata)
    except Exception:
        logger.warning(
            "Could not associate the MLflow trace with its session", exc_info=True
        )
