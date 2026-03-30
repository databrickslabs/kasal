"""Event listener shell for CrewAI engine.

All event subscriptions and trace DB writes are handled by the OTel pipeline
(OTelEventBridge → CrewAIInstrumentor → KasalDBSpanExporter → DB).

This module retains the AgentTraceEventListener class as a thin shell because
it is referenced by execution_runner, process_crew_executor, process_flow_executor,
and callback_manager. The setup_listeners() method is intentionally a no-op —
the OTel bridge registers its own handlers directly from crewai.events.
"""

import logging
from datetime import datetime, timezone

from crewai.events import BaseEventListener

logger = logging.getLogger(__name__)


class AgentTraceEventListener(BaseEventListener):
    """Thin shell — OTel bridge handles all event subscriptions.

    Kept for backward compatibility with callers that instantiate it and
    call setup_listeners(). Both are no-ops; real work is done by
    OTelEventBridge in src/services/otel_tracing/event_bridge.py.
    """

    def __init__(
        self,
        job_id: str,
        group_context=None,
        register_global_events=False,
        task_event_queue=None,
    ):
        if not job_id or not isinstance(job_id, str):
            raise ValueError("job_id must be a non-empty string")

        self.job_id = job_id
        self.group_context = group_context
        self._task_event_queue = task_event_queue
        self._init_time = datetime.now(timezone.utc)

        logger.info(
            f"[AgentTraceEventListener][{self.job_id}] Initialized "
            f"(OTel bridge handles event subscriptions)"
        )

    def setup_listeners(self, crewai_event_bus):
        """No-op — OTel bridge registers all event handlers directly."""
        logger.info(
            f"[AgentTraceEventListener][{self.job_id}] "
            f"setup_listeners called (no-op, OTel bridge handles events)"
        )


class TaskCompletionEventListener(BaseEventListener):
    """Deprecated — task completion handled by OTel bridge."""

    def __init__(self, job_id: str, group_context=None):
        self.job_id = job_id
        self.group_context = group_context

    def setup_listeners(self, crewai_event_bus):
        """No-op."""
        pass
