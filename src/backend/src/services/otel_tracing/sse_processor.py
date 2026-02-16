"""KasalSSESpanProcessor — broadcasts task lifecycle spans via SSE on on_end().

Only broadcasts task_started/task_completed/task_failed to match the current
TraceManager SSE behavior. Skips SSE in CREW_SUBPROCESS_MODE=true since the
main process TraceBroadcastService handles via DB polling.
"""

import logging
import os
from datetime import datetime

from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor

logger = logging.getLogger(__name__)

# Event types that trigger SSE broadcast
_SSE_EVENT_TYPES = {"task_started", "task_completed", "task_failed"}


class KasalSSESpanProcessor(SpanProcessor):
    """Broadcasts task lifecycle OTel spans as SSE events for real-time UI.

    Only active in main-process mode. In subprocess mode, the main process
    polls new DB records via TraceBroadcastService.
    """

    def __init__(self, job_id: str):
        self._job_id = job_id
        self._is_subprocess = (
            os.environ.get("CREW_SUBPROCESS_MODE") == "true"
        )

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        pass  # No action needed on start

    def on_end(self, span: ReadableSpan) -> None:
        if self._is_subprocess:
            return  # Main process handles SSE via DB polling

        try:
            attrs = dict(span.attributes) if span.attributes else {}
            event_type = str(
                attrs.get("kasal.event_type", "")
            )

            # Also check span name mapping for auto-instrumented spans
            if not event_type or event_type not in _SSE_EVENT_TYPES:
                name = span.name or ""
                if "task" in name.lower():
                    if "complete" in name.lower():
                        event_type = "task_completed"
                    elif "fail" in name.lower():
                        event_type = "task_failed"
                    elif "execute" in name.lower() or "start" in name.lower():
                        event_type = "task_started"

            if event_type not in _SSE_EVENT_TYPES:
                return

            # Build SSE payload matching TraceManager format
            task_name = str(
                attrs.get("kasal.extra.task_name")
                or attrs.get("crewai.task.description", "")
            )
            task_id = str(attrs.get("kasal.extra.task_id", ""))
            agent_role = str(
                attrs.get("kasal.extra.agent_role")
                or attrs.get("crewai.agent.role", "")
            )
            crew_name = str(attrs.get("kasal.extra.crew_name", ""))
            frontend_task_id = str(
                attrs.get("kasal.extra.frontend_task_id", "")
            )

            sse_data = {
                "job_id": self._job_id,
                "event_type": event_type,
                "event_context": task_name,
                "trace_metadata": {
                    "task_name": task_name,
                    "task_id": task_id or None,
                    "agent_role": agent_role or None,
                    "crew_name": crew_name or None,
                    "frontend_task_id": frontend_task_id or None,
                },
                "created_at": datetime.now().isoformat(),
            }

            # Import SSE manager and broadcast
            from src.core.sse_manager import SSEEvent, sse_manager
            import asyncio

            event = SSEEvent(
                data=sse_data,
                event="trace",
                id=f"{self._job_id}_{event_type}_{datetime.now().timestamp()}",
            )

            # Best-effort async broadcast
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(
                    sse_manager.broadcast_to_job(self._job_id, event)
                )
            except RuntimeError:
                # No running loop — run synchronously in a thread
                import concurrent.futures

                def _broadcast():
                    asyncio.run(
                        sse_manager.broadcast_to_job(self._job_id, event)
                    )

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    ex.submit(_broadcast)

            logger.debug(
                f"[OTel-SSE][{self._job_id}] Broadcast {event_type} for task: {task_name[:50]}"
            )

        except Exception as e:
            logger.warning(
                f"[OTel-SSE][{self._job_id}] SSE broadcast error: {e}"
            )

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True
