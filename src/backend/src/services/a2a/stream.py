"""A2A streaming — SendStreamingMessage and SubscribeToTask.

The frames come from ``services/external/streaming.stream_run`` — the SAME
generator the MCP surface streams. Only the encoding differs, which is the whole
point of the shared layer: a change to when a run reports progress lands on both
protocols at once, and neither adapter owns polling logic.

What this module adds is A2A's event vocabulary. The spec does not stream raw
state; it streams ``TaskStatusUpdateEvent`` and ``TaskArtifactUpdateEvent``, each
as its own named SSE event, ending with one whose ``final`` is true. A client
that sees ``final`` closes the connection instead of holding it open on a task
that will never speak again.
"""

import json
import logging
from typing import Any, AsyncIterator

from src.services.a2a.render import to_stream_events
from src.services.external.identity import ExternalCaller
from src.services.external.permissions import RUN_ROLES, require_role
from src.services.external.streaming import stream_run

logger = logging.getLogger(__name__)


async def stream_task(
    caller: ExternalCaller, task_id: str, session: Any = None
) -> AsyncIterator[bytes]:
    """SSE bytes for a task's lifetime.

    Authorisation happens HERE rather than in the router, because a streaming
    response's status line is sent before the body: raising inside the generator
    would produce a 200 with an error in it, so the check has to run before the
    first yield. Callers await the first chunk, which is why this is a generator
    that validates eagerly on entry.
    """
    require_role(caller, RUN_ROLES)

    async for frame in stream_run(caller, task_id, session=session):
        for event in to_stream_events(frame):
            yield _encode(event.kind, event.model_dump(exclude_none=True))


def _encode(event_name: str, payload: dict) -> bytes:
    """One SSE event.

    Serialised on a single line: a bare newline inside ``data:`` terminates the
    event, so a pretty-printed payload would silently truncate every message.
    """
    body = json.dumps(payload, default=str, separators=(",", ":"))
    return f"event: {event_name}\ndata: {body}\n\n".encode("utf-8")
