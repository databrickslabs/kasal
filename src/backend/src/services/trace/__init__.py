"""Execution traces: storing them, shaping them for a response, broadcasting them.

These four modules were four files scattered through ``src/services`` — the
service, the queue, the broadcaster and the row-shaping helpers — with nothing
but a name prefix to say they belonged together. They are one subsystem: a run
emits trace rows, they are queued and written, read back shaped for an API
response, and pushed to a live view.

The old module paths (``src.services.trace.service`` and friends) are
gone rather than shimmed: a shim that forwards forever is how two homes for one
thing become permanent.
"""

from src.services.trace.broadcast import TraceBroadcastService, trace_broadcast_service
from src.services.trace.queue import TraceQueue, get_trace_queue
from src.services.trace.row_view import mask_sensitive_data, preview_trace
from src.services.trace.service import ExecutionTraceService
from src.services.trace.writer import resolve_attribution, write_rows

__all__ = [
    "ExecutionTraceService",
    "TraceBroadcastService",
    "TraceQueue",
    "get_trace_queue",
    "mask_sensitive_data",
    "preview_trace",
    "resolve_attribution",
    "write_rows",
    "trace_broadcast_service",
]
