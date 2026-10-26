"""How a trace row is shaped before it leaves the API.

Two transforms, one home. Both answer "what does the caller actually get to
see", and both used to live somewhere else: masking was a private helper inside
the 812-line service, previewing did not exist.

* **Masking** removes credentials. A trace row records whatever an agent said
  and whatever a tool returned, which is exactly where a token ends up.
* **Previewing** trims the long text for LIST responses. A run's rows carry
  everything that was said — prompts with the whole A2UI component catalog in
  them, tool results, composed surfaces — and the timeline draws one-line
  labels from them. Shipping it all means the browser downloads, and then
  HOLDS, a run's entire transcript to render a list; a session that opens a
  dozen runs keeps every one alive.

So the list is previews and the detail is whole: rows come back trimmed with
their true sizes recorded, and opening one fetches ``GET /traces/{id}``, which
is never trimmed. Nothing is lost — it is fetched when it is looked at.

The true lengths matter as much as the trimming: a label reading
"(20,000 chars)" for a trimmed 34,000-char prompt is not a smaller truth, it is
a wrong one, so every trimmed field records what it actually was.
"""

from typing import Any, Dict, Optional, Tuple

from src.schemas.execution_trace import ExecutionTraceItem
from src.utils.sensitive_data_utils import mask_sensitive_fields

#: Characters of each text field kept in a list response.
#:
#: Generous enough that short rows — most of them — are untouched and read
#: exactly as before, small enough that one enormous prompt cannot dominate the
#: payload. Rows are fetched in full the moment one is opened.
DEFAULT_PREVIEW_CHARS = 2000

#: Fields trimmed inside ``output`` and ``trace_metadata``. These carry
#: transcripts; everything else on a row is small by nature.
_TEXT_FIELDS = ("content", "prompt", "input", "output", "value", "memory_content")


def mask_sensitive_data(trace: ExecutionTraceItem) -> ExecutionTraceItem:
    """Mask credentials, secrets and tokens in a row before it is returned."""
    if trace.trace_metadata:
        trace.trace_metadata = mask_sensitive_fields(trace.trace_metadata)
    if trace.output and isinstance(trace.output, dict):
        trace.output = mask_sensitive_fields(trace.output)
    return trace


def _trim_text(value: Any, limit: int) -> Tuple[Any, Optional[int]]:
    """Trim one field, reporting its true length only when it was trimmed."""
    if not isinstance(value, str) or len(value) <= limit:
        return value, None
    return value[:limit], len(value)


def _trim_mapping(payload: Any, limit: int, sizes: Dict[str, int]) -> Any:
    if not isinstance(payload, dict):
        return payload
    trimmed = dict(payload)
    for field in _TEXT_FIELDS:
        if field in trimmed:
            value, true_length = _trim_text(trimmed[field], limit)
            if true_length is not None:
                trimmed[field] = value
                sizes[f"{field}_chars"] = true_length
    extra = trimmed.get("extra_data")
    if isinstance(extra, dict):
        trimmed["extra_data"] = _trim_mapping(extra, limit, sizes)
    return trimmed


def preview_trace(
    trace: ExecutionTraceItem, limit: int = DEFAULT_PREVIEW_CHARS
) -> ExecutionTraceItem:
    """Return ``trace`` with its long text trimmed for a list response.

    Records ``<field>_chars`` for anything trimmed and sets ``preview=True``, so
    a client can label the row with its real size and know it is abridged. Rows
    that were already small come back untouched and unmarked.
    """
    if limit <= 0:
        return trace

    sizes: Dict[str, int] = {}
    output = _trim_mapping(trace.output, limit, sizes)
    metadata = _trim_mapping(trace.trace_metadata, limit, sizes)

    if not sizes:
        return trace

    metadata = dict(metadata) if isinstance(metadata, dict) else {}
    metadata.update(sizes)
    metadata["preview"] = True

    return trace.model_copy(update={"output": output, "trace_metadata": metadata})
