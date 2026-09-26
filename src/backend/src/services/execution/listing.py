"""Shape execution-history rows for the run LIST (``GET /executions``).

The list used to return every column of every row, including the full
``result`` and ``inputs`` JSON (plus ``agents_yaml``/``tasks_yaml`` re-serialized
from the inputs). Those are routinely 10-150 KB each, on up to 100 rows, on
the most-polled endpoint. A list row is now a summary with a truncated
``result_preview``; the full payload is on ``GET /executions/{execution_id}``,
or on the list itself behind the explicit ``include_payload`` flag.

Both shapes come from ONE builder (:func:`_entry`). They used to be two
hand-written dicts that had already drifted (``model`` from a projected column
in one and the raw inputs in the other, different ``execution_type`` and
``flow_id`` fallbacks). A payload row is the summary row plus the payload keys.
"""

import json
from typing import Any, Callable, Dict, Optional

from src.repositories.execution_history_repository import RESULT_PREVIEW_CHARS

#: Keys only a payload (``include_payload=true``) row carries.
PAYLOAD_KEYS = ("result", "inputs", "agents_yaml", "tasks_yaml")


def result_preview(value: Optional[str]) -> Optional[str]:
    """The projected preview, with a JSON ``null`` result reported as absent."""
    return None if value in (None, "", "null") else value


def _text(value: Any) -> Optional[str]:
    """A JSON field as the summary's SQL extraction (``->>``) renders it.

    A string as is; anything else as compact JSON text, which is what SQLite
    returns (PostgreSQL's spacing differs, the content does not); a missing
    or null field as None.
    """
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"))


def _entry(
    row: Any,
    *,
    input_execution_type: Optional[str],
    input_flow_id: Optional[str],
    model: Optional[str],
    preview: Optional[str],
) -> Dict[str, Any]:
    """The summary keys, from a projection row or a full ORM row alike.

    ``getattr`` for the columns the startup self-heal adds (``harness``,
    ``flow_id``, ``crew_id``, ``execution_type``): a row that predates them
    still lists.
    """
    flow_id = getattr(row, "flow_id", None)
    crew_id = getattr(row, "crew_id", None)
    return {
        "execution_id": row.job_id,
        "status": row.status,
        "created_at": row.created_at,
        "completed_at": row.completed_at,
        "run_name": row.run_name,
        "error": row.error,
        "group_email": row.group_email,
        "group_id": row.group_id,  # the frontend filters on it for isolation
        "execution_type": getattr(row, "execution_type", None)
        or input_execution_type
        or "crew",
        "harness": getattr(row, "harness", None),
        "flow_id": str(flow_id) if flow_id else input_flow_id,
        "crew_id": str(crew_id) if crew_id else None,
        "model": model,
        "result_preview": result_preview(preview),
    }


def summary_row(row: Any) -> Dict[str, Any]:
    """A list entry from an ``execution_summary_columns`` projection row."""
    return _entry(
        row,
        input_execution_type=row.input_execution_type,
        input_flow_id=row.input_flow_id,
        model=row.model,
        preview=row.result_preview,
    )


def full_row(
    run: Any, mask: Callable[[Dict[str, Any]], Dict[str, Any]]
) -> Dict[str, Any]:
    """A list entry carrying the whole payload (``include_payload=true``).

    Every summary key (derived the same way, from the full row) plus
    :data:`PAYLOAD_KEYS`. ``mask`` redacts sensitive tool configuration
    inside the inputs.
    """
    inputs: Optional[Dict[str, Any]] = mask(run.inputs) if run.inputs else None
    fields = inputs if isinstance(inputs, dict) else {}
    preview = (
        None if run.result is None else json.dumps(run.result)[:RESULT_PREVIEW_CHARS]
    )
    entry = _entry(
        run,
        input_execution_type=_text(fields.get("execution_type")),
        input_flow_id=_text(fields.get("flow_id")),
        model=_text(fields.get("model")),
        preview=preview,
    )
    entry["result"] = run.result
    entry["inputs"] = inputs
    for key in ("agents_yaml", "tasks_yaml"):
        if key in fields:
            value = fields[key]
            entry[key] = json.dumps(value) if isinstance(value, dict) else value
    return entry
