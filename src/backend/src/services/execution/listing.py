"""Shape execution-history rows for the run LIST (``GET /executions``).

The list used to return every column of every row, including the full
``result`` and ``inputs`` JSON (plus ``agents_yaml``/``tasks_yaml`` re-serialized
from the inputs). Those are routinely 10-150 KB each, on up to 100 rows, on
the most-polled endpoint. A list row is now a summary with a truncated
``result_preview``; the full payload is on ``GET /executions/{execution_id}``,
or on the list itself behind the explicit ``include_payload`` flag.
"""

import json
from typing import Any, Callable, Dict, Optional


def result_preview(value: Optional[str]) -> Optional[str]:
    """The projected preview, with a JSON ``null`` result reported as absent."""
    return None if value in (None, "", "null") else value


def summary_row(row: Any) -> Dict[str, Any]:
    """A list entry from an ``execution_summary_columns`` projection row."""
    return {
        "execution_id": row.job_id,
        "status": row.status,
        "created_at": row.created_at,
        "completed_at": row.completed_at,
        "run_name": row.run_name,
        "error": row.error,
        "group_email": row.group_email,
        "group_id": row.group_id,  # the frontend filters on it for isolation
        "execution_type": row.execution_type or row.input_execution_type or "crew",
        "harness": row.harness,
        "flow_id": str(row.flow_id) if row.flow_id else row.input_flow_id,
        "crew_id": str(row.crew_id) if row.crew_id else None,
        "model": row.model,
        "result_preview": result_preview(row.result_preview),
    }


def full_row(
    run: Any, mask: Callable[[Dict[str, Any]], Dict[str, Any]]
) -> Dict[str, Any]:
    """A list entry carrying the whole payload (``include_payload=true``).

    ``mask`` redacts sensitive tool configuration inside the inputs.
    """
    inputs: Optional[Dict[str, Any]] = mask(run.inputs) if run.inputs else None
    entry = {
        "execution_id": run.job_id,
        "status": run.status,
        "created_at": run.created_at,
        "completed_at": run.completed_at,
        "run_name": run.run_name,
        "result": run.result,
        "error": run.error,
        "group_email": run.group_email,
        "group_id": run.group_id,
        "inputs": inputs,
        "execution_type": getattr(run, "execution_type", None)
        or (inputs.get("execution_type") if inputs else None)
        or "crew",
        # The column is added by the startup self-heal, hence getattr: a row
        # that predates it still lists.
        "harness": getattr(run, "harness", None),
        "flow_id": (
            str(run.flow_id)
            if getattr(run, "flow_id", None)
            else (inputs.get("flow_id") if inputs else None)
        ),
        "crew_id": str(run.crew_id) if getattr(run, "crew_id", None) else None,
        "model": inputs.get("model") if inputs else None,
    }
    if isinstance(inputs, dict):
        for key in ("agents_yaml", "tasks_yaml"):
            if key in inputs:
                value = inputs[key]
                entry[key] = (
                    json.dumps(value)
                    if isinstance(value, dict)
                    else inputs.get(key, "")
                )
    return entry
