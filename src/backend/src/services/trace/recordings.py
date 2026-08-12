"""The replay cassette: prior runs' tool calls, read back as recordings.

Every completed tool call is already on the trace with its arguments AND its
result, so a re-run has a recording of the previous one without any new storage.
This module is the trace domain's read of that — the engine's replay hook asks
here rather than building ``ExecutionTraceRepository`` itself.

Kept out of ``service.py`` because that file is already at the size target;
this is a cohesive read with its own shape, not another method on the CRUD
service.
"""

import ast
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.repositories.execution_trace_repository import ExecutionTraceRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ToolRecording:
    """One completed tool call from an earlier run."""

    job_id: str
    tool_name: str
    task_name: str
    #: Arguments in a canonical form — sorted keys, JSON — so two calls that
    #: differ only in the order the model emitted its arguments still match.
    args_key: str
    output: str
    recorded_at: Optional[datetime]


class ToolRecordingsService:
    """Reads earlier runs' tool calls for replay."""

    def __init__(self, session: Any) -> None:
        self.repository = ExecutionTraceRepository(session)

    async def cassette_for(
        self,
        *,
        group_ids: List[str],
        exclude_job_id: str,
        max_age_seconds: int,
        limit: int = 500,
    ) -> List[ToolRecording]:
        """Every tool call from the most recent earlier run.

        ONE source run, not a pool of every matching call ever made: replay
        falls back to position ("the second search this task ran") when the
        arguments do not match, and a position is only meaningful within a
        single run. Mixing runs would answer the second search of one workload
        with the second search of another.

        Chronological within that run, which is the order the positions mean.

        Deliberately NOT filtered by tool. It used to take the replayable
        tools' names and filter on them, which quietly dropped most of the
        cassette: those names are catalogue TITLES ("ScrapeWebsiteTool") while
        a recording carries the tool's runtime name ("Read website content"),
        and the two only coincide for some tools. Whether a call may be
        replayed is decided per call by the tool's own policy, which is the
        only place that knows — so this read stays dumb.
        """
        if not group_ids:
            return []

        # NAIVE UTC. `execution_trace.created_at` is TIMESTAMP WITHOUT TIME
        # ZONE and rows are written naive, so asyncpg rejects an aware bound
        # parameter outright ("can't subtract offset-naive and offset-aware
        # datetimes"). The read below catches that, which is the dangerous part:
        # on Postgres the cassette would have come back empty forever and replay
        # would have looked simply switched off. SQLite never noticed.
        since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            seconds=max_age_seconds
        )
        try:
            rows = await self.repository.tool_recordings(
                group_ids,
                since=since,
                exclude_job_id=exclude_job_id,
                limit=limit,
            )
        except Exception:  # noqa: BLE001
            # A cassette that cannot be read is not an error — it means the
            # call goes out for real, which is what would have happened anyway.
            logger.debug("could not read tool recordings", exc_info=True)
            return []

        parsed = [r for r in (_recording(row) for row in rows) if r]
        if not parsed:
            return []

        # Rows arrive newest-first, so the first job_id seen is the latest run
        # that used one of these tools.
        source_job = parsed[0].job_id
        take = [r for r in parsed if r.job_id == source_job]
        take.reverse()
        return take


def canonical_args(args: Any) -> str:
    """A stable key for a call's arguments.

    Sorted keys so argument order cannot make two identical calls look
    different, and ``default=str`` because a tool may be handed something JSON
    does not know (a date, a Decimal) and a key that raises is worse than a key
    that stringifies.
    """
    if isinstance(args, str):
        args = _loads_loosely(args)
    try:
        return json.dumps(args, sort_keys=True, default=str, ensure_ascii=False)
    except Exception:  # noqa: BLE001
        return str(args)


def _recording(row: Any) -> Optional[ToolRecording]:
    output = _row_json(getattr(row, "output", None))
    if not isinstance(output, dict):
        return None
    extra = output.get("extra_data")
    extra = extra if isinstance(extra, dict) else {}

    tool_name = extra.get("tool_name") or _row_json(
        getattr(row, "trace_metadata", None)
    ).get("tool_name")
    content = output.get("content")
    if not tool_name or content is None:
        return None

    return ToolRecording(
        job_id=str(getattr(row, "job_id", "") or ""),
        tool_name=str(tool_name),
        task_name=str(extra.get("task_name") or ""),
        args_key=canonical_args(extra.get("tool_args")),
        output=str(content),
        recorded_at=getattr(row, "created_at", None),
    )


def _row_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = _loads_loosely(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _loads_loosely(text: str) -> Any:
    """JSON, falling back to a Python literal.

    ``tool_args`` reaches the trace as ``str(kwargs)`` — a Python repr, with
    single quotes and ``True``/``None`` — so half the recordings are not JSON
    at all. ``literal_eval`` parses those safely (it evaluates literals, never
    code), and a value that is neither comes back as the raw string, which
    still matches itself.
    """
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        pass
    try:
        return ast.literal_eval(text)
    except Exception:  # noqa: BLE001
        return text
