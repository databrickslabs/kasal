"""Value coercion between ``MemoryRecord`` and Postgres/pgvector columns.

Pure functions, extracted from ``lakebase_storage_backend`` so the backend
module stays readable. Two of them exist because of a specific, expensive bug —
see :func:`to_aware_utc`.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def vector_to_pg(vector: list[float]) -> str:
    return "[" + ",".join(str(float(v)) for v in vector) + "]"


def loads_or_empty(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def to_naive_utc(dt: datetime) -> datetime:
    """Coerce a datetime to offset-naive UTC.

    CrewAI's recency math (``datetime.utcnow() - record.created_at``) is
    offset-naive, so every datetime we hand back on a ``MemoryRecord`` must be
    naive UTC — otherwise mixing with offset-aware values (e.g. Postgres
    ``timestamptz``) raises ``can't subtract offset-naive and offset-aware``.
    """
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def to_aware_utc(dt: datetime) -> datetime:
    """Coerce a datetime to offset-AWARE UTC for binding to TIMESTAMPTZ columns.

    The inverse of :func:`to_naive_utc`. asyncpg's timestamptz encoder runs
    ``obj.astimezone(utc)``, and ``astimezone`` on a NAIVE datetime presumes the
    host's LOCAL timezone — so a naive ``datetime.utcnow()`` gets shifted by the
    machine's UTC offset before it is stored. Stamping UTC tzinfo up front makes
    that encode a no-op and persists the true instant regardless of host tz.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_aware_utc_or_none(dt: datetime | None) -> datetime | None:
    """:func:`to_aware_utc`, but ``None`` passes through.

    The validity-window columns are nullable — an episodic record has no
    ``valid_from`` and a current fact has no ``valid_to``.
    """
    return None if dt is None else to_aware_utc(dt)


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return to_naive_utc(value)
    if not value:
        return datetime.utcnow()
    try:
        return to_naive_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return datetime.utcnow()
