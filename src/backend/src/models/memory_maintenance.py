"""Per-scope watermark for memory maintenance.

Maintenance used to run only at the end of a run, which made its COVERAGE a
function of run frequency rather than store size — precisely backwards. A big,
stale store attached to a rarely-run crew is the case that needs consolidating
most and got it least, while a busy chat workspace re-scanned the same recent
records over and over.

A watermark inverts that: the sweep asks "which scope has gone longest without
maintenance?" and works from there, so every scope is eventually reached
regardless of how often anyone runs anything in it.

It also has to be DURABLE, which is why this is a table and not a dict. The chat
path throttles in process memory, which is fine as a rate limiter but is lost on
restart and is per-replica — on a multi-replica deployment each replica keeps its
own idea of when a scope was last maintained, so a scope can be swept N times per
interval or, after a rolling restart, immediately again.
"""

from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import JSON, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


def generate_uuid() -> str:
    return str(uuid4())


class MemoryMaintenanceWatermark(Base):
    """When a group's memory scope was last maintained, and what it did."""

    __tablename__ = "memory_maintenance_watermarks"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=generate_uuid)

    # One row per tenant. The memory root scope is always ``/<group_id>``
    # (see CrewMemoryService._build_memory_kwargs), so the group IS the scope.
    group_id: Mapped[str] = mapped_column(
        String(100), nullable=False, unique=True, index=True
    )

    # Null means "never maintained" — which sorts first, so a newly configured
    # workspace is picked up on the next sweep rather than after one interval.
    last_maintained_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, index=True
    )

    # Kept so a sweep that keeps failing is visible without reading logs: a
    # scope stuck at "error" for days is the signal that its backend, embedder
    # or credentials are broken.
    last_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending"
    )
    last_error: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Pass-by-pass counts from the last run (deleted, merged, superseded,
    # forgotten). This is the only place memory maintenance is observable as
    # data rather than as log lines.
    last_stats: Mapped[Any] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "group_id": self.group_id,
            "last_maintained_at": (
                self.last_maintained_at.isoformat() if self.last_maintained_at else None
            ),
            "last_status": self.last_status,
            "last_error": self.last_error,
            "last_stats": self.last_stats or {},
        }
