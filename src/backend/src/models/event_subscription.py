"""Event choreography config: subscriptions and emit rules.

Two tables that turn the queue into a pub/sub graph (see
``src/docs/EVENT_TRIGGERS.md``):

- ``EventSubscription`` — "run this crew/flow when event ``event_type`` fires".
  The inbound half: an event with an ``event_type`` fans out to every enabled
  subscription bound to that name.
- ``EmitRule`` — "when this crew/flow completes, emit event ``event_type``
  (carrying its output)". The outbound half: a run's output becomes the next
  event.

The pair sharing an ``event_type`` string is the whole wiring — crew A's emit
rule and crew B's subscription need only agree on the name. An optional
``schema_ref`` names an Object Management schema (the payload contract).

Both tables are created at runtime by the ``_ensure_*`` self-heal helpers in
``db/session.py`` (alembic does not run at startup here) and registered in
``db/all_models.py`` for ``create_all`` on fresh DBs.
"""

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, Boolean, DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class EventSubscription(Base):
    """Inbound: an ``event_type`` triggers a target crew/flow."""

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    group_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    #: The event name this subscription listens for (the glue string).
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    #: What to run: {"kind": "crew"|"flow", "id": ...}.
    target: Mapped[Any] = mapped_column(JSON, nullable=False)
    #: Optional per-run engine override ("kasal" | "crewai").
    harness: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    #: Optional STATIC input overrides for the triggered run (a plain dict used
    #: as-is — NOT a payload projection; a JSONPath-style mapping is future
    #: work). Null = pass the whole payload through as inputs.
    input_mapping: Mapped[Any] = mapped_column(JSON, nullable=True)
    #: Optional Object Management schema name the payload is expected to match.
    schema_ref: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    __table_args__ = (
        Index("ix_eventsubscription_event_type", "event_type"),
        Index("ix_eventsubscription_group_id", "group_id"),
    )

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if self.enabled is None:
            self.enabled = True
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()


class EmitRule(Base):
    """Outbound: a target crew/flow's completion emits an ``event_type``."""

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    group_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    #: Whose completion fires this rule: {"kind": "crew"|"flow", "id": ...}.
    on_target: Mapped[Any] = mapped_column(JSON, nullable=False)
    #: The event name to emit.
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    #: Optional Object Management schema name the emitted payload matches.
    schema_ref: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    #: Optional guard expression over the run's structured output; null = always.
    condition: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    __table_args__ = (
        Index("ix_emitrule_event_type", "event_type"),
        Index("ix_emitrule_group_id", "group_id"),
    )

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if self.enabled is None:
            self.enabled = True
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
