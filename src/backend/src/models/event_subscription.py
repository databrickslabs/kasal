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

from sqlalchemy import JSON, Boolean, Column, DateTime, Index, Integer, String

from src.db.base import Base


class EventSubscription(Base):
    """Inbound: an ``event_type`` triggers a target crew/flow."""

    id = Column(Integer, primary_key=True)
    group_id = Column(String(100), nullable=True)

    #: The event name this subscription listens for (the glue string).
    event_type = Column(String(255), nullable=False)
    #: What to run: {"kind": "crew"|"flow", "id": ...}.
    target = Column(JSON, nullable=False)
    #: Optional per-run engine override ("kasal" | "crewai").
    harness = Column(String(20), nullable=True)
    #: Optional JSONPath-ish map event payload -> crew inputs. Null = pass the
    #: whole payload through as inputs.
    input_mapping = Column(JSON, nullable=True)
    #: Optional Object Management schema name the payload is expected to match.
    schema_ref = Column(String(255), nullable=True)
    enabled = Column(Boolean, default=True, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_eventsubscription_event_type", "event_type"),
        Index("ix_eventsubscription_group_id", "group_id"),
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.enabled is None:
            self.enabled = True
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()


class EmitRule(Base):
    """Outbound: a target crew/flow's completion emits an ``event_type``."""

    id = Column(Integer, primary_key=True)
    group_id = Column(String(100), nullable=True)

    #: Whose completion fires this rule: {"kind": "crew"|"flow", "id": ...}.
    on_target = Column(JSON, nullable=False)
    #: The event name to emit.
    event_type = Column(String(255), nullable=False)
    #: Optional Object Management schema name the emitted payload matches.
    schema_ref = Column(String(255), nullable=True)
    #: Optional guard expression over the run's structured output; null = always.
    condition = Column(String, nullable=True)
    enabled = Column(Boolean, default=True, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_emitrule_event_type", "event_type"),
        Index("ix_emitrule_group_id", "group_id"),
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.enabled is None:
            self.enabled = True
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
