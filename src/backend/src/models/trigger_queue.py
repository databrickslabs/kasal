"""The event-trigger queue table.

A durable Postgres/Lakebase queue that drives crew/flow runs. A producer inserts
a row (an *event*); a background consumer claims it transactionally and dispatches
the bound crew/flow through the same path the scheduler uses. See
``services/triggers/`` for the consumer and ``src/docs/EVENT_TRIGGERS.md``.

A row is a TRIGGER, not the agents' working state: it carries a ``target`` (what
to run), a ``payload`` (the event body, incl. inputs), tenancy (``group_id``) and
a small correlation envelope — never definitions, tools, memory or secrets.

The table name is ``triggerqueue`` (``Base`` lower-cases the class name), matching
the codebase convention (``schedule``, ``executionhistory``). Created at runtime by
``create_all`` because this model is imported in ``models/__init__.py`` — alembic
does not run at startup here (see the migration for the alembic-managed path).
"""

from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, Index, Integer, String

from src.db.base import Base

# Status lifecycle (free string, like execution_history.trigger_type):
#   pending -> claimed -> dispatched | failed | dead
STATUS_PENDING = "pending"
STATUS_CLAIMED = "claimed"
STATUS_DISPATCHED = "dispatched"
STATUS_FAILED = "failed"
STATUS_DEAD = "dead"


class TriggerQueue(Base):
    """One queued event that should trigger a crew/flow run."""

    id = Column(Integer, primary_key=True)

    # Tenancy — becomes the run's GroupContext; the one field trusted end-to-end.
    group_id = Column(String(100), nullable=True)

    # Routing. In Phase 1 the row names its ``target`` directly; ``event_type`` is
    # reserved for Phase 2 subscription matching (topic mode).
    event_type = Column(String(255), nullable=True)
    target = Column(
        JSON, nullable=True
    )  # {"kind": "flow"|"inline", "id"?, "config"?, "harness"?}
    payload = Column(JSON, default=dict)  # the event body (inputs live here)

    # Delivery state.
    status = Column(String(20), default=STATUS_PENDING, nullable=False)
    attempts = Column(Integer, default=0, nullable=False)
    # Visibility: NULL means "available now"; a future time delays/backs off.
    available_at = Column(DateTime, nullable=True)
    # When the row was claimed — used to reclaim rows stuck by a crashed worker.
    claimed_at = Column(DateTime, nullable=True)
    last_error = Column(String, nullable=True)

    # Correlation envelope (threads a chain of crew→crew hand-offs).
    correlation_id = Column(String(100), nullable=True)
    causation_run_id = Column(String(100), nullable=True)
    # Dedupe: a producer that re-emits the same logical event can't double-fire.
    idempotency_key = Column(String(255), nullable=True, unique=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        # The claim scan filters on status + available_at and orders by created_at.
        Index("ix_triggerqueue_status_available", "status", "available_at"),
        Index("ix_triggerqueue_group_id", "group_id"),
        Index("ix_triggerqueue_correlation_id", "correlation_id"),
        Index("ix_triggerqueue_event_type", "event_type"),
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.status is None:
            self.status = STATUS_PENDING
        if self.attempts is None:
            self.attempts = 0
        if self.payload is None:
            self.payload = {}
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
