"""DTOs for the event-trigger queue — the producer/enqueue contract.

A queue message is a TRIGGER, not the agents' state: it names a ``target`` (what
to run), an event ``payload`` (the body, with ``inputs``), tenancy and a small
correlation envelope. See ``src/docs/EVENT_TRIGGERS.md``.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class TriggerTarget(BaseModel):
    """What a queued event should run.

    - ``kind="flow"`` + ``id`` — run a saved flow by id (loaded from the repo).
    - ``kind="crew"`` + ``id`` — run a saved crew by id (resolved via the catalog).
    - ``kind="inline"`` + ``config`` — run a full CrewConfig/FlowConfig inline.
    - ``kind="webhook"`` + ``url`` — POST the event to an external service
      instead of running anything (server-to-server delivery; the queue's
      retry/backoff/dead-letter semantics apply to the delivery).
    """

    kind: str = Field(description="'flow' | 'inline' | 'crew' | 'webhook'")
    id: Optional[str] = Field(
        None, description="Saved crew/flow id (for kind flow/crew)"
    )
    url: Optional[str] = Field(
        None, description="http(s) endpoint to POST the event to (for kind 'webhook')"
    )
    config: Optional[Dict[str, Any]] = Field(
        None, description="Inline CrewConfig/FlowConfig fields (for kind 'inline')"
    )
    harness: Optional[str] = Field(
        None, description="Per-run engine override: 'kasal' | 'crewai'"
    )


class EnqueueTrigger(BaseModel):
    """The row a producer inserts to trigger a run."""

    target: TriggerTarget
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Event body; run inputs are read from payload['inputs']",
    )
    # NOTE: no ``group_id`` field — the tenant is ALWAYS stamped from the
    # authenticated caller's GroupContext. Accepting one here implied a producer
    # could choose its tenant; it never could (the service ignored it).
    event_type: Optional[str] = Field(
        None, description="Topic name (reserved for Phase 2 subscription matching)"
    )
    correlation_id: Optional[str] = Field(
        None, description="Chain id — threads crew→crew hand-offs"
    )
    causation_run_id: Optional[str] = Field(
        None, description="Run id that emitted this event (Phase 2)"
    )
    idempotency_key: Optional[str] = Field(
        None, description="Unique — dedupes duplicate producers"
    )


class TriggerEventResponse(BaseModel):
    """A queued trigger event, as returned by the API."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    group_id: Optional[str] = None
    event_type: Optional[str] = None
    target: Optional[Dict[str, Any]] = None
    payload: Optional[Dict[str, Any]] = None
    status: str
    attempts: int
    available_at: Optional[datetime] = None
    claimed_at: Optional[datetime] = None
    last_error: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_run_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TriggerListResponse(BaseModel):
    """A page of queued trigger events."""

    events: List[TriggerEventResponse]
    total: int


class DispatchResult(BaseModel):
    """Outcome of an on-demand drain: how many due rows were claimed and had a
    dispatch launched (each launched run continues in the background)."""

    claimed: int


# --------------------------------------------------------------------------
# Choreography config: subscriptions (event -> crew/flow) and emit rules
# (crew/flow completion -> event). See src/docs/EVENT_TRIGGERS.md.
# --------------------------------------------------------------------------


class SubscriptionCreate(BaseModel):
    """Bind an event name to a crew/flow that should run when it fires."""

    event_type: str = Field(description="The event name to listen for")
    target: TriggerTarget = Field(description="Crew/flow to run (kind + id)")
    harness: Optional[str] = Field(None, description="Per-run engine override")
    input_mapping: Optional[Dict[str, Any]] = Field(
        None, description="event payload → crew inputs; null = pass payload through"
    )
    schema_ref: Optional[str] = Field(
        None, description="Object Management schema name the payload should match"
    )
    enabled: bool = True


class SubscriptionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    group_id: Optional[str] = None
    event_type: str
    target: Optional[Dict[str, Any]] = None
    harness: Optional[str] = None
    input_mapping: Optional[Dict[str, Any]] = None
    schema_ref: Optional[str] = None
    enabled: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class EmitRuleCreate(BaseModel):
    """When a crew/flow completes, emit an event carrying its output."""

    on_target: TriggerTarget = Field(
        description="Crew/flow whose completion fires this"
    )
    event_type: str = Field(description="The event name to emit")
    schema_ref: Optional[str] = Field(
        None, description="Object Management schema name the emitted payload matches"
    )
    condition: Optional[str] = Field(
        None, description="Optional guard over the run output; null = always emit"
    )
    enabled: bool = True


class EmitRuleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    group_id: Optional[str] = None
    on_target: Optional[Dict[str, Any]] = None
    event_type: str
    schema_ref: Optional[str] = None
    condition: Optional[str] = None
    enabled: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class SubscriptionListResponse(BaseModel):
    subscriptions: List[SubscriptionResponse]
    emit_rules: List[EmitRuleResponse]
