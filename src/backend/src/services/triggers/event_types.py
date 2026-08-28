"""Standard event types for the choreography feature — a constant enum, no store.

The producer of an event is always a saved crew/flow that is *already* chosen when
an emit rule is created, so nobody needs to invent an event name. Instead they
pick a standard lifecycle TYPE from this enum, and the backend concatenates the
producer's identity into a distinct, unambiguous event name:

    canonical_event_name("crew", "<uuid>", "completed")  ->  "crew:<uuid>:completed"

That name is what a subscription listens for. Keeping the type set constant (and
mirrored as a plain list on the frontend) avoids a registry table and the
free-text drift it was meant to prevent (e.g. ``research`` vs ``research.done``).
"""

from __future__ import annotations

from enum import Enum


class EventType(str, Enum):
    """The lifecycle events a run can emit. Mirror on the frontend as a constant."""

    COMPLETED = "completed"
    FAILED = "failed"


VALID_EVENT_TYPES = tuple(e.value for e in EventType)


def canonical_event_name(entity_type: str, entity_id: str, event_type: str) -> str:
    """Build the distinct event name a producer emits.

    ``entity_type`` is ``crew``/``flow``, ``entity_id`` is the saved id (no colons
    in any of the three parts, so the name splits back cleanly).
    """
    return f"{entity_type}:{entity_id}:{event_type}"
