""" "The catalogue changed" — announced once, listened to by each surface.

A published capability becomes a tool on the MCP surface, a skill on the A2A
card and a route in the chat catalogue. When a publication is created, renamed
or withdrawn, every one of those is out of date, and the MCP surface is the one
that CANNOT notice by itself: a client fetches ``tools/list`` when it connects
and has no reason to ask again, so a capability published a minute later is
invisible to it until it reconnects.

So the registry says what happened, and whoever cares listens. The alternative —
``publication.py`` importing the MCP session registry — would put a transport
adapter inside the protocol-NEUTRAL thing whose whole point is that all three
surfaces read one list. The next surface would add a second import, and the
registry would end up knowing about every protocol Kasal speaks.

Listeners are best-effort and isolated: one that raises is logged and the others
still run. Publishing a crew must not fail because a notification could not be
delivered to someone's editor.
"""

import asyncio
import inspect
import logging
from typing import Any, Callable, List, Optional

logger = logging.getLogger(__name__)

#: Callables invoked after a publication is created, changed or removed. Each
#: takes the affected ``group_ids`` (None meaning "any group").
_listeners: List[Callable[[Optional[List[str]]], Any]] = []


def on_catalogue_changed(listener: Callable[[Optional[List[str]]], Any]):
    """Register a listener. Usable as a decorator."""
    if listener not in _listeners:
        _listeners.append(listener)
    return listener


def clear_listeners() -> None:
    """Drop every listener. For tests, which must not leak into each other."""
    _listeners.clear()


async def catalogue_changed(group_ids: Optional[List[str]] = None) -> None:
    """Tell every surface that the published set is different now.

    ``group_ids`` narrows it to the workspaces affected, so a publish in one
    tenant does not make every other tenant's client refetch.
    """
    if not _listeners:
        return
    for listener in list(_listeners):
        try:
            result = listener(group_ids)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — a listener never fails a publish
            logger.warning("[publications] catalogue listener failed: %s", exc)
