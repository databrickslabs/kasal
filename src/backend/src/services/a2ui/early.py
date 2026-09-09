"""The instant shell: a surface frame on screen before the answer exists.

For the deliverables that OWN a canvas (quiz, flashcards, mindmap, map), the
request alone tells us a surface of that shape is coming — so a frame with a
derived title ships immediately, no model call, and the composed surface
replaces it when it lands. Best-effort and never raises: if it fails,
composition proceeds exactly as before and the reader simply waits.

(The deck shell and the outline pre-plan that used to live here are gone —
presentations are no longer an A2UI deliverable; they render on the chat
HTML path.)
"""

from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable, Dict, List, Optional

from src.services.a2ui.compose import infer_deliverable
from src.services.a2ui.stream import (
    SHELLABLE_KINDS,
    SURFACE_ID,
    shell_from_request,
    surface_to_messages,
)

logger = logging.getLogger(__name__)

DeltaSink = Callable[[Dict[str, Any]], Awaitable[None]]


#: How much of the streaming answer to wait for before planning the outline.
#: The outline needs the gist, not the ending — the deck call still gets the
#: WHOLE answer plus the plan, and its prompt already tells it to adjust where
#: the content cannot fill a slide. Too low and the plan is drawn from an
#: introduction; this is roughly a third of a typical answer.
def early_enabled() -> bool:
    """Kill-switch for both head starts (``A2UI_EARLY=false``)."""
    return os.getenv("A2UI_EARLY", "true").strip().lower() not in ("0", "false", "no")


def shell_kind(query: str) -> Optional[str]:
    """The surfaceKind to frame for this request, or None to frame nothing.

    A shell is a PROMISE that a surface of this shape is coming, so it is offered
    only for the kinds that own a canvas — quiz, flashcards, mindmap, map.
    Those are exactly the kinds the prose gate never drops.

    Dashboards, reports and Genie answers render on ``dashboard``/``document``,
    which ARE dropped back to plain text when the answer carries no real data. A
    frame for one of those would be shown and then retracted, which is a worse
    experience than never showing it — so they wait for a surface that is certain.
    """
    if not query:
        return None
    return SHELLABLE_KINDS.get(infer_deliverable(query) or "")


def wants_instant_shell(query: str) -> bool:
    return shell_kind(query) is not None


async def _resolve(group_id, query):
    """(enabled, guidance) from the workspace's UIConfig — never raises."""
    try:
        from src.services.a2ui.runner import _resolve_config

        enabled, catalog, guidance = await _resolve_config(group_id, query)
        return bool(enabled and catalog), guidance
    except Exception as err:  # noqa: BLE001
        logger.debug(f"[a2ui] early config not resolved: {err}")
        return False, ""


async def _ship(on_delta: DeltaSink, messages: List[Dict[str, Any]]) -> bool:
    sent = False
    for msg in messages:
        try:
            await on_delta(msg)
            sent = True
        except Exception as err:  # noqa: BLE001
            logger.debug(f"[a2ui] early message not shipped: {err}")
    return sent


async def emit_instant_shell(
    query: str,
    *,
    on_delta: DeltaSink,
    group_id: Optional[str] = None,
) -> bool:
    """Put a surface frame on screen before the agent has written a word.

    Returns True if anything shipped — the caller must pass that on to
    ``compose_surface(shell_shipped=...)`` so a run that ends up NOT producing a
    rich surface retracts the frame instead of stranding it.
    """
    if not early_enabled() or on_delta is None or not wants_instant_shell(query):
        return False
    try:
        enabled, _guidance = await _resolve(group_id, query)
        if not enabled:
            return False
        kind = shell_kind(query)
        if not kind:
            return False
        shell = shell_from_request(
            query,
            kind=kind,
            variant=infer_deliverable(query) or "",
        )
        if not shell:
            return False
        return await _ship(on_delta, surface_to_messages(shell, SURFACE_ID))
    except Exception as err:  # noqa: BLE001 — a head start must never fail a run
        logger.debug(f"[a2ui] instant shell skipped: {err}")
        return False
