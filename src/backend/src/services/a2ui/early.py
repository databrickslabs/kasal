"""Everything a deck can show BEFORE the answer it is made from exists.

Streaming the composer fixed the last phase of a presentation and left the first
two untouched. Measured on one run ("create a presentation on how llm works",
78 components):

    0.0s  run starts
   13.4s  the agent finishes writing its prose answer   <- compose cannot start
   22.1s  the outline pre-pass returns, skeleton ships  <- first thing on screen
   31s    slides begin streaming in
   58s    done

Composition is called with the FINISHED answer, and the outline is derived from
it, so the reader waits 22 seconds for the first structure no matter how well the
deck itself streams. This module attacks those 22 seconds from both ends:

* :func:`emit_instant_shell` — no model at all. The moment the request is
  recognised as a presentation we know a deck is coming and roughly how big, so
  a frame with a derived title goes out immediately.
* :func:`plan_outline_early` — runs the outline pre-pass on a PARTIAL answer
  while the agent is still writing, so the real titles land around the time the
  answer finishes instead of ~9 seconds after it.

Both are best-effort and never raise: if either fails, composition proceeds
exactly as it does today and the reader simply waits longer.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Awaitable, Callable, Dict, List, Optional

from src.services.a2ui.compose import (
    infer_deliverable,
    plan_presentation_outline,
    slide_target,
)
from src.services.a2ui.stream import (
    SHELLABLE_KINDS,
    SURFACE_ID,
    shell_from_request,
    skeleton_from_outline,
    surface_to_messages,
)

logger = logging.getLogger(__name__)

DeltaSink = Callable[[Dict[str, Any]], Awaitable[None]]

#: How much of the streaming answer to wait for before planning the outline.
#: The outline needs the gist, not the ending — the deck call still gets the
#: WHOLE answer plus the plan, and its prompt already tells it to adjust where
#: the content cannot fill a slide. Too low and the plan is drawn from an
#: introduction; this is roughly a third of a typical answer.
def outline_headstart_chars() -> int:
    try:
        return max(0, int(os.getenv("A2UI_OUTLINE_HEADSTART_CHARS", "1500")))
    except Exception:  # noqa: BLE001
        return 1500


def early_enabled() -> bool:
    """Kill-switch for both head starts (``A2UI_EARLY=false``)."""
    return os.getenv("A2UI_EARLY", "true").strip().lower() not in ("0", "false", "no")


def shell_kind(query: str) -> Optional[str]:
    """The surfaceKind to frame for this request, or None to frame nothing.

    A shell is a PROMISE that a surface of this shape is coming, so it is offered
    only for the kinds that own a canvas — deck, quiz, flashcards, mindmap, map.
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


async def _note_skip(
    execution_id: Optional[str], reason: str, group_context: Any = None
) -> None:
    """Record a head start that was WANTED but did not happen, in the TRACE.

    The a2ui modules log through a plain ``logging.getLogger(__name__)``, which
    on this deployment reaches the console and no log file — so two rounds of
    "why is the outline still running after the answer?" had no evidence to read
    anywhere. The trace is queryable and already scoped to the run, so a skipped
    head start leaves a row that says why. Only written when the request really
    was a deck and the answer really was long enough, so an ordinary prose turn
    adds nothing.
    """
    if not execution_id:
        return
    try:
        from src.services.trace.writer import write_rows

        await write_rows(
            execution_id,
            [("a2ui_outline_skipped", "kasal.a2ui.outline_skipped", reason, {"reason": reason})],
            fallback_source="A2UI",
            fallback_context="a2ui outline head start",
            group_context=group_context,
        )
    except Exception as err:  # noqa: BLE001
        logger.debug(f"[a2ui] head-start skip not traced: {err}")


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
    """Put a deck frame on screen before the agent has written a word.

    Returns True if anything shipped — the caller must pass that on to
    ``compose_surface(shell_shipped=...)`` so a run that ends up NOT producing a
    deck retracts the frame instead of stranding it.
    """
    if not early_enabled() or on_delta is None or not wants_instant_shell(query):
        return False
    try:
        enabled, guidance = await _resolve(group_id, query)
        if not enabled:
            return False
        shell = shell_from_request(
            query,
            kind=shell_kind(query) or "presentation",
            variant=infer_deliverable(query) or "",
            slides=slide_target(guidance) or 8,
        )
        if not shell:
            return False
        return await _ship(on_delta, surface_to_messages(shell, SURFACE_ID))
    except Exception as err:  # noqa: BLE001 — a head start must never fail a run
        logger.debug(f"[a2ui] instant shell skipped: {err}")
        return False


async def plan_outline_early(
    partial_answer: str,
    *,
    query: str,
    purpose: str = "",
    model: Optional[str] = None,
    on_delta: Optional[DeltaSink] = None,
    group_id: Optional[str] = None,
    execution_id: Optional[str] = None,
    group_context: Any = None,
) -> Optional[List[Dict[str, str]]]:
    """Plan the deck from a partial answer, and ship its skeleton at once.

    Returns the outline so the caller can hand it to ``compose_surface`` — which
    then skips its own pre-pass, so this is a head start rather than an extra
    call. Returns None on anything unexpected, and composition falls back to
    planning the outline itself from the complete answer.
    """
    if not early_enabled() or not wants_instant_shell(query):
        return None
    text = (partial_answer or "").strip()
    if len(text) < outline_headstart_chars():
        return None
    try:
        enabled, guidance = await _resolve(group_id, query)
        if not enabled:
            await _note_skip(execution_id, "A2UI is off for this workspace", group_context)
            return None

        from src.services.llm.manager import LLMManager

        # RESTORE THE TENANT CONTEXT FIRST. This coroutine runs in a task created
        # from the token-flush callback, and that callback is scheduled onto the
        # loop from an LLM WORKER THREAD — so the context it copies is the loop's
        # default one, not the request's. `LLMManager.get_llm` RAISES when it
        # cannot see a group_id (that check is the multi-tenant isolation
        # guarantee, so it must not be bypassed), which meant this whole head
        # start failed on every single run and said so only at debug level.
        if group_context is not None:
            try:
                from src.utils.user_context import UserContext

                if UserContext.get_group_context() is None:
                    UserContext.set_group_context(group_context)
            except Exception as ctx_err:  # noqa: BLE001
                logger.debug(f"[a2ui] early outline context not set: {ctx_err}")

        model_name = model or os.getenv("CREW_MODEL") or "databricks-llama-4-maverick"
        llm = await LLMManager.get_llm(model_name, temperature=0)
        loop = asyncio.get_running_loop()

        def _llm_call(messages: List[Dict[str, str]]) -> str:
            import time as _t

            began = _t.monotonic()
            out = llm.call(messages)
            text_out = out if isinstance(out, str) else str(out)
            # Its own trace rows, labelled as the OUTLINE rather than as a
            # compose attempt. Reading them as "A2UI Compose Request #1" is what
            # made a two-call deck look like a single slow one.
            if execution_id:
                try:
                    from src.services.trace.writer import write_rows

                    prompt = "\n\n".join(
                        f"{m.get('role', '')}: {m.get('content', '')}" for m in messages
                    )
                    shared = {"llm_purpose": "a2ui_outline", "model": model_name}
                    asyncio.run_coroutine_threadsafe(
                        write_rows(
                            execution_id,
                            [
                                (
                                    "llm_call",
                                    "kasal.a2ui.outline_call",
                                    prompt,
                                    {**shared, "prompt": prompt},
                                ),
                                (
                                    "llm_response",
                                    "kasal.a2ui.outline_response",
                                    text_out,
                                    {
                                        **shared,
                                        "duration_ms": round(
                                            (_t.monotonic() - began) * 1000, 2
                                        ),
                                        "head_start": True,
                                        "answer_chars": len(text),
                                    },
                                ),
                            ],
                            fallback_source="A2UI",
                            fallback_context="a2ui outline (head start)",
                            group_context=group_context,
                        ),
                        loop,
                    )
                except Exception as trace_err:  # noqa: BLE001
                    logger.debug(f"[a2ui] outline trace skipped: {trace_err}")
            return text_out

        outline = await asyncio.to_thread(
            plan_presentation_outline, text, query, purpose, _llm_call, guidance
        )
        if not outline:
            await _note_skip(
                execution_id,
                "the planner returned no usable outline from the partial answer",
                group_context,
            )
            return None
        if on_delta is not None:
            skeleton = skeleton_from_outline(outline)
            if skeleton:
                await _ship(on_delta, surface_to_messages(skeleton, SURFACE_ID))
        return outline
    except Exception as err:  # noqa: BLE001
        # WARNING: by this point the request WAS a presentation and the answer
        # WAS long enough, so a failure here is a head start that should have
        # happened and did not — worth a line, not a debug whisper.
        logger.warning(
            f"[a2ui] outline head start failed ({err}); "
            "the outline will run after the answer instead"
        )
        await _note_skip(execution_id, f"{type(err).__name__}: {err}", group_context)
        return None
