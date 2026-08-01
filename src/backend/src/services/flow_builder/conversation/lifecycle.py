"""Keeping a long-lived thread affordable, and noticing when two turns collide.

Two problems a conversation develops once it runs for a while.

**It grows.** ``trim_messages`` kept the newest 100 and dropped the rest, which
bounds the cost and loses the beginning of the conversation outright — the flow
forgets what it was asked to do first. Folding the dropped turns into a summary
channel keeps the facts and still bounds the size. The summarizer is the one
chat already uses (``services/chat/context_compaction``): same prompt, same
instructions, same env kill-switch, because two summarizers would drift and a
thread summarised differently from a chat session is a difference nobody wants
to debug.

**Two turns can race.** Kasal runs each turn in its own subprocess and nothing
serialises them, so two messages sent quickly both restore the same checkpoint,
both run, and the slower one's save overwrites the faster one's — a lost update
that looks exactly like the flow ignoring a message. There is no lock to take
here (the append-only table has nothing to lock on), so this DETECTS the
collision and says so, rather than pretending it cannot happen. A hard guard
needs the ``flow_threads`` row that the design deliberately deferred.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

#: Channel the folded history lives in. Declared like any other channel, so a
#: task can interpolate it and a condition can read it.
SUMMARY_CHANNEL = "summary"

#: How many recent messages stay verbatim. Everything older is foldable.
KEEP_VERBATIM = 20


def split_for_fold(
    messages: List[Dict[str, Any]], keep: int = KEEP_VERBATIM
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """``(to_fold, verbatim)`` — the older messages, and the ones kept as-is.

    Mirrors ``context_compaction.split_for_compaction``: newest ``keep`` stay,
    the older remainder folds. Nothing folds until there is more than ``keep``,
    so a short conversation never pays for a summarizer call.
    """
    if not isinstance(messages, list) or len(messages) <= keep:
        return [], list(messages or [])
    return messages[:-keep], messages[-keep:]


def render_transcript(messages: List[Dict[str, Any]]) -> str:
    """The folded turns as text for the summarizer."""
    lines = []
    for entry in messages:
        role = (entry or {}).get("role", "?")
        content = str((entry or {}).get("content", "")).strip()
        if content:
            lines.append(f"{role}: {content}")
    return "\n".join(lines)


async def fold_thread_history(
    state: Any,
    model_name: Optional[str] = None,
    keep: int = KEEP_VERBATIM,
) -> bool:
    """Fold a thread's older messages into its summary channel.

    Returns True when a fold happened. Best-effort by design, exactly like the
    chat path: a summarizer failure must not fail a turn the user already has an
    answer from, so it is logged and the messages are left alone — the next turn
    tries again.

    Requires the flow to declare a ``summary`` channel. Without one there is
    nowhere to put the folded text, and dropping it would be the truncation this
    exists to replace, so an undeclared channel means no fold at all.
    """
    from src.services.chat.context_compaction import (
        build_summary_prompt,
        compaction_enabled,
    )

    if not compaction_enabled():
        return False
    if not hasattr(state, "merge") or SUMMARY_CHANNEL not in getattr(
        type(state), "model_fields", {}
    ):
        return False

    messages = getattr(state, "messages", None)
    to_fold, verbatim = split_for_fold(messages, keep)
    if not to_fold:
        return False

    try:
        from src.services.llm.manager import LLMManager

        prompt = build_summary_prompt(
            getattr(state, SUMMARY_CHANNEL, None) or None, render_transcript(to_fold)
        )
        response = await LLMManager.completion(messages=prompt, model=model_name)
        summary = (
            response["choices"][0]["message"]["content"]
            if isinstance(response, dict)
            else str(response)
        ).strip()
        if not summary:
            return False
    except Exception as exc:  # noqa: BLE001 — never fail a turn for bookkeeping
        logger.warning("[flow-thread] could not fold history: %s", exc)
        return False

    # `summary` REPLACES (its reducer is the default) while `messages` is
    # rewritten wholesale — the folded ones are now represented by the summary,
    # so appending would keep both copies.
    state.summary = summary
    state.messages = verbatim
    logger.info(
        "[flow-thread] folded %d message(s) into the summary of thread %s",
        len(to_fold),
        getattr(state, "id", "?"),
    )
    return True


def base_checkpoint_of(state: Any) -> Optional[int]:
    """The checkpoint id a turn restored from, if it recorded one."""
    return getattr(state, "_base_checkpoint_id", None)


def note_base_checkpoint(state: Any, checkpoint_id: Optional[int]) -> None:
    """Remember which checkpoint this turn started from.

    Stored on the object rather than in a channel: it describes THIS run's view
    of the thread, not the thread's contents, and persisting it would put a
    detail of one turn into the state every later turn restores.
    """
    try:
        object.__setattr__(state, "_base_checkpoint_id", checkpoint_id)
    except Exception:  # noqa: BLE001 — a state that refuses it simply opts out
        logger.debug("[flow-thread] could not record the base checkpoint")


def detect_concurrent_turn(
    state: Any, latest_checkpoint_id: Optional[int]
) -> Optional[str]:
    """Whether another turn advanced this thread while this one was running.

    Returns a description of the collision, or None. Detection only: with no
    lock available the write still proceeds last-writer-wins, which is what
    happens today — the difference is that it now says so instead of losing a
    message in silence.
    """
    base = base_checkpoint_of(state)
    if base is None or latest_checkpoint_id is None:
        return None
    if latest_checkpoint_id <= base:
        return None
    return (
        f"thread {getattr(state, 'id', '?')} advanced from checkpoint {base} to "
        f"{latest_checkpoint_id} while this turn was running; another turn ran "
        "concurrently and one of the two updates will be overwritten"
    )
