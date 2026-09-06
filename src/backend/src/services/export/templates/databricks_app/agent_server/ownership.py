"""Who a conversation belongs to.

A conversation id used to be the only key: whoever knew (or guessed) one could
read its history, poll its progress and surface, and cancel its turn. Now the
first turn claims the conversation for the signed-in user, the claim is
persisted beside the history, and every later turn and every read route checks
it. Identity is derived ONE way — from Databricks' forwarded headers — so a
conversation claimed on a turn is readable by the same person and nobody else.
Without a proxy (a local run) there is no identity and everything belongs to
the anonymous user, which is what a local run means.

Three rules make the claim safe (second review, R2-02):

- it is ATOMIC in the store (insert-if-absent), so two first turns racing for
  one id cannot both win; the loser sees the winner's claim and is refused;
- history that predates ownership is never claimable — an unowned id that
  already holds messages belongs to nobody and reads as not found;
- the claim is refreshed on every authorized turn, so it lives exactly as long
  as the history it protects and cannot be pruned out from under it; and a
  store that cannot answer fails CLOSED rather than reading as "unowned".
"""

import json
from typing import Any, Mapping, Optional

from agent_server import state_store

_OWNER_KEY = "owner"
#: conversation.py's history key — checked here so an unowned id with history
#: is recognised as legacy rather than claimable.
_HISTORY_KEY = "history"


class ConversationOwnedByAnother(Exception):
    """The conversation was started by someone else."""


class ConversationUnclaimable(Exception):
    """The conversation holds history from before ownership tracking."""


def identity_from_headers(headers: Mapping[str, Any]) -> Optional[str]:
    """The signed-in user as Databricks Apps forwards them; None without a proxy."""
    get = headers.get
    return (
        get("x-forwarded-preferred-username")
        or get("x-forwarded-email")
        or get("x-forwarded-user")
        or None
    )


def _read_owner(conversation_id: str) -> Optional[str]:
    """The stored owner, or None when unclaimed. Raises when the store cannot
    answer — an unreadable claim must not read as no claim."""
    stored = state_store.get_json(conversation_id, _OWNER_KEY, strict=True)
    if isinstance(stored, dict) and "user" in stored:
        return str(stored["user"] or "")
    return None


def owner_of(conversation_id: Optional[str]) -> Optional[str]:
    """The user a conversation was claimed for, or None if never claimed."""
    if not conversation_id:
        return None
    try:
        return _read_owner(conversation_id)
    except state_store.StorageUnavailable:
        return None


def owned_by(conversation_id: Optional[str], user_id: Optional[str]) -> bool:
    """Whether ``user_id`` may read this conversation. An unclaimed id is
    nobody's, and so is one the store cannot answer for."""
    if not conversation_id:
        return False
    try:
        owner = _read_owner(conversation_id)
    except state_store.StorageUnavailable:
        return False
    return owner is not None and owner == (user_id or "")


def _has_history(conversation_id: str) -> bool:
    stored = state_store.get_json(conversation_id, _HISTORY_KEY, strict=True)
    return isinstance(stored, list) and len(stored) > 0


def ensure_owner(conversation_id: Optional[str], user_id: Optional[str]) -> None:
    """Claim an unclaimed conversation for ``user_id``; refuse another user's.

    Raises ``ConversationOwnedByAnother``, ``ConversationUnclaimable`` or
    ``state_store.StorageUnavailable``. Never claims on a guess.
    """
    if not conversation_id:
        return
    me = user_id or ""
    owner = _read_owner(conversation_id)
    if owner is None:
        if _has_history(conversation_id):
            raise ConversationUnclaimable(conversation_id)
        if not state_store.claim_text(
            conversation_id, _OWNER_KEY, json.dumps({"user": me})
        ):
            if _read_owner(conversation_id) != me:  # somebody else won the race
                raise ConversationOwnedByAnother(conversation_id)
        return
    if owner != me:
        raise ConversationOwnedByAnother(conversation_id)
    # Refresh: the claim lives as long as the history it protects.
    state_store.set_json(conversation_id, _OWNER_KEY, {"user": me})


def require_owner(conversation_id: str, headers: Mapping[str, Any]) -> None:
    """For the read routes: 404 unless the caller owns the conversation."""
    from fastapi import HTTPException

    if not owned_by(conversation_id, identity_from_headers(headers)):
        raise HTTPException(status_code=404, detail="Conversation not found")
