"""Who a conversation belongs to.

A conversation id used to be the only key: whoever knew (or guessed) one could
read its history, poll its progress and surface, and cancel its turn. Now the
first turn claims the conversation for the signed-in user, the claim is
persisted beside the history, and every later turn and every read route checks
it. Identity is derived ONE way — from Databricks' forwarded headers — so a
conversation claimed on a turn is readable by the same person and nobody else.
Without a proxy (a local run) there is no identity and everything belongs to
the anonymous user, which is what a local run means.
"""

from typing import Any, Mapping, Optional

from agent_server import state_store

_OWNER_KEY = "owner"


class ConversationOwnedByAnother(Exception):
    """The conversation was started by someone else."""


def identity_from_headers(headers: Mapping[str, Any]) -> Optional[str]:
    """The signed-in user as Databricks Apps forwards them; None without a proxy."""
    get = headers.get
    return (
        get("x-forwarded-preferred-username")
        or get("x-forwarded-email")
        or get("x-forwarded-user")
        or None
    )


def owner_of(conversation_id: Optional[str]) -> Optional[str]:
    """The user a conversation was claimed for, or None if never claimed."""
    if not conversation_id:
        return None
    stored = state_store.get_json(conversation_id, _OWNER_KEY)
    if isinstance(stored, dict) and "user" in stored:
        return str(stored["user"] or "")
    return None


def owned_by(conversation_id: Optional[str], user_id: Optional[str]) -> bool:
    """Whether ``user_id`` may read this conversation. An unclaimed id is
    nobody's — reading it is refused rather than granted by default."""
    owner = owner_of(conversation_id)
    return owner is not None and owner == (user_id or "")


def ensure_owner(conversation_id: Optional[str], user_id: Optional[str]) -> None:
    """Claim an unclaimed conversation for ``user_id``; refuse another user's."""
    if not conversation_id:
        return
    owner = owner_of(conversation_id)
    if owner is None:
        state_store.set_json(conversation_id, _OWNER_KEY, {"user": user_id or ""})
    elif owner != (user_id or ""):
        raise ConversationOwnedByAnother(conversation_id)


def require_owner(conversation_id: str, headers: Mapping[str, Any]) -> None:
    """For the read routes: 404 unless the caller owns the conversation."""
    from fastapi import HTTPException

    if not owned_by(conversation_id, identity_from_headers(headers)):
        raise HTTPException(status_code=404, detail="Conversation not found")
