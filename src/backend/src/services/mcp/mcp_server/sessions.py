"""Open MCP sessions, and the server-initiated stream that keeps them current.

Kasal's tool list is not fixed: there is one tool per published capability, so
publishing a crew adds a tool and deleting one removes it. A client fetches
``tools/list`` when it connects and — with nothing to tell it otherwise — never
asks again. That is not a theoretical staleness: a workspace watched an editor
offer ten tools for crews that had been deleted, while the crew published five
minutes earlier was missing, and the only cure was to reconnect.

The transport already allows the fix. Streamable HTTP lets a client open a GET
on the same endpoint and hold it as an SSE stream for server-initiated
messages; this module is the registry of those streams and the fan-out onto
them. With it, the server can honestly declare ``tools.listChanged: true`` and
push ``notifications/tools/list_changed`` when the catalogue moves — which is
what let the generic ``list_crews`` and ``start_crew`` tools be retired, since
their only remaining job was working around a list that could not refresh.

What this deliberately is not
=============================

**Not a message bus.** One notification type, no server-initiated requests, no
resource subscriptions. A queue per session with a small bound, and a full queue
drops rather than blocks: the notification says "the list moved", so a client
that misses one and receives the next is in exactly the same state.

**Not shared between workers.** The registry is in-process. Under multiple
uvicorn workers a publish handled by worker A does not reach a stream held by
worker B, and that client keeps its stale list until it reconnects — the
behaviour everyone had before this existed. Fixing it needs a real broker, and
pretending otherwise here would hide which deployments are covered.
"""

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

#: Messages a session may fall behind by before the oldest are dropped. They are
#: all the same notification, so a backlog carries no extra information.
_QUEUE_LIMIT = 16

#: Upper bound on tracked sessions. A client that disconnects without a DELETE
#: leaves its entry until the stream's `finally` runs; this is the backstop for
#: the case where that never happens.
_MAX_SESSIONS = 512

#: How often an idle stream emits an SSE comment. Without it, proxies and load
#: balancers close a connection that has been silent for a minute or two, and
#: the client sees a broken stream rather than a quiet one.
_KEEPALIVE_SECONDS = 20.0


@dataclass
class _Session:
    id: str
    group_ids: tuple = ()
    queue: "asyncio.Queue[str]" = field(
        default_factory=lambda: asyncio.Queue(maxsize=_QUEUE_LIMIT)
    )


_sessions: Dict[str, _Session] = {}


def open_session(group_ids: Optional[Sequence[str]] = None) -> str:
    """Register a session and return its id, for the ``Mcp-Session-Id`` header."""
    if len(_sessions) >= _MAX_SESSIONS:
        # Oldest first: dict preserves insertion order, and a session old enough
        # to be evicted here is one whose stream never closed properly.
        stale = next(iter(_sessions))
        _sessions.pop(stale, None)
        logger.warning("[mcp] session table full; evicted %s", stale)

    session_id = uuid.uuid4().hex
    _sessions[session_id] = _Session(id=session_id, group_ids=tuple(group_ids or ()))
    return session_id


def adopt_session(session_id: str, group_ids: Optional[Sequence[str]] = None) -> str:
    """Return an existing session, or register the id the client presented.

    A client may open the GET stream with a session id from a previous process
    — after a reload, say. Adopting it beats refusing: the id is the client's to
    choose, nothing is authorised by it (the caller is resolved from headers on
    every request), and refusing would leave that client with no stream at all.
    """
    known = _sessions.get(session_id)
    if known is not None:
        if group_ids:
            known.group_ids = tuple(group_ids)
        return session_id
    _sessions[session_id] = _Session(id=session_id, group_ids=tuple(group_ids or ()))
    return session_id


def close_session(session_id: str) -> bool:
    """Forget a session. True when there was one."""
    return _sessions.pop(session_id, None) is not None


def active_sessions() -> List[str]:
    return list(_sessions)


def _notification(method: str, params: Optional[dict] = None) -> str:
    message = {"jsonrpc": "2.0", "method": method}
    if params:
        message["params"] = params
    return json.dumps(message)


def _deliver(session: _Session, payload: str) -> None:
    try:
        session.queue.put_nowait(payload)
    except asyncio.QueueFull:
        # Drop the oldest and keep the newest: every message here is the same
        # "refetch your tools" and the freshest one is the only one that matters.
        try:
            session.queue.get_nowait()
            session.queue.put_nowait(payload)
        except (asyncio.QueueEmpty, asyncio.QueueFull):  # pragma: no cover
            logger.debug("[mcp] could not queue notification for %s", session.id)


def notify_tools_changed(group_ids: Optional[Sequence[str]] = None) -> int:
    """Push ``notifications/tools/list_changed`` to the sessions it concerns.

    Group-scoped: a publish in one workspace must not make every other
    workspace's client refetch. A session with no recorded groups receives
    everything, because "we do not know which tenant this stream belongs to" is
    not a reason to withhold a message whose only effect is a refetch.
    """
    if not _sessions:
        return 0
    wanted = set(group_ids or ())
    payload = _notification("notifications/tools/list_changed")

    sent = 0
    for session in list(_sessions.values()):
        if wanted and session.group_ids and not wanted.intersection(session.group_ids):
            continue
        _deliver(session, payload)
        sent += 1
    if sent:
        logger.info("[mcp] told %d session(s) the tool list changed", sent)
    return sent


async def stream(session_id: str) -> AsyncIterator[bytes]:
    """SSE frames for one session: notifications, plus a periodic keepalive.

    Ends when the client disconnects, which is what removes the session — the
    generator's ``finally`` is the only reliable close signal an HTTP stream
    gives us.
    """
    session = _sessions.get(session_id)
    if session is None:
        return
    try:
        while True:
            try:
                payload = await asyncio.wait_for(
                    session.queue.get(), timeout=_KEEPALIVE_SECONDS
                )
            except asyncio.TimeoutError:
                yield b": keepalive\n\n"
                continue
            yield f"data: {payload}\n\n".encode("utf-8")
    except asyncio.CancelledError:
        raise
    finally:
        close_session(session_id)
        logger.debug("[mcp] session %s stream closed", session_id)


def _on_catalogue_changed(group_ids: Optional[List[str]] = None) -> None:
    notify_tools_changed(group_ids)


def register() -> None:
    """Listen for catalogue changes. Called once, at import."""
    from src.services.publications.signals import on_catalogue_changed

    on_catalogue_changed(_on_catalogue_changed)


register()
