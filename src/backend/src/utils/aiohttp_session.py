"""
Shared aiohttp ClientSession pool, keyed by running event loop.

PERF-012/PERF-020: embedding and Vector Search call sites used to create a
fresh aiohttp.ClientSession (new TCP + TLS handshake, ~50-150ms against a
Databricks workspace) for every single HTTP call. aiohttp sessions are bound
to the event loop they were created on, so a plain module-level singleton
breaks under Kasal's multi-loop reality (FastAPI loop, crew subprocess loops,
sync-bridge thread loops). Instead we keep one session per *live* loop and
hand it out via a context manager that never closes it.

Usage (drop-in for ``async with aiohttp.ClientSession() as session:``):

    from src.utils.aiohttp_session import shared_client_session

    async with shared_client_session() as session:
        async with session.post(url, json=payload, timeout=timeout) as resp:
            ...

Pass per-request ``timeout=aiohttp.ClientTimeout(...)`` to the request call —
session-level timeouts don't apply here because the session is shared.
"""

import asyncio
import contextlib
import logging
from typing import Dict, Tuple

import aiohttp

logger = logging.getLogger(__name__)

# loop id -> (loop, session). The loop reference lets us prune entries whose
# loop has been closed (their sockets die with the loop; the OS reclaims them).
_SESSIONS: Dict[int, Tuple[asyncio.AbstractEventLoop, aiohttp.ClientSession]] = {}


def _prune_dead_loops() -> None:
    for key in [k for k, (loop, _) in _SESSIONS.items() if loop.is_closed()]:
        _SESSIONS.pop(key, None)


async def get_shared_session() -> aiohttp.ClientSession:
    """Return the shared ClientSession for the current event loop.

    Created lazily on first use per loop; reused (HTTP keep-alive, no
    per-call TLS handshake) for every subsequent call on that loop.
    """
    loop = asyncio.get_running_loop()
    key = id(loop)
    entry = _SESSIONS.get(key)
    if entry is not None:
        cached_loop, session = entry
        if cached_loop is loop and not session.closed:
            return session
        _SESSIONS.pop(key, None)

    _prune_dead_loops()
    session = aiohttp.ClientSession()
    _SESSIONS[key] = (loop, session)
    logger.debug("Created shared aiohttp session for loop %s (%d live)", key, len(_SESSIONS))
    return session


@contextlib.asynccontextmanager
async def shared_client_session():
    """Async context manager yielding the shared session WITHOUT closing it.

    Drop-in replacement for ``async with aiohttp.ClientSession() as s:`` at
    call sites — same shape, but exit does not tear down the connection pool.
    """
    yield await get_shared_session()


async def close_shared_sessions() -> None:
    """Close all shared sessions (app shutdown / test teardown)."""
    for key, (loop, session) in list(_SESSIONS.items()):
        _SESSIONS.pop(key, None)
        if not session.closed and not loop.is_closed():
            try:
                await session.close()
            except Exception:
                pass
