"""One long-lived event loop for the memory backends' sync→async bridge.

The unified ``StorageBackend`` protocol is synchronous, but every real backend
talks to an async client. The bridge used to be a fresh
``ThreadPoolExecutor`` + event loop per call, which paid thread and loop setup
on every memory operation AND gave loop-bound resources (the shared aiohttp
session, cached auth, the SQLAlchemy engine) no stable home — so
``_is_engine_loop_stale()`` tripped on EVERY Lakebase operation, forcing engine
recreation plus ~3 Databricks control-plane round trips before each <10ms
pgvector query (PERF-012 / PERF-013).

One loop on one daemon thread serves every memory operation instead. It lives
here, in its own module, because it is shared: it used to sit inside the
Databricks Vector Search backend, and the Lakebase backend reached across to
import it — so retiring Vector Search would have taken the loop with it.
"""

from __future__ import annotations

import asyncio
import threading

_BRIDGE_LOOP: asyncio.AbstractEventLoop | None = None
_BRIDGE_LOCK = threading.Lock()


def get_bridge_loop() -> asyncio.AbstractEventLoop:
    """The shared bridge loop, started on first use."""
    global _BRIDGE_LOOP
    with _BRIDGE_LOCK:
        if _BRIDGE_LOOP is None or _BRIDGE_LOOP.is_closed():
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=loop.run_forever, name="kasal-memory-bridge", daemon=True
            )
            thread.start()
            _BRIDGE_LOOP = loop
        return _BRIDGE_LOOP


def run_on_bridge_loop(coro: object) -> object:
    """Run ``coro`` on the bridge loop and block for its result.

    Non-coroutines pass straight through, so a backend method that short-circuits
    to a plain value needs no special casing at the call site.
    """
    if not asyncio.iscoroutine(coro):
        return coro
    return asyncio.run_coroutine_threadsafe(coro, get_bridge_loop()).result()


def shutdown_bridge_loop(timeout: float = 10.0) -> None:
    """Dispose the bridge loop's Lakebase engine and stop the loop. Never raises.

    Call at the end of a crew/flow SUBPROCESS, after ``flush_memory_writes``. The
    bridge loop is a daemon thread running ``run_forever``; on interpreter exit it
    is killed abruptly, so a thread-local Lakebase engine bound to it (memory
    backends run all their async DB work here) is orphaned — its pooled asyncpg
    connection then surfaces as "Event loop is closed" and a GC "non-checked-in
    connection" during teardown. Disposing the engine ON the bridge loop first,
    then stopping the loop, returns those connections cleanly.

    No-op in the long-lived server process is fine too: nothing recreates the loop
    unless another memory op needs it, and ``get_bridge_loop`` rebuilds it lazily.
    """
    global _BRIDGE_LOOP
    with _BRIDGE_LOCK:
        loop = _BRIDGE_LOOP
        _BRIDGE_LOOP = None
    if loop is None or loop.is_closed():
        return

    async def _dispose() -> None:
        # Runs ON the bridge thread, so ``_thread_local`` here is the bridge
        # thread's — exactly the factory that needs disposing.
        try:
            from src.db.lakebase_session import (
                dispose_thread_local_lakebase_factory,
            )

            await dispose_thread_local_lakebase_factory()
        except Exception:  # noqa: BLE001 — teardown must never raise
            pass

    try:
        asyncio.run_coroutine_threadsafe(_dispose(), loop).result(timeout=timeout)
    except Exception:  # noqa: BLE001 — bridge may already be gone; never raise
        pass
    try:
        loop.call_soon_threadsafe(loop.stop)
    except Exception:  # noqa: BLE001
        pass
