"""
Context-preserving sync→async bridge for CrewAI tools.

CrewAI tools execute synchronously, often inside a thread that already has a
running event loop (flow execution) or inside a plain worker thread. Tools
that need to await coroutines (LLMManager.completion, ToolSessionProvider
sessions) must bridge to async without losing the request-scoped ContextVars
(UserContext group/token) — new threads start with an EMPTY context, so a bare
``ThreadPoolExecutor.submit(asyncio.run, coro)`` silently drops the group_id
and OBO token, breaking multi-tenant isolation and LLM auth.

All tools must use :func:`run_async_with_context` instead of hand-rolled
executors so context propagation is guaranteed in one place.
"""

import asyncio
import contextvars
import logging
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 300


def run_async_with_context(coro, timeout: float = DEFAULT_TIMEOUT):
    """Run a coroutine from sync code, preserving the caller's ContextVars.

    Handles both cases:
    1. Caller is inside a running event loop — offload to a worker thread
       (with the caller's context copied in) and ``asyncio.run`` there.
    2. No running loop — run directly via ``asyncio.run`` in this thread
       (context is already correct).

    Args:
        coro: The coroutine to execute.
        timeout: Max seconds to wait when offloading to a worker thread.

    Returns:
        The coroutine's result.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    ctx = contextvars.copy_context()
    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(ctx.run, asyncio.run, coro).result(timeout=timeout)


def run_sync_with_context(fn, timeout: float = DEFAULT_TIMEOUT):
    """Run a blocking callable, offloading to a worker thread if needed.

    If the current thread has a running event loop, the callable (which may
    block on sleeps or run its own loops) is offloaded to a worker thread with
    the caller's ContextVars copied in. Otherwise it runs inline.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return fn()

    ctx = contextvars.copy_context()
    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(ctx.run, fn).result(timeout=timeout)
