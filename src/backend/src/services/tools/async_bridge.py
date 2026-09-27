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
import threading
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any, Callable, Coroutine, Optional, Tuple, TypeVar

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 300

#: Name prefix of the worker threads this module starts (tests count them).
WORKER_THREAD_PREFIX = "async-bridge"

_T = TypeVar("_T")


class _TaskHandle:
    """Where a worker's task runs, so a timed-out caller can cancel it.

    The caller and the worker race: the timeout can fire before the worker's
    loop has started the task. Whichever side comes second sees the other's
    state under the lock, so a cancel is never lost.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._task: Optional["asyncio.Task[Any]"] = None
        self._cancelled = False

    def attach(
        self, loop: asyncio.AbstractEventLoop, task: "asyncio.Task[Any]"
    ) -> bool:
        """Record the running task; False if the caller already gave up."""
        with self._lock:
            if self._cancelled:
                return False
            self._loop, self._task = loop, task
            return True

    def cancel(self) -> None:
        """Cancel the task on its own loop (thread-safe, idempotent)."""
        with self._lock:
            self._cancelled = True
            loop, task = self._loop, self._task
        if loop is None or task is None:
            return
        try:
            loop.call_soon_threadsafe(task.cancel)
        except RuntimeError:  # loop already closed: the task has finished
            pass


def _settle(future: "Future[Any]", fn: Callable[[], Any]) -> None:
    """Run ``fn`` and put its outcome on ``future``."""
    try:
        result = fn()
    except BaseException as exc:  # noqa: BLE001 — handed to the caller as-is
        future.set_exception(exc)
    else:
        future.set_result(result)


async def _dispose_thread_local_lakebase() -> None:
    """Dispose a Lakebase engine the coroutine bound to this worker's loop.

    ``get_lakebase_session`` binds a thread-local engine to the running loop;
    ``asyncio.run`` would close the loop without disposing it, orphaning the
    connection. The worker thread is fresh, so any such engine is this call's.
    Never raises; imported lazily so the bridge works without a Lakebase build.
    """
    try:
        from src.db.lakebase_session import dispose_thread_local_lakebase_factory

        await dispose_thread_local_lakebase_factory()
    except Exception as exc:  # noqa: BLE001 — teardown must never fail the call
        logger.debug("thread-local Lakebase dispose skipped: %s", exc)


def _start_worker(target: Callable[[], None]) -> None:
    """Start ``target`` on a daemon thread that nobody joins.

    A daemon thread, not a (shared) ``ThreadPoolExecutor``: a timed-out caller
    must be able to walk away. An executor's ``with``-exit joins its worker,
    which is how a 0.5 s timeout used to return after the full 3 s of work;
    its threads are also joined at interpreter exit, so one stuck call would
    hold up the crew/flow subprocess's shutdown. And a bounded shared pool
    deadlocks when an offloaded coroutine calls a sync tool that bridges again.
    """
    threading.Thread(
        target=target,
        name=f"{WORKER_THREAD_PREFIX}-{threading.get_ident()}",
        daemon=True,
    ).start()


def _wait(
    future: "Future[_T]", timeout: Optional[float], on_abandon: Callable[[], None]
) -> _T:
    """Wait up to ``timeout`` for ``future``; on giving up, call ``on_abandon``."""
    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError:
        on_abandon()
        raise TimeoutError(
            f"async_bridge call did not finish within {timeout}s"
        ) from None
    except BaseException:
        if not future.done():  # interrupted while waiting (e.g. KeyboardInterrupt)
            on_abandon()
        raise


def run_async_with_context(
    coro: Coroutine[Any, Any, _T], timeout: float = DEFAULT_TIMEOUT
) -> _T:
    """Run a coroutine from sync code, preserving the caller's ContextVars.

    The coroutine always runs on its own event loop in a fresh daemon thread,
    with a copy of the caller's context. That works whether or not the caller
    is inside a running loop, and it makes ``timeout`` real in both cases:

    - The call returns (raising ``TimeoutError``) within about ``timeout``
      seconds. It never waits for the worker thread.
    - The timed-out coroutine is cancelled on its loop
      (``call_soon_threadsafe(task.cancel)``), so it stops at its next
      ``await`` instead of running on, holding DB sessions and a thread.
      A coroutine that blocks without awaiting, or swallows the
      ``CancelledError``, cannot be stopped: its daemon thread runs until it
      finishes, but the caller has still returned.

    Args:
        coro: The coroutine to execute.
        timeout: Max seconds to wait for the result.

    Returns:
        The coroutine's result; its exception propagates unchanged.
    """
    ctx = contextvars.copy_context()
    handle = _TaskHandle()
    future: "Future[_T]" = Future()

    async def _driver() -> _T:
        task = asyncio.current_task()
        assert task is not None
        if not handle.attach(asyncio.get_running_loop(), task):
            close = getattr(coro, "close", None)
            if close is not None:  # timed out before it started: never run it
                close()
            raise asyncio.CancelledError()
        try:
            return await coro
        finally:
            await _dispose_thread_local_lakebase()

    def _abandon() -> None:
        logger.warning(
            "run_async_with_context: %r exceeded %ss; cancelling it",
            getattr(coro, "__qualname__", coro),
            timeout,
        )
        handle.cancel()

    _start_worker(lambda: _settle(future, lambda: ctx.run(asyncio.run, _driver())))
    return _wait(future, timeout, _abandon)


def run_sync_with_context(fn: Callable[[], _T], timeout: float = DEFAULT_TIMEOUT) -> _T:
    """Run a blocking callable, offloading to a worker thread if needed.

    If the current thread has a running event loop, the callable (which may
    block on sleeps or run its own loops) is offloaded to a daemon worker
    thread with the caller's ContextVars copied in, and the call raises
    ``TimeoutError`` within about ``timeout`` seconds. A sync callable cannot
    be cancelled, so a timed-out one finishes in the background; its result is
    discarded.

    Otherwise it runs inline, on the caller's thread and without a timeout —
    offloading would only add a thread that could not be stopped either.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return fn()

    ctx = contextvars.copy_context()
    future: "Future[_T]" = Future()
    _start_worker(lambda: _settle(future, lambda: ctx.run(fn)))

    def _abandon() -> None:
        logger.warning(
            "run_sync_with_context: %r exceeded %ss; it keeps running in the "
            "background and its result will be discarded",
            fn,
            timeout,
        )

    return _wait(future, timeout, _abandon)


def workspace_llm_credentials(
    workspace_url: Optional[str] = None, token: Optional[str] = None
) -> Tuple[str, str]:
    """``(workspace_url, token)`` for this run's workspace; given values win.

    For sync tool code that calls a Databricks serving endpoint. Whatever the
    caller did not supply comes from ``get_auth_context`` (OBO -> this group's
    PAT -> app SP), never from ``DATABRICKS_TOKEN`` in the process environment,
    which every workspace shares. Missing values come back as ``""``.
    """
    if workspace_url and token:
        return workspace_url, token
    from src.utils.databricks_auth import get_auth_context

    try:
        auth = run_async_with_context(get_auth_context())
    except Exception as exc:  # noqa: BLE001 — callers degrade to "no LLM"
        logger.warning("Could not resolve Databricks credentials: %s", exc)
        auth = None
    auth_url = (getattr(auth, "workspace_url", "") or "").rstrip("/")
    auth_token = getattr(auth, "token", "") or ""
    return workspace_url or auth_url, token or auth_token
