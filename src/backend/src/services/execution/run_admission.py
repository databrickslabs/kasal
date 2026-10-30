"""One limit on concurrently running Agent Builder and Flow Builder runs.

Each crew or flow run is a spawned Python interpreter, and while it lives the
parent holds one thread of ``RUN_WAIT_EXECUTOR`` (its result reader, see
``run_wait.py``) and at most one outstanding read on ``EVENT_RELAY_EXECUTOR``.
Both pools have ``RUN_WAIT_MAX_WORKERS`` threads. Before this gate, nothing
capped how many runs started, so run 17 and later waited silently inside the
pool, and a finished run's completion could queue behind other runs' whole-run
waits for up to an hour.

## Over the limit, a run QUEUES; it is not rejected

Runs are admitted asynchronously: the API, the scheduler, event triggers and
resume all create the ``execution_history`` row and return before the
subprocess is spawned. A rejection at that point cannot become an HTTP error.
It would become a FAILED run, so a transient capacity limit would turn into
lost scheduled work that someone has to re-run by hand. A queued run instead:

- waits on an asyncio future, never on a pool thread, so it cannot stall the
  runs that are already going;
- is visible: its row goes to ``PENDING`` with a message naming the limit
  and its queue position, and back to ``RUNNING`` when it is admitted;
- is stoppable: a stop request removes it from the queue
  (:meth:`RunAdmission.cancel_waiting`), and it never spawns. The same call
  covers a run that was admitted but has not started its process yet: the
  executor claims the start (:meth:`RunAdmission.claim_start`) right before
  ``process.start()``, and a stop that came first makes that claim fail;
- does not use up its timeout: the executors start the run clock after
  admission.

## Where the limit comes from

The operator sets it as an engine-config row (``engine_name="kasal"``,
``config_key="max_concurrent_runs"``). There is no environment variable. The
value is clamped to the pool size (``MAX_CONCURRENT_RUNS_CEILING``), because a
limit above it would bring back the silent in-pool wait this gate removes. With no
row, the default is the pool size: nothing that ran before is queued now,
except what used to stall in the pool.

The gate re-reads the row at most every ``_LIMIT_CACHE_SECONDS``. A change made
through ``EngineConfigService`` in this process takes effect at once
(:meth:`RunAdmission.apply_limit` / :meth:`RunAdmission.invalidate_limit`), and
a raised limit admits queued runs immediately rather than when a slot frees.

## One slot per execution id

A second ``acquire`` for an id that already holds or awaits a slot is REJECTED
with :class:`DuplicateRunError`. Making it idempotent would let two processes
run under one slot (the first release frees it while the second still runs),
and overwriting the entry, as the gate used to, did the same while also
leaving the first run unstoppable once the executor's handle was replaced.

The gate lives in the parent process and is per server process, like the pool
it protects.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Deque,
    Dict,
    Optional,
    Set,
)

from src.services.execution.blocking_pools import (
    EVENT_RELAY_MAX_WORKERS,
    RUN_WAIT_MAX_WORKERS,
)

logger = logging.getLogger(__name__)

CONFIG_ENGINE_NAME = "kasal"
CONFIG_KEY = "max_concurrent_runs"
# Every live run needs a reader thread and a relay thread; above this, one of
# them would wait inside a pool.
MAX_CONCURRENT_RUNS_CEILING = min(RUN_WAIT_MAX_WORKERS, EVENT_RELAY_MAX_WORKERS)
DEFAULT_MAX_CONCURRENT_RUNS = MAX_CONCURRENT_RUNS_CEILING

# The configured value is re-read at most this often: one DB read per run start
# is cheap, but a burst of scheduled runs should not become a burst of reads.
_LIMIT_CACHE_SECONDS = 30.0

LimitProvider = Callable[[], Awaitable[Optional[int]]]
QueuedCallback = Callable[[int, int], Awaitable[None]]


class RunCancelledWhileQueued(Exception):
    """A stop request reached a run before its process started: while it was
    queued, or after admission but before ``process.start()``."""


class DuplicateRunError(RuntimeError):
    """An execution id asked for a slot it already holds or is waiting for."""


def clamp_limit(value: Optional[int]) -> int:
    """Clamp a configured limit to ``[1, MAX_CONCURRENT_RUNS_CEILING]``."""
    if value is None:
        return DEFAULT_MAX_CONCURRENT_RUNS
    if value > MAX_CONCURRENT_RUNS_CEILING:
        logger.warning(
            "[run_admission] %s=%s exceeds the run-wait pools (%s threads); using %s",
            CONFIG_KEY,
            value,
            MAX_CONCURRENT_RUNS_CEILING,
            MAX_CONCURRENT_RUNS_CEILING,
        )
        return MAX_CONCURRENT_RUNS_CEILING
    return max(1, value)


def configured_limit_of(row: Any) -> Optional[int]:
    """The limit an engine-config row sets, or None (no row, disabled, junk)."""
    if row is None or getattr(row, "enabled", True) is False:
        return None
    try:
        return int(str(row.config_value).strip())
    except (TypeError, ValueError):
        logger.warning(
            "[run_admission] ignoring non-integer %s=%r",
            CONFIG_KEY,
            row.config_value,
        )
        return None


async def read_configured_limit() -> Optional[int]:
    """The operator's limit from the engine-config table, or None if unset.

    A disabled or unparseable row counts as unset. Read through the settings
    SERVICE on a routed session, the same way every other operator setting is.
    """
    from src.db.session import routed_scoped_session
    from src.services.settings.engine import EngineConfigService

    async with routed_scoped_session() as session:
        row = await EngineConfigService(session).find_by_engine_and_key(
            CONFIG_ENGINE_NAME, CONFIG_KEY
        )
    return configured_limit_of(row)


@dataclass
class _Waiter:
    execution_id: str
    kind: str
    future: "asyncio.Future[bool]"
    queued_at: float = field(default_factory=time.monotonic)


class RunAdmission:
    """A FIFO admission gate whose limit can change while runs are waiting.

    ``asyncio.Semaphore`` cannot be resized, and the limit is an operator
    setting, so the gate keeps its own count. All state is touched from the
    event loop only; there is no lock because there is no await between a
    check and its update.
    """

    def __init__(self, limit_provider: Optional[LimitProvider] = None) -> None:
        self._limit_provider: LimitProvider = limit_provider or read_configured_limit
        self._active: Dict[str, str] = {}
        self._waiters: Deque[_Waiter] = deque()
        self._limit: int = DEFAULT_MAX_CONCURRENT_RUNS
        self._limit_read_at: Optional[float] = None
        self._limit_known = False  # a read has succeeded at least once
        # Admitted runs whose executor has claimed the start (see claim_start),
        # and admitted runs a stop reached before that claim.
        self._started: Set[str] = set()
        self._stop_requested: Set[str] = set()

    # -- observation -------------------------------------------------------

    @property
    def active_count(self) -> int:
        return len(self._active)

    @property
    def queued_count(self) -> int:
        return len(self._waiters)

    def is_queued(self, execution_id: str) -> bool:
        return any(w.execution_id == execution_id for w in self._waiters)

    def snapshot(self) -> Dict[str, object]:
        """What the gate is doing now, for metrics and logs."""
        return {
            "limit": self._limit,
            "active": dict(self._active),
            "queued": [w.execution_id for w in self._waiters],
            "starting": sorted(set(self._active) - self._started),
        }

    # -- the limit ---------------------------------------------------------

    async def current_limit(self) -> int:
        now = time.monotonic()
        if (
            self._limit_read_at is not None
            and now - self._limit_read_at < _LIMIT_CACHE_SECONDS
        ):
            return self._limit
        try:
            configured = await self._limit_provider()
            self._limit_known = True
        except Exception as e:  # noqa: BLE001 — a config read must not stop a run
            configured = self._limit if self._limit_known else None
            logger.warning(
                "[run_admission] could not read %s (%s); using limit %s",
                CONFIG_KEY,
                e,
                clamp_limit(configured),
            )
        self._set_limit(clamp_limit(configured), now)
        return self._limit

    def invalidate_limit(self) -> None:
        """Force the next admission to re-read the configured limit."""
        self._limit_read_at = None

    def apply_limit(self, configured: Optional[int]) -> int:
        """Use a limit the caller just wrote (None: the row is gone/disabled).

        Takes effect now: a raised limit admits queued runs straight away. A
        lowered one never stops a running run; it only holds new ones back.
        """
        self._limit_known = True
        self._set_limit(clamp_limit(configured), time.monotonic())
        logger.info("[run_admission] limit set to %s", self._limit)
        return self._limit

    def _set_limit(self, limit: int, read_at: float) -> None:
        raised = limit > self._limit
        self._limit = limit
        self._limit_read_at = read_at
        if raised:
            self._admit_waiters()

    # -- admission ---------------------------------------------------------

    @asynccontextmanager
    async def slot(
        self,
        execution_id: str,
        kind: str,
        on_queued: Optional[QueuedCallback] = None,
        on_admitted_after_queue: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> AsyncIterator[None]:
        """Hold one run slot for the body of the ``async with``.

        Raises:
            RunCancelledWhileQueued: a stop request removed the run from the
                queue before it was admitted.
        """
        queued = await self.acquire(execution_id, kind, on_queued=on_queued)
        try:
            if queued and on_admitted_after_queue is not None:
                await _call_quietly(on_admitted_after_queue)
            yield
        finally:
            self.release(execution_id)

    async def acquire(
        self,
        execution_id: str,
        kind: str,
        on_queued: Optional[QueuedCallback] = None,
    ) -> bool:
        """Take a slot, waiting in FIFO order if none is free.

        Returns True when the run had to queue.

        Raises:
            DuplicateRunError: ``execution_id`` already holds or awaits a slot.
        """
        self._reject_duplicate(execution_id)
        limit = await self.current_limit()
        # current_limit() may have awaited a DB read: check again, so two
        # acquires for one id that raced through that read cannot both pass.
        self._reject_duplicate(execution_id)
        if not self._waiters and len(self._active) < limit:
            self._active[execution_id] = kind
            return False

        waiter = _Waiter(execution_id, kind, asyncio.get_running_loop().create_future())
        self._waiters.append(waiter)
        position = len(self._waiters)
        logger.warning(
            "[run_admission] %s run %s queued at position %d: %d/%d run slots in use",
            kind,
            execution_id,
            position,
            len(self._active),
            limit,
        )
        if on_queued is not None:
            await _call_quietly(on_queued, position, limit)

        try:
            admitted = await waiter.future
        except asyncio.CancelledError:
            # Admitted in the same tick the task was cancelled: give the slot
            # back, or it leaks and the gate shrinks by one for good.
            if waiter.future.done() and not waiter.future.cancelled():
                if waiter.future.result():
                    self.release(execution_id)
            else:
                self._discard(waiter)
            raise
        if not admitted:
            raise RunCancelledWhileQueued(execution_id)
        logger.info(
            "[run_admission] %s run %s admitted after %.1fs in the queue",
            kind,
            execution_id,
            time.monotonic() - waiter.queued_at,
        )
        return True

    def _reject_duplicate(self, execution_id: str) -> None:
        if execution_id in self._active or self.is_queued(execution_id):
            logger.error(
                "[run_admission] refused a second run slot for execution %s: "
                "it already holds or awaits one",
                execution_id,
            )
            raise DuplicateRunError(
                f"Execution {execution_id} already holds or awaits a run slot"
            )

    def release(self, execution_id: str) -> None:
        self._started.discard(execution_id)
        self._stop_requested.discard(execution_id)
        if self._active.pop(execution_id, None) is not None:
            self._admit_waiters()

    def claim_start(self, execution_id: str) -> None:
        """Called by an executor immediately before ``process.start()``.

        With no ``await`` between this and the start, a stop either lands
        before it (and is honoured here) or finds the started process.

        Raises:
            RunCancelledWhileQueued: a stop reached the run after it was
                admitted and before this claim.
        """
        if execution_id in self._stop_requested:
            self._stop_requested.discard(execution_id)
            logger.info(
                "[run_admission] run %s was stopped before its process started",
                execution_id,
            )
            raise RunCancelledWhileQueued(execution_id)
        self._started.add(execution_id)

    def cancel_waiting(self, execution_id: str) -> bool:
        """Stop a run that has not started its process, so it never spawns.

        A queued run leaves the queue. An admitted run whose executor has not
        claimed the start yet is marked, and :meth:`claim_start` refuses it.
        True in either case; False when the run is unknown or already started
        (the caller then stops the process itself).
        """
        for waiter in list(self._waiters):
            if waiter.execution_id == execution_id:
                self._discard(waiter)
                if not waiter.future.done():
                    waiter.future.set_result(False)
                logger.info("[run_admission] removed queued run %s", execution_id)
                return True
        if execution_id in self._active and execution_id not in self._started:
            self._stop_requested.add(execution_id)
            logger.info(
                "[run_admission] stop requested for admitted run %s before its "
                "process started",
                execution_id,
            )
            return True
        return False

    def _discard(self, waiter: _Waiter) -> None:
        try:
            self._waiters.remove(waiter)
        except ValueError:
            pass

    def _admit_waiters(self) -> None:
        while self._waiters and len(self._active) < self._limit:
            waiter = self._waiters.popleft()
            if waiter.future.done():
                continue
            self._active[waiter.execution_id] = waiter.kind
            try:
                waiter.future.set_result(True)
            except RuntimeError:
                # Its event loop is gone (a test that ended mid-wait).
                self._active.pop(waiter.execution_id, None)


def queue_status_callbacks(execution_id: str) -> Dict[str, Any]:
    """``slot()`` keyword arguments that show a queued run on its row.

    Both writes use ``preserve_terminal``, so a stop that lands while the
    run is queued is never overwritten back to PENDING or RUNNING.
    """
    from src.models.execution_status import ExecutionStatus
    from src.services.execution.status import ExecutionStatusService

    async def on_queued(position: int, limit: int) -> None:
        await ExecutionStatusService.update_status(
            job_id=execution_id,
            status=ExecutionStatus.PENDING.value,
            message=(
                f"Queued: all {limit} run slots are in use (position {position}). "
                "The run starts when a slot frees."
            ),
            preserve_terminal=True,
        )

    async def on_admitted() -> None:
        await ExecutionStatusService.update_status(
            job_id=execution_id,
            status=ExecutionStatus.RUNNING.value,
            message="Run slot acquired; starting the run",
            preserve_terminal=True,
        )

    return {"on_queued": on_queued, "on_admitted_after_queue": on_admitted}


def stopped_while_queued(execution_id: str) -> Dict[str, Any]:
    """The executors' result for a run stopped before its process started."""
    return {
        "status": "STOPPED",
        "execution_id": execution_id,
        "message": "Execution was stopped before it started (queued or starting)",
    }


async def _call_quietly(callback: Callable[..., Awaitable[None]], *args: int) -> None:
    """Run a status callback; a failed status write must not lose the slot."""
    try:
        await callback(*args)
    except Exception as e:  # noqa: BLE001 — status is advisory here
        logger.warning("[run_admission] status update failed: %s", e)


# The one gate both subprocess executors share.
run_admission = RunAdmission()
