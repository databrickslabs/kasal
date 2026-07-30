"""Requests-per-minute throttling for LLM calls.

``max_rpm`` has existed on ``Agent`` and ``Crew`` since the engine was
vendored, is offered in the agent form, is carried through the config builders
— and was read by nothing. A knob shaped like a cap that is not one is worse
than no knob: the form promised pacing that never happened, and the eleven
parallel searches that blew a deep-research budget went out unthrottled with
"Max RPM 10" on screen.

**This blocks the calling thread.** That is safe here and only here: both async
entry points (``Agent.kickoff_async``, ``Crew.kickoff_async``) hand off through
``asyncio.to_thread``, and the crew's parallel tasks run on a thread pool, so
the transport never executes on the event loop. Do not lift this into a
coroutine path without replacing the sleep.

The window is fixed rather than sliding: a counter that resets every 60s, which
is what ``max_rpm`` means to the person typing it in the form. A sliding window
smooths bursts better and needs per-request timestamps; if that is ever wanted,
it goes here behind the same ``acquire()``.
"""

import logging
import threading
import time
from typing import Callable

logger = logging.getLogger(__name__)

#: Seconds per counting window. "Per minute" is the whole contract.
WINDOW_SECONDS = 60.0


class RPMController:
    """Lets ``max_rpm`` calls through per window, sleeping out the remainder.

    One instance is shared by every agent in a run (stamped by
    ``Crew.kickoff``), so the limit applies to the RUN rather than being
    silently multiplied by the number of agents. Thread-safe because a crew's
    parallel tasks acquire from several threads at once.
    """

    def __init__(
        self,
        max_rpm: int,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_rpm <= 0:
            raise ValueError(f"max_rpm must be positive, got {max_rpm}")
        self.max_rpm = max_rpm
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._used = 0
        self._window_start = clock()

    def acquire(self) -> None:
        """Take a slot, waiting for the next window if this one is spent."""
        while True:
            with self._lock:
                now = self._clock()
                elapsed = now - self._window_start
                if elapsed >= WINDOW_SECONDS:
                    self._window_start = now
                    self._used = 0
                    elapsed = 0.0
                if self._used < self.max_rpm:
                    self._used += 1
                    return
                wait = WINDOW_SECONDS - elapsed
            # Outside the lock: another thread whose window has rolled over
            # must be able to proceed while this one waits. Re-checked on the
            # next pass rather than assumed, so a slot freed by a rollover is
            # taken under the lock like any other.
            logger.info(
                "max_rpm=%d reached; waiting %.1fs for the next window",
                self.max_rpm,
                wait,
            )
            self._sleep(max(wait, 0.0))


def throttle(from_agent: object) -> None:
    """Pace one LLM call against its agent's limiter, if it has one.

    Duck-typed on purpose, like ``resolve_execution_budget``: the transport
    stays independent of the runtime's Agent class, and a direct LLM call with
    no agent is simply unthrottled.
    """
    controller = getattr(from_agent, "rpm_controller", None)
    if isinstance(controller, RPMController):
        controller.acquire()
