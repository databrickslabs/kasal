"""Making ``max_execution_time`` actually stop a CrewAI agent.

## What CrewAI does, and why it is not enough

CrewAI honours ``Agent.max_execution_time`` in ``execute_task``: it runs the
agent loop in a ``ThreadPoolExecutor`` and calls ``future.result(timeout=...)``.
That DETECTS the overrun — but Python cannot kill a thread, and the enclosing
``with ThreadPoolExecutor()`` block joins on exit. So the loop keeps running and
the ``TimeoutError`` only surfaces once the agent finishes of its own accord.

Measured on a real run: one task, ``max_execution_time=30``, still making LLM
calls 145 seconds later, no timeout reported until the very end. The cap
reports; it does not bound.

## What the Kasal harness does instead

The transport enforces a deadline INSIDE its round loop
(``budget.check_deadline``), so an agent that has run out of time stops making
calls. That is real enforcement, and it is why the same cap works on the other
harness.

``resolve_execution_budget`` builds that deadline as
``min(now + max_execution_time, run_deadline)`` — recomputed on every ``call()``.
Under Kasal one ``call()`` runs the agent's whole turn, so the per-call term
bounds the turn. Under CrewAI the executor owns the tool loop, so one ``call()``
is ONE ROUND: the per-call term restarts every round and bounds nothing.
``run_deadline`` is the only term that survives across calls.

So this module stamps ``run_deadline`` for the duration of each agent turn,
taking the earlier of the turn cap and any run-level ceiling already set by
``Crew.kickoff``. The transport then stops the agent where it always would have.
"""

from __future__ import annotations

import importlib
import time
from contextlib import contextmanager
from typing import Any, Iterator, List, Tuple

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew

#: Set by the crew subclass at kickoff; the run-level ceiling to restore to.
RUN_DEADLINE_ATTR = "_kasal_run_deadline"


def _combined_deadline(agent: Any) -> float | None:
    """The earlier of this turn's cap and the run's ceiling."""
    candidates: List[float] = []

    cap = getattr(agent, "max_execution_time", None)
    if isinstance(cap, (int, float)) and cap > 0:
        candidates.append(time.monotonic() + float(cap))

    run = getattr(agent, RUN_DEADLINE_ATTR, None)
    if isinstance(run, (int, float)):
        candidates.append(float(run))

    return min(candidates) if candidates else None


def _restore(agent: Any) -> None:
    """Put the run-level ceiling back once the turn ends.

    Leaving the TURN deadline in place would make it the run's ceiling: the
    second task would start already past a deadline set for the first.
    """
    run = getattr(agent, RUN_DEADLINE_ATTR, None)
    try:
        object.__setattr__(agent, "run_deadline", run)
    except Exception:  # noqa: BLE001 — never fail a run over bookkeeping
        pass


@contextmanager
def enforce_turn_deadlines() -> Iterator[None]:
    """Bound each agent turn for the duration of a run.

    Driven by CrewAI's own agent lifecycle events, so the clock starts when the
    agent starts working rather than when the crew was assembled — the same
    rule ``Crew.kickoff`` follows for the run-level deadline.
    """
    try:
        bus = importlib.import_module("crewai.events.event_bus").crewai_event_bus
        agent_events = importlib.import_module("crewai.events.types.agent_events")
    except Exception as e:  # noqa: BLE001 — a cap is not worth a failed run
        logger.warning("Could not install CrewAI turn deadlines (%s)", e)
        yield
        return

    def _on_start(source: Any, event: Any) -> None:
        agent = getattr(event, "agent", None) or source
        deadline = _combined_deadline(agent)
        if deadline is None:
            return
        try:
            object.__setattr__(agent, "run_deadline", deadline)
        except Exception as e:  # noqa: BLE001
            logger.debug("Could not stamp a turn deadline: %s", e)
            return
        logger.info(
            "[crewai] agent %r turn bounded to %.0fs",
            getattr(agent, "role", "?"),
            deadline - time.monotonic(),
        )

    def _on_end(source: Any, event: Any) -> None:
        _restore(getattr(event, "agent", None) or source)

    registered: List[Tuple[type, Any]] = []
    for name, handler in (
        ("AgentExecutionStartedEvent", _on_start),
        ("AgentExecutionCompletedEvent", _on_end),
        ("AgentExecutionErrorEvent", _on_end),
    ):
        event_class = getattr(agent_events, name, None)
        if event_class is None:
            continue
        bus.register_handler(event_class, handler)
        registered.append((event_class, handler))

    try:
        yield
    finally:
        for event_class, handler in registered:
            try:
                bus.off(event_class, handler)
            except Exception as e:  # noqa: BLE001 — teardown must not raise
                logger.debug("Could not remove a turn-deadline handler: %s", e)
