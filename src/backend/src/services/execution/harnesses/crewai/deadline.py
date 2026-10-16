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

## The cap is kept off CrewAI's field

CrewAI's own timeout does not just fail to bound the turn — it ends it badly:
``TimeoutError`` fails the task, and a budget error raised inside the pool
comes back as ``RuntimeError``, which ``max_retry_limit`` answers by running the
whole task again, tools included. So ``build_agent`` never sets
``crewai.Agent.max_execution_time``; ``carry_turn_cap`` keeps the value beside
it, where this module and the transport (``budget.turn_cap``) read it. A spent
clock then ends the turn with the "Truncated" partial answer instead.
"""

from __future__ import annotations

import importlib
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator, List, Tuple

from src.core.llm.transport.budget import (
    BUDGET_SPENT_AT_ATTR,
    BUDGET_SPENT_ATTR,
    TURN_CAP_ATTR,
    turn_cap,
)
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().crew

#: Set by the crew subclass at kickoff; the run-level ceiling to restore to.
RUN_DEADLINE_ATTR = "_kasal_run_deadline"


def carry_turn_cap(agent: object, seconds: float | None) -> None:
    """Keep ``seconds`` as the agent's turn cap without arming CrewAI's timeout.

    Also clears CrewAI's field, for an agent CrewAI built itself (the
    hierarchical manager) and had a cap stamped onto it afterwards.
    """
    try:
        object.__setattr__(agent, TURN_CAP_ATTR, seconds)
        object.__setattr__(agent, "max_execution_time", None)
    except Exception as e:  # noqa: BLE001 — a cap is not worth a failed build
        logger.debug("Could not carry max_execution_time: %s", e)


def _combined_deadline(agent: Any) -> float | None:
    """The earlier of this turn's cap and the run's ceiling."""
    candidates: List[float] = []

    cap = turn_cap(agent)
    if cap is not None:
        candidates.append(time.monotonic() + cap)

    run = getattr(agent, RUN_DEADLINE_ATTR, None)
    if isinstance(run, (int, float)):
        candidates.append(float(run))

    return min(candidates) if candidates else None


def _forget_earlier_truncation(event: object) -> None:
    """A new attempt at the task drops a truncation an EARLIER attempt left.

    A guardrail retry or a ``max_retry_limit`` retry re-enters
    ``execute_task`` with a fresh turn clock; if it answers in full, that
    answer is not truncated. Compared by time rather than cleared outright:
    this handler runs on CrewAI's pool, so it can land after the new attempt
    has already been truncated itself (a run ceiling spent before it began).
    """
    task = getattr(event, "task", None)
    marked = getattr(task, BUDGET_SPENT_AT_ATTR, None)
    started = getattr(event, "timestamp", None)
    if not isinstance(marked, float) or not isinstance(started, datetime):
        return
    if marked < started.timestamp():
        try:
            object.__setattr__(task, BUDGET_SPENT_ATTR, None)
        except Exception as e:  # noqa: BLE001 — bookkeeping never fails a run
            logger.debug("Could not clear an earlier truncation: %s", e)


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
        _forget_earlier_truncation(event)
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
