"""A CrewAI task's outcome: did its tools work, and did it finish in time?

Kasal's ``Task`` answers both for itself. It starts the tool-outcome ledger at
the top of each task (``reset_tool_ledger``) and, once the task is done, marks
an output whose every tool call failed (``_flag_unavailable_sources``). A
CrewAI task has neither step, so on the default harness the ledger was never
started, every outcome was dropped, and a task whose every Genie, MCP or search
call failed reported a confident answer as a clean success.

``KasalMemoryCrew`` calls the two functions here from the crew seams it already
overrides: ``begin_task`` when the task's context is built (just before it
runs), ``flag_output`` after it completes and BEFORE the memory sinks, so a
degraded answer is never stored as a finding.

The ledger is a ContextVar holding a dict. CrewAI runs a turn and its parallel
tool calls under ``contextvars.copy_context()``, and a copy shares the dict, so
every tool call made for the task lands in it. The task's context is kept on
the task, so an ``async_execution`` task is judged by its own ledger even when
another task has started since.
"""

from __future__ import annotations

import contextvars

from src.core.llm.transport.budget import TRUNCATED_MARKER
from src.core.logger import LoggerManager
from src.services.execution.harnesses.crewai.guardrails import _annotate

logger = LoggerManager.get_instance().crew

_CONTEXT_ATTR = "_kasal_outcome_context"


def begin_task(task: object) -> None:
    """A fresh tool ledger for ``task``, as Kasal's ``Task.execute_sync`` does."""
    from src.services.execution.runtime.executor import reset_tool_ledger

    reset_tool_ledger()
    try:
        object.__setattr__(task, _CONTEXT_ATTR, contextvars.copy_context())
    except Exception as e:  # noqa: BLE001 — bookkeeping never fails a run
        logger.debug("Could not keep the task's outcome context: %s", e)


def flag_output(task: object, output: object) -> None:
    """Mark ``output`` degraded when it ran out of time or had no sources.

    In place: CrewAI already holds this object in its task list and in the next
    task's context. FLAGGED, never raised — the reasons are the Kasal ``Task``'s.
    """
    if getattr(output, "degraded", None) is True:
        return  # a guardrail already said so, with more detail
    raw = getattr(output, "raw", None)
    if isinstance(raw, str) and TRUNCATED_MARKER in raw:
        # The transport's banner is already in the text; add the structure.
        _mark(output, "exceeded the execution budget; this answer is incomplete")
        return

    dead = _wholly_failed_tools(task)
    if not dead:
        return
    logger.warning(
        "task %r completed with wholly unavailable tool(s): %s",
        getattr(task, "name", None) or str(getattr(task, "description", ""))[:40],
        ", ".join(dead),
    )
    _annotate(
        output,
        f"every call to {', '.join(dead)} failed, so this answer was produced "
        "without the information those tools were meant to supply",
    )


def _wholly_failed_tools(task: object) -> list[str]:
    from src.services.execution.runtime.executor import wholly_failed_tools

    context = getattr(task, _CONTEXT_ATTR, None)
    try:
        if isinstance(context, contextvars.Context):
            return context.run(wholly_failed_tools)
        return wholly_failed_tools()
    except Exception as e:  # noqa: BLE001 — flagging never fails a run
        logger.debug("Could not read the tool ledger: %s", e)
        return []


def _mark(output: object, reason: str) -> None:
    try:
        object.__setattr__(output, "degraded", True)
        object.__setattr__(output, "degradation_reason", reason)
    except Exception as e:  # noqa: BLE001 — the text banner still carries it
        logger.debug("Could not mark the output degraded: %s", e)
