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
every tool call made for the task lands in it. The dict itself is kept on the
task, so an ``async_execution`` task is judged by its own ledger even when
another task has started since.

Truncation is read from a FACT, not from the text: the transport records the
task whose turn it ended (``budget.BUDGET_SPENT_ATTR``). Matching the banner
would also flag a downstream task that merely quotes its truncated context.
"""

from __future__ import annotations

from src.core.llm.transport.budget import BUDGET_SPENT_ATTR
from src.core.logger import LoggerManager
from src.services.execution.harnesses.crewai.guardrails import annotate_degraded

logger = LoggerManager.get_instance().crew

_LEDGER_ATTR = "_kasal_tool_ledger"


def begin_task(task: object) -> None:
    """A fresh tool ledger for ``task``, as Kasal's ``Task.execute_sync`` does."""
    from src.services.execution.runtime.executor import reset_tool_ledger

    ledger = reset_tool_ledger()
    try:
        object.__setattr__(task, _LEDGER_ATTR, ledger)
        object.__setattr__(task, BUDGET_SPENT_ATTR, None)
    except Exception as e:  # noqa: BLE001 — bookkeeping never fails a run
        logger.debug("Could not keep the task's outcome ledger: %s", e)


def flag_output(task: object, output: object) -> None:
    """Mark ``output`` degraded when its turn ran out of time or had no sources.

    In place: CrewAI already holds this object in its task list and in the next
    task's context. FLAGGED, never raised — the reasons are the Kasal ``Task``'s.
    """
    if getattr(output, "degraded", None) is True:
        return  # a guardrail already said so, with more detail
    spent = getattr(task, BUDGET_SPENT_ATTR, None)
    if isinstance(spent, str) and spent:
        # The transport's banner is already in the text; add the structure.
        _mark(output, f"exceeded the execution budget ({spent})")
        return

    dead = _wholly_failed_tools(task)
    if not dead:
        return
    logger.warning(
        "task %r completed with wholly unavailable tool(s): %s",
        getattr(task, "name", None) or str(getattr(task, "description", ""))[:40],
        ", ".join(dead),
    )
    annotate_degraded(
        output,
        f"every call to {', '.join(dead)} failed, so this answer was produced "
        "without the information those tools were meant to supply",
    )


def _wholly_failed_tools(task: object) -> list[str]:
    from src.services.execution.runtime.executor import wholly_failed_tools

    ledger = getattr(task, _LEDGER_ATTR, None)
    try:
        return wholly_failed_tools(ledger if isinstance(ledger, dict) else None)
    except Exception as e:  # noqa: BLE001 — flagging never fails a run
        logger.debug("Could not read the tool ledger: %s", e)
        return []


def _mark(output: object, reason: str) -> None:
    try:
        object.__setattr__(output, "degraded", True)
        object.__setattr__(output, "degradation_reason", reason)
    except Exception as e:  # noqa: BLE001 — the text banner still carries it
        logger.debug("Could not mark the output degraded: %s", e)
