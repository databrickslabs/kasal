"""Execution budget for one LLM call: tool rounds and wall clock.

Extracted from ``completion.py`` rather than appended to it — that module was
already past the 800-line ceiling, and this is a self-contained concern with a
clean seam: it reads an agent's caps and a conversation, and returns numbers.
``OpenAICompletion`` keeps thin methods delegating here, so the call sites and
the existing test surface are unchanged.
"""

import time
from typing import Any

from .exceptions import ExecutionBudgetExceededError

#: Tool-calling rounds allowed when no agent supplies a cap.
#:
#: Note this is TIGHTER than ``Agent.max_iter``'s default of 25, so attaching an
#: agent currently loosens the round cap rather than tightening it.
MAX_TOOL_ROUNDS = 15


def last_assistant_text(conversation: list[dict[str, Any]] | None) -> str:
    """The most recent thing the model wrote, for a degraded partial answer.

    Handles both transport shapes: chat messages (``role: assistant``) and the
    Responses API's ``function_call_output`` entries, which carry no assistant
    text of their own — hence the role check rather than a positional read.
    """
    for entry in reversed(conversation or []):
        if not isinstance(entry, dict) or entry.get("role") != "assistant":
            continue
        content = entry.get("content")
        if isinstance(content, str) and content.strip():
            return content
    return ""


def resolve_execution_budget(from_agent: Any) -> tuple[int, float | None]:
    """Resolve (max tool rounds, wall-clock deadline) for one ``call()``.

    ``Agent.max_iter`` and ``Agent.max_execution_time`` were accepted-but-inert
    fields (crewAI never enforced them either); here they are real. Direct LLM
    calls with no agent keep the default round cap.

    The per-call clock restarts on EVERY call, so by itself it bounds one agent
    turn and nothing else — six tasks with three guardrail retries each got
    twenty-four fresh deadlines and no run-level ceiling existed at all.
    ``Agent.run_deadline`` is one fixed point for the whole run; taking the
    earlier of the two makes the run-level promise the binding one.
    """
    rounds = MAX_TOOL_ROUNDS
    deadline: float | None = None
    if from_agent is None:
        return rounds, deadline

    max_iter = getattr(from_agent, "max_iter", None)
    if isinstance(max_iter, int) and max_iter > 0:
        rounds = max_iter

    max_seconds = getattr(from_agent, "max_execution_time", None)
    if isinstance(max_seconds, (int, float)) and max_seconds > 0:
        deadline = time.monotonic() + float(max_seconds)

    run_deadline = getattr(from_agent, "run_deadline", None)
    if isinstance(run_deadline, (int, float)):
        deadline = (
            float(run_deadline)
            if deadline is None
            else min(deadline, float(run_deadline))
        )
    return rounds, deadline


def check_deadline(
    deadline: float | None,
    rounds_done: int,
    model: str,
    conversation: list[dict[str, Any]] | None = None,
) -> None:
    """Raise if the wall clock has run out, carrying the partial answer."""
    if deadline is not None and time.monotonic() >= deadline:
        raise ExecutionBudgetExceededError(
            f"max_execution_time exceeded after {rounds_done} tool round(s) "
            f"for model {model}.",
            partial=last_assistant_text(conversation),
        )


def rounds_exhausted(
    rounds: int, model: str, conversation: list[dict[str, Any]] | None = None
) -> ExecutionBudgetExceededError:
    """The error for a tool loop that never converged."""
    return ExecutionBudgetExceededError(
        f"Tool-calling did not converge within {rounds} rounds for model {model}.",
        partial=last_assistant_text(conversation),
    )
