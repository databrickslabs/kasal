"""Execution budget for one LLM call: tool rounds and wall clock.

Extracted from ``completion.py`` rather than appended to it — that module was
already past the 800-line ceiling, and this is a self-contained concern with a
clean seam: it reads an agent's caps and a conversation, and returns numbers.
``OpenAICompletion`` keeps thin methods delegating here, so the call sites and
the existing test surface are unchanged.
"""

import contextlib
import time
from typing import Any

from src.core.execution_stop import stop_requested

from .exceptions import ExecutionBudgetExceededError, ExecutionStoppedError
from .response_parsing import REDACTED_REASONING

#: Tool-calling rounds allowed when no agent supplies a cap.
#:
#: Note this is TIGHTER than ``Agent.max_iter``'s default of 25, so attaching an
#: agent currently loosens the round cap rather than tightening it.
MAX_TOOL_ROUNDS = 15

#: What ``DatabricksRetryLLM`` writes into a tool-call message that has no
#: content (the endpoint rejects empty content). Never an answer — and under
#: CrewAI, which sends tool-call messages with ``content=None``, it is the last
#: "assistant text" of almost every conversation.
TOOL_CALL_PLACEHOLDER = "Calling tools."


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
        if not isinstance(content, str) or not content.strip():
            continue
        if content.strip() == TOOL_CALL_PLACEHOLDER:
            continue
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
    from .request_deadline import current_deadline

    deadline = current_deadline()
    if from_agent is None:
        return rounds, deadline

    max_iter = getattr(from_agent, "max_iter", None)
    if isinstance(max_iter, int) and max_iter > 0:
        rounds = max_iter

    max_seconds = turn_cap(from_agent)
    if max_seconds is not None:
        turn_deadline = time.monotonic() + max_seconds
        deadline = (
            min(deadline, turn_deadline) if deadline is not None else turn_deadline
        )

    run_deadline = getattr(from_agent, "run_deadline", None)
    if isinstance(run_deadline, (int, float)):
        deadline = (
            float(run_deadline)
            if deadline is None
            else min(deadline, float(run_deadline))
        )
    return rounds, deadline


def deadline_passed(deadline: float | None) -> bool:
    """Whether the wall clock has run out. No deadline means never."""
    return deadline is not None and time.monotonic() >= deadline


def check_deadline(
    deadline: float | None,
    rounds_done: int,
    model: str,
    conversation: list[dict[str, Any]] | None = None,
) -> None:
    """Raise if the wall clock has run out, carrying the partial answer."""
    if deadline_passed(deadline):
        raise ExecutionBudgetExceededError(
            f"max_execution_time exceeded after {rounds_done} tool round(s) "
            f"for model {model}.",
            partial=last_assistant_text(conversation),
        )


def check_stopped(
    rounds_done: int,
    model: str,
    conversation: list[dict[str, Any]] | None = None,
) -> None:
    """Raise when THIS context's execution was stopped by the user.

    Same boundary as ``check_deadline``: both are reasons the loop must end
    before the next round starts. A context with no bound stop event — every
    subprocess, and non-execution work — always passes.
    """
    if stop_requested():
        raise ExecutionStoppedError(
            f"Execution stopped by user after {rounds_done} tool round(s) "
            f"for model {model}.",
            partial=last_assistant_text(conversation),
        )


def exhausted_mid_round(
    model: str, conversation: list[dict[str, Any]] | None = None
) -> ExecutionBudgetExceededError:
    """The error for a clock that ran out WHILE a round's tools were running.

    Distinct wording from ``check_deadline`` on purpose. That message reports
    the round count and so read "exceeded after 1 tool round(s)" for a batch of
    eleven searches — technically true (the check fires at the top of the next
    round) and thoroughly misleading about where the time went.
    """
    return ExecutionBudgetExceededError(
        f"max_execution_time exceeded during tool execution for model {model}.",
        partial=last_assistant_text(conversation),
    )


#: Asked of the model when its budget is gone, in place of raising.
#:
#: crewAI (``force_final_answer``) and LangChain (``early_stopping_method=
#: "generate"``) both spend one extra call here rather than discarding the turn,
#: and both are right: an agent eleven searches deep has the material for an
#: answer, and throwing it away to raise "did not converge" is the worst of the
#: available outcomes. No tools are offered on this call, so it cannot open
#: another round.
FORCE_FINAL_ANSWER = (
    "Your time budget for this task is spent. Stop using tools and answer now, "
    "using only what you have already gathered. Do not start new research and "
    "do not apologise for stopping — give your best possible answer from the "
    "material above, and state briefly what is missing or unverified."
)


#: The second, and last, wrap-up attempt — sent only after the first spent its
#: whole OUTPUT allowance deliberating and produced no answer text. Observed on
#: a 25-round research task: 8,192 completion tokens, the compiled list sitting
#: in the reasoning channel, ``content`` empty. Reasoning counts against the
#: output budget, so the instruction is not "answer" but "stop thinking".
FORCE_FINAL_ANSWER_DIRECTLY = (
    "Your previous attempt ran out of output before writing any answer, because "
    "it kept deliberating. Do not think it over again. Write the answer "
    "immediately, starting with the first line of the deliverable, and keep it "
    "compact — a shorter answer that finishes beats a longer one that is cut "
    "off. State briefly what is missing or unverified."
)


def wrapup_conversation(
    conversation: list[dict[str, Any]], *, direct: bool = False
) -> list[dict[str, Any]]:
    """``conversation`` plus the stop-and-answer instruction, as a new list.

    A copy, not an append: the caller's list is still the live conversation the
    partial answer is read from if this last call also fails. ``direct`` selects
    the second attempt's wording (see ``FORCE_FINAL_ANSWER_DIRECTLY``).
    """
    instruction = FORCE_FINAL_ANSWER_DIRECTLY if direct else FORCE_FINAL_ANSWER
    return [*conversation, {"role": "user", "content": instruction}]


def partial_from_reasoning(reasoning: str | None, finish_reason: str | None) -> str:
    """A degraded partial answer lifted from the model's REASONING channel.

    For the wrap-up only, when neither attempt produced answer text.
    ``answer_from_reasoning`` is deliberately conservative — thinking prose
    passed off as an ANSWER would replace a visible empty result with a
    plausible wrong one — and this does not loosen it: the text travels as the
    budget error's ``partial``, which the degrade path prints under its own
    "⚠️ Truncated" banner, and this adds a second one naming the source. A draft
    the model wrote while compiling its answer, labelled as a draft, beats
    losing every search the run made to an empty string.

    '' when there is no reasoning, or only the redaction flag.
    """
    text = (reasoning or "").strip()
    if not text or text == REDACTED_REASONING:
        return ""
    why = (
        "it ran out of output tokens while still deliberating"
        if finish_reason == "length"
        else "it produced no answer text"
    )
    return (
        f"> ⚠️ Recovered from the model's reasoning: {why}, so this is its "
        "working draft, not a finished answer.\n\n" + text
    )


def rounds_exhausted(
    rounds: int, model: str, conversation: list[dict[str, Any]] | None = None
) -> ExecutionBudgetExceededError:
    """The error for a tool loop that never converged."""
    return ExecutionBudgetExceededError(
        f"Tool-calling did not converge within {rounds} rounds for model {model}.",
        partial=last_assistant_text(conversation),
    )


#: Where the CrewAI harness carries ``max_execution_time`` instead of CrewAI's
#: own field. Set there, CrewAI wraps the turn in a ThreadPool timeout that
#: FAILS the task; carried here, the transport's deadline bounds the turn and
#: a spent clock degrades it (``spent_budget_answer``).
TURN_CAP_ATTR = "_kasal_max_execution_time"

#: The banner a degraded, out-of-budget answer carries. Same text as
#: ``runtime/task.py``'s, so anything matching on it works on both harnesses.
TRUNCATED_MARKER = "> ⚠️ Truncated: "


def turn_cap(agent: object) -> float | None:
    """An agent's per-turn wall clock, wherever its harness keeps it."""
    for value in (
        getattr(agent, "max_execution_time", None),
        getattr(agent, TURN_CAP_ATTR, None),
    ):
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
    return None


def truncated_answer(error: Exception) -> str:
    """The partial answer under the "Truncated" banner."""
    partial = getattr(error, "partial", "") or ""
    return (
        f"{partial}\n\n{TRUNCATED_MARKER}exceeded the execution budget "
        f"({error}). This answer is incomplete."
    ).strip()


#: Set on the task whose turn ``end_spent_turn`` ended, with the reason. The
#: harness marks that task's output degraded from THIS, never from the banner
#: text — a downstream task that quotes its truncated context is not truncated.
BUDGET_SPENT_ATTR = "_kasal_budget_spent"

#: When that mark was made (``time.time()``), so a retry can tell a truncation
#: left by an EARLIER attempt from its own.
BUDGET_SPENT_AT_ATTR = "_kasal_budget_spent_at"


def end_spent_turn(
    error: Exception,
    model: str,
    conversation: list[dict[str, object]] | None,
    task: object,
) -> str:
    """A task turn's last answer once its budget is spent; records it on ``task``.

    Reworded from the round-0 message, which reads "exceeded after 0 tool
    round(s)": true of one delegated call and wrong about the turn it ends.
    """
    reason = f"max_execution_time reached for model {model}"
    with contextlib.suppress(Exception):  # a mark never fails a run
        object.__setattr__(task, BUDGET_SPENT_ATTR, reason)
        object.__setattr__(task, BUDGET_SPENT_AT_ATTR, time.time())
    return truncated_answer(
        ExecutionBudgetExceededError(
            reason,
            partial=getattr(error, "partial", "") or last_assistant_text(conversation),
        )
    )


def spent_budget_answer(
    error: Exception,
    model: str,
    conversation: list[dict[str, object]] | None,
    task: object,
) -> str | None:
    """A delegated round that hit the wall clock, as the turn's last answer.

    For a TASK's turn under ``delegate_tool_calls`` only, where the caller owns
    the tool loop and each ``call()`` is one round: ``_answer_within_budget`` is
    unreachable there, so raising handed CrewAI an exception it turns into a
    failed task or a re-run of the whole task. Returning the partial as TEXT
    ends the turn the way the Kasal harness's ``Task._run_agent`` does. (A call
    with no task — Chat's lone agent — keeps raising: its caller degrades it.)

    Covers the deadline found spent at round 0 and a request cut off by it
    (``bounded_params`` sizes the timeout to the time left). None — raise as
    before — for a user Stop, or an error while time remained.
    """
    from .request_deadline import current_deadline

    if isinstance(error, ExecutionStoppedError):
        return None
    if not isinstance(error, ExecutionBudgetExceededError) and not deadline_passed(
        current_deadline()
    ):
        return None
    return end_spent_turn(error, model, conversation, task)
