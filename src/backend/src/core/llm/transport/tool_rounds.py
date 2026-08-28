"""Executing one round of tool calls, with the clock still running.

Extracted from ``completion.py`` — already past the file-size ceiling — rather
than appended to it. The two transports (Chat Completions and the Responses
API) do the SAME thing in two different message shapes, and the budget check
below had to exist in both; one copy of the logic, two shapes at the edge.

**The check is the reason this module exists.** The round loop in
``completion.py`` tests the deadline once per round, before asking the model.
Tool execution inside a round was unbounded, so a round that fanned out to
eleven ``sonar-deep-research`` calls — minutes each, run sequentially — ran to
completion however far past the deadline it went, and the overrun was only
noticed at the top of the following round. A 300s budget was routinely
exceeded by a factor of four before anything looked at the clock.

crewAI bounds this by running the whole agent turn in a thread and killing it
with ``future.result(timeout=…)``; LangChain's async path wraps the loop in
``asyncio_timeout``. Both interrupt mid-tool. This is the same guarantee inside
a synchronous loop: check before dispatching each call, and once the budget is
gone stop dispatching.

Remaining calls are still ANSWERED, with ``SKIPPED_RESULT``, rather than left
dangling. A conversation carrying an assistant turn whose ``tool_calls`` have no
matching results is malformed, and every provider rejects it — so an abort that
just returned would poison the transcript for anything that later reuses it
(the guardrail retry path, or a wrap-up call on the same list).
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .budget import deadline_passed

#: Stands in for a tool that was never run because the budget went. Phrased for
#: the MODEL, which reads it as a tool result: it explains the gap and tells it
#: what to do, so a wrap-up answer can say what is missing instead of inventing
#: what the call would have returned.
SKIPPED_RESULT = (
    "Not executed: the agent's time budget ran out before this call started. "
    "Answer from the results you already have and note this gap."
)

#: Stands in for a call dropped because an earlier one in the same batch is the
#: answer. Distinct from SKIPPED_RESULT: nothing went wrong here.
SUPERSEDED_RESULT = (
    "Not executed: an earlier tool in this batch produced the final answer."
)


@dataclass
class RoundOutcome:
    """What one round of tool calls decided about the turn.

    ``final_answer`` set means a ``result_as_answer`` tool ran and its output IS
    the agent's answer — stop, do not go back to the model. ``exhausted`` means
    the clock ran out mid-round. Both can only be acted on by the caller, which
    owns the return contract.
    """

    results: list[str] = field(default_factory=list)
    exhausted: bool = False
    final_answer: str | None = None


#: What ``_handle_tool_execution`` returning None means. Kept here so both
#: shapes render it identically.
_TOOL_NOT_FOUND = "Tool not found."

#: A tool executor: (name, arguments) -> result text.
ToolExecutor = Callable[[str, Any], Any]


def answers_directly(
    available_functions: dict[str, Callable[..., Any]] | None, name: str
) -> bool:
    """Whether this tool's raw output is meant to BE the agent's answer.

    Stamped on the wrapper by ``runtime/executor.wrap_tool`` from the tool's
    ``result_as_answer`` field, which is seeded per tool in ``seeds/tools.py``.
    Opt-in and false by default: it bypasses the agent entirely, so it suits a
    tool that already produces the deliverable (a finished report, a rendered
    chart) and not a search whose result is one input among several.
    """
    return bool(
        getattr((available_functions or {}).get(name), "result_as_answer", False)
    )


def _run_calls(
    function_calls: list[dict[str, Any]],
    execute: ToolExecutor,
    deadline: float | None,
    available_functions: dict[str, Callable[..., Any]] | None,
) -> RoundOutcome:
    """Run each call until the clock goes or one of them IS the answer.

    Both stop conditions are checked per call rather than after the batch. For
    the deadline that is the whole point of this module; for
    ``result_as_answer`` it is what makes the short-circuit worth having —
    deciding after the batch would still pay for all eleven searches before
    noticing the first one had already answered. crewAI reaches the same
    conclusion from the other direction, disabling parallel tool execution for
    any batch containing such a tool.
    """
    outcome = RoundOutcome()
    for call in function_calls:
        stop = outcome.exhausted or outcome.final_answer is not None
        if not stop and deadline_passed(deadline):
            outcome.exhausted = True
            stop = True
        if stop:
            outcome.results.append(
                SKIPPED_RESULT if outcome.exhausted else SUPERSEDED_RESULT
            )
            continue
        result = execute(call["name"], call["arguments"])
        text = result if result is not None else _TOOL_NOT_FOUND
        outcome.results.append(text)
        if result is not None and answers_directly(available_functions, call["name"]):
            outcome.final_answer = text
    return outcome


def run_chat_round(
    conversation: list[dict[str, Any]],
    content: str | None,
    function_calls: list[dict[str, Any]],
    execute: ToolExecutor,
    deadline: float | None = None,
    available_functions: dict[str, Callable[..., Any]] | None = None,
) -> RoundOutcome:
    """Chat Completions shape: assistant turn, then one ``tool`` message each.

    Mutates ``conversation``.
    """
    conversation.append(
        {
            "role": "assistant",
            "content": content,
            "tool_calls": [
                {
                    "id": call["id"],
                    "type": "function",
                    "function": {
                        "name": call["name"],
                        "arguments": call["arguments"],
                    },
                }
                for call in function_calls
            ],
        }
    )
    outcome = _run_calls(function_calls, execute, deadline, available_functions)
    for call, result in zip(function_calls, outcome.results):
        conversation.append(
            {"role": "tool", "tool_call_id": call["id"], "content": result}
        )
    return outcome


def run_responses_round(
    conversation: list[dict[str, Any]],
    function_calls: list[dict[str, Any]],
    execute: ToolExecutor,
    deadline: float | None = None,
    available_functions: dict[str, Callable[..., Any]] | None = None,
) -> RoundOutcome:
    """Responses API shape: ``function_call_output`` entries, no assistant turn.

    Mutates ``conversation``.
    """
    outcome = _run_calls(function_calls, execute, deadline, available_functions)
    for call, result in zip(function_calls, outcome.results):
        conversation.append(
            {
                "type": "function_call_output",
                "call_id": call["id"],
                "output": result,
            }
        )
    return outcome


# ---------------------------------------------------------------------------
# Degenerate-loop defenses
# ---------------------------------------------------------------------------
# Observed: a weak model closed an already-closed browser 21 times in a row —
# each round announcing its final answer and then re-issuing the identical
# call — and the run ended by returning the model's raw tool-call MARKUP as
# the "answer". Two defenses, both model-agnostic:
#   1. RepeatGuard — identical call batches stop being executed after the
#      second repeat; the model gets a stub result telling it to answer, and
#      one more repeat after that drops the tools entirely.
#   2. strip_tool_markup / salvage_last_assistant_text — un-executed tool-call
#      syntax never leaves the transport as an answer.

#: Told to the model instead of re-running a call it has already made with the
#: same arguments. Phrased as a tool result, like SKIPPED_RESULT.
REPEATED_RESULT = (
    "Not executed: you have already made this exact call and its result will "
    "not change. STOP calling tools — write your complete final answer now."
)


def calls_signature(function_calls: list[dict[str, Any]]) -> str:
    """A stable identity for one round's batch: names + arguments, order kept."""
    try:
        import json as _json

        return _json.dumps(
            [
                {"n": c.get("name"), "a": c.get("arguments")}
                for c in (function_calls or [])
            ],
            sort_keys=True,
            default=str,
        )
    except Exception:  # noqa: BLE001 — a guard must never break the loop
        return str(function_calls)


@dataclass
class RepeatGuard:
    """Counts CONSECUTIVE identical call batches across rounds.

    ``observe`` returns how many times in a row this exact batch has now been
    seen beyond the first (0 = fresh). The caller acts on the count: at 2 the
    batch is stubbed instead of executed; at 3+ it also drops the tools so the
    next round can only produce an answer.
    """

    last: str | None = None
    repeats: int = 0

    def observe(self, function_calls: list[dict[str, Any]]) -> int:
        sig = calls_signature(function_calls)
        self.repeats = self.repeats + 1 if sig == self.last else 0
        self.last = sig
        return self.repeats


def stub_repeated_chat_round(
    conversation: list[dict[str, Any]],
    content: str | None,
    function_calls: list[dict[str, Any]],
) -> None:
    """Answer a repeated batch with REPEATED_RESULT stubs (Chat shape).

    The assistant turn still enters the conversation — a turn whose
    ``tool_calls`` have no matching results is malformed — but nothing runs.
    """
    conversation.append(
        {
            "role": "assistant",
            "content": content,
            "tool_calls": [
                {
                    "id": call["id"],
                    "type": "function",
                    "function": {
                        "name": call["name"],
                        "arguments": call["arguments"],
                    },
                }
                for call in function_calls
            ],
        }
    )
    for call in function_calls:
        conversation.append(
            {"role": "tool", "tool_call_id": call["id"], "content": REPEATED_RESULT}
        )


def stub_repeated_responses_round(
    conversation: list[dict[str, Any]],
    function_calls: list[dict[str, Any]],
) -> None:
    """Answer a repeated batch with REPEATED_RESULT stubs (Responses shape)."""
    for call in function_calls:
        conversation.append(
            {
                "type": "function_call_output",
                "call_id": call["id"],
                "output": REPEATED_RESULT,
            }
        )


#: Un-executed tool-call syntax, in the shapes self-hosted function-calling
#: models emit as plain text when they degenerate: <tool_call>...</tool_call>
#: blocks and stray <function=...>/<parameter=...> fragments.
import re as _re

_TOOL_MARKUP_RE = _re.compile(
    r"<tool_call>[\s\S]*?(?:</tool_call>|$)"
    r"|</?function[^>]*>"
    r"|</?parameter[^>]*>"
    r"|</?tool_call>",
    _re.IGNORECASE,
)


def strip_tool_markup(text: str | None) -> str:
    """Remove un-executed tool-call syntax from an answer. '' when that is all
    the text was — the caller decides what to fall back to."""
    if not text:
        return ""
    return _TOOL_MARKUP_RE.sub("", text).strip()


def salvage_last_assistant_text(conversation: list[dict[str, Any]]) -> str:
    """The last real thing the model SAID, for a turn that ended in markup.

    Walks backwards over assistant turns and returns the first content that
    survives ``strip_tool_markup`` — e.g. the "Now I'll build the deck" line
    before the loop degenerated. '' when there is none.
    """
    for message in reversed(conversation or []):
        if not isinstance(message, dict) or message.get("role") != "assistant":
            continue
        content = message.get("content")
        if isinstance(content, str):
            cleaned = strip_tool_markup(content)
            if cleaned:
                return cleaned
    return ""
