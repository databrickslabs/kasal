"""LLM judge for answers that a number check cannot settle, through Kasal's LLM layer.

The judge sees the question, the expected reference (the expected answer, or the
expected result table rendered as text) and the answer, and replies with a JSON
object ``{"correct": bool, "reason": str}``. The call goes through
``LLMManager.completion_with_usage`` (the public LLM facade, which carries telemetry
and the configured endpoint) and is run from sync code with
``run_async_with_context``. Failures come back as ``Judgement(error=...)``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass

import openai

from src.services.tools.async_bridge import run_async_with_context
from src.services.tools.genie_space_sync_utils.verdicts import short

DEFAULT_JUDGE_MODEL = "databricks-claude-sonnet-4-5"
_MAX_FIELD_CHARS = 8000
_TIMEOUT_S = 300
_MAX_REPLY_TOKENS = 400
SYSTEM_PROMPT = (
    "You grade an analytics answer against an expected result. Compare the figures, "
    "not the wording. A figure rounded to the precision it is written with is correct. "
    "A missing, different or unsupported figure is incorrect. Reply with only a JSON "
    'object: {"correct": true or false, "reason": "one short sentence"}.'
)


@dataclass(frozen=True)
class JudgeReply:
    content: str
    total_tokens: int = 0


#: (system prompt, user prompt) -> the model's reply.
CompleteFn = Callable[[str, str], JudgeReply]


@dataclass(frozen=True)
class Judgement:
    correct: bool | None = None
    reason: str = ""
    error: str = ""
    total_tokens: int = 0


def build_prompt(question: str, reference: str, answer: str) -> str:
    return (
        f"QUESTION:\n{_clip(question)}\n\n"
        f"EXPECTED:\n{_clip(reference)}\n\n"
        f"ANSWER:\n{_clip(answer)}"
    )


def _clip(text: str) -> str:
    return text if len(text) <= _MAX_FIELD_CHARS else text[:_MAX_FIELD_CHARS] + " ..."


def parse_reply(content: str) -> tuple[bool | None, str]:
    """``(correct, reason)`` from the judge's reply; ``correct`` is None when the reply is not the JSON object."""
    start, end = content.find("{"), content.rfind("}")
    if start == -1 or end <= start:
        return None, ""
    try:
        parsed: object = json.loads(content[start : end + 1])
    except ValueError:
        return None, ""
    if not isinstance(parsed, dict) or not isinstance(parsed.get("correct"), bool):
        return None, ""
    reason = parsed.get("reason")
    return parsed["correct"], short(reason) if isinstance(reason, str) else ""


def judge(
    complete: CompleteFn, question: str, reference: str, answer: str
) -> Judgement:
    """Grade *answer* against *reference*; a failed call or an unreadable reply is an error, not a verdict."""
    try:
        reply = complete(SYSTEM_PROMPT, build_prompt(question, reference, answer))
    except (openai.OpenAIError, RuntimeError, ValueError, TimeoutError, OSError) as exc:
        return Judgement(error=f"judge call failed: {short(exc)}")
    correct, reason = parse_reply(reply.content)
    if correct is None:
        return Judgement(
            error="judge reply was not a JSON verdict", total_tokens=reply.total_tokens
        )
    return Judgement(correct=correct, reason=reason, total_tokens=reply.total_tokens)


def kasal_completion(model: str = DEFAULT_JUDGE_MODEL) -> CompleteFn:
    """The judge's completion through Kasal's LLM facade, callable from sync code."""

    def _complete(system: str, user: str) -> JudgeReply:
        from src.services.llm.manager import LLMManager

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        result = run_async_with_context(
            LLMManager.completion_with_usage(
                messages=messages,
                model=model,
                temperature=0.0,
                max_tokens=_MAX_REPLY_TOKENS,
            ),
            timeout=_TIMEOUT_S,
        )
        usage = result.get("usage")
        tokens = usage.get("total_tokens") if isinstance(usage, dict) else None
        content = result.get("content")
        return JudgeReply(
            content=content if isinstance(content, str) else "",
            total_tokens=tokens if isinstance(tokens, int) else 0,
        )

    return _complete
