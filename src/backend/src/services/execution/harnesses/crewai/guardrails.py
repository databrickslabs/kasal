"""Kasal's degrade-on-exhausted policy, on a CrewAI task.

A guardrail that rejects an output three times leaves a choice: abort the task,
or keep the best attempt and say it is unverified. Kasal's ``Task`` models that
as ``guardrail_on_exhausted`` and the generated research/deep modes set it to
``"degrade"`` for a specific reason, recorded where it is set:

    Losing a six-task run because task four could not satisfy a judge on the
    third attempt throws away everything already produced.

CrewAI has no such field. Its task raises once ``guardrail_max_retries`` is
spent, so the same crew that degrades on one harness would abort on the other —
and the difference would show up as a research run that simply failed.

## Done through the guardrail contract, not through CrewAI's internals

The obvious implementation is to override the method that raises. This does not:
it wraps the guardrail CALLABLE, counts its own attempts, and on the last one
returns success with the output annotated instead of failure. That uses only the
public ``(output) -> (ok, value)`` contract both harnesses already share, so it
cannot break when CrewAI reorganises its retry loop.

The annotation is deliberately BOTH forms: the marker in the text, which is what
a reader sees, and the structured ``degraded`` / ``degradation_reason``
attributes, which is what a recipe gate or the A2UI composer asks. Text alone
would let an automated consumer treat a degraded answer as a clean one.
"""

from __future__ import annotations

from typing import Any, Callable, Tuple  # noqa: F401 — Tuple documents the contract

from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().guardrails

#: Prefix used by the Kasal runtime, kept identical so anything matching on it
#: works the same under either harness.
DEGRADED_MARKER = "> ⚠️ Unverified: "


def _reason_for(result: Any) -> str:
    """Why the output is soft, in terms a reader can act on.

    Prefers the real cause over the judge's guess: a run whose every source call
    returned 503 was once annotated "does not define named agents", which told a
    reader nothing about why. If the tools all failed, say so.
    """
    try:
        from src.services.execution.runtime.executor import wholly_failed_tools

        dead = wholly_failed_tools()
    except Exception:  # noqa: BLE001 — annotation must never fail a run
        dead = []
    if dead:
        return (
            f"every call to {', '.join(dead)} failed, so the information this "
            f"task needed was never available"
        )
    return str(result)


def _annotate(output: Any, reason: str) -> Any:
    """Mark an output degraded, in text and in structure."""
    raw = getattr(output, "raw", None)
    if isinstance(raw, str) and DEGRADED_MARKER not in raw:
        try:
            object.__setattr__(output, "raw", f"{raw}\n\n{DEGRADED_MARKER}{reason}")
        except Exception:  # noqa: BLE001
            pass
    for attribute, value in (("degraded", True), ("degradation_reason", reason)):
        try:
            object.__setattr__(output, attribute, value)
        except Exception:  # noqa: BLE001 — the text marker still carries it
            pass
    return output


def degrade_on_exhausted(
    guardrail: Callable[[Any], Tuple[bool, Any]],
    max_retries: int,
    label: str = "guardrail",
) -> Callable[[Any], Tuple[bool, Any]]:
    """``guardrail``, but accepting the last attempt instead of aborting.

    ``max_retries`` mirrors ``Task.guardrail_max_retries``: the guardrail runs
    ``max_retries + 1`` times in total, and only the final rejection degrades.
    """
    attempts = {"n": 0}

    # DELIBERATELY UNANNOTATED — do not "fix" this by adding `-> Tuple[bool, Any]`.
    #
    # CrewAI validates a guardrail's return annotation with
    # `inspect.signature(...).return_annotation` and `get_origin`. This module
    # uses `from __future__ import annotations`, so an annotation here is the
    # STRING "Tuple[bool, Any]", `get_origin` returns None, and Task
    # construction fails with "If return type is annotated, it must be
    # Tuple[bool, Any]" — about an annotation that says exactly that.
    # No annotation means no validation, which is what we want.
    def wrapped(output: Any):
        attempts["n"] += 1
        try:
            verdict = guardrail(output)
        except Exception:
            raise
        ok, value = verdict if isinstance(verdict, tuple) else (bool(verdict), verdict)
        if ok:
            return True, value
        if attempts["n"] <= max_retries:
            return False, value

        reason = _reason_for(value)
        logger.warning(
            "task failed %s after %d retries; degrading (%s)",
            label,
            max_retries,
            reason,
        )
        return True, _annotate(output, reason)

    return wrapped
