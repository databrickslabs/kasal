"""Ordered composition of a task's guardrails.

The engine runs ``task_args['guardrails']`` (plural) in order, each with its own
retry budget, so ORDER IS COST. The stack is arranged cheapest-first:

    1. schema gate      — free, no I/O. Rejects prose, truncated JSON, wrong shape.
    2. detection rule   — free. Rejects empty, thin, uncited, low-confidence.
    3. LLM judge        — one model call. Rejects plausible-but-wrong.
    4. human review     — a person. Whatever the user wants eyes on.

An expensive check must never grade output a free check would have rejected —
without that ordering, every thin answer costs a judge call before anyone
notices it has two findings instead of three.

This replaces an ad-hoc composition in ``task_builder`` that *popped* the
singular ``guardrail`` key to fold it under the human-review gate. That worked
for exactly two layers and had no room for a third.
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

#: Rank per layer. Lower runs first. Anything unrecognised sorts between the
#: free checks and the judge — an unknown guardrail is assumed to cost something
#: but not to be the final human word.
_ORDER = {
    "schema": 10,
    "detection": 20,
    "content": 50,
    "human": 90,
}


def build_guardrail_stack(
    task_args: Dict[str, Any], layers: List[Any], task_key: str
) -> None:
    """Fold ``layers`` into ``task_args`` as an ordered guardrail stack.

    ``layers`` is a list of ``(kind, guardrail)`` pairs where ``kind`` is a key
    of ``_ORDER``. Any guardrail already sitting on ``task_args`` under the
    singular ``guardrail`` key is absorbed as ``content`` — that is where the
    LLM judge and the code-based factory guardrails land.

    Mutates ``task_args``: sets ``guardrails`` and removes ``guardrail``, since
    the engine prefers the plural and reading both would run one twice.
    """
    existing = task_args.pop("guardrail", None)
    combined: List[tuple] = ([("content", existing)] if existing else []) + [
        (kind, guardrail) for kind, guardrail in layers if guardrail is not None
    ]
    if not combined:
        return

    combined.sort(key=lambda pair: _ORDER.get(pair[0], 60))
    ordered = [guardrail for _, guardrail in combined]

    if len(ordered) == 1:
        # Keep the singular key for a single guardrail so existing callers that
        # check ``'guardrail' in task_args`` (the crew path wires a fallback
        # callback off it) keep behaving as they do today.
        task_args["guardrail"] = ordered[0]
    else:
        task_args["guardrails"] = ordered

    logger.info(
        "Task %s guardrail stack: %s",
        task_key,
        " → ".join(kind for kind, _ in combined),
    )
