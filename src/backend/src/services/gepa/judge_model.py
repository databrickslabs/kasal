"""Choosing which model grades a candidate.

The judge must not be the model being optimised — a model asked to grade its
own output rates it generously, and the optimiser then climbs that bias
instead of the task.
"""

import logging
import os
from typing import Any, Dict, List, Optional
from src.core.exceptions import BadRequestError

logger = logging.getLogger(__name__)


def _stored_judge_model_to_key(stored: Any) -> Optional[str]:
    """Kasal model key from a judge's stored model field.

    New judges store the Kasal key wrapped as 'openai:/<key>' purely to
    satisfy make_judge's URI shape (the judge is INVOKED via LLMManager, the
    URI is inert). Legacy judges stored real provider URIs — the remainder
    after '<scheme>:/' is the best available key for those too.
    """
    if not stored:
        return None
    text = str(stored)
    if ":/" in text:
        return text.split(":/", 1)[1] or None
    return text


def _crew_target_model(agents: Any) -> Optional[str]:
    """The model a crew actually runs on: the most common agent ``llm``.

    There is no crew-level model column — the model lives per agent — so the
    honest answer for "what is this crew's model" is whichever one most of its
    agents use. Returns None when no agent declares one, leaving the caller to
    fall back to the global default.
    """
    from collections import Counter

    declared = [a.llm for a in (agents or []) if getattr(a, "llm", None)]
    if not declared:
        return None
    return Counter(declared).most_common(1)[0][0]


def _resolve_judge_model(requested: Optional[str], target_model: str, what: str) -> str:
    """Pick the correctness judge's model. It may never be the target.

    Resolution order:
      1. an explicit `judge_model` on the request,
      2. the configured default `GEPA_JUDGE_MODEL`,
      3. no judge — the run is REFUSED.

    Judging with the model under optimization is self-preference: the judge
    grades its own outputs and systematically favours them, so the score climbs
    whether or not the prompts improved. Because that score is the fitness
    function GEPA optimises against, the search can then be rewarding reward
    hacking with nothing to distinguish it from real progress.

    This used to warn and continue. A warning was the wrong shape for it: the
    run still produced an authoritative-looking number, the log line was never
    read, and the resulting gain was indistinguishable from a real one. Refusing
    up front costs a corrected setting; proceeding costs trust in every score
    the system has ever reported.

    Raises:
        BadRequestError: when the judge would be the target, or none is set.
    """
    chosen = (requested or "").strip()
    if chosen:
        if chosen == target_model:
            raise BadRequestError(
                f"{what}: the judge model and the model under optimization are "
                f"both '{target_model}'. A model grading its own output prefers "
                f"it, so the score would rise without the prompts improving. "
                f"Pick a different judge model."
            )
        return chosen

    configured = (os.getenv("GEPA_JUDGE_MODEL") or "").strip()
    if configured and configured != target_model:
        logger.info(
            "%s: judge model defaulted to GEPA_JUDGE_MODEL=%s (target=%s)",
            what,
            configured,
            target_model,
        )
        return configured

    if configured:
        raise BadRequestError(
            f"{what}: GEPA_JUDGE_MODEL is '{configured}', which is also the "
            f"model under optimization. A model grading its own output prefers "
            f"it. Set GEPA_JUDGE_MODEL to a different model, or pass an "
            f"explicit judge_model."
        )
    raise BadRequestError(
        f"{what}: no judge model configured. The judge decides which candidate "
        f"prompts win, so it cannot silently fall back to the model under "
        f"optimization ('{target_model}') — that grades its own work. Pass "
        f"judge_model on the request, or set GEPA_JUDGE_MODEL."
    )
