"""Grade a crew deliverable with the crew's custom (registered) judges.

Extracted from the crew runner's ``output_correct`` scorer. Registered judges
grade the SAME deliverable inside Kasal's grading step rather than running as
separate MLflow scorers: trace-based scorers each re-triggered their own crew
execution (observed live as bursts of one execution per judge), multiplying
the budget. They are RENDERED AND INVOKED HERE through LLMManager, never via
mlflow's own model client. The judge contributes its instructions, retrieved
examples and model key; provider routing, keys and request quirks stay
centralized in the manager (invoking judges through mlflow's client is what
produced the retired-DeepSeek and Kimi failures).
"""

import asyncio
import logging
from typing import List, Mapping, Optional, Sequence, Tuple

from src.services.prompt_optimization.gepa import reflection
from src.services.prompt_optimization.gepa.grading import _parse_grade_from_text
from src.services.prompt_optimization.gepa.judge_memory import JudgeMemory
from src.services.prompt_optimization.gepa.judge_model import (
    _stored_judge_model_to_key,
)
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


def grade_registered_judges(
    loop: asyncio.AbstractEventLoop,
    judges: Sequence[object],
    memories: Mapping[str, JudgeMemory],
    request: str,
    text: str,
    judge_model: str,
    group_context: Optional[GroupContext],
    user_token: Optional[str],
) -> Tuple[List[float], List[str]]:
    """``(grades, rationale lines)`` from each judge that returned a grade.

    A judge whose reply is not numeric is skipped; one that fails is logged
    and skipped — except an ALIGNED judge (it has memory), which raises: it
    must not silently drop out of the optimization metric.
    """
    grades: List[float] = []
    rationale_parts: List[str] = []
    for judge in judges:
        judge_name = getattr(judge, "name", "judge")
        try:
            instructions = memories[judge_name].instructions(
                inputs=request, outputs=text
            )
            rendered = (
                instructions.replace("{{ outputs }}", text)
                .replace("{{outputs}}", text)
                .replace("{{ inputs }}", request)
                .replace("{{inputs}}", request)
            )
            judge_reply = reflection._sync_llm_completion(
                loop,
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"{rendered}\n\nEnd your reply with the "
                            "numeric grade 0-10 alone on the LAST line."
                        ),
                    }
                ],
                model=_stored_judge_model_to_key(getattr(judge, "model", None))
                or judge_model,
                max_tokens=1500,
                group_context=group_context,
                user_token=user_token,
            )
            grade = _parse_grade_from_text(judge_reply)
            if grade is None:
                logger.warning(
                    "Registered judge '%s' reply not numeric; skipping: %.200r",
                    judge_name,
                    judge_reply,
                )
            else:
                grades.append(grade)
                if judge_reply and judge_reply.strip():
                    rationale_parts.append(f"[{judge_name}] {judge_reply.strip()}")
        except (
            Exception
        ) as judge_err:  # noqa: BLE001 — one failing custom judge is skipped, not fatal (aligned ones re-raise)
            if getattr(judge, "memory", None):
                # Do not silently drop an aligned judge from the
                # optimization metric when memory/provider access fails.
                raise RuntimeError(
                    f"Aligned judge '{judge_name}' could not score with its memory"
                ) from judge_err
            logger.warning("Registered judge '%s' failed: %s", judge_name, judge_err)
    return grades, rationale_parts
