"""LLMGuardrail — LLM-as-judge validation of a task output.

Authored module; surface validated against the kasal_engine datamodel.
Called by Task's guardrail loop: returns (valid, feedback-or-output).
An unparseable judge response counts as valid (logged) so a flaky judge
cannot dead-end a pipeline; a parseable rejection triggers the retry loop.
"""

import logging
from typing import Any

from .executor import call_llm, extract_json_dict

logger = logging.getLogger(__name__)

_JUDGE_PROMPT = """You are validating a task result against a quality criterion.

Criterion: {description}

Task result:
{output}

Answer ONLY with a JSON object: {{"valid": true/false, "feedback": "<why, and what to fix if invalid>"}}"""


class LLMGuardrail:
    def __init__(self, description: str, llm: Any):
        self.description = description
        self.llm = llm

    def __call__(self, task_output: Any) -> tuple[bool, Any]:
        prompt = _JUDGE_PROMPT.format(
            description=self.description, output=task_output.raw
        )
        try:
            response = call_llm(self.llm, [{"role": "user", "content": prompt}])
        except Exception as e:
            logger.warning("LLMGuardrail judge call failed: %s", e)
            return True, task_output.raw

        verdict = extract_json_dict(response)
        if verdict is None or "valid" not in verdict:
            logger.warning(
                "LLMGuardrail got an unparseable verdict, accepting output: %.200s",
                response,
            )
            return True, task_output.raw
        if verdict["valid"]:
            return True, task_output.raw
        return False, verdict.get("feedback", "The output did not meet the criterion.")
