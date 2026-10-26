"""LLMGuardrail — LLM-as-judge validation of a task output.

Authored module; surface validated against the kasal_engine datamodel.
Called by Task's guardrail loop: returns (valid, feedback-or-output).
An unparseable judge response counts as valid (logged) so a flaky judge
cannot dead-end a pipeline; a parseable rejection triggers the retry loop.
"""

import logging
from typing import Any

from .executor import call_llm, extract_json_dict, tool_failure_summary

logger = logging.getLogger(__name__)

_JUDGE_PROMPT = """You are validating a task result against a quality criterion.

Criterion: {description}

Task result:
{output}
{tools}
Answer ONLY with a JSON object: {{"valid": true/false, "feedback": "<why, and what to fix if invalid>"}}"""

#: Appended only when something actually failed. The judge otherwise sees the
#: output text ALONE, and so explains a shortfall with the only cause it can
#: observe — the writing. Observed: a task whose every source call returned
#: 404/503 was told three times to "define named agents and an ordered task
#: sequence". No rewrite could have satisfied it; the data did not exist. With
#: this, the judge can say "cannot verify ingestion: the source was
#: unavailable", which is a verdict a human can act on.
_TOOL_CONTEXT = """
Tool calls made while producing this result did not all succeed:
{summary}

Weigh this. If the criterion requires evidence that a failed tool was the only
way to obtain, say so in your feedback and name the tool — do not ask for a
rewrite that cannot fix it. Judge the result on what was achievable.
"""


class LLMGuardrail:
    def __init__(self, description: str, llm: Any):
        self.description = description
        self.llm = llm

    def __call__(self, task_output: Any) -> tuple[bool, Any]:
        failures = tool_failure_summary()
        prompt = _JUDGE_PROMPT.format(
            description=self.description,
            output=task_output.raw,
            tools=_TOOL_CONTEXT.format(summary=failures) if failures else "",
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
