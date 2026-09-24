"""Validate provider answers before any caller may act on them."""

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Choice:
    selected: str
    confidence: float
    probabilities: dict[str, float]

    @property
    def accepted(self) -> bool:
        # Conservative initial gate, not a claim of calibrated Kasal accuracy.
        return self.confidence >= 0.85 and self.probabilities[self.selected] >= 0.85


def choices_from_response(payload: dict, questions: dict) -> dict[str, Choice]:
    answers = payload["answers"]
    if not isinstance(answers, dict) or set(answers) != set(questions):
        raise ValueError("Question IDs do not match")
    result = {}
    for key, question in questions.items():
        answer = answers[key]
        probabilities = answer["probabilities"]
        if answer["type"] != "choice" or set(probabilities) != set(
            question["criteria"]
        ):
            raise ValueError("Choice options do not match")
        values = [answer["confidence"], *probabilities.values()]
        if any(
            isinstance(v, bool)
            or not isinstance(v, (int, float))
            or not math.isfinite(v)
            or not 0 <= v <= 1
            for v in values
        ):
            raise ValueError("Invalid probabilities")
        if not math.isclose(sum(probabilities.values()), 1, abs_tol=0.01):
            raise ValueError("Probabilities do not sum to one")
        selected = answer["choice"]
        if selected not in probabilities or probabilities[selected] != max(
            probabilities.values()
        ):
            raise ValueError("Invalid selected option")
        result[key] = Choice(selected, answer["confidence"], probabilities)
    return result
