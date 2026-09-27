"""Score a deliverable with the run's selected built-in judges.

Built-ins join Kasal's grading step next to the custom judges rather than
running as separate GEPA scorers, so the deliverable cache and the execution
budget still hold. Each judge answers yes/no; ``combine`` folds the answers
into the judge score:

* a **graded** judge's verdict (yes=1, no=0) joins the weighted mean of the
  judge grades, with the catalog's weight (Kasal's own judges weigh 1 each);
* a **gate** judge's "no" multiplies the result by ``GATE_FAILURE_MULTIPLIER``.

A built-in that fails (provider error, unparseable reply, a verdict that is
not yes/no) is logged and left out; the mean is then taken over the judges
that did score. Scoring never raises.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple

from src.services.prompt_optimization.builtin_judges.bridge import (
    JudgeRoute,
    judge_route,
    placeholder_uri,
)
from src.services.prompt_optimization.builtin_judges.catalog import (
    GATE_FAILURE_MULTIPLIER,
    BuiltinJudge,
    build_scorer,
    resolve_selection,
)

if TYPE_CHECKING:
    from mlflow.genai.scorers import Scorer

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BuiltinVerdict:
    judge: BuiltinJudge
    #: 1.0 for yes, 0.0 for no, None when the judge failed and is left out.
    score: Optional[float]
    rationale: str = ""


def to_score(value: object) -> Optional[float]:
    """yes -> 1.0, no -> 0.0 (also bools and 0..1 numbers); else None."""
    raw = getattr(value, "value", value)  # Feedback / CategoricalRating
    if isinstance(raw, bool):
        return 1.0 if raw else 0.0
    if isinstance(raw, (int, float)):
        return float(raw) if 0.0 <= raw <= 1.0 else None
    if isinstance(raw, str):
        return {"yes": 1.0, "no": 0.0}.get(raw.strip().lower())
    return None


def _verdict(judge: BuiltinJudge, result: object) -> BuiltinVerdict:
    """One verdict from what ``Scorer.run`` returned (a Feedback, or a list
    of them for per-span judges, reduced by mean)."""
    items = result if isinstance(result, list) else [result]
    scores: List[float] = []
    rationales: List[str] = []
    for item in items:
        error = getattr(item, "error", None)
        if error:
            raise ValueError(f"judge returned an error: {error}")
        score = to_score(item)
        if score is None:
            raise ValueError(f"not a yes/no verdict: {getattr(item, 'value', item)!r}")
        scores.append(score)
        rationale = getattr(item, "rationale", None)
        if rationale:
            rationales.append(str(rationale))
    if not scores:
        raise ValueError("judge returned no verdict")
    return BuiltinVerdict(judge, sum(scores) / len(scores), " ".join(rationales))


class BuiltinJudgeRunner:
    """The selected built-ins for one run, built once and cached per deliverable."""

    def __init__(
        self,
        judges: Sequence[BuiltinJudge],
        judge_model: str,
        route: JudgeRoute,
        guidelines: Sequence[str] = (),
    ) -> None:
        self.judges = list(judges)
        self._model_uri = placeholder_uri(judge_model)
        self._route = route
        self._guidelines = list(guidelines)
        self._scorers: Dict[str, Scorer] = {}
        self._cache: Dict[str, List[BuiltinVerdict]] = {}

    @classmethod
    def for_run(
        cls,
        judge_ids: Sequence[str],
        judge_model: str,
        route: JudgeRoute,
        rubric: str = "",
    ) -> Optional["BuiltinJudgeRunner"]:
        """The runner for a run's selection, or None when nothing is selected.

        ``rubric`` (the tasks' expected outputs plus the user's guidance, one
        per line) becomes the Guidelines judge's guidelines.
        """
        if not judge_ids:
            return None
        guidelines = [
            line.strip().lstrip("-").strip()
            for line in rubric.splitlines()
            if line.strip().lstrip("-").strip()
        ]
        return cls(resolve_selection(judge_ids), judge_model, route, guidelines)

    def score(self, request: str, response: str) -> List[BuiltinVerdict]:
        """Every selected judge's verdict on ``response``. Never raises."""
        key = hashlib.sha256(f"{request}\x00{response}".encode("utf-8")).hexdigest()
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        with judge_route(self._route):
            verdicts = [self._score_one(j, request, response) for j in self.judges]
        self._cache[key] = verdicts
        return verdicts

    def _score_one(
        self, judge: BuiltinJudge, request: str, response: str
    ) -> BuiltinVerdict:
        try:
            scorer = self._scorers.get(judge.id)
            if scorer is None:
                scorer = build_scorer(judge.id, self._model_uri, self._guidelines)
                self._scorers[judge.id] = scorer
            return _verdict(
                judge, scorer.run(inputs={"request": request}, outputs=response)
            )
        except Exception:
            # One broken judge must not fail the metric (mlflow would abort the
            # whole optimization); it is left out and the mean renormalises.
            logger.exception(
                "Built-in judge %s failed; left out of this score", judge.id
            )
            return BuiltinVerdict(judge, None)


def combine(
    grades: Sequence[float], verdicts: Sequence[BuiltinVerdict]
) -> Tuple[float, List[str]]:
    """The judge score and the rationale lines the verdicts add to it.

    ``grades`` are Kasal's own judge grades (0..1, weight 1 each). With no
    built-ins this is exactly the plain mean the grading step always used.
    """
    total = float(sum(grades))
    weight = float(len(grades))
    lines: List[str] = []
    gate_failed: List[str] = []
    for verdict in verdicts:
        if verdict.score is None:
            continue
        answer = "yes" if verdict.score >= 0.5 else "no"
        lines.append(
            f"[builtin:{verdict.judge.id}] {answer}. {verdict.rationale}".strip()
        )
        if verdict.judge.role == "gate":
            if verdict.score < 0.5:
                gate_failed.append(verdict.judge.id)
        else:
            total += verdict.judge.weight * verdict.score
            weight += verdict.judge.weight
    value = total / weight if weight else 0.0
    if gate_failed:
        value *= GATE_FAILURE_MULTIPLIER
        lines.append(
            f"Gate failed ({', '.join(gate_failed)}): the judge score is "
            f"multiplied by {GATE_FAILURE_MULTIPLIER}."
        )
    return value, lines
