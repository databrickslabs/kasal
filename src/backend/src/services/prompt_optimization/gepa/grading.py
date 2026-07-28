"""Turning judge output into a number.

GEPA needs a scalar to optimise against, but a judge answers in prose. These
convert one to the other — and every one of them has to fail SOFT, returning
None rather than raising, because an unparseable verdict must cost one sample
and not the whole optimisation run.
"""

import logging
import re
from typing import Any, Dict, List, Optional

from src.utils.prompt_utils import robust_json_parser

logger = logging.getLogger(__name__)

_CATEGORICAL_GRADES = {
    1.0: ("excellent", "perfect", "outstanding", "flawless", "exceptional"),
    0.75: ("good", "great", "strong", "satisfactory", "yes", "true", "pass", "correct"),
    0.5: (
        "partial",
        "partially correct",
        "fair",
        "moderate",
        "average",
        "mixed",
        "acceptable",
    ),
    0.25: ("poor", "weak", "insufficient", "lacking"),
    0.0: (
        "bad",
        "fail",
        "failed",
        "wrong",
        "incorrect",
        "no",
        "false",
        "unsatisfactory",
    ),
}

JUDGE_SPREAD_WARN = 0.3

VALID_INTENTS = {
    "generate_task",
    "generate_agent",
    "generate_crew",
    "execute_crew",
    "configure_crew",
    "unknown",
}


def _judge_value_to_grade(value: Any) -> Optional[float]:
    """Normalize a judge verdict (number, bool, or categorical word) to 0-1.

    Returns None when the value carries no usable grade.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        number = float(value)
        return max(0.0, min(1.0, number / 10.0 if number > 1.0 else number))
    if isinstance(value, str):
        text = value.strip().lower()
        try:
            number = float(text)
            return max(0.0, min(1.0, number / 10.0 if number > 1.0 else number))
        except ValueError:
            pass
        for grade, words in _CATEGORICAL_GRADES.items():
            if text in words:
                return grade
    return None


def _checklist_grade(verdict: str, n_requirements: int) -> Optional[float]:
    """Compute a 0-1 grade from a checklist verdict's PASS/FAIL marks.

    The grade is COMPUTED from the marks, never taken from the model's own
    arithmetic — a judge writing "40" as its final number would clamp to a
    perfect 10/10 (observed live). Blend: 0.8 x fraction of requirements
    passed + 0.2 x the judge's base-quality Q mark (default 5 when absent),
    so requirement-equal candidates still order by answer quality.

    Returns None when no marks are found (caller falls back to last-number
    parsing).
    """
    marks = re.findall(r"\bR(\d+)\s*[:.]?\s*(PASS|FAIL)", verdict or "", re.IGNORECASE)
    if not marks or n_requirements <= 0:
        return None
    seen_marks: Dict[str, bool] = {}
    for num, mark in marks:
        # First mark per requirement wins (models sometimes restate at the end).
        seen_marks.setdefault(num, mark.upper() == "PASS")
    passed = sum(1 for ok in seen_marks.values() if ok)
    fraction = passed / max(n_requirements, len(seen_marks))
    quality = 0.5
    q_match = re.search(r"\bQ\s*[:.]?\s*(\d+(?:\.\d+)?)", verdict or "", re.IGNORECASE)
    if q_match:
        q_value = float(q_match.group(1))
        quality = max(0.0, min(10.0, q_value)) / 10.0
    return max(0.0, min(1.0, 0.8 * fraction + 0.2 * quality))


def _grade_judge_verdict(verdict: str, n_requirements: int) -> Optional[tuple]:
    """One correctness-judge verdict -> (grade 0-1, rationale), or None.

    Order matters and is load-bearing:
      1. CHECKLIST first when the run has human requirements — the grade is
         COMPUTED from the PASS/FAIL marks, never from the judge's own
         arithmetic (a judge writing "40" as its final number would otherwise
         clamp to a perfect 10/10, observed live).
      2. Otherwise LAST number wins — thinking models emit incidental numbers
         while reasoning before stating the grade — with values in (10, 100]
         read as percentages, because clamping alone turned a hallucinated
         "40" into 10/10.

    Returns None when the reply carries no usable grade at all, so the caller
    can discard that sample instead of scoring it 0 (a parse miss is not
    evidence the deliverable was bad).
    """
    text = str(verdict or "").strip()
    rationale = text
    if n_requirements > 0:
        checklist_value = _checklist_grade(text, n_requirements)
        if checklist_value is not None:
            return checklist_value, rationale
    matches = re.findall(r"\d+(?:\.\d+)?", text)
    if not matches:
        logger.warning(f"Crew optimization judge reply not numeric: {verdict!r}")
        return None
    number = float(matches[-1])
    if 10.0 < number <= 100.0:
        number /= 10.0
    return max(0.0, min(10.0, number)) / 10.0, rationale


def _parse_grade_from_text(text: str) -> Optional[float]:
    """0-1 grade from a judge reply: LAST number wins (thinking models emit
    incidental numbers first); values in (10, 100] are read as percentages —
    clamping alone turned a hallucinated '40' into a perfect 10/10."""
    matches = re.findall(r"\d+(?:\.\d+)?", text or "")
    if not matches:
        return None
    number = float(matches[-1])
    if 10.0 < number <= 100.0:
        number /= 10.0
    return max(0.0, min(10.0, number)) / 10.0


def _job_name_score(outputs: Any) -> float:
    """Format scorer for generate_job_name: a short plain-text name (2-4 words,
    no JSON/markdown artifacts)."""
    text = str(outputs or "").strip().strip('"').strip()
    if not text or "\n" in text or "{" in text or len(text) > 80:
        return 0.0
    words = len(text.split())
    return 1.0 if 2 <= words <= 4 else 0.5 if 1 <= words <= 6 else 0.0


def _intent_format_score(outputs: Any) -> float:
    """Deterministic scorer: does the output honor the template's JSON contract?"""
    try:
        parsed = robust_json_parser(str(outputs))
    except Exception:
        return 0.0
    if not isinstance(parsed, dict):
        return 0.0
    score = 0.0
    if parsed.get("intent") in VALID_INTENTS:
        score += 0.6
    try:
        confidence = float(parsed.get("confidence"))
        if 0.0 <= confidence <= 1.0:
            score += 0.2
    except (TypeError, ValueError):
        pass
    if isinstance(parsed.get("extracted_info"), dict):
        score += 0.1
    if (
        isinstance(parsed.get("suggested_prompt"), str)
        and parsed["suggested_prompt"].strip()
    ):
        score += 0.1
    return score


def _json_keys_score(outputs: Any, required_keys: tuple) -> float:
    """Generic format scorer: output parses as a JSON object and every required
    key is present with a non-empty value (string, list, or object). Returns
    the satisfied fraction."""
    try:
        parsed = robust_json_parser(str(outputs))
    except Exception:
        return 0.0
    if not isinstance(parsed, dict):
        return 0.0

    def _ok(value: Any) -> bool:
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, dict)):
            return len(value) > 0
        return value is not None

    satisfied = sum(1 for key in required_keys if _ok(parsed.get(key)))
    return satisfied / len(required_keys) if required_keys else 0.0


def _median_sample(samples: List[tuple]) -> tuple:
    """Reduce (grade, rationale) samples to (median grade, one rationale).

    MEDIAN, not mean: the judge is stochastic and its outliers are large (the
    same prompt has drawn 0.0 and 0.4 minutes apart), and a median ignores a
    single wild draw instead of letting it move the score. The rationale kept
    is the one from the sample NEAREST the median, so the text GEPA's
    reflection model reads actually explains the score it was given.
    """
    if not samples:
        return 0.0, ""
    if len(samples) == 1:
        return samples[0]
    grades = sorted(float(g) for g, _ in samples)
    middle = len(grades) // 2
    median = (
        grades[middle]
        if len(grades) % 2
        else (grades[middle - 1] + grades[middle]) / 2.0
    )
    nearest = min(samples, key=lambda s: abs(float(s[0]) - median))
    spread = grades[-1] - grades[0]
    if spread >= JUDGE_SPREAD_WARN:
        logger.warning(
            "Judge disagreed with itself across %d samples of the SAME "
            "deliverable: %s (spread %.2f, median %.2f). A judge this "
            "unstable cannot rank candidates — the run's score movement may "
            "be noise.",
            len(grades),
            [round(g, 2) for g in grades],
            spread,
            median,
        )
    return median, nearest[1]


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
