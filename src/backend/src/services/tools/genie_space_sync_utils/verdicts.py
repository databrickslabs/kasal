"""Verdict records for Genie space checks (pure).

Each check yields one ``Verdict`` per object it looked at. ``pass`` means the
object checked out, ``fail`` means it is broken (the space or a mapping would
misbehave), ``review`` is a hint for a human, never a failure.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass

PASS = "pass"
FAIL = "fail"
REVIEW = "review"


@dataclass(frozen=True)
class Verdict:
    check: str
    target: str
    status: str
    detail: str = ""

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


def summarize(verdicts: list[Verdict]) -> dict[str, int]:
    counts = Counter(verdict.status for verdict in verdicts)
    return {status: counts.get(status, 0) for status in (PASS, FAIL, REVIEW)}


def short(text: object, limit: int = 300) -> str:
    """A bounded, single-line rendering of an error or a label."""
    flat = " ".join(str(text or "").split())
    return flat if len(flat) <= limit else flat[: limit - 3] + "..."
