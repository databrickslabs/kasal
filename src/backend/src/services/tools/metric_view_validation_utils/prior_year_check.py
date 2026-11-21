"""Judges a prior-year measure: the window in the YAML against the one-year shift in its DAX.

The generator emits a prior-year measure as its base (``MEASURE(base)`` or the base's
aggregate) plus a ``window`` that orders by the period key and reads one year back. The
DAX is the same calculation shifted one year back (``SAMEPERIODLASTYEAR``, ``DATEADD``,
a prior-year calendar column). A rule-based check can only tell whether the window
carries that shift; it cannot tell from a window alone that the shift is there.

``judge_prior_year`` returns the decided verdict when the window settles the measure,
or the un-shifted DAX to compare with the normal rules when the base still has to be
compared. It returns None when the measure is not a prior-year measure and has no window.
"""

import re
from dataclasses import dataclass
from typing import Optional

from ..metric_view_utils.prior_year_dax import unshifted
from .constants import STATUS_INVALID, STATUS_REVIEW, STATUS_VALID

_MEASURE_REF = re.compile(r"^\s*MEASURE\(\s*`?(\w+)`?\s*\)\s*$", re.IGNORECASE)
_DAX_MEASURE_REF = re.compile(r"^\s*\[\s*([^\]]+?)\s*\]\s*$")
_ONE_YEAR_BACK_TEXT = re.compile(
    r"^\s*-\s*(1\s*years?|12\s*months?|4\s*quarters?)\s*$", re.IGNORECASE
)
_INTEGER = re.compile(r"^\s*-?\d+\s*$")


@dataclass(frozen=True)
class PriorYearJudgement:
    """``status`` is set when the window settles the measure; otherwise ``base_dax`` is
    the DAX without its shift, to be compared with the measure's SQL by the normal rules.
    """

    status: Optional[str]
    reason: str
    base_dax: Optional[str] = None


def loose_name(name: object) -> str:
    """A measure name with case, spaces, underscores and punctuation removed.

    ``%`` is spelled ``pct`` first, as the generator spells it in YAML names, so
    ``Margin % PY`` and ``Margin PY`` stay two different names.
    """
    text = str(name or "").lower().replace("%", "pct")
    return re.sub(r"[^a-z0-9]", "", text)


def window_shifts_back(window: object) -> bool:
    """True when some window spec reads a negative offset (one period or more back).

    Offsets are negative steps on the window's key (``-1000`` for YYYYPPP, ``-100`` for
    YYYYMM, or a configured step) or the text ``-1 year``. The validator does not know the
    step size, so any negative offset counts as a shift back.
    """
    specs = window if isinstance(window, list) else [window]
    for spec in specs:
        if not isinstance(spec, dict):
            continue
        offset = spec.get("offset")
        if isinstance(offset, bool):
            continue
        if isinstance(offset, (int, float)) and offset < 0:
            return True
        if isinstance(offset, str):
            if _INTEGER.match(offset) and int(offset) < 0:
                return True
            if _ONE_YEAR_BACK_TEXT.match(offset):
                return True
    return False


def judge_prior_year(
    yaml_measure: dict, dax_expr: object
) -> Optional[PriorYearJudgement]:
    """The prior-year judgement for one YAML measure against its DAX, or None when the
    measure is neither a prior-year measure nor carries a window."""
    window = yaml_measure.get("window")
    prior = unshifted(dax_expr)
    if prior is None:
        if not window:
            return None
        if not window_shifts_back(window):
            return PriorYearJudgement(
                STATUS_INVALID,
                "window does not shift one year back, and the DAX is not a prior-year measure",
            )
        return PriorYearJudgement(
            STATUS_REVIEW,
            "window present, but the DAX is not a prior-year measure as a whole, "
            "so the shift cannot be checked against it",
        )
    if not window:
        return PriorYearJudgement(
            STATUS_INVALID,
            f"prior year lost: the DAX reads {prior.calendar}.{prior.column} one year "
            "back, but the measure has no window",
        )
    if not window_shifts_back(window):
        return PriorYearJudgement(
            STATUS_INVALID,
            "window does not shift one year back, but the DAX reads the prior year",
        )
    expr = str(yaml_measure.get("expr") or "")
    ref = _MEASURE_REF.match(expr)
    if ref is None:
        return PriorYearJudgement(
            None,
            "prior year: window shifts one year back, as the DAX does; base compared",
            prior.dax,
        )
    base = _DAX_MEASURE_REF.match(prior.dax)
    if base is None:
        return PriorYearJudgement(
            STATUS_REVIEW,
            f"prior year: MEASURE({ref.group(1)}) cannot be compared, the DAX base is "
            "not a single measure",
        )
    if loose_name(base.group(1)) != loose_name(ref.group(1)):
        return PriorYearJudgement(
            STATUS_INVALID,
            f"prior year: window reads MEASURE({ref.group(1)}), but the DAX shifts "
            f"[{base.group(1)}]",
        )
    return PriorYearJudgement(
        STATUS_VALID,
        f"prior year: window shifts MEASURE({ref.group(1)}) one year back, as the DAX does",
    )


def is_prior_year_dax(dax_expr: object) -> bool:
    """True when the DAX shifts the whole result one year back."""
    return unshifted(dax_expr) is not None
