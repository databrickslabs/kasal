"""DAX-quality measurement for extracted PBI measures.

A measure "carries real DAX" when its expression is longer than a bare column
reference. A model with many measures but almost none carrying real DAX is
degraded: the run would emit plausible but wrong measures, so it is flagged.
"""

import re
from collections.abc import Mapping, Sequence

from src.services.tools.pipeline_config_generator_utils.config_access import (
    text_value,
)

MIN_MEASURES_FOR_GUARD = 20
_MIN_REAL_DAX_LENGTH = 20
_BARE_REFERENCE = re.compile(r"[\w ]+")


def _expression(measure: Mapping[str, object]) -> str:
    return text_value(measure.get("expression")).strip()


def has_real_dax(measure: Mapping[str, object]) -> bool:
    """True when the measure's expression is more than a bare column reference."""
    expression = _expression(measure)
    return (
        len(expression) > _MIN_REAL_DAX_LENGTH
        and _BARE_REFERENCE.fullmatch(expression) is None
    )


def dax_quality(measures: Sequence[Mapping[str, object]]) -> tuple[int, int]:
    """``(measures_with_real_dax, total_measures)``."""
    return sum(1 for measure in measures if has_real_dax(measure)), len(measures)


def is_degraded(with_real: int, total: int) -> bool:
    """A real-sized model where fewer than a quarter of the measures carry DAX."""
    return total >= MIN_MEASURES_FOR_GUARD and with_real < max(1, total // 4)


def degraded_dax_message(with_real: int, total: int) -> str:
    """The loud warning recorded when a run stays degraded after the retry."""
    return (
        f"DEGRADED MEASURE DAX: only {with_real}/{total} measures have "
        "real DAX expressions — the rest are bare column references. "
        "Generated measures will be largely incorrect. Root cause is "
        "upstream measure extraction (Execute Queries / DMV returned "
        "empty EXPRESSION, or admin-scan fallback). Do NOT treat this "
        "run as production-quality; re-run or check the non-admin SP's "
        "Execute Queries access to this dataset."
    )
