"""Deterministic number check: does an answer contain the expected figures? (pure)

Ported from the reference ``genie_pbi_as_code`` numeric check. The tolerance is the
precision the figure is written with: a figure counts as found when some number in
the text equals it at the coarser of the two precisions. "42.9" matches 42.869,
"43.1" does not; "56.1759" matches an expected 56.176. Signs are ignored, and a
fraction matches the same figure written as a percentage (0.57415 and 57.415%).
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

_NUMBER_RE = re.compile(r"(?<![\w.])[-+−]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?![\w])")
_WHOLE_NUMBER_RE = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")
_YEAR_RANGE = (1900, 2100)
_SMALL_INT_MAX = 12
_FRACTION_MAX = 1.5  # up to here a decimal figure may be a fraction of a percentage


@dataclass(frozen=True)
class Number:
    value: float
    decimals: int


def decimals_of(value: float) -> int:
    """Decimals a float is written with (57.11 -> 2); integers and exponent forms count as 0."""
    text = repr(float(value))
    return 0 if "e" in text or text.endswith(".0") else len(text.split(".")[1])


def figure(value: float) -> Number:
    """An expected figure given as a number (a key fact or a SQL cell)."""
    return Number(abs(float(value)), decimals_of(value))


def extract_numbers(text: str) -> list[Number]:
    """Every number in *text*, with the number of decimals it is written with."""
    numbers: list[Number] = []
    for raw in _NUMBER_RE.findall(text or ""):
        cleaned = raw.replace(",", "").replace("−", "-")
        decimals = len(cleaned.split(".")[1]) if "." in cleaned else 0
        numbers.append(Number(float(cleaned), decimals))
    return numbers


def parse_number(text: str) -> Number | None:
    """The number a whole cell value spells (``"1234.50"``, ``"1.0E7"``), or None."""
    stripped = (text or "").strip()
    if not _WHOLE_NUMBER_RE.fullmatch(stripped):
        return None
    value = float(stripped)
    if not math.isfinite(value):
        return None
    if "e" in stripped.lower():
        return Number(abs(value), decimals_of(value))
    decimals = len(stripped.split(".")[1]) if "." in stripped else 0
    return Number(abs(value), decimals)


def expected_from_text(expected_answer: str) -> list[Number]:
    """The figures an expected answer states: its numbers, minus years and bare small integers.

    "top 3" and "2026" are wording, not results, so they are skipped.
    """
    figures: list[Number] = []
    for number in extract_numbers(expected_answer):
        is_whole = number.decimals == 0
        if is_whole and (
            _YEAR_RANGE[0] <= number.value <= _YEAR_RANGE[1]
            or abs(number.value) <= _SMALL_INT_MAX
        ):
            continue
        if abs(number.value) not in [f.value for f in figures]:
            figures.append(Number(abs(number.value), number.decimals))
    return figures


def _readings(number: Number) -> list[tuple[float, int]]:
    """The ``(value, decimals)`` pairs a number in the text can stand for: itself, and a fraction as a percent."""
    readings = [(abs(number.value), number.decimals)]
    if number.decimals >= 2 and abs(number.value) <= _FRACTION_MAX:
        readings.append((abs(number.value) * 100, number.decimals - 2))
    return readings


def _targets(expected: Number) -> list[tuple[float, int]]:
    """The ``(value, decimals)`` pairs an expected figure can stand for: itself, and a fraction written as a percent."""
    value = abs(expected.value)
    is_fraction = 0 < value <= _FRACTION_MAX and value != int(value)
    if not is_fraction:
        return [(value, expected.decimals)]
    return [(value, expected.decimals), (value * 100, expected.decimals - 2)]


def _half_unit(decimals: int) -> float:
    return 0.5 / float(10**decimals)


def _same_value(
    value: float, decimals: int, target: float, target_decimals: int
) -> bool:
    """A number in the text against an expected figure, each at the precision it is written in.

    * The text is as precise or less: it must be the expected figure rounded to the text's
      precision, capped so a bare "3" cannot stand for 3.098.
    * The text is more precise: it must round to the expected figure, which is itself a
      rounded value ("56.1759" for 56.176).
    """
    if decimals > target_decimals:
        return abs(value - target) <= _half_unit(target_decimals) + 1e-9
    cap = max(0.005 * target, 0.05)
    return abs(value - target) <= min(_half_unit(decimals), cap) + 1e-9


def is_found(expected: Number, numbers: list[Number]) -> bool:
    """True when some number in *numbers* is *expected* at the coarser of the two precisions."""
    return any(
        _same_value(value, decimals, target, target_decimals)
        for target, target_decimals in _targets(expected)
        for number in numbers
        for value, decimals in _readings(number)
    )


def relative_difference(expected: Number, numbers: list[Number]) -> float | None:
    """How far the closest number in *numbers* is from *expected*, relative to it (0 = exact).

    None when there is no number at all. A zero expected figure gives the absolute difference.
    """
    best: float | None = None
    for target, _ in _targets(expected):
        for number in numbers:
            for value, _ in _readings(number):
                gap = (
                    abs(value - target)
                    if target == 0
                    else abs(value - target) / abs(target)
                )
                best = gap if best is None else min(best, gap)
    return None if best is None else round(float(best), 6)
