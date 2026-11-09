"""What a single value may show in confidential-values mode."""

import datetime
from decimal import Decimal

import pytest

from src.services.tools.confidential.cells import (
    PLACEHOLDER,
    classify_text,
    classify_value,
    may_show_kind,
)


@pytest.mark.parametrize(
    "text, kind",
    [
        ("North", "text"),
        ("2024-01-02", "date"),
        ("2024-01-02T10:15:00.123Z", "date"),
        ("12", "int"),
        ("-7", "int"),
        ("true", "bool"),
        ("False", "bool"),
        ("", "null"),
        ("None", "null"),
        ("NaN", "null"),
        ("0.3311", "fractional"),
        ("14.0", "fractional"),
        ("12.5 kg", "fractional"),
        ("3.0%", "fractional"),
        ("1,234.50", "fractional"),
        ("2e-4", "fractional"),
        ("Plant 1234", "text"),
    ],
)
def test_text_is_classified_by_what_it_says(text, kind):
    assert classify_text(text) == kind


@pytest.mark.parametrize(
    "value, kind",
    [
        (None, "null"),
        (True, "bool"),
        (7, "int"),
        (0.0, "fractional"),
        (3.0, "fractional"),
        (-1.25, "fractional"),
        ("Plant A", "text"),
        (datetime.date(2024, 1, 2), "date"),
        (datetime.datetime(2024, 1, 2, 3, 4), "date"),
        (Decimal("12.50"), "fractional"),
    ],
)
def test_structured_values_are_classified_by_type(value, kind):
    assert classify_value(value) == kind


def test_every_float_is_fractional_even_when_whole():
    # The type decides, not the value: a DAX 3.0 is a measure, not a key.
    assert classify_value(3.0) == "fractional"


@pytest.mark.parametrize(
    "kind, shown",
    [
        ("text", True),
        ("int", True),
        ("date", True),
        ("bool", True),
        ("null", True),
        ("fractional", False),
    ],
)
def test_only_fractional_kind_is_withheld(kind, shown):
    assert may_show_kind(kind) is shown


def test_placeholder_is_not_a_number():
    assert not any(ch.isdigit() for ch in PLACEHOLDER)
