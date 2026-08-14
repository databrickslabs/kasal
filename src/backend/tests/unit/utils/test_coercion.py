"""One guard, in one place.

Seven copies of "coerce this config value to a positive int" existed across the
tree and five of them did not catch OverflowError — so a JSON `Infinity` raised
out of code whose whole job was tolerating bad input. Two call sites now share
this; the rest can follow.
"""

import pytest

from src.utils.coercion import positive_int


@pytest.mark.parametrize(
    "value,expected", [(1, 1), (131072, 131072), ("3600", 3600), (7.9, 7)]
)
def test_a_usable_number_comes_back(value, expected):
    assert positive_int(value) == expected


@pytest.mark.parametrize("value", [None, "", "soon", {}, [], object()])
def test_what_configuration_actually_contains_is_refused(value):
    assert positive_int(value) is None


@pytest.mark.parametrize("value", [0, -1, -131072])
def test_zero_and_negatives_are_refused(value):
    """A zero window or TTL is not a small value, it is a broken one."""
    assert positive_int(value) is None


def test_infinity_is_refused_rather_than_raising():
    """int(float('inf')) raises OverflowError, not ValueError — the clause five
    of the seven copies are missing."""
    assert positive_int(float("inf")) is None
    assert positive_int(float("-inf")) is None


def test_the_default_is_returned_on_refusal():
    assert positive_int("nonsense", 3600) == 3600
    assert positive_int(None, 3600) == 3600


def test_a_good_value_beats_the_default():
    assert positive_int(60, 3600) == 60
