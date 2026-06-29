"""Tests for coerce_scalar_value (router condition value normalization).

A crew may return ``has_results: true`` (JSON bool → Python True) on one run and
``"true"``/``"True"`` (string) on another. A router condition like
``has_results == True`` would silently be False for the string form, so the flow
dead-ends non-deterministically. coerce_scalar_value maps boolean/numeric strings
to their natural types so both forms compare equal.
"""

from src.engines.crewai.paths.flow.modules.flow_builder import coerce_scalar_value


def test_true_strings_become_python_true():
    for v in ("true", "True", "TRUE", "  true  "):
        assert coerce_scalar_value(v) is True


def test_false_strings_become_python_false():
    for v in ("false", "False", "FALSE", " false "):
        assert coerce_scalar_value(v) is False


def test_string_true_compares_equal_to_bool_after_coercion():
    # The actual router bug: "true" == True is False until coerced.
    assert ("true" == True) is False  # noqa: E712 — demonstrates the bug
    assert (coerce_scalar_value("true") == True) is True  # noqa: E712


def test_numeric_strings_convert():
    assert coerce_scalar_value("14") == 14
    assert coerce_scalar_value("0.96") == 0.96


def test_non_scalar_strings_pass_through():
    assert coerce_scalar_value("Databricks Blog") == "Databricks Blog"


def test_non_strings_pass_through_unchanged():
    assert coerce_scalar_value(True) is True
    assert coerce_scalar_value(False) is False
    assert coerce_scalar_value(7) == 7
    assert coerce_scalar_value(None) is None
