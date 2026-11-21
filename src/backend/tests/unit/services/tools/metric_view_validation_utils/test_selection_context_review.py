"""An INVALID verdict that rests on a measure whose definition depends on the selection
(ISFILTERED, HASONEVALUE) is REVIEW: the static comparison cannot read that logic. It never
promotes a verdict."""

from src.services.tools.metric_view_validation_utils.constants import (
    STATUS_INVALID,
    STATUS_REVIEW,
    STATUS_VALID,
)
from src.services.tools.metric_view_validation_utils.expression_validator import (
    ExpressionValidator,
)

_DEFINITIONS = {
    "Base Item Total": "SWITCH(TRUE(), ISFILTERED(Dim[item]), SUM(Fact[qty]), SUM(Fact[qty]))",
    "Plain Total": "SUM(Fact[qty])",
}


class _Handler:
    def get_measure_definition(self, name):
        return _DEFINITIONS.get(name)


def _validator() -> ExpressionValidator:
    validator = ExpressionValidator(
        data_handler=_Handler(), table_mappings={}, column_mappings={}
    )
    return validator


def _result(status: str) -> dict:
    return {
        "status": status,
        "is_valid": status == STATUS_VALID,
        "confidence": "medium",
        "differences": ["Aggregation mismatch: made-up"],
        "similarities": [],
        "recommendations": [],
    }


def test_invalid_resting_on_a_selection_dependent_measure_becomes_review():
    result = _validator()._selection_context_review(
        _result(STATUS_INVALID),
        'CALCULATE([Base Item Total], Fact[k] = "z")',
        "SUM(source.qty) FILTER (WHERE source.k = 'z')",
    )
    assert result["status"] == STATUS_REVIEW
    assert result["is_valid"] is False
    assert any("selection" in r for r in result["recommendations"])


def test_a_plain_referenced_measure_leaves_the_invalid_verdict():
    result = _validator()._selection_context_review(
        _result(STATUS_INVALID),
        'CALCULATE([Plain Total], Fact[k] = "z")',
        "SUM(source.qty)",
    )
    assert result["status"] == STATUS_INVALID


def test_a_measure_the_view_calls_with_measure_is_judged_on_its_own_row():
    result = _validator()._selection_context_review(
        _result(STATUS_INVALID), "[Base Item Total] * 2", "MEASURE(base_item_total) * 2"
    )
    assert result["status"] == STATUS_INVALID


def test_the_rule_never_promotes_a_verdict():
    for status in (STATUS_VALID, STATUS_REVIEW):
        result = _validator()._selection_context_review(
            _result(status),
            'CALCULATE([Base Item Total], Fact[k] = "z")',
            "SUM(source.qty)",
        )
        assert result["status"] == status


def test_no_data_handler_keeps_the_verdict():
    validator = ExpressionValidator(
        data_handler=None, table_mappings={}, column_mappings={}
    )
    result = validator._selection_context_review(
        _result(STATUS_INVALID), "[Base Item Total]", "SUM(x)"
    )
    assert result["status"] == STATUS_INVALID
