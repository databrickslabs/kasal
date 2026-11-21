"""A MEASURE(name) call is a reference to a measure, not a column: the names and the arithmetic
around the calls are compared with the DAX that calls the measure as [Name]."""

import pytest

from src.services.tools.metric_view_validation_utils.expression_validator import (
    ExpressionValidator,
)
from src.services.tools.metric_view_validation_utils.measure_expression_tree import (
    canonical_shape,
    compare_shapes,
    composed_of_measures_only,
)
from src.services.tools.metric_view_validation_utils.measure_references import (
    dax_measure_refs,
    strip_measure_calls,
    ucmv_measure_refs,
)


def _validator() -> ExpressionValidator:
    return ExpressionValidator(data_handler=None, table_mappings={}, column_mappings={})


class TestMeasureReferenceSets:
    def test_call_and_bracket_name_the_same_measure_despite_snake_case(self):
        assert ucmv_measure_refs("MEASURE(sales_py) * 2") == {"salespy"}
        assert dax_measure_refs("[Sales PY] * 2") == {"salespy"}

    def test_a_table_qualified_column_is_not_a_measure_reference(self):
        assert dax_measure_refs("CALCULATE(SUM(Fact_Orders[amount]))") == set()

    def test_strip_removes_the_name_so_it_is_not_read_as_a_column(self):
        assert "unit_rate" not in strip_measure_calls(
            "MEASURE(unit_rate) * SUM(source.qty)"
        )


class TestMeasureCallsInValidation:
    def test_alias_of_the_measure_the_dax_names_is_a_reference_match(self):
        result = _validator().validate("MEASURE(order_gap)", "[Order Gap]")
        assert "Reference mismatch" not in " ".join(result["differences"])
        assert any("Measure references match" in s for s in result["similarities"])

    def test_call_to_a_measure_the_dax_does_not_use_is_a_difference(self):
        result = _validator().validate("MEASURE(order_gap) * 2", "[Stock Gap] * 2")
        assert any(
            "calls a measure the DAX does not reference" in d
            for d in result["differences"]
        )
        assert result["is_valid"] is False

    def test_dax_measure_the_expression_does_not_call_is_a_difference(self):
        result = _validator().validate(
            "MEASURE(order_gap)", "[Order Gap] + [Stock Gap]"
        )
        assert any("does not call" in d for d in result["differences"])

    def test_operator_precedence_bug_over_measures_is_a_structure_difference(self):
        db = "MEASURE(part_a) + MEASURE(part_b) / NULLIF(ABS(MEASURE(base)), 0)"
        dax = "DIVIDE([Part A] + [Part B], ABS([Base]), 0)"
        result = _validator().validate(db, dax)
        assert result["is_valid"] is False
        assert any("Structure mismatch (measures)" in d for d in result["differences"])

    def test_same_ratio_of_a_measure_and_a_column_sum_is_matched(self):
        db = "MEASURE(unit_rate) * SUM(source.qty) / NULLIF(SUM(source.total) * 100, 0)"
        dax = "DIVIDE([Unit Rate] * CALCULATE(SUM(Fact[qty])), CALCULATE(SUM(Fact[total]) * 100), BLANK())"
        result = _validator().validate(db, dax)
        assert not any("Structure mismatch" in d for d in result["differences"])

    def test_arithmetic_over_measures_alone_is_not_valid_on_its_own_row(self):
        result = _validator().validate(
            "1 - MEASURE(share_a) - MEASURE(share_b)", "1 - [Share A] - [Share B]"
        )
        assert result["is_valid"] is False
        assert any("Composed of measures only" in d for d in result["differences"])

    def test_bare_alias_of_one_measure_stays_valid(self):
        result = _validator().validate("MEASURE(order_gap)", "[Order Gap]")
        assert result["is_valid"] is True


class TestShapeReader:
    @pytest.mark.parametrize(
        "sql, dax",
        [
            ("MEASURE(a) - MEASURE(b)", "[A] - [B]"),
            ("1 - MEASURE(a) - MEASURE(b)", "1 - [B] - [A]"),
            (
                "MEASURE(a) * SUM(source.x) / NULLIF(SUM(source.y) * 100, 0)",
                "DIVIDE([A] * CALCULATE(SUM(Fact[x])), CALCULATE(SUM(Fact[y]) * 100), BLANK())",
            ),
            (
                "SUM(source.x) FILTER (WHERE source.k = 'z') / NULLIF(SUM(source.y), 0)",
                'CALCULATE(SUM(Fact[x]), Fact[k] = "z") / CALCULATE(SUM(Fact[y]))',
            ),
            ("MEASURE(a) / NULLIF(MEASURE(b), 0)", "DIVIDE([A], [B], 0)"),
        ],
    )
    def test_same_shape_reads_equal(self, sql, dax):
        same, _ = compare_shapes(sql, dax)
        assert same is True

    @pytest.mark.parametrize(
        "sql, dax",
        [
            ("MEASURE(a) - MEASURE(b)", "[B] - [A]"),
            ("MEASURE(a) + MEASURE(b) / MEASURE(c)", "([A] + [B]) / [C]"),
            (
                "MEASURE(a) * SUM(source.x) / NULLIF(SUM(source.y), 0)",
                "[A] * CALCULATE(SUM(Fact[y])) / CALCULATE(SUM(Fact[x]))",
            ),
        ],
    )
    def test_different_shape_reads_unequal(self, sql, dax):
        same, _ = compare_shapes(sql, dax)
        assert same is False

    def test_an_unmodelled_construct_is_not_readable_not_equal(self):
        same, why = compare_shapes(
            "CASE WHEN MEASURE(a) = 0 THEN 0 ELSE MEASURE(b) END", "[B]"
        )
        assert same is None
        assert "not readable" in why

    def test_division_guard_forms_are_the_same_division(self):
        assert canonical_shape("[A] / NULLIF([B], 0)") == canonical_shape(
            "DIVIDE([A], [B])"
        )
        assert canonical_shape("DIVIDE([A], [B], 0)") == canonical_shape("[A] / [B]")

    def test_composed_flag_excludes_a_bare_alias_and_column_aggregates(self):
        assert composed_of_measures_only("MEASURE(a) + MEASURE(b)") is True
        assert composed_of_measures_only("MEASURE(a)") is False
        assert composed_of_measures_only("MEASURE(a) * SUM(source.x)") is False
