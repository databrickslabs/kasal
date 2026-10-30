"""A filter whose sub-conditions cannot ALL be translated is a gap.

The translator used to keep only the ``&&`` parts it could read (and read a
condition's translatable prefix), so ``Region = "EU" && OrderDate >= DATE(...)``
shipped as ``Region = 'EU'``: an all-years total labelled as a dated KPI.
"""

import pytest

from src.services.tools.metric_view_utils.dax_translator import DaxTranslator

DATED = 'Sales[Region] = "EU" && Sales[OrderDate] >= DATE(2024,1,1)'


def _translate(dax: str, trivial_only: bool = False):
    return DaxTranslator().translate(
        {"measure_name": "m", "dax_expression": dax, "original_name": "m"},
        "Sales",
        trivial_only=trivial_only,
    )


class TestConditionIsAllOrNothing:
    def test_untranslatable_conjunct_refuses_the_condition(self):
        assert DaxTranslator()._dax_condition_to_sql(DATED, "Sales") == ""

    def test_translatable_prefix_is_not_accepted(self):
        t = DaxTranslator()
        assert (
            t._dax_condition_to_sql("Sales[Amount] > 100 * Sales[Qty]", "Sales") == ""
        )
        assert t._translate_single_condition('Sales[A] = "x" + 1', "Sales") is None

    def test_cross_table_and_qualified_are_all_or_nothing(self):
        t = DaxTranslator({"dim_alias_map": {"Calendar": "cal"}})
        cond = 'Calendar[Month] = "Jan" && YEAR(Calendar[Date]) = 2024'
        assert t._dax_condition_to_sql_qualified(cond, "Calendar", "Sales") == ""
        assert t._dax_condition_to_sql_cross_table(cond, "Calendar") == ""

    def test_fully_translatable_condition_keeps_every_part(self):
        sql = DaxTranslator()._dax_condition_to_sql(
            'Sales[Region] = "EU" && Sales[Year] >= 2024', "Sales"
        )
        assert sql == "source.Region = 'EU' AND source.Year >= 2024"

    @pytest.mark.parametrize(
        "cond",
        [
            'Sales[A] = "x" || Sales[B] = "y" && Sales[C] = "z"',
            'Sales[A] = "x" && Sales[B] = "y" || Sales[C] = "z"',
        ],
    )
    def test_unparenthesized_and_or_mix_is_refused(self, cond):
        # DAX reads A || B && C as A || (B && C); splitting on && first would
        # have produced (A OR B) AND C.
        t = DaxTranslator()
        assert t._dax_condition_to_sql(cond, "Sales") == ""
        assert t._translate_single_condition(cond, "Sales") is None
        r = _translate(f"SUMX(FILTER(Sales, {cond}), Sales[Amount])")
        assert r.sql_expr is None

    def test_parenthesized_or_inside_and_still_translates(self):
        sql = DaxTranslator()._dax_condition_to_sql(
            '(Sales[A] = "x" || Sales[B] = "y") && Sales[C] = "z"', "Sales"
        )
        assert sql == ("(source.A = 'x' OR source.B = 'y') AND source.C = 'z'")

    def test_numeric_in_list_keeps_its_values(self):
        sql = DaxTranslator()._dax_condition_to_sql(
            "Sales[Year] in {2023, 2024}", "Sales"
        )
        assert sql == "source.Year IN (2023, 2024)"


class TestMeasuresWithPartialFiltersAreGaps:
    @pytest.mark.parametrize("trivial_only", [True, False])
    def test_sumx_filter_with_dated_conjunct(self, trivial_only):
        r = _translate(f"SUMX(FILTER(Sales, {DATED}), Sales[Amount])", trivial_only)
        assert r.sql_expr is None
        assert r.is_translatable is False

    @pytest.mark.parametrize("trivial_only", [True, False])
    def test_countx_with_untranslatable_condition_is_not_unfiltered(self, trivial_only):
        r = _translate(
            "COUNTX(FILTER(Sales, YEAR(Sales[OrderDate]) = 2024), Sales[OrderId])",
            trivial_only,
        )
        assert r.sql_expr is None
        assert "Untranslatable FILTER condition" in r.skip_reason

    def test_averagex_with_dated_conjunct(self):
        r = _translate(f"AVERAGEX(FILTER(Sales, {DATED}), Sales[Amount])")
        assert r.sql_expr is None

    def test_measure_ref_with_untranslatable_calculate_filter(self):
        t = DaxTranslator(
            {"measure_resolutions": {"Revenue": {"base_expr": "SUM(source.amount)"}}}
        )
        r = t.translate(
            {
                "measure_name": "m",
                "dax_expression": "CALCULATE([Revenue], YEAR(Sales[OrderDate]) = 2024)",
                "original_name": "m",
            },
            "Sales",
        )
        assert r.sql_expr is None
        assert "Untranslatable CALCULATE filter" in r.skip_reason

    def test_variable_reference_with_extra_conjunct_is_refused(self):
        t = DaxTranslator()
        part = {
            "condition": 'Dim[Type] in allowed && Sales[Region] = "EU"',
            "filter_table": "Sales",
        }
        dax = 'var allowed = {"A","B"} return 1'
        assert t._build_filter_clause(part, "Sales", dax) == ""

    def test_fully_translatable_filter_still_translates(self):
        r = _translate(
            'SUMX(FILTER(Sales, Sales[Region] = "EU" && Sales[Year] >= 2024), Sales[Amount])'
        )
        assert r.sql_expr == (
            "SUM(source.Amount) FILTER (WHERE source.Region = 'EU' AND source.Year >= 2024)"
        )
