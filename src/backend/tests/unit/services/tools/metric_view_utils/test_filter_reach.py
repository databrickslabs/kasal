"""Predicates on a table that does not filter the fact are left out of the translation."""

from src.services.tools.metric_view_utils.data_classes import TranslationResult
from src.services.tools.metric_view_utils.dax_translator import DaxTranslator
from src.services.tools.metric_view_utils.filter_reach import (
    drop_non_filtering_filters,
    filter_reach,
    model_mentions,
)
from src.services.tools.metric_view_utils.yaml_annotations import (
    _dropped_filter_suffix,
)

# from = the many side (the fact), to = the one side (the dimension), as in the model export.
RELS = [
    {"from_table": "Fact", "to_table": "Dim_Region", "is_active": True},
    {"from_table": "Fact", "to_table": "Dim_Calendar", "is_active": True},
    {"from_table": "Dim_Region", "to_table": "Dim_Country", "is_active": True},
    {"from_table": "Fact", "to_table": "Dim_Selector", "is_active": False},
    # a table that only relates to another dimension: never reaches the fact
    {"from_table": "Dim_Geo", "to_table": "Dim_Plant", "is_active": True},
]
REACH = filter_reach("Fact", RELS)


def test_filter_reach_follows_active_relationships_to_the_one_side():
    assert REACH == {"Fact", "Dim_Region", "Dim_Calendar", "Dim_Country"}
    assert "Dim_Geo" not in REACH  # no path from the fact
    assert "Dim_Selector" not in REACH  # inactive relationship


def test_a_table_no_relationship_names_is_not_known_to_the_model():
    assert model_mentions("Dim_Geo", RELS)
    assert not model_mentions("Dim_Unknown", RELS)


def test_predicate_on_an_unreachable_table_is_left_out_and_named():
    dax = (
        "var ret= DIVIDE(CALCULATE(SUMX(Fact,Fact[a]),FILTER(Dim_Geo,Dim_Geo[code]<>0307)), "
        "CALCULATE(SUMX(Fact,Fact[b]),FILTER(Dim_Geo,Dim_Geo[code]<>0307))) return ret"
    )
    out, dropped = drop_non_filtering_filters(dax, "Fact", REACH)
    assert dropped == ["Dim_Geo"]
    assert "Dim_Geo" not in out
    assert out == (
        "var ret= DIVIDE(CALCULATE(SUMX(Fact,Fact[a])), CALCULATE(SUMX(Fact,Fact[b]))) return ret"
    )


def test_predicate_on_a_reachable_table_is_kept():
    dax = 'CALCULATE(SUMX(Fact,Fact[a]),FILTER(Dim_Region,Dim_Region[code]="x"))'
    assert drop_non_filtering_filters(dax, "Fact", REACH) == (dax, [])


def test_boolean_predicate_on_an_unreachable_table_is_dropped():
    dax = 'CALCULATE(SUM(Fact[a]), Dim_Geo[code] = "x")'
    out, dropped = drop_non_filtering_filters(dax, "Fact", REACH)
    assert dropped == ["Dim_Geo"]
    assert out == "CALCULATE(SUM(Fact[a]))"


def test_a_predicate_next_to_a_measure_reference_is_kept():
    # the referenced measure may read the filtered table itself (the filter then applies)
    dax = 'CALCULATE([Total Value], Dim_Geo[code] = "x", Dim_Geo[other] = "y")'
    assert drop_non_filtering_filters(dax, "Fact", REACH) == (dax, [])


def test_virtual_relationship_or_inactive_switch_keeps_the_predicate():
    for construct in (
        "TREATAS(x, Dim_Geo[code])",
        "USERELATIONSHIP(Fact[k], Dim_Geo[k])",
    ):
        dax = f"CALCULATE(SUM(Fact[a]), FILTER(Dim_Geo, Dim_Geo[code]<>1), {construct})"
        assert drop_non_filtering_filters(dax, "Fact", REACH) == (dax, [])


def test_unreachable_table_named_elsewhere_keeps_the_measure_as_written():
    # the measure's own data reads the other table: the fact is not all it uses
    dax = "CALCULATE(SUM(Dim_Geo[v]), FILTER(Dim_Geo, Dim_Geo[code]<>1))"
    assert drop_non_filtering_filters(dax, "Fact", REACH) == (dax, [])


def test_translator_leaves_the_predicate_out_and_names_it_only_with_relationships():
    measure = {
        "measure_name": "ratio",
        "original_name": "Ratio",
        "dax_expression": (
            "var ret= DIVIDE(CALCULATE(SUMX(Fact,Fact[a]),FILTER(Dim_Geo,Dim_Geo[code]<>1)), "
            "CALCULATE(SUMX(Fact,Fact[b]),FILTER(Dim_Geo,Dim_Geo[code]<>1))) return ret"
        ),
    }
    translator = DaxTranslator(config={})
    unchanged = translator.translate(dict(measure), "Fact")
    assert unchanged.dropped_filter_tables == ()
    assert "Dim_Geo" in unchanged.dax_expression

    translator.set_model_relationships(RELS)
    result = translator.translate(dict(measure), "Fact")
    assert result.dropped_filter_tables == ("Dim_Geo",)
    assert "Dim_Geo" not in result.dax_expression
    # a ratio whose denominator the rule path cannot keep is not shipped as a bare numerator
    assert not (result.is_translatable and result.sql_expr == "SUM(source.a)")


def test_comment_names_the_left_out_table():
    measure = TranslationResult(
        measure_name="ratio",
        original_name="Ratio",
        sql_expr=None,
        is_translatable=False,
        skip_reason="",
        dax_expression="",
        confidence="none",
        category="unassigned",
        dropped_filter_tables=("Dim_Geo",),
    )
    assert "predicate on Dim_Geo left out" in _dropped_filter_suffix(measure)
    plain = TranslationResult(
        measure_name="x",
        original_name="x",
        sql_expr=None,
        is_translatable=False,
        skip_reason="",
        dax_expression="",
        confidence="none",
        category="unassigned",
    )
    assert _dropped_filter_suffix(plain) == ""
