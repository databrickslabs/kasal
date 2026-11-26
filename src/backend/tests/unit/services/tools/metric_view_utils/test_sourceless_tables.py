"""A measure rooted in a table the generator has no source for is reported, not translated
against another table's columns."""

from types import SimpleNamespace

from src.services.tools.metric_view_utils.sourceless_tables import (
    demote_rooted,
    live_dax,
    rooted_measures,
    sourceless_tables,
)

COMBINE = "let\n  Source = Table.Combine({QueryA, QueryB})\nin\n  Source"


def _info(raw, source_table="", is_fact=False):
    return SimpleNamespace(raw_transpiled_sql=raw, source_table=source_table, is_fact=is_fact)


def _row(name, dax):
    return {"measure_name": name, "original_name": name, "dax_expression": dax}


def test_a_table_whose_power_query_only_combines_other_queries_has_no_source():
    tables = {
        "Planning": _info(COMBINE),
        "Sales": _info("SELECT a, SUM(b) AS b FROM c.s.sales GROUP BY a", "c.s.sales", True),
    }
    assert sourceless_tables(tables) == {"Planning"}


def test_unused_variables_and_comments_are_not_live():
    dax = (
        "VAR start = [First Day]  // helper\n"
        "VAR total = SUM(Sales[amount])\n"
        "/* VAR old = SUM(Planning[value]) */\n"
        "RETURN total"
    )
    text = live_dax(dax)
    assert "Sales[amount]" in text
    assert "First Day" not in text and "Planning" not in text


def test_a_measure_that_aggregates_the_table_or_references_one_that_does_is_rooted():
    rows = [
        _row("Plan Total", "VAR s = [First Day] RETURN CALCULATE(SUMX(Planning, Planning[value]))"),
        _row("Plan A", 'CALCULATE([Plan Total], Planning[kind] = "A")'),
        _row("Plan Ratio", "DIVIDE([Plan A], [Sales Total])"),
        _row("Sales Total", "SUM(Sales[amount])"),
        _row("First Day", "MIN(Calendar[day])"),
        _row("Unused", "VAR p = [Plan Total] RETURN [Sales Total]"),
    ]
    rooted = rooted_measures(rows, {"Planning"})
    assert rooted == {"Plan Total": "Planning", "Plan A": "Planning", "Plan Ratio": "Planning"}


def test_a_filter_on_the_table_alone_does_not_root_a_measure():
    # A dimension-like use: the measure aggregates its own fact.
    rows = [_row("Filtered", 'CALCULATE(SUM(Sales[amount]), Planning[kind] = "A")')]
    assert rooted_measures(rows, {"Planning"}) == {}


def _measure(name, sql, category="single_table"):
    return SimpleNamespace(
        measure_name=name.lower().replace(" ", "_"),
        original_name=name,
        sql_expr=sql,
        is_translatable=True,
        skip_reason="",
        window_spec=None,
        category=category,
    )


def test_rooted_measures_leave_the_view_with_the_reason():
    spec = SimpleNamespace(
        measures=[
            _measure("Plan A", "SUM(source.value) FILTER (WHERE source.kind = 'A')"),
            _measure("Sales Total", "SUM(source.amount)"),
            _measure("Plan A", "SUM(source.value)", category="switch_decomposition"),
        ],
        untranslatable=[],
    )
    moved = demote_rooted(spec, {"Plan A": "Planning"})
    assert moved == ["Plan A"]
    assert [m.original_name for m in spec.measures] == ["Sales Total", "Plan A"]
    gone = spec.untranslatable[0]
    assert gone.sql_expr is None and gone.is_translatable is False
    assert "reads Planning" in gone.skip_reason
