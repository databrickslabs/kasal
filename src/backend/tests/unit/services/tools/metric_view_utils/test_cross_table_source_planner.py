"""Cross-fact measures on a combined source at the shared grain (cross_table_source_planner)."""

import pytest

from src.services.tools.metric_view_utils.cross_table_source_planner import (
    CombinedSource,
    _unsafe_reason,
    union_sql,
)
from src.services.tools.metric_view_utils.data_classes import TableInfo
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline


def _fact(name, source_table, grain, aggs):
    return TableInfo(
        table_name=name,
        source_table=source_table,
        aggregate_columns=[{"name": n, "source_col": n} for n in aggs],
        group_by_columns=list(grain),
        calculated_columns=[],
        is_fact=True,
        full_sql="",
    )


def _facts(
    sales_grain=("CalMonth", "Region", "Channel"),
    target_grain=("CalMonth", "Region", "Product"),
):
    return {
        "FactSales": _fact("FactSales", "cat.sch.fact_sales", sales_grain, ["Amount"]),
        "FactTarget": _fact(
            "FactTarget", "cat.sch.fact_target", target_grain, ["Budget"]
        ),
    }


CROSS_RATIO = {
    "measure_name": "Attainment",
    "original_name": "Attainment",
    "dax_expression": "DIVIDE(SUM(FactSales[Amount]), SUM(FactTarget[Budget]))",
    "proposed_allocation": "FactSales",
}
SALES_TOTAL = {
    "measure_name": "Sales Total",
    "original_name": "Sales Total",
    "dax_expression": "SUM(FactSales[Amount])",
    "proposed_allocation": "FactSales",
}


def _pipeline(mapping, mquery, **config):
    cfg = {
        "read_from_source_layer": True,
        "reconciliation_active": True,
        "source_layer_materialized": False,
        **config,
    }
    return MetricViewPipeline(mapping=mapping, mquery_tables=mquery, config=cfg)


def test_union_is_null_aligned_on_measure_columns():
    cs = CombinedSource(
        facts=("FactSales", "FactTarget"),
        grain=["CalMonth", "Region"],
        grain_by_fact={
            "FactSales": ["CalMonth", "Region"],
            "FactTarget": ["CalMonth", "Region"],
        },
        out_names=["Amount", "Budget"],
        columns_by_fact={
            "FactSales": {"Amount": "Amount"},
            "FactTarget": {"Budget": "Budget"},
        },
    )
    sql = union_sql(cs, lambda f: f"`cat`.`sch`.`src_{f.lower()}`")
    assert sql.count("UNION ALL") == 1
    assert "Amount AS Amount" in sql and "NULL AS Budget" in sql
    assert "Budget AS Budget" in sql and "NULL AS Amount" in sql
    assert "FROM `cat`.`sch`.`src_factsales`" in sql
    assert "FROM `cat`.`sch`.`src_facttarget`" in sql


@pytest.mark.parametrize(
    "dax",
    [
        'CALCULATE(SUM(FactSales[Amount]), FactSales[Region] = "X") / SUM(FactTarget[Budget])',
        "COUNTROWS(FactSales) / SUM(FactTarget[Budget])",
        "SUMX(FactSales, FactSales[Amount]) / SUM(FactTarget[Budget])",
        "DIVIDE(SUM(FactSales[Amount]), SUM(FactTarget[Budget]) + COUNT(*))",
    ],
)
def test_filter_context_and_row_counts_are_not_accepted(dax):
    assert _unsafe_reason(dax) is not None


def test_plain_sums_and_ratios_are_accepted():
    assert (
        _unsafe_reason("DIVIDE(SUM(FactSales[Amount]), SUM(FactTarget[Budget]))")
        is None
    )
    assert (
        _unsafe_reason("DISTINCTCOUNT(FactSales[Amount]) / AVERAGE(FactTarget[Budget])")
        is None
    )


def test_cross_measure_becomes_combined_view_with_union_source_and_shared_grain_dims():
    p = _pipeline([CROSS_RATIO, SALES_TOTAL], _facts())
    specs = p.run()
    key = "combined_factsales_facttarget"
    assert key in specs and key in p.combined_sources
    assert p.combined_sources[key].grain == ["CalMonth", "Region"]

    yaml_by_view = p.emit_all_yaml("cat", "sch")
    combined_yaml = yaml_by_view[key]
    assert "src_combined_factsales_facttarget" in combined_yaml
    assert "name: attainment" in combined_yaml
    assert "SUM(source.Amount) / NULLIF(SUM(source.Budget), 0)" in combined_yaml
    # the shared grain is the view's dimensions: a period and a second dimension
    assert "name: calmonth" in combined_yaml and "name: region" in combined_yaml
    # the plain fact measure stays on its own fact view
    assert (
        "Sales Total" in yaml_by_view["FactSales"]
        or "sales_total" in yaml_by_view["FactSales"]
    )

    ddl = p.source_layer_views[key]["ddl"]
    assert "UNION ALL" in ddl and "NULL AS Budget" in ddl and "NULL AS Amount" in ddl
    assert "src_factsales" in ddl and "src_facttarget" in ddl
    assert p.source_layer_views[key]["error"] is None


@pytest.mark.parametrize(
    "config",
    [
        {"reconciliation_active": False},
        {"read_from_source_layer": False},
        {"cross_table_union_sources": False},
    ],
)
def test_no_combined_view_when_gated_off(config):
    p = _pipeline([CROSS_RATIO], _facts(), **config)
    p.run()
    assert p.combined_sources == {}
    assert not any(k.startswith("combined_") for k in p.all_specs)


def test_shared_grain_without_period_is_deferred_with_reason():
    facts = _facts(
        sales_grain=("Region", "Channel", "CalMonth"),
        target_grain=("Region", "Product", "Channel"),
    )
    # shared grain Region+Channel: no period column
    p = _pipeline([CROSS_RATIO], facts)
    p.run()
    assert p.combined_sources == {}
    reasons = [d["reason"] for d in p._limitations.get("cross_table_deferred", [])]
    assert any("cannot be compared" in r for r in reasons)


def test_three_fact_measure_is_deferred_not_emitted():
    facts = {
        "FactA": _fact("FactA", "c.s.a", ["CalMonth", "Region"], ["X"]),
        "FactB": _fact("FactB", "c.s.b", ["CalMonth", "Region"], ["Y"]),
        "FactC": _fact("FactC", "c.s.c", ["CalMonth", "Region"], ["Z"]),
    }
    mapping = [
        {
            "measure_name": "Three",
            "original_name": "Three",
            "dax_expression": "SUM(FactA[X]) + SUM(FactB[Y]) + SUM(FactC[Z])",
            "proposed_allocation": "FactA",
        }
    ]
    p = _pipeline(mapping, facts)
    p.run()
    assert p.combined_sources == {}
    reasons = [d["reason"] for d in p._limitations.get("cross_table_deferred", [])]
    assert any("no two-fact cover" in r for r in reasons)


def test_column_on_both_facts_outside_shared_grain_is_deferred():
    facts = {
        "FactSales": _fact(
            "FactSales", "c.s.sales", ["CalMonth", "Region"], ["Sales", "Amount"]
        ),
        "FactTarget": _fact(
            "FactTarget", "c.s.target", ["CalMonth", "Region"], ["Budget", "Amount"]
        ),
    }
    mapping = [
        {
            "measure_name": "Mixed",
            "original_name": "Mixed",
            "dax_expression": "SUM(FactSales[Sales]) / SUM(FactTarget[Budget]) + SUM(FactSales[Amount])",
            "proposed_allocation": "FactSales",
        }
    ]
    p = _pipeline(mapping, facts)
    p.run()
    assert p.combined_sources == {}
    reasons = [d["reason"] for d in p._limitations.get("cross_table_deferred", [])]
    assert any("exists on both facts" in r for r in reasons)
