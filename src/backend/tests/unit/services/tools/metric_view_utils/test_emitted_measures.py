"""A translated measure the YAML writer leaves out is reported as untranslated."""

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.dependency_graph import (
    build_dependency_graph,
)
from src.services.tools.metric_view_utils.emitted_measures import (
    NOT_WRITTEN,
    report_unemitted,
)
from src.services.tools.metric_view_utils.sql_measure_sanitizer import (
    detect_silent_wrong,
)


def _measure(name, sql, dax="DAX", window=None):
    return TranslationResult(
        measure_name=name,
        original_name=name,
        sql_expr=sql,
        is_translatable=True,
        skip_reason="",
        dax_expression=dax,
        confidence="high",
        category="single_table",
        window_spec=window,
    )


def _spec(measures):
    return MetricViewSpec(
        fact_table_key="Fact",
        source_table="c.s.fact",
        view_name="fact",
        comment="",
        joins=[],
        dimensions=[{"name": "country", "expr": "source.country"}],
        measures=measures,
        untranslatable=[],
    )


WRITTEN = """version: '1.1'
source: c.s.fact
dimensions:
  - name: country
    expr: source.country
measures:
  - name: sales
    expr: SUM(source.amount)
"""


def test_a_measure_missing_from_the_written_view_moves_to_the_open_list():
    window = {"order": "fiscper_key", "range": "current", "offset": -1000}
    spec = _spec(
        [
            _measure("sales", "SUM(source.amount)"),
            _measure("sales_py", "MEASURE(sales)", window=window),
            _measure("margin", "SUM(source.margin)"),
            _measure("column_sum", "SUM(source.x)", dax=""),  # not a Power BI measure
        ]
    )
    moved = report_unemitted({"Fact": spec}, lambda key, s: WRITTEN)
    assert moved == {"Fact": ["sales_py", "margin"]}
    assert [m.measure_name for m in spec.measures] == ["sales", "column_sum"]
    reasons = {m.measure_name: m.skip_reason for m in spec.untranslatable}
    assert "prior year" in reasons["sales_py"] and "fiscper_key" in reasons["sales_py"]
    assert reasons["margin"] == NOT_WRITTEN
    assert all(m.sql_expr is None for m in spec.untranslatable)


def test_a_view_that_cannot_be_read_changes_nothing():
    spec = _spec([_measure("sales", "SUM(source.amount)")])
    assert report_unemitted({"Fact": spec}, lambda key, s: "measures: [") == {}
    assert len(spec.measures) == 1


def test_the_dependency_order_follows_the_input_not_the_hash_seed():
    measures = [
        {"measure_name": name, "dax_expression": dax}
        for name, dax in (
            ("Zeta", "SUM(T[a])"),
            ("Alpha", "SUM(T[b])"),
            ("Ratio", "DIVIDE([Zeta], [Alpha])"),
            ("Mid", "SUM(T[c])"),
        )
    ]
    order = build_dependency_graph(measures)["topo_order"]
    assert order.index("Zeta") < order.index("Alpha") < order.index("Mid")
    assert order.index("Ratio") > order.index("Alpha")


def test_an_aggregate_inside_an_aggregate_is_rejected_before_deploy():
    assert detect_silent_wrong("SUM(MEASURE(share) * source.weight)")
    assert detect_silent_wrong("SUM(CASE WHEN source.k = 'a' THEN SUM(source.v) END)")
    for fine in (
        "MEASURE(share) * SUM(source.weight) / NULLIF(SUM(source.sales) * 100, 0)",
        "SUM(COALESCE(source.v, 0))",
        "COUNT(DISTINCT CASE WHEN source.k = 'a' THEN source.id END)",
        "SUM(source.v) FILTER (WHERE source.k IN ('a', 'b'))",
    ):
        assert not detect_silent_wrong(fine), fine
