"""Prior-year measures are compared in a second pass with their own time dimension: the metric
view grouped by the window's dimension, Power BI by the calendar the measure shifts on.
"""

import json

import pandas as pd
import pytest

from src.services.tools.metric_view_utils import reconciliation as recon
from src.services.tools.metric_view_utils.reconciliation import time_shift
from src.services.tools.metric_view_utils.reconciliation.mapping import (
    load_mapping_dict,
)
from src.services.tools.metric_view_utils.reconciliation.mapping_assembly import (
    assemble_mappings,
)
from src.services.tools.metric_view_utils.reconciliation.review_list import (
    build_review_list,
)
from src.services.tools.ucmv_reconciliation_tool import UCMVReconciliationTool

YAML = """version: '1.1'
source: prod.sales.fact
joins:
  - name: dim_calendar
    source: prod.md.calendar
    "on": source.fiscper = dim_calendar.fiscper
dimensions:
  - name: country
    expr: source.country
  - name: fiscper
    expr: dim_calendar.fiscper
  - name: fiscper_key
    expr: TRY_CAST(dim_calendar.fiscper AS INT)
measures:
  - name: sales
    expr: SUM(source.amount)
  - name: sales_py
    expr: MEASURE(sales)
    window:
      - order: fiscper_key
        range: current
        offset: -1000
        semiadditive: last
  - name: growth
    expr: MEASURE(sales) - MEASURE(sales_py)
  - name: latest_stock
    expr: SUM(source.stock)
    window:
      - order: fiscper
        range: current
        semiadditive: last
"""
RESOLVED = {
    "Fact": [
        {
            "measure_name": "sales",
            "original_name": "Sales",
            "dax_expression": "SUM(Fact[amount])",
        },
        {
            "measure_name": "sales_py",
            "original_name": "Sales PY",
            "dax_expression": "CALCULATE([Sales], SAMEPERIODLASTYEAR(Calendar[Date]))",
        },
        {
            "measure_name": "growth",
            "original_name": "Growth",
            "dax_expression": "[Sales] - [Sales PY]",
        },
    ]
}
JOINED = {"Calendar": {"alias": "dim_calendar", "dim_columns": ["fiscper", "date"]}}
RELATIONSHIPS = [
    {
        "from_table": "Fact",
        "from_column": "date",
        "to_table": "Calendar",
        "to_column": "Date",
    },
    {
        "from_table": "Other",
        "from_column": "fiscper",
        "to_table": "Calendar",
        "to_column": "Fiscper",
    },
]
MODEL = {"dataset_id": "ds", "workspace_id": "ws"}


def _assemble(relationships=RELATIONSHIPS, joined=JOINED):
    return assemble_mappings(
        {"Fact": YAML}, RESOLVED, {"Fact": "c.s.fact"}, joined, MODEL, relationships
    )


def test_a_measure_is_time_shifted_by_its_window_or_by_what_it_reads():
    import yaml

    names, order = time_shift.shifted_measures(yaml.safe_load(YAML))
    # a window without an offset (a latest-period measure) is not a shift
    assert names == {"sales_py", "growth"} and order == "fiscper_key"
    assert time_shift.shifted_measures(
        {"measures": [{"name": "a", "expr": "SUM(x)"}]}
    ) == (
        set(),
        "",
    )


def test_the_mapping_marks_them_and_gives_them_the_calendar_as_time_dimension():
    mappings, gaps = _assemble()
    mapping = mappings["Fact"]
    assert not gaps
    # the other measures are compared as before
    assert mapping["time_dimension"]["ucmv"] == {"mode": "column", "name": "fiscper"}
    assert mapping["shifted_time_dimension"] == {
        "grain": "month",
        "ucmv": {"mode": "column", "name": "fiscper_key"},
        "pbi_table": "Calendar",
        "pbi_column": "Fiscper",  # as the relationship spells it
    }
    marked = {m["ucmv_measure"] for m in mapping["measures"] if m.get("time_shifted")}
    assert marked == {"sales_py", "growth"}
    plain, shifted = recon.split_time_shifted(load_mapping_dict(mapping))
    assert [m.ucmv_measure for m in plain.measures] == ["sales", "latest_stock"]
    assert [m.ucmv_measure for m in shifted.measures] == ["sales_py", "growth"]
    sql = recon.build_ucmv_query(shifted, "country", [], [])
    assert (
        "`fiscper_key` AS period" in sql and "GROUP BY `country`, `fiscper_key`" in sql
    )
    dax = recon.build_direct_queries(shifted, "country", [], shifted.measures)[0]
    assert "'Calendar'[Fiscper]" in dax and "[Sales PY]" in dax
    assert "`fiscper` AS period" in recon.build_ucmv_query(plain, "country", [], [])


def test_without_a_known_period_column_on_the_calendar_they_are_left_out_with_the_reason():
    mappings, _ = _assemble(
        relationships=RELATIONSHIPS[:1], joined={"Calendar": {"alias": "dim_calendar"}}
    )
    mapping = mappings["Fact"]
    assert "shifted_time_dimension" not in mapping
    assert [m["ucmv_measure"] for m in mapping["measures"]] == ["sales", "latest_stock"]
    assert set(mapping["not_compared"]) == {"sales_py", "growth"}
    load_mapping_dict(mapping)  # still loads
    rows = build_review_list(
        {
            "yaml": {"Fact": YAML},
            "pbi_ucmv_mapping": mappings,
            "reconciliation": {
                "views": {"Fact": {"measures": {"sales": {"pct_aligned": 100.0}}}}
            },
        }
    )
    reasons = {r["measure"]: r.get("reason") for r in rows}
    assert "calendar" in reasons["sales_py"] and "calendar" in reasons["growth"]


def test_a_calendar_that_does_not_filter_the_fact_leaves_them_out():
    inactive = [{**RELATIONSHIPS[0], "is_active": False}, RELATIONSHIPS[1]]
    mapping = _assemble(relationships=inactive)[0]["Fact"]
    assert (
        "shifted_time_dimension" not in mapping
        and "sales_py" in mapping["not_compared"]
    )


def test_a_marked_measure_needs_the_second_time_dimension():
    mapping = dict(_assemble()[0]["Fact"])
    del mapping["shifted_time_dimension"]
    with pytest.raises(ValueError, match="shifted_time_dimension"):
        load_mapping_dict(mapping)


def test_frames_merge_on_dimension_and_period():
    first = pd.DataFrame([{"country": "AT", "period": "2026001", "sales": 1.0}])
    second = pd.DataFrame([{"country": "AT", "period": "2026001", "sales_py": 2.0}])
    merged = time_shift.merge_frames(first, second, "country")
    assert merged.to_dict("records") == [
        {"country": "AT", "period": "2026001", "sales": 1.0, "sales_py": 2.0}
    ]
    assert time_shift.merge_frames(pd.DataFrame(), second, "country") is second
    assert time_shift.merge_frames(first, pd.DataFrame(), "country") is first


class _SQL:
    """The metric view: the plain query groups by fiscper, the second pass by its key."""

    def __init__(self, fail_second=False):
        self.fail_second = fail_second
        self.queries = []

    def execute(self, sql):
        self.queries.append(sql)
        if "`fiscper_key` AS period" in sql:
            if self.fail_second:
                return None, "window rejected"
            return (
                pd.DataFrame(
                    [
                        {
                            "country": "AT",
                            "period": "2026001",
                            "sales_py": 5.0,
                            "growth": 5.0,
                        }
                    ]
                ),
                None,
            )
        return (
            pd.DataFrame([{"country": "AT", "period": "2026001", "sales": 10.0}]),
            None,
        )


class _PBI:
    def __init__(self):
        self.queries = []

    def execute(self, model_id, dax):
        self.queries.append(dax)
        if "[Sales PY]" in dax:
            return (
                pd.DataFrame(
                    [
                        {
                            "Fact[country]": "AT",
                            "Calendar[Fiscper]": "2026001",
                            "sales_py": 5.0,
                            "growth": 4.0,
                        }
                    ]
                ),
                None,
            )
        return (
            pd.DataFrame(
                [{"Fact[country]": "AT", "Calendar[fiscper]": "2026001", "sales": 10.0}]
            ),
            None,
        )


def _run(monkeypatch, sql):
    tool = UCMVReconciliationTool()
    pbi = _PBI()
    monkeypatch.setattr(tool, "_build_sql_executor", lambda *a, **k: sql)
    monkeypatch.setattr(tool, "_build_pbi_executor", lambda *a, **k: pbi)
    out = json.loads(
        tool._run(
            mappings_json=json.dumps(_assemble()[0]),
            warehouse_id="wh",
            reference_years=["2026"],
            dimension_values=["AT"],
        )
    )
    return out["views"]["Fact"], pbi


def test_the_second_pass_is_merged_into_the_view_report(monkeypatch):
    sql = _SQL()
    view, pbi = _run(monkeypatch, sql)
    assert view["measures"]["sales"]["pct_aligned"] == 100.0
    assert view["measures"]["sales_py"]["pct_aligned"] == 100.0
    assert view["measures"]["growth"]["pct_aligned"] == 0.0
    assert len(sql.queries) == 2 and len(pbi.queries) == 2
    # the second pass has no year filter: last year's rows must be in the query
    second = next(q for q in sql.queries if "`fiscper_key` AS period" in q)
    assert "WHERE" not in second.replace("WHERE `country`", "")
    assert "not_compared" not in view


def test_a_failing_second_pass_keeps_the_first(monkeypatch):
    view, _ = _run(monkeypatch, _SQL(fail_second=True))
    assert view["measures"]["sales"]["pct_aligned"] == 100.0
    assert set(view["measures"]) == {"sales", "latest_stock"}
    assert set(view["not_compared"]) == {"sales_py", "growth"}
    assert "prior-year comparison failed" in view["not_compared"]["sales_py"]


# ── periods Power BI returns nothing for ───────────────────────────────────


def test_periods_power_bi_lacks_are_the_view_periods_it_returns_no_row_for():
    lacks = recon.periods_power_bi_lacks(
        ["2015001", "2015002", "2026001"], ["2026001", "2026002", "nan"]
    )
    assert lacks == ["2015001", "2015002"]
    # no period in common is a format that does not match: nothing is left out
    assert recon.periods_power_bi_lacks(["2026001"], ["202601"]) == []
    assert recon.periods_power_bi_lacks(["2026001"], []) == []


class _HistorySQL:
    def execute(self, sql):
        rows = [
            {"country": "AT", "period": p, "sales": 10.0}
            for p in ("2015001", "2015002", "2025001")
        ]
        return pd.DataFrame(rows), None


class _RecentPBI:
    def execute(self, model_id, dax):
        return (
            pd.DataFrame(
                [{"Fact[country]": "AT", "Calendar[fiscper]": "2025001", "sales": 10.0}]
            ),
            None,
        )


def test_history_power_bi_does_not_show_is_left_out_and_counted(monkeypatch):
    mapping = {
        k: v for k, v in _assemble()[0]["Fact"].items() if k != "shifted_time_dimension"
    }
    mapping["measures"] = [
        m for m in mapping["measures"] if m["ucmv_measure"] == "sales"
    ]
    tool = UCMVReconciliationTool()
    monkeypatch.setattr(tool, "_build_sql_executor", lambda *a, **k: _HistorySQL())
    monkeypatch.setattr(tool, "_build_pbi_executor", lambda *a, **k: _RecentPBI())
    out = json.loads(
        tool._run(
            mappings_json=json.dumps({"Fact": mapping}),
            warehouse_id="wh",
            dimension_values=["AT"],
        )
    )
    view = out["views"]["Fact"]
    assert view["measures"]["sales"]["pct_aligned"] == 100.0
    assert view["measures"]["sales"]["cells_total"] == 1
    assert view["periods_not_in_power_bi"] == {
        "periods": 2,
        "first": "2015001",
        "last": "2015002",
    }
    rows = build_review_list({"yaml": {"Fact": YAML}, "reconciliation": out})
    from src.services.tools.metric_view_utils.reconciliation.review_list import (
        review_summary,
    )

    summary = review_summary({"yaml": {"Fact": YAML}, "reconciliation": out}, rows)
    assert summary["Fact"]["periods_not_in_power_bi"]["periods"] == 2


# ── one option of a Power BI SWITCH measure ────────────────────────────────


def test_a_switch_option_is_compared_with_its_selector_or_not_at_all():
    view = """version: '1.1'
source: prod.sales.fact
dimensions:
  - name: country
    expr: source.country
  - name: fiscper
    expr: source.fiscper
measures:
  - name: kpi_volume
    expr: SUM(source.volume)
  - name: kpi_margin
    expr: SUM(source.margin)
  - name: kpi_other
    expr: SUM(source.other)
  - name: sales
    expr: SUM(source.amount) FILTER (WHERE source.kind = 'a')
"""
    resolved = {
        "Fact": [
            {
                "measure_name": "kpi_volume",
                "original_name": "KPI",
                "dax_expression": "",
                "pbi_kind": "switch",
                "pbi_sources": [
                    {
                        "pbi_measure": "KPI",
                        "switch_selector_table": "Selector",
                        "switch_selector_column": "Name",
                        "switch_selector_value": "Volume",
                    }
                ],
            },
            {
                "measure_name": "kpi_margin",
                "original_name": "KPI",
                "dax_expression": "SWITCH(...)",
                "pbi_kind": "unresolved",
                "pbi_sources": [],
            },
            {
                "measure_name": "kpi_other",
                "original_name": "KPI by plant",
                "dax_expression": "SWITCH(...)",
                "pbi_kind": "dimension_conditional",
            },
            {
                "measure_name": "sales",
                "original_name": "Sales",
                "dax_expression": "CALCULATE(...)",
            },
        ]
    }
    mappings, gaps = assemble_mappings(
        {"Fact": view}, resolved, {"Fact": "c.s.fact"}, {}, MODEL
    )
    entries = {m["ucmv_measure"]: m for m in mappings["Fact"]["measures"]}
    assert entries["kpi_volume"]["pbi_kind"] == "switch"
    assert entries["kpi_volume"]["pbi_measure"] == "KPI"
    assert entries["kpi_volume"]["switch_selector_value"] == "Volume"
    assert entries["kpi_margin"]["pbi_kind"] == "unresolved"
    assert entries["kpi_other"]["pbi_kind"] == "unresolved"
    assert entries["sales"]["pbi_kind"] == "direct"
    loaded = load_mapping_dict(mappings["Fact"])
    assert [m.ucmv_measure for m in loaded.resolved_measures()] == [
        "kpi_volume",
        "sales",
    ]
