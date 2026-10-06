"""Unit tests for the deterministic reconciliation core.

Covers: mapping loading + operand/column normalisation, exact UCMV SQL and PBI
DAX per pbi_kind, TREATAS filter operators, value parsing, comparison tolerance
(absolute + relative + zero-exact), summary roll-up, classification, and the
executor-driven runner with a fake executor (no network).
"""

import pandas as pd
import pytest

from src.services.tools.metric_view_utils import reconciliation as r

# ── mapping loading + normalisation ──────────────────────────────────────────

BASE_MAPPING = {
    "ucmv_table": "cat.sch.fact_sc",
    "pbi_semantic_model_id": "model-123",
    "pbi_fact_table": "Fact_SC",
    "pbi_workspace_id": "ws-1",
    "time_dimension": {
        "grain": "month",
        "ucmv": {"mode": "column", "name": "fiscper"},
        "pbi_column": "fiscper",
        "pbi_table": "C_Dim_calendar",
    },
    "default_dimension": "country",
    "dimensions": {
        "country": {
            "ucmv_column": "country_group",
            "pbi_dim_table": "C_Dim_Geography",
            "pbi_dim_column": "country",
        }
    },
    "measures": [
        {
            "ucmv_measure": "sweetener_yield_bp",
            "pbi_kind": "direct",
            "pbi_measure": "Sweetner_Yeild_BP",
        },
        {
            "ucmv_measure": "actual_value",
            "pbi_kind": "raw_column",
            "pbi_column": "Fact_SC[value]",
            "extra_filter": [
                {"table": "Fact_SC", "column": "bic_chversion", "values": ["0000"]}
            ],
        },
    ],
}


def _mapping(overrides=None):
    data = dict(BASE_MAPPING)
    if overrides:
        data = {**data, **overrides}
    return r.load_mapping_dict(data)


def test_load_and_resolved_measures():
    m = _mapping()
    assert [x.ucmv_measure for x in m.measures] == [
        "sweetener_yield_bp",
        "actual_value",
    ]
    assert len(m.resolved_measures()) == 2


def test_unresolved_excluded_from_resolved():
    data = dict(BASE_MAPPING)
    data["measures"] = BASE_MAPPING["measures"] + [
        {"ucmv_measure": "mystery", "pbi_kind": "unresolved", "raw_hint": "TODO"}
    ]
    m = r.load_mapping_dict(data)
    assert len(m.measures) == 3
    assert "mystery" not in [x.ucmv_measure for x in m.resolved_measures()]


def test_raw_column_pbi_table_plus_bare_column_normalised():
    """Kasal's emitter emits pbi_table + bare pbi_column; both conventions must
    normalise to the combined 'Table[Column]' form."""
    data = dict(BASE_MAPPING)
    data["measures"] = [
        {
            "ucmv_measure": "v",
            "pbi_kind": "raw_column",
            "pbi_table": "Fact_SC",
            "pbi_column": "value",
        }
    ]
    m = r.load_mapping_dict(data)
    assert m.measure("v").pbi_column == "Fact_SC[value]"
    assert r.measure_dax_expr(m.measure("v")) == "SUM('Fact_SC'[value])"


def test_default_dimension_must_exist():
    data = dict(BASE_MAPPING)
    data["default_dimension"] = "nope"
    with pytest.raises(ValueError):
        r.load_mapping_dict(data)


def test_measure_validation_direct_requires_measure():
    data = dict(BASE_MAPPING)
    data["measures"] = [{"ucmv_measure": "x", "pbi_kind": "direct"}]
    with pytest.raises(ValueError):
        r.load_mapping_dict(data)


def test_load_mapping_from_yaml_text():
    import yaml

    m = r.load_mapping(yaml.safe_dump(BASE_MAPPING))
    assert m.binding.pbi_fact_table == "Fact_SC"


# ── UCMV SQL ──────────────────────────────────────────────────────────────────


def test_build_ucmv_query_exact():
    m = _mapping()
    sql = r.build_ucmv_query(m, "country", ["AT", "NG"], [2025, 2026])
    assert "`country_group` AS country" in sql
    assert "`fiscper` AS period" in sql
    assert "MEASURE(`sweetener_yield_bp`) AS `sweetener_yield_bp`" in sql
    assert "WHERE `fiscal_year` IN ('2025', '2026')" in sql
    assert "AND `country_group` IN ('AT', 'NG')" in sql
    assert sql.startswith("SELECT")
    assert "FROM `cat`.sch.fact_sc" in sql
    assert "GROUP BY `country_group`, `fiscper`" in sql


def test_build_ucmv_query_reconstruct_mode():
    data = dict(BASE_MAPPING)
    data["time_dimension"] = {
        "grain": "month",
        "ucmv": {"mode": "reconstruct", "from": ["fiscal_year", "fiscal_month"]},
        "pbi_column": "fiscper",
    }
    m = r.load_mapping_dict(data)
    sql = r.build_ucmv_query(m, "country", [], [2025])
    assert "CONCAT(CAST(`fiscal_year` AS STRING), `fiscal_month`) AS period" in sql


# ── PBI DAX per pbi_kind ──────────────────────────────────────────────────────


def test_direct_query_exact():
    m = _mapping()
    direct = [
        x for x in m.resolved_measures() if x.ucmv_measure == "sweetener_yield_bp"
    ]
    dax = r.build_direct_query(m, "country", ["AT"], direct)
    assert dax == (
        "EVALUATE\nSUMMARIZECOLUMNS(\n"
        "    'C_Dim_Geography'[country],\n"
        "    'C_Dim_calendar'[fiscper],\n"
        "    TREATAS({\"AT\"}, 'C_Dim_Geography'[country]),\n"
        '    "sweetener_yield_bp", [Sweetner_Yeild_BP]\n'
        ")"
    )


def test_direct_query_none_when_empty():
    m = _mapping()
    assert r.build_direct_query(m, "country", ["AT"], []) is None


def test_raw_column_expr_and_extra_filter():
    m = _mapping()
    av = m.measure("actual_value")
    assert r.measure_dax_expr(av) == "SUM('Fact_SC'[value])"
    dax = r.build_direct_query(m, "country", ["AT"], [av])
    # The measure-level extra_filter becomes a TREATAS in the query body.
    assert "TREATAS({\"0000\"}, 'Fact_SC'[bic_chversion])" in dax


def test_raw_column_aggregation_average():
    data = dict(BASE_MAPPING)
    data["measures"] = [
        {
            "ucmv_measure": "a",
            "pbi_kind": "raw_column",
            "pbi_column": "T[c]",
            "pbi_aggregation": "Average",
        }
    ]
    m = r.load_mapping_dict(data)
    assert r.measure_dax_expr(m.measure("a")) == "AVERAGE('T'[c])"


def test_composite_expr():
    data = dict(BASE_MAPPING)
    data["measures"] = [
        {
            "ucmv_measure": "avb",
            "pbi_kind": "composite",
            "composite_operator": "subtract",
            "composite_a": {
                "pbi_column": "Fact_SC[value]",
                "extra_filter": [
                    {"table": "Fact_SC", "column": "bic_chversion", "values": ["0000"]}
                ],
            },
            "composite_b": {
                "pbi_column": "Fact_SC[value]",
                "extra_filter": [
                    {"table": "Fact_SC", "column": "bic_chversion", "values": ["B000"]}
                ],
            },
        }
    ]
    m = r.load_mapping_dict(data)
    assert r.measure_dax_expr(m.measure("avb")) == (
        "(CALCULATE(SUM('Fact_SC'[value]), TREATAS({\"0000\"}, 'Fact_SC'[bic_chversion])) - "
        "CALCULATE(SUM('Fact_SC'[value]), TREATAS({\"B000\"}, 'Fact_SC'[bic_chversion])))"
    )


def test_switch_query_shape():
    data = dict(BASE_MAPPING)
    data["measures"] = [
        {
            "ucmv_measure": "epl",
            "pbi_kind": "switch",
            "pbi_measure": "Prod_Exec_Line_Item_Actual",
            "switch_selector_table": "Mapping_Line_Item",
            "switch_selector_column": "KBI_Display_calculate",
            "switch_selector_value": "EPL",
        },
        {
            "ucmv_measure": "opl",
            "pbi_kind": "switch",
            "pbi_measure": "Prod_Exec_Line_Item_Actual",
            "switch_selector_table": "Mapping_Line_Item",
            "switch_selector_column": "KBI_Display_calculate",
            "switch_selector_value": "OPL",
        },
    ]
    m = r.load_mapping_dict(data)
    queries = r.build_switch_queries(m, "country", ["AT"], m.resolved_measures())
    # EPL and OPL share one selector triple -> a single query grouped by the selector.
    assert len(queries) == 1
    dax, group, sel_col, sel2 = queries[0]
    assert sel_col == "KBI_Display_calculate"
    assert sel2 is None
    assert "'Mapping_Line_Item'[KBI_Display_calculate]" in dax
    assert (
        'TREATAS({"EPL", "OPL"}, \'Mapping_Line_Item\'[KBI_Display_calculate])' in dax
    )
    assert '"value", [Prod_Exec_Line_Item_Actual]' in dax


def test_compound_switch_second_selector():
    data = dict(BASE_MAPPING)
    data["measures"] = [
        {
            "ucmv_measure": "x",
            "pbi_kind": "switch",
            "pbi_measure": "CF_Line_Item_Actual",
            "switch_selector_table": "Sel1",
            "switch_selector_column": "A",
            "switch_selector_value": "Unit Case",
            "switch_selector2_table": "Sel2",
            "switch_selector2_column": "B",
            "switch_selector2_value": "Cost",
        }
    ]
    m = r.load_mapping_dict(data)
    dax, group, sel_col, sel2 = r.build_switch_queries(
        m, "country", [], m.resolved_measures()
    )[0]
    assert sel2 == "B"
    assert "'Sel2'[B]" in dax
    assert "TREATAS({\"Cost\"}, 'Sel2'[B])" in dax


# ── filter operators ──────────────────────────────────────────────────────────


def test_filter_in_treatas():
    f = {"table": "T", "column": "c", "values": ["a", "b"]}
    assert r.filter_expression(f) == 'TREATAS({"a", "b"}, \'T\'[c])'


def test_filter_not_in():
    f = {"table": "T", "column": "c", "values": ["x"], "operator": "not_in"}
    assert r.filter_expression(f) == "FILTER(ALL('T'[c]), NOT('T'[c] IN {\"x\"}))"


def test_filter_prefix():
    f = {"table": "T", "column": "c", "values": ["9"], "operator": "prefix"}
    assert r.filter_expression(f) == "FILTER(ALL('T'[c]), LEFT('T'[c], 1) = \"9\")"


def test_filter_numeric_values_unquoted():
    f = {"table": "T", "column": "c", "values": [0, 1]}
    assert r.filter_expression(f) == "TREATAS({0, 1}, 'T'[c])"


def test_filter_empty_values_raises():
    with pytest.raises(ValueError):
        r.filter_expression({"table": "T", "column": "c", "values": []})


# ── value parsing ─────────────────────────────────────────────────────────────


def test_parse_raw_number():
    out = r.parse_value(pd.Series(["1.5", "2", None]), "raw_number")
    assert out.tolist()[:2] == [1.5, 2.0]
    assert pd.isna(out.tolist()[2])


def test_parse_percent_string():
    out = r.parse_value(pd.Series(["5.29%", "1,605.9 %"]), "percent_string")
    assert out.tolist() == [pytest.approx(0.0529), pytest.approx(16.059)]


def test_parse_percent_string_unscaled():
    out = r.parse_value(pd.Series(["0.06%"]), "percent_string_unscaled")
    assert out.tolist() == [pytest.approx(0.06)]


def test_parse_strict_raises_on_garbage():
    with pytest.raises(Exception):
        r.parse_value(pd.Series(["2.11 K"]), "raw_number", strict=True)


def test_parse_coerce_does_not_raise():
    out = r.parse_value(pd.Series(["2.11 K"]), "raw_number", strict=False)
    assert pd.isna(out.tolist()[0])


# ── comparison + tolerance ────────────────────────────────────────────────────


def _cmp_mapping(measure):
    data = dict(BASE_MAPPING)
    data["measures"] = [measure]
    return r.load_mapping_dict(data)


def test_compare_absolute_tolerance_pass_and_fail():
    m = _cmp_mapping(
        {
            "ucmv_measure": "v",
            "pbi_kind": "direct",
            "pbi_measure": "V",
            "tolerance": {"kind": "absolute", "value": 0.001},
        }
    )
    ucmv = pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "v": 10.0005},
            {"country": "NG", "period": "2025001", "v": 10.5},
        ]
    )
    pbi = pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "v": 10.0},
            {"country": "NG", "period": "2025001", "v": 10.0},
        ]
    )
    wide = r.compare(ucmv, pbi, m, "country", [2025])
    long_df = r.to_long_format(wide, m, "country")
    tol = dict(zip(long_df["dimension"], long_df["within_tolerance"]))
    assert tol["AT"] is True or bool(tol["AT"]) is True
    assert bool(tol["NG"]) is False


def test_compare_relative_tolerance_and_zero_exact():
    m = _cmp_mapping(
        {
            "ucmv_measure": "v",
            "pbi_kind": "raw_column",
            "pbi_column": "T[c]",
            "tolerance": {"kind": "relative", "value": 1e-9},
        }
    )
    ucmv = pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "v": 1e15},
            {
                "country": "ZZ",
                "period": "2025001",
                "v": 0.0,
            },  # both zero -> exact match
        ]
    )
    pbi = pd.DataFrame(
        [
            {
                "country": "AT",
                "period": "2025001",
                "v": 1e15 + 1,
            },  # 1/1e15 < 1e-9 -> aligned
            {"country": "ZZ", "period": "2025001", "v": 0.0},
        ]
    )
    wide = r.compare(ucmv, pbi, m, "country", [2025])
    long_df = r.to_long_format(wide, m, "country")
    tol = {
        row["dimension"]: bool(row["within_tolerance"]) for _, row in long_df.iterrows()
    }
    assert tol["AT"] is True
    assert tol["ZZ"] is True


def test_compare_excludes_open_period():
    """The current, still-open period is excluded from comparison."""
    from src.services.tools.metric_view_utils.reconciliation.periods import (
        current_period,
    )

    m = _cmp_mapping({"ucmv_measure": "v", "pbi_kind": "direct", "pbi_measure": "V"})
    open_p = current_period()
    year = int(open_p[:4])
    ucmv = pd.DataFrame([{"country": "AT", "period": open_p, "v": 1.0}])
    pbi = pd.DataFrame([{"country": "AT", "period": open_p, "v": 999.0}])
    wide = r.compare(ucmv, pbi, m, "country", [year])
    assert wide.empty


def test_summarize_axis_breakdown():
    m = _cmp_mapping({"ucmv_measure": "v", "pbi_kind": "direct", "pbi_measure": "V"})
    ucmv = pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "v": 1.0},
            {"country": "AT", "period": "2025002", "v": 5.0},
        ]
    )
    pbi = pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "v": 1.0},
            {"country": "AT", "period": "2025002", "v": 9.0},
        ]
    )
    wide = r.compare(ucmv, pbi, m, "country", [2025])
    summ = r.summarize(r.to_long_format(wide, m, "country"))
    row = summ.iloc[0]
    assert row["total"] == 2
    assert row["aligned"] == 1
    assert row["pct_aligned"] == 50.0
    assert row["n_periods"] == 2
    assert row["periods_failing"] == 1


# ── classification ────────────────────────────────────────────────────────────


def test_classify_aligned():
    assert r.classify(r.MismatchStats(100, 4, 0, 10, 0, [])) == "aligned"


def test_classify_known_defect():
    st = r.MismatchStats(
        70, 4, 1, 10, 1, ["2025001"], value_format="percent_string_unscaled"
    )
    assert r.classify(st) == "known_defect"


def test_classify_drift_trailing_edge():
    st = r.MismatchStats(90, 8, 2, 10, 10, ["2026008", "2026009"])
    assert r.classify(st, pbi_refresh_period="2026007") == "drift"


def test_classify_structural_both_axes_broad():
    st = r.MismatchStats(30, 4, 4, 10, 9, ["2025001", "2025002", "2025003", "2025004"])
    assert r.classify(st) == "structural"


def test_classify_real_error_default():
    st = r.MismatchStats(95, 8, 1, 10, 1, ["2025003"])
    assert r.classify(st) == "real_error"


# ── runner with a fake executor ───────────────────────────────────────────────


class _FakeExecutor:
    """Duck-typed PBI executor: routes on query content."""

    def __init__(
        self, direct_rows=None, switch_rows=None, direct_empty=False, switch_empty=False
    ):
        self.direct_rows = direct_rows or []
        self.switch_rows = switch_rows or []
        self.direct_empty = direct_empty
        self.switch_empty = switch_empty
        self.calls = []

    def execute(self, model_id, dax):
        self.calls.append(dax)
        if '"value", [' in dax:  # a switch query
            if self.switch_empty:
                return pd.DataFrame(), "no rows"
            return pd.DataFrame(self.switch_rows), None
        if self.direct_empty:
            return pd.DataFrame(), "boom"
        return pd.DataFrame(self.direct_rows), None


def _switch_mapping():
    data = dict(BASE_MAPPING)
    data["measures"] = [
        {"ucmv_measure": "m_direct", "pbi_kind": "direct", "pbi_measure": "M_Direct"},
        {
            "ucmv_measure": "m_switch",
            "pbi_kind": "switch",
            "pbi_measure": "Sel",
            "switch_selector_table": "Map",
            "switch_selector_column": "K",
            "switch_selector_value": "EPL",
        },
    ]
    return r.load_mapping_dict(data)


def test_run_pbi_queries_merges_direct_and_switch():
    m = _switch_mapping()
    ex = _FakeExecutor(
        direct_rows=[{"Geo[country]": "AT", "fiscper": "2025001", "m_direct": 10.0}],
        switch_rows=[
            {
                "Geo[country]": "AT",
                "fiscper": "2025001",
                "Map[K]": "EPL",
                "[value]": 3.0,
            }
        ],
    )
    out = r.run_pbi_queries(
        ex, "mid", m, "country", ["AT"], context_periods=["2025001"]
    )
    row = out.iloc[0]
    assert row["country"] == "AT"
    assert row["m_direct"] == 10.0
    assert row["m_switch"] == 3.0


def test_run_pbi_queries_empty_switch_not_fatal():
    m = _switch_mapping()
    ex = _FakeExecutor(
        direct_rows=[{"Geo[country]": "AT", "fiscper": "2025001", "m_direct": 10.0}],
        switch_empty=True,
    )
    out = r.run_pbi_queries(
        ex, "mid", m, "country", ["AT"], context_periods=["2025001"]
    )
    assert "m_switch" in out.columns
    assert pd.isna(out.iloc[0]["m_switch"])


def test_run_pbi_queries_empty_direct_is_fatal():
    m = _switch_mapping()
    ex = _FakeExecutor(direct_empty=True)
    with pytest.raises(RuntimeError):
        r.run_pbi_queries(ex, "mid", m, "country", ["AT"], context_periods=["2025001"])


def test_dimension_conditional_usage_guard():
    data = dict(BASE_MAPPING)
    data["dimensions"]["plant"] = {
        "ucmv_column": "plant",
        "pbi_dim_table": "Dim_Plant",
        "pbi_dim_column": "plant",
    }
    data["measures"] = [
        {
            "ucmv_measure": "plant_kbi",
            "pbi_kind": "dimension_conditional",
            "pbi_measure": "Plant_Comp KBI",
            "context_dimension": "plant",
            "context_included": True,
        }
    ]
    m = r.load_mapping_dict(data)
    # Querying by 'country' when the measure requires its context_dimension 'plant'.
    with pytest.raises(ValueError):
        r.validate_dimension_conditional_usage(m.resolved_measures(), "country")


def test_run_ucmv_query_empty_raises():
    class E:
        def execute(self, sql):
            return pd.DataFrame(), "nothing"

    with pytest.raises(RuntimeError):
        r.run_ucmv_query(E(), "SELECT 1", "country")


# ── Bounded-concurrency query execution (runner._execute_many) ──────────────

from src.services.tools.metric_view_utils.reconciliation import runner as _runner


def test_execute_many_preserves_order_under_concurrency():
    """Results come back in INPUT order even though later queries finish first —
    so the downstream in-order merge is identical to a serial run."""
    import time as _t

    class _OrderExec:
        def execute(self, model_id, dax):
            n = int(dax)
            _t.sleep(0.02 * (6 - n))  # earlier indices sleep longer
            return pd.DataFrame([{"n": n}]), None

    out = _runner._execute_many(_OrderExec(), "mid", [str(i) for i in range(6)])
    assert [df.iloc[0]["n"] for df, _ in out] == list(range(6))


def test_execute_many_edge_cases():
    class _Exec:
        def __init__(self):
            self.calls = 0

        def execute(self, model_id, dax):
            self.calls += 1
            return pd.DataFrame([{"x": dax}]), None

    assert _runner._execute_many(_Exec(), "mid", []) == []
    ex = _Exec()
    out = _runner._execute_many(ex, "mid", ["only"])  # single → inline, no pool
    assert ex.calls == 1 and out[0][0].iloc[0]["x"] == "only"


def test_execute_many_propagates_exception_like_serial():
    class _Boom:
        def execute(self, model_id, dax):
            raise RuntimeError("exec failed")

    with pytest.raises(RuntimeError):
        _runner._execute_many(_Boom(), "mid", ["a", "b", "c"])
