"""Count check verdicts and the refresh plan, with fake PBI and SQL executors."""

import pytest

from src.services.tools.ucmv_source_layer_utils import count_check as cc
from src.services.tools.ucmv_source_layer_utils import refresh_plan as rp
from src.services.tools.ucmv_source_layer_utils.sql_types import SqlError, SqlResult

TARGET = "`cat`.`sch`.`src_sales`"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_worst_verdict_ordering():
    assert cc.worst_verdict(["match", "drift"]) == "drift"
    assert cc.worst_verdict(["drift", "cut", "match"]) == "cut"
    assert cc.worst_verdict(["cut", "differs"]) == "differs"
    assert cc.worst_verdict([]) == "match"


def test_two_pass_verdict():
    assert cc.two_pass_verdict(True, None) == "match"
    assert cc.two_pass_verdict(False, True) == "drift"
    assert cc.two_pass_verdict(False, False) == "differs"
    assert cc.two_pass_verdict(False, None) == "differs"


def test_normalize_key_cuts_dates_and_folds_case():
    assert cc.normalize_key("2024-01-01T00:00:00") == "2024-01-01"
    assert cc.normalize_key(202401.0) == "202401"
    assert cc.normalize_key("EU ") == "eu"
    assert cc.normalize_key(None) == "(blank)"


def test_resolve_column_uses_map_then_own_name_then_snake():
    names = ["calendar_day", "Region"]
    assert cc.resolve_uc_column(names, "Calendar Day", {}) == "calendar_day"
    assert cc.resolve_uc_column(names, "region", {}) == "Region"
    assert cc.resolve_uc_column(names, "Day", {"Day": "calendar_day"}) == "calendar_day"
    assert cc.resolve_uc_column(names, "Missing", {}) is None


def test_parse_describe_stops_at_detail_block():
    rows = [
        ["Month", "date", None],
        ["Amount", "double", None],
        ["# Partition Information", None, None],
    ]
    assert cc.parse_describe(rows) == [("Month", "date"), ("Amount", "double")]


def test_group_differences_show_counts_and_sum_names_only():
    pbi = {"2024-01-01": [2, 30.0], "2024-02-01": [2, 5.0]}
    uc = {"2024-01-01": [2, 30.0], "2024-02-01": [3, 9.0], "2024-03-01": [1, 1.0]}
    diffs = cc.group_differences(pbi, uc, ["Amount"])
    by_period = {d["period"]: d for d in diffs}
    assert "2024-01-01" not in by_period
    assert by_period["2024-02-01"] == {
        "period": "2024-02-01",
        "pbi_rows": 2,
        "uc_rows": 3,
        "sums_differ": ["Amount"],
    }
    assert by_period["2024-03-01"]["only_in"] == "UC"


def test_member_comparison_lists_values_only_for_text():
    text = cc.member_comparison(["EU", "US"], ["EU", "APAC"], text=True)
    assert text["only_pbi"] == 1 and text["only_uc"] == 1
    assert text["only_uc_values"] == ["apac"]
    numeric = cc.member_comparison([1, 2], [1, 3], text=False)
    assert "only_uc_values" not in numeric
    assert numeric["verdict"] == "differs"


# ---------------------------------------------------------------------------
# Full run with fakes
# ---------------------------------------------------------------------------


def _pbi(query):
    if query.startswith("EVALUATE TOPN(1"):
        return [
            {
                "Sales[Month]": "2024-01-01T00:00:00",
                "Sales[Amount]": 1.0,
                "Sales[Region]": "EU",
            }
        ]
    if query.startswith('EVALUATE ROW("n", COUNTROWS(DISTINCT'):
        return [{"[n]": 2}]
    if query.startswith('EVALUATE ROW("n", COUNTROWS(SUMMARIZE'):
        return [{"[n]": 2}]
    if query.startswith("EVALUATE ROW(\"n\", COUNTROWS('Sales'))"):
        return [{"[n]": 4}]
    if query.startswith("EVALUATE SUMMARIZECOLUMNS"):
        return [
            {"Sales[Month]": "2024-01-01T00:00:00", "[n]": 2, "[s0]": 30.0},
            {"Sales[Month]": "2024-02-01T00:00:00", "[n]": 2, "[s0]": 5.0},
        ]
    if query.startswith("EVALUATE DISTINCT"):
        return [{"Sales[Region]": "EU"}, {"Sales[Region]": "US"}]
    raise AssertionError(f"unexpected PBI query: {query}")


def _uc(statement):
    if statement.startswith("DESCRIBE TABLE"):
        return SqlResult(
            rows=[
                ["Month", "date", None],
                ["Amount", "double", None],
                ["Region", "string", None],
            ]
        )
    if statement.startswith("SELECT DISTINCT"):
        return SqlResult(rows=[["EU"], ["APAC"]])
    if "GROUP BY" in statement:
        if "WHERE" in statement:
            return SqlResult(
                rows=[["2024-01-01", "2", "30.0"], ["2024-02-01", "2", "5.0"]]
            )
        return SqlResult(rows=[["2024-01-01", "2", "30.0"], ["2024-02-01", "3", "9.0"]])
    if statement.startswith("SELECT COUNT(*)"):
        if "WHERE" in statement:
            return SqlResult(rows=[["4"]])
        return SqlResult(rows=[["5"]])
    raise AssertionError(f"unexpected SQL: {statement}")


def test_count_check_reports_drift_after_refresh_cutoff():
    req = cc.CountRequest(
        pbi_table="Sales",
        target=TARGET,
        by="Month",
        members=("Region",),
        sums=True,
        cutoff_column="loaded_at_utc",
    )
    report = cc.run_count_check(
        req,
        dax=_pbi,
        sql=_uc,
        last_refresh=lambda: "2024-03-01T10:00:00Z",
    )
    assert report["total"] == {
        "pbi_rows": 4,
        "uc_rows": 5,
        "uc_rows_after_cutoff": 4,
        "verdict": "drift",
    }
    assert report["cutoff"] == {
        "column": "loaded_at_utc",
        "pbi_refresh_utc": "2024-03-01 10:00:00",
    }
    assert report["by"]["verdict"] == "drift"
    assert report["by"]["sum_columns_compared"] == ["Amount"]
    assert report["members"][0]["only_uc_values"] == ["apac"]
    assert report["members"][0]["verdict"] == "differs"
    assert report["verdict"] == "differs"


def test_count_check_match_without_cutoff():
    def uc(statement):
        if statement.startswith("DESCRIBE"):
            return SqlResult(rows=[["Month", "date", None], ["Amount", "double", None]])
        if "GROUP BY" in statement:
            return SqlResult(
                rows=[["2024-01-01", "2", "30.0"], ["2024-02-01", "2", "5.0"]]
            )
        return SqlResult(rows=[["4"]])

    req = cc.CountRequest(pbi_table="Sales", target=TARGET, by="Month", sums=True)
    report = cc.run_count_check(req, dax=_pbi, sql=uc, last_refresh=lambda: None)
    assert report["total"]["verdict"] == "match"
    assert report["by"]["verdict"] == "match"
    assert report["verdict"] == "match"


def test_count_check_cut_when_pbi_listing_hits_a_limit():
    def dax(query):
        if query.startswith("EVALUATE SUMMARIZECOLUMNS"):
            return [{"Sales[Month]": "2024-01-01", "[n]": 2}]
        if query.startswith('EVALUATE ROW("n", COUNTROWS(SUMMARIZE'):
            return [{"[n]": 7}]
        return _pbi(query)

    req = cc.CountRequest(pbi_table="Sales", target=TARGET, by="Month")
    report = cc.run_count_check(req, dax=dax, sql=_uc, last_refresh=lambda: None)
    assert report["by"]["verdict"] == "cut"
    assert report["verdict"] == "cut" or report["verdict"] == "differs"


def test_count_check_cutoff_needs_a_refresh_time():
    req = cc.CountRequest(
        pbi_table="Sales", target=TARGET, cutoff_column="loaded_at_utc"
    )
    with pytest.raises(ValueError, match="no completed Power BI refresh"):
        cc.run_count_check(req, dax=_pbi, sql=_uc, last_refresh=lambda: None)


# ---------------------------------------------------------------------------
# Refresh plan
# ---------------------------------------------------------------------------

ALTERED = {
    "src_sales": "2026-11-04T12:00:00.000Z",
    "src_dim": "2026-11-01T08:00:00.000Z",
}


def test_refresh_decision_skips_only_when_every_view_is_newer():
    needed, _ = rp.refresh_decision(
        "2026-11-04T10:00:00Z", {"src_sales": ALTERED["src_sales"]}
    )
    assert needed is False
    needed, reason = rp.refresh_decision("2026-11-04T10:00:00Z", ALTERED)
    assert needed is True and "1 view(s)" in reason
    assert rp.refresh_decision(None, ALTERED)[0] is True


def test_select_views_checks_explicit_names_and_prefix():
    req = rp.RefreshRequest(catalog="c", schema="s", views=("c.s.src_sales",))
    assert rp.select_views(ALTERED, req) == ["src_sales"]
    with pytest.raises(ValueError, match="not materialized views"):
        rp.select_views(
            ALTERED, rp.RefreshRequest(catalog="c", schema="s", views=("ghost",))
        )
    prefixed = rp.RefreshRequest(catalog="c", schema="s", prefix="src_")
    assert rp.select_views(ALTERED, prefixed) == ["src_dim", "src_sales"]


def test_run_refresh_skips_without_touching_snapshots_or_views():
    refreshed = []
    loaded = []
    report = rp.run_refresh(
        rp.RefreshRequest(catalog="c", schema="s", skip_if_unchanged=True),
        last_refresh=lambda: "2026-11-04T10:00:00Z",
        list_views=lambda: {"src_sales": ALTERED["src_sales"]},
        load_snapshots=lambda t: loaded.append(t) or {"all_loaded": True},
        refresh_view=refreshed.append,
    )
    assert report["skipped"] is True
    assert refreshed == [] and loaded == []


def test_run_refresh_stops_views_when_a_snapshot_fails():
    refreshed = []
    report = rp.run_refresh(
        rp.RefreshRequest(catalog="c", schema="s"),
        last_refresh=lambda: "2026-11-04T10:00:00Z",
        list_views=lambda: {"src_sales": ALTERED["src_sales"]},
        load_snapshots=lambda t: {"all_loaded": False, "snapshots": []},
        refresh_view=refreshed.append,
    )
    assert refreshed == []
    assert report["all_ok"] is False
    assert report["views_skipped"] == "snapshot load failed"


def test_run_refresh_reports_each_view_and_continues_after_failure():
    def refresh_view(fqn):
        if fqn.endswith("src_dim"):
            raise SqlError("boom")

    report = rp.run_refresh(
        rp.RefreshRequest(catalog="c", schema="s", prefix="src_"),
        last_refresh=lambda: None,
        list_views=lambda: ALTERED,
        load_snapshots=None,
        refresh_view=refresh_view,
    )
    statuses = {v["view"]: v["status"] for v in report["views"]}
    assert statuses == {"src_dim": "failed", "src_sales": "refreshed"}
    assert report["all_ok"] is False


def test_list_views_sql_escapes_schema_quote():
    sql = rp.list_views_sql("cat", "o'x")
    assert "table_schema = 'o''x'" in sql
    assert "MATERIALIZED_VIEW" in sql
