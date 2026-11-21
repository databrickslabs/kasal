"""Reconciliation core: the mapping features added on top of the base layer.

Covers value_format raw_number_hundred, week grain, month grain by calendar
year + month, ucmv group_by / period_format, extra_group_by label columns with
NULL periods, the timeout ladder (retry per measure, then chunks, then "not
compared"), per-measure pbi_chunk_by, the period x member cross breakdown, and
the cross-process run lock. Executors are fakes; nothing touches the network.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import time

import pandas as pd
import pytest
import yaml

from src.services.tools.metric_view_utils import reconciliation as r
from src.services.tools.metric_view_utils.reconciliation import run_lock as lock
from src.services.tools.metric_view_utils.reconciliation.ucmv_query import (
    ucmv_context_alias,
)


def _mapping(time_dimension: dict, measures: list[dict]) -> r.UCMVMapping:
    return r.load_mapping_dict(
        {
            "ucmv_table": "cat.sch.fact_sc",
            "pbi_semantic_model_id": "model-123",
            "pbi_fact_table": "Fact_SC",
            "time_dimension": time_dimension,
            "default_dimension": "country",
            "dimensions": {
                "country": {
                    "ucmv_column": "country_group",
                    "pbi_dim_table": "C_Dim_Geography",
                    "pbi_dim_column": "country",
                }
            },
            "measures": measures,
        }
    )


MONTH_TIME = {
    "grain": "month",
    "ucmv": {"mode": "column", "name": "fiscper"},
    "pbi_column": "fiscper",
    "pbi_table": "C_Dim_calendar",
}


def _direct(name: str, pbi: str, **extra: object) -> dict:
    return {"ucmv_measure": name, "pbi_kind": "direct", "pbi_measure": pbi, **extra}


class _Pbi:
    """PBI executor fake: ``handler(dax) -> (df, err)``; records every query."""

    def __init__(self, handler) -> None:
        self.handler = handler
        self.calls: list[str] = []

    def execute(self, model_id: str, dax: str):
        self.calls.append(dax)
        return self.handler(dax)


# ── value format ─────────────────────────────────────────────────────────────


def test_raw_number_hundred_divides_by_100():
    out = r.parse_value(pd.Series([25.3, "40"]), "raw_number_hundred")
    assert out.tolist() == pytest.approx([0.253, 0.4])


def test_raw_number_hundred_is_a_valid_mapping_value_format():
    m = _mapping(MONTH_TIME, [_direct("pct", "PCT", value_format="raw_number_hundred")])
    assert m.measure("pct").value_format == "raw_number_hundred"


# ── week grain ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value, expected",
    [
        ("202641", "202641"),
        (202641, "202641"),
        (202641.0, "202641"),
        ("202641.0", "202641"),
        ("41.2026", "202641"),
        ("2026-41", "202641"),
        ("2026W41", "202641"),
        ("04.2025", "202504"),
    ],
)
def test_week_key_spellings_normalise_to_yyyyww(value, expected):
    assert r.week_key(value) == expected


@pytest.mark.parametrize("value", [None, float("nan"), pd.NA, "<NA>", "", "nan"])
def test_blank_week_is_not_a_week_key(value):
    assert not r.is_week_key(r.week_key(value))


def test_closed_weeks_excludes_latest_and_blanks():
    assert r.closed_weeks(["202640", "41.2026", None, 202642.0, "nan"]) == {
        "202640",
        "202641",
    }


def _week_mapping() -> r.UCMVMapping:
    return _mapping(
        {
            "grain": "week",
            "ucmv": {"mode": "column", "name": "week_key"},
            "pbi_column": "week",
            "pbi_table": "Dim_Calendar_Daily",
        },
        [_direct("m1", "M1")],
    )


def test_week_compare_uses_normalised_keys_closed_weeks_and_ignores_blanks():
    mapping = _week_mapping()
    ucmv = pd.DataFrame(
        [
            {"country": "AT", "period": "202640", "m1": 1.0},
            {"country": "AT", "period": "202641", "m1": 2.0},
            {"country": "AT", "period": "202642", "m1": 3.0},  # latest: open
            {"country": "AT", "period": None, "m1": 9.0},  # blank key: ignored
        ]
    )
    pbi = pd.DataFrame(
        [
            {"country": "AT", "period": "40.2026", "m1": 1.0},
            {"country": "AT", "period": 202641.0, "m1": 2.0},
            {"country": "AT", "period": "42.2026", "m1": 3.0},
        ]
    )
    wide = r.compare(ucmv, pbi, mapping, "country", [])
    assert sorted(wide["period"]) == ["202640", "202641"]
    assert wide["m1_delta"].tolist() == [0.0, 0.0]


def test_week_years_are_selected_by_fiscal_year():
    mapping = _week_mapping()
    ucmv = pd.DataFrame(
        [
            {"country": "AT", "period": "202540", "m1": 1.0},
            {"country": "AT", "period": "202641", "m1": 2.0},
            {"country": "AT", "period": "202642", "m1": 3.0},
        ]
    )
    pbi = pd.DataFrame(
        [
            {"country": "AT", "period": "40.2025", "m1": 1.0},
            {"country": "AT", "period": "41.2026", "m1": 2.0},
        ]
    )
    wide = r.compare(ucmv, pbi, mapping, "country", [2026])
    assert wide["period"].tolist() == ["202641"]


# ── month grain by calendar year + month ─────────────────────────────────────

YEAR_MONTH_TIME = {
    "grain": "month",
    "ucmv": {"mode": "column", "name": "fiscper"},
    "pbi_year_column": "year",
    "pbi_month_column": "month",
    "pbi_table": "C_Dim_calendar",
}


def test_year_month_period_filter_and_grouping():
    mapping = _mapping(YEAR_MONTH_TIME, [_direct("m1", "M1")])
    dax = r.build_direct_query(
        mapping, "country", ["AT"], [mapping.measure("m1")], "2025003"
    )
    assert "'C_Dim_calendar'[year]" in dax and "'C_Dim_calendar'[month]" in dax
    assert "TREATAS({2025}, 'C_Dim_calendar'[year])" in dax
    assert "TREATAS({3}, 'C_Dim_calendar'[month])" in dax


def test_year_month_run_builds_period_key():
    mapping = _mapping(
        YEAR_MONTH_TIME, [_direct("m1", "M1", value_format="raw_number")]
    )

    def handler(dax):
        frame = pd.DataFrame(
            [
                {
                    "C_Dim_Geography[country]": "AT",
                    "C_Dim_calendar[year]": 2025,
                    "C_Dim_calendar[month]": 3,
                    "m1": 4.5,
                }
            ]
        )
        return frame, None

    out, report = r.run_pbi_queries_reported(
        _Pbi(handler), "model-123", mapping, "country", ["AT"]
    )
    assert out["period"].tolist() == ["2025003"]
    assert out["m1"].tolist() == [4.5]
    assert report.timed_out == set()


def test_year_month_needs_both_columns_and_month_grain():
    with pytest.raises(ValueError, match="go together"):
        _mapping({**YEAR_MONTH_TIME, "pbi_month_column": None}, [_direct("m1", "M1")])
    with pytest.raises(ValueError, match="need grain 'month'"):
        _mapping({**YEAR_MONTH_TIME, "grain": "week"}, [_direct("m1", "M1")])


def test_time_dimension_needs_a_period_column_or_year_and_month():
    with pytest.raises(ValueError, match="needs pbi_column"):
        _mapping({"grain": "month", "ucmv": {"mode": "column", "name": "fiscper"}}, [])


# ── ucmv group_by and period_format ──────────────────────────────────────────


def test_ucmv_group_by_is_grouped_not_selected_and_date_period_is_converted():
    mapping = _mapping(
        {
            "grain": "month",
            "ucmv": {
                "mode": "column",
                "name": "year_month",
                "group_by": ["order_col"],
                "period_format": "date_to_fiscper",
            },
            "pbi_column": "fiscper",
            "pbi_table": "C_Dim_calendar",
        },
        [_direct("m1", "M1")],
    )
    sql = r.build_ucmv_query(mapping, "country", ["AT"], [2025])
    assert (
        "CONCAT(SUBSTR(CAST(`year_month` AS STRING), 1, 4), '0', "
        "SUBSTR(CAST(`year_month` AS STRING), 6, 2)) AS period"
    ) in sql
    assert "GROUP BY `country_group`, `year_month`, `order_col`" in sql
    assert "`order_col` AS" not in sql


def test_period_format_needs_column_mode():
    with pytest.raises(ValueError, match="needs ucmv.mode='column'"):
        _mapping(
            {
                "grain": "month",
                "ucmv": {
                    "mode": "reconstruct",
                    "from": ["fiscal_year", "fiscal_month"],
                    "period_format": "date_to_fiscper",
                },
                "pbi_column": "fiscper",
            },
            [],
        )


# ── value_format + extra_group_by {pbi, ucmv} parsing ───────────────────────


def test_extra_group_by_object_form_sets_label_mapping():
    text = """
ucmv_table: cat.sch.fact_sc
pbi_semantic_model_id: model-123
pbi_fact_table: Fact_SC
time_dimension:
  grain: month
  ucmv: {mode: column, name: fiscper}
  pbi_column: fiscper
  pbi_table: C_Dim_calendar
default_dimension: country
dimensions:
  country: {ucmv_column: country_group, pbi_dim_table: C_Dim_Geography, pbi_dim_column: country}
measures:
- ucmv_measure: m5
  pbi_kind: direct
  pbi_measure: M5
  extra_group_by:
  - "C_Dim_Calendar[Month]"
  - {pbi: "C_Dim_Calendar_Daily[Latest_Month]", ucmv: latest_month}
"""
    m5 = r.load_mapping(text).measure("m5")
    assert m5.pbi_extra_group_by == [
        "C_Dim_Calendar[Month]",
        "C_Dim_Calendar_Daily[Latest_Month]",
    ]
    assert m5.ucmv_context_columns == {
        "C_Dim_Calendar_Daily[Latest_Month]": "latest_month"
    }


def test_extra_group_by_object_without_ucmv_column_is_rejected():
    with pytest.raises(ValueError, match="extra_group_by entries"):
        _mapping(
            MONTH_TIME,
            [
                _direct(
                    "m5",
                    "M5",
                    extra_group_by=[{"pbi": "C_Dim_Calendar_Daily[Latest_Month]"}],
                )
            ],
        )


def test_pbi_chunk_by_must_be_period_or_member():
    with pytest.raises(ValueError, match="pbi_chunk_by"):
        _mapping(MONTH_TIME, [_direct("m1", "M1", pbi_chunk_by="week")])


# ── timeout ladder ───────────────────────────────────────────────────────────

PERIODS = ["2025001", "2025002"]
VALUES = {"m1": 1.0, "m2": 2.0, "m3": 3.0}


def _rows(dax: str, members: list[str]) -> pd.DataFrame:
    """What SUMMARIZECOLUMNS returns: one row per (member, period) it was asked
    for, with the measure columns the query names."""
    periods = [p for p in PERIODS if f'"{p}"' in dax] or PERIODS
    measures = [m for m in VALUES if f'"{m}", ' in dax]
    rows = [
        {
            "C_Dim_Geography[country]": member,
            "C_Dim_calendar[fiscper]": p,
            **{m: VALUES[m] for m in measures},
        }
        for member in members
        for p in periods
    ]
    return pd.DataFrame(rows)


def test_timeout_ladder_retries_per_measure_then_chunks_then_reports():
    mapping = _mapping(
        MONTH_TIME,
        [_direct("m1", "M1"), _direct("m2", "M2"), _direct("m3", "M3")],
    )

    def handler(dax):
        if '"m3", ' in dax:
            return pd.DataFrame(), "The query timed out"  # never completes
        if '"2025' not in dax and '"m2", ' in dax:
            return pd.DataFrame(), "timeout"  # m2 completes only per period
        return _rows(dax, ["AT"]), None

    pbi = _Pbi(handler)
    out, report = r.run_pbi_queries_reported(
        pbi, "model-123", mapping, "country", ["AT"], context_periods=PERIODS
    )
    assert report.timed_out == {"m3"}
    assert sorted(out[out["m1"].notna()]["period"]) == PERIODS
    assert sorted(out[out["m2"].notna()]["period"]) == PERIODS
    assert out["m3"].isna().all()
    # m2 went through the per-period chunks, not only the batch.
    assert any('"2025001"' in q and '"m2", ' in q for q in pbi.calls)


def test_non_timeout_error_on_direct_query_is_fatal():
    mapping = _mapping(MONTH_TIME, [_direct("m1", "M1")])

    def handler(dax):
        return pd.DataFrame(), "DAX syntax error"

    with pytest.raises(RuntimeError, match="DAX query failed"):
        r.run_pbi_queries_reported(
            _Pbi(handler),
            "model-123",
            mapping,
            "country",
            ["AT"],
            context_periods=PERIODS,
        )


def test_pbi_chunk_by_member_skips_the_all_at_once_attempt():
    mapping = _mapping(MONTH_TIME, [_direct("m1", "M1", pbi_chunk_by="member")])

    def handler(dax):
        return _rows(dax, ["AT", "NG"] if "NG" in dax else ["AT"]), None

    pbi = _Pbi(handler)
    out, report = r.run_pbi_queries_reported(
        pbi, "model-123", mapping, "country", ["AT", "NG"], context_periods=PERIODS
    )
    assert report.timed_out == set()
    assert not any('"AT", "NG"' in q or '"NG", "AT"' in q for q in pbi.calls)
    assert sorted(set(out["country"])) == ["AT", "NG"]


# ── calendar labels: NULL periods are not queried and not compared ───────────

LABEL = "C_Dim_Calendar_Daily[Latest_Month]"


def _label_mapping() -> r.UCMVMapping:
    return _mapping(
        MONTH_TIME,
        [
            _direct(
                "m5",
                "M5",
                extra_group_by=[{"pbi": LABEL, "ucmv": "latest_month"}],
            )
        ],
    )


def _label_ucmv() -> pd.DataFrame:
    alias = ucmv_context_alias(LABEL)
    return pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "m5": 10.0, alias: "Jan"},
            {"country": "AT", "period": "2025002", "m5": 7.0, alias: None},
        ]
    )


def test_null_label_period_is_not_queried_and_is_reported():
    mapping = _label_mapping()
    ucmv = _label_ucmv()
    labels = r.labels_from_ucmv(ucmv, mapping.resolved_measures())
    assert labels == {"2025001": {LABEL: "Jan"}}

    def handler(dax):
        return pd.DataFrame([{"C_Dim_Geography[country]": "AT", "m5": 10.0}]), None

    pbi = _Pbi(handler)
    out, report = r.run_pbi_queries_reported(
        pbi,
        "model-123",
        mapping,
        "country",
        ["AT"],
        context_periods=["2025001", "2025002"],
        context_labels=labels,
    )
    assert len(pbi.calls) == 1 and '{"Jan"}' in pbi.calls[0]
    assert report.skipped_periods == {"m5": ["2025002"]}
    assert out["period"].tolist() == ["2025001"]

    wide = r.compare(ucmv, out, mapping, "country", [])
    long_df = r.to_long_format(wide, mapping, "country")
    kept, dropped = r.drop_skipped_cells(long_df, report.skipped_periods, "month")
    assert dropped == {"m5": 1}
    assert kept["period"].tolist() == ["2025001"]


def test_label_mismatch_within_one_period_is_an_error():
    alias = ucmv_context_alias(LABEL)
    ucmv = pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "m5": 1.0, alias: "Jan"},
            {"country": "AT", "period": "2025001", "m5": 1.0, alias: "Feb"},
        ]
    )
    with pytest.raises(ValueError, match="more than one UCMV label"):
        r.labels_from_ucmv(ucmv, _label_mapping().resolved_measures())


def test_label_measure_without_context_periods_is_rejected():
    with pytest.raises(ValueError, match="context_periods is required"):
        r.run_pbi_queries_reported(
            _Pbi(lambda dax: (pd.DataFrame(), None)),
            "model-123",
            _label_mapping(),
            "country",
            ["AT"],
        )


# ── cross breakdown ──────────────────────────────────────────────────────────


def _cross_long() -> pd.DataFrame:
    mapping = _mapping(MONTH_TIME, [_direct("m1", "M1")])
    ucmv = pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "m1": 1.0},
            {"country": "AT", "period": "2025002", "m1": 2.0},
            {"country": "NG", "period": "2025001", "m1": 1.0},
            {"country": "NG", "period": "2025002", "m1": 5.0},
        ]
    )
    pbi = pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "m1": 1.0},
            {"country": "AT", "period": "2025002", "m1": 9.0},
            {"country": "NG", "period": "2025002", "m1": 5.0},  # NG 2025001 missing
        ]
    )
    wide = r.compare(ucmv, pbi, mapping, "country", [])
    return r.to_long_format(wide, mapping, "country")


def test_cross_breakdown_counts_cells_and_carries_no_values():
    breakdown = r.cross_breakdown(_cross_long())
    assert set(breakdown) == {"m1"}
    rows = breakdown["m1"]
    assert len(rows) == 4
    assert sum(row["differ"] for row in rows) == 1
    assert sum(row["extra_in_uc"] for row in rows) == 1
    assert sum(row["aligned"] for row in rows) == 2
    differing = next(row for row in rows if row["differ"])
    assert differing["period"] == "2025002" and differing["dimension"] == "AT"
    assert differing["relative_difference"] == pytest.approx(7 / 9)
    assert set(rows[0]) == {
        "period",
        "dimension",
        "aligned",
        "missing_in_uc",
        "extra_in_uc",
        "differ",
        "relative_difference",
    }


def test_cross_breakdown_skips_fully_aligned_measures():
    mapping = _mapping(MONTH_TIME, [_direct("m1", "M1")])
    ucmv = pd.DataFrame([{"country": "AT", "period": "2025001", "m1": 1.0}])
    pbi = pd.DataFrame([{"country": "AT", "period": "2025001", "m1": 1.0}])
    long_df = r.to_long_format(
        r.compare(ucmv, pbi, mapping, "country", []), mapping, "country"
    )
    assert r.cross_breakdown(long_df) == {}


# ── run lock ─────────────────────────────────────────────────────────────────


def test_lock_is_exclusive_per_warehouse_and_released(tmp_path: Path):
    path = r.lock_path_for("wh-a", tmp_path)
    with r.warehouse_run_lock("wh-a", base_dir=tmp_path):
        assert path.exists()
        with pytest.raises(r.RunLockHeldError, match="locked by PID"):
            with r.warehouse_run_lock("wh-a", base_dir=tmp_path):
                pass
        # another warehouse is not affected
        with r.warehouse_run_lock("wh-b", base_dir=tmp_path):
            pass
    assert not path.exists()


def test_lock_file_name_hides_the_warehouse_id(tmp_path: Path):
    path = r.lock_path_for("../../etc/evil id", tmp_path)
    assert path.parent == tmp_path
    assert "evil" not in path.name


def test_stale_lock_is_taken_over(tmp_path: Path):
    path = r.lock_path_for("wh-stale", tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "pid": 4242424,
                "started": "x",
                "started_epoch": 1.0,
                "token": "old",
                "warehouse_hash": "",
            }
        ),
        encoding="utf-8",
    )
    with r.warehouse_run_lock("wh-stale", base_dir=tmp_path, alive=lambda h: False):
        held = json.loads(path.read_text(encoding="utf-8"))
        assert held["pid"] == os.getpid()
    assert not path.exists()


def test_reused_pid_counts_as_stale(tmp_path: Path):
    holder = lock.LockHolder(
        pid=os.getpid(), started="x", started_epoch=0.0, token="t", warehouse_hash=""
    )
    assert lock.process_alive(holder) is False
    live = lock.LockHolder(
        pid=os.getpid(),
        started="x",
        started_epoch=time.time() + 3600,
        token="t",
        warehouse_hash="",
    )
    assert lock.process_alive(live) is True


def test_wait_polls_until_the_holder_is_gone(tmp_path: Path):
    path = r.lock_path_for("wh-wait", tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "started": "x",
                "started_epoch": 1.0,
                "token": "held",
                "warehouse_hash": "",
            }
        ),
        encoding="utf-8",
    )
    states = iter([True, False])
    clock = [0.0]
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    with r.warehouse_run_lock(
        "wh-wait",
        base_dir=tmp_path,
        wait_seconds=60,
        alive=lambda h: next(states),
        clock=lambda: clock[0],
        sleep=sleep,
    ):
        pass
    assert sleeps == [lock.POLL_SECONDS]


def test_wait_gives_up_after_the_limit(tmp_path: Path):
    path = r.lock_path_for("wh-limit", tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "started": "x",
                "started_epoch": 1.0,
                "token": "held",
                "warehouse_hash": "",
            }
        ),
        encoding="utf-8",
    )
    clock = [0.0]

    def sleep(seconds: float) -> None:
        clock[0] += seconds

    with pytest.raises(r.RunLockHeldError, match="not waited"):
        with r.warehouse_run_lock(
            "wh-limit",
            base_dir=tmp_path,
            wait_seconds=12,
            alive=lambda h: True,
            clock=lambda: clock[0],
            sleep=sleep,
        ):
            pass
    assert clock[0] == pytest.approx(12.0)


def test_week_yaml_keeps_group_by_and_grain():
    text = yaml.safe_dump(
        {
            "ucmv_table": "cat.sch.fact_sc",
            "pbi_semantic_model_id": "model-123",
            "pbi_fact_table": "Fact_SC",
            "time_dimension": {
                "grain": "week",
                "ucmv": {"mode": "column", "name": "week_key", "group_by": ["order"]},
                "pbi_column": "week",
                "year_filter_column": "fiscal_year",
            },
            "default_dimension": "country",
            "dimensions": {
                "country": {
                    "ucmv_column": "c",
                    "pbi_dim_table": "D",
                    "pbi_dim_column": "c",
                }
            },
            "measures": [],
        }
    )
    mapping = r.load_mapping(text)
    td = mapping.binding.time_dimension
    assert td.grain == "week" and td.ucmv_group_by == ["order"]
