"""UCMVReconciliationTool: the options added to the reconciliation layer.

``cross_breakdown``, timed-out measures, calendar-label periods left out of the
comparison, and the run lock. Executors are fakes (same pattern as
``test_ucmv_reconciliation_tool.py``); there is no live network.
"""

import json
import os
import time

import pandas as pd

from src.services.tools.metric_view_utils.reconciliation import lock_path_for
from src.services.tools.metric_view_utils.reconciliation.run_lock import LockHolder
from src.services.tools.metric_view_utils.reconciliation.ucmv_query import (
    ucmv_context_alias,
)
from src.services.tools.ucmv_reconciliation_tool import UCMVReconciliationTool

VIEW = "cat.sch.fact_sc"

MAPPING_YAML = """
ucmv_table: cat.sch.fact_sc
pbi_semantic_model_id: model-123
pbi_fact_table: Fact_SC
pbi_workspace_id: ws-1
time_dimension:
  grain: month
  ucmv:
    mode: column
    name: fiscper
  pbi_column: fiscper
  pbi_table: C_Dim_calendar
default_dimension: country
dimensions:
  country:
    ucmv_column: country_group
    pbi_dim_table: C_Dim_Geography
    pbi_dim_column: country
measures:
- ucmv_measure: sweetener_yield_bp
  pbi_kind: direct
  pbi_measure: Sweetner_Yeild_BP
- ucmv_measure: pet_yield_bp
  pbi_kind: direct
  pbi_measure: PET_Yeild_BP
"""

LABEL_YAML = """
ucmv_table: cat.sch.fact_sc
pbi_semantic_model_id: model-123
pbi_fact_table: Fact_SC
pbi_workspace_id: ws-1
time_dimension:
  grain: month
  ucmv:
    mode: column
    name: fiscper
  pbi_column: fiscper
  pbi_table: C_Dim_calendar
default_dimension: country
dimensions:
  country:
    ucmv_column: country_group
    pbi_dim_table: C_Dim_Geography
    pbi_dim_column: country
measures:
- ucmv_measure: m5
  pbi_kind: direct
  pbi_measure: M5
  extra_group_by:
  - pbi: C_Dim_Calendar_Daily[Latest_Month]
    ucmv: latest_month
"""


class _FakeSQL:
    def __init__(self, df):
        self._df = df

    def execute(self, sql):
        return self._df.copy(), None


class _FakePBI:
    """``handler(dax) -> (df, err)``: answers every PBI query."""

    def __init__(self, handler):
        self.handler = handler
        self.calls: list[str] = []

    def execute(self, model_id, dax):
        self.calls.append(dax)
        return self.handler(dax)


def _ucmv_frame():
    return pd.DataFrame(
        [
            {
                "country": "AT",
                "period": "2025001",
                "sweetener_yield_bp": 1.0,
                "pet_yield_bp": 1.0,
            },
            {
                "country": "AT",
                "period": "2025002",
                "sweetener_yield_bp": 0.5,
                "pet_yield_bp": 2.0,
            },
        ]
    )


def _pbi_rows(dax, pet_value_for_second: float = 9.0) -> pd.DataFrame:
    """Answer a whole-model query with both measures and both periods."""
    rows = [
        {
            "C_Dim_Geography[country]": "AT",
            "C_Dim_calendar[fiscper]": "2025001",
            "sweetener_yield_bp": 1.0,
            "pet_yield_bp": 1.0,
        },
        {
            "C_Dim_Geography[country]": "AT",
            "C_Dim_calendar[fiscper]": "2025002",
            "sweetener_yield_bp": 0.5,
            "pet_yield_bp": pet_value_for_second,
        },
    ]
    keep = [c for c in ("sweetener_yield_bp", "pet_yield_bp") if f'"{c}"' in dax]
    return pd.DataFrame(
        [
            {
                k: v
                for k, v in row.items()
                if k in ("C_Dim_Geography[country]", "C_Dim_calendar[fiscper]")
                or k in keep
            }
            for row in rows
        ]
    )


def _run(tool, mapping=MAPPING_YAML, **extra):
    return json.loads(
        tool._run(
            mapping=mapping,
            warehouse_id="wh-1",
            reference_years=[2025],
            dimension_values=["AT"],
            **extra,
        )
    )


def test_cross_breakdown_option_adds_the_per_cell_table(monkeypatch):
    pbi = _FakePBI(lambda dax: (_pbi_rows(dax), None))
    tool = UCMVReconciliationTool(cross_breakdown=True)
    monkeypatch.setattr(
        tool, "_build_sql_executor", lambda *a, **k: _FakeSQL(_ucmv_frame())
    )
    monkeypatch.setattr(tool, "_build_pbi_executor", lambda *a, **k: pbi)

    view = _run(tool)["views"][VIEW]
    assert set(view["cross_breakdown"]) == {"pet_yield_bp"}
    rows = view["cross_breakdown"]["pet_yield_bp"]
    assert len(rows) == 2
    differing = [row for row in rows if row["differ"]]
    assert len(differing) == 1 and differing[0]["period"] == "2025002"
    assert "pbi_value" not in json.dumps(view["cross_breakdown"])


def test_cross_breakdown_is_off_by_default(monkeypatch):
    tool = UCMVReconciliationTool()
    monkeypatch.setattr(
        tool, "_build_sql_executor", lambda *a, **k: _FakeSQL(_ucmv_frame())
    )
    monkeypatch.setattr(
        tool,
        "_build_pbi_executor",
        lambda *a, **k: _FakePBI(lambda d: (_pbi_rows(d), None)),
    )
    assert "cross_breakdown" not in _run(tool)["views"][VIEW]


def test_timed_out_measure_is_reported_and_not_compared(monkeypatch):
    def handler(dax):
        if '"PET_Yeild_BP"' in dax or '"pet_yield_bp", ' in dax:
            return pd.DataFrame(), "The query timed out"
        return _pbi_rows(dax), None

    tool = UCMVReconciliationTool()
    monkeypatch.setattr(
        tool, "_build_sql_executor", lambda *a, **k: _FakeSQL(_ucmv_frame())
    )
    monkeypatch.setattr(tool, "_build_pbi_executor", lambda *a, **k: _FakePBI(handler))

    data = _run(tool)
    view = data["views"][VIEW]
    assert view["timed_out_measures"] == ["pet_yield_bp"]
    assert "pet_yield_bp" not in view["measures"]
    assert view["summary"]["measures_timed_out"] == 1
    assert view["measures"]["sweetener_yield_bp"]["pct_aligned"] == 100.0
    assert data["overall"]["measures_timed_out"] == 1


def test_label_null_period_is_left_out_and_counted(monkeypatch):
    alias = ucmv_context_alias("C_Dim_Calendar_Daily[Latest_Month]")
    ucmv = pd.DataFrame(
        [
            {"country": "AT", "period": "2025001", "m5": 10.0, alias: "Jan"},
            {"country": "AT", "period": "2025002", "m5": 7.0, alias: None},
        ]
    )

    def handler(dax):
        assert '"2025002"' not in dax  # the NULL-label period is never queried
        return pd.DataFrame([{"C_Dim_Geography[country]": "AT", "m5": 10.0}]), None

    pbi = _FakePBI(handler)
    tool = UCMVReconciliationTool()
    monkeypatch.setattr(tool, "_build_sql_executor", lambda *a, **k: _FakeSQL(ucmv))
    monkeypatch.setattr(tool, "_build_pbi_executor", lambda *a, **k: pbi)

    view = _run(tool, mapping=LABEL_YAML)["views"][VIEW]
    m5 = view["measures"]["m5"]
    assert m5["periods_skipped"] == 1
    assert m5["cells_left_out"] == 1
    assert m5["cells_total"] == 1
    assert m5["pct_aligned"] == 100.0
    assert len(pbi.calls) == 1


def test_lock_held_by_a_live_run_stops_the_reconciliation(monkeypatch):
    path = lock_path_for("wh-held-test")
    path.parent.mkdir(parents=True, exist_ok=True)
    holder_text = json.dumps(
        {
            "pid": os.getpid(),
            "started": "now",
            "started_epoch": time.time(),
            "token": "other-run",
            "warehouse_hash": "",
        }
    )
    path.write_text(holder_text, encoding="utf-8")
    try:
        tool = UCMVReconciliationTool(lock=True)
        monkeypatch.setattr(
            tool, "_build_sql_executor", lambda *a, **k: _FakeSQL(_ucmv_frame())
        )
        raw = tool._run(
            mapping=MAPPING_YAML,
            warehouse_id="wh-held-test",
            reference_years=[2025],
            dimension_values=["AT"],
        )
        assert "not started" in json.loads(raw)["error"]
        assert path.exists() and json.loads(path.read_text())["token"] == "other-run"
    finally:
        path.unlink(missing_ok=True)


def test_lock_is_off_by_default_and_released_after_a_run(monkeypatch):
    path = lock_path_for("wh-free-test")
    tool = UCMVReconciliationTool()
    monkeypatch.setattr(
        tool, "_build_sql_executor", lambda *a, **k: _FakeSQL(_ucmv_frame())
    )
    monkeypatch.setattr(
        tool,
        "_build_pbi_executor",
        lambda *a, **k: _FakePBI(lambda d: (_pbi_rows(d), None)),
    )
    raw = tool._run(
        mapping=MAPPING_YAML,
        warehouse_id="wh-free-test",
        reference_years=[2025],
        dimension_values=["AT"],
    )
    assert "views" in json.loads(raw)
    assert not path.exists()

    locked = UCMVReconciliationTool(lock=True)
    monkeypatch.setattr(
        locked, "_build_sql_executor", lambda *a, **k: _FakeSQL(_ucmv_frame())
    )
    monkeypatch.setattr(
        locked,
        "_build_pbi_executor",
        lambda *a, **k: _FakePBI(lambda d: (_pbi_rows(d), None)),
    )
    out = json.loads(
        locked._run(
            mapping=MAPPING_YAML,
            warehouse_id="wh-free-test",
            reference_years=[2025],
            dimension_values=["AT"],
        )
    )
    assert "views" in out
    assert not path.exists()  # released after the run


def test_lock_holder_is_described_for_the_operator():
    holder = LockHolder(
        pid=321,
        started="2026-11-05T10:00:00+00:00",
        started_epoch=1.0,
        token="t",
        warehouse_hash="h",
    )
    assert "PID 321" in holder.describe()
