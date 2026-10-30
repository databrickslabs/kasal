"""Tests for UCMVReconciliationTool — orchestration + output shape only.

Executors are mocked; there is NO live network. The deterministic query building
and comparison are covered in
``metric_view_utils/test_reconciliation_core.py``.
"""

import json

import pandas as pd

from src.services.tools.ucmv_reconciliation_tool import UCMVReconciliationTool

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


class _FakeSQL:
    """UCMV-side executor: ``execute(sql) -> (df, err)``."""

    def __init__(self, df):
        self._df = df

    def execute(self, sql):
        return self._df.copy(), None


class _FakePBI:
    """PBI-side executor: ``execute(model_id, dax) -> (df, err)``."""

    def __init__(self, df):
        self._df = df

    def execute(self, model_id, dax):
        return self._df.copy(), None


def test_initialization():
    tool = UCMVReconciliationTool()
    assert tool.name == "UCMV Reconciliation"


def test_dry_run_builds_queries_without_network():
    tool = UCMVReconciliationTool()
    result = tool._run(
        mapping=MAPPING_YAML,
        reference_years=[2025],
        dimension_values=["AT"],
        dry_run=True,
    )
    data = json.loads(result)
    view = data["views"]["cat.sch.fact_sc"]
    assert view["dry_run"] is True
    assert "SELECT" in view["ucmv_sql"]
    # pbi_dax["direct"] is a LIST of queries (one per extra_filter group).
    assert isinstance(view["pbi_dax"]["direct"], list)
    assert any("SUMMARIZECOLUMNS" in q for q in view["pbi_dax"]["direct"])
    assert set(view["measures"]) == {"sweetener_yield_bp", "pet_yield_bp"}


# Mapping with two raw_column measures on the SAME column but DIFFERENT
# extra_filters (the actual_value/budget_value shape) — they must NOT share one
# query, or both would collapse to the same unfiltered value.
MAPPING_TWO_FILTERS = """
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
- ucmv_measure: actual_value
  pbi_kind: raw_column
  pbi_column: Fact_SC[value]
  extra_filter:
  - table: Fact_SC
    column: bic_chversion
    values: ['0000']
- ucmv_measure: budget_value
  pbi_kind: raw_column
  pbi_column: Fact_SC[value]
  extra_filter:
  - table: Fact_SC
    column: bic_chversion
    values: ['B000']
"""


def test_dry_run_splits_distinct_extra_filters_into_separate_queries():
    """Regression: actual_value ('0000') and budget_value ('B000') must land in
    SEPARATE direct queries, each carrying its own TREATAS filter — never one
    filterless query that collapses both to the unfiltered total."""
    tool = UCMVReconciliationTool()
    data = json.loads(
        tool._run(
            mapping=MAPPING_TWO_FILTERS,
            reference_years=[2025],
            dimension_values=["AT"],
            dry_run=True,
        )
    )
    direct = data["views"]["cat.sch.fact_sc"]["pbi_dax"]["direct"]
    assert len(direct) == 2  # one query per distinct extra_filter
    joined = "\n".join(direct)
    assert "0000" in joined and "B000" in joined
    # each query filters its own version and neither mixes the two
    q0000 = next(q for q in direct if "0000" in q)
    qb000 = next(q for q in direct if "B000" in q)
    assert "B000" not in q0000
    assert "0000" not in qb000
    assert "bic_chversion" in q0000


def test_missing_mapping_errors():
    tool = UCMVReconciliationTool()
    data = json.loads(tool._run(reference_years=[2025]))
    assert "error" in data


def test_missing_warehouse_errors(monkeypatch):
    tool = UCMVReconciliationTool()
    # Non-dry-run without warehouse_id -> per-view error.
    data = json.loads(
        tool._run(mapping=MAPPING_YAML, reference_years=[2025], dimension_values=["AT"])
    )
    assert data["views"]["cat.sch.fact_sc"]["error"] == "warehouse_id is required"


def test_reconcile_with_mocked_executors(monkeypatch):
    ucmv_df = pd.DataFrame(
        [
            {
                "country": "AT",
                "period": "2025001",
                "sweetener_yield_bp": 0.997,
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
    # PBI: sweetener matches both periods; pet mismatches period 2025002.
    pbi_df = pd.DataFrame(
        [
            {
                "C_Dim_Geography[country]": "AT",
                "C_Dim_calendar[fiscper]": "2025001",
                "sweetener_yield_bp": 0.997,
                "pet_yield_bp": 1.0,
            },
            {
                "C_Dim_Geography[country]": "AT",
                "C_Dim_calendar[fiscper]": "2025002",
                "sweetener_yield_bp": 0.5,
                "pet_yield_bp": 9.0,
            },
        ]
    )

    tool = UCMVReconciliationTool()
    monkeypatch.setattr(tool, "_build_sql_executor", lambda *a, **k: _FakeSQL(ucmv_df))
    monkeypatch.setattr(tool, "_build_pbi_executor", lambda *a, **k: _FakePBI(pbi_df))

    result = tool._run(
        mapping=MAPPING_YAML,
        warehouse_id="wh-1",
        reference_years=[2025],
        dimension_values=["AT"],
    )
    data = json.loads(result)
    view = data["views"]["cat.sch.fact_sc"]

    assert view["measures"]["sweetener_yield_bp"]["pct_aligned"] == 100.0
    assert view["measures"]["pet_yield_bp"]["pct_aligned"] == 50.0
    assert view["measures"]["pet_yield_bp"]["cells_total"] == 2
    assert view["measures"]["pet_yield_bp"]["cells_aligned"] == 1
    # A mismatch is sampled for the failing measure.
    samples = view["measures"]["pet_yield_bp"]["sample_mismatches"]
    assert len(samples) == 1
    assert samples[0]["period"] == "2025002"
    assert samples[0]["pbi_value"] == 9.0
    # Classification present.
    assert "classification" in view["measures"]["pet_yield_bp"]

    assert view["summary"]["measures_at_100"] == 1
    assert view["summary"]["total_measures"] == 2
    assert data["overall"]["measures_at_100"] == 1
    assert data["overall"]["total_measures"] == 2
    assert 0.0 <= data["overall"]["overall_cell_pct"] <= 100.0


def test_dimension_discovery_when_values_omitted(monkeypatch):
    ucmv_df = pd.DataFrame(
        [
            {
                "country": "AT",
                "period": "2025001",
                "sweetener_yield_bp": 1.0,
                "pet_yield_bp": 1.0,
            }
        ]
    )

    class _DiscoveringPBI:
        def execute(self, model_id, dax):
            if "DISTINCT" in dax:  # the scope-discovery query
                return pd.DataFrame({"C_Dim_Geography[country]": ["AT"]}), None
            return (
                pd.DataFrame(
                    [
                        {
                            "C_Dim_Geography[country]": "AT",
                            "C_Dim_calendar[fiscper]": "2025001",
                            "sweetener_yield_bp": 1.0,
                            "pet_yield_bp": 1.0,
                        }
                    ]
                ),
                None,
            )

    tool = UCMVReconciliationTool()
    monkeypatch.setattr(tool, "_build_sql_executor", lambda *a, **k: _FakeSQL(ucmv_df))
    monkeypatch.setattr(tool, "_build_pbi_executor", lambda *a, **k: _DiscoveringPBI())

    data = json.loads(
        tool._run(mapping=MAPPING_YAML, warehouse_id="wh-1", reference_years=[2025])
    )
    view = data["views"]["cat.sch.fact_sc"]
    assert view["measures"]["sweetener_yield_bp"]["pct_aligned"] == 100.0


def test_multi_view_via_mappings_json(monkeypatch):
    import yaml

    mp = yaml.safe_load(MAPPING_YAML)
    mappings_json = json.dumps({"view_a": mp, "view_b": mp})

    ucmv_df = pd.DataFrame(
        [
            {
                "country": "AT",
                "period": "2025001",
                "sweetener_yield_bp": 1.0,
                "pet_yield_bp": 1.0,
            }
        ]
    )
    pbi_df = pd.DataFrame(
        [
            {
                "C_Dim_Geography[country]": "AT",
                "C_Dim_calendar[fiscper]": "2025001",
                "sweetener_yield_bp": 1.0,
                "pet_yield_bp": 1.0,
            }
        ]
    )

    tool = UCMVReconciliationTool()
    monkeypatch.setattr(tool, "_build_sql_executor", lambda *a, **k: _FakeSQL(ucmv_df))
    monkeypatch.setattr(tool, "_build_pbi_executor", lambda *a, **k: _FakePBI(pbi_df))

    data = json.loads(
        tool._run(
            mappings_json=mappings_json,
            warehouse_id="wh-1",
            reference_years=[2025],
            dimension_values=["AT"],
        )
    )
    assert set(data["views"]) == {"view_a", "view_b"}
    assert data["overall"]["views_scored"] == 2


def test_config_injected_auth_not_in_schema():
    """PBI credentials are constructor-injected, never LLM-fillable."""
    tool = UCMVReconciliationTool(pbi_access_token="secret", pbi_workspace_id="ws-x")
    props = tool.args_schema.model_json_schema().get("properties", {})
    assert "pbi_access_token" not in props
    assert "pbi_client_secret" not in props
    assert tool._default_config["pbi_access_token"] == "secret"
