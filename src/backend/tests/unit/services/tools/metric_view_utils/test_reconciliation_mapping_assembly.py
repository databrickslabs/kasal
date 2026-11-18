"""Reconciliation mappings assembled for the views a verify-numbers cycle deployed: the generator's
own mapping files are drafts the reconciliation cannot load (no time dimension, TODO fact table,
another view name)."""

import json

from src.services.tools.metric_view_utils.reconciliation.mapping import (
    load_mapping_dict,
)
from src.services.tools.metric_view_utils.reconciliation.mapping_assembly import (
    assemble_mappings,
)
from src.services.tools.ucmv_iterative_recon import run_iterative_ucmv_generation

YAML = """version: '1.1'
source: prod.fm.co012
joins:
  - name: dim_calendar
    source: prod.md.ca_dim_calendar
    "on": source.fiscper = dim_calendar.fiscper
  - name: dim_geography
    source: prod.md.ca_dim_company
    "on": source.comp_code = dim_geography.comp_code
dimensions:
  - name: market_segment
    expr: dim_geography.market_segment
  - name: comp_code
    expr: dim_geography.comp_code
  - name: fiscper
    expr: dim_calendar.fiscper
measures:
  - name: volume
    expr: SUM(source.volume)
  - name: sc_people_cost
    expr: SUM(source.cost) FILTER (WHERE source.kind = 'people')
"""
RESOLVED = {
    "Fact_CO012": [
        {"measure_name": "volume", "original_name": "volume", "dax_expression": ""},
        {
            "measure_name": "sc_people_cost",
            "original_name": "SC People Cost",
            "dax_expression": "CALCULATE(...)",
        },
        {
            "measure_name": "not_in_view",
            "original_name": "Gone",
            "dax_expression": "SUM(x)",
        },
    ]
}
JOIN_KEY_MAP = {
    "C_Dim_calendar": {"alias": "dim_calendar"},
    "C_Dim_Geography": {"alias": "dim_geography"},
}
MODEL = {"dataset_id": "ds", "workspace_id": "ws"}


def test_a_deployed_view_gets_a_loadable_mapping_with_its_translated_measures():
    mappings, gaps = assemble_mappings(
        {"Fact_CO012": YAML},
        RESOLVED,
        {"Fact_CO012": "acme-dev.sb.fact_co012"},
        JOIN_KEY_MAP,
        MODEL,
    )
    assert gaps == {}
    mapping = load_mapping_dict(mappings["Fact_CO012"])
    assert mapping.binding.ucmv_table == "acme-dev.sb.fact_co012"
    assert mapping.binding.pbi_fact_table == "Fact_CO012"
    assert [(m.ucmv_measure, m.pbi_measure) for m in mapping.measures] == [
        ("sc_people_cost", "SC People Cost")
    ]
    td = mapping.binding.time_dimension
    assert (td.ucmv_column, td.pbi_column, td.pbi_table) == (
        "fiscper",
        "fiscper",
        "C_Dim_calendar",
    )
    assert (
        mapping.binding.default_dimension == "comp_code"
    )  # preferred over market_segment
    assert mapping.binding.dimensions["comp_code"].pbi_dim_table == "C_Dim_Geography"


def test_views_that_cannot_be_compared_are_named_with_the_reason():
    no_period = YAML.replace("  - name: fiscper\n    expr: dim_calendar.fiscper\n", "")
    only_base = {
        "Fact_CO012": [
            {"measure_name": "volume", "original_name": "volume", "dax_expression": ""}
        ]
    }
    _, gaps = assemble_mappings(
        {"Fact_CO012": no_period},
        RESOLVED,
        {"Fact_CO012": "c.s.v"},
        JOIN_KEY_MAP,
        MODEL,
    )
    assert "no period dimension" in gaps["Fact_CO012"]
    _, gaps = assemble_mappings(
        {"Fact_CO012": YAML}, only_base, {"Fact_CO012": "c.s.v"}, JOIN_KEY_MAP, MODEL
    )
    assert "no DAX-translated measure" in gaps["Fact_CO012"]


def test_the_loop_reconciles_with_the_assembled_mapping_bound_to_the_deployed_view():
    class _Generator:
        def _run(self, **kwargs):
            return json.dumps(
                {
                    "yaml": {"Fact_CO012": YAML, "Fact_Base": YAML},
                    "pbi_ucmv_mapping": {
                        "mv_co012": "binding:\n  ucmv: mv_co012\n  pbi_fact_table: TODO\n"
                    },
                    "resolved_measures_by_table": {
                        **RESOLVED,
                        "Fact_Base": [
                            {"measure_name": "volume", "original_name": "volume"}
                        ],
                    },
                }
            )

    class _Deployer:
        def _run(self, **kwargs):
            return json.dumps(
                {
                    "deployment_results": {
                        "Fact_CO012": {"status": "deployed"},
                        "Fact_Base": {"status": "deployed"},
                    }
                }
            )

    class _Reconciler:
        seen = []

        def _run(self, **kwargs):
            self.seen.append(json.loads(kwargs["mappings_json"]))
            return json.dumps(
                {"views": {}, "overall": {"cells_total": 2, "overall_cell_pct": 100.0}}
            )

    recon = _Reconciler()
    out = run_iterative_ucmv_generation(
        generator=_Generator(),
        deployer=_Deployer(),
        reconciler=recon,
        gen_kwargs={"config_json": json.dumps({"join_key_map": JOIN_KEY_MAP}), **MODEL},
        deploy_kwargs={
            "catalog": "acme-dev",
            "schema_name": "sb",
            "warehouse_id": "wh",
        },
        max_cycles=1,
    )
    (mappings,) = recon.seen
    assert list(mappings) == ["Fact_CO012"]
    assert mappings["Fact_CO012"]["binding"]["ucmv_table"] == "acme-dev.sb.fact_co012"
    assert (
        "no DAX-translated measure"
        in out["cycles"][0]["deploy_failures"]["mapping:Fact_Base"]
    )
