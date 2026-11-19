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
    assert [(m.ucmv_measure, m.pbi_kind) for m in mapping.measures] == [
        ("sc_people_cost", "direct"),
        ("volume", "raw_column"),  # SUM(source.volume): Power BI sums the same column
    ]
    assert mapping.measures[0].pbi_measure == "SC People Cost"
    assert (mapping.measures[1].pbi_column, mapping.measures[1].pbi_aggregation) == (
        "Fact_CO012[volume]",
        "Sum",
    )
    td = mapping.binding.time_dimension
    # the fact's own join key: it filters the fact whatever the model's relationships
    assert (td.ucmv_column, td.pbi_column) == ("fiscper", "fiscper")
    assert (
        mapping.binding.default_dimension == "comp_code"
    )  # preferred over market_segment
    assert mapping.binding.dimensions["comp_code"].pbi_dim_table == "C_Dim_Geography"


def test_views_that_cannot_be_compared_are_named_with_the_reason():
    no_period = YAML.replace("  - name: fiscper\n    expr: dim_calendar.fiscper\n", "")
    _, gaps = assemble_mappings(
        {"Fact_CO012": no_period},
        RESOLVED,
        {"Fact_CO012": "c.s.v"},
        JOIN_KEY_MAP,
        MODEL,
    )
    assert "no period dimension" in gaps["Fact_CO012"]
    complex_only = YAML.replace(
        "SUM(source.volume)", "SUM(source.a) / NULLIF(SUM(source.b), 0)"
    )
    _, gaps = assemble_mappings(
        {"Fact_CO012": complex_only}, {}, {"Fact_CO012": "c.s.v"}, JOIN_KEY_MAP, MODEL
    )
    assert "no measure to compare" in gaps["Fact_CO012"]


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
    # Fact_Base has only a column aggregate: compared too now, as raw_column
    assert sorted(mappings) == ["Fact_Base", "Fact_CO012"]
    assert mappings["Fact_CO012"]["binding"]["ucmv_table"] == "acme-dev.sb.fact_co012"
    assert [m["pbi_kind"] for m in mappings["Fact_Base"]["measures"]] == ["raw_column"]
    assert "mapping:Fact_Base" not in out["cycles"][0]["deploy_failures"]


def test_crew_2b_output_is_complete_for_crew_4():
    """Crew 2b returned only yaml and the reconciliation; the workbench (crew 4) also needs the
    mappings to compare and the untranslated measures to list the gaps."""
    from src.services.tools.ucmv_workbench_utils.state import Workbench

    class _Generator:
        def _run(self, **kwargs):
            return json.dumps(
                {
                    "yaml": {"Fact_CO012": YAML},
                    "resolved_measures_by_table": RESOLVED,
                    "untranslatable_items": [
                        {
                            "table_key": "Fact_CO012",
                            "original_name": "Margin %",
                            "dax_expression": "DIVIDE(1, 2)",
                            "skip_reason": "x",
                        }
                    ],
                    "source_layer_ddl": {
                        "Fact_CO012": {"ddl": "CREATE VIEW x AS SELECT 1"}
                    },
                }
            )

    out = run_iterative_ucmv_generation(
        generator=_Generator(),
        deployer=type(
            "D",
            (),
            {
                "_run": lambda self, **k: json.dumps(
                    {"deployment_results": {"Fact_CO012": {"status": "deployed"}}}
                )
            },
        )(),
        reconciler=type(
            "R",
            (),
            {
                "_run": lambda self, **k: json.dumps(
                    {
                        "views": {
                            "Fact_CO012": {
                                "summary": {
                                    "cells_total": 4,
                                    "cells_aligned": 4,
                                    "overall_cell_pct": 100.0,
                                }
                            }
                        },
                        "overall": {"cells_total": 4, "overall_cell_pct": 100.0},
                    }
                )
            },
        )(),
        gen_kwargs={"config_json": json.dumps({"join_key_map": JOIN_KEY_MAP}), **MODEL},
        deploy_kwargs={
            "catalog": "acme-dev",
            "schema_name": "sb",
            "warehouse_id": "wh",
        },
        max_cycles=1,
    )
    assert (
        out["pbi_ucmv_mapping"]["Fact_CO012"]["binding"]["ucmv_table"]
        == "acme-dev.sb.fact_co012"
    )
    assert (
        out["untranslatable_items"][0]["original_name"] == "Margin %"
        and "source_layer_ddl" in out
    )
    bench = Workbench.from_generator_output(out)
    view = bench.view("Fact_CO012")
    assert (
        view.mapping["binding"]["pbi_fact_table"] == "Fact_CO012"
        and len(view.untranslatable) == 1
    )


def test_base_measure_shapes_are_compared_as_power_bi_column_aggregates():
    from src.services.tools.metric_view_utils.reconciliation.mapping_assembly import (
        _base_measures,
    )

    spec = {
        "measures": [
            {"name": "a", "expr": "SUM(COALESCE(source.qty, 0))"},
            {"name": "b", "expr": "COUNT(DISTINCT source.order_id)"},
            {"name": "c", "expr": "AVG(source.price)"},
            {
                "name": "d",
                "expr": "SUM(source.x) / SUM(source.y)",
            },  # not one column: not a base measure
            {"name": "e", "expr": "SUM(source.z)"},
        ]
    }
    out = {m["ucmv_measure"]: m for m in _base_measures(spec, "Fact", taken={"e"})}
    assert sorted(out) == ["a", "b", "c"]
    assert (out["a"]["pbi_aggregation"], out["a"].get("null_equals_zero")) == (
        "Sum",
        True,
    )
    assert (
        out["b"]["pbi_aggregation"] == "DistinctCount"
        and out["c"]["pbi_aggregation"] == "Average"
    )
    assert out["a"]["pbi_column"] == "Fact[qty]"


def test_a_joined_period_is_compared_on_the_facts_own_join_key():
    """A period read from a joined calendar (dim_calendar.fiscper) was compared on that table in
    Power BI. A calendar with no active relationship to the fact does not filter it: Power BI
    returned the same total for every period and almost every cell differed."""
    mappings, _ = assemble_mappings(
        {"Fact_CO012": YAML},
        RESOLVED,
        {"Fact_CO012": "c.s.v"},
        JOIN_KEY_MAP,
        MODEL,
    )
    td = mappings["Fact_CO012"]["time_dimension"]
    assert td["pbi_column"] == "fiscper" and "pbi_table" not in td
    other_key = YAML.replace(
        "source.fiscper = dim_calendar.fiscper",
        "source.period_key = dim_calendar.fiscper",
    )
    mappings, _ = assemble_mappings(
        {"Fact_CO012": other_key},
        RESOLVED,
        {"Fact_CO012": "c.s.v"},
        JOIN_KEY_MAP,
        MODEL,
    )
    assert mappings["Fact_CO012"]["time_dimension"]["pbi_column"] == "period_key"


def test_a_view_is_not_compared_by_a_fact_column_its_measures_filter_on():
    """A DAX measure that sets Fact[version] = "0000" returns its value on every version row;
    the metric view's FILTER returns it on that row only. Compared by version, every such
    measure differed on the other rows although it was translated correctly."""
    text = """version: '1.1'
source: prod.fm.pl
joins:
  - name: dim_scenario
    source: prod.md.scenario
    "on": source.version = dim_scenario.key
dimensions:
  - name: version
    expr: source.version
  - name: scenario
    expr: dim_scenario.label
  - name: fiscper
    expr: source.fiscper
measures:
  - name: cost_actuals
    expr: SUM(source.value) FILTER (WHERE source.version = '0000' AND kind IN ('a'))
"""
    resolved = {
        "Fact_PL": [
            {
                "measure_name": "cost_actuals",
                "original_name": "Cost",
                "dax_expression": "x",
            }
        ]
    }
    mappings, _ = assemble_mappings(
        {"Fact_PL": text},
        resolved,
        {"Fact_PL": "c.s.v"},
        {"Scenario": {"alias": "dim_scenario"}},
        MODEL,
    )
    assert mappings["Fact_PL"]["binding"]["default_dimension"] == "scenario"
    assert mappings["Fact_PL"]["dimensions"]["scenario"]["pbi_dim_table"] == "Scenario"


RELATIONSHIPS = [
    # the fact's own relationship to the geography is inactive; the plant one is active
    {
        "from_table": "Fact_CO012",
        "from_column": "comp_code",
        "to_table": "C_Dim_Geography",
        "to_column": "comp_code",
        "is_active": False,
    },
    {
        "from_table": "Fact_CO012",
        "from_column": "plant",
        "to_table": "Dim_Plant",
        "to_column": "plant",
        "is_active": True,
    },
    {
        "from_table": "Dim_Plant",
        "from_column": "region",
        "to_table": "Dim_Region",
        "to_column": "region",
        "is_active": True,
    },
]


def test_a_table_that_does_not_filter_the_fact_in_power_bi_is_not_grouped_by():
    """A join built from an inactive relationship (or from none: a selector table) gave a
    comparison by a table Power BI does not filter the fact with: the same total on every
    member, and a view at 25 % although its numbers were right."""
    from src.services.tools.metric_view_utils.reconciliation.mapping_assembly import (
        fact_filters,
    )

    filters = fact_filters("Fact_CO012", RELATIONSHIPS)
    assert filters is not None
    assert filters.reach("Dim_Plant") and filters.reach("Dim_Region")  # across two hops
    assert not filters.reach("C_Dim_Geography") and not filters.reach("C_Dim_calendar")

    mappings, _ = assemble_mappings(
        {"Fact_CO012": YAML},
        RESOLVED,
        {"Fact_CO012": "c.s.v"},
        JOIN_KEY_MAP,
        MODEL,
        relationships=RELATIONSHIPS,
    )
    mapping = mappings["Fact_CO012"]
    # comp_code is the join key: compared on the fact's own column, not on the geography
    assert mapping["dimensions"]["comp_code"] == {
        "ucmv_column": "comp_code",
        "pbi_dim_table": "Fact_CO012",
        "pbi_dim_column": "comp_code",
    }
    assert "pbi_table" not in mapping["time_dimension"]


def test_without_the_full_list_an_inactive_relationship_still_counts():
    mappings, _ = assemble_mappings(
        {"Fact_CO012": YAML},
        RESOLVED,
        {"Fact_CO012": "c.s.v"},
        JOIN_KEY_MAP,
        MODEL,
        inactive_relationships=[RELATIONSHIPS[0]],
    )
    assert (
        mappings["Fact_CO012"]["dimensions"]["comp_code"]["pbi_dim_table"]
        == "Fact_CO012"
    )
    untouched, _ = assemble_mappings(
        {"Fact_CO012": YAML}, RESOLVED, {"Fact_CO012": "c.s.v"}, JOIN_KEY_MAP, MODEL
    )
    assert untouched["Fact_CO012"]["dimensions"]["comp_code"]["pbi_dim_table"] == (
        "C_Dim_Geography"
    )
