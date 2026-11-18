"""Deploy healing in the verify-numbers loop: remove exactly what Databricks names, redeploy, compare
the rest, and hand removed measures back to the DAX LLM."""

import json

import yaml

from src.services.tools.metric_view_utils.reconciliation.loop import (
    feedback_to_prompt_map,
)
from src.services.tools.ucmv_iterative_recon import run_iterative_ucmv_generation
from src.services.tools.ucmv_recon_heal import heal_failed_views, heal_view

YAML = """version: '1.1'
source: prod.md.fact
joins:
  - name: dim_calendar
    source: prod.md.cal
    "on": source.date_key = dim_calendar.date_id
  - name: dim_geo
    source: prod.md.geo
    "on": source.comp_code = dim_geo.comp_code
dimensions:
  - name: month
    expr: dim_calendar.month
  - name: country
    expr: dim_geo.country
  - name: fiscper
    expr: source.fiscper
measures:
  - name: volume
    expr: SUM(source.volume)
  - name: fte
    expr: SUM(source.fte)
  - name: dif_actual
    expr: SUM(source.a) = 1
"""


def _names(text, key):
    return [i["name"] for i in yaml.safe_load(text).get(key) or []]


def test_a_named_measure_is_removed():
    text, removed = heal_view(
        YAML,
        "[PARSE_SYNTAX_ERROR] Syntax error at or near '='. == SQL of METRIC VIEW x [measures.`dif_actual`.expr]",
    )
    assert _names(text, "measures") == ["volume", "fte"] and removed == [
        "measure dif_actual"
    ]


def test_an_unresolved_fact_column_removes_the_items_that_read_it():
    text, removed = heal_view(
        YAML,
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `source`.`fte` cannot be resolved.",
    )
    assert "fte" not in _names(text, "measures") and removed == ["measure fte"]


def test_an_unresolved_join_key_removes_the_join_and_what_reads_it():
    text, removed = heal_view(
        YAML,
        "with name `__auto_generated_subquery_name_source`.`date_key` cannot be resolved",
    )
    assert _names(text, "joins") == ["dim_geo"]
    assert "month" not in _names(text, "dimensions") and "fiscper" in _names(
        text, "dimensions"
    )
    assert removed[:2] == ["join dim_calendar", "dimension month"]


def test_a_missing_struct_field_and_a_missing_join_table():
    text, removed = heal_view(
        YAML, "[FIELD_NOT_FOUND] No such struct field `month` in `calday`, `fiscper`."
    )
    assert removed == ["dimension month"]
    text, removed = heal_view(
        YAML,
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `prod`.`md`.`geo` cannot be found.",
    )
    assert "join dim_geo" in removed and "country" not in _names(text, "dimensions")


def test_nothing_is_guessed():
    assert heal_view(
        YAML,
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `prod`.`md`.`fact` cannot be found.",
    ) == (None, [])
    assert heal_view(YAML, "[INTERNAL_ERROR] something else") == (None, [])


def test_errors_are_healed_in_rounds_until_the_view_deploys():
    errors = iter(
        [
            "with name `source`.`fte` cannot be resolved",
            "[measures.`dif_actual`.expr] syntax error",
        ]
    )

    def redeploy(batch):
        try:
            return {"deployed_views": [], "failures": {k: next(errors) for k in batch}}
        except StopIteration:
            return {"deployed_views": list(batch), "failures": {}}

    verdict = {
        "ok": False,
        "error": "no view deployed",
        "failures": {"Fact": "[measures.`dif_actual`.expr] x"},
    }
    verdict["failures"][
        "Fact"
    ] = "with name `__auto_generated_subquery_name_source`.`date_key` cannot be resolved"
    merged, healed_yaml, healed = heal_failed_views(redeploy, {"Fact": YAML}, verdict)
    assert merged["ok"] is True and merged["deployed_views"] == ["Fact"]
    assert [i.split(" (")[0] for i in healed["Fact"]][:1] == ["join dim_calendar"]
    assert any(i.startswith("measure fte") for i in healed["Fact"])
    assert _names(healed_yaml["Fact"], "measures") == ["volume"]


def test_the_loop_compares_the_healed_view_and_feeds_the_removed_measure_back():
    feedback_seen = []

    class _Generator:
        def _run(self, **kwargs):
            feedback_seen.append(kwargs.get("refinement_feedback"))
            return json.dumps(
                {
                    "yaml": {"Fact": YAML},
                    "resolved_measures_by_table": {
                        "Fact": [
                            {
                                "measure_name": "fte",
                                "original_name": "FTE",
                                "dax_expression": "SUM(x)",
                            },
                            {
                                "measure_name": "volume",
                                "original_name": "Volume",
                                "dax_expression": "SUM(v)",
                            },
                        ]
                    },
                }
            )

    class _Deployer:
        def _run(self, yaml_specs_json, **kwargs):
            specs = json.loads(yaml_specs_json)
            results = {}
            for key, text in specs.items():
                bad = "fte" in _names(text, "measures")
                results[key] = (
                    {
                        "status": "error",
                        "message": "with name `source`.`fte` cannot be resolved",
                    }
                    if bad
                    else {"status": "deployed"}
                )
            return json.dumps({"deployment_results": results})

    class _Reconciler:
        def __init__(self):
            self.n = 0
            self.mappings = []

        def _run(self, **kwargs):
            self.n += 1
            self.mappings.append(json.loads(kwargs["mappings_json"]))
            return json.dumps(
                {
                    "views": {"Fact": {"measures": {"volume": {"pct_aligned": 50.0}}}},
                    "overall": {"cells_total": 2, "overall_cell_pct": 50.0 + self.n},
                }
            )

    recon = _Reconciler()
    out = run_iterative_ucmv_generation(
        generator=_Generator(),
        deployer=_Deployer(),
        reconciler=recon,
        gen_kwargs={
            "dataset_id": "ds",
            "workspace_id": "ws",
            "config_json": json.dumps({"join_key_map": {"Geo": {"alias": "dim_geo"}}}),
        },
        deploy_kwargs={"catalog": "c", "schema_name": "s", "warehouse_id": "wh"},
        max_cycles=2,
    )
    cycle = out["cycles"][0]
    assert cycle["deploy_ok"] is True
    assert cycle["deploy_healed"]["Fact"][0].startswith("measure fte")
    assert [m["ucmv_measure"] for m in recon.mappings[0]["Fact"]["measures"]] == [
        "volume"
    ]
    second = json.loads(feedback_seen[1])
    assert any(
        item.get("measure") == "fte" and item.get("deploy_error") for item in second
    )
    assert "failed to deploy" in feedback_to_prompt_map(second)["fte"]


def test_a_missing_struct_field_drops_every_reference_outside_the_listed_fields():
    text = YAML.replace(
        "  - name: country\n    expr: dim_geo.country\n",
        "  - name: country\n    expr: dim_geo.country\n  - name: bu\n    expr: dim_geo.business_unit\n"
        "  - name: code\n    expr: dim_geo.comp_code\n",
    )
    healed, removed = heal_view(
        text,
        "[FIELD_NOT_FOUND] No such struct field `country` in `region`, `co_code_bw`, `comp_code`.",
    )
    assert sorted(removed) == [
        "dimension bu",
        "dimension country",
    ]  # one round, not two
    assert "code" in _names(healed, "dimensions")


def test_a_healed_view_without_a_new_mapping_keeps_no_stale_one():
    from src.services.tools.metric_view_utils.reconciliation.loop import (
        run_reconciliation_loop,
    )

    seen = []
    run_reconciliation_loop(
        generate=lambda fb: {
            "yaml": {"A": "a", "B": "b"},
            "mappings": {"A": "old-a", "B": "old-b"},
        },
        deploy=lambda y: {
            "ok": True,
            "deployed_views": ["A", "B"],
            "yaml_overrides": {"A": "healed-a", "B": "healed-b"},
            "mapping_overrides": {"A": "new-a"},
        },
        reconcile=lambda y, m: seen.append(dict(m))
        or {"overall": {"cells_total": 1, "overall_cell_pct": 100.0}},
        max_cycles=1,
    )
    assert seen == [{"A": "new-a"}]


def test_the_period_survives_a_dropped_calendar_join():
    text = YAML.replace(
        "  - name: fiscper\n    expr: source.fiscper\n",
        "  - name: fiscper\n    expr: dim_calendar.fiscper\n",
    )
    healed, removed = heal_view(
        text,
        "with name `__auto_generated_subquery_name_source`.`date_key` cannot be resolved",
    )
    dims = {d["name"]: d["expr"] for d in yaml.safe_load(healed)["dimensions"]}
    assert dims["fiscper"] == "source.fiscper" and "month" not in dims
    assert "dimension fiscper now reads source.fiscper" in removed
