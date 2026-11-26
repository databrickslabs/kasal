"""Join sources of generated metric views that Databricks rejected on a production Power BI model:
joins to Power BI's internal auto date tables, two-part ``schema.table`` joins (resolved against the
metric view's own catalog at deploy, not the source's), and a hyphenated catalog written bare.
"""

import yaml

from src.services.tools.metric_view_deployer_tool import (
    MetricViewDeployerTool,
    _RunOptions,
)
from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline
from src.services.tools.metric_view_utils.yaml_emitter import emit_yaml


def _measure(name="amount", expr="SUM(source.amount)"):
    return TranslationResult(
        measure_name=name,
        original_name=name,
        sql_expr=expr,
        is_translatable=True,
        skip_reason="",
        dax_expression="",
        confidence="high",
        category="base",
    )


def _spec(source="prod.sales.fact", joins=None, dims=None, measures=None):
    return MetricViewSpec(
        fact_table_key="Fact",
        source_table=source,
        view_name="fact",
        comment="c",
        joins=joins or [],
        dimensions=dims or [],
        measures=measures or [_measure()],
        untranslatable=[],
    )


def _emitted(spec):
    return yaml.safe_load(emit_yaml(spec))


def test_a_join_to_a_power_bi_auto_date_table_is_dropped_with_its_dimension():
    spec = _spec(
        joins=[
            {
                "name": "local_date",
                "source": "acme.sch.local_date_table_cbb50d17_bad7_422a",
                "join_on": "source.d = local_date.Date",
            },
            {
                "name": "dim_geo",
                "source": "prod.md.dim_geo",
                "join_on": "source.k = dim_geo.k",
            },
        ],
        dims=[
            {"name": "year", "expr": "local_date.Year"},
            {"name": "geo", "expr": "dim_geo.name"},
        ],
        measures=[
            _measure(),
            _measure("by_geo", "SUM(source.amount) FILTER (WHERE dim_geo.name <> '')"),
        ],
    )
    out = _emitted(spec)
    assert [j["name"] for j in out["joins"]] == ["dim_geo"]
    assert all("local_date" not in d["expr"] for d in out.get("dimensions") or [])


def test_two_part_join_sources_get_the_source_catalog():
    spec = _spec(
        joins=[
            {
                "name": "dim_company",
                "source": "md.ca_dim_company",
                "join_on": "source.c = dim_company.c",
            },
            {
                "name": "dim_plant",
                "source": "prod.md.dim_plant",
                "join_on": "source.p = dim_plant.p",
            },
            {
                "name": "inline",
                "source": "(SELECT 1 AS k)",
                "join_on": "source.k = inline.k",
            },
        ]
    )
    MetricViewPipeline._qualify_source_catalog(spec, "prod")
    assert [j["source"] for j in spec.joins] == [
        "prod.md.ca_dim_company",
        "prod.md.dim_plant",
        "(SELECT 1 AS k)",
    ]


def test_a_hyphenated_catalog_is_back_quoted_in_the_yaml():
    spec = _spec(
        source="acme-dev.sch.fact",
        joins=[
            {
                "name": "dim_geo",
                "source": "acme-dev.sch.dim_geo",
                "join_on": "source.k = dim_geo.k",
            }
        ],
        dims=[{"name": "geo", "expr": "dim_geo.name"}],
        measures=[
            _measure(),
            _measure(
                "geo_amount", "SUM(source.amount) FILTER (WHERE dim_geo.name <> '')"
            ),
        ],
    )
    out = _emitted(spec)
    assert out["source"] == "`acme-dev`.sch.fact"
    assert out["joins"][0]["source"] == "`acme-dev`.sch.dim_geo"
    assert _emitted(_spec())["source"] == "prod.sales.fact"  # plain names unchanged


def test_a_view_with_a_todo_source_is_skipped_not_deployed():
    tool = MetricViewDeployerTool()
    opts = _RunOptions(
        catalog="c",
        schema="s",
        host_override=None,
        warehouse_id="wh",
        preserve_grants=True,
        group_by=None,
        timing=False,
        slow_s=5.0,
    )
    result = tool._deploy_one(
        "none_allocated", "version: '1.1'\nsource: TODO.none_allocated.x\n", opts, None
    )
    assert result["status"] == "skipped" and "TODO" in result["message"]
