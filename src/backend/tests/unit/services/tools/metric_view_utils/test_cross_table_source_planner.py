"""Phase 1 cross-table planner: same-grain UNION, strict gating, fail-open."""

from src.services.tools.metric_view_utils.cross_table_source_planner import (
    _build_union_source,
    _is_same_grain,
    plan_cross_table_sources,
)
from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TableInfo,
)


def _fact(name, source_table, grain, aggs):
    return TableInfo(
        table_name=name,
        source_table=source_table,
        aggregate_columns=[{"name": n, "source_col": s} for n, s in aggs],
        group_by_columns=list(grain),
        calculated_columns=[],
        is_fact=True,
        full_sql="",
    )


class _FakePipeline:
    """Minimal stand-in: records _process_table calls, returns a real spec."""

    def __init__(self, report, mquery_tables, mapping):
        self._allocation_report = report
        self.mquery_tables = mquery_tables
        self.mapping = mapping
        self.calls = []

    def _process_table(self, key, info, dax_measures):
        self.calls.append((key, info, dax_measures))
        return MetricViewSpec(
            fact_table_key=key,
            source_table=info.source_table,
            view_name=key,
            comment="",
            joins=[],
            dimensions=[],
            measures=[],
            untranslatable=[],
        )


def _report(*entries):
    return {"cross_fact": list(entries)}


def test_same_grain_bucket_builds_combined_union_spec():
    mq = {
        "FactSales": _fact(
            "FactSales", "cat.sch.sales", ["date", "region"], [("sales", "amt")]
        ),
        "FactTarget": _fact(
            "FactTarget", "cat.sch.target", ["date", "region"], [("target", "tgt")]
        ),
    }
    report = _report(
        {
            "measure": "attainment",
            "facts": ["FactSales", "FactTarget"],
            "shared_grain": ["date", "region"],
        }
    )
    pipe = _FakePipeline(
        report, mq, [{"measure_name": "attainment", "dax": "DIVIDE([sales],[target])"}]
    )

    specs, lims = plan_cross_table_sources(pipe)

    assert lims == []
    assert len(specs) == 1
    spec = next(iter(specs.values()))
    # routed through the real translator path
    assert pipe.calls, "should call _process_table"
    _k, info, dicts = pipe.calls[0]
    assert {a["name"] for a in info.aggregate_columns} == {"sales", "target"}
    assert [d["measure_name"] for d in dicts] == ["attainment"]
    # inline UNION source with NULL-alignment + discriminator
    sql = spec.source_sql
    assert "UNION ALL" in sql
    assert "amt AS sales" in sql and "NULL AS target" in sql  # FactSales arm
    assert "tgt AS target" in sql and "NULL AS sales" in sql  # FactTarget arm
    assert "'FactSales' AS _source_fact" in sql


def test_grain_mismatch_is_deferred_not_emitted():
    mq = {
        "FactA": _fact("FactA", "c.s.a", ["date", "region"], [("a", "a")]),
        "FactB": _fact("FactB", "c.s.b", ["date"], [("b", "b")]),  # coarser grain
    }
    report = _report(
        {"measure": "r", "facts": ["FactA", "FactB"], "shared_grain": ["date"]}
    )
    pipe = _FakePipeline(report, mq, [{"measure_name": "r", "dax": "x"}])
    specs, lims = plan_cross_table_sources(pipe)
    assert specs == {}
    assert lims and "grain mismatch" in lims[0]["reason"]


def test_three_fact_span_is_deferred():
    mq = {
        f: _fact(f, f"c.s.{f}", ["d"], [(f.lower(), f.lower())])
        for f in ("FactA", "FactB", "FactC")
    }
    report = _report(
        {"measure": "r", "facts": ["FactA", "FactB", "FactC"], "shared_grain": ["d"]}
    )
    pipe = _FakePipeline(report, mq, [{"measure_name": "r", "dax": "x"}])
    specs, lims = plan_cross_table_sources(pipe)
    assert specs == {}
    assert lims and "3+" in lims[0]["reason"]


def test_name_collision_across_facts_is_deferred():
    mq = {
        "FactA": _fact("FactA", "c.s.a", ["d"], [("amount", "a")]),
        "FactB": _fact("FactB", "c.s.b", ["d"], [("amount", "b")]),  # same output name
    }
    report = _report(
        {"measure": "r", "facts": ["FactA", "FactB"], "shared_grain": ["d"]}
    )
    pipe = _FakePipeline(report, mq, [{"measure_name": "r", "dax": "x"}])
    specs, lims = plan_cross_table_sources(pipe)
    assert specs == {}
    assert lims


def test_no_cross_fact_entries_is_noop():
    pipe = _FakePipeline({"cross_fact": []}, {}, [])
    assert plan_cross_table_sources(pipe) == ({}, [])


def test_is_same_grain_strictness():
    mq = {
        "A": _fact("A", "c.s.a", ["date", "region"], []),
        "B": _fact("B", "c.s.b", ["date", "region"], []),
    }
    assert _is_same_grain(["A", "B"], ["date", "region"], mq) is True
    assert _is_same_grain(["A", "B"], ["date"], mq) is False  # partial grain
    assert _is_same_grain(["A", "B"], [], mq) is False  # empty grain never same


def test_build_union_source_null_alignment():
    mq = {
        "A": _fact("A", "c.s.a", ["d"], [("sa", "x")]),
        "B": _fact("B", "c.s.b", ["d"], [("sb", "y")]),
    }
    sql, aggs = _build_union_source(["A", "B"], ["d"], mq)
    assert {a["name"] for a in aggs} == {"sa", "sb"}
    assert "x AS sa" in sql and "NULL AS sb" in sql
    assert "y AS sb" in sql and "NULL AS sa" in sql
