"""Indirect (backtraced) visual usage.

Direct usage (`used_in_visuals`) is stamped only on measures literally drawn or
filtered in a visual. `annotate_indirect_visual_usage` then propagates that DOWN
the measure-dependency graph: a sub-KPI referenced by a visual-placed KPI is
surfaced as `indirect_visual_usage` (kept separate from direct), so a slicer KPI
that depends on other KPIs no longer hides those dependencies.
"""

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.visual_usage_annotator import (
    annotate_indirect_visual_usage,
    prune_non_visual_measures,
)


def _measure(original_name: str, dax: str, used_in_visuals=None) -> TranslationResult:
    return TranslationResult(
        measure_name=original_name.lower().replace(" ", "_"),
        original_name=original_name,
        sql_expr="SUM(source.x)",
        is_translatable=True,
        skip_reason="",
        dax_expression=dax,
        confidence="high",
        category="single_table",
        used_in_visuals=list(used_in_visuals or []),
    )


def _spec(measures) -> MetricViewSpec:
    return MetricViewSpec(
        fact_table_key="fact_otc",
        source_table="cat.sch.fact_otc",
        view_name="mv_otc",
        comment="",
        joins=[],
        dimensions=[],
        measures=measures,
        untranslatable=[],
    )


class TestIndirectVisualUsage:
    def test_propagates_direct_usage_to_a_referenced_sub_kpi(self):
        # Health Score is on a slicer and references [Detractors] + [Promoters];
        # neither sub-KPI is on any visual itself.
        health = _measure(
            "OTC Health Score",
            "DIVIDE([Promoters], [Detractors])",
            used_in_visuals=[
                {"page": "OTC Scorecard", "visual_type": "slicer", "role": "filter"}
            ],
        )
        promoters = _measure("Promoters", "COUNTROWS(x)")
        detractors = _measure("Detractors", "COUNTROWS(y)")
        spec = _spec([health, promoters, detractors])

        n = annotate_indirect_visual_usage({"fact_otc": spec})

        assert n == 2  # both sub-KPIs gained indirect usage
        assert health.indirect_visual_usage == []  # the direct one is untouched
        for sub in (promoters, detractors):
            assert len(sub.indirect_visual_usage) == 1
            e = sub.indirect_visual_usage[0]
            assert e["page"] == "OTC Scorecard"
            assert e["via"] == "OTC Health Score"

    def test_transitive_chain_attributes_to_the_visual_placed_root(self):
        # A (on a visual) -> B -> C. Both B and C are indirectly used, via A.
        a = _measure(
            "A",
            "[B] + 1",
            used_in_visuals=[{"page": "P", "visual_type": "card", "role": "drawn"}],
        )
        b = _measure("B", "[C] * 2")
        c = _measure("C", "SUM(z)")
        spec = _spec([a, b, c])

        annotate_indirect_visual_usage({"fact_otc": spec})

        assert [e["via"] for e in b.indirect_visual_usage] == ["A"]
        assert [e["via"] for e in c.indirect_visual_usage] == ["A"]

    def test_cycle_is_safe(self):
        # A <-> B mutual reference, A on a visual. Must terminate, not loop.
        a = _measure(
            "A",
            "[B]",
            used_in_visuals=[{"page": "P", "visual_type": "card", "role": "drawn"}],
        )
        b = _measure("B", "[A]")
        spec = _spec([a, b])
        annotate_indirect_visual_usage({"fact_otc": spec})
        # B is reachable from A → indirect; A is only self-via, not indirect.
        assert any(e["via"] == "A" for e in b.indirect_visual_usage)

    def test_no_direct_usage_means_no_indirect(self):
        a = _measure("A", "[B]")
        b = _measure("B", "SUM(z)")
        spec = _spec([a, b])
        assert annotate_indirect_visual_usage({"fact_otc": spec}) == 0
        assert b.indirect_visual_usage == []


class TestPruneNonVisualMeasures:
    """Triage: measures not reachable from any visual (direct or via a dependency
    chain) are dropped; visual-placed, dependency-reachable, and base measures
    stay."""

    def _base(self, name):
        return TranslationResult(
            measure_name=name,
            original_name=name,
            sql_expr=f"SUM(source.{name})",
            is_translatable=True,
            skip_reason="",
            dax_expression="",
            confidence="high",
            category="base",
        )

    def test_prunes_only_measures_with_no_direct_or_indirect_usage(self):
        visible = _measure(
            "KPI", "[Sub]", used_in_visuals=[{"page": "P", "role": "drawn"}]
        )
        sub = _measure("Sub", "SUM(x)")  # reached transitively by KPI
        orphan = _measure("Legacy_PY", "SUM(y)")  # used nowhere
        base = self._base("cases")  # implicit column measure — always kept
        spec = _spec([visible, sub, orphan, base])
        specs = {"fact_otc": spec}

        annotate_indirect_visual_usage(specs)  # stamps `sub` via KPI
        removed = prune_non_visual_measures(specs)

        names = {m.original_name for m in spec.measures}
        assert removed == 1
        assert names == {"KPI", "Sub", "cases"}  # orphan pruned, base kept

    def test_prunes_untranslatable_bucket_too(self):
        spec = _spec([_measure("KPI", "SUM(x)", used_in_visuals=[{"page": "P"}])])
        spec.untranslatable = [_measure("Dead_Legacy", "SOME(dax)")]
        specs = {"f": spec}
        annotate_indirect_visual_usage(specs)
        removed = prune_non_visual_measures(specs)
        assert removed == 1
        assert spec.untranslatable == []

    def test_noop_when_nothing_is_visually_used(self):
        # (Caller gates on direct-annotation > 0; this just shows the function
        # does not special-case an all-unused set beyond the base rule.)
        a = _measure("A", "SUM(x)")
        b = _measure("B", "SUM(y)")
        spec = _spec([a, b])
        removed = prune_non_visual_measures({"f": spec})
        assert removed == 2  # both unused → both pruned (hence the caller's gate)
