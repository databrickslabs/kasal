"""Catch-all emitter: orphaned measures are translated where possible and
otherwise documented — never silently dropped. "Covered" is gated on what
actually rendered in the exported YAML, so a measure on a spec that emitted
nothing still lands in the catch-all."""

import re

from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.none_allocated_emitter import (
    build_none_allocated_spec,
    build_none_allocated_yaml,
)

_ARTIFACT_RE = re.compile(r"fx_SparkLineSVG|SELECTEDVALUE|FORMAT\(", re.IGNORECASE)


def _snake(n):
    return n.lower().replace(" ", "_")


def _tr(orig, sql=None, ok=False, reason="No matching pattern", dax=""):
    return TranslationResult(
        measure_name=_snake(orig),
        original_name=orig,
        sql_expr=sql,
        is_translatable=ok,
        skip_reason=reason,
        dax_expression=dax,
        confidence="high" if ok else "low",
        category="single_table" if ok else "unassigned",
    )


class _FakeTranslator:
    """Translates 'Good Ratio' to SQL; everything else is untranslatable."""

    def translate(self, measure, table_key):
        orig = measure.get("original_name") or measure.get("measure_name") or measure.get("name")
        dax = measure.get("dax_expression", "") or measure.get("expression", "")
        if orig == "Good Ratio":
            return _tr(orig, sql="SUM(source.a) / NULLIF(SUM(source.b), 0)", ok=True, dax=dax)
        return _tr(orig, ok=False, dax=dax)


class _NeverTranslator:
    def translate(self, measure, table_key):
        orig = measure.get("original_name") or measure.get("name")
        return _tr(orig, ok=False, dax=measure.get("dax_expression") or measure.get("expression", ""))


def _spec_with(covered_names):
    return MetricViewSpec(
        fact_table_key="fact_x", source_table="cat.sch.fact_x", view_name="fact_x",
        comment="", joins=[], dimensions=[],
        measures=[_tr(n, sql="SUM(source.c)", ok=True) for n in covered_names],
        untranslatable=[],
    )


def _yaml_for(names):
    """Minimal rendered-view text so `nm in blob` gating sees these as emitted."""
    return {"fact_x": "measures:\n" + "\n".join(f"  - name: {_snake(n)}" for n in names)}


def _mapping():
    return [
        {"measure_name": "already_here", "original_name": "Already Here",
         "dax_expression": "SUM(fact_x[c])"},
        {"measure_name": "good_ratio", "original_name": "Good Ratio",
         "dax_expression": "DIVIDE(SUM(f[a]), SUM(f[b]))"},
        {"measure_name": "orders_billed", "original_name": "Orders billed same day %",
         "dax_expression": "VAR _c1 = CALCULATE(...) RETURN DIVIDE(_c1,_c2)"},
        {"measure_name": "trend", "original_name": "Trend Sparkline",
         "dax_expression": "fx_SparkLineSVG([ACT], FALSE())"},
    ]


def test_orphans_split_into_translated_and_documented():
    specs = {"fact_x": _spec_with(["Already Here"])}
    emitted = _yaml_for(["Already Here"])
    spec = build_none_allocated_spec(_mapping(), specs, emitted, _FakeTranslator(), _ARTIFACT_RE)

    assert spec is not None
    m_names = {m.original_name for m in spec.measures}
    u_names = {u.original_name for u in spec.untranslatable}
    assert "Good Ratio" in m_names                       # translatable orphan -> measure
    assert "Orders billed same day %" in u_names         # untranslatable -> documented
    assert "Trend Sparkline" in u_names
    assert "Already Here" not in m_names and "Already Here" not in u_names  # rendered -> covered


def test_measure_on_unrendered_spec_still_caught():
    """A measure on a spec whose YAML rendered EMPTY must NOT be treated as
    covered — it should still land in the catch-all (the real 9-leak bug)."""
    # spec claims "Already Here" but the emitted YAML is empty (0-measure dim view).
    specs = {"fact_x": _spec_with(["Already Here"])}
    spec = build_none_allocated_spec(_mapping(), specs, {"fact_x": ""}, _FakeTranslator(), _ARTIFACT_RE)
    names = {m.original_name for m in spec.measures} | {u.original_name for u in spec.untranslatable}
    assert "Already Here" in names  # not in the export -> caught here


def test_visual_artifact_gets_labelled_reason():
    spec = build_none_allocated_spec(_mapping(), {}, {}, _FakeTranslator(), _ARTIFACT_RE)
    trend = next(u for u in spec.untranslatable if u.original_name == "Trend Sparkline")
    assert "no Genie/analytical form" in trend.skip_reason
    billed = next(u for u in spec.untranslatable if u.original_name.startswith("Orders billed"))
    assert "Genie" not in billed.skip_reason


def test_none_when_everything_is_covered():
    covered = ["Already Here", "Good Ratio", "Orders billed same day %", "Trend Sparkline"]
    specs = {"fact_x": _spec_with(covered)}
    assert build_none_allocated_spec(_mapping(), specs, _yaml_for(covered), _FakeTranslator(), _ARTIFACT_RE) is None


def test_zero_translated_still_emits_documented_yaml():
    universe = [
        {"name": "Overview_Agg_Vendor_Score", "expression": "CALCULATE([Overview_Agg_Score], 'D'[Domain]=\"Vendor\")"},
        {"name": "AI BUs Improving", "expression": "COUNTROWS(FILTER(x,[AI Improvement]>0))"},
    ]
    yaml_text = build_none_allocated_yaml(universe, {}, {}, _NeverTranslator(), _ARTIFACT_RE)
    assert yaml_text and "none_allocated_measures" in yaml_text
    assert "_documented_only_placeholder" in yaml_text
    assert "Overview_Agg_Vendor_Score" in yaml_text


def test_entry_shape_tolerance_name_and_expression_keys():
    universe = [{"name": "Some Orphan KPI", "expression": "SUM(f[x])"}]
    spec = build_none_allocated_spec(universe, {}, {}, _NeverTranslator(), _ARTIFACT_RE)
    all_names = {m.original_name for m in spec.measures} | {u.original_name for u in spec.untranslatable}
    assert "Some Orphan KPI" in all_names


def test_yaml_emits_and_carries_view_name_and_dax():
    specs = {"fact_x": _spec_with(["Already Here"])}
    yaml_text = build_none_allocated_yaml(_mapping(), specs, _yaml_for(["Already Here"]), _FakeTranslator(), _ARTIFACT_RE)
    assert yaml_text and "none_allocated_measures" in yaml_text
    assert "SUM(source.a)" in yaml_text          # translated measure present
    assert "fx_SparkLineSVG" in yaml_text         # documented orphan's DAX preserved
