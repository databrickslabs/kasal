"""Catch-all emitter: orphaned measures are translated where possible and
otherwise documented — never silently dropped."""

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


def _tr(orig, sql=None, ok=False, reason="No matching pattern", dax=""):
    return TranslationResult(
        measure_name=orig.lower().replace(" ", "_"),
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
        orig = measure.get("original_name") or measure.get("measure_name")
        dax = measure.get("dax_expression", "")
        if orig == "Good Ratio":
            return _tr(orig, sql="SUM(source.a) / NULLIF(SUM(source.b), 0)", ok=True, dax=dax)
        return _tr(orig, ok=False, dax=dax)


def _spec_with(covered_names):
    return MetricViewSpec(
        fact_table_key="fact_x",
        source_table="cat.sch.fact_x",
        view_name="fact_x",
        comment="",
        joins=[],
        dimensions=[],
        measures=[_tr(n, sql="SUM(source.c)", ok=True) for n in covered_names],
        untranslatable=[],
    )


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
    spec = build_none_allocated_spec(_mapping(), specs, _FakeTranslator(), _ARTIFACT_RE)

    assert spec is not None
    m_names = {m.original_name for m in spec.measures}
    u_names = {u.original_name for u in spec.untranslatable}

    # translatable orphan → a real measure
    assert "Good Ratio" in m_names
    # untranslatable orphans → documented, never dropped
    assert "Orders billed same day %" in u_names
    assert "Trend Sparkline" in u_names
    # already covered on a fact view → not repeated here
    assert "Already Here" not in m_names and "Already Here" not in u_names


def test_visual_artifact_gets_labelled_reason():
    specs = {"fact_x": _spec_with([])}
    spec = build_none_allocated_spec(_mapping(), specs, _FakeTranslator(), _ARTIFACT_RE)
    trend = next(u for u in spec.untranslatable if u.original_name == "Trend Sparkline")
    assert "no Genie/analytical form" in trend.skip_reason
    # a non-artifact orphan keeps its plain reason
    billed = next(u for u in spec.untranslatable if u.original_name.startswith("Orders billed"))
    assert "Genie" not in billed.skip_reason


def test_none_when_everything_is_covered():
    covered = ["Already Here", "Good Ratio", "Orders billed same day %", "Trend Sparkline"]
    specs = {"fact_x": _spec_with(covered)}
    assert build_none_allocated_spec(_mapping(), specs, _FakeTranslator(), _ARTIFACT_RE) is None


def test_yaml_emits_and_carries_view_name_and_dax():
    specs = {"fact_x": _spec_with(["Already Here"])}
    yaml_text = build_none_allocated_yaml(_mapping(), specs, _FakeTranslator(), _ARTIFACT_RE)
    assert yaml_text and "none_allocated_measures" in yaml_text
    # best-effort translated measure present as a real measure
    assert "SUM(source.a)" in yaml_text
    # documented orphan's DAX preserved in the comment block
    assert "fx_SparkLineSVG" in yaml_text
