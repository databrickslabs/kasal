"""Measure-section renderers for the UC Metric View YAML.

Each ``render_*`` function appends the YAML block for one measure bucket
(base / DAX-translated / SWITCH-decomposed / aggregated-column / untranslated
comments) to ``lines``. ``emit_yaml`` decides which buckets exist and in what
order; this module only knows how one bucket is written.
"""

from __future__ import annotations

import logging
import re

from .data_classes import MetricViewSpec, TranslationResult
from .drift.fingerprint import dax_fingerprint
from .metadata_generator import MetadataGenerator
from .utils import col_to_readable
from .yaml_annotations import (
    _categorize_untranslatable,
    _indirect_visual_usage_suffix,
    _provenance_suffix,
    _usage_suffix,
    _visual_usage_suffix,
)
from .yaml_scalars import _yaml_scalar, _yaml_val

logger = logging.getLogger(__name__)


def render_base_measures(
    lines: list[str],
    base_measures: list[TranslationResult],
    measure_overrides: dict,
    meta_gen: MetadataGenerator,
) -> None:
    """Append the ``measures:`` block for base (SUM-over-column) measures."""
    lines.append(
        f"# \u2500\u2500\u2500 Base Measures ({len(base_measures)}) "
        + "\u2500" * (58 - len(str(len(base_measures))))
    )
    lines.append("")
    lines.append("measures:")
    for m in base_measures:
        lines.append(f"  - name: {m.measure_name}")
        expr = m.sql_expr
        if expr is None:
            continue
        lines.append(f"    expr: {expr}")
        # Use per-table metadata override, fall back to generated
        m_override = measure_overrides.get(m.measure_name, {})
        if m_override.get("comment"):
            comment = m_override["comment"]  # human override wins verbatim
        else:
            comment = (
                m.skip_reason or col_to_readable(m.measure_name)
            ) + _provenance_suffix(m)
        comment += _usage_suffix(m.referenced_by)
        comment += _visual_usage_suffix(m)
        comment += _indirect_visual_usage_suffix(m)
        lines.append(f"    comment: {_yaml_val(comment)}")
        m_meta = meta_gen.get_measure_meta(m.measure_name, expr)
        display_name = m_override.get("display_name") or m_meta.get("display_name", "")
        if display_name:
            lines.append(f"    display_name: {_yaml_val(display_name)}")
        # Format BEFORE synonyms (matching customer target style)
        if m_meta.get("format"):
            fmt = m_meta["format"]
            lines.append("    format:")
            lines.append(f'      type: {fmt["type"]}')
            if "currency_code" in fmt:
                lines.append(f'      currency_code: {fmt["currency_code"]}')
            if "decimal_places" in fmt:
                lines.append("      decimal_places:")
                lines.append("        type: exact")
                lines.append(f'        places: {fmt["decimal_places"]["places"]}')
        synonyms = m_override.get("synonyms", [])
        if synonyms:
            lines.append("    synonyms:")
            for syn in synonyms:
                lines.append(f"      - {_yaml_val(syn)}")
        lines.append("")


def render_dax_measures(
    lines: list[str],
    dax_measures: list[TranslationResult],
    measure_overrides: dict,
    meta_gen: MetadataGenerator,
    budget_suffix: str,
    percentage_multiplier_patterns: list[str] | None,
) -> None:
    """Append the DAX-translated measure entries (one section header + measures)."""
    lines.append(
        f"  # \u2500\u2500\u2500 DAX-Translated Measures ({len(dax_measures)}) "
        + "\u2500" * (50 - len(str(len(dax_measures))))
    )
    lines.append("")
    for m in dax_measures:
        if not m.sql_expr:
            continue  # safety: skip measures with no SQL
        lines.append(f"  - name: {m.measure_name}")
        expr = m.sql_expr
        # Apply percentage multiplier only if explicitly configured via patterns
        if percentage_multiplier_patterns and "/" in expr:
            for _pmp in percentage_multiplier_patterns:
                if re.search(_pmp, m.measure_name, re.IGNORECASE):
                    expr = f"100*{expr}"
                    break
        if (
            any(c in expr for c in ("'", '"', ":", "#", "{", "}", "[", "]"))
            or "FILTER" in expr
        ):
            if len(expr) > 120 or "\n" in expr or ('"' in expr and "'" in expr):
                # Use the shared scalar formatter: multi-line values become a
                # properly-indented block scalar (|-). Writing a raw multi-line
                # string after 'expr: >-' left continuation lines unindented and
                # broke YAML parsing ("while scanning a simple key").
                lines.append(f"    expr: {_yaml_scalar(expr, indent=4)}")
            elif '"' in expr:
                lines.append(f"    expr: '{expr}'")
            else:
                lines.append(f'    expr: "{expr}"')
        else:
            lines.append(f"    expr: {expr}")
        if m.window_spec:
            lines.append("    window:")
            lines.append(f"      - order: {m.window_spec['order']}")
            lines.append(f"        range: {m.window_spec['range']}")
            lines.append(
                f"        semiadditive: {m.window_spec.get('semiadditive', 'last')}"
            )
        # Comment and metadata from per-table overrides or auto-generated
        m_override = measure_overrides.get(m.measure_name, {})
        dax_comment = m_override.get("comment", "")
        if not dax_comment:
            if m.original_name != m.measure_name:
                dax_comment = f"PBI: {m.original_name}"
            # Source-DAX fingerprint, early in the comment so UC's 4000-char
            # truncation never drops it — the drift monitor compares it with
            # today's DAX to detect a measure changed in Power BI.
            _fp = dax_fingerprint(getattr(m, "dax_expression", "") or "")
            if _fp:
                dax_comment = f"{dax_comment} · {_fp}" if dax_comment else _fp
            dax_comment += _provenance_suffix(m)
            # M11: a semi-additive (latest-period) measure is NOT additive over
            # its order column — flag it so a reviewer/Genie doesn't sum it
            # across periods. The `window: semiadditive: last` below enforces it;
            # the caveat explains why the value is a point-in-time snapshot.
            if m.window_spec and m.window_spec.get("semiadditive"):
                dax_comment += (
                    f" · CAVEAT: non-additive — value taken at the "
                    f"{m.window_spec.get('semiadditive', 'last')} "
                    f"{m.window_spec.get('order', 'period')} of the selected "
                    f"period (semi-additive snapshot; do not sum across periods)"
                )
        dax_comment += _usage_suffix(m.referenced_by)
        dax_comment += _visual_usage_suffix(m)
        dax_comment += _indirect_visual_usage_suffix(m)
        if dax_comment:
            lines.append(f"    comment: {_yaml_val(dax_comment)}")
        # Display name
        m_meta = meta_gen.get_measure_meta(m.measure_name, expr)
        dax_display = m_override.get("display_name", "")
        if not dax_display:
            dax_display = m_meta.get("display_name", "")
            if dax_display:
                if m.measure_name.endswith(budget_suffix):
                    dax_display += " (Budget)"
                elif any(
                    m.measure_name.replace(budget_suffix, "") == m2.measure_name
                    for m2 in dax_measures
                    if m2.measure_name.endswith(budget_suffix)
                ):
                    dax_display += " (Actual)"
        if dax_display:
            lines.append(f"    display_name: {_yaml_val(dax_display)}")
        # Format BEFORE synonyms (matching customer target style)
        if m_meta.get("format"):
            fmt = m_meta["format"]
            lines.append("    format:")
            lines.append(f'      type: {fmt["type"]}')
            if "currency_code" in fmt:
                lines.append(f'      currency_code: {fmt["currency_code"]}')
            if "decimal_places" in fmt:
                lines.append("      decimal_places:")
                lines.append("        type: exact")
                lines.append(f'        places: {fmt["decimal_places"]["places"]}')
        # Synonyms AFTER format
        synonyms = m_override.get("synonyms", [])
        if synonyms:
            lines.append("    synonyms:")
            for syn in synonyms:
                lines.append(f"      - {_yaml_val(syn)}")
        lines.append("")


def render_switch_measures(
    lines: list[str], switch_measures: list[TranslationResult]
) -> None:
    """Append the SWITCH-decomposed measure entries."""
    lines.append(
        f"  # \u2500\u2500\u2500 SWITCH-Decomposed Measures ({len(switch_measures)}) "
        + "\u2500" * (46 - len(str(len(switch_measures))))
    )
    lines.append("")
    for m in switch_measures:
        lines.append(f"  - name: {m.measure_name}")
        expr = m.sql_expr
        if expr is None:
            continue
        if len(expr) > 120 or "FILTER" in expr or "\n" in expr:
            # Shared scalar formatter → multi-line becomes an indented block
            # scalar (|-); avoids the unindented-continuation YAML parse error.
            lines.append(f"    expr: {_yaml_scalar(expr, indent=4)}")
        else:
            lines.append(f"    expr: {expr}")
        if m.window_spec:
            lines.append("    window:")
            lines.append(f"      - order: {m.window_spec['order']}")
            lines.append(f"        range: {m.window_spec['range']}")
            lines.append(
                f"        semiadditive: {m.window_spec.get('semiadditive', 'last')}"
            )
        lines.append(
            f"    comment: {_yaml_val(m.skip_reason + _usage_suffix(m.referenced_by) + _visual_usage_suffix(m) + _indirect_visual_usage_suffix(m))}"
        )
        lines.append("")


def render_implicit_measures(
    lines: list[str], implicit_measures: list[TranslationResult]
) -> None:
    """Append the aggregated-column (implicit visual column) measure entries."""
    # Its own labeled section \u2014 never folded into "DAX-Translated" \u2014 so a
    # reviewer sees at a glance that these came from a raw column PBI
    # itself aggregates, not from a named measure's DAX. The comment
    # (built in table_processor.py's Step 6d) always states that, and
    # `_visual_usage_suffix` always appends "Used on: <pages>" since a
    # column only reaches this bucket by being found in a visual.
    lines.append(
        f"  # \u2500\u2500\u2500 Aggregated Column Measures ({len(implicit_measures)}) "
        + "\u2500" * (44 - len(str(len(implicit_measures))))
    )
    lines.append("")
    for m in implicit_measures:
        lines.append(f"  - name: {m.measure_name}")
        expr = m.sql_expr
        if expr is None:
            continue
        lines.append(f"    expr: {expr}")
        lines.append(
            f"    comment: {_yaml_val(m.skip_reason + _usage_suffix(m.referenced_by) + _visual_usage_suffix(m) + _indirect_visual_usage_suffix(m))}"
        )
        lines.append("")


def render_untranslatable_comments(lines: list[str], spec: MetricViewSpec) -> None:
    """Append the commented-out ``Not emitted as measures`` block with DAX + drafts."""
    lines.append(
        f"  # \u2500\u2500\u2500 Not emitted as measures ({len(spec.untranslatable)}) \u2014 grouped by reason \u2500\u2500\u2500"
    )
    # Group by human category so a reviewer sees WHY each measure was skipped
    # (display artifact / slicer-scalar / dynamic selector / prior-year /
    # complex-DAX-needs-manual). Within a group, highest-usage first.
    _by_cat: dict[str, list] = {}
    _cat_why: dict[str, str] = {}
    for m in spec.untranslatable:
        cat, why = _categorize_untranslatable(m)
        _by_cat.setdefault(cat, []).append(m)
        _cat_why.setdefault(cat, why)
    # Stable, informative order: the "not-a-measure-by-nature" buckets first
    # (expected, no action), then the ones that need work.
    _order = [
        "display artifact",
        "slicer/scalar helper",
        "dynamic KPI selector",
        "disconnected-slicer dispatch (TREATAS)",
        "parameter/label lookup (LOOKUPVALUE)",
        "prior-year time-intelligence",
        "distinct-count pattern",
        # translatable-with-source-view-work buckets last (actionable):
        "fixed-LOD (ALLEXCEPT)",
        "top-N row selection (TOPN)",
        "group-then-aggregate (SUMMARIZE/CALCULATETABLE)",
        "complex DAX \u2014 needs manual translation",
    ]
    for cat in _order + [c for c in _by_cat if c not in _order]:
        ms = _by_cat.get(cat)
        if not ms:
            continue
        lines.append("  #")
        lines.append(f"  # [{cat}] ({len(ms)}) \u2014 {_cat_why[cat]}")
        for m in sorted(ms, key=lambda x: x.referenced_by, reverse=True):
            lines.append(
                f"  #   - {m.original_name}{_usage_suffix(m.referenced_by)}{_visual_usage_suffix(m)}{_indirect_visual_usage_suffix(m)}"
            )
            # Preserve the full original DAX so a reviewer can hand-translate
            # without re-opening the PBIX. Each DAX line is emitted as its own
            # comment line (multi-line DAX would otherwise break YAML). Indented
            # under the measure name for readability.
            dax = (getattr(m, "dax_expression", "") or "").strip()
            if dax:
                lines.append("  #       DAX:")
                for dax_line in dax.split("\n"):
                    lines.append(f"  #         {dax_line}")
            # Best-effort, UNVERIFIED source-view SQL scaffold for the cross-fact /
            # multi-stage cases — a starting point to build a new view + UCMV on it.
            # Never an emitted measure; the reviewer completes + verifies it.
            try:
                from .recovery_recommender import draft_source_view

                _draft = draft_source_view(
                    dax, measure_name=m.measure_name, fact_table=spec.fact_table_key
                )
            except Exception:  # the scaffold is optional; the measure stays documented
                logger.warning(
                    "source-view draft failed for %s", m.measure_name, exc_info=True
                )
                _draft = None
            if _draft:
                lines.append(
                    "  #       SOURCE-VIEW DRAFT (build this, then a UCMV on it):"
                )
                for _dl in _draft.split("\n"):
                    lines.append(f"  #         {_dl}")
