"""Deterministic PBI-measure ↔ UCMV-measure mapping — the reconciliation
framework's input.

Ports the CONCEPTS (not the code — separate repo, no dependency) of
``dqa/kpi_reconciliation/mapping_schema.py``'s ``pbi_kind`` taxonomy and
``mapping_candidates.py``'s deterministic first-pass generator: for each
UCMV measure, classify how its SQL was derived from the PBI side and emit a
draft YAML file per fact table, in the same shape
``dqa/kpi_reconciliation/kpi_reconciliation.py`` already consumes as a
mapping file, so a report using Kasal's own generation no longer needs one
hand-authored.

Classification never guesses from the SQL text — it reads provenance Kasal's
own generation pipeline already has:

- An ORDINARILY-translated measure (regex/LLM, not a SWITCH/fx dispatch) is
  ``pbi_kind: direct`` against its own ``original_name`` — correct for the
  large majority: it IS the named PBI measure it was translated from.
- A SWITCH/fx-resolved measure carries its real provenance on
  ``TranslationResult.pbi_kind``/``pbi_sources``/``pbi_operator``, stamped by
  ``switch_decomposition.py``/``custom_function_resolution.py`` at generation
  time (single passthrough -> ``direct``; two-measure arithmetic ->
  ``composite``; plant/company dimension split -> ``dimension_conditional``;
  fx_OTCKPI's code-filtered EAV ratio -> ``composite``/``divide`` over
  ``raw_column`` sources, not measure names).
- Anything untranslatable (or still a TODO skeleton) is ``unresolved``, with
  ``raw_hint`` carrying the original DAX/skip reason for a human or the LLM
  review pass — matching ``mapping_candidates.py``'s own behavior for a
  measure it couldn't resolve either.

``binding:`` (the fact table's own PBI name, join keys, default reconciliation
dimension) is left ``TODO`` deliberately, exactly like the reference
generator — it needs the same kind of live PBI-model investigation
(``fact_sc``/``pe002`` in the reference repo went through by hand), not
something to guess at from the extraction alone.

Output is suffixed ``.mapping_candidates.yml`` (never bare ``.yml``) so it is
never mistaken for a reviewed, production mapping — same convention the
reference generator uses for exactly the same reason.
"""

from __future__ import annotations

from src.services.tools.metric_view_utils.data_classes import MetricViewSpec

_VALID_PBI_KINDS = {
    "direct",
    "composite",
    "dimension_conditional",
    "raw_column",
    "unresolved",
}


def _yaml_str(value: str) -> str:
    """Minimal YAML scalar quoting — this module's values are measure/table/
    column identifiers and short hints, never multi-line, so this only needs
    to handle the few characters that would otherwise be misread as YAML
    syntax (unlike yaml_emitter.py's fuller helper, which also handles block
    scalars for arbitrary DAX text)."""
    if not value:
        return "''"
    if any(c in value for c in (":", "#", "'", '"', "{", "}", "[", "]", "%")):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if value != value.strip():
        return f'"{value}"'
    return value


def _measure_pbi_kind(m) -> dict:
    """Classify one TranslationResult's PBI provenance.

    Returns a dict ready to splice into the YAML entry — always has `kind`;
    the other keys depend on which kind it resolved to.
    """
    sql = m.sql_expr or ""
    is_unresolved = (not m.is_translatable) or sql.strip().upper().startswith("TODO")

    if is_unresolved:
        hint = (m.dax_expression or m.skip_reason or "").strip()
        return {"kind": "unresolved", "raw_hint": hint[:300]}

    if m.pbi_kind and m.pbi_kind in _VALID_PBI_KINDS:
        if m.pbi_kind == "raw_column":
            # No named PBI measure exists at all — implicit_column_measures.py
            # (derive_implicit_column_measures) promoted a raw column PBI
            # itself aggregates, matching mapping_schema.py's own `raw_column`
            # pbi_kind (the reconciliation framework this feeds distinguishes
            # it from `direct` for exactly this reason: there is no measure
            # name to look up on the PBI side, only a table+column).
            src = m.pbi_sources[0] if m.pbi_sources else {}
            return {
                "kind": "raw_column",
                "pbi_table": src.get("table"),
                "pbi_column": src.get("column", m.original_name),
            }
        if m.pbi_kind == "dimension_conditional":
            parent = m.pbi_sources[0] if m.pbi_sources else m.original_name
            return {"kind": "dimension_conditional", "context_measure": parent}
        if m.pbi_kind == "composite":
            sources = m.pbi_sources or []
            if len(sources) == 2 and all(isinstance(s, str) for s in sources):
                return {
                    "kind": "composite",
                    "operator": m.pbi_operator or "subtract",
                    "operand_a": {"pbi_measure": sources[0]},
                    "operand_b": {"pbi_measure": sources[1]},
                }
            if len(sources) == 2 and all(isinstance(s, dict) for s in sources):
                return {
                    "kind": "composite",
                    "operator": m.pbi_operator or "divide",
                    "operand_a": {
                        "pbi_table": sources[0].get("table"),
                        "pbi_column": sources[0].get("column"),
                        "value_column": sources[0].get("value_column"),
                        "filter_value": sources[0].get("filter_value"),
                    },
                    "operand_b": {
                        "pbi_table": sources[1].get("table"),
                        "pbi_column": sources[1].get("column"),
                        "value_column": sources[1].get("value_column"),
                        "filter_value": sources[1].get("filter_value"),
                    },
                }
            # Shape we don't recognize (e.g. an operand count this module
            # hasn't seen live) — degrade honestly to unresolved rather than
            # emit a partially-wrong composite.
            return {
                "kind": "unresolved",
                "raw_hint": f"composite with unexpected sources: {sources!r}"[:300],
            }
        # "direct" via a switch/fx single-passthrough — the resolved SOURCE
        # measure, not this UCMV measure's own (synthetic) name.
        source = m.pbi_sources[0] if m.pbi_sources else m.original_name
        return {"kind": "direct", "pbi_measure": source}

    # No stamped provenance — the common case: an ordinarily-translated
    # measure IS the PBI measure named in original_name.
    return {"kind": "direct", "pbi_measure": m.original_name}


def _emit_measure_entry(m) -> list[str]:
    lines: list[str] = []
    lines.append(f"  - ucmv_measure: {_yaml_str(m.measure_name)}")
    info = _measure_pbi_kind(m)
    lines.append(f"    pbi_kind: {info['kind']}")

    if info["kind"] == "direct":
        lines.append(f"    pbi_measure: {_yaml_str(info['pbi_measure'])}")
    elif info["kind"] == "raw_column":
        if info.get("pbi_table"):
            lines.append(f"    pbi_table: {_yaml_str(info['pbi_table'])}")
        lines.append(f"    pbi_column: {_yaml_str(info['pbi_column'])}")
    elif info["kind"] == "dimension_conditional":
        lines.append(f"    context_measure: {_yaml_str(info['context_measure'])}")
        lines.append(
            "    notes: resolved by which dimension is present in the "
            "query's own grouping, not by a selector value"
        )
    elif info["kind"] == "composite":
        lines.append(f"    composite_operator: {info['operator']}")
        for key in ("operand_a", "operand_b"):
            op = info[key]
            lines.append(f"    {key}:")
            for field_name, field_val in op.items():
                if field_val is None:
                    continue
                lines.append(f"      {field_name}: {_yaml_str(str(field_val))}")
    elif info["kind"] == "unresolved":
        lines.append(f"    raw_hint: {_yaml_str(info['raw_hint'])}")

    if m.used_in_visuals:
        pages = sorted({o.get("page", "") for o in m.used_in_visuals if o.get("page")})
        if pages:
            lines.append("    used_in_visuals:")
            for page in pages:
                lines.append(f"      - {_yaml_str(page)}")

    lines.append("")
    return lines


def derive_pbi_ucmv_mapping(specs: dict[str, MetricViewSpec]) -> dict[str, str]:
    """One ``<view_name>.mapping_candidates.yml`` text per fact table.

    Covers every measure the pipeline produced a result for — translated
    (any ``pbi_kind``) and untranslatable alike — so the file is a complete,
    reviewable draft, not just the easy cases. Returns ``{}`` for a spec with
    no measures at all (nothing to map).
    """
    out: dict[str, str] = {}
    for table_key, spec in specs.items():
        all_measures = list(spec.measures) + list(spec.untranslatable)
        if not all_measures:
            continue

        lines: list[str] = [
            f"# {spec.view_name}.mapping_candidates.yml — DRAFT, deterministic "
            f"first pass. Review before use.",
            "# binding: fields are not derivable from extraction alone — same "
            "as dqa/kpi_reconciliation's own mapping_candidates.py, fill in "
            "from a live PBI-model investigation before running reconciliation.",
            "binding:",
            f"  ucmv: {_yaml_str(spec.view_name)}",
            "  pbi_fact_table: TODO",
            "  default_dimension: TODO",
            "",
            "measures:",
        ]
        for m in sorted(all_measures, key=lambda x: x.measure_name):
            lines.extend(_emit_measure_entry(m))

        out[spec.view_name] = "\n".join(lines)
    return out
