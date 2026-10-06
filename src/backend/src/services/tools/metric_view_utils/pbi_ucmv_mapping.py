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

2026-09-29 additions (aligned with mapping_schema.py rev):

- ``pbi_aggregation`` on ``raw_column``: the PBI SummarizeBy value confirmed
  via ``INFO.VIEW.COLUMNS()`` (Sum/Average/Count/Min/Max/DistinctCount).
  Emitted only when non-Sum (Sum is the documented default, omitting it is
  safe and matches the reference YAML convention). Read from
  ``pbi_sources[0]["summarize_by"]`` if present; never guessed. Kasal's
  ``implicit_column_measures.py`` currently stores the aggregation in the SQL
  expression but not in ``pbi_sources`` — wire ``summarize_by`` into every
  ``pbi_sources`` entry there for non-Sum columns to propagate the value here.
- ``composite_operator`` restricted to ``{subtract, add}``; ``divide`` and
  string-source composites (two named-measure operands from switch
  decomposition, unsupported in the schema) now degrade to ``unresolved``.
- Per-operand ``extra_filter`` inside ``composite_a``/``composite_b``
  (renamed from ``operand_a``/``operand_b``), with ``pbi_column`` emitted as
  ``"Table[Column]"`` instead of separate ``pbi_table``/``pbi_column`` keys.
- ``value_format: raw_number`` and ``tolerance: {kind: absolute, value: 0.001}``
  now emitted on every measure entry (sensible defaults; reviewer fills in
  per-measure overrides).
- Measure-level ``extra_filter`` (constant page/visual-level filters) emitted
  for ``raw_column`` measures that carry the list in ``pbi_sources[0]``.
- ``switch`` added to ``_VALID_PBI_KINDS``; emits selector fields from
  ``pbi_sources[0]`` when available, or degrades to ``unresolved``.

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
    "switch",
    "unresolved",
}

# PBI's 6 real SummarizeBy values confirmed present in live INFO.VIEW.COLUMNS()
# output. "None" excluded — that means "not aggregatable" and is never promoted.
_VALID_PBI_AGGREGATIONS = {"Sum", "Average", "Count", "Min", "Max", "DistinctCount"}

# 2026-09-29: only these two operators are supported by the reconciliation
# framework. "divide" (fx_OTCKPI EAV) and string-pair composites degrade to
# unresolved — the reviewer promotes them once a native schema shape is added.
_VALID_COMPOSITE_OPERATORS = {"subtract", "add"}


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


def _emit_filter_block(lines: list[str], filters: list, indent: str = "    ") -> None:
    """Emit a YAML ``extra_filter:`` list of ``{table, column, values}`` dicts.

    Used both for measure-level filters (``raw_column`` / ``direct``) and for
    per-operand filters inside ``composite_a``/``composite_b``.  ``indent``
    controls the leading whitespace so the same helper serves both call sites.
    """
    lines.append(f"{indent}extra_filter:")
    for f in filters:
        lines.append(f"{indent}- table: {_yaml_str(str(f.get('table', '')))}")
        lines.append(f"{indent}  column: {_yaml_str(str(f.get('column', '')))}")
        vals = f.get("values") or []
        lines.append(f"{indent}  values:")
        for v in vals:
            lines.append(f"{indent}  - {_yaml_str(str(v))}")


def _measure_pbi_kind(m) -> dict:
    """Classify one TranslationResult's PBI provenance.

    Returns a dict ready to splice into the YAML entry — always has ``kind``;
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
            agg = src.get("summarize_by", "Sum")
            if agg not in _VALID_PBI_AGGREGATIONS:
                agg = "Sum"
            result: dict = {
                "kind": "raw_column",
                "pbi_table": src.get("table"),
                "pbi_column": src.get("column", m.original_name),
            }
            # 2026-09-29: emit explicitly when non-Sum — never guess silently.
            # When summarize_by is absent from pbi_sources (the current default
            # from implicit_column_measures.py) we omit the field: the schema
            # default is "Sum" and emitting it when we haven't confirmed it
            # would be misleading. Wire summarize_by into pbi_sources entries
            # in implicit_column_measures.py to propagate non-Sum values here.
            if agg != "Sum":
                result["pbi_aggregation"] = agg
            ef = src.get("extra_filter")
            if ef:
                result["extra_filter"] = ef
            return result

        if m.pbi_kind == "switch":
            # A PBI measure that branches on SELECTEDVALUE() over a Mapping_*
            # selector table. Kasal's current switch_decomposition.py resolves
            # these to `direct` passthrough rather than stamping `switch`, so
            # this branch is defensive-forward: emit selector fields if they
            # are present in pbi_sources[0], otherwise degrade to unresolved.
            src = (
                m.pbi_sources[0]
                if m.pbi_sources and isinstance(m.pbi_sources[0], dict)
                else {}
            )
            pbi_measure = src.get("pbi_measure")
            sw_table = src.get("switch_selector_table")
            sw_col = src.get("switch_selector_column")
            sw_val = src.get("switch_selector_value")
            if pbi_measure and sw_table and sw_col and sw_val:
                return {
                    "kind": "switch",
                    "pbi_measure": pbi_measure,
                    "switch_selector_table": sw_table,
                    "switch_selector_column": sw_col,
                    "switch_selector_value": sw_val,
                }
            return {
                "kind": "unresolved",
                "raw_hint": (
                    f"switch without selector details: pbi_sources={m.pbi_sources!r}"
                )[:300],
            }

        if m.pbi_kind == "dimension_conditional":
            parent = m.pbi_sources[0] if m.pbi_sources else m.original_name
            return {"kind": "dimension_conditional", "context_measure": parent}

        if m.pbi_kind == "composite":
            sources = m.pbi_sources or []
            op = m.pbi_operator or "subtract"

            # 2026-09-29: only subtract/add are in the schema. The divide
            # operator (fx_OTCKPI EAV ratios) and any future operator outside
            # this set degrade to unresolved rather than emit an invalid entry.
            if op not in _VALID_COMPOSITE_OPERATORS:
                return {
                    "kind": "unresolved",
                    "raw_hint": (
                        f"composite operator {op!r} not in "
                        f"{sorted(_VALID_COMPOSITE_OPERATORS)!r}: {sources!r}"
                    )[:300],
                }

            # String sources (two named-measure operands from switch
            # decomposition) — not expressible in the schema's composite shape,
            # which requires pbi_column per operand. Degrade honestly.
            if len(sources) == 2 and all(isinstance(s, str) for s in sources):
                return {
                    "kind": "unresolved",
                    "raw_hint": (
                        f"composite of named measures not yet in schema: {sources!r}"
                    )[:300],
                }

            # Dict sources — validate and build schema-compatible operands.
            if len(sources) == 2 and all(isinstance(s, dict) for s in sources):
                # EAV/fx_OTCKPI shape carries value_column/filter_value which
                # don't map to the schema's extra_filter list — degrade.
                if any(s.get("value_column") or s.get("filter_value") for s in sources):
                    return {
                        "kind": "unresolved",
                        "raw_hint": (f"composite EAV shape not in schema: {sources!r}")[
                            :300
                        ],
                    }

                def _operand(src: dict) -> dict:
                    table = src.get("table", "")
                    col = src.get("column", "")
                    # Format as "Table[Column]" per mapping_schema.py convention.
                    pbi_col = f"{table}[{col}]" if table and col else col
                    d: dict = {"pbi_column": pbi_col}
                    ef = src.get("extra_filter")
                    if ef:
                        d["extra_filter"] = ef
                    return d

                return {
                    "kind": "composite",
                    "composite_operator": op,
                    "composite_a": _operand(sources[0]),
                    "composite_b": _operand(sources[1]),
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
        # 2026-09-29: emit pbi_aggregation when non-Sum (omit when Sum — it's
        # the default and the reference YAML convention omits it too).
        if "pbi_aggregation" in info:
            lines.append(f"    pbi_aggregation: {_yaml_str(info['pbi_aggregation'])}")

    elif info["kind"] == "switch":
        lines.append(f"    pbi_measure: {_yaml_str(info['pbi_measure'])}")
        lines.append(
            f"    switch_selector_table: {_yaml_str(info['switch_selector_table'])}"
        )
        lines.append(
            f"    switch_selector_column: {_yaml_str(info['switch_selector_column'])}"
        )
        lines.append(
            f"    switch_selector_value: {_yaml_str(info['switch_selector_value'])}"
        )

    elif info["kind"] == "dimension_conditional":
        lines.append(f"    context_measure: {_yaml_str(info['context_measure'])}")
        lines.append(
            "    notes: resolved by which dimension is present in the "
            "query's own grouping, not by a selector value"
        )

    elif info["kind"] == "composite":
        lines.append(f"    composite_operator: {info['composite_operator']}")
        # 2026-09-29: composite_a/composite_b (renamed from operand_a/operand_b).
        # Each operand carries pbi_column ("Table[Column]") and optional
        # per-operand extra_filter — the whole point of the composite kind vs
        # raw_column is that each operand filters independently.
        for key in ("composite_a", "composite_b"):
            op = info[key]
            lines.append(f"    {key}:")
            lines.append(f"      pbi_column: {_yaml_str(op['pbi_column'])}")
            ef = op.get("extra_filter")
            if ef:
                _emit_filter_block(lines, ef, indent="      ")

    elif info["kind"] == "unresolved":
        lines.append(f"    raw_hint: {_yaml_str(info['raw_hint'])}")

    # 2026-09-29: value_format and tolerance on every entry.
    # TranslationResult carries no per-measure overrides today; a reviewer
    # sets non-default values (percent_string, relative tolerance) by hand.
    lines.append("    value_format: raw_number")
    lines.append("    tolerance:")
    lines.append("      kind: absolute")
    lines.append("      value: 0.001")

    # Measure-level extra_filter (constant page/visual-level filters for
    # raw_column / direct measures).  Composite operands already carry their
    # own per-operand filters above; this covers the flat measure case only.
    if info["kind"] in ("raw_column", "direct"):
        ef = info.get("extra_filter")
        if ef:
            _emit_filter_block(lines, ef, indent="    ")

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
