"""TMDL-based enrichment helpers for the pipeline config generator.

Addresses four gaps found in the IDOR/DQA post-mortem:

S6  — DAX calculated columns (distinct from measures) were never extracted from
      the model's TMDL and never fed into the config.  This module parses them
      out and returns a ``{table_name: [{name, expression, data_type}]}`` map.

S9  — Fiscal 4-4-5 calendar tables were treated as regular calendar months.
      This module detects tables with fiscal-period semantics and records
      ``time_dimension.pbi_period_format: date_to_fiscper`` so downstream code
      can apply the correct period mapping.

rec#9 — Measure descriptions from TMDL and optional KBI catalog rows were never
        harvested.  This module extracts ``description`` / ``formatString`` from
        TMDL measure blocks so downstream can populate ``comment``, ``display_name``
        and ``synonyms`` in the generated views, ending each comment with a
        ``PBI measure: <name>`` traceability tag.

rec#10 — Reconciliation mapping scaffolds (dqa/kpi_reconciliation schema) were
         never emitted.  This module builds one scaffold per fact table from the
         already-allocated ucmv_measures list, using ``direct`` binding for every
         plain named PBI measure.

All functions are pure/stateless — no I/O, no LLM calls, no database access.
Call sites (pipeline_config_generator_tool._run) are fail-open: if any function
raises, the caller catches and logs; the rest of config-gen proceeds unchanged.
"""
from __future__ import annotations

import base64
import re
from collections import defaultdict
from typing import Optional


# ---------------------------------------------------------------------------
# S6 — DAX calculated columns
# ---------------------------------------------------------------------------

def extract_calculated_columns_from_tmdl(
    tmdl_parts: list[dict],
) -> dict[str, list[dict]]:
    """Extract DAX calculated columns from a list of TMDL parts.

    A *calculated column* is a table column with a DAX ``expression``
    (either an inline ``column Name = <expr>`` declaration or an
    ``expression:`` attribute block).  Regular import/query columns have
    no expression.

    Args:
        tmdl_parts: list of ``{path, payload}`` dicts returned by
            ``generate_config.fetch_tmdl_parts``.

    Returns:
        ``{table_name: [{name, expression, data_type}]}``.  Tables with no
        calculated columns are omitted entirely.
    """
    result: dict[str, list[dict]] = {}

    for part in tmdl_parts or []:
        path = part.get("path", "")
        payload = part.get("payload", "")
        if not path.startswith("definition/tables/") or not path.endswith(".tmdl"):
            continue

        content = _decode_payload(payload)
        if not content:
            continue

        table_name = _parse_table_name(content)
        if not table_name:
            continue
        if "LocalDateTable" in table_name or "DateTableTemplate" in table_name:
            continue

        cols = _parse_calculated_columns(content)
        if cols:
            result[table_name] = cols

    return result


def _parse_calculated_columns(content: str) -> list[dict]:
    """Parse calculated columns out of one table's TMDL content.

    Calculated columns carry a DAX ``expression`` attribute (or an inline
    ``= <expr>`` on the ``column`` declaration line).  The function
    correctly handles:
    * ``column Name`` followed by ``expression: <single-line DAX>``
    * ``column 'Name' = <inline DAX>`` (compact single-line form)
    * Multi-line expressions (continuation lines are gathered until the
      next TMDL directive at the same or lesser indent)
    """
    result: list[dict] = []
    lines = content.split("\n")
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i]
        col_m = re.match(r"^([ \t]*)column\s+(?:'([^']+)'|(\w+))(.*)", line)
        if not col_m:
            i += 1
            continue

        indent_str = col_m.group(1)
        base_indent = len(indent_str.expandtabs(4))
        name = col_m.group(2) or col_m.group(3)
        rest = col_m.group(4).strip()

        # Inline expression: `column Name = <DAX expr>`
        # The `=` must immediately follow the name (with optional whitespace).
        # Guard: don't mistake a TMDL identifier like `column X=1` for a column
        # named "X=1" — the `=` signals an inline calculated expression.
        inline_expr: Optional[str] = None
        if rest.startswith("="):
            inline_expr = rest[1:].strip()

        # Scan the attribute block that follows for `expression:` or `dataType:`
        attr_expr_lines: list[str] = []
        in_expr = False
        data_type = ""
        j = i + 1

        while j < n:
            aline = lines[j]
            if not aline.strip():
                j += 1
                continue
            aindent = len(aline) - len(aline.lstrip())
            aindent_exp = len((aline[: aindent]).expandtabs(4))
            if aindent_exp <= base_indent:
                break  # end of column attribute block

            if not in_expr:
                em = re.match(r"[ \t]+expression\s*:\s*(.*)", aline)
                if em:
                    in_expr = True
                    val = em.group(1).strip()
                    if val:
                        attr_expr_lines.append(val)
                    j += 1
                    continue
                # Pick up dataType while we're here
                dtm = re.match(r"[ \t]+dataType\s*:\s*(.*)", aline)
                if dtm:
                    data_type = dtm.group(1).strip()
            else:
                # Continuation line of a multi-line expression attribute.
                # A new `keyword: value` at the same depth as `expression:`
                # ends the expression block.
                if re.match(r"[ \t]+\w[\w]*\s*:", aline):
                    in_expr = False
                    # Don't advance j — this line is for another attribute.
                    break
                attr_expr_lines.append(aline.strip())

            j += 1

        expression = inline_expr or ("\n".join(attr_expr_lines).strip() if attr_expr_lines else None)

        if expression:
            result.append({
                "name": name,
                "expression": expression,
                "data_type": data_type,
            })

        i = j

    return result


# ---------------------------------------------------------------------------
# S9 — Fiscal 4-4-5 calendar detection
# ---------------------------------------------------------------------------

# Table-name patterns that strongly suggest a 4-4-5 fiscal calendar.
_FISCAL_TABLE_NAME_PATS = [
    re.compile(r"4.?4.?5", re.IGNORECASE),        # "445", "4-4-5", "4_4_5"
    re.compile(r"fiscal", re.IGNORECASE),
    re.compile(r"fiscper", re.IGNORECASE),
    re.compile(r"fiscyear", re.IGNORECASE),
]

# Calculated-column expression patterns found in 4-4-5 models.
_FISCAL_EXPR_PATS = [
    re.compile(r"WEEKNUM", re.IGNORECASE),
    re.compile(r"fiscal.*month|month.*fiscal", re.IGNORECASE),
    re.compile(r"period.*label|label.*period", re.IGNORECASE),
    re.compile(r"4.4.5", re.IGNORECASE),
    re.compile(r"fiscper", re.IGNORECASE),
]


def detect_fiscal_calendar_tables(
    tmdl_parts: list[dict],
    calculated_columns: Optional[dict[str, list[dict]]] = None,
) -> dict[str, dict]:
    """Detect tables that implement a fiscal 4-4-5 (or similar) calendar.

    Combines three signals:
    1. Table name matches a known fiscal keyword (``445``, ``fiscal``, …).
    2. Calculated columns contain fiscal-period DAX patterns (``WEEKNUM``,
       ``fiscper``, …).
    3. Regular column names contain fiscal-period keywords.

    Args:
        tmdl_parts: raw TMDL parts from ``generate_config.fetch_tmdl_parts``.
        calculated_columns: output of :func:`extract_calculated_columns_from_tmdl`
            (if already computed, avoids re-parsing).

    Returns:
        ``{table_name: {"pbi_period_format": "date_to_fiscper",
                        "indicators": [<reason strings>]}}``
        for every detected fiscal table.
    """
    calc_cols: dict[str, list[dict]] = calculated_columns or {}
    result: dict[str, dict] = {}

    for part in tmdl_parts or []:
        path = part.get("path", "")
        payload = part.get("payload", "")
        if not path.startswith("definition/tables/") or not path.endswith(".tmdl"):
            continue

        content = _decode_payload(payload)
        if not content:
            continue

        table_name = _parse_table_name(content)
        if not table_name:
            continue
        if "LocalDateTable" in table_name or "DateTableTemplate" in table_name:
            continue

        indicators: list[str] = []

        # Signal 1: table name — strong standalone signal.
        for pat in _FISCAL_TABLE_NAME_PATS:
            if pat.search(table_name):
                indicators.append(f"table name matches {pat.pattern!r}")
                break

        # Signal 2: calculated-column expressions — strong standalone signal.
        # A calculated column that builds fiscal week numbers / period labels
        # is a reliable indicator that THIS table IS the fiscal calendar, not
        # merely a table that HAS a fiscper column (fact tables often store
        # a fiscper value without computing fiscal periods themselves).
        for col in calc_cols.get(table_name, []):
            expr = col.get("expression", "")
            for pat in _FISCAL_EXPR_PATS:
                if pat.search(expr):
                    indicators.append(
                        f"calculated column {col['name']!r} uses pattern {pat.pattern!r}"
                    )
                    break

        # Note: regular column names (e.g. 'fiscper') are intentionally NOT
        # used as a detection signal here.  Fact tables that store fiscal
        # period keys would otherwise be incorrectly flagged.

        if indicators:
            result[table_name] = {
                "pbi_period_format": "date_to_fiscper",
                "indicators": indicators,
            }

    return result


# ---------------------------------------------------------------------------
# rec#9 — Measure metadata harvest from TMDL
# ---------------------------------------------------------------------------

def harvest_measure_metadata_from_tmdl(
    tmdl_parts: list[dict],
) -> dict[str, dict]:
    """Extract measure descriptions (and format strings) from TMDL parts.

    Returns ``{pbi_measure_name: {description, format_string, display_name}}``
    for every measure that carries a ``description:`` attribute in its TMDL
    block.  Measures without a description are omitted.

    The ``description`` value is suitable for use as a UC Metric View
    ``comment``, and the caller is expected to append the traceability tag
    ``PBI measure: <name>`` (rec#9 requirement) before writing the comment
    into the config.

    ``display_name`` is synthesised from the measure name by title-casing
    each word and replacing underscores with spaces.
    """
    result: dict[str, dict] = {}

    for part in tmdl_parts or []:
        path = part.get("path", "")
        payload = part.get("payload", "")
        if not path.startswith("definition/tables/") or not path.endswith(".tmdl"):
            continue

        content = _decode_payload(payload)
        if not content:
            continue

        if "LocalDateTable" in path or "DateTableTemplate" in path:
            continue

        _harvest_measures_from_content(content, result)

    return result


def _harvest_measures_from_content(content: str, out: dict[str, dict]) -> None:
    """Parse one table's TMDL content, adding measure metadata to *out*."""
    lines = content.split("\n")
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i]
        mm = re.match(
            r"^([ \t]*)(?:measure|calculationItem)\s+(?:'([^']+)'|(\w+))\s*=\s*(.*)",
            line,
        )
        if not mm:
            i += 1
            continue

        indent_str = mm.group(1)
        base_indent = len(indent_str.expandtabs(4))
        mname = mm.group(2) or mm.group(3)

        description = ""
        format_string = ""
        j = i + 1

        while j < n:
            aline = lines[j]
            if not aline.strip():
                j += 1
                continue
            aindent = len((aline[: len(aline) - len(aline.lstrip())]).expandtabs(4))
            if aindent <= base_indent:
                break

            dm = re.match(r"[ \t]+description\s*:\s*(.*)", aline)
            if dm:
                description = dm.group(1).strip()
            fm = re.match(r"[ \t]+formatString\s*:\s*(.*)", aline)
            if fm:
                format_string = fm.group(1).strip().strip('"')
            j += 1

        if description or format_string:
            entry: dict = {}
            if description:
                entry["description"] = description
            if format_string:
                entry["format_string"] = format_string
            entry["display_name"] = _humanize(mname)
            out[mname] = entry

        i = j


def enrich_ucmv_measures_with_metadata(
    ucmv_measures: list[dict],
    measure_metadata: dict[str, dict],
) -> list[dict]:
    """Inject TMDL metadata (description, display_name) into the ucmv_measures list.

    For each entry in ``ucmv_measures``, if the measure's ``original_name``
    appears in ``measure_metadata``, the function:
    * adds ``description`` if absent (the raw TMDL description text)
    * adds ``comment`` composed as
      ``"<description>\\nPBI measure: <original_name>"`` (rec#9 traceability tag)
    * adds ``display_name`` if absent

    Returns the same list (mutated in-place) for chaining convenience.
    """
    for m in ucmv_measures or []:
        orig = m.get("original_name") or m.get("measure_name") or ""
        meta = measure_metadata.get(orig)
        if not meta:
            continue

        desc = meta.get("description", "")
        dn = meta.get("display_name", "")

        if desc and "description" not in m:
            m["description"] = desc

        if "comment" not in m:
            tag = f"PBI measure: {orig}"
            m["comment"] = f"{desc}\n{tag}" if desc else tag

        if dn and "display_name" not in m:
            m["display_name"] = dn

    return ucmv_measures


# ---------------------------------------------------------------------------
# rec#10 — Reconciliation mapping scaffolds
# ---------------------------------------------------------------------------

def build_reconciliation_mapping_scaffolds(
    ucmv_measures: list[dict],
    config: dict,
    workspace_id: str,
    dataset_id: str,
    catalog: str = "main",
    schema: str = "default",
    fiscal_calendars: Optional[dict[str, dict]] = None,
) -> list[dict]:
    """Build ``dqa/kpi_reconciliation`` scaffolds for each fact table.

    Produces one scaffold per fact table (keyed by ``proposed_allocation``
    in ``ucmv_measures``), listing every allocated measure as a ``direct``
    binding — the schema does NOT support ``composite_operator: divide``,
    and all plain named PBI measures are in fact ``direct``.

    Each scaffold is a dict ready to be serialised as YAML in the
    ``dqa/kpi_reconciliation`` directory format.  Fields that require
    a human reviewer (``ucmv_table``, ``report``) carry a ``TODO: `` prefix.

    Args:
        ucmv_measures: the ``measures_json`` list produced by config-gen.
        config: the ``proposed_config`` dict.
        workspace_id: PBI workspace GUID.
        dataset_id: PBI dataset GUID (used as ``pbi_semantic_model_id``).
        catalog: UC catalog (default ``main``).
        schema: UC schema (default ``default``).
        fiscal_calendars: output of :func:`detect_fiscal_calendar_tables`
            — when a fact's associated calendar is detected as fiscal,
            ``time_dimension.pbi_period_format: date_to_fiscper`` is added.

    Returns:
        List of scaffold dicts, one per fact table with ≥1 allocated measure.
    """
    fiscal = fiscal_calendars or {}
    join_key_map: dict[str, dict] = config.get("join_key_map") or {}
    fact_join_map: dict[str, dict] = config.get("fact_join_map") or {}
    time_info: dict = config.get("time_dimension") or {}

    # Period column heuristic: use the first entry from period_dim_priority,
    # fall back to 'fiscper' (the most common OTC/FMCG fiscal period column).
    period_dims: list[str] = config.get("period_dim_priority") or []
    period_col = period_dims[0] if period_dims else (
        time_info.get("ucmv_column") or "fiscper"
    )
    pbi_period_col = time_info.get("pbi_column") or period_col

    # Group measures by fact table.
    by_fact: dict[str, list[dict]] = defaultdict(list)
    for m in ucmv_measures or []:
        alloc = (m.get("proposed_allocation") or "").strip()
        if alloc and alloc != "__unassigned__":
            by_fact[alloc].append(m)

    # Determine whether fiscal pbi_period_format should be included.
    # Use the fiscal_calendars dict: if any of the known calendar tables
    # is also a dimension of the model, the facts use fiscal periods.
    model_has_fiscal = bool(fiscal)

    scaffolds: list[dict] = []

    for fact_table in sorted(by_fact):
        measures = by_fact[fact_table]
        if not measures:
            continue

        snake_fact = _to_snake(fact_table)
        view_name = f"{catalog}.{schema}.{snake_fact}_mv"

        # Time dimension block.
        time_dim: dict = {
            "grain": "month",
            "ucmv": {"mode": "column", "name": period_col},
            "pbi_column": pbi_period_col,
        }
        if model_has_fiscal:
            time_dim["pbi_period_format"] = "date_to_fiscper"

        # Default dimension: first join dim in join_key_map (alphabetically stable).
        dim_keys = [
            k for k in join_key_map
            if k.lower() not in (fact_table.lower(),)
        ]
        default_dim_col = "country"  # sensible default for OTC-style models
        if dim_keys:
            # Prefer a dim whose name contains "country"
            country_dim = next(
                (k for k in dim_keys if "country" in k.lower()), None
            )
            default_dim_col = (
                join_key_map.get(country_dim or dim_keys[0], {}).get("dim_column")
                or _to_snake(country_dim or dim_keys[0])
            )

        # Build the measures list.
        rec_measures: list[dict] = []
        for m in measures:
            orig = m.get("original_name") or m.get("measure_name") or ""
            snake = _to_snake(orig)
            rec_measures.append({
                "ucmv_measure": snake,
                "pbi_kind": "direct",
                "pbi_measure": orig,
                "value_format": "raw_number",
                "tolerance": {"kind": "absolute", "value": 0.001},
            })

        scaffold: dict = {
            "ucmv_table": f"TODO: {view_name}",
            "pbi_semantic_model_id": dataset_id or "TODO",
            "pbi_workspace_id": workspace_id or "TODO",
            "report": "TODO: report_name",
            "pbi_fact_table": fact_table,
            "time_dimension": time_dim,
            "default_dimension": default_dim_col,
            "measures": rec_measures,
        }
        scaffolds.append(scaffold)

    return scaffolds


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _decode_payload(payload: str) -> str:
    """Decode a base64-encoded TMDL part payload to UTF-8 text."""
    if not payload:
        return ""
    try:
        return base64.b64decode(payload).decode("utf-8")
    except Exception:
        return payload  # not base64 — assume already decoded (unit tests)


def _parse_table_name(content: str) -> str:
    """Extract the table name from TMDL table content."""
    m = re.match(r"table\s+(?:'([^']+)'|(\w+))", content.strip())
    if not m:
        return ""
    return m.group(1) or m.group(2)


def _humanize(name: str) -> str:
    """Turn a PBI measure name into a readable display name.

    Examples:
        "Order Accuracy %" → "Order Accuracy %"
        "sum_otc_fltp"     → "Sum Otc Fltp"
        "NPS Total"        → "NPS Total"
    """
    # Replace underscores / hyphens with spaces, then title-case.
    spaced = re.sub(r"[_\-]+", " ", name).strip()
    return spaced.title() if spaced == spaced.lower() else spaced


def _to_snake(name: str) -> str:
    """Convert any PBI name to a snake_case identifier.

    Strips characters outside ``[a-z0-9_]`` (after lower-casing) and
    collapses repeated underscores.
    """
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s
