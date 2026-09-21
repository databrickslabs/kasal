"""Resolve PBI SWITCH-based measures into real per-fact-table SQL.

Split out of ``pipeline_config.py`` (which is over the file-size ceiling) as its
own seam: everything here is about turning one PBI measure whose DAX is a
``SELECTEDVALUE(...) + SWITCH(...)`` dispatcher — used to show several KPI
variants through one visual/matrix via a selector table — into a real,
deployable UCMV measure attributed to the correct fact table, instead of the
non-fact selector table (``Measures_Table``, ``OTC_MeasuresSlicer``, ...) the
raw extraction always allocates the dispatcher measure to.

``to_snake_case`` is imported lazily (function-local) from ``pipeline_config``
to avoid a circular top-level import — ``pipeline_config.build_config`` calls
back into this module's ``derive_switch_decompositions``/
``derive_geo_switch_decompositions``.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any


def _extract_switch_branches(dax: str) -> list[dict]:
    """Parse named ``SWITCH(TRUE(), var="value", expr, ...)`` branches from DAX.

    Only matches the shape where each branch's condition is a clean
    ``variable = "literal"`` equality — used by ``derive_filter_sets`` (in
    ``pipeline_config.py``) to recover a selector's value list. Most of this
    report's own SWITCH measures branch on an arbitrary boolean condition
    instead (see ``_switch_result_candidates``), so this often returns empty —
    that's expected, not a bug.
    """
    branches: list[dict] = []
    switch_match = re.search(
        r"SWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,\s*(.+)",
        dax,
        re.IGNORECASE | re.DOTALL,
    )
    if not switch_match:
        return branches

    body = switch_match.group(1)
    branch_re = re.compile(
        r'(\w+)\s*=\s*"([^"]+)"\s*,\s*([^,]+?)(?=,\s*\w+\s*=\s*"|$)',
        re.DOTALL,
    )
    for bm in branch_re.finditer(body):
        branches.append(
            {
                "variable": bm.group(1),
                "case_value": bm.group(2),
                "dax_snippet": bm.group(3).strip().rstrip(",").strip()[:200],
            }
        )
    return branches


def _calculate_branch_bodies(text: str) -> list[str]:
    """Return the balanced inner text of every top-level ``CALCULATE( ... )``."""
    bodies: list[str] = []
    for m in re.finditer(r"CALCULATE\s*\(", text, re.IGNORECASE):
        start = m.end()
        depth = 1
        pos = start
        while pos < len(text) and depth > 0:
            if text[pos] == "(":
                depth += 1
            elif text[pos] == ")":
                depth -= 1
                if depth == 0:
                    bodies.append(text[start:pos])
                    break
            pos += 1
    return bodies


def derive_geo_switch_decompositions(measures: list[dict]) -> dict[str, list[dict]]:
    """Detect plant/company geo-selector SWITCH measures and emit BOTH branches.

    Shape (no SELECTEDVALUE — that's the parameterized case handled by
    ``derive_switch_decompositions``):
        SWITCH(TRUE(),
               Or(ISFILTERED(dim[plant_desc]), HASONEVALUE(dim[plant])),
               CALCULATE(<agg>, … creg_type="Plant"),      -- branch A (plant)
               CALCULATE(<agg>, … creg_type="Company Code"))-- branch B (company)

    A UC metric view has no slicer context, so the single PBI SWITCH measure must
    become TWO static measures — ``plant_<base>`` and ``company_<base>`` — matching
    the ground truth. Each branch is resolved to real SQL via the same
    ``_resolve_referenced_measure_dax`` used for measure-refs, so downstream
    dependents (which reference the parent by name) still resolve, and the second
    (company) variant — previously dropped entirely — is now emitted.

    Returns ``{table: [ {name, raw_expr, comment}, ... ]}`` list-format entries
    (real SQL, not skeletons), mergeable into ``switch_decompositions``.
    """
    from src.services.powerbi.pipeline_config import to_snake_case

    out: dict[str, list[dict]] = defaultdict(list)

    for m in measures:
        dax = m.get("expression", "") or ""
        name = m.get("original_name") or m.get("measure_name", "")
        table = m.get("table_name", "") or m.get("proposed_allocation", "")
        if not dax or not name:
            continue
        du = dax.upper()
        # geo-selector: SWITCH(TRUE(), …) whose condition tests plant filter state
        if not re.search(r"SWITCH\s*\(\s*TRUE\s*\(\s*\)", du):
            continue
        if not re.search(r"ISFILTERED|HASONEVALUE", du):
            continue
        # Strip var…return scaffolding so the branch CALCULATEs are the ones found.
        body = dax
        _ret = re.search(r"\breturn\b\s*(.+)$", body, re.IGNORECASE | re.DOTALL)
        if _ret and re.match(r"(?is)^\s*var\s+", body):
            body = _ret.group(1)
        branches = _calculate_branch_bodies(body)
        if len(branches) < 2:
            continue  # need at least a plant + company branch

        # Base measure name → strip a leading Plant_Comp / Plant_Company token and
        # the geo word, so `Plant_Comp KBI_Value_Actual` → `kbi_value_actual`.
        base = re.sub(r"(?i)^\s*plant[_ ]?comp(?:any)?[_ ]*", "", name).strip()
        base_snake = to_snake_case(base) or to_snake_case(name)

        emitted = []
        for label, branch in (("plant", branches[0]), ("company", branches[1])):
            resolved = _resolve_referenced_measure_dax(f"CALCULATE({branch})")
            if not resolved:
                continue
            base_expr = resolved["base_expr"]
            filters = resolved["base_filters"]
            sql = base_expr
            if filters:
                sql = f"{base_expr} FILTER (WHERE {' AND '.join(filters)})"
            emitted.append(
                {
                    "name": f"{label}_{base_snake}",
                    "raw_expr": sql,
                    "comment": f"{label.capitalize()} branch of geo-selector SWITCH [{name}]",
                }
            )
        # Only emit when BOTH branches resolved — a half-decomposition would be
        # worse than leaving the parent measure to the normal path.
        if len(emitted) == 2:
            out[table].extend(emitted)

    return dict(out)


def _resolve_referenced_measure_dax(dax: str) -> dict | None:
    """PROP-1: transpile a REFERENCED measure's DAX into a UCMV ``base_expr``
    (+ ``base_filters``) so ``[MeasureRef]`` resolutions carry real SQL instead of
    a ``TODO`` placeholder.

    Handles the concrete shapes seen in the reference model (and common elsewhere):
      * numeric constant                     → base_expr = the number
      * ``SUM(T[col])`` / ``SUMX(T, T[col])`` → base_expr = ``SUM(source.col)``
      * ``CALCULATE(SUM(T[col]), T[a]="x", …)`` → base_expr + base_filters
      * ``SWITCH(TRUE(), <cond>, CALCULATE(...), CALCULATE(...))`` (plant-vs-company
        selector) → the FIRST CALCULATE branch (the default the GT also picks)

    Returns ``{'base_expr': str, 'base_filters': [str]}`` or ``None`` when the DAX
    is not one of these self-contained aggregate shapes (caller then keeps a TODO).
    """
    from src.services.powerbi.pipeline_config import to_snake_case

    if not dax:
        return None
    d = dax.strip()

    # Strip leading slicer-scalar scaffolding vars before locating the aggregate.
    # Measures on the _BP/_PY side carry `var std = CALCULATE([F_Start_date]) var
    # etd = CALCULATE([F_End_date]) return <real expr>` — those date-window vars
    # are display scaffolding (ignored elsewhere in the pipeline). If we don't
    # drop them, the FIRST CALCULATE( found is `CALCULATE([F_Start_date])` (the
    # scaffolding), not the real aggregate branch, and resolution fails — which is
    # exactly why the _BP twins dropped while the scaffolding-free _Actual twins
    # resolved. Cut to the RETURN body so the aggregate is the first CALCULATE.
    _ret = re.search(r"\breturn\b\s*(.+)$", d, re.IGNORECASE | re.DOTALL)
    if _ret and re.match(r"(?is)^\s*var\s+", d):
        d = _ret.group(1).strip()

    # Constant (e.g. AVG_KBI_Div_Factor's effective value is 1 in the GT).
    if re.fullmatch(r"-?\d+(?:\.\d+)?", d):
        return {"base_expr": d, "base_filters": []}

    def _first_calculate_body(text: str) -> str | None:
        """Return the balanced inner text of the FIRST ``CALCULATE( ... )``."""
        cm = re.search(r"CALCULATE\s*\(", text, re.IGNORECASE)
        if not cm:
            return None
        start = cm.end()
        depth = 1
        pos = start
        while pos < len(text) and depth > 0:
            if text[pos] == "(":
                depth += 1
            elif text[pos] == ")":
                depth -= 1
                if depth == 0:
                    return text[start:pos]
            pos += 1
        return text[start:pos]

    # If it's a SWITCH selector (or any wrapper containing CALCULATE), resolve to
    # the FIRST CALCULATE(...) branch — the plant/default branch the ground truth
    # picks. Bound the parse to that single balanced CALCULATE so a second branch's
    # filters (e.g. "Company Code") don't leak into this resolution.
    if re.match(r"(?is)^\s*(?:var\s+.*?return\s+)?switch\s*\(", d) or (
        "CALCULATE" in d.upper() and not re.match(r"(?is)^\s*CALCULATE\s*\(", d)
    ):
        body = _first_calculate_body(d)
        if body is not None:
            inner = body
        else:
            inner = d
    else:
        # CALCULATE(SUM(T[col]), <filter>, ...)  — take its balanced body
        body = _first_calculate_body(d)
        inner = body if body is not None else d

    # Aggregate over Table[Column]  →  SUM(source.col)
    agg = re.search(
        r"\b(SUM|SUMX|AVERAGE|MIN|MAX|COUNT|DISTINCTCOUNT)\s*\("
        r"(?:\s*\w+\s*,\s*)?"  # SUMX(Table, ...) optional table arg
        r"(?:'[^']+'|\w+)\[(\w+)\]",  # Table[col] / 'Table Name'[col]
        inner,
        re.IGNORECASE,
    )
    if not agg:
        # Also handle the SUMX(FILTER(fact, <pred>), fact[col]) shape, where the
        # first arg is a FILTER(...) table rather than a bare table — the column
        # is the FINAL Table[col] argument. (Plant/Company KBI selectors on the
        # _BP side use this shape, unlike the _Actual side's CALCULATE(SUM,…).)
        agg = re.search(
            r"\b(SUMX|COUNTX|AVERAGEX)\s*\(\s*FILTER\s*\(.*\)\s*,\s*"
            r"(?:'[^']+'|\w+)\[(\w+)\]\s*\)",
            inner,
            re.IGNORECASE | re.DOTALL,
        )
        if not agg:
            return None
    func = agg.group(1).upper()
    spark_func = {
        "SUMX": "SUM",
        "COUNTX": "COUNT",
        "AVERAGEX": "AVG",
        "DISTINCTCOUNT": "COUNT_DISTINCT",
    }.get(func, func)
    col = to_snake_case(agg.group(2))
    base_expr = f"{spark_func}(source.{col})"

    # Extract equality filters  T[a] = "x"  →  a = 'x'
    filters: list[str] = []
    for fm in re.finditer(
        r"""(?:'[^']+'|\w+)\[(\w+)\]\s*=\s*("[^"]*"|'[^']*'|-?\d+(?:\.\d+)?)""",
        inner,
    ):
        fcol = to_snake_case(fm.group(1))
        val = fm.group(2)
        if val[0] == '"':
            val = "'" + val[1:-1] + "'"
        filters.append(f"{fcol} = {val}")

    return {"base_expr": base_expr, "base_filters": filters}


def _parse_dax_vars(dax: str) -> dict[str, str]:
    """Extract ``VAR name = expr`` assignments from a DAX block (before RETURN)."""
    vars_map: dict[str, str] = {}
    for vm in re.finditer(
        r"VAR\s+(\w+)\s*=\s*(.+?)(?=\s+VAR\s+\w+\s*=|\s+RETURN\b|$)",
        dax,
        re.IGNORECASE | re.DOTALL,
    ):
        vars_map[vm.group(1)] = vm.group(2).strip()
    return vars_map


def _substitute_vars(expr: str, vars_map: dict[str, str], _depth: int = 0) -> str:
    """Inline ``_var`` tokens with their VAR definitions (bounded to avoid cycles)."""
    if _depth > 3 or not vars_map:
        return expr
    changed = False
    for name, val in vars_map.items():
        pattern = r"(?<![\w\[])" + re.escape(name) + r"(?![\w\]])"
        if re.search(pattern, expr):
            expr = re.sub(pattern, f"({val})", expr)
            changed = True
    return _substitute_vars(expr, vars_map, _depth + 1) if changed else expr


def _split_balanced_args(text: str) -> list[str]:
    """Split a comma-separated argument list on TOP-LEVEL commas only.

    Respects ``()``/``[]``/``{}`` nesting and quoted strings, so a comma inside
    ``{"OTC NPS","Cycle Time"}`` or a nested ``CALCULATE(...)`` doesn't split the
    argument in two.
    """
    args: list[str] = []
    depth = 0
    quote: str | None = None
    start = 0
    for i, ch in enumerate(text):
        if quote:
            if ch == quote:
                quote = None
            continue
        if ch in "\"'":
            quote = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            args.append(text[start:i].strip())
            start = i + 1
    args.append(text[start:].strip())
    return [a for a in args if a]


def _switch_result_candidates(dax: str) -> list[str]:
    """Result-expression candidates from a ``SWITCH(TRUE(), cond, result, ...)``.

    A ``SWITCH(TRUE(), ...)`` argument list alternates ``condition, result`` pairs
    with an optional trailing default (odd arg count). This model's own SWITCH
    measures often branch on a display/formatting condition (e.g. percentage
    scaling for certain KPIs) rather than on picking between different measures —
    confirmed live on ``ACT vs Target``: both branches reduce to the same
    ``_KPI - _Target`` formula, one of them ``*100``-scaled. So every RESULT
    candidate (not just the default) is a legitimate resolution attempt, tried in
    order — the first one that resolves via ``_resolve_switch_branch`` wins,
    mirroring ``_resolve_referenced_measure_dax``'s existing "first branch is the
    ground truth" convention for a plant/company SWITCH.
    """
    switch_match = re.search(
        r"SWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,\s*(.+)\)\s*$",
        dax.strip(),
        re.IGNORECASE | re.DOTALL,
    )
    if not switch_match:
        return []
    args = _split_balanced_args(switch_match.group(1))
    # cond, result, cond, result, ..., [default] — results are odd indices
    # (0-based: 1, 3, 5, ...); a trailing unpaired arg is the default result.
    results = [args[i] for i in range(1, len(args), 2)]
    if len(args) % 2 == 1:
        results.append(args[-1])
    return results


def _resolve_switch_branch(
    expanded_expr: str, measure_by_name: dict[str, dict]
) -> dict | None:
    """Resolve a VAR-expanded SWITCH branch to real SQL + its real target table.

    Handles the two shapes confirmed live in this report's own SWITCH measures
    (and the only two ``dqa/kpi_reconciliation``'s own mapping schema models for a
    SWITCH branch): a single ``[MeasureName]`` passthrough (the branch just picks
    one already-defined measure verbatim), and a two-operand ``[A] - [B]`` / ``[A]
    + [B]`` composite (its ``composite`` pbi_kind, ``subtract``/``add``). Each
    referenced measure is resolved via the SAME ``_resolve_referenced_measure_dax``
    helper ``derive_measure_resolutions``/``derive_geo_switch_decompositions``
    already use — so a branch that references a measure this transpiler can already
    turn into SQL never needs a fresh resolver written for it. Anything else
    (three+ operands, non-additive arithmetic, a non-measure literal) returns
    ``None`` — the caller keeps a TODO, now carrying the expanded expression
    instead of the raw unexpanded snippet, which is still strictly more useful.
    """

    def _resolve_one(mrow: dict) -> dict | None:
        return _resolve_referenced_measure_dax(mrow.get("expression", "") or "")

    def _sql_with_filters(resolved: dict) -> str:
        sql = resolved["base_expr"]
        if resolved["base_filters"]:
            sql = f"{sql} FILTER (WHERE {' AND '.join(resolved['base_filters'])})"
        return sql

    stripped = expanded_expr.strip()
    single = re.fullmatch(r"\(*\[([^\]]+)\]\)*", stripped)
    if single:
        mrow = measure_by_name.get(single.group(1))
        resolved = _resolve_one(mrow) if mrow else None
        if not resolved:
            return None
        return {
            "sql": _sql_with_filters(resolved),
            "table": mrow.get("table_name", ""),
            "sources": [single.group(1)],
        }

    op_match = re.fullmatch(
        r"\(*\[([^\]]+)\]\)*\s*([+-])\s*\(*\[([^\]]+)\]\)*", stripped
    )
    if op_match:
        a_name, op, b_name = op_match.group(1), op_match.group(2), op_match.group(3)
        a_row, b_row = measure_by_name.get(a_name), measure_by_name.get(b_name)
        a_res = _resolve_one(a_row) if a_row else None
        b_res = _resolve_one(b_row) if b_row else None
        if not a_res or not b_res:
            return None
        sql = f"({_sql_with_filters(a_res)}) {op} ({_sql_with_filters(b_res)})"
        table = a_row.get("table_name", "") or b_row.get("table_name", "")
        return {"sql": sql, "table": table, "sources": [a_name, b_name]}

    return None


def derive_switch_decompositions(measures: list[dict]) -> dict[str, list[dict]]:
    """Detect SELECTEDVALUE+SWITCH measures and resolve each branch to real SQL.

    A SWITCH branch usually just selects one of the model's OTHER already-defined
    measures (or a simple two-measure composite) via VAR aliases — e.g. "ACT vs
    Target" -> ``_KPI - _Target`` where ``_KPI = [ACT]`` and ``_Target = [BP]``.
    Each branch is VAR-expanded and resolved via ``_resolve_switch_branch``, then
    allocated to the REAL fact table the referenced measure(s) live on — NOT the
    selector/slicer table (``OTC_MeasuresSlicer``, ``Measures_Table``, ...) the
    parent SWITCH measure itself was attributed to. That selector table has no
    warehouse source and is always skipped, which is what silently dropped every
    one of these decompositions from the final UCMV output before this fix: the
    branches were detected (``_detected_branches``) but never resolved to SQL,
    and even the raw skeleton was parked under a table that never survives to a
    deployable view.
    """
    from src.services.powerbi.pipeline_config import to_snake_case

    decompositions: dict[str, list[dict]] = defaultdict(list)
    measure_by_name = {
        m.get("measure_name", ""): m for m in measures if m.get("measure_name")
    }

    for m in measures:
        dax = m.get("expression", "") or ""
        name = m.get("measure_name", "")
        selector_table = m.get("table_name", "")

        if not dax:
            continue
        dax_upper = dax.upper()
        if "SELECTEDVALUE" not in dax_upper or "SWITCH" not in dax_upper:
            continue

        vars_map = _parse_dax_vars(dax)
        resolved = None
        for candidate in _switch_result_candidates(dax):
            expanded = _substitute_vars(candidate, vars_map)
            resolved = _resolve_switch_branch(expanded, measure_by_name)
            if resolved:
                break

        entry: dict[str, Any] = {"name": to_snake_case(name)}
        if resolved:
            entry["raw_expr"] = resolved["sql"]
            entry["comment"] = (
                f"SWITCH measure [{name}] — resolved from {resolved['sources']}"
            )
            target_table = resolved["table"] or selector_table
        else:
            entry["raw_expr"] = (
                f"TODO: SQL expression for SWITCH measure '{name}' "
                f"(DAX: {dax[:120]}...)"
            )
            entry["comment"] = f"SWITCH measure from {name}"
            target_table = selector_table

        # `derive_filter_sets` (in `pipeline_config.py`) reads `_detected_branches`
        # (the OLDER named-branch shape: `variable="value", expr`) off this key
        # when present, to recover a selector's value list — best-effort, only
        # populated when the DAX happens to use that shape (most of this report's
        # own SWITCHes branch on an arbitrary boolean condition instead, per
        # `_switch_result_candidates`'s docstring, so this is often empty and
        # that's fine).
        named_branches = _extract_switch_branches(dax)
        if named_branches:
            entry["_detected_branches"] = named_branches

        decompositions[target_table].append(entry)

    return dict(decompositions)
