"""Pattern-driven DAX→SQL translators for the group-then-aggregate / VAR-ratio /
CALCULATE-filter / latest-week defect classes (IDOR/DQA post-mortem M1–M5, M9, M11).

These are GENERIC, tenant-agnostic shape matchers extracted from
``dax_translator.py`` to keep that module under the file-size ceiling. Each
``match_*`` function inspects a (pre-cleaned) DAX string and returns a match dict
or ``None``; each ``translate_*`` function turns a match into Spark SQL (or
``(None, reason)`` when it cannot fully build it, so the measure falls through to
the next registered pattern / the LLM). They are wired into
``DaxTranslator._register_patterns`` and run BEFORE the generic catch-alls.

Design rules honored here:
- Column references emit as ``source.<col>`` (M9 — always qualify).
- DAX ``<>`` / ``!=`` inequality is BLANK-passing: a row where the column is
  BLANK satisfies ``col <> 'X'`` in DAX, but SQL ``col <> 'X'`` drops NULLs. We
  emit the NULL-aware ``(source.col IS NULL OR source.col <> 'X')`` (M3).
- Every value interpolated into SQL is escaped (``_lit`` / ``_in_list``): a
  semantic-model author controls filter values, so quote-breakout must be
  impossible.
- A matcher returns ``None`` unless it can FULLY parse its shape — no partial /
  silently-wrong output.
"""

from __future__ import annotations

import re

from .utils import to_snake_case


# ── SQL literal escaping (mirrors dax_translator._sql_str_literal; kept local to
#    avoid a circular import — dax_translator imports THIS module) ──────────────
def _lit(value: str) -> str:
    """Safe single-quoted SQL string literal (doubles quotes, strips NULs)."""
    s = str(value).replace("\x00", "")
    return "'" + s.replace("'", "''") + "'"


def _in_list_str(values) -> str:
    return ", ".join(_lit(v) for v in values)


def _in_list_num(values) -> str:
    return ", ".join(str(v) for v in values)


# A DAX column reference, optionally qualified by a bare or single-quoted table:
#   Table[col] | 'Quoted Table'[col] | [col]
# Only the COLUMN is captured — the table is ignored (same-fact → ``source``),
# which is what lets these matchers handle single-quoted table names with spaces
# and hyphens (e.g. 'AI_Invoice-DataBricks SQL') that the bareword ``\w+`` regexes
# in constants.py cannot.
_REF = r"(?:'[^']*'|\w+)?\s*\[\s*(\w+)\s*\]"

# Like _REF but the column may contain spaces/punctuation (e.g. a PBI column
# literally named "Activity last 6 months, no DP&Stock"). Used where the column
# label is turned into a physical name via to_snake_case, not matched verbatim.
_REF_LOOSE = r"(?:'[^']*'|\w+)?\s*\[\s*([^\]]+?)\s*\]"


def _strip_wrapping_parens(cond: str) -> str:
    cond = cond.strip()
    while len(cond) >= 2 and cond[0] == "(" and cond[-1] == ")":
        depth = 0
        wraps_all = True
        for i, ch in enumerate(cond):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and i != len(cond) - 1:
                    wraps_all = False
                    break
        if not wraps_all:
            break
        cond = cond[1:-1].strip()
    return cond


def _in_list_from_body(body: str) -> str:
    strs = re.findall(r'"([^"]*)"', body)
    if not strs:
        strs = re.findall(r"'([^']*)'", body)
    if strs:
        return _in_list_str(strs)
    nums = re.findall(r"-?\d+(?:\.\d+)?", body)
    return _in_list_num(nums)


def _atom_to_sql(atom: str) -> str | None:
    """Translate a single DAX predicate to source-qualified SQL, NULL-aware on
    inequality. Returns None when the atom shape isn't recognised."""
    atom = _strip_wrapping_parens(atom)

    # NOT <ref> IN {..}
    m = re.match(rf"^NOT\s+{_REF}\s+in\s+\{{([^}}]*)\}}$", atom, re.IGNORECASE)
    if m:
        col, body = m.group(1), m.group(2)
        return f"source.{col} NOT IN ({_in_list_from_body(body)})"
    # <ref> IN {..}
    m = re.match(rf"^{_REF}\s+in\s+\{{([^}}]*)\}}$", atom, re.IGNORECASE)
    if m:
        col, body = m.group(1), m.group(2)
        return f"source.{col} IN ({_in_list_from_body(body)})"
    # <ref> <op> <value>
    m = re.match(rf"^{_REF}\s*(<>|!=|<=|>=|=|<|>)\s*(.+)$", atom)
    if m:
        col, op, val = m.group(1), m.group(2), m.group(3).strip()
        sm = re.fullmatch(r'"([^"]*)"|\'([^\']*)\'', val)
        if sm:
            lit = _lit(sm.group(1) if sm.group(1) is not None else sm.group(2))
            if op in ("<>", "!="):
                # DAX BLANK passes col <> 'x'; SQL drops NULLs. Preserve semantics.
                return f"(source.{col} IS NULL OR source.{col} {op} {lit})"
            return f"source.{col} {op} {lit}"
        if re.fullmatch(r"-?\d+(?:\.\d+)?", val):
            if op in ("<>", "!="):
                return f"(source.{col} IS NULL OR source.{col} {op} {val})"
            return f"source.{col} {op} {val}"
    return None


def cond_to_sql(cond: str) -> str | None:
    """Translate a DAX filter condition (supporting && / ||) to SQL. Returns None
    if any sub-atom cannot be translated (so the caller can bail cleanly)."""
    cond = _strip_wrapping_parens(cond)
    if not cond:
        return None
    if "&&" in cond:
        parts = [cond_to_sql(p) for p in re.split(r"\s*&&\s*", cond)]
        if any(p is None for p in parts):
            return None
        return " AND ".join(parts)
    if "||" in cond:
        parts = [cond_to_sql(p) for p in re.split(r"\s*\|\|\s*", cond)]
        if any(p is None for p in parts):
            return None
        return "(" + " OR ".join(parts) + ")"
    return _atom_to_sql(cond)


def _split_top_args(s: str) -> list[str]:
    """Split a comma-separated argument list at the TOP paren/brace level,
    respecting quotes."""
    args: list[str] = []
    depth = 0
    cur: list[str] = []
    q: str | None = None
    for ch in s:
        if q:
            cur.append(ch)
            if ch == q:
                q = None
            continue
        if ch in ("'", '"'):
            q = ch
            cur.append(ch)
            continue
        if ch in "([{":
            depth += 1
            cur.append(ch)
            continue
        if ch in ")]}":
            depth -= 1
            cur.append(ch)
            continue
        if ch == "," and depth == 0:
            args.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    if cur:
        args.append("".join(cur))
    return [a.strip() for a in args if a.strip()]


def _var_segments(dax: str) -> tuple[dict[str, str], str]:
    """Split ``VAR name = body ... RETURN expr`` into (var_map, return_expr).

    Works whether the vars span multiple lines or one line — a boundary tokenizer
    on ``VAR <name> =`` / ``RETURN`` rather than a line split (which captures only
    the first var and silently drops the rest)."""
    boundary = re.compile(r"\bVAR\s+(\w+)\s*=\s*|\bRETURN\b", re.IGNORECASE)
    marks = list(boundary.finditer(dax))
    if not marks:
        return {}, dax.strip()
    var_map: dict[str, str] = {}
    ret_expr = ""
    for i, mk in enumerate(marks):
        seg_start = mk.end()
        seg_end = marks[i + 1].start() if i + 1 < len(marks) else len(dax)
        segment = dax[seg_start:seg_end].strip()
        if mk.group(1):
            var_map[mk.group(1)] = segment
        else:
            ret_expr = segment
    return var_map, ret_expr


def _snake_col(col: str) -> str:
    """Physical-column snake_case for a DAX column label (delegates to the shared
    ``to_snake_case`` so measure/dim/column naming stays consistent)."""
    return to_snake_case(col)


# ═══════════════════════════════════════════════════════════════════════════
# M1 / M2 — SUMMARIZE + DISTINCTCOUNT "group-then-aggregate" (NPS)
# ═══════════════════════════════════════════════════════════════════════════
#
# DAX shape:
#   VAR _GroupTable = CALCULATETABLE(
#       ADDCOLUMNS(SUMMARIZE('T', [score]),
#                  "@Count", CALCULATE(DISTINCTCOUNT('T'[id]))) [, REMOVEFILTERS(..)])
#   VAR _X = SUMX(FILTER(_GroupTable, <cond over [score]>), [@Count])
#   VAR _Responses = SUMX(_GroupTable, [@Count])  |  [SomeDistinctCountMeasure]
#   RETURN <_X> | DIVIDE((_A - _B), _Responses) * 100
#
# A "response" is a DISTINCT (score, id) pair, so a filtered SUMX over @Count is
# COUNT(DISTINCT concat(score,'|',id)) with the row-level condition applied inside
# a CASE. REMOVEFILTERS(drivers) is a no-op here — we never filter by the driver
# columns, which is exactly what removing their filter context means.


def _pair_expr(score_col: str, id_col: str) -> str:
    return (
        f"concat(CAST(source.{score_col} AS STRING), '|', "
        f"CAST(source.{id_col} AS STRING))"
    )


def _distinct_pair_count(score_col: str, id_col: str, cond_sql: str | None) -> str:
    pair = _pair_expr(score_col, id_col)
    if cond_sql:
        return f"COUNT(DISTINCT CASE WHEN {cond_sql} THEN {pair} END)"
    return f"COUNT(DISTINCT {pair})"


def match_summarize_distinct(dax: str, name: str) -> dict | None:
    up = dax.upper()
    if "SUMMARIZE" not in up or "DISTINCTCOUNT" not in up:
        return None
    # Single grouping column: SUMMARIZE('T', [score]) — a 2+-column SUMMARIZE (e.g.
    # SUMMARIZE(T,[a],[b])) is a different, non-generic shape left to the LLM.
    score_m = re.search(
        r"SUMMARIZE\s*\(\s*(?:'[^']*'|\w+)\s*,\s*\[?(\w+)\]?\s*\)", dax, re.IGNORECASE
    )
    id_m = re.search(rf"DISTINCTCOUNT\s*\(\s*{_REF}\s*\)", dax, re.IGNORECASE)
    if not score_m or not id_m:
        return None
    var_map, ret_expr = _var_segments(dax)
    if not ret_expr:
        return None
    return {
        "score_col": score_m.group(1),
        "id_col": id_m.group(1),
        "var_map": var_map,
        "return_expr": ret_expr,
    }


def _resolve_nps_var(body: str, score_col: str, id_col: str, translator) -> str | None:
    """Resolve one NPS VAR body to SQL, or None if it isn't a known shape."""
    body = body.strip()
    # SUMX(FILTER(_GroupTable, <cond>), [@Count]) → filtered distinct-pair count
    fm = re.search(
        r"SUMX\s*\(\s*FILTER\s*\(\s*\w+\s*,\s*(.+?)\s*\)\s*,\s*\[?@?\w+\]?\s*\)\s*$",
        body,
        re.IGNORECASE | re.DOTALL,
    )
    if fm:
        cond_sql = cond_to_sql(fm.group(1))
        if cond_sql is None:
            return None
        return _distinct_pair_count(score_col, id_col, cond_sql)
    # SUMX(_GroupTable, [@Count]) → total distinct pairs
    if re.fullmatch(
        r"\s*SUMX\s*\(\s*\w+\s*,\s*\[?@?\w+\]?\s*\)\s*", body, re.IGNORECASE
    ):
        return _distinct_pair_count(score_col, id_col, None)
    # A measure ref [DistinctCountCG AGG] → distinct respondent count. Prefer a
    # configured resolution; else the distinct count of the id column (which is
    # what a "responses"/"distinct CG" denominator means in this pattern).
    mref = re.fullmatch(r"\s*\[([^\]]+)\]\s*", body)
    if mref:
        res = translator._measure_resolutions.get(mref.group(1))
        if res and res.get("base_expr") and not res.get("base_filters"):
            return res["base_expr"]
        return f"COUNT(DISTINCT source.{id_col})"
    return None


def translate_summarize_distinct(match: dict, dax: str, table_key, translator):
    score_col, id_col = match["score_col"], match["id_col"]
    var_map = match["var_map"]
    ret = match["return_expr"].strip()

    def resolve(var_name: str) -> str | None:
        body = var_map.get(var_name)
        return _resolve_nps_var(body, score_col, id_col, translator) if body else None

    # RETURN a single var → that var's SQL (Promoters / Detractors / Passives)
    single = re.fullmatch(r"\s*(\w+)\s*", ret)
    if single:
        sql = resolve(single.group(1))
        return (sql, "") if sql else (None, "NPS: unresolved RETURN var")

    # RETURN DIVIDE((a - b), c) * 100 → (a - b) / NULLIF(c, 0) * 100
    dm = re.fullmatch(
        r"\s*DIVIDE\s*\(\s*\(?\s*(\w+)\s*-\s*(\w+)\s*\)?\s*,\s*(\w+)\s*\)\s*\*\s*100\s*",
        ret,
        re.IGNORECASE,
    )
    if dm:
        a, b, c = resolve(dm.group(1)), resolve(dm.group(2)), resolve(dm.group(3))
        if a and b and c:
            return f"({a} - {b}) / NULLIF({c}, 0) * 100", ""
        return None, "NPS: unresolved ratio vars"
    return None, "NPS: unrecognised RETURN shape"


# ═══════════════════════════════════════════════════════════════════════════
# M4 / M5 — CALCULATE([measure], <code filter>) VAR ratio (OTC KBIs)
# ═══════════════════════════════════════════════════════════════════════════
#
# DAX shape:
#   VAR _c1 = CALCULATE([Sum OTC FLTP], 'Fact_OTC'[bic_csubkbi] = "K…" | in {"K…","K…"})
#   VAR _c2 = CALCULATE([Sum OTC FLTP], 'Fact_OTC'[bic_csubkbi] = "K…")
#   [VAR _Div = DIVIDE(_c2, _c1)]
#   RETURN DIVIDE(_c1, _c2) | 1 - _Div | IF(ISBLANK(_Div), BLANK(), 1 - _Div)
#
# Each code-filtered CALCULATE([base sum], filter) → SUM(CASE WHEN <filter> THEN
# source.<basecol> END). The CALCULATE filter args (incl. IN {..}) are PRESERVED
# (M5: dropping them made SUM(fltp)/SUM(fltp) = always 1). The RETURN arithmetic
# (1 - x/y) is kept (M4: it was lost, leaving invalid _VarName SQL).


def _reduce_if_isblank(expr: str) -> str:
    """IF(ISBLANK(<var>), NULL|BLANK(), <else>) → <else>. BLANK() is already NULL
    after preclean. Paren-balanced extraction of the else-branch."""
    m = re.match(r"\s*IF\s*\(", expr, re.IGNORECASE)
    if not m:
        return expr
    open_paren = expr.index("(", m.start())
    depth = 0
    end = None
    for i in range(open_paren, len(expr)):
        if expr[i] == "(":
            depth += 1
        elif expr[i] == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        return expr
    args = _split_top_args(expr[open_paren + 1 : end])
    if len(args) == 3 and re.match(r"\s*ISBLANK\s*\(", args[0], re.IGNORECASE):
        blank = args[1].strip().upper().replace(" ", "")
        if blank in ("NULL", "BLANK()"):
            return args[2].strip()
    return expr


def _resolve_base_sum_col(translator, ref_name: str) -> str | None:
    """A measure ref that resolves to SUM(source.<col>) → return <col>."""
    res = translator._measure_resolutions.get(ref_name)
    if not res:
        return None
    base = (res.get("base_expr") or "").strip()
    m = re.fullmatch(r"SUM\s*\(\s*source\.(\w+)\s*\)", base, re.IGNORECASE)
    return m.group(1) if m else None


def match_calc_measure_code_ratio(dax: str, name: str) -> dict | None:
    if "CALCULATE" not in dax.upper() or "[" not in dax:
        return None
    var_map, ret_expr = _var_segments(dax)
    if not var_map or not ret_expr:
        return None
    has_calc_ref = any(
        re.match(r"\s*CALCULATE\s*\(\s*\[", v, re.IGNORECASE) for v in var_map.values()
    )
    if not has_calc_ref:
        return None
    return {"var_map": var_map, "return_expr": ret_expr}


def translate_calc_measure_code_ratio(match: dict, dax: str, table_key, translator):
    var_map = match["var_map"]
    ret = _reduce_if_isblank(match["return_expr"].strip())

    resolved: dict[str, str] = {}  # var → SQL aggregate
    divide_vars: dict[str, tuple[str, str]] = {}  # var → (num_var, den_var)

    for var, body in var_map.items():
        body = body.strip()
        cm = re.fullmatch(
            r"CALCULATE\s*\(\s*\[([^\]]+)\]\s*,\s*(.+?)\s*\)\s*",
            body,
            re.IGNORECASE | re.DOTALL,
        )
        if cm:
            ref_name, filt = cm.group(1), cm.group(2)
            cond_sql = cond_to_sql(filt)
            if cond_sql is None:
                continue
            base_col = _resolve_base_sum_col(translator, ref_name)
            if base_col:
                resolved[var] = f"SUM(CASE WHEN {cond_sql} THEN source.{base_col} END)"
            else:
                res = translator._measure_resolutions.get(ref_name)
                if not res or not res.get("base_expr"):
                    continue
                resolved[var] = f"{res['base_expr']} FILTER (WHERE {cond_sql})"
            continue
        dm = re.fullmatch(
            r"DIVIDE\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)\s*", body, re.IGNORECASE
        )
        if dm:
            divide_vars[var] = (dm.group(1), dm.group(2))

    def ratio_sql(num_var: str, den_var: str) -> str | None:
        n, d = resolved.get(num_var), resolved.get(den_var)
        if not n or not d:
            return None
        return f"{n} / NULLIF({d}, 0)"

    # RETURN DIVIDE(a, b)
    m = re.fullmatch(r"\s*DIVIDE\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)\s*", ret, re.IGNORECASE)
    if m:
        sql = ratio_sql(m.group(1), m.group(2))
        return (sql, "") if sql else (None, "OTC ratio: unresolved vars")
    # RETURN 1 - DIVIDE(a, b)
    m = re.fullmatch(
        r"\s*1\s*-\s*DIVIDE\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)\s*", ret, re.IGNORECASE
    )
    if m:
        sql = ratio_sql(m.group(1), m.group(2))
        return (f"1 - {sql}", "") if sql else (None, "OTC ratio: unresolved vars")
    # RETURN 1 - _Div  (where _Div = DIVIDE(a, b))
    m = re.fullmatch(r"\s*1\s*-\s*(\w+)\s*", ret)
    if m and m.group(1) in divide_vars:
        num_var, den_var = divide_vars[m.group(1)]
        sql = ratio_sql(num_var, den_var)
        return (f"1 - {sql}", "") if sql else (None, "OTC ratio: unresolved vars")
    # RETURN _Div  (a bare divide var)
    m = re.fullmatch(r"\s*(\w+)\s*", ret)
    if m and m.group(1) in divide_vars:
        num_var, den_var = divide_vars[m.group(1)]
        sql = ratio_sql(num_var, den_var)
        return (sql, "") if sql else (None, "OTC ratio: unresolved vars")
    return None, "OTC ratio: unrecognised RETURN shape"


# ═══════════════════════════════════════════════════════════════════════════
# M3 / M9 — CALCULATE(SUM(..) | [measure], <filter>+) with BLANK-aware inequality
# ═══════════════════════════════════════════════════════════════════════════
#
# DAX shape: CALCULATE( SUM('T'[col]) | [Measure], f1 [, f2 …] )
# Emits <agg> FILTER (WHERE f1 AND f2 …). Handles single-quoted table names, so
# filter columns get source-qualified (M9), and BLANK-passing <> becomes
# NULL-aware (M3). Deliberately only claims the shapes the bareword
# calculate_equality_filter can't: a quoted table, >1 filter, an inequality, or a
# measure-ref aggregate — so it never changes that converter's output.


def match_calc_filters(dax: str, name: str) -> dict | None:
    m = re.fullmatch(
        r"\s*CALCULATE\s*\((.*)\)\s*", dax.strip(), re.IGNORECASE | re.DOTALL
    )
    if not m:
        return None
    args = _split_top_args(m.group(1))
    if len(args) < 2:
        return None
    agg_arg, filters = args[0], args[1:]
    is_sum = re.fullmatch(rf"\s*SUM\s*\(\s*{_REF}\s*\)\s*", agg_arg, re.IGNORECASE)
    is_ref = re.fullmatch(r"\s*\[([^\]]+)\]\s*", agg_arg)
    if not is_sum and not is_ref:
        return None
    # Only claim SIMPLE-PREDICATE filters. A filter that is a FUNCTION call
    # (FILTER(...), ALL(...), SAMEPERIODLASTYEAR(...), USERELATIONSHIP(...) …) is a
    # table/context modifier the existing patterns (calculate_measure_ref,
    # sameperiodlastyear, …) own — bail so the registry falls through to them
    # instead of failing terminally on it. Validate every predicate up front so a
    # match ALWAYS yields SQL.
    conds: list[str] = []
    for f in filters:
        if re.match(r"\s*\w+\s*\(", f):  # a function invocation, not a predicate
            return None
        c = cond_to_sql(f)
        if c is None:
            return None
        conds.append(c)
    # Only claim the shapes the existing patterns botch: a quoted table (their
    # bareword regexes drop the predicate), an inequality (they aren't NULL-aware),
    # or >1 filter. A plain bareword single-predicate CALCULATE stays with the
    # existing calculate_equality_filter (CASE WHEN) / calculate_measure_ref (which
    # also merges the resolution's base_filters) converters.
    has_quote = "'" in dax
    has_ineq = any(op in dax for op in ("<>", "!="))
    if not (has_quote or has_ineq or len(filters) > 1):
        return None
    return {
        "sum_col": is_sum.group(1) if is_sum else None,
        "ref_name": is_ref.group(1) if is_ref else None,
        "conds": conds,
    }


def translate_calc_filters(match: dict, dax: str, table_key, translator):
    if match["sum_col"]:
        agg = f"SUM(source.{match['sum_col']})"
    else:
        res = translator._measure_resolutions.get(match["ref_name"])
        if not res or not res.get("base_expr"):
            return None, f"Cannot resolve [{match['ref_name']}]"
        agg = res["base_expr"]
    return f"{agg} FILTER (WHERE {' AND '.join(match['conds'])})", ""


# ═══════════════════════════════════════════════════════════════════════════
# M11 — latest-period semi-additive ratio (SKU weekly snapshot)
# ═══════════════════════════════════════════════════════════════════════════
#
# DAX shape:
#   VAR Latest… = MAX('Cal'[period])
#   VAR LatestWk = CALCULATE(MAX('Fact'[wk]), …)
#   VAR Filtered = FILTER('Fact', 'Fact'[wk] = LatestWk)
#   VAR A = CALCULATE(SUM('Fact'[a]), Filtered)  … (one or more numerator terms)
#   VAR C = CALCULATE(SUM('Fact'[c]), Filtered)
#   RETURN DIVIDE(A + B, C, 0)
#
# The latest-week FILTER is a semi-additive "value at the latest period" select —
# expressed as a metric-view window (semiadditive: last over the period column),
# so the measure body is just the additive ratio (no latest-week filter). The
# translator emits a ``__WINDOW__:order=…;range=current;semiadditive=last``
# sentinel that DaxTranslator turns into a ``window_spec``.


def match_latest_week_ratio(dax: str, name: str) -> dict | None:
    up = dax.upper()
    if "FILTER" not in up or "DIVIDE" not in up or "MAX" not in up:
        return None
    var_map, ret_expr = _var_segments(dax)
    if not var_map or not ret_expr:
        return None
    # The latest-period FILTER: FILTER('Fact', 'Fact'[wk] = <var>)
    filt_var = None
    wk_col = None
    for var, body in var_map.items():
        fm = re.fullmatch(
            rf"\s*FILTER\s*\(\s*(?:'[^']*'|\w+)\s*,\s*{_REF}\s*=\s*(\w+)\s*\)\s*",
            body,
            re.IGNORECASE,
        )
        if fm:
            filt_var, wk_col = var, fm.group(1)
            break
    if not filt_var:
        return None
    # Numerator/denominator sum terms filtered by that latest-period var.
    sum_cols: dict[str, str] = {}
    for var, body in var_map.items():
        sm = re.fullmatch(
            rf"\s*CALCULATE\s*\(\s*SUM\s*\(\s*{_REF_LOOSE}\s*\)\s*,\s*{re.escape(filt_var)}\s*\)\s*",
            body,
            re.IGNORECASE,
        )
        if sm:
            sum_cols[var] = sm.group(1)
    if not sum_cols:
        return None
    dm = re.search(r"DIVIDE\s*\((.*)\)", ret_expr, re.IGNORECASE | re.DOTALL)
    if not dm:
        return None
    dargs = _split_top_args(dm.group(1))
    if len(dargs) < 2:
        return None
    num_vars = [v.strip() for v in re.split(r"\+", dargs[0])]
    den_var = dargs[1].strip()
    zero_default = len(dargs) >= 3 and dargs[2].strip() == "0"
    if any(v not in sum_cols for v in num_vars) or den_var not in sum_cols:
        return None
    return {
        "sum_cols": sum_cols,
        "num_vars": num_vars,
        "den_var": den_var,
        "wk_col": wk_col,
        "zero_default": zero_default,
    }


def translate_latest_week_ratio(match: dict, dax: str, table_key, translator):
    sum_cols = match["sum_cols"]
    num_terms = [
        f"COALESCE(SUM(source.{_snake_col(sum_cols[v])}), 0)" for v in match["num_vars"]
    ]
    den_col = _snake_col(sum_cols[match["den_var"]])
    numerator = " + ".join(num_terms)
    if len(num_terms) > 1:
        numerator = f"({numerator})"
    ratio = f"{numerator} / NULLIF(SUM(source.{den_col}), 0)"
    if match["zero_default"]:
        ratio = f"COALESCE({ratio}, 0)"
    wk = _snake_col(match["wk_col"])
    sentinel = f"__WINDOW__:order={wk};range=current;semiadditive=last"
    return ratio, sentinel


def parse_window_sentinel(sentinel: str) -> dict:
    """Parse ``__WINDOW__:order=<col>;range=<r>;semiadditive=<s>`` into a
    window_spec dict (M11 latest-period semi-additive measures)."""
    body = sentinel[len("__WINDOW__:") :]
    params: dict[str, str] = {}
    for kv in body.split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            params[k.strip()] = v.strip()
    return {
        "order": params.get("order", "fiscper"),
        "range": params.get("range", "current"),
        "semiadditive": params.get("semiadditive", "last"),
    }


# Registered (name, match_fn, translate_fn) triples in priority order, consumed by
# DaxTranslator._register_patterns and prepended before the generic catch-alls.
# translate_fn takes (match, dax, table_key, translator).
DEFECT_PATTERNS = [
    (
        "otc_code_ratio",
        match_calc_measure_code_ratio,
        translate_calc_measure_code_ratio,
    ),
    ("nps_summarize_distinct", match_summarize_distinct, translate_summarize_distinct),
    ("latest_week_ratio", match_latest_week_ratio, translate_latest_week_ratio),
    ("calc_filters", match_calc_filters, translate_calc_filters),
]

# Pattern names that run in the llm_first fast-path (deterministic, exact,
# reproducible — verified against the customer ground-truth new_expr).
DEFECT_FAST_PATH = frozenset(name for name, _, _ in DEFECT_PATTERNS)
