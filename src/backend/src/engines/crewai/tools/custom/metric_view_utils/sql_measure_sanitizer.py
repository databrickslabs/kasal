"""SQL measure sanitizer — P5 transpiler correctness fixes.

A single post-translation cleanup applied to every translated/base measure's
``sql_expr``, so the fixes benefit measures regardless of whether they came from
the regex fast-path, the LLM translator, or a SWITCH decomposition. Each fix is
independent, idempotent, and conservative (a no-op when the pattern is absent).

Bugs addressed (from the CCHBC iom35 output review):
  1. ``x / NULLIF(1, 0)``  — a no-op division by a literal 1 (the denominator
     never resolved). Strip it: the value is just ``x``.
  2. ``expr / NULLIF(expr, 0)`` — a self-division that always yields 1 (or NULL).
     Collapse to the literal ``1`` with a marker comment on the measure.
  3. Base measures not NULL-safe — wrap a bare ``SUM(source.col)`` as
     ``SUM(COALESCE(source.col, 0))`` so NULLs sum to 0 (matches the customer's
     ground-truth convention). Only applied to base measures.
"""

from __future__ import annotations

import re

# x / NULLIF(1, 0)  → x   (denominator is a literal 1; the divide is a no-op)
_NULLIF_ONE = re.compile(r"\s*/\s*NULLIF\(\s*1\s*,\s*0\s*\)")

# SUM(source.col)  (a single bare aggregate over a source column, no COALESCE)
_BARE_SUM_SOURCE = re.compile(r"\bSUM\(\s*(source\.\w+)\s*\)", re.IGNORECASE)


def strip_nullif_one(sql: str) -> str:
    """Remove no-op ``/ NULLIF(1, 0)`` divisions (bug 1)."""
    if not sql:
        return sql
    return _NULLIF_ONE.sub("", sql)


def _split_top_level_divide(sql: str):
    """Split ``num / NULLIF(den, 0)`` at the top level. Returns (num, den) with
    surrounding parens stripped, or None when the shape isn't a single ratio."""
    m = re.search(r"^\s*(.*?)\s*/\s*NULLIF\(\s*(.*)\s*,\s*0\s*\)\s*$", sql, re.DOTALL)
    if not m:
        return None
    num, den = m.group(1).strip(), m.group(2).strip()

    def _unwrap(x: str) -> str:
        while x.startswith("(") and x.endswith(")"):
            # only unwrap if the parens are balanced as an outer pair
            depth = 0
            balanced = True
            for i, ch in enumerate(x):
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0 and i != len(x) - 1:
                        balanced = False
                        break
            if balanced:
                x = x[1:-1].strip()
            else:
                break
        return x

    return _unwrap(num), _unwrap(den)


def detect_self_division(sql: str) -> bool:
    """True when ``sql`` is ``expr / NULLIF(expr, 0)`` (numerator == denominator,
    ignoring whitespace) — a self-division that is always 1 or NULL (bug 2)."""
    if not sql or "/ NULLIF(" not in sql.replace("/NULLIF(", "/ NULLIF("):
        # normalise a little then bail early if there's no ratio at all
        if "NULLIF(" not in (sql or ""):
            return False
    parts = _split_top_level_divide(sql)
    if not parts:
        return False
    num, den = parts
    _norm = lambda s: re.sub(r"\s+", "", s)
    return bool(num) and _norm(num) == _norm(den)


def coalesce_wrap_base(sql: str) -> str:
    """Wrap bare ``SUM(source.col)`` as ``SUM(COALESCE(source.col, 0))`` for
    NULL-safety (bug 3). Only touches aggregates that are not already wrapped."""
    if not sql or "SUM(" not in sql.upper():
        return sql

    def _repl(m: re.Match) -> str:
        col = m.group(1)
        return f"SUM(COALESCE({col}, 0))"

    return _BARE_SUM_SOURCE.sub(_repl, sql)


# PROP-3a/4a: markers of a measure that would emit a SILENTLY-WRONG or invalid
# result. Such a measure must be demoted to untranslatable (an honest TODO) rather
# than shipped — a wrong number that runs is worse than a documented gap.
_SILENT_WRONG_CHECKS: list[tuple[str, "re.Pattern[str]"]] = [
    # empty numerator/denominator: "/ NULLIF(, 0)" or leading "/ ..."
    ("empty ratio operand", re.compile(r"NULLIF\(\s*,|^\s*/\s")),
    # 3+-arg NULLIF (malformed): NULLIF(x, 0, 0)
    ("malformed NULLIF (3 args)", re.compile(r"NULLIF\([^()]*,[^()]*,[^()]*\)")),
    # unresolved DAX measure ref or placeholder left in the SQL
    ("unresolved DAX measure ref", re.compile(r"\bTODO\b|/\*\s*UNRESOLVED|\b\w+\[[^\]]+\]")),
    # surviving prior-year time-intel (would silently emit the current-period value)
    ("prior-year time-intel not applied",
     re.compile(r"SAMEPERIODLASTYEAR|DATEADD|PARALLELPERIOD|SAMEPERIOD", re.IGNORECASE)),
    # raw DAX constructs that never got translated
    ("untranslated DAX (SUMX/FILTER/CALCULATE)",
     re.compile(r"\b(SUMX|CALCULATE)\s*\(|FILTER\s*\(\s*\w+\s*,", re.IGNORECASE)),
    # dangling bare single-letter var identifiers (a, b) left in arithmetic
    ("dangling DAX var identifier",
     re.compile(r"(?<![\w.])[a-z]\d?(?![\w.(])\s*[-+/*]|[-+/*]\s*(?<![\w.])[a-z]\d?(?![\w.(])")),
    # dangling multi-letter DAX var names (res1, res2, std, etd, num, den) — a
    # var whose CALCULATE body never got inlined, left as a bare identifier
    # adjacent to arithmetic. Bounded to a small known set to avoid matching real
    # SQL identifiers/columns.
    ("dangling DAX var name",
     re.compile(r"(?<![\w.])(res\d+|std|etd|num|den|val\d+)(?![\w.(])\s*[-+/*]"
                r"|[-+(]\s*(?<![\w.])(res\d+|std|etd|num|den|val\d+)(?![\w.(])",
                re.IGNORECASE)),
]


def detect_lost_dax_component(dax: str, sql: str) -> str | None:
    """Compare the ORIGINAL DAX against the GENERATED SQL and return a reason when
    the SQL silently DROPPED a component the DAX clearly had — producing valid
    SQL that returns a WRONG number.

    This catches the class ``detect_silent_wrong`` cannot: output that *looks*
    clean (a well-formed ``SUM(...) FILTER(...)``) but is missing a ratio
    denominator, a prior-year shift, or an exclusion filter that the DAX
    specified. Conservative by design — each check requires strong evidence the
    DAX had the feature AND the SQL lacks any trace of it, so faithful
    translations (the majority) are never flagged.
    """
    if not dax or not sql:
        return None
    du = dax.upper()
    su = sql.upper()

    # 1. Prior-year time-intelligence present in DAX but NOT reflected in SQL.
    #    (A faithful PY translation would carry a window spec, a calendar self-join
    #    on a *_py column, a LAG(), or a DATE_ADD — none present ⇒ silently emits
    #    the current-period value, identical to the non-PY sibling.)
    if re.search(r'SAMEPERIODLASTYEAR|PARALLELPERIOD|PREVIOUSYEAR|DATEADD', du):
        if not re.search(r'\bLAG\s*\(|\bLEAD\s*\(|DATE_ADD|_PY\b|OVER\s*\(|WINDOW', su):
            return ('prior-year shift dropped — SQL emits the current-period value '
                    '(SAMEPERIODLASTYEAR/DATEADD in DAX not reflected in SQL)')

    # 2. DAX is a DIVIDE/ratio but the SQL has no division at all → the whole
    #    denominator was dropped, leaving a bare numerator.
    if re.search(r'\bDIVIDE\s*\(', du):
        if '/' not in sql and 'NULLIF' not in su:
            return ('ratio denominator dropped — DAX has DIVIDE(...) but SQL has no '
                    'division (numerator emitted alone)')

    # 3. DAX has an exclusion predicate (<> / != / NOT ... IN) but the SQL carries
    #    no exclusion of any form → the exclusion was dropped and totals will
    #    include rows the DAX excludes.
    dax_has_excl = (
        '<>' in dax or '!=' in dax
        or re.search(r'NOT\s+[\w\'.\[\]]+\s+IN', du) is not None
    )
    if dax_has_excl:
        if '<>' not in sql and 'NOT IN' not in su and '!=' not in sql:
            return ('exclusion filter dropped — DAX excludes values (<> / NOT IN) '
                    'but SQL has no exclusion (totals will include excluded rows)')

    # 4. Multi-block arithmetic collapse: the DAX binds >=2 aggregate blocks to
    #    vars and the RETURN combines them with + / - (NOT inside a DIVIDE), but
    #    the SQL emitted a single aggregate term — i.e. only the first block (`a`)
    #    survived and the `- b` / `+ b` was dropped. Produces clean-looking SQL
    #    that is silently wrong (e.g. cost_to_supply = a - b emitted as just a).
    #    Only fires on the non-ratio case (the DIVIDE case is covered by check 2).
    if '/' not in sql and 'DIVIDE' not in du:
        # Count aggregate blocks the DAX evaluates (CALCULATE/SUMX wrappers).
        agg_blocks = len(re.findall(r'\bCALCULATE\s*\(|\bSUMX\s*\(', du))
        # The RETURN (or whole expr) combines vars/terms with + or - at the top.
        ret_m = re.search(r'\bRETURN\b(.+)$', dax, re.I | re.S)
        ret_expr = ret_m.group(1) if ret_m else dax
        combines_terms = re.search(r'\b\w+\s*[-+]\s*\w+', ret_expr) is not None
        # The SQL has a single aggregate term: one FILTER (or none) and no top-level
        # + / - joining two aggregates.
        sql_agg_terms = len(re.findall(r'\bFILTER\b', su)) or su.count('SUM(')
        sql_combines = re.search(r'\)\s*[-+]\s*(?:\(|SUM|COUNT|AVG|MIN|MAX)', su) is not None
        if agg_blocks >= 2 and combines_terms and sql_agg_terms <= 1 and not sql_combines:
            return ('additive term dropped — DAX combines multiple aggregate blocks '
                    'with + / - (e.g. a - b) but SQL emitted a single term '
                    '(the other term was silently dropped)')

    # 5. Share-of-total collapse: DAX is a ratio whose denominator removes filter
    #    context with ALL()/ALLSELECTED (CALCULATE([M], ALL(dim))), but the SQL
    #    has no coarser-LOD signal (no `window`/`range: all`/`MEASURE(*_all*)`)
    #    AND its two ratio sides are identical → the "total" side collapsed to the
    #    same value as the numerator, so the share is a constant 1.0. Only fires
    #    when the DAX genuinely divides (a share-of-total, not a bare ALL total).
    dax_has_all_total = re.search(r'\bALL\s*\(|\bALLSELECTED\s*\(', du) is not None
    if dax_has_all_total and re.search(r'\bDIVIDE\s*\(', du) and '/' in sql:
        has_lod_signal = (
            'WINDOW' in su or 'RANGE: ALL' in su or 'RANGE:ALL' in su
            or re.search(r'MEASURE\s*\(\s*\w*_ALL\w*\s*\)', su) is not None
        )
        if not has_lod_signal:
            # Compare the two sides of the top-level division.
            sides = re.split(r'/\s*NULLIF|/', sql, maxsplit=1)
            if len(sides) == 2:
                norm = lambda s: re.sub(r'[^a-z0-9<>=]', '', s.lower().replace('nullif', '').rstrip(', 0)'))
                if norm(sides[0]) and norm(sides[0]) == norm(sides[1]):
                    return ('share-of-total collapsed — DAX removes filter context with '
                            'ALL()/ALLSELECTED for the denominator but SQL has no coarser-LOD '
                            'window (range: all); numerator == denominator so the share is a '
                            'constant 1.0')

    return None


def detect_silent_wrong(sql: str) -> str | None:
    """Return a short reason string when ``sql`` would silently produce a wrong or
    invalid result (see ``_SILENT_WRONG_CHECKS``), else None. Used to demote a
    measure to untranslatable-with-TODO instead of emitting bad SQL."""
    if not sql:
        return None
    for reason, pat in _SILENT_WRONG_CHECKS:
        if pat.search(sql):
            return reason
    return None


def sanitize_measure_sql(sql: str, *, is_base: bool) -> tuple[str | None, str | None]:
    """Apply all P5 sanitizers to a measure's SQL.

    Returns ``(new_sql, note)``. ``note`` is a short marker string when the
    sanitizer changed the semantics in a way worth surfacing (self-division), else
    None. ``new_sql`` is None only when the input was None.
    """
    if not sql:
        return sql, None

    note: str | None = None

    # Bug 2 first: a self-division is a translation error — flag before we mutate
    # the expression (stripping NULLIF(1,0) could mask it).
    if detect_self_division(sql):
        note = "self-division (numerator == denominator) — always 1; likely a translation error"

    # Bug 1: no-op division by NULLIF(1, 0).
    sql = strip_nullif_one(sql)

    # Bug 3: NULL-safe base aggregates.
    if is_base:
        sql = coalesce_wrap_base(sql)

    return sql, note
