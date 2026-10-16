"""Canonical fingerprints that prove a DAX measure and its SQL translation agree.

The validator's component checks report similarities and differences; on their
own they cannot tell "the same measure written another way" from "a measure
with a filter missing". EQUIVALENT therefore needs proof: both expressions are
reduced to the same canonical form, built from

* the shape: one aggregation, or a ratio of two (``DIVIDE`` / ``/``);
* per aggregation: its kind (sum, avg, count, ...), the normalized row
  expression it aggregates, and every filter conjunct that applies to it.

Identifiers are compared case- and punctuation-insensitively (``Sales[Order
Amount]`` == ``source.order_amount``); string literals are compared exactly.
Anything outside the supported shapes yields ``None`` ("cannot prove"), which
the validator reports as REVIEW or INVALID, never as EQUIVALENT.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable

Literal = tuple[str, str]
Conjunct = tuple[object, ...]
Fingerprint = tuple[object, ...]


@dataclass(frozen=True)
class AggSig:
    kind: str
    row: str
    filters: tuple[Conjunct, ...]


_DAX_KINDS = {
    "SUM": "sum",
    "AVERAGE": "avg",
    "MIN": "min",
    "MAX": "max",
    "COUNT": "count",
    "COUNTA": "count",
    "DISTINCTCOUNT": "count_distinct",
    "DISTINCTCOUNTNOBLANK": "count_distinct",
}
_DAX_ITERATORS = {
    "SUMX": "sum",
    "AVERAGEX": "avg",
    "MINX": "min",
    "MAXX": "max",
    "COUNTX": "count",
}
_DAX_COL = r"(?:'[^']+'|\w+)?\[[^\]]+\]"
_DAX_LIT = (
    r'"(?:[^"]|"")*"|-?\d+(?:\.\d+)?|TRUE\s*\(\s*\)|FALSE\s*\(\s*\)'
    r"|DATE\s*\(\s*\d+\s*,\s*\d+\s*,\s*\d+\s*\)"
)
_SQL_COL = r"(?:`[^`]+`|\w+)(?:\.(?:`[^`]+`|\w+))*"
_SQL_LIT = (
    r"'(?:[^']|'')*'|-?\d+(?:\.\d+)?|TRUE|FALSE|DATE\s*'[^']*'"
    r"|CAST\s*\(\s*'[^']*'\s+AS\s+DATE\s*\)|TO_DATE\s*\(\s*'[^']*'\s*\)"
    r"|MAKE_DATE\s*\(\s*\d+\s*,\s*\d+\s*,\s*\d+\s*\)"
)
_OPS = r"==|=|<>|!=|>=|<=|>|<"
_FLAGS = re.IGNORECASE | re.DOTALL


def _ident(name: str) -> str:
    return re.sub(r"[^0-9a-z]", "", name.lower())


def _sql_col(ref: str) -> str:
    return _ident(ref.split(".")[-1].strip("`"))


def _scan(text: str, quote: str) -> list[tuple[int, str, int]]:
    """``(index, char, depth)`` for every char outside string literals."""
    out = []
    depth = 0
    in_str = False
    for i, ch in enumerate(text):
        if ch == quote:
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        out.append((i, ch, depth))
    return out


def _split(text: str, sep: str, quote: str) -> list[str]:
    """Split on a top-level separator regex (outside parens and strings)."""
    pattern = re.compile(sep, re.IGNORECASE)
    parts, start = [], 0
    for i, _, depth in _scan(text, quote):
        if depth != 0 or i < start:
            continue
        m = pattern.match(text, i)
        if m and m.end() > i:
            parts.append(text[start:i])
            start = m.end()
    parts.append(text[start:])
    return [p.strip() for p in parts]


def _unwrap(text: str, quote: str) -> str:
    """Strip parentheses that wrap the whole expression."""
    t = text.strip()
    while t.startswith("(") and t.endswith(")"):
        if any(d == 0 and ch == ")" and i < len(t) - 1 for i, ch, d in _scan(t, quote)):
            break
        t = t[1:-1].strip()
    return t


def _call(text: str, quote: str) -> tuple[str, list[str]] | None:
    """``NAME(args)`` spanning the whole text -> (NAME upper-cased, args)."""
    m = re.match(r"([A-Za-z_][\w.]*)\s*\(", text)
    if not m or not text.endswith(")"):
        return None
    inner = text[m.end() : -1]
    if any(d < 0 for _, _, d in _scan(inner, quote)):
        return None
    return m.group(1).upper(), (_split(inner, ",", quote) if inner.strip() else [])


def _has_top_operator(text: str, quote: str) -> bool:
    return any(d == 0 and ch in "+-*/" for _, ch, d in _scan(text, quote))


def _literal(text: str) -> Literal:
    t = text.strip()
    if t and t[0] in "\"'":
        return ("s", t[1:-1].replace(t[0] * 2, t[0]))
    up = t.upper().replace(" ", "")
    if up in ("TRUE", "TRUE()"):
        return ("b", "true")
    if up in ("FALSE", "FALSE()"):
        return ("b", "false")
    nums = re.findall(r"\d+", t)
    iso = re.search(r"'(\d{4})-(\d{1,2})-(\d{1,2})'", t)
    if re.match(r"(DATE|MAKE_DATE)\s*\(", t, re.I) and len(nums) == 3:
        y, mo, d = (int(n) for n in nums)
    elif iso:
        y, mo, d = (int(n) for n in iso.groups())
    else:
        try:
            return ("n", repr(float(t)))
        except ValueError:
            return ("raw", t)
    return ("d", f"{y:04d}-{mo:02d}-{d:02d}")


def _items(text: str, lit: str) -> tuple[Literal, ...] | None:
    """Every literal of an IN list, or ``None`` if any item is not a literal."""
    item = re.compile(rf"\s*({lit})\s*(?:,|$)", re.IGNORECASE)
    found: list[Literal] = []
    pos = 0
    while pos < len(text.rstrip()):
        m = item.match(text, pos)
        if not m:
            return None
        found.append(_literal(m.group(1)))
        pos = m.end()
    return tuple(sorted(found)) or None


def _conjunct(text: str, col_of: Callable[[str], str], dialect: str) -> Conjunct:
    """Canonical ``(col, op, literal)``, ``(col, in, items)`` or an OR group.

    Anything else becomes a ``raw`` conjunct tagged with its dialect, so it can
    never match the other side.
    """
    quote = '"' if dialect == "dax" else "'"
    t = _unwrap(text, quote)
    arms = _split(t, r"\|\|" if dialect == "dax" else r"\|\||\bOR\b", quote)
    if len(arms) > 1:
        return ("or", tuple(sorted(repr(_conjunct(a, col_of, dialect)) for a in arms)))
    col_re, lit = (_DAX_COL, _DAX_LIT) if dialect == "dax" else (_SQL_COL, _SQL_LIT)
    m = re.fullmatch(rf"({col_re})\s*({_OPS})\s*({lit})", t, _FLAGS)
    if m:
        op = {"==": "=", "!=": "<>"}.get(m.group(2), m.group(2))
        return (col_of(m.group(1)), op, _literal(m.group(3)))
    if dialect == "dax":
        m = re.fullmatch(
            rf"(NOT\s*\(?\s*)?({col_re})\s+IN\s*\{{(.*)\}}\s*\)?", t, _FLAGS
        )
        col, neg, body = (m.group(2), m.group(1), m.group(3)) if m else ("", "", "")
    else:
        m = re.fullmatch(rf"({col_re})\s+(NOT\s+)?IN\s*\((.*)\)", t, _FLAGS)
        col, neg, body = (m.group(1), m.group(2), m.group(3)) if m else ("", "", "")
    items = _items(body, lit) if m else None
    if items:
        return (col_of(col), "not in" if neg else "in", items)
    return ("raw", dialect, t)


def _mixes(text: str, and_re: str, or_re: str, quote: str) -> bool:
    """Both a top-level AND and a top-level OR: AND binds tighter, so reading
    the parts as a flat conjunction would be wrong; callers refuse instead."""
    t = _unwrap(text, quote)
    return len(_split(t, and_re, quote)) > 1 and len(_split(t, or_re, quote)) > 1


def _sorted(ctx: tuple[Conjunct, ...], more: list[Conjunct]) -> tuple[Conjunct, ...]:
    return tuple(sorted((*ctx, *more), key=repr))


def _ratio(num: AggSig | None, den: AggSig | None) -> Fingerprint | None:
    return ("div", num, den) if num and den else None


def _row(text: str, token: str, col_of: Callable[[str], str]) -> str | None:
    """Row expression of columns, numbers and arithmetic only; else ``None``."""
    pattern = re.compile(rf"\s*({token}|\d+(?:\.\d+)?|[-+*/()])")
    out = []
    pos = 0
    while pos < len(text.rstrip()):
        m = pattern.match(text, pos)
        if not m:
            return None
        tok = m.group(1)
        if re.match(r"[A-Za-z_`'\[]", tok):
            if re.match(r"\s*\(", text[m.end() :]):
                return None  # a function call
            tok = col_of(tok)
        out.append(tok)
        pos = m.end()
    return "".join(out) or None


class _Dax:
    """DAX side; column mappings apply before identifiers are normalized."""

    def __init__(self, column_mappings: dict[str, str]):
        self.column_mappings = column_mappings

    def col(self, ref: str) -> str:
        name = ref[ref.index("[") + 1 : ref.rindex("]")]
        return _ident(self.column_mappings.get(name, name))

    def filters(self, arg: str) -> list[Conjunct] | None:
        """Conjuncts of one CALCULATE filter argument; ``None`` = unprovable."""
        t = _unwrap(arg, '"')
        call = _call(t, '"')
        if call:
            name, args = call
            if name != "FILTER" or len(args) != 2:
                return None  # ALL, KEEPFILTERS, USERELATIONSHIP ... change context
            if not re.fullmatch(r"'[^']+'|\w+", args[0]):
                return None
            t = args[1]
        if _mixes(t, r"&&", r"\|\|", '"'):
            return None  # && and || without parentheses: grouping is not proven
        return [_conjunct(c, self.col, "dax") for c in _split(t, r"&&", '"')]

    def _table_filters(self, arg: str) -> list[Conjunct] | None:
        return self.filters(arg) if "(" in arg else []

    def term(self, text: str, ctx: tuple[Conjunct, ...]) -> AggSig | None:
        call = _call(_unwrap(text, '"'), '"')
        if not call:
            return None
        name, args = call
        if name == "CALCULATE" and args:
            extra = self._calculate_filters(args[1:], ctx)
            return self.term(args[0], extra) if extra is not None else None
        if name in _DAX_ITERATORS and len(args) == 2:
            table = self._table_filters(args[0])
            row = _row(args[1], _DAX_COL, self.col)
            if table is None or row is None:
                return None
            return AggSig(_DAX_ITERATORS[name], row, _sorted(ctx, table))
        if name in _DAX_KINDS and len(args) == 1:
            row = _row(args[0], _DAX_COL, self.col)
            return AggSig(_DAX_KINDS[name], row, _sorted(ctx, [])) if row else None
        if name == "COUNTROWS" and len(args) == 1:
            table = self._table_filters(args[0])
            return (
                AggSig("count_rows", "*", _sorted(ctx, table))
                if table is not None
                else None
            )
        return None

    def _calculate_filters(
        self, args: list[str], ctx: tuple[Conjunct, ...]
    ) -> tuple[Conjunct, ...] | None:
        extra: list[Conjunct] = list(ctx)
        for f in args:
            conj = self.filters(f)
            if conj is None:
                return None
            extra += conj
        return tuple(extra)

    def fingerprint(
        self, text: str, ctx: tuple[Conjunct, ...] = ()
    ) -> Fingerprint | None:
        t = _unwrap(text, '"')
        call = _call(t, '"')
        if call and call[0] == "CALCULATE" and call[1]:
            inner = _call(_unwrap(call[1][0], '"'), '"')
            if inner and inner[0] == "DIVIDE":
                extra = self._calculate_filters(call[1][1:], ctx)
                return (
                    self.fingerprint(call[1][0], extra) if extra is not None else None
                )
        if call and call[0] == "DIVIDE" and len(call[1]) in (2, 3):
            alt = call[1][2] if len(call[1]) == 3 else "BLANK()"
            if not re.fullmatch(r"BLANK\s*\(\s*\)", alt, re.I):
                return None  # an alternate result changes the zero-denominator rows
            return _ratio(self.term(call[1][0], ctx), self.term(call[1][1], ctx))
        halves = _split(t, r"/", '"')
        if len(halves) == 2:
            return _ratio(self.term(halves[0], ctx), self.term(halves[1], ctx))
        if _has_top_operator(t, '"'):
            return None
        sig = self.term(t, ctx)
        return ("agg", sig) if sig else None


def _sql_conjuncts(text: str) -> list[Conjunct]:
    if _mixes(text, r"\bAND\b", r"\bOR\b", "'"):
        return [("raw", "sql-mixed", text)]  # AND binds tighter: never flattened
    return [_conjunct(c, _sql_col, "sql") for c in _split(text, r"\bAND\b", "'")]


def _sql_term(text: str) -> AggSig | None:
    t = _unwrap(text, "'")
    m = re.match(r"(SUM|AVG|MIN|MAX|COUNT)\s*\(", t, re.I)
    if not m:
        return None
    end = next(
        (i for i, ch, d in _scan(t, "'") if i >= m.end() and ch == ")" and d == 0), -1
    )
    if end < 0:
        return None
    kind, content, rest = m.group(1).lower(), t[m.end() : end].strip(), t[end + 1 :]
    filters: list[Conjunct] = []
    if rest.strip():
        fm = re.fullmatch(r"\s*FILTER\s*\(\s*WHERE\s+(.*)\)\s*", rest, _FLAGS)
        if not fm:
            return None
        filters += _sql_conjuncts(fm.group(1))
    case = re.fullmatch(
        r"CASE\s+WHEN\s+(.*?)\s+THEN\s+(.*?)\s+(?:ELSE\s+NULL\s+)?END", content, _FLAGS
    )
    if case:
        filters += _sql_conjuncts(case.group(1))
        content = case.group(2)
    if kind == "count" and content in ("*", "1"):
        return AggSig("count_rows", "*", _sorted((), filters))
    distinct = re.fullmatch(r"DISTINCT\s+(.*)", content, _FLAGS)
    if kind == "count" and distinct:
        kind, content = "count_distinct", distinct.group(1)
    coalesce = re.fullmatch(r"COALESCE\s*\((.*),\s*0\s*\)", content, _FLAGS)
    row = _row(coalesce.group(1) if coalesce else content, _SQL_COL, _sql_col)
    return AggSig(kind, row, _sorted((), filters)) if row else None


def sql_fingerprint(sql: str) -> Fingerprint | None:
    """Canonical form of a UC metric-view measure expression, or ``None``."""
    t = _unwrap(sql or "", "'")
    if not t or re.search(r"\b(OVER|JOIN|SELECT|BETWEEN)\b", t, re.I):
        return None
    call = _call(t, "'")
    if call and call[0] == "TRY_DIVIDE" and len(call[1]) == 2:
        return _ratio(_sql_term(call[1][0]), _sql_term(call[1][1]))
    halves = _split(t, r"/", "'")
    if len(halves) == 2:
        den = _unwrap(halves[1], "'")
        nullif = re.fullmatch(r"NULLIF\s*\((.*),\s*0\s*\)", den, _FLAGS)
        return _ratio(
            _sql_term(halves[0]), _sql_term(nullif.group(1) if nullif else den)
        )
    if _has_top_operator(t, "'"):
        return None
    sig = _sql_term(t)
    return ("agg", sig) if sig else None


def dax_fingerprint(
    dax: str, column_mappings: dict[str, str] | None = None
) -> Fingerprint | None:
    """Canonical form of a (VAR-free) DAX measure expression, or ``None``."""
    if not dax or re.search(r"\b(VAR|RETURN)\b", dax, re.I):
        return None
    return _Dax(column_mappings or {}).fingerprint(dax)


def var_used_inside_calculate(dax: str) -> bool:
    """True when a VAR is referenced inside a CALCULATE / CALCULATETABLE.

    A DAX variable is evaluated once, where it is defined; CALCULATE does not
    re-evaluate it under its own filters. Substituting the variable's text
    into the CALCULATE would apply filters the measure never applies, so such
    a measure cannot be fingerprinted.
    """
    names = re.findall(r"\bVAR\s+(\w+)\s*=", dax or "", re.IGNORECASE)
    if not names:
        return False
    for m in re.finditer(r"\bCALCULATE(?:TABLE)?\s*\(", dax, re.IGNORECASE):
        inside = dax[m.end() :]
        depth, end = 1, len(inside)
        for i, ch in enumerate(inside):
            depth += {"(": 1, ")": -1}.get(ch, 0)
            if depth == 0:
                end = i
                break
        if any(re.search(rf"\b{re.escape(n)}\b", inside[:end]) for n in names):
            return True
    return False
