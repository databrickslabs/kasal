"""Which Power Query M steps the deterministic M -> SQL resolver reproduces.

``resolve_mquery_to_sql`` compiles a catalog-browse M source into
``SELECT ... FROM catalog.schema.table``. That SQL is only faithful when EVERY
applied step is one it reproduces: the connector call, its ``{[Name=...]}[Data]``
navigation, and steps that touch columns but never rows. A row filter
(``Table.SelectRows``), a dedup (``Table.Distinct``), a group, a join, a
computed column, or anything else changes which rows the table holds; compiling
past one would make every measure aggregate rows Power BI excluded.

So this module walks the ``let`` steps against an explicit allowlist and names
every step it cannot reproduce. The resolver returns ``None`` ("not
resolvable") when that list is not empty, and the caller falls back to the raw
M (or the LLM recovery path) instead of shipping a lossy ``SELECT *``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .mquery_let_evaluator import (
    _extract_call_args,
    _extract_let_block,
    _parse_binding,
    _skip_string,
)

# Catalog-browse connectors (same families as mquery_parser's gate) plus
# Sql.Database(s) table navigation. Only valid as the FIRST step.
_CONNECTOR_RE = re.compile(
    r"(Databricks\.\w+|Snowflake\.Databases|PostgreSQL\.Database|MySQL\.Database|"
    r"Oracle\.Database|AmazonRedshift\.Database|GoogleBigQuery\.Database|"
    r"Sql\.Databases?)"
)

# Steps that change columns only. Each is reproduced by ``build_select_clause``
# (projection, alias, exclusion) or has no SQL effect (column order, column
# types: the Unity Catalog table already carries its own types).
COLUMN_ONLY_STEPS = frozenset(
    {
        "Table.SelectColumns",
        "Table.RenameColumns",
        "Table.RemoveColumns",
        "Table.ReorderColumns",
        "Table.TransformColumnTypes",
    }
)

# One navigation accessor: {[Name="x", Kind="Table"]}[Data]
_NAV = r"\{\s*\[[^\[\]{}()]*\]\s*\}\s*\[\s*Data\s*\]"
_NAV_TAIL_RE = re.compile(rf"\s*(?:{_NAV}\s*)*")
_REF = r'#"[^"]*"|[A-Za-z_]\w*'
_NAV_STEP_RE = re.compile(rf"(?P<ref>{_REF})\s*(?:{_NAV}\s*)+")
_CALL_HEAD_RE = re.compile(r"(?P<fn>[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+)\s*\(")
_STRING_RE = re.compile(r'"(?:[^"]|"")*"')


@dataclass
class MStep:
    """One ``let`` binding, classified."""

    name: str
    kind: str  # "connector" | "navigation" | "column" | "reference"
    function: str = ""
    args: list[str] = field(default_factory=list)


def _ref_name(expr: str) -> str | None:
    """``#"Quoted Name"`` or a bare identifier -> the name; else ``None``."""
    e = expr.strip()
    m = re.fullmatch(r'#"([^"]*)"', e)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z_]\w*", e):
        return e
    return None


def _call_end(text: str, open_idx: int) -> int | None:
    """Index of the ``)`` closing the call whose ``(`` is at ``open_idx``."""
    depth = 0
    i = open_idx
    while i < len(text):
        ch = text[i]
        if ch == '"':
            i = _skip_string(text, i)
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def _has_logic(text: str) -> bool:
    """True when ``text`` (string literals removed) holds a call or a lambda."""
    bare = _STRING_RE.sub('""', text)
    bare = re.sub(r"#duration\s*\([^()]*\)", "", bare)
    return "(" in bare or "=>" in bare or bool(re.search(r"\beach\b", bare))


def _classify(
    name: str, expr: str, prev: str | None, source_is_reference: bool = False
) -> MStep | None:
    """Classify one step against the allowlist; ``None`` = not reproduced."""
    e = expr.strip()
    if prev is None and source_is_reference:
        # The table reads another named query; the caller resolves that one.
        return MStep(name, "reference") if _ref_name(e) else None
    head = _CALL_HEAD_RE.match(e)
    if head:
        fn = head.group("fn")
        end = _call_end(e, head.end() - 1)
        if end is None:
            return None
        args = _extract_call_args(e, head.end()) or []
        tail = e[end + 1 :]
        if prev is None and _CONNECTOR_RE.fullmatch(fn):
            # The connector itself, optionally with its navigation inline. A
            # native-SQL option (Sql.Database(..., [Query="..."])) is a query,
            # not a table, so it is not reproduced here.
            bare_args = _STRING_RE.sub('""', ",".join(args))
            if _has_logic(",".join(args)) or re.search(r"\bQuery\s*=", bare_args):
                return None
            if not _NAV_TAIL_RE.fullmatch(tail):
                return None
            return MStep(name, "connector", fn, args)
        if fn in COLUMN_ONLY_STEPS and not tail.strip() and args:
            if _ref_name(args[0]) != prev or any(_has_logic(a) for a in args[1:]):
                return None
            return MStep(name, "column", fn, args)
        return None
    nav = _NAV_STEP_RE.fullmatch(e)
    if nav and prev is not None and _ref_name(nav.group("ref")) == prev:
        return MStep(name, "navigation")
    if prev is not None and _ref_name(e) == prev:
        return MStep(name, "reference")
    return None


def _raw_steps(text: str) -> tuple[list[tuple[str, str]], str | None, str | None]:
    """``(steps, final_ref, error)`` for a ``let`` block or a single expression."""
    if not re.match(r"let\b", text, re.IGNORECASE):
        return [("", text)], None, None
    parsed = _extract_let_block(text)
    if not parsed:
        return [], None, "M let block could not be parsed"
    bindings, final_expr = parsed
    steps: list[tuple[str, str]] = []
    for binding in bindings:
        pb = _parse_binding(binding)
        if not pb:
            return [], None, f"unparsed step: {binding.strip()[:80]}"
        steps.append(pb)
    final_ref = _ref_name(final_expr)
    if final_ref is None:
        return [], None, f"final expression is not a step: {final_expr[:80]}"
    return steps, final_ref, None


def _describe(name: str, expr: str) -> str:
    head = _CALL_HEAD_RE.match(expr.strip())
    return f"{name or 'expression'}: {head.group('fn') if head else expr.strip()[:60]}"


def classify_m_steps(
    mquery: str, source_is_reference: bool = False
) -> tuple[list[MStep], list[str]]:
    """Walk ``mquery``'s steps in order.

    Returns ``(steps, unhandled)``: the classified steps, and a description of
    every step the resolver does not reproduce. ``unhandled`` empty means the
    whole M is reproducible; anything else means "not resolvable".
    ``source_is_reference``: the first step reads another named query (the
    reference-following path) instead of calling a connector.
    """
    text = (mquery or "").strip()
    if not text:
        return [], ["empty M expression"]
    raw_steps, final_ref, error = _raw_steps(text)
    if error:
        return [], [error]
    steps: list[MStep] = []
    unhandled: list[str] = []
    prev: str | None = None
    for name, expr in raw_steps:
        step = _classify(name, expr, prev, source_is_reference)
        if step is None:
            unhandled.append(_describe(name, expr))
        else:
            steps.append(step)
        prev = name
    if final_ref is not None and final_ref != raw_steps[-1][0]:
        unhandled.append(f"result is step {final_ref!r}, not the last step")
    return steps, unhandled


# After a native query only steps with no effect on the SQL result may follow:
# the embedded SQL is shipped verbatim, so anything else would be dropped.
_NO_EFFECT_STEPS = frozenset({"Table.TransformColumnTypes", "Table.ReorderColumns"})


def steps_after_native_query(mquery: str) -> list[str]:
    """Steps after the ``Value.NativeQuery`` binding that change the result.

    Empty when the native query's SQL alone reproduces the table.
    """
    raw_steps, final_ref, error = _raw_steps((mquery or "").strip())
    if error:
        return [error]
    unhandled: list[str] = []
    prev: str | None = None
    seen_native = False
    for name, expr in raw_steps:
        if seen_native:
            step = _classify(name, expr, prev)
            if step is None or (
                step.kind == "column" and step.function not in _NO_EFFECT_STEPS
            ):
                unhandled.append(_describe(name, expr))
        elif "Value.NativeQuery" in expr:
            seen_native = True
        prev = name
    if final_ref is not None and raw_steps and final_ref != raw_steps[-1][0]:
        unhandled.append(f"result is step {final_ref!r}, not the last step")
    return unhandled


# Type conversions that can change values, not only labels: whole-number
# types round, Currency rounds to 4 decimals, date / time drop part of a
# timestamp (which breaks date-key joins), text and logical change what an
# aggregate sees. The SQL source does not apply them, so they are reported.
_VALUE_CHANGING_TYPE = re.compile(
    r"\bInt(?:8|16|32|64)\.Type\b|\bCurrency\.Type\b|\b(?:Date|Time|Text|Logical)\.Type\b"
    r"|\btype\s+(?:date|time|text|logical)\b"
)


# Spark SQL for each conversion. Power Query's Int64.From / Currency.From round
# half to even by default (RoundingMode.ToEven), which BROUND matches; Date
# drops the time; Logical maps 0 to false. Text and Time have no exact
# equivalent (Text.From formats by culture; Spark has no TIME type).
_INT_SQL_TYPE = {"8": "TINYINT", "16": "SMALLINT", "32": "INT", "64": "BIGINT"}


def type_change_sql(column: str, type_text: str) -> str | None:
    """The SQL that reproduces one conversion, or ``None`` when none does."""
    col = _quote(column)
    m = re.search(r"\bInt(8|16|32|64)\.Type\b", type_text)
    if m:
        return f"CAST(BROUND({col}) AS {_INT_SQL_TYPE[m.group(1)]})"
    if re.search(r"\bCurrency\.Type\b", type_text):
        return f"BROUND({col}, 4)"
    if re.search(r"\bDate\.Type\b|\btype\s+date\b", type_text):
        return f"CAST({col} AS DATE)"
    if re.search(r"\bLogical\.Type\b|\btype\s+logical\b", type_text):
        return f"CAST({col} AS BOOLEAN)"
    return None


def type_change_notes(steps: list[MStep]) -> list[str]:
    """One note per ``Table.TransformColumnTypes`` entry whose type can change
    the column's values: ``"amount -> Int64.Type: apply CAST(BROUND(amount)
    AS BIGINT)"``, or that no SQL reproduces it exactly."""
    notes: list[str] = []
    for step in steps:
        if step.function != "Table.TransformColumnTypes" or len(step.args) < 2:
            continue
        for col, typ in re.findall(
            r'\{\s*"((?:[^"]|"")*)"\s*,\s*([^{}]+?)\s*\}', step.args[1]
        ):
            if not _VALUE_CHANGING_TYPE.search(typ):
                continue
            column = col.replace('""', '"')
            sql = type_change_sql(column, typ)
            how = f"apply {sql}" if sql else "no exact SQL equivalent"
            notes.append(f"{column} -> {typ.strip()}: {how}")
    return notes


def _quote(name: str) -> str:
    if re.fullmatch(r"[A-Za-z_]\w*", name):
        return name
    return "`" + name.replace("`", "``") + "`"


def _names(arg: str) -> list[str]:
    return [s[1:-1].replace('""', '"') for s in _STRING_RE.findall(arg)]


def _pairs(arg: str) -> list[tuple[str, str]] | None:
    names = _names(arg)
    if len(names) % 2:
        return None
    return list(zip(names[0::2], names[1::2]))


def build_select_clause(steps: list[MStep]) -> str | None:
    """The SELECT list the column-only steps produce, in order.

    ``None`` when a step references a column the projection no longer holds
    (the result could not be reproduced faithfully).
    """
    cols: list[tuple[str, str]] | None = None  # (source, output); None = *
    renamed: dict[str, str] = {}  # source -> output, while still SELECT *
    removed: list[str] = []
    for step in steps:
        if step.kind != "column" or len(step.args) < 2:
            continue
        if step.function == "Table.SelectColumns":
            inverse = {out: src for src, out in renamed.items()}
            current = cols
            picked: list[tuple[str, str]] = []
            for out in _names(step.args[1]):
                if current is not None:
                    src = next((s for s, o in current if o == out), None)
                elif out in removed:
                    src = None
                else:
                    src = inverse.get(out, out if out not in renamed else None)
                if src is None:
                    return None
                picked.append((src, out))
            cols, renamed, removed = picked, {}, []
        elif step.function == "Table.RenameColumns":
            applied = _apply_renames(cols, renamed, removed, _pairs(step.args[1]))
            if applied is None:
                return None
            cols, renamed = applied
        elif step.function == "Table.RemoveColumns":
            gone = set(_names(step.args[1]))
            if cols is not None:
                cols = [(s, o) for s, o in cols if o not in gone]
            else:
                for out in gone:
                    src = next((s for s, o in renamed.items() if o == out), out)
                    renamed.pop(src, None)
                    removed.append(src)
    if cols is not None:
        return ", ".join(
            _quote(s) if s == o else f"{_quote(s)} AS {_quote(o)}" for s, o in cols
        )
    if not renamed and not removed:
        return "*"
    excluded = ", ".join(_quote(c) for c in [*removed, *renamed])
    aliases = "".join(f", {_quote(s)} AS {_quote(o)}" for s, o in renamed.items())
    return f"* EXCEPT ({excluded}){aliases}"


def _apply_renames(
    cols: list[tuple[str, str]] | None,
    renamed: dict[str, str],
    removed: list[str],
    pairs: list[tuple[str, str]] | None,
) -> tuple[list[tuple[str, str]] | None, dict[str, str]] | None:
    """One ``Table.RenameColumns`` step. All its pairs read the step's INPUT
    names at once (a swap ``a->b, b->a`` is a swap, not ``a->a``). ``None``
    when an old name does not exist or two columns would get one name."""
    if not pairs or len({new for _, new in pairs}) != len(pairs):
        return None
    mapping = dict(pairs)
    if cols is not None:
        if not set(mapping) <= {o for _, o in cols}:
            return None
        cols = [(s, mapping.get(o, o)) for s, o in cols]
        outputs = [o for _, o in cols]
        return (cols, renamed) if len(set(outputs)) == len(outputs) else None
    current = {out: src for src, out in renamed.items()}  # output -> source
    sources: dict[str, str] = {}
    for old in mapping:
        src = current.get(old) or (
            old if old not in renamed and old not in removed else None
        )
        if src is None:
            return None
        sources[old] = src
    updated = dict(renamed)
    for old, new in mapping.items():
        updated[sources[old]] = new
    taken = {o for o in updated.values()}
    if len(taken) != len(updated):
        return None
    return None, {s: o for s, o in updated.items() if s != o}
