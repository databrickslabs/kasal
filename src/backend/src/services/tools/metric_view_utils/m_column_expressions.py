"""Power Query column expressions (the body of ``each …``) to Spark SQL.

Covers the shapes that derive keys and labels in dimension tables, e.g. a company code padded
to four characters::

    each if Text.Length(Text.From([co_code_bw])) = 3 then "0" & Text.From([co_code_bw])
         else Text.From([co_code_bw])

Supported: ``[column]``, text and number literals, ``null``, ``true``/``false``, ``&``, the
comparisons ``= <> < <= > >=``, ``and``/``or``/``not``, ``if … then … else …`` (nested),
``Text.From``, ``Text.Length``, ``Text.Upper``/``Lower``/``Trim``, ``Text.Start``/``End``,
``Text.PadStart``/``PadEnd`` and ``Number.From``. Anything else returns ``None``: the caller
records the step as a TODO instead of shipping a half-translated expression.

Pure functions: no I/O.
"""

from __future__ import annotations

import re

from .dax_condition_sql import _sql_str_literal

_TOKEN = re.compile(
    r'\s*(?:(?P<str>"(?:[^"]|"")*")|(?P<col>\[[^\]]+\])|(?P<num>\d+(?:\.\d+)?)'
    r"|(?P<op><>|<=|>=|=|<|>|&|\+|-|\*|/|\(|\)|,)|(?P<word>[A-Za-z_][\w.]*))"
)
_FUNCTIONS = {
    "Text.From": ("string", 1),
    "Text.Length": ("length", 1),
    "Text.Upper": ("upper", 1),
    "Text.Lower": ("lower", 1),
    "Text.Trim": ("trim", 1),
    "Text.Start": ("left", 2),
    "Text.End": ("right", 2),
    "Text.PadStart": ("lpad", 3),
    "Text.PadEnd": ("rpad", 3),
    "Number.From": ("double", 1),
}
_COLUMN = re.compile(r"^[A-Za-z_]\w*$")


class _Unsupported(ValueError):
    pass


def _tokens(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    pos = 0
    text = text.strip()
    while pos < len(text):
        match = _TOKEN.match(text, pos)
        if not match or match.end() == pos:
            raise _Unsupported(text[pos:])
        kind = match.lastgroup or ""
        out.append((kind, match.group(kind)))
        pos = match.end()
    return out


class _Parser:
    """Recursive descent over M's operator precedence (or < and < not < comparison < & / + -)."""

    def __init__(self, tokens: list[tuple[str, str]]) -> None:
        self.tokens = tokens
        self.i = 0

    def peek(self) -> tuple[str, str]:
        return self.tokens[self.i] if self.i < len(self.tokens) else ("", "")

    def take(self, value: str | None = None) -> tuple[str, str]:
        token = self.peek()
        if value is not None and token[1] != value:
            raise _Unsupported(f"expected {value!r}, got {token[1]!r}")
        self.i += 1
        return token

    def expression(self) -> str:
        if self.peek() == ("word", "if"):
            self.take()
            cond = self.expression()
            self.take("then")
            then = self.expression()
            self.take("else")
            other = self.expression()
            return f"CASE WHEN {cond} THEN {then} ELSE {other} END"
        return self.disjunction()

    def disjunction(self) -> str:
        left = self.conjunction()
        while self.peek() == ("word", "or"):
            self.take()
            left = f"({left} OR {self.conjunction()})"
        return left

    def conjunction(self) -> str:
        left = self.negation()
        while self.peek() == ("word", "and"):
            self.take()
            left = f"({left} AND {self.negation()})"
        return left

    def negation(self) -> str:
        if self.peek() == ("word", "not"):
            self.take()
            return f"(NOT {self.negation()})"
        return self.comparison()

    def comparison(self) -> str:
        left = self.additive()
        if self.peek()[1] in ("=", "<>", "<", "<=", ">", ">="):
            op = self.take()[1]
            right = self.additive()
            if "NULL" in (left, right) and op in ("=", "<>"):
                # M's [x] = null is true for a null x; SQL's x = NULL never is.
                value = right if left == "NULL" else left
                return f"({value} IS {'NOT ' if op == '<>' else ''}NULL)"
            return f"({left} {op} {right})"
        return left

    def additive(self) -> str:
        left = self.primary()
        while self.peek()[1] in ("&", "+", "-", "*", "/"):
            op = self.take()[1]
            right = self.primary()
            left = f"concat({left}, {right})" if op == "&" else f"({left} {op} {right})"
        return left

    def primary(self) -> str:
        kind, value = self.take()
        if kind == "str":
            return _sql_str_literal(value[1:-1].replace('""', '"'))
        if kind == "num":
            return value
        if kind == "col":
            name = value[1:-1].strip()
            if not _COLUMN.match(name):
                raise _Unsupported(name)
            return name
        if value == "(":
            inner = self.expression()
            self.take(")")
            return f"({inner})"
        if kind == "word" and value in ("null", "true", "false"):
            return value.upper()
        if kind == "word" and value in _FUNCTIONS:
            sql_name, arity = _FUNCTIONS[value]
            self.take("(")
            args = [self.expression()]
            while self.peek()[1] == ",":
                self.take()
                args.append(self.expression())
            self.take(")")
            if len(args) != arity:
                raise _Unsupported(value)
            return f"{sql_name}({', '.join(args)})"
        raise _Unsupported(value)


def m_column_expression_to_sql(body: str) -> str | None:
    """Spark SQL for an M column expression (without the leading ``each``), or None."""
    text = re.sub(r"^\s*each\b", "", body or "").strip()
    if not text:
        return None
    try:
        parser = _Parser(_tokens(text))
        sql = parser.expression()
        if parser.i != len(parser.tokens):
            return None
        return sql
    except (_Unsupported, IndexError):
        return None


def keep_one_row_per_key(sql: str, keys: list[str]) -> str:
    """``Table.Distinct(t, {keys})``: one row per key. Power BI keeps the first row in
    source order, which a query does not define either, so any one row is as faithful;
    what matters for a dimension is that a join cannot fan out."""
    key_list = ", ".join(keys)
    return (
        "SELECT * EXCEPT (_kasal_rn)\nFROM (\n"
        f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_list} ORDER BY {key_list}) AS _kasal_rn\n"
        f"  FROM (\n{sql}\n  ) _kasal_keyed\n) _kasal_dedup\nWHERE _kasal_rn = 1"
    )  # keys are plain identifiers (regex above) — nosemgrep: tmp.databricks.sql-injection.fstring-select


def sql_ident(name: str) -> str:
    """A column name as SQL needs it: bare when plain, back-quoted otherwise (Power Query
    names such as ``fis_code - Copy`` from a duplicated column)."""
    return name if _COLUMN.match(name) else "`" + name.replace("`", "``") + "`"


def star_select_list(
    removed: set[str],
    transforms: dict[str, str],
    renames: dict[str, str],
    added: set[str],
    duplicates: dict[str, str],
) -> str:
    """The column list over a ``SELECT *`` read whose columns are unknown.

    Keeps every source column and replaces or removes only those the steps name. Columns the
    steps create (added, duplicated, renamed-to) are not in the source, so they never go into
    ``EXCEPT``, and an expression on a duplicated column reads the column it was copied from
    (``DuplicateColumn fis_code -> "fis_code - Copy"``, renamed ``Func_area``, then replaced).
    Renamed columns come before added ones: an added column may read the new name (a lateral
    column alias in Databricks SQL).
    """
    origin = dict(duplicates)  # {new column: the source column it copies}
    for old, new in renames.items():
        if old in origin:
            origin[new] = origin[old]
    created = set(added) | set(duplicates) | set(renames.values())
    renamed_source = {
        o: n for o, n in renames.items() if o not in origin and o not in removed
    }
    excepted = sorted(
        ((set(removed) | set(transforms)) - created) | set(renamed_source)
    )
    parts = [f"* EXCEPT ({', '.join(map(sql_ident, excepted))})" if excepted else "*"]
    parts += [
        f"{sql_ident(o)} AS {sql_ident(n)}"
        for o, n in renamed_source.items()
        if n not in transforms
    ]
    parts += [
        f"{sql_ident(src)} AS {sql_ident(new)}"
        for new, src in origin.items()
        if new not in renames and new not in transforms and new not in removed
    ]
    for col, expr in transforms.items():
        if col in origin:
            expr = re.sub(
                rf"(?<![\w`]){re.escape(col)}(?![\w`])", sql_ident(origin[col]), expr
            )
        parts.append(f"{expr} AS {sql_ident(col)}")
    return ",\n".join(f"  {part}" for part in parts)
