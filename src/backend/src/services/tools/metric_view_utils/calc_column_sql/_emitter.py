"""Walks the parsed DAX tree and emits Databricks SQL, one ``Val`` per node.

Column references resolve against the source view's columns (case-insensitively,
returning the view's spelling). A reference to another table is refused unless it
is ``RELATED(Other[Col])``, which records a ``LEFT JOIN`` the caller must add.
"""

from __future__ import annotations

from collections.abc import Mapping

from ._common import (
    KIND_BLANK,
    KIND_BOOLEAN,
    KIND_DATE,
    KIND_INTEGER,
    KIND_NUMBER,
    KIND_TEXT,
    KIND_UNKNOWN,
    MAX_SQL_CHARS,
    DaxRefusal,
    RelatedJoin,
    RelatedTable,
    Val,
    as_bool,
    as_date,
    as_number,
    as_text,
    ident,
    sql_str,
)
from ._functions_core import CORE_FUNCTIONS, REFUSED_FUNCTIONS
from ._functions_date import DATE_FUNCTIONS
from ._functions_text import TEXT_FUNCTIONS
from ._parser import (
    Binary,
    Bool,
    Call,
    ColRef,
    Group,
    InList,
    Logic,
    Num,
    Ref,
    Str,
    Unary,
    VarBlock,
)

FUNCTIONS = {**CORE_FUNCTIONS, **TEXT_FUNCTIONS, **DATE_FUNCTIONS}
_ARITHMETIC = frozenset({"+", "-", "*"})
#: Kinds a comparison side may have, per the kind it is compared as.
_COMPARABLE = {
    KIND_TEXT: {KIND_TEXT, KIND_UNKNOWN, KIND_BLANK},
    KIND_NUMBER: {KIND_INTEGER, KIND_NUMBER, KIND_UNKNOWN, KIND_BLANK},
    KIND_BOOLEAN: {KIND_BOOLEAN, KIND_UNKNOWN, KIND_BLANK},
    KIND_DATE: {KIND_DATE, KIND_UNKNOWN, KIND_BLANK},
}


class RowEmitter:
    """Emits SQL for one calculated column of ``table``."""

    def __init__(
        self,
        *,
        table: str,
        columns: tuple[str, ...],
        column_kinds: Mapping[str, str],
        calculated: tuple[str, ...],
        related: Mapping[str, RelatedTable],
        source_alias: str | None,
    ) -> None:
        self.table = table
        self._columns = {name.casefold(): name for name in columns}
        self._kinds = {name.casefold(): kind for name, kind in column_kinds.items()}
        self._calculated = {name.casefold() for name in calculated}
        self._related = {name.casefold(): (name, rel) for name, rel in related.items()}
        self._source_alias = source_alias
        self.joins: dict[str, RelatedJoin] = {}

    # -- dispatch ------------------------------------------------------------

    def emit(self, node: object, env: Mapping[str, Val]) -> Val:
        """SQL for one node. Refuses an SQL text that grows past the bound."""
        val = self._emit(node, env)
        if len(val.sql) > MAX_SQL_CHARS:
            raise DaxRefusal(
                "the translated expression is too large (VAR values are inlined); "
                "split the calculation"
            )
        return val

    def _emit(self, node: object, env: Mapping[str, Val]) -> Val:
        if isinstance(node, Num):
            kind = KIND_NUMBER if "." in node.text else KIND_INTEGER
            return Val(node.text, kind, False, lit=node.text)
        if isinstance(node, Str):
            return Val(sql_str(node.value), KIND_TEXT, False, lit=node.value)
        if isinstance(node, Bool):
            return Val(
                "TRUE" if node.value else "FALSE", KIND_BOOLEAN, False, lit=node.value
            )
        if isinstance(node, ColRef):
            return self._column(node)
        if isinstance(node, Ref):
            found = env.get(node.name.casefold())
            if found is None:
                raise DaxRefusal(
                    f"'{node.name}' is neither a VAR nor a column reference; "
                    "write Table[Column] or [Column]"
                )
            return found
        if isinstance(node, Group):
            return self.emit(node.inner, env)
        if isinstance(node, Unary):
            operand = self.emit(node.operand, env)
            kind = (
                KIND_INTEGER
                if operand.kind in (KIND_INTEGER, KIND_BLANK)
                else KIND_NUMBER
            )
            return Val(f"(-{as_number(operand, 'unary minus')})", kind, False)
        if isinstance(node, Binary):
            return self._binary(node, env)
        if isinstance(node, Logic):
            left = as_bool(self.emit(node.left, env), "a condition")
            right = as_bool(self.emit(node.right, env), "a condition")
            word = "AND" if node.op == "&&" else "OR"
            return Val(f"({left} {word} {right})", KIND_BOOLEAN, False)
        if isinstance(node, InList):
            return self._in_list(node, env)
        if isinstance(node, VarBlock):
            scope = dict(env)
            for name, expr in node.bindings:
                scope[name.casefold()] = self.emit(expr, scope)
            return self.emit(node.body, scope)
        if isinstance(node, Call):
            return self._call(node, env)
        raise DaxRefusal("unsupported expression")

    def _call(self, node: Call, env: Mapping[str, Val]) -> Val:
        if node.name in REFUSED_FUNCTIONS:
            raise DaxRefusal(REFUSED_FUNCTIONS[node.name])
        function = FUNCTIONS.get(node.name)
        if function is None:
            raise DaxRefusal(
                f"function {node.name} is not translated in a row-level calculated column"
            )
        return function(self, node.args, env)

    # -- operators -----------------------------------------------------------

    def _binary(self, node: Binary, env: Mapping[str, Val]) -> Val:
        if node.op in _ARITHMETIC:
            left = self.emit(node.left, env)
            right = self.emit(node.right, env)
            both_whole = left.kind in (KIND_INTEGER, KIND_BLANK) and right.kind in (
                KIND_INTEGER,
                KIND_BLANK,
            )
            sql = (
                f"({as_number(left, 'arithmetic')} {node.op} "
                f"{as_number(right, 'arithmetic')})"
            )
            return Val(sql, KIND_INTEGER if both_whole else KIND_NUMBER, False)
        if node.op == "/":
            return self._divide_operator(node, env)
        if node.op == "&":
            left = self.emit(node.left, env)
            right = self.emit(node.right, env)
            sql = f"concat({as_text(left, '&')}, {as_text(right, '&')})"
            return Val(sql, KIND_TEXT, False)
        return self.compare(
            node.op, self.emit(node.left, env), self.emit(node.right, env)
        )

    def _divide_operator(self, node: Binary, env: Mapping[str, Val]) -> Val:
        """``/`` only with a non-zero literal divisor. A zero divisor gives a result
        (infinity, NaN) that differs from a SQL NULL, so the operator is refused
        for any other divisor and DIVIDE() is the translated form."""
        divisor = node.right
        if not (isinstance(divisor, Num) and float(divisor.text) != 0):
            raise DaxRefusal(
                "the / operator with a divisor that is not a non-zero literal is not "
                "translated; use DIVIDE()"
            )
        left = self.emit(node.left, env)
        return Val(f"({as_number(left, '/')} / {divisor.text})", KIND_NUMBER, False)

    def _in_list(self, node: InList, env: Mapping[str, Val]) -> Val:
        if not node.items:
            raise DaxRefusal("an empty IN list is not translated")
        subject = self.emit(node.expr, env)
        tests: list[str] = []
        for item in node.items:
            value = self.emit(item, env)
            if value.lit is None:
                raise DaxRefusal("an IN list holds constants only")
            tests.append(self.compare("=", subject, value).sql)
        return Val(f"({' OR '.join(tests)})", KIND_BOOLEAN, False)

    # -- comparison ----------------------------------------------------------

    def compare(self, op: str, left: Val, right: Val) -> Val:
        """A two-valued DAX comparison. Each side reads BLANK the way DAX does, so the
        result is never NULL. Text equality ignores case (``lower`` on both sides)."""
        if left.kind == KIND_BLANK and right.kind == KIND_BLANK:
            truth = op in ("=", "<=", ">=")
            return Val("TRUE" if truth else "FALSE", KIND_BOOLEAN, False)
        kind = _comparison_kind(left, right)
        if kind in (KIND_TEXT, KIND_BOOLEAN) and op not in ("=", "<>"):
            raise DaxRefusal(
                f"ordering comparison ({op}) of {kind} values is not translated"
            )
        left_sql = _compare_side(left, kind)
        right_sql = _compare_side(right, kind)
        return Val(f"({left_sql} {op} {right_sql})", KIND_BOOLEAN, False)

    # -- columns -------------------------------------------------------------

    def _own(self, name: str) -> str:
        column = ident(name)
        return f"{self._source_alias}.{column}" if self._source_alias else column

    def _column(self, node: ColRef) -> Val:
        if node.table is not None and node.table.casefold() != self.table.casefold():
            raise DaxRefusal(
                f"{node.table}[{node.column}] is in another table; a row reads only its "
                "own columns, and RELATED(...) for a many-to-one relationship"
            )
        key = node.column.casefold()
        actual = self._columns.get(key)
        if actual is None:
            if key in self._calculated:
                raise DaxRefusal(
                    f"column {node.column} is a calculated column; a calculated column "
                    "cannot read another one in the source view"
                )
            raise DaxRefusal(
                f"column {node.column} is not in the source view (a measure, or a column "
                "the view does not expose)"
            )
        return Val(self._own(actual), self._kinds.get(key, KIND_UNKNOWN), True)

    def related_column(self, node: object) -> Val:
        """``RELATED(Other[Col])``: a column of a many-to-one related table, read through
        a LEFT JOIN the caller adds. BLANK when no related row matches, as in DAX."""
        if not isinstance(node, ColRef) or node.table is None:
            raise DaxRefusal("RELATED takes one qualified column, Table[Column]")
        if node.table.casefold() == self.table.casefold():
            raise DaxRefusal("RELATED of this table's own column is not translated")
        entry = self._related.get(node.table.casefold())
        if entry is None:
            raise DaxRefusal(
                f"no direct many-to-one relationship from {self.table} to {node.table} "
                "was supplied; multi-hop RELATED is not translated"
            )
        if self._source_alias is None:
            raise DaxRefusal("RELATED needs the source view read under an alias")
        name, rel = entry
        related_cols = {col.casefold(): col for col in rel.columns}
        column = related_cols.get(node.column.casefold())
        if column is None:
            raise DaxRefusal(
                f"column {node.column} is not in the related view of {name}"
            )
        to_column = related_cols.get(rel.to_column.casefold())
        from_column = self._columns.get(rel.from_column.casefold())
        if to_column is None or from_column is None:
            raise DaxRefusal(
                f"the join key of the relationship to {name} is not in both views"
            )
        join_sql = (
            f"LEFT JOIN {rel.view} AS {rel.alias} ON "
            f"{self._source_alias}.{ident(from_column)} = {rel.alias}.{ident(to_column)}"
        )
        self.joins.setdefault(name.casefold(), RelatedJoin(name, rel.alias, join_sql))
        kinds = {key.casefold(): kind for key, kind in rel.column_kinds.items()}
        kind = kinds.get(column.casefold(), KIND_UNKNOWN)
        return Val(f"{rel.alias}.{ident(column)}", kind, True)


def _comparison_kind(left: Val, right: Val) -> str:
    """The kind both sides are compared as. A literal decides an unknown column's kind."""
    kinds = []
    for side in (left.kind, right.kind):
        if side in (KIND_BLANK, KIND_UNKNOWN):
            continue
        kinds.append(KIND_NUMBER if side == KIND_INTEGER else side)
    if not kinds:
        if left.kind == KIND_UNKNOWN and right.kind == KIND_UNKNOWN:
            raise DaxRefusal(
                "compares two columns of unknown type; pass their kinds in column_kinds"
            )
        raise DaxRefusal(
            "compares a column of unknown type with BLANK(); use ISBLANK() instead"
        )
    if len(set(kinds)) > 1:
        raise DaxRefusal(
            f"compares {kinds[0]} with {kinds[1]}; DAX returns an error for mixed types"
        )
    return kinds[0]


def _compare_side(value: Val, kind: str) -> str:
    """One side of a comparison, with BLANK read as the kind's zero value."""
    if value.kind not in _COMPARABLE[kind]:
        raise DaxRefusal(f"compares {kind} with {value.kind}")
    if kind == KIND_TEXT:
        if value.kind == KIND_BLANK:
            return "''"
        if isinstance(value.lit, str):
            return sql_str(value.lit.lower())
        base = f"COALESCE({value.sql}, '')" if value.blank else value.sql
        return f"lower({base})"
    if kind == KIND_NUMBER:
        return as_number(value, "comparison")
    if kind == KIND_BOOLEAN:
        return as_bool(value, "comparison")
    return as_date(value, "comparison")
