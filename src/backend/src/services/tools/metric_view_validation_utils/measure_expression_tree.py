"""Operator-shape check for an expression that calls other measures.

A metric-view expression calls a measure with ``MEASURE(name)``; the DAX calls it with
``[Name]``. Once the names agree, the arithmetic around the calls still has to agree: the
operator precedence, the ratio shape and the grouping of the terms. This module reads both
expressions into a small tree and compares the canonical forms.

Leaves: a measure call (``m``), a number (``n``), and an aggregate (``agg``) keyed by its
function and the column names it reads (its filters are compared by the filter check, not
here). Shapes: sums (order-free, with signs), products (order-free), a division, ``ABS``, and
the guards that do not change a value: ``x / NULLIF(y, 0)`` and ``DIVIDE(x, y, 0)`` are the
same division. An expression with any other construct is not readable, and the caller must
not treat an unreadable expression as correct.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

from .measure_references import UCMV_MEASURE_CALL
from .prior_year_check import loose_name

_TOKEN = re.compile(
    r"\s*(?:(?P<num>\d+(?:\.\d+)?)|(?P<bracket>\[[^\[\]]+\])|(?P<name>[A-Za-z_]\w*)"
    r"|(?P<str>'[^']*'|\"[^\"]*\")|(?P<op><=|>=|<>|!=|[-+*/(),=<>.{}]))"
)
_AGGREGATES = frozenset(
    {
        "SUM",
        "AVG",
        "AVERAGE",
        "COUNT",
        "MIN",
        "MAX",
        "SUMX",
        "AVERAGEX",
        "COUNTX",
        "MINX",
        "MAXX",
        "COUNTROWS",
    }
)
_KEYWORDS = frozenset(
    {
        "AND",
        "OR",
        "NOT",
        "IN",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "IS",
        "NULL",
        "DISTINCT",
        "FILTER",
        "WHERE",
        "BLANK",
        "TRUE",
        "FALSE",
        "ALL",
        "AS",
    }
)
Token = Tuple[str, str]
Node = tuple  # a node of the expression tree: ("m", name), ("agg", fn, cols), ...


class _Unreadable(Exception):
    """The expression has a construct this reader does not model."""


def _tokens(text: str) -> List[Token]:
    out: List[Token] = []
    pos = 0
    text = text.strip()
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m or m.end() == pos:
            raise _Unreadable(f"untokenised text at {pos}")
        kind = m.lastgroup or ""
        out.append((kind, m.group(kind)))
        pos = m.end()
    return out


def _split_args(tokens: List[Token]) -> List[List[Token]]:
    """The top-level comma-separated arguments of a token list."""
    if not tokens:
        return []
    args: List[List[Token]] = [[]]
    depth = 0
    for tok in tokens:
        if tok == ("op", "("):
            depth += 1
        elif tok == ("op", ")"):
            depth -= 1
        if tok == ("op", ",") and depth == 0:
            args.append([])
        else:
            args[-1].append(tok)
    return args


def _read(tokens: List[Token]) -> Node:
    """The tree of one argument, which must be a whole expression."""
    return _Reader(tokens).expression_all()


def _unwrap_nullif(node: Node) -> Node:
    return node[1] if node and node[0] == "nullif" else node


def _column_names(tokens: List[Token]) -> Tuple[str, ...]:
    """Column names an aggregate reads: ``src.col`` and ``Table[col]`` columns, and bare names;
    a table name, a function name and a keyword are not columns."""
    cols = set()
    for idx, (kind, text) in enumerate(tokens):
        if kind == "bracket":
            cols.add(text[1:-1].lower())
        elif kind == "name" and text.upper() not in _KEYWORDS:
            nxt = tokens[idx + 1] if idx + 1 < len(tokens) else None
            if nxt in (("op", "("), ("op", ".")) or (
                nxt is not None and nxt[0] == "bracket"
            ):
                continue
            cols.add(text.lower())
    return tuple(sorted(cols))


@dataclass
class _Reader:
    toks: List[Token]
    i: int = 0

    def peek(self, offset: int = 0) -> Optional[Token]:
        j = self.i + offset
        return self.toks[j] if j < len(self.toks) else None

    def take(self) -> Token:
        tok = self.peek()
        if tok is None:
            raise _Unreadable("unexpected end")
        self.i += 1
        return tok

    def skip_group(self) -> List[Token]:
        """Consume a balanced ``( ... )`` group and return its tokens without the parentheses."""
        if self.take() != ("op", "("):
            raise _Unreadable("expected (")
        depth = 1
        inner: List[Token] = []
        while True:
            tok = self.take()
            if tok == ("op", "("):
                depth += 1
            elif tok == ("op", ")"):
                depth -= 1
                if depth == 0:
                    return inner
            inner.append(tok)

    def expression(self) -> Node:
        terms = [(1, self.term())]
        while self.peek() in (("op", "+"), ("op", "-")):
            sign = 1 if self.take()[1] == "+" else -1
            terms.append((sign, self.term()))
        if len(terms) == 1 and terms[0][0] == 1:
            return terms[0][1]
        return ("sum", terms)

    def term(self) -> Node:
        factors = [self.unary()]
        while self.peek() == ("op", "*"):
            self.take()
            factors.append(self.unary())
        node = factors[0] if len(factors) == 1 else ("prod", factors)
        if self.peek() == ("op", "/"):
            self.take()
            divisor = _unwrap_nullif(self.unary())
            node = ("div", node, divisor, None)
            if self.peek() in (("op", "*"), ("op", "/")):
                raise _Unreadable("a chain of divisions or products after a division")
        return node

    def unary(self) -> Node:
        if self.peek() == ("op", "-"):
            self.take()
            return ("prod", [("n", -1.0), self.unary()])
        return self.primary()

    def primary(self) -> Node:
        kind, text = self.take()
        if kind == "num":
            return ("n", float(text))
        if kind == "bracket":
            return ("m", loose_name(text[1:-1]))
        if (kind, text) == ("op", "("):
            self.i -= 1
            return _read(self.skip_group())
        if kind == "name":
            return self.call(text.upper())
        raise _Unreadable(f"unexpected token {text}")

    def call(self, name: str) -> Node:
        if self.peek() != ("op", "("):
            raise _Unreadable(f"bare name {name}: a column or an unmodelled construct")
        if name == "MEASURE":
            inner = self.skip_group()
            if len(inner) != 1 or inner[0][0] != "name":
                raise _Unreadable("MEASURE takes one name")
            return ("m", loose_name(inner[0][1]))
        if name in _AGGREGATES:
            inner = self.skip_group()
            if self.peek() == ("name", "FILTER") and self.peek(1) == ("op", "("):
                self.take()
                self.skip_group()  # FILTER (WHERE ...): compared by the filter check
            return ("agg", name, _column_names(inner))
        return self.function(name, _split_args(self.skip_group()))

    def function(self, name: str, args: List[List[Token]]) -> Node:
        if name == "DIVIDE" and len(args) in (2, 3):
            alt = _read(args[2]) if len(args) == 3 else None
            if alt in (("n", 0.0), ("blank",)):
                alt = None
            return ("div", _read(args[0]), _unwrap_nullif(_read(args[1])), alt)
        if name == "NULLIF" and len(args) == 2 and _read(args[1]) == ("n", 0.0):
            return ("nullif", _read(args[0]))
        if name == "ABS" and len(args) == 1:
            return ("abs", _read(args[0]))
        if name == "COALESCE" and len(args) == 2 and _read(args[1]) == ("n", 0.0):
            return _read(args[0])
        if name == "CALCULATE" and args:
            return _read(
                args[0]
            )  # the filter arguments are compared by the filter check
        if name == "BLANK" and not args:
            return ("blank",)
        raise _Unreadable(f"function {name} is not modelled")

    def expression_all(self) -> Node:
        node = self.expression()
        if self.peek() is not None:
            raise _Unreadable("trailing tokens")
        return node


def _canon(node: Node) -> str:
    kind = node[0]
    if kind == "m":
        return f"m:{node[1]}"
    if kind == "n":
        return f"n:{node[1]:g}"
    if kind == "blank":
        return "blank"
    if kind == "agg":
        return f"{node[1]}({','.join(node[2])})"
    if kind == "abs":
        return f"abs({_canon(node[1])})"
    if kind == "nullif":
        return f"nullif({_canon(node[1])})"
    if kind == "div":
        alt = "" if node[3] is None else f",{_canon(node[3])}"
        return f"div({_canon(node[1])},{_canon(node[2])}{alt})"
    if kind == "prod":
        return "prod(" + ",".join(sorted(_canon(f) for f in node[1])) + ")"
    if kind == "sum":
        return (
            "sum("
            + ",".join(sorted(("+" if s > 0 else "-") + _canon(t) for s, t in node[1]))
            + ")"
        )
    raise _Unreadable(f"unknown node {kind}")


def canonical_shape(expr: str) -> Optional[str]:
    """The canonical operator shape of an expression, or None when it is not readable."""
    try:
        return _canon(_read(_tokens(expr)))
    except (_Unreadable, IndexError, ValueError, TypeError):
        return None


def sql_with_measure_brackets(sql: str) -> str:
    """The metric-view SQL with each ``MEASURE(name)`` written as ``[name]``, the form the
    reader takes a measure in (DAX writes it so already)."""
    return UCMV_MEASURE_CALL.sub(lambda m: f"[{m.group(1)}]", sql)


def composed_of_measures_only(expr: str) -> bool:
    """True when the expression is arithmetic over measure calls and numbers (not a bare
    alias of one measure, and with no aggregate of a column). Its value is the referenced
    measures' values combined by arithmetic the static check cannot test against the data:
    the expression is judged by its own row and by the rows of the measures it calls."""
    try:
        tree = _read(_tokens(sql_with_measure_brackets(expr)))
    except (_Unreadable, IndexError, ValueError, TypeError):
        return False
    if tree[0] == "m":
        return (
            False  # a bare alias: its value is that one measure's value, judged there
        )

    def leaves_are_measures(node: Node) -> bool:
        kind = node[0]
        if kind in ("m", "n", "blank"):
            return True
        if kind == "agg":
            return False
        if kind == "prod":
            return all(leaves_are_measures(f) for f in node[1])
        if kind == "sum":
            return all(leaves_are_measures(t) for _, t in node[1])
        if kind == "div":
            return (
                leaves_are_measures(node[1])
                and leaves_are_measures(node[2])
                and (node[3] is None or leaves_are_measures(node[3]))
            )
        if kind in ("abs", "nullif"):
            return leaves_are_measures(node[1])
        return False

    return leaves_are_measures(tree)


def compare_shapes(sql_expr: str, dax_expr: str) -> Tuple[Optional[bool], str]:
    """``(True, why)`` when both expressions read and have the same shape, ``(False, why)``
    when both read and differ, ``(None, why)`` when one of them is not readable."""
    sql_shape = canonical_shape(sql_with_measure_brackets(sql_expr))
    dax_shape = canonical_shape(dax_expr)
    if sql_shape is None or dax_shape is None:
        side = "metric view" if sql_shape is None else "DAX"
        return None, f"the {side} arithmetic over measures is not readable"
    if sql_shape == dax_shape:
        return True, "measure arithmetic has the same shape"
    return (
        False,
        "measure arithmetic differs in shape (operator precedence, ratio or grouping)",
    )
