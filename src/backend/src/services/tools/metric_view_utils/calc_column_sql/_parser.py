"""Parser for row-level DAX: tokens -> a small AST.

Precedence follows Microsoft's operator table, lowest to highest: ``&&``, ``||``
(not in that table; here ``&&`` binds tighter, and mixing them without parentheses
is refused), comparison and ``IN``, ``&``, ``+ -``, ``* /``, unary ``-``, ``^``
(refused). ``NOT`` is a function. A parenthesised group is kept as a ``Group`` node
so the mixed ``&&`` / ``||`` check can tell ``(A && B) || C`` from ``A && B || C``.
"""

from __future__ import annotations

from dataclasses import dataclass

from ._common import DaxRefusal
from ._lexer import Token, tokenize

_COMPARE_OPS = frozenset({"=", "<>", "<", "<=", ">", ">="})
_VAR_KEYWORDS = frozenset({"VAR", "RETURN", "IN", "TRUE", "FALSE", "NOT"})
#: Deepest nesting accepted, so a pathological input cannot exhaust the stack.
MAX_DEPTH = 60


@dataclass(frozen=True)
class Num:
    text: str


@dataclass(frozen=True)
class Str:
    value: str


@dataclass(frozen=True)
class Bool:
    value: bool


@dataclass(frozen=True)
class Ref:
    """A bare name: a VAR reference, or the interval keyword of DATEDIFF."""

    name: str


@dataclass(frozen=True)
class ColRef:
    table: str | None
    column: str


@dataclass(frozen=True)
class Call:
    name: str  # upper-case
    args: tuple


@dataclass(frozen=True)
class Group:
    inner: object


@dataclass(frozen=True)
class Unary:
    operand: object


@dataclass(frozen=True)
class Binary:
    op: str
    left: object
    right: object


@dataclass(frozen=True)
class Logic:
    """``&&`` (AND) or ``||`` (OR) between two conditions."""

    op: str
    left: object
    right: object


@dataclass(frozen=True)
class InList:
    expr: object
    items: tuple


@dataclass(frozen=True)
class VarBlock:
    bindings: tuple  # ((name, expr), ...)
    body: object


def parse_dax(source: str) -> object:
    """Parse one row-level DAX expression. Raises DaxRefusal when it cannot."""
    return _Parser(tokenize(source)).parse()


class _Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self._tokens = tokens
        self._index = 0
        self._depth = 0

    # -- token helpers -------------------------------------------------------

    def _peek(self) -> Token:
        return self._tokens[self._index]

    def _take(self) -> Token:
        token = self._tokens[self._index]
        if token.kind != "END":
            self._index += 1
        return token

    def _is_op(self, value: str) -> bool:
        token = self._peek()
        return token.kind == "OP" and token.value == value

    def _is_name(self, value: str) -> bool:
        token = self._peek()
        return token.kind == "NAME" and token.value.upper() == value

    def _expect_op(self, value: str) -> None:
        if not self._is_op(value):
            raise DaxRefusal(self._unexpected(f"expected '{value}'"))
        self._take()

    def _unexpected(self, detail: str) -> str:
        token = self._peek()
        found = (
            "the end of the expression" if token.kind == "END" else repr(token.value)
        )
        return f"could not parse the expression: {detail}, found {found}"

    # -- grammar -------------------------------------------------------------

    def parse(self) -> object:
        node = self._expr()
        if self._peek().kind != "END":
            raise DaxRefusal(self._unexpected("extra text after the expression"))
        return node

    def _expr(self) -> object:
        self._depth += 1
        if self._depth > MAX_DEPTH:
            raise DaxRefusal("the expression nests too deeply to translate")
        try:
            if self._is_name("VAR"):
                return self._var_block()
            return self._or()
        finally:
            self._depth -= 1

    def _var_block(self) -> VarBlock:
        self._take()  # VAR
        bindings: list[tuple[str, object]] = []
        while True:
            name = self._take()
            if name.kind != "NAME" or name.value.upper() in _VAR_KEYWORDS:
                raise DaxRefusal(self._unexpected("expected a VAR name"))
            self._expect_op("=")
            bindings.append((name.value, self._expr()))
            if not self._is_name("VAR"):
                break
            self._take()
        if not self._is_name("RETURN"):
            raise DaxRefusal(self._unexpected("expected RETURN"))
        self._take()
        return VarBlock(tuple(bindings), self._expr())

    def _or(self) -> object:
        node = self._and()
        while self._is_op("||"):
            self._take()
            rhs = self._and()
            _refuse_mixed(node, rhs)
            node = Logic("||", node, rhs)
        return node

    def _and(self) -> object:
        node = self._comparison()
        while self._is_op("&&"):
            self._take()
            node = Logic("&&", node, self._comparison())
        return node

    def _comparison(self) -> object:
        left = self._concat()
        token = self._peek()
        if token.kind == "OP" and token.value == "==":
            raise DaxRefusal("strict equality (==) is not translated; use =")
        if token.kind == "OP" and token.value in _COMPARE_OPS:
            self._take()
            return Binary(token.value, left, self._concat())
        if self._is_name("IN"):
            self._take()
            return InList(left, self._in_items())
        return left

    def _in_items(self) -> tuple:
        self._expect_op("{")
        items: list[object] = []
        while not self._is_op("}"):
            items.append(self._concat())
            if not self._is_op(","):
                break
            self._take()
        self._expect_op("}")
        return tuple(items)

    def _concat(self) -> object:
        node = self._additive()
        while self._is_op("&"):
            self._take()
            node = Binary("&", node, self._additive())
        return node

    def _additive(self) -> object:
        node = self._multiplicative()
        while self._is_op("+") or self._is_op("-"):
            op = self._take().value
            node = Binary(op, node, self._multiplicative())
        return node

    def _multiplicative(self) -> object:
        node = self._unary()
        while self._is_op("*") or self._is_op("/"):
            op = self._take().value
            node = Binary(op, node, self._unary())
        return node

    def _unary(self) -> object:
        if self._is_op("-"):
            self._take()
            return Unary(self._unary())
        if self._is_op("+"):
            self._take()  # a unary plus changes nothing (DAX documentation)
            return self._unary()
        return self._power()

    def _power(self) -> object:
        node = self._primary()
        if self._is_op("^"):
            raise DaxRefusal("exponentiation (^) is not translated")
        return node

    def _primary(self) -> object:
        token = self._take()
        if token.kind == "NUM":
            return Num(token.value)
        if token.kind == "STR":
            return Str(token.value)
        if token.kind == "COL":
            return ColRef(token.table, token.value)
        if token.kind == "BRACKET":
            return ColRef(None, token.value)
        if token.kind == "OP" and token.value == "(":
            inner = self._expr()
            self._expect_op(")")
            return Group(inner)
        if token.kind == "NAME":
            return self._name_primary(token)
        if token.kind == "END":
            raise DaxRefusal("the expression ends early")
        raise DaxRefusal(f"could not parse the expression near {token.value!r}")

    def _name_primary(self, token: Token) -> object:
        upper = token.value.upper()
        if upper == "VAR":
            self._index -= 1
            return self._var_block()
        if self._is_op("("):
            self._take()
            args: list[object] = []
            if not self._is_op(")"):
                args.append(self._expr())
                while self._is_op(","):
                    self._take()
                    args.append(self._expr())
            self._expect_op(")")
            return Call(upper, tuple(args))
        if upper == "TRUE":
            return Bool(True)
        if upper == "FALSE":
            return Bool(False)
        return Ref(token.value)


def _refuse_mixed(left: object, right: object) -> None:
    """``&&`` and ``||`` side by side without parentheses: the grouping is not stated."""
    if isinstance(left, Logic) and left.op == "&&":
        raise DaxRefusal("&& and || are mixed without parentheses; add parentheses")
    if isinstance(right, Logic) and right.op == "&&":
        raise DaxRefusal("&& and || are mixed without parentheses; add parentheses")
