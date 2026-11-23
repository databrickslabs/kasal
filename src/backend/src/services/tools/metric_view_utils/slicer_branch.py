"""SQL of one SWITCH/IF branch that reads other measures by arithmetic.

A branch of a selector choice is often a composition of measures: ``[A] - [B]``,
``[A] * 2``, ``DIVIDE([A], [B])``. The rule translator does not compose measure references,
so these are read here, and each reference is resolved by the caller (the branch of the
referenced measure, through the same path). Only three operations are composed:

* ``+``, ``-``, ``*`` between operands, written as ``(a) op (b)``;
* ``DIVIDE(a, b)`` or ``DIVIDE(a, b, BLANK())``, written as ``(a) / NULLIF((b), 0)``, which is
  the DAX result (BLANK) on a zero divisor. Any other alternate is refused.

A bare ``/`` is not composed: DAX ``/`` on a zero divisor is an error, not BLANK, and a guess
would be silent. Anything else falls back to the caller's translator on the whole text.
"""

from __future__ import annotations

import re
from collections.abc import Callable

from .slicer_parameter_dax import DispatchDecline

_TOKEN = re.compile(
    r"\s*(?:(\[[^\]]+\])|(\d+(?:\.\d+)?)|(DIVIDE)\b|(BLANK\s*\(\s*\))|([+\-*(),]))",
    re.IGNORECASE,
)


class _Unsupported(Exception):
    """The text is not an arithmetic composition; the caller translates it whole."""


def _tokens(text: str) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []
    pos = 0
    while pos < len(text):
        if not text[pos:].strip():
            break
        match = _TOKEN.match(text, pos)
        if match is None:
            raise _Unsupported
        pos = match.end()
        if match.group(1):
            tokens.append(("ref", match.group(1)[1:-1]))
        elif match.group(2):
            tokens.append(("num", match.group(2)))
        elif match.group(3):
            tokens.append(("divide", "DIVIDE"))
        elif match.group(4):
            tokens.append(("blank", "BLANK()"))
        else:
            tokens.append(("op", match.group(5)))
    return tokens


class _Parser:
    def __init__(self, tokens: list[tuple[str, str]]) -> None:
        self.tokens = tokens
        self.pos = 0

    def _peek(self) -> tuple[str, str] | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _take(self, kind: str, value: str | None = None) -> tuple[str, str]:
        token = self._peek()
        if (
            token is None
            or token[0] != kind
            or (value is not None and token[1] != value)
        ):
            raise _Unsupported
        self.pos += 1
        return token

    def parse(self) -> tuple:
        node = self._expr()
        if self._peek() is not None:
            raise _Unsupported
        return node

    def _expr(self) -> tuple:
        node = self._term()
        while (token := self._peek()) and token[0] == "op" and token[1] in "+-":
            self.pos += 1
            node = ("bin", token[1], node, self._term())
        return node

    def _term(self) -> tuple:
        node = self._primary()
        while (token := self._peek()) and token[0] == "op" and token[1] == "*":
            self.pos += 1
            node = ("bin", "*", node, self._primary())
        return node

    def _primary(self) -> tuple:
        token = self._peek()
        if token is None:
            raise _Unsupported
        if token[0] == "ref":
            self.pos += 1
            return ("ref", token[1])
        if token[0] == "num":
            self.pos += 1
            return ("num", token[1])
        if token[0] == "op" and token[1] == "(":
            self.pos += 1
            node = self._expr()
            self._take("op", ")")
            return node
        if token[0] == "divide":
            self.pos += 1
            self._take("op", "(")
            numerator = self._expr()
            self._take("op", ",")
            denominator = self._expr()
            if self._peek() == ("op", ","):
                self.pos += 1
                if self._peek() is None or self._peek()[0] != "blank":
                    raise DispatchDecline(
                        "DIVIDE with an alternate other than BLANK() is not translated"
                    )
                self.pos += 1
            self._take("op", ")")
            return ("divide", numerator, denominator)
        raise _Unsupported


def _render(node: tuple, resolve_ref: Callable[[str], str]) -> str:
    kind = node[0]
    if kind == "ref":
        return resolve_ref(node[1])
    if kind == "num":
        return node[1]
    if kind == "bin":
        return f"({_render(node[2], resolve_ref)}) {node[1]} ({_render(node[3], resolve_ref)})"
    return f"({_render(node[1], resolve_ref)}) / NULLIF(({_render(node[2], resolve_ref)}), 0)"


def branch_sql(
    text: str,
    resolve_ref: Callable[[str], str],
    translate_text: Callable[[str], str],
) -> str:
    """The SQL of a branch: a plain reference, a composition, or the translator's own SQL.

    ``resolve_ref(name)`` gives the SQL of a referenced measure; ``translate_text(text)``
    the translator's SQL of a DAX text it does not compose. Raises ``DispatchDecline``.
    """
    try:
        tokens = _tokens(text.strip())
        node = _Parser(tokens).parse() if tokens else None
    except _Unsupported:
        return translate_text(text.strip())
    if node is None:
        return translate_text(text.strip())
    if node[0] == "ref":
        return resolve_ref(node[1])
    if node[0] == "num":
        return node[1]
    return _render(node, resolve_ref)
