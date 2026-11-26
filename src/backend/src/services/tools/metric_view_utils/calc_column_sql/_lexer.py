"""Tokenizer for the row-level DAX subset.

Produces NUM, STR, NAME, COL (``Table[Col]`` or ``'Table'[Col]``), BRACKET (``[Col]``),
OP and a final END token. ``//`` and ``/* */`` comments are skipped. Anything the
tokenizer does not know is refused rather than guessed at.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from ._common import DaxRefusal

_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_NUMBER = re.compile(r"\d+(?:\.\d+)?")
# Longest first, so "<=" is not read as "<" then "=".
_OPERATORS = (
    "&&",
    "||",
    "<>",
    "<=",
    ">=",
    "==",
    "=",
    "<",
    ">",
    "+",
    "-",
    "*",
    "/",
    "&",
    "^",
    "(",
    ")",
    "{",
    "}",
    ",",
)


@dataclass(frozen=True)
class Token:
    kind: str
    value: str
    table: str | None = None


def tokenize(source: str) -> list[Token]:
    """Split a DAX expression into tokens, ending with an END token."""
    tokens: list[Token] = []
    pos = 0
    length = len(source)
    while pos < length:
        ch = source[pos]
        if ch.isspace():
            pos += 1
        elif source.startswith("//", pos):
            end = source.find("\n", pos)
            pos = length if end < 0 else end
        elif source.startswith("/*", pos):
            end = source.find("*/", pos + 2)
            if end < 0:
                raise DaxRefusal("unterminated /* comment")
            pos = end + 2
        elif ch == '"':
            value, pos = _read_quoted(source, pos, '"')
            tokens.append(Token("STR", value))
        elif ch == "'":
            table, pos = _read_quoted(source, pos, "'")
            if pos >= length or source[pos] != "[":
                raise DaxRefusal("a quoted table name must be followed by [Column]")
            column, pos = _read_bracket(source, pos)
            tokens.append(Token("COL", column, table))
        elif ch == "[":
            column, pos = _read_bracket(source, pos)
            tokens.append(Token("BRACKET", column))
        elif ch.isdigit():
            pos = _read_number(source, pos, tokens)
        elif ch.isascii() and (ch.isalpha() or ch == "_"):
            pos = _read_name(source, pos, tokens)
        else:
            pos = _read_operator(source, pos, tokens)
    tokens.append(Token("END", ""))
    return tokens


def _read_quoted(source: str, pos: int, quote: str) -> tuple[str, int]:
    """Read a ``"text"`` or ``'table'`` run; a doubled quote stands for one quote."""
    out: list[str] = []
    cursor = pos + 1
    while cursor < len(source):
        if source[cursor] == quote:
            if source.startswith(quote, cursor + 1):
                out.append(quote)
                cursor += 2
                continue
            return "".join(out), cursor + 1
        out.append(source[cursor])
        cursor += 1
    raise DaxRefusal(f"unterminated {quote} literal")


def _read_bracket(source: str, pos: int) -> tuple[str, int]:
    """Read ``[Column]``; ``]]`` stands for a literal ``]``."""
    out: list[str] = []
    cursor = pos + 1
    while cursor < len(source):
        if source[cursor] == "]":
            if source.startswith("]", cursor + 1):
                out.append("]")
                cursor += 2
                continue
            name = "".join(out)
            if not name.strip():
                raise DaxRefusal("empty [column] reference")
            return name, cursor + 1
        out.append(source[cursor])
        cursor += 1
    raise DaxRefusal("unterminated [column] reference")


def _read_number(source: str, pos: int, tokens: list[Token]) -> int:
    match = _NUMBER.match(source, pos)
    assert match is not None  # a digit is at pos
    end = match.end()
    if end < len(source) and (source[end].isalpha() or source[end] == "_"):
        raise DaxRefusal("number formats such as 1E3 are not translated")
    tokens.append(Token("NUM", match.group()))
    return end


def _read_name(source: str, pos: int, tokens: list[Token]) -> int:
    match = _NAME.match(source, pos)
    assert match is not None  # a letter is at pos
    end = match.end()
    if end < len(source) and source[end] == "[":
        column, end = _read_bracket(source, end)
        tokens.append(Token("COL", column, match.group()))
    else:
        tokens.append(Token("NAME", match.group()))
    return end


def _read_operator(source: str, pos: int, tokens: list[Token]) -> int:
    for operator in _OPERATORS:
        if source.startswith(operator, pos):
            tokens.append(Token("OP", operator))
            return pos + len(operator)
    raise DaxRefusal(f"unexpected character {source[pos]!r}")
