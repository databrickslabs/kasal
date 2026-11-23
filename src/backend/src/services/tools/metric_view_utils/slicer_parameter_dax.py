"""DAX text of a measure that reads a slicer selector (``SELECTEDVALUE(T[c])``).

Three operations, none of them model-aware (``slicer_parameters`` decides which tables are
selectors):

* ``selector_refs`` finds the ``SELECTEDVALUE`` calls of a measure;
* ``rewrite_selectors`` replaces each call by the placeholder literal of its parameter
  (``slicer_parameter_text``), so the normal translation path can carry it;
* ``parse_case`` reads a measure whose result is a choice on one parameter, as
  ``SWITCH(<param>, "A", x, ...)``, ``SWITCH(TRUE(), <param> = "A", x, ...)`` or
  ``IF(<param> = "A", x, y)``, into its options. Anything else raises ``DispatchDecline``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.services.powerbi.switch_decomposition import (
    _parse_dax_vars,
    _split_balanced_args,
)

from .slicer_parameter_text import TOKEN_PREFIX


class DispatchDecline(Exception):
    """A measure whose selector use this module cannot read. The message is the reason."""


@dataclass(frozen=True)
class SelectorRef:
    """A ``SELECTEDVALUE(table[column] [, alternate])`` call."""

    table: str
    column: str
    alternate: str | None  # the text literal of the second argument, when given


@dataclass(frozen=True)
class CaseShape:
    """A choice on one parameter: ``options`` are (value, DAX result) pairs in order and
    ``otherwise`` the DAX result when no option matches (None: no such branch)."""

    param: str
    options: tuple[tuple[str, str], ...]
    otherwise: str | None
    shape: str  # "SWITCH" or "IF"


_SELECTOR_CALL = re.compile(
    r"SELECTEDVALUE\s*\(\s*(?:'([^']+)'|([A-Za-z_]\w*))\s*\[\s*(\w+)\s*\]",
    re.IGNORECASE,
)
_SELECTEDVALUE_HEAD = re.compile(r"\bSELECTEDVALUE\s*\(", re.IGNORECASE)
_COLUMN_ARG = re.compile(r"\s*(?:'([^']+)'|([A-Za-z_]\w*))\s*\[\s*(\w+)\s*\]\s*")
_TEXT_LITERAL = re.compile(r'\s*"([^"]*)"\s*')
_TOKEN_LITERAL = re.compile(rf'\s*"{TOKEN_PREFIX}(\w+)"\s*')
_GROUPING_FUNCTION = re.compile(
    r"\b(ISFILTERED|HASONEVALUE|ISINSCOPE)\s*\(", re.IGNORECASE
)
EXTERNAL_MEASURE = re.compile(r"\bEXTERNALMEASURE\s*\(", re.IGNORECASE)


def selector_refs(dax: str) -> list[SelectorRef]:
    """Every ``SELECTEDVALUE(T[c])`` written in ``dax``, VAR definitions included."""
    return [
        SelectorRef(table=m.group(1) or m.group(2), column=m.group(3), alternate=None)
        for m in _SELECTOR_CALL.finditer(dax or "")
    ]


def reads_query_grouping(dax: str) -> bool:
    """Whether ``dax`` asks about the query's grouping (``ISFILTERED``, ``HASONEVALUE``,
    ``ISINSCOPE``). That is not a selector value and a parameter cannot stand for it."""
    return bool(_GROUPING_FUNCTION.search(dax or ""))


def rewrite_selectors(dax: str, register) -> str:
    """``dax`` with each ``SELECTEDVALUE(T[c] [, alt])`` replaced by its parameter's token.

    ``register(ref)`` returns the parameter name for a selector (and records its alternate).
    Raises ``DispatchDecline`` for a call whose arguments are not a column and a text literal.
    """
    pieces: list[str] = []
    pos = 0
    for head in _SELECTEDVALUE_HEAD.finditer(dax):
        if head.start() < pos:
            continue  # inside a call already rewritten
        open_index = head.end() - 1
        close = _matching_close(dax, open_index)
        if close < 0:
            raise DispatchDecline("SELECTEDVALUE with unbalanced parentheses")
        args = _split_balanced_args(dax[open_index + 1 : close])
        if len(args) not in (1, 2):
            raise DispatchDecline("SELECTEDVALUE with an unexpected argument list")
        column = _COLUMN_ARG.fullmatch(args[0])
        if column is None:
            raise DispatchDecline("SELECTEDVALUE does not name a table column")
        alternate = (
            _text_literal(args[1], "the SELECTEDVALUE alternate")
            if len(args) == 2
            else None
        )
        ref = SelectorRef(
            column.group(1) or column.group(2), column.group(3), alternate
        )
        name = register(ref)
        pieces.append(dax[pos : head.start()])
        pieces.append(f'"{TOKEN_PREFIX}{name}"')
        pos = close + 1
    pieces.append(dax[pos:])
    return "".join(pieces)


def parse_case(dax: str) -> CaseShape:
    """Read a rewritten measure as a choice on one parameter. Raises ``DispatchDecline``."""
    body = expanded_body(dax)
    switch = _whole_call(body, "SWITCH")
    if switch is not None:
        return _switch_case(switch)
    if_args = _whole_call(body, "IF")
    if if_args is not None:
        return _if_case(if_args)
    raise DispatchDecline("not a SWITCH or IF on a selector parameter")


def _switch_case(args: list[str]) -> CaseShape:
    if len(args) < 3:
        raise DispatchDecline("SWITCH without a branch")
    first = _strip_parens(args[0])
    rest = args[1:]
    otherwise = rest[-1] if len(rest) % 2 == 1 else None
    pairs = rest[:-1] if otherwise is not None else rest
    if first.upper() == "TRUE()":
        # SWITCH(TRUE(), <param> = "A", x, <param> = "B", y, [otherwise])
        params: set[str] = set()
        options = []
        for i in range(0, len(pairs), 2):
            name, value = _equality(pairs[i])
            params.add(name)
            options.append((value, pairs[i + 1]))
        if len(params) != 1:
            raise DispatchDecline(
                "the conditions read more than one selector parameter"
            )
        return CaseShape(params.pop(), tuple(options), otherwise, "SWITCH")
    name = _token_name(first)
    if name is None:
        raise DispatchDecline("SWITCH is not on a selector parameter")
    options = tuple(
        (_text_literal(pairs[i], "a SWITCH option value"), pairs[i + 1])
        for i in range(0, len(pairs), 2)
    )
    return CaseShape(name, options, otherwise, "SWITCH")


def _if_case(args: list[str]) -> CaseShape:
    if len(args) not in (2, 3):
        raise DispatchDecline("IF without a true and a false branch")
    name, value = _equality(args[0])
    otherwise = args[2] if len(args) == 3 else None
    return CaseShape(name, ((value, args[1]),), otherwise, "IF")


def _equality(condition: str) -> tuple[str, str]:
    """``(parameter, value)`` of ``<param> = "value"`` (either side); raises otherwise."""
    text = _strip_parens(condition)
    eq = _top_level_equals(text)
    if eq is None:
        raise DispatchDecline(
            "a condition is not an equality with a selector parameter"
        )
    left = _strip_parens(text[:eq])
    right = _strip_parens(text[eq + 1 :])
    for param_side, value_side in ((left, right), (right, left)):
        name = _token_name(param_side)
        if name is not None:
            return name, _text_literal(
                value_side, "the value compared with the selector"
            )
    raise DispatchDecline("a condition is not an equality with a selector parameter")


def _token_name(text: str) -> str | None:
    match = _TOKEN_LITERAL.fullmatch(_strip_parens(text))
    return match.group(1) if match else None


def expanded_body(dax: str) -> str:
    """The RETURN expression with VAR definitions inlined and outer parentheses removed.

    DAX names are case-insensitive: ``var a = ...`` is read where ``A`` is written."""
    text = (dax or "").strip()
    vars_map: dict[str, str] = {}
    if re.match(r"(?is)^var\s", text):
        vars_map = _parse_dax_vars(text)
        ret = re.search(r"\breturn\b\s*(.+)$", text, re.IGNORECASE | re.DOTALL)
        if ret is None:
            raise DispatchDecline("VAR block without a RETURN expression")
        text = ret.group(1)
    return _strip_parens(_inline_vars(text, vars_map))


def _inline_vars(text: str, vars_map: dict[str, str], depth: int = 0) -> str:
    """``text`` with each VAR name (any letter case) replaced by its definition, nested VARs
    included; bounded, so a cycle stops."""
    if depth > 4 or not vars_map:
        return text
    lower = {name.lower(): value for name, value in vars_map.items()}
    pattern = re.compile(
        r"(?<![\w\[])(" + "|".join(re.escape(name) for name in lower) + r")(?![\w\]])",
        re.IGNORECASE,
    )
    new = pattern.sub(lambda m: f"({lower[m.group(1).lower()]})", text)
    return _inline_vars(new, vars_map, depth + 1) if new != text else text


def _matching_close(text: str, open_index: int) -> int:
    """Index of the parenthesis that closes the one at ``open_index`` (-1 if none)."""
    depth = 0
    quote: str | None = None
    for index in range(open_index, len(text)):
        ch = text[index]
        if quote:
            if ch == quote:
                quote = None
            continue
        if ch in "\"'":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return index
    return -1


def _whole_call(text: str, function: str) -> list[str] | None:
    """The arguments of ``text`` when the whole text is one call of ``function``."""
    stripped = text.strip()
    head = re.match(rf"{function}\s*\(", stripped, re.IGNORECASE)
    if head is None:
        return None
    open_index = head.end() - 1
    if _matching_close(stripped, open_index) != len(stripped) - 1:
        return None
    return _split_balanced_args(stripped[open_index + 1 : -1])


def _strip_parens(text: str) -> str:
    text = text.strip()
    while text.startswith("(") and _matching_close(text, 0) == len(text) - 1:
        text = text[1:-1].strip()
    return text


def _text_literal(text: str, what: str) -> str:
    match = _TEXT_LITERAL.fullmatch(text)
    if match is None:
        raise DispatchDecline(f"{what} is not a text literal: {text.strip()[:60]}")
    return match.group(1)


def _top_level_equals(text: str) -> int | None:
    """Index of the single ``=`` outside brackets and quotes (None for <=, >=, <>, ==)."""
    depth = 0
    quote: str | None = None
    for index, ch in enumerate(text):
        if quote:
            if ch == quote:
                quote = None
            continue
        if ch in "\"'":
            quote = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "=" and depth == 0:
            before = text[index - 1] if index else ""
            after = text[index + 1] if index + 1 < len(text) else ""
            if before in "<>!=" or after == "=":
                return None
            return index
    return None
