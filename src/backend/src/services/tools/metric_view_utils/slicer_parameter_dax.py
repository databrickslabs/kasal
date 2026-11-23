"""DAX shape of a measure that dispatches on a slicer selector.

A Power BI selector is a value a report reader picks on a slicer, read back in a
measure with ``SELECTEDVALUE(T[c])``. When ``T`` is a disconnected table (no
relationship to any table of the model), the measure's value depends on that
pick and on nothing in the query's grouping. This module recognises the two shapes
that a metric view can carry as a parameter:

* ``SWITCH(SELECTEDVALUE(T[c] [, alternate]), "A", x, "B", y [, otherwise])``
* ``IF(SELECTEDVALUE(T[c]) = "A", x, y [, ...])``

A VAR-bound selector (``VAR s = SELECTEDVALUE(T[c]) RETURN SWITCH(s, ...)``) is
read as the same shape once the VARs are expanded. Anything else raises
``DispatchDecline`` with the reason, so the caller can report it.

Pure DAX text handling, no model state: whether ``T`` is disconnected is decided by
the caller (``slicer_parameters``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.services.powerbi.switch_decomposition import (
    _parse_dax_vars,
    _split_balanced_args,
    _substitute_vars,
)


class DispatchDecline(Exception):
    """A measure with a selector that cannot be read as a parameter. The message is the
    reason shown in the report; it names the branch when a branch is the cause."""


@dataclass(frozen=True)
class SelectorRef:
    """A ``SELECTEDVALUE(table[column] [, alternate])`` call."""

    table: str
    column: str
    alternate: str | None  # the text literal of the second argument, when given


@dataclass(frozen=True)
class SelectorOption:
    """One selector value and the DAX that the measure returns for it."""

    value: str
    result: str


@dataclass(frozen=True)
class SelectorDispatch:
    """A measure read as one selector and the result for each of its values."""

    selector: SelectorRef
    options: tuple[SelectorOption, ...]
    otherwise: (
        str | None
    )  # the DAX returned when no option matches, when the DAX has one
    shape: str  # "SWITCH" or "IF"


_SELECTOR_CALL = re.compile(
    r"SELECTEDVALUE\s*\(\s*(?:'([^']+)'|([A-Za-z_]\w*))\s*\[\s*(\w+)\s*\]",
    re.IGNORECASE,
)
_COLUMN_ARG = re.compile(r"\s*(?:'([^']+)'|([A-Za-z_]\w*))\s*\[\s*(\w+)\s*\]\s*")
_TEXT_LITERAL = re.compile(r'\s*"([^"]*)"\s*')
_GROUPING_FUNCTION = re.compile(
    r"\b(ISFILTERED|HASONEVALUE|ISINSCOPE)\s*\(", re.IGNORECASE
)


def selector_refs(dax: str) -> list[SelectorRef]:
    """Every ``SELECTEDVALUE(T[c])`` written in ``dax``, VAR definitions included."""
    refs = []
    for match in _SELECTOR_CALL.finditer(dax or ""):
        table = match.group(1) or match.group(2)
        refs.append(SelectorRef(table=table, column=match.group(3), alternate=None))
    return refs


def reads_query_grouping(dax: str) -> bool:
    """Whether ``dax`` asks about the query's grouping (``ISFILTERED``, ``HASONEVALUE``,
    ``ISINSCOPE``). That is not a selector value and a parameter cannot stand for it."""
    return bool(_GROUPING_FUNCTION.search(dax or ""))


def parse_selector_dispatch(dax: str) -> SelectorDispatch:
    """Read a measure as a SWITCH or IF on one SELECTEDVALUE. Raises ``DispatchDecline``."""
    body = _expanded_body(dax)
    switch_args = _whole_call(body, "SWITCH")
    if switch_args is not None:
        return _switch_dispatch(switch_args)
    if_args = _whole_call(body, "IF")
    if if_args is not None:
        return _if_dispatch(if_args)
    raise DispatchDecline("not a SWITCH or IF on a selector value")


def _expanded_body(dax: str) -> str:
    """The RETURN expression with VAR definitions inlined and outer parentheses removed."""
    text = (dax or "").strip()
    vars_map: dict[str, str] = {}
    if re.match(r"(?is)^var\s", text):
        vars_map = _parse_dax_vars(text)
        ret = re.search(r"\breturn\b\s*(.+)$", text, re.IGNORECASE | re.DOTALL)
        if ret is None:
            raise DispatchDecline("VAR block without a RETURN expression")
        text = ret.group(1)
    text = _substitute_vars(text, vars_map).strip()
    while text.startswith("(") and _matching_close(text, 0) == len(text) - 1:
        text = text[1:-1].strip()
    return text


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


def _parse_selectedvalue(text: str) -> SelectorRef:
    args = _whole_call(_strip_parens(text), "SELECTEDVALUE")
    if args is None or len(args) not in (1, 2):
        raise DispatchDecline("the selector is not a SELECTEDVALUE(table[column]) call")
    column = _COLUMN_ARG.fullmatch(args[0])
    if column is None:
        raise DispatchDecline("SELECTEDVALUE does not name a table column")
    alternate = None
    if len(args) == 2:
        alternate = _text_literal(args[1], "the SELECTEDVALUE alternate")
    return SelectorRef(
        table=column.group(1) or column.group(2),
        column=column.group(3),
        alternate=alternate,
    )


def _text_literal(text: str, what: str) -> str:
    match = _TEXT_LITERAL.fullmatch(text)
    if match is None:
        raise DispatchDecline(f"{what} is not a text literal: {text.strip()[:60]}")
    return match.group(1)


def _switch_dispatch(args: list[str]) -> SelectorDispatch:
    if len(args) < 3:
        raise DispatchDecline("SWITCH without a branch")
    first = _strip_parens(args[0])
    if not first.upper().startswith("SELECTEDVALUE"):
        raise DispatchDecline("SWITCH is on a condition, not on a selector value")
    selector = _parse_selectedvalue(first)
    rest = args[1:]
    otherwise = rest[-1] if len(rest) % 2 == 1 else None
    pairs = rest[:-1] if otherwise is not None else rest
    options = tuple(
        SelectorOption(
            value=_text_literal(pairs[i], "a SWITCH option value"),
            result=pairs[i + 1],
        )
        for i in range(0, len(pairs), 2)
    )
    return SelectorDispatch(selector, options, otherwise, "SWITCH")


def _if_dispatch(args: list[str]) -> SelectorDispatch:
    if len(args) not in (2, 3):
        raise DispatchDecline("IF without a true and a false branch")
    condition = _strip_parens(args[0])
    eq = _top_level_equals(condition)
    if eq is None:
        raise DispatchDecline("IF condition is not an equality with a selector value")
    selector = _parse_selectedvalue(_strip_parens(condition[:eq]))
    value = _text_literal(condition[eq + 1 :], "the IF comparison value")
    otherwise = args[2] if len(args) == 3 else None
    return SelectorDispatch(
        selector, (SelectorOption(value, args[1]),), otherwise, "IF"
    )


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
