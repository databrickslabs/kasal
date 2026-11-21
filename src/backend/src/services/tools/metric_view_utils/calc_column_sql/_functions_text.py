"""Text functions: LEFT, RIGHT, MID, LEN, UPPER, LOWER, TRIM, CONCATENATE, SUBSTITUTE,
SEARCH, FIND, VALUE, FORMAT.

Positions are 1-based in DAX and in Spark (``locate``, ``substring``), so they carry
over unchanged. SEARCH is case-insensitive in DAX (documented) and FIND is
case-sensitive, so ``lower()`` is applied to SEARCH only.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Callable

import re

from ._common import (
    KIND_BLANK,
    KIND_DATE,
    KIND_INTEGER,
    KIND_NUMBER,
    KIND_TEXT,
    KIND_UNKNOWN,
    DaxRefusal,
    Val,
    arity,
    as_number,
    as_text,
    sql_str,
)
from ._parser import Num

# A DAX date token must not run into more letters (so "mmm", a month name, is refused).
_DATE_TOKEN = re.compile(r"(yyyy|yy|mm|m|dd|d)(?![A-Za-z])|[- ]")
_SPARK_DATE_TOKEN = {
    "yyyy": "yyyy",
    "yy": "yy",
    "mm": "MM",
    "m": "M",
    "dd": "dd",
    "d": "d",
}
_SEARCH_WILDCARDS = frozenset("?*~")

if TYPE_CHECKING:
    from ._emitter import RowEmitter


def _start_position(
    em: RowEmitter, node: object, env: Mapping[str, Val], name: str
) -> str:
    """An optional 1-based start position: a literal >= 1, or BLANK (which means 1)."""
    value = em.emit(node, env)
    if value.kind == KIND_BLANK:
        return "1"
    if not (isinstance(node, Num) and node.text.isdigit() and int(node.text) >= 1):
        raise DaxRefusal(f"{name} start position must be a whole-number literal >= 1")
    return node.text


def _left_or_right(
    name: str,
) -> Callable[[RowEmitter, tuple[object, ...], Mapping[str, Val]], Val]:
    def function(
        em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]
    ) -> Val:
        arity(args, 2, 2, name.upper())
        text = as_text(em.emit(args[0], env), name.upper())
        count = as_number(em.emit(args[1], env), name.upper())
        return Val(f"{name}({text}, CAST({count} AS INT))", KIND_TEXT, False)

    return function


def _mid(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 3, 3, "MID")
    text = as_text(em.emit(args[0], env), "MID")
    start = _start_position(em, args[1], env, "MID")
    count = as_number(em.emit(args[2], env), "MID")
    return Val(f"substring({text}, {start}, CAST({count} AS INT))", KIND_TEXT, False)


def _len(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 1, 1, "LEN")
    return Val(f"length({as_text(em.emit(args[0], env), 'LEN')})", KIND_INTEGER, False)


def _case_function(
    fn: str,
) -> Callable[[RowEmitter, tuple[object, ...], Mapping[str, Val]], Val]:
    def function(
        em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]
    ) -> Val:
        arity(args, 1, 1, fn.upper())
        text = as_text(em.emit(args[0], env), fn.upper())
        return Val(f"{fn}({text})", KIND_TEXT, False)

    return function


def _trim(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    """DAX TRIM removes spaces except single spaces between words (documented), so runs
    of inner spaces collapse to one. Spark's ``trim`` only strips the ends."""
    arity(args, 1, 1, "TRIM")
    text = as_text(em.emit(args[0], env), "TRIM")
    sql = f"regexp_replace(trim({text}), {sql_str(' +')}, {sql_str(' ')})"
    return Val(sql, KIND_TEXT, False)


def _concatenate(
    em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]
) -> Val:
    arity(args, 2, 2, "CONCATENATE")
    left = as_text(em.emit(args[0], env), "CONCATENATE")
    right = as_text(em.emit(args[1], env), "CONCATENATE")
    return Val(f"concat({left}, {right})", KIND_TEXT, False)


def _substitute(
    em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]
) -> Val:
    """Three-argument SUBSTITUTE: every occurrence, case-sensitive. An empty search text
    leaves the text unchanged (checked against the warehouse)."""
    arity(args, 3, 3, "SUBSTITUTE")
    text = as_text(em.emit(args[0], env), "SUBSTITUTE")
    old = as_text(em.emit(args[1], env), "SUBSTITUTE")
    new = as_text(em.emit(args[2], env), "SUBSTITUTE")
    return Val(f"replace({text}, {old}, {new})", KIND_TEXT, False)


def _search_or_find(
    name: str, case_insensitive: bool
) -> Callable[[RowEmitter, tuple[object, ...], Mapping[str, Val]], Val]:
    def function(
        em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]
    ) -> Val:
        if len(args) != 4:
            raise DaxRefusal(
                f"{name} without NotFoundValue (a fourth argument) returns an error when the "
                "text is not found; a calculated column cannot reproduce that"
            )
        return _position(em, args, env, case_insensitive, name)

    return function


def _position(
    em: RowEmitter,
    args: tuple[object, ...],
    env: Mapping[str, Val],
    case_insensitive: bool,
    name: str,
) -> Val:
    """SEARCH / FIND. The 4-argument form is required: without NotFoundValue DAX raises an
    error when the text is absent, which a calculated column cannot reproduce."""
    find = em.emit(args[0], env)
    if find.kind != KIND_TEXT:
        raise DaxRefusal(f"{name} search text must be text")
    within = as_text(em.emit(args[1], env), name)
    start = _start_position(em, args[2], env, name)
    not_found = em.emit(args[3], env)
    if not_found.kind not in (KIND_INTEGER, KIND_NUMBER, KIND_BLANK):
        raise DaxRefusal(f"{name} NotFoundValue must be a number or BLANK()")
    if case_insensitive:
        if not isinstance(find.lit, str):
            raise DaxRefusal("SEARCH needs a literal search text")
        if _SEARCH_WILDCARDS & set(find.lit):
            raise DaxRefusal(
                "SEARCH wildcards (? * ~) in the search text are not translated; FIND "
                "matches literally"
            )
        needle = sql_str(find.lit.lower())
        haystack = f"lower({within})"
    else:
        needle = as_text(find, name)
        haystack = within
    position = f"locate({needle}, {haystack}, {start})"
    sql = f"COALESCE(NULLIF({position}, 0), {not_found.sql})"
    kind = KIND_NUMBER if not_found.kind == KIND_NUMBER else KIND_INTEGER
    return Val(sql, kind, not_found.kind == KIND_BLANK)


def _value(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    """VALUE(text). Documented: a text in a number format converts, anything else is an
    error. Here non-numeric text and empty text become BLANK instead of an error, which is
    not faithful; the refusal is in the module docstring."""
    arity(args, 1, 1, "VALUE")
    value = em.emit(args[0], env)
    if value.kind == KIND_BLANK:
        return Val("NULL", KIND_NUMBER, True)
    if value.kind in (KIND_INTEGER, KIND_NUMBER):
        return Val(value.sql, KIND_NUMBER, value.blank)
    if value.kind == KIND_TEXT:
        is_literal = isinstance(value.lit, str)
        blank = not is_literal or value.lit == "" or value.blank
        return Val(f"try_cast({value.sql} AS DOUBLE)", KIND_NUMBER, blank)
    if value.kind == KIND_UNKNOWN:
        return Val(f"try_cast({value.sql} AS DOUBLE)", KIND_NUMBER, True)
    raise DaxRefusal(f"VALUE needs text or a number, but it is {value.kind}")


def _spark_date_pattern(text: str) -> str:
    """A DAX date format string as a Spark ``date_format`` pattern. Only the tokens
    below are mapped; anything else (``mmm``, ``dddd``, ``/`` which is locale-dependent,
    times) is refused, not guessed."""
    out: list[str] = []
    pos = 0
    while pos < len(text):
        match = _DATE_TOKEN.match(text, pos)
        if match is None:
            raise DaxRefusal(
                f"FORMAT pattern element {text[pos:]!r} is not translated; only yyyy, yy, "
                "mm, m, dd, d and - or space are"
            )
        token = match.group()
        out.append(_SPARK_DATE_TOKEN.get(token, token))
        pos = match.end()
    return "".join(out)


def _format(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    """FORMAT(date, pattern) for numeric date patterns only. A BLANK value gives "" (DAX
    documentation). Numeric formats are refused (rounding and locale rules)."""
    arity(args, 2, 2, "FORMAT")
    pattern = em.emit(args[1], env)
    if pattern.kind != KIND_TEXT or not isinstance(pattern.lit, str):
        raise DaxRefusal("FORMAT needs a literal pattern")
    spark = _spark_date_pattern(pattern.lit)
    value = em.emit(args[0], env)
    if value.kind == KIND_BLANK:
        return Val("''", KIND_TEXT, False)
    if value.kind != KIND_DATE:
        raise DaxRefusal(
            "FORMAT is translated for date values only; a number or column of unknown "
            "type is refused"
        )
    sql = f"date_format({value.sql}, {sql_str(spark)})"
    if value.blank:
        # DAX gives "" for a BLANK date; formatting the 1899-12-30 stand-in would not.
        sql = f"COALESCE({sql}, '')"
    return Val(sql, KIND_TEXT, False)


TEXT_FUNCTIONS = {
    "LEFT": _left_or_right("left"),
    "RIGHT": _left_or_right("right"),
    "MID": _mid,
    "LEN": _len,
    "UPPER": _case_function("upper"),
    "LOWER": _case_function("lower"),
    "TRIM": _trim,
    "CONCATENATE": _concatenate,
    "SUBSTITUTE": _substitute,
    "SEARCH": _search_or_find("SEARCH", case_insensitive=True),
    "FIND": _search_or_find("FIND", case_insensitive=False),
    "VALUE": _value,
    "FORMAT": _format,
}
