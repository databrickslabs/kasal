"""Shared types for the row-level DAX translator (calculated column -> SQL).

``Val`` is one translated sub-expression: its SQL, its DAX kind, and whether that
SQL can be NULL (DAX BLANK). The BLANK mapping is written down once, here, in the
``as_*`` helpers:

* number: BLANK is 0 (arithmetic, numeric arguments, comparisons with numbers);
* text: BLANK is ``""`` (``&``, text functions, comparisons with text);
* boolean: BLANK is FALSE (a condition);
* date: BLANK is ``DATE(1899, 12, 30)``, DAX's date 0 (date functions, comparisons).

DAX's comparison operators treat BLANK as 0, ``""``, 1899-12-30 or FALSE, which is
the same mapping. ``IF`` without ``ELSE``, a ``DIVIDE`` by zero and a ``SWITCH``
with no match return BLANK, so those are NULL.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from ..dax_condition_sql import _sql_str_literal

#: Longest expression accepted. Long inputs are refused, not truncated.
MAX_EXPRESSION_CHARS = 8000
#: Longest SQL one sub-expression may become. VAR values are inlined, so a chain of
#: VARs that each reuse the previous one doubles the text; this bounds it.
MAX_SQL_CHARS = 4000

KIND_INTEGER = "integer"
KIND_NUMBER = "number"
KIND_TEXT = "text"
KIND_BOOLEAN = "boolean"
KIND_DATE = "date"
KIND_BLANK = "blank"
KIND_UNKNOWN = "unknown"

#: Kinds a caller may declare for a source-view column (``column_kinds``).
DECLARABLE_KINDS = frozenset(
    {KIND_INTEGER, KIND_NUMBER, KIND_TEXT, KIND_BOOLEAN, KIND_DATE}
)

#: DAX's stored date 0. BLANK compares equal to it and date functions read BLANK as it.
DATE_ZERO = "DATE '1899-12-30'"


class DaxRefusal(Exception):
    """An expression, or part of one, that this translator does not map exactly."""


@dataclass(frozen=True)
class Val:
    """One translated sub-expression.

    ``lit`` holds the Python value for a literal (text, number text, or bool) and is
    ``None`` otherwise. ``blank`` is True when ``sql`` can be NULL.
    """

    sql: str
    kind: str
    blank: bool
    lit: str | bool | None = None


@dataclass(frozen=True)
class RelatedTable:
    """A many-to-one relationship from the calculated column's table to another.

    The caller passes only active, direct relationships where ``to_column`` is unique
    in ``view``. ``RELATED(Other[Col])`` becomes a ``LEFT JOIN`` on that key, which
    returns BLANK (NULL) when no related row matches, as DAX does.
    """

    alias: str
    view: str
    from_column: str
    to_column: str
    columns: Collection[str]
    column_kinds: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class RelatedJoin:
    """The join the caller must add to the source view for one related table."""

    table: str
    alias: str
    sql: str


def ident(name: str) -> str:
    """A back-quoted Databricks identifier (backticks inside the name are doubled)."""
    return "`" + name.replace("`", "``") + "`"


def sql_str(value: str) -> str:
    """A single-quoted SQL string literal, escaped like every other DAX-sourced value."""
    return _sql_str_literal(value)


def arity(args: tuple, low: int, high: int, name: str) -> None:
    """Refuse a call whose argument count DAX would not accept here."""
    if not low <= len(args) <= high:
        if low == high:
            expected = f"{low}"
        else:
            expected = f"{low} to {high}"
        raise DaxRefusal(f"{name} takes {expected} arguments here, got {len(args)}")


def unify_kinds(first: str, second: str, what: str) -> str:
    """The kind of a result that is either of two branch kinds.

    BLANK and unknown defer to the other side. Integer and number merge to number.
    Anything else would make DAX convert one branch to the other, which this module
    does not reproduce, so it is refused.
    """
    if first == KIND_BLANK:
        return second
    if second == KIND_BLANK:
        return first
    if first == KIND_UNKNOWN:
        return second
    if second == KIND_UNKNOWN:
        return first
    if first == second:
        return first
    if {first, second} == {KIND_INTEGER, KIND_NUMBER}:
        return KIND_NUMBER
    raise DaxRefusal(
        f"{what} mix {first} and {second} results; DAX converts between them and "
        "this translation does not"
    )


def _unknown_hint(kind: str) -> str:
    if kind == KIND_UNKNOWN:
        return " (a column of unknown type: pass its kind in column_kinds)"
    return ""


def as_number(value: Val, what: str) -> str:
    """SQL for a DAX number. BLANK reads as 0."""
    if value.kind == KIND_BLANK:
        return "0"
    if value.kind in (KIND_INTEGER, KIND_NUMBER, KIND_UNKNOWN):
        return f"COALESCE({value.sql}, 0)" if value.blank else value.sql
    raise DaxRefusal(
        f"{what} needs a number, but it is {value.kind}{_unknown_hint(value.kind)}"
    )


def as_text(value: Val, what: str) -> str:
    """SQL for DAX text. BLANK reads as the empty string.

    Numbers are refused: DAX formats a number to text with its own rules, and a
    Spark cast of a decimal ("1.50") differs. Integers are cast, which is exact.
    """
    if value.kind == KIND_BLANK:
        return "''"
    if value.kind == KIND_TEXT:
        return f"COALESCE({value.sql}, '')" if value.blank else value.sql
    if value.kind == KIND_INTEGER:
        cast = f"CAST({value.sql} AS STRING)"
        return f"COALESCE({cast}, '')" if value.blank else cast
    if value.kind == KIND_NUMBER:
        raise DaxRefusal(
            f"{what} converts a number to text; DAX's number formatting is not "
            "reproduced for non-integer numbers"
        )
    raise DaxRefusal(
        f"{what} needs text, but it is {value.kind}{_unknown_hint(value.kind)}"
    )


def as_bool(value: Val, what: str) -> str:
    """SQL for a two-valued DAX condition. BLANK reads as FALSE.

    An unknown-kind column is taken as a boolean: DAX rejects a non-boolean
    condition, so a model that evaluates at all has a boolean there.
    """
    if value.kind == KIND_BLANK:
        return "FALSE"
    if value.kind in (KIND_BOOLEAN, KIND_UNKNOWN):
        return f"COALESCE({value.sql}, FALSE)" if value.blank else value.sql
    raise DaxRefusal(f"{what} needs a condition, but it is {value.kind}")


def as_date(value: Val, what: str) -> str:
    """SQL for a DAX date. BLANK reads as 1899-12-30 (DAX's date 0)."""
    if value.kind == KIND_BLANK:
        return DATE_ZERO
    if value.kind in (KIND_DATE, KIND_UNKNOWN):
        return f"COALESCE({value.sql}, {DATE_ZERO})" if value.blank else value.sql
    raise DaxRefusal(
        f"{what} needs a date, but it is {value.kind}{_unknown_hint(value.kind)}"
    )
