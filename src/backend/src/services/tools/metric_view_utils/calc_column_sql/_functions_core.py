"""Logic, BLANK handling, DIVIDE, RELATED, and the functions refused by name.

Each function takes ``(emitter, args, env)`` and returns a ``Val``. ``args`` are parser
nodes; the emitter turns them into values.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from ._common import (
    KIND_BLANK,
    KIND_BOOLEAN,
    KIND_NUMBER,
    DaxRefusal,
    Val,
    arity,
    as_bool,
    as_number,
    unify_kinds,
)
from ._parser import Bool, Call, Group, Num, Unary

#: Functions that cannot be a row-level expression, with the reason shown to the user.
REFUSED_FUNCTIONS: dict[str, str] = {
    "TODAY": "TODAY() changes every day; a refreshed calculated column does not",
    "NOW": "NOW() changes every time it is read; a refreshed calculated column does not",
    "RELATEDTABLE": "RELATEDTABLE returns rows of another table (row context over another table)",
    "CALCULATE": "CALCULATE changes filter context; it is not a row-level expression",
    "CALCULATETABLE": "CALCULATETABLE changes filter context; it is not a row-level expression",
    "FILTER": "FILTER returns rows of another table (row context over another table)",
    "SUMX": "SUMX iterates another table row by row (row context over another table)",
    "AVERAGEX": "AVERAGEX iterates another table row by row (row context over another table)",
    "MINX": "MINX iterates another table row by row (row context over another table)",
    "MAXX": "MAXX iterates another table row by row (row context over another table)",
    "COUNTX": "COUNTX iterates another table row by row (row context over another table)",
    "COUNTAX": "COUNTAX iterates another table row by row (row context over another table)",
    "PRODUCTX": "PRODUCTX iterates another table row by row (row context over another table)",
    "CONCATENATEX": "CONCATENATEX iterates another table row by row (row context over another table)",
    "EARLIER": "EARLIER reads an outer row context; not translated",
    "EARLIEST": "EARLIEST reads an outer row context; not translated",
    "LOOKUPVALUE": (
        "LOOKUPVALUE searches another table with a filter (a correlated lookup). It could "
        "become a LEFT JOIN on a unique key; this translator does not build one"
    ),
    "RANKX": "RANKX ranks over a table; not a row-level expression",
    "USERELATIONSHIP": (
        "USERELATIONSHIP activates an inactive relationship; the source view has only the "
        "active join"
    ),
    "PATH": "PATH walks a parent-child hierarchy; not translated",
    "PATHITEM": "PATHITEM walks a parent-child hierarchy; not translated",
    "PATHITEMREVERSE": "PATHITEMREVERSE walks a parent-child hierarchy; not translated",
    "PATHLENGTH": "PATHLENGTH walks a parent-child hierarchy; not translated",
    "PATHCONTAINS": "PATHCONTAINS walks a parent-child hierarchy; not translated",
    "PATHPOSITION": "PATHPOSITION walks a parent-child hierarchy; not translated",
    "VALUES": "VALUES reads filter context; not a row-level expression",
    "SELECTEDVALUE": "SELECTEDVALUE reads filter context; not a row-level expression",
    "HASONEVALUE": "HASONEVALUE reads filter context; not a row-level expression",
    "ALL": "ALL removes filters; not a row-level expression",
    "ALLSELECTED": "ALLSELECTED reads filter context; not a row-level expression",
    "ISFILTERED": "ISFILTERED reads filter context; not a row-level expression",
}

if TYPE_CHECKING:
    from ._emitter import RowEmitter


def _is_true(node: object) -> bool:
    if isinstance(node, Bool):
        return node.value
    return isinstance(node, Call) and node.name == "TRUE" and not node.args


def _is_constant(node: object) -> bool:
    """DIVIDE's alternate result must be a constant in DAX."""
    if isinstance(node, Group):
        return _is_constant(node.inner)
    if isinstance(node, Unary):
        return isinstance(node.operand, Num)
    return isinstance(node, (Num, Bool)) or (
        isinstance(node, Call) and node.name == "BLANK" and not node.args
    )


def _if(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 2, 3, "IF")
    condition = as_bool(em.emit(args[0], env), "IF")
    then = em.emit(args[1], env)
    if len(args) == 2:
        return Val(f"(CASE WHEN {condition} THEN {then.sql} END)", then.kind, True)
    other = em.emit(args[2], env)
    kind = unify_kinds(then.kind, other.kind, "IF branches")
    sql = f"(CASE WHEN {condition} THEN {then.sql} ELSE {other.sql} END)"
    return Val(sql, kind, then.blank or other.blank)


def _switch_arms(
    em: RowEmitter, subject_node: object, pairs: list[object], env: Mapping[str, Val]
) -> list[tuple[str, Val]]:
    """(test, result) per arm. ``SWITCH(TRUE(), ...)`` tests are conditions; otherwise
    each test is a DAX equality with the subject (BLANK matching 0 or "" as DAX does).
    """
    arms: list[tuple[str, Val]] = []
    if _is_true(subject_node):
        for test_node, result_node in zip(pairs[::2], pairs[1::2]):
            test = as_bool(em.emit(test_node, env), "SWITCH(TRUE(), ...)")
            arms.append((test, em.emit(result_node, env)))
        return arms
    subject = em.emit(subject_node, env)
    for test_node, result_node in zip(pairs[::2], pairs[1::2]):
        test = em.compare("=", subject, em.emit(test_node, env)).sql
        arms.append((test, em.emit(result_node, env)))
    return arms


def _switch(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 3, 255, "SWITCH")
    rest: list[object] = list(args[1:])
    default_node: object | None = None
    if len(rest) % 2 == 1:
        default_node, rest = rest[-1], rest[:-1]
    arms = _switch_arms(em, args[0], rest, env)
    default = None if default_node is None else em.emit(default_node, env)
    kind = KIND_BLANK
    blank = default is None
    for _, result in arms:
        kind = unify_kinds(kind, result.kind, "SWITCH results")
        blank = blank or result.blank
    if default is not None:
        kind = unify_kinds(kind, default.kind, "SWITCH results")
        blank = blank or default.blank
    whens = " ".join(f"WHEN {test} THEN {result.sql}" for test, result in arms)
    otherwise = "" if default is None else f" ELSE {default.sql}"
    return Val(f"(CASE {whens}{otherwise} END)", kind, blank)


def _and(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 2, 2, "AND")
    left = as_bool(em.emit(args[0], env), "AND")
    right = as_bool(em.emit(args[1], env), "AND")
    return Val(f"({left} AND {right})", KIND_BOOLEAN, False)


def _or(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 2, 2, "OR")
    left = as_bool(em.emit(args[0], env), "OR")
    right = as_bool(em.emit(args[1], env), "OR")
    return Val(f"({left} OR {right})", KIND_BOOLEAN, False)


def _not(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 1, 1, "NOT")
    return Val(f"(NOT {as_bool(em.emit(args[0], env), 'NOT')})", KIND_BOOLEAN, False)


def _isblank(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 1, 1, "ISBLANK")
    value = em.emit(args[0], env)
    if not value.blank:
        return Val("FALSE", KIND_BOOLEAN, False)
    return Val(f"({value.sql} IS NULL)", KIND_BOOLEAN, False)


def _blank(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 0, 0, "BLANK")
    return Val("NULL", KIND_BLANK, True)


def _true(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 0, 0, "TRUE")
    return Val("TRUE", KIND_BOOLEAN, False, lit=True)


def _false(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 0, 0, "FALSE")
    return Val("FALSE", KIND_BOOLEAN, False, lit=False)


def _coalesce(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    """First argument that is not BLANK; BLANK if all are (DAX documentation)."""
    arity(args, 2, 255, "COALESCE")
    values = [em.emit(node, env) for node in args]
    kind = KIND_BLANK
    for value in values:
        kind = unify_kinds(kind, value.kind, "COALESCE arguments")
    blank = all(value.blank for value in values)
    return Val(f"COALESCE({', '.join(v.sql for v in values)})", kind, blank)


def _divide(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    """DIVIDE(a, b[, alt]): BLANK (or ``alt``) when b is zero or BLANK."""
    arity(args, 2, 3, "DIVIDE")
    numerator = as_number(em.emit(args[0], env), "DIVIDE numerator")
    denominator = as_number(em.emit(args[1], env), "DIVIDE denominator")
    quotient = f"({numerator} / NULLIF({denominator}, 0))"
    if len(args) == 2:
        return Val(quotient, KIND_NUMBER, True)
    if not _is_constant(args[2]):
        raise DaxRefusal("DIVIDE's alternate result must be a constant (a DAX rule)")
    alternate = em.emit(args[2], env)
    if alternate.kind == KIND_BLANK:
        return Val(f"COALESCE({quotient}, NULL)", KIND_NUMBER, True)
    alt_sql = as_number(alternate, "DIVIDE alternate result")
    return Val(f"COALESCE({quotient}, {alt_sql})", KIND_NUMBER, False)


def _related(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 1, 1, "RELATED")
    return em.related_column(args[0])


CORE_FUNCTIONS = {
    "IF": _if,
    "SWITCH": _switch,
    "AND": _and,
    "OR": _or,
    "NOT": _not,
    "ISBLANK": _isblank,
    "BLANK": _blank,
    "TRUE": _true,
    "FALSE": _false,
    "COALESCE": _coalesce,
    "DIVIDE": _divide,
    "RELATED": _related,
}
