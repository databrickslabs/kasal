"""Power BI calculated columns -> Databricks SQL for the source view (standalone).

``translate_calculated_column`` reads one DAX row-level formula and returns either SQL
over the table's own source-view columns (plus any ``RELATED`` joins the caller must
add) or a refusal with a reason. It is pure: no I/O, no warehouse, no model access.
It is not wired into the source-layer builder.

Naming: a source-view column is named as the Power BI column is (the source view's
``Table.RenameColumns`` aliases carry the model names). The translator does not rename
or snake_case anything; ``columns`` must be the names the view actually exposes, and a
DAX reference is matched to them case-insensitively, returning the view's spelling.

Translated (row level, one table, DAX semantics kept):

* columns of the same table; literals; ``+ - *``; unary ``-``; ``/`` only with a
  non-zero literal divisor; ``&``; ``= <> < <= > >=``; ``&&`` ``||`` ``NOT``; ``IN {...}``
  with literal items; parentheses.
* ``IF`` (2 and 3 arguments), ``SWITCH(expr, ...)`` and ``SWITCH(TRUE(), ...)``,
  ``AND``, ``OR``, ``ISBLANK``, ``BLANK``, ``COALESCE``, ``DIVIDE`` (2 and 3 arguments,
  constant alternate result), ``VAR ... RETURN`` (values are inlined).
* text: ``LEFT``, ``RIGHT``, ``MID`` (literal start), ``LEN``, ``UPPER``, ``LOWER``,
  ``TRIM`` (collapses inner space runs, as DAX does), ``CONCATENATE`` (2 arguments),
  ``SUBSTITUTE`` (3 arguments), ``SEARCH`` and ``FIND`` (with NotFoundValue; SEARCH
  literal text without ``? * ~``), ``VALUE``, ``FORMAT`` (date patterns yyyy, yy, mm, m,
  dd, d with - or space).
* dates: ``YEAR``, ``MONTH``, ``DAY``, ``DATE``, ``EOMONTH``, ``EDATE``, ``DATEDIFF``
  (DAY only), ``WEEKDAY`` (return type 1, 2, 3), ``WEEKNUM`` (return type 1, 2, 21).
* ``RELATED(Other[Col])`` through a direct many-to-one ``RelatedTable``: a
  ``LEFT JOIN`` on the key, returned in ``joins`` (the source view must be read
  ``AS`` ``source_alias`` and must select only its own columns, so the join adds none).

Refused with a reason: ``TODAY`` and ``NOW`` (the value changes between refreshes),
``RELATEDTABLE``, ``CALCULATE``, ``FILTER``, the ``*X`` iterators, ``EARLIER``,
``LOOKUPVALUE`` (a join could replace it later; not built), ``RANKX``, ``PATH*``,
``USERELATIONSHIP``, filter-context functions, any other function, a reference to
another table outside ``RELATED``, and a reference to a calculated column (the source
view holds those only as TODO). Also refused: text ordering, mixed-type comparisons,
number-to-text conversion of non-integers, a comparison of two columns of unknown type
(pass ``column_kinds``), ``^``, ``==``, and ``DATEDIFF`` on a column without a date kind.

BLANK mapping (see ``_common``): BLANK is NULL. Arithmetic and numeric arguments read
it as 0; text reads it as ``""``; a condition reads it as FALSE; a date as 1899-12-30.
Comparison operators read BLANK as 0, ``""``, 1899-12-30 or FALSE, so comparisons are
two-valued and never NULL. ``IF`` without ``ELSE``, ``DIVIDE`` by zero, ``SWITCH`` with
no match, and ``COALESCE`` of BLANKs return BLANK (NULL).

Not faithful, or unverified against Power BI (see the report):

* ``A - B`` with both BLANK gives 0 here; DAX may give BLANK.
* text ``=`` ignores case (``lower`` on both sides); the Microsoft pages fetched do not
  state it, so it rests on DAX behaviour as understood, not on a checked source.
* ``VALUE`` of non-numeric or empty text gives BLANK; DAX raises an error.
* ``DATE``: a month or day of 0 or below is not documented and is not guarded.
* a date column read as BLANK (YEAR, WEEKDAY, DATEDIFF) is read as 1899-12-30; the
  documentation says BLANK is 0 in date arithmetic, but this was not run in Power BI.
* ``DIVIDE`` with a BLANK denominator is treated as a zero denominator (BLANK is 0).
* the translated SQL is verified on a Databricks warehouse; the DAX side is verified
  only against the Microsoft documentation, not by evaluating DAX.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass

from ._common import (
    DECLARABLE_KINDS,
    MAX_EXPRESSION_CHARS,
    DaxRefusal,
    RelatedJoin,
    RelatedTable,
)
from ._emitter import RowEmitter
from ._parser import parse_dax

__all__ = [
    "CalcColumnSql",
    "RelatedJoin",
    "RelatedTable",
    "translate_calculated_column",
]

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class CalcColumnSql:
    """The result of one translation.

    ``refusal`` is empty when the formula translated. Otherwise ``sql`` is empty and
    ``refusal`` names the construct. ``kind`` is the DAX kind of the result (``number``,
    ``text``, ``boolean``, ``date``, ``integer``, ``unknown``, ``blank``). ``may_be_blank``
    says the result can be NULL (DAX BLANK). ``joins`` are the ``LEFT JOIN`` clauses the
    caller adds to the source view, one per related table, in the order referenced.
    """

    sql: str
    kind: str
    may_be_blank: bool
    joins: tuple[RelatedJoin, ...]
    refusal: str

    @property
    def translated(self) -> bool:
        return not self.refusal


def translate_calculated_column(
    expression: str,
    table: str,
    columns: Collection[str],
    related: Mapping[str, RelatedTable] | None = None,
    *,
    column_kinds: Mapping[str, str] | None = None,
    calculated_columns: Collection[str] = (),
    source_alias: str | None = None,
) -> CalcColumnSql:
    """Translate one DAX calculated-column formula of ``table``.

    Args:
        expression: the DAX formula (the TMDL ``expression``, without a leading ``=``).
        table: the Power BI table the column belongs to.
        columns: the column names the table's source view exposes, as named there.
        related: direct many-to-one relationships from ``table``, keyed by the related
            table's Power BI name. Only the active ones.
        column_kinds: optional DAX kind per column (``integer``, ``number``, ``text``,
            ``boolean``, ``date``). Without a kind, a column is refused where the
            translation needs its type (text operations, date arithmetic, column-column
            comparison).
        calculated_columns: the table's other calculated-column names, so a reference to
            one is refused with that reason.
        source_alias: the alias the source view is read under. Required for ``RELATED``.
            When given, own columns are qualified with it.

    Raises:
        ValueError: a caller error (a bad kind, alias or relationship); a formula that
            cannot be translated is a refusal, not an exception.
    """
    related = dict(related or {})
    kinds = dict(column_kinds or {})
    _check_inputs(kinds, related, source_alias)
    if not expression or not expression.strip():
        return _refused("the formula is empty")
    if len(expression) > MAX_EXPRESSION_CHARS:
        return _refused(f"the formula is longer than {MAX_EXPRESSION_CHARS} characters")
    emitter = RowEmitter(
        table=table,
        columns=tuple(columns),
        column_kinds=kinds,
        calculated=tuple(calculated_columns),
        related=related,
        source_alias=source_alias,
    )
    try:
        result = emitter.emit(parse_dax(expression), {})
    except DaxRefusal as refusal:
        return _refused(str(refusal))
    except RecursionError:
        return _refused("the formula nests too deeply to translate")
    return CalcColumnSql(
        sql=result.sql,
        kind=result.kind,
        may_be_blank=result.blank,
        joins=tuple(emitter.joins.values()),
        refusal="",
    )


def _refused(reason: str) -> CalcColumnSql:
    return CalcColumnSql(sql="", kind="", may_be_blank=False, joins=(), refusal=reason)


def _check_inputs(
    kinds: Mapping[str, str],
    related: Mapping[str, RelatedTable],
    source_alias: str | None,
) -> None:
    for column, kind in kinds.items():
        if kind not in DECLARABLE_KINDS:
            raise ValueError(
                f"column_kinds[{column!r}] must be one of {sorted(DECLARABLE_KINDS)}, "
                f"got {kind!r}"
            )
    if source_alias is not None and not _IDENTIFIER.fullmatch(source_alias):
        raise ValueError(
            f"source_alias must be a plain identifier, got {source_alias!r}"
        )
    for name, rel in related.items():
        if not _IDENTIFIER.fullmatch(rel.alias):
            raise ValueError(f"related alias for {name!r} must be a plain identifier")
        if source_alias is not None and rel.alias.casefold() == source_alias.casefold():
            raise ValueError(f"related alias for {name!r} equals source_alias")
        if not rel.view.strip():
            raise ValueError(f"related view for {name!r} is empty")
