"""Date functions: YEAR, MONTH, DAY, DATE, EOMONTH, EDATE, DATEDIFF, WEEKDAY, WEEKNUM.

Documented DAX rules that are reproduced here:

* DATE: a year of 0-49 means 2000+, 50-99 means 1900+, 100-9999 is itself; a month
  above 12 and a day above the month length roll over (``DATE(2008, 14, 2)`` is
  2009-02-02). Done as ``add_months`` and ``date_add`` on January 1st.
* DATEDIFF counts interval boundaries. Only DAY is translated: DAY is the day count
  between the two dates (documented example: 914), which is the exact difference
  for date values. MONTH, QUARTER, YEAR and WEEK count boundaries and are refused.
* WEEKDAY return types 1 (Sunday = 1), 2 (Monday = 1) and 3 (Monday = 0) are the
  documented ones. WEEKNUM return types 1 and 2 use "the week containing January 1 is
  week 1" and 21 is ISO 8601; 11-17 are refused.

TODAY and NOW are refused in ``_functions_core``: their value changes between refreshes.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Callable

from ._common import (
    KIND_BLANK,
    KIND_DATE,
    KIND_INTEGER,
    DaxRefusal,
    Val,
    arity,
    as_date,
    as_number,
)
from ._parser import Num, Ref

if TYPE_CHECKING:
    from ._emitter import RowEmitter


def _whole(expression: str) -> str:
    """DAX rounds a non-integer year, month or day to the nearest whole number."""
    return f"CAST(ROUND({expression}) AS INT)"


def _date_part(
    spark_fn: str, name: str
) -> Callable[[RowEmitter, tuple[object, ...], Mapping[str, Val]], Val]:
    def function(
        em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]
    ) -> Val:
        arity(args, 1, 1, name)
        return Val(
            f"{spark_fn}({as_date(em.emit(args[0], env), name)})", KIND_INTEGER, False
        )

    return function


def _date(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 3, 3, "DATE")
    year = _whole(as_number(em.emit(args[0], env), "DATE year"))
    month = _whole(as_number(em.emit(args[1], env), "DATE month"))
    day = _whole(as_number(em.emit(args[2], env), "DATE day"))
    full_year = (
        f"(CASE WHEN {year} BETWEEN 0 AND 49 THEN {year} + 2000 "
        f"WHEN {year} BETWEEN 50 AND 99 THEN {year} + 1900 ELSE {year} END)"
    )
    sql = f"date_add(add_months(make_date({full_year}, 1, 1), {month} - 1), {day} - 1)"
    return Val(sql, KIND_DATE, False)


def _shifted_date(
    args: tuple[object, ...], env: Mapping[str, Val], em: RowEmitter, name: str
) -> tuple[str, str]:
    arity(args, 2, 2, name)
    start = as_date(em.emit(args[0], env), name)
    months = _whole(as_number(em.emit(args[1], env), f"{name} months"))
    return start, months


def _eomonth(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    """Last day of the month ``months`` after the start date."""
    start, months = _shifted_date(args, env, em, "EOMONTH")
    return Val(f"last_day(add_months({start}, {months}))", KIND_DATE, False)


def _edate(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    """The date ``months`` after the start; the day is clamped to the month length."""
    start, months = _shifted_date(args, env, em, "EDATE")
    return Val(f"add_months({start}, {months})", KIND_DATE, False)


def _datediff(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 3, 3, "DATEDIFF")
    unit = args[2]
    if not (isinstance(unit, Ref) and unit.name.upper() == "DAY"):
        raise DaxRefusal(
            "DATEDIFF is translated for DAY only: MONTH, QUARTER, YEAR and WEEK count "
            "interval boundaries, and SECOND, MINUTE, HOUR need datetime handling"
        )
    start = em.emit(args[0], env)
    end = em.emit(args[1], env)
    for value, label in ((start, "start"), (end, "end")):
        if value.kind not in (KIND_DATE, KIND_BLANK):
            raise DaxRefusal(
                f"DATEDIFF needs a date for its {label} argument, but it is {value.kind}; "
                "pass the column's kind as 'date' in column_kinds"
            )
    sql = f"datediff({as_date(end, 'DATEDIFF')}, {as_date(start, 'DATEDIFF')})"
    return Val(sql, KIND_INTEGER, False)


def _return_type(
    args: tuple[object, ...],
    index: int,
    allowed: tuple[int, ...],
    name: str,
    default: int,
) -> int:
    if len(args) <= index:
        return default
    node = args[index]
    if not (isinstance(node, Num) and node.text.isdigit()):
        raise DaxRefusal(f"{name} return_type must be a whole-number literal")
    value = int(node.text)
    if value not in allowed:
        raise DaxRefusal(f"{name} return_type {value} is not translated")
    return value


def _weekday(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 1, 2, "WEEKDAY")
    rtype = _return_type(args, 1, (1, 2, 3), "WEEKDAY", 1)
    day = f"dayofweek({as_date(em.emit(args[0], env), 'WEEKDAY')})"  # Sunday = 1
    if rtype == 1:
        return Val(day, KIND_INTEGER, False)
    if rtype == 2:
        return Val(f"(pmod({day} - 2, 7) + 1)", KIND_INTEGER, False)
    return Val(f"pmod({day} - 2, 7)", KIND_INTEGER, False)  # Monday = 0


def _weeknum(em: RowEmitter, args: tuple[object, ...], env: Mapping[str, Val]) -> Val:
    arity(args, 1, 2, "WEEKNUM")
    rtype = _return_type(args, 1, (1, 2, 21), "WEEKNUM", 1)
    day = as_date(em.emit(args[0], env), "WEEKNUM")
    if rtype == 21:
        return Val(f"weekofyear({day})", KIND_INTEGER, False)
    first_weekday = 1 if rtype == 1 else 2  # the week starts on this dayofweek value
    jan1 = f"make_date(year({day}), 1, 1)"
    offset = f"pmod(dayofweek({jan1}) - {first_weekday}, 7)"
    sql = f"(CAST(FLOOR((dayofyear({day}) - 1 + {offset}) / 7) AS INT) + 1)"
    return Val(sql, KIND_INTEGER, False)


DATE_FUNCTIONS = {
    "YEAR": _date_part("year", "YEAR"),
    "MONTH": _date_part("month", "MONTH"),
    "DAY": _date_part("dayofmonth", "DAY"),
    "DATE": _date,
    "EOMONTH": _eomonth,
    "EDATE": _edate,
    "DATEDIFF": _datediff,
    "WEEKDAY": _weekday,
    "WEEKNUM": _weeknum,
}
