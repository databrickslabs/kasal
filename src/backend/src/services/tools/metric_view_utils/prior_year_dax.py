"""Prior-year measures: find the year shift in a measure's DAX and take it out.

A prior-year measure is another calculation, one year earlier::

    CALCULATE([Sales], SAMEPERIODLASTYEAR(Calendar[Date]))
    CALCULATE([Sales], DATEADD(Calendar[Date], -1, YEAR))
    CALCULATE([Sales], ALL(Calendar), Calendar[Date] IN VALUES(Calendar[Date_PY]))

A metric view measure cannot hold the shift as SQL, but a metric view has it as a window
(``offset`` over the period). So the generator translates the calculation without the shift,
like any other measure, and adds the window afterwards (``prior_year.py``).

``unshifted`` returns the DAX without the shift and the calendar column the shift is on. It
returns None when the measure is not a prior-year measure as a whole: it has no shift, it
shifts on two calendars, or part of its result is not shifted
(``[Sales] - CALCULATE([Sales], SAMEPERIODLASTYEAR(...))``). Variables the result does not
use are left out of the returned DAX.

Pure functions: no I/O.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .period_folding import detect_prior_period_column

_MAY_SHIFT = re.compile(
    r"SAMEPERIODLASTYEAR|DATEADD|PARALLELPERIOD|\bIN\s+VALUES\s*\(", re.IGNORECASE
)
_COLUMN = r"(?:'([^']+)'|([A-Za-z_]\w*))\s*\[([^\]]+)\]"
_SAME_PERIOD = re.compile(
    rf"\s*SAMEPERIODLASTYEAR\s*\(\s*{_COLUMN}\s*\)\s*", re.IGNORECASE
)
_DATE_SHIFT = re.compile(
    rf"\s*(DATEADD|PARALLELPERIOD)\s*\(\s*{_COLUMN}\s*,\s*-\s*(\d+)\s*,\s*(\w+)\s*\)\s*",
    re.IGNORECASE,
)
_ONE_YEAR = {("1", "YEAR"), ("12", "MONTH"), ("4", "QUARTER")}
_ALL_OF = re.compile(
    r"\s*(?:ALL|REMOVEFILTERS)\s*\(\s*(?:'([^']+)'|([A-Za-z_]\w*))\s*\)\s*",
    re.IGNORECASE,
)
_IN_VALUES = re.compile(
    rf"\s*{_COLUMN}\s+IN\s+VALUES\s*\(\s*{_COLUMN}\s*\)\s*", re.IGNORECASE
)
_CALCULATE = re.compile(r"\bCALCULATE\s*\(", re.IGNORECASE)
_KEYWORD = re.compile(r"\b(VAR\s+(\w+)\s*=|RETURN\b)", re.IGNORECASE)
_SHIFTED = "\x00"  # stands for a shifted CALCULATE while the rest is checked
_WHOLE_CALL = re.compile(r"\s*[A-Za-z_]\w*\s*\(")


@dataclass(frozen=True)
class PriorYear:
    """A measure without its year shift, and the calendar column the shift was on."""

    dax: str
    calendar: str
    column: str


def _skip(text: str, i: int) -> int:
    """The index after the literal or bracketed name that starts at ``i`` (a string, a
    quoted table, a ``[name]``), or ``i`` when none starts there."""
    close = {'"': '"', "'": "'", "[": "]"}.get(text[i])
    if close is None:
        return i
    j = i + 1
    while j < len(text):
        if text[j] == close:
            if close == '"' and text[j + 1 : j + 2] == '"':
                j += 2  # "" is a quote inside a string
                continue
            return j + 1
        j += 1
    return len(text)


def _without_comments(dax: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(dax):
        after = _skip(dax, i)
        if after > i:
            out.append(dax[i:after])
            i = after
        elif dax.startswith(("//", "--"), i):
            end = dax.find("\n", i)
            i = len(dax) if end < 0 else end
        elif dax.startswith("/*", i):
            end = dax.find("*/", i)
            i = len(dax) if end < 0 else end + 2
            out.append(" ")
        else:
            out.append(dax[i])
            i += 1
    return "".join(out)


def _closing(text: str, open_at: int) -> int:
    """The index of the parenthesis that closes the one at ``open_at``, -1 if none."""
    depth = 0
    i = open_at
    while i < len(text):
        after = _skip(text, i)
        if after > i:
            i = after
            continue
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _arguments(text: str) -> list[str]:
    """``text`` split on the commas outside parentheses, braces, strings and names."""
    parts: list[str] = []
    depth = 0
    start = i = 0
    while i < len(text):
        after = _skip(text, i)
        if after > i:
            i = after
            continue
        if text[i] in "({":
            depth += 1
        elif text[i] in ")}":
            depth -= 1
        elif text[i] == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
        i += 1
    parts.append(text[start:])
    return parts


def _names_only(text: str) -> str:
    """``text`` with strings and bracketed or quoted names blanked (same length): what is
    left are functions, variables and operators."""
    out: list[str] = []
    i = 0
    while i < len(text):
        after = _skip(text, i)
        if after > i:
            out.append(" " * (after - i))
            i = after
        else:
            out.append(text[i])
            i += 1
    return "".join(out)


def _table(match: re.Match[str], group: int) -> str:
    return str(match.group(group) or match.group(group + 1))


def _year_shift(arguments: list[str]) -> tuple[set[int], tuple[str, str]] | None:
    """The CALCULATE filter arguments that shift by one year (their indexes) and the calendar
    column they shift, None when there are none."""
    for index, argument in enumerate(arguments):
        same = _SAME_PERIOD.fullmatch(argument)
        if same:
            return {index}, (_table(same, 1), same.group(3))
        shift = _DATE_SHIFT.fullmatch(argument)
        if shift and (shift.group(5), shift.group(6).upper()) in _ONE_YEAR:
            if shift.group(1).upper() == "PARALLELPERIOD" and shift.group(5) != "12":
                continue  # a whole year or quarter, not the same period a year back
            return {index}, (_table(shift, 2), shift.group(4))
        lookup = _IN_VALUES.fullmatch(argument)
        if not lookup or not detect_prior_period_column([lookup.group(6)]):
            continue
        calendar = _table(lookup, 1)
        if _table(lookup, 4).lower() != calendar.lower():
            continue
        # Calendar[Date] IN VALUES(Calendar[Date_PY]) only moves the dates when the
        # calendar's own filter is lifted in the same CALCULATE.
        for other, text in enumerate(arguments):
            lifted = _ALL_OF.fullmatch(text)
            if lifted and _table(lifted, 1).lower() == calendar.lower():
                return {index, other}, (calendar, lookup.group(3))
    return None


def _call_without(arguments: list[str], dropped: set[int]) -> str:
    kept = [a.strip() for i, a in enumerate(arguments) if i not in dropped]
    if len(kept) > 1:
        return f"CALCULATE({', '.join(kept)})"
    inner = kept[0]
    whole_call = _WHOLE_CALL.match(inner) and _closing(inner, inner.find("(")) == (
        len(inner) - 1
    )
    whole_name = inner.startswith("[") and _skip(inner, 0) == len(inner)
    return inner if whole_call or whole_name else f"({inner})"


@dataclass
class _Part:
    """One expression of the measure (a variable or the result): without its shifts, and
    with each shifted CALCULATE masked so the rest can be checked."""

    text: str
    masked: str


def _take_out(
    expr: str, calendars: set[tuple[str, str]]
) -> tuple[str, str] | tuple[None, None]:
    """``expr`` without the year shifts and ``expr`` with the shifted calls masked;
    ``(None, None)`` when a shifted call shifts again inside."""
    text: list[str] = []
    masked: list[str] = []
    position = 0
    for call in _CALCULATE.finditer(expr):
        if call.start() < position:
            continue  # inside a call already taken
        opening = call.end() - 1
        closing = _closing(expr, opening)
        if closing < 0:
            continue
        arguments = _arguments(expr[opening + 1 : closing])
        shift = _year_shift(arguments[1:])
        if shift is None:
            continue
        dropped, calendar = shift
        rewritten = _call_without(arguments, {i + 1 for i in dropped})
        if _MAY_SHIFT.search(rewritten) and _has_shift(rewritten):
            return None, None
        calendars.add((calendar[0].lower(), calendar[1].lower()))
        between = expr[position : call.start()]
        text += [between, rewritten]
        masked += [between, _SHIFTED]
        position = closing + 1
    text.append(expr[position:])
    masked.append(expr[position:])
    return "".join(text), "".join(masked)


def _has_shift(expr: str) -> bool:
    for call in _CALCULATE.finditer(expr):
        closing = _closing(expr, call.end() - 1)
        if closing > 0 and _year_shift(_arguments(expr[call.end() : closing])[1:]):
            return True
    return False


def _split(dax: str) -> tuple[list[tuple[str, str]], str] | None:
    """``[(variable, body)]`` and the result of ``VAR a = ... RETURN ...``; None when a
    VAR or RETURN sits inside a call (a nested block is not taken apart)."""
    bare = _names_only(dax)
    marks = list(_KEYWORD.finditer(bare))
    if not marks:
        return [], dax.strip()
    for mark in marks:
        before = bare[: mark.start()]
        if before.count("(") != before.count(")"):
            return None
    variables: list[tuple[str, str]] = []
    result = ""
    for index, mark in enumerate(marks):
        end = marks[index + 1].start() if index + 1 < len(marks) else len(dax)
        body = dax[mark.end() : end].strip()
        if mark.group(2):
            variables.append((mark.group(2), body))
        else:
            result = body
    return (variables, result) if result else None


def _variables_in(expr: str, names: list[str]) -> list[str]:
    words = {w.lower() for w in re.findall(r"[A-Za-z_]\w*", _names_only(expr))}
    return [n for n in names if n.lower() in words]


def may_shift(dax: object) -> bool:
    """Whether the DAX text has a time-shift function at all (a cheap pre-check)."""
    return bool(_MAY_SHIFT.search(str(dax or "")))


def shifted_calls(dax: object) -> tuple[str, list[tuple[int, int]]]:
    """The DAX without comments, and the ``(start, end)`` span (``end`` exclusive) of each
    CALCULATE call in it that shifts one year. A call inside a listed call is not listed.
    """
    text = _without_comments(str(dax or ""))
    spans: list[tuple[int, int]] = []
    position = 0
    for call in _CALCULATE.finditer(text):
        if call.start() < position:
            continue
        opening = call.end() - 1
        closing = _closing(text, opening)
        if closing < 0:
            continue
        if _year_shift(_arguments(text[opening + 1 : closing])[1:]) is None:
            continue
        spans.append((call.start(), closing + 1))
        position = closing + 1
    return text, spans


def unshifted(dax: object) -> PriorYear | None:
    """The measure without its one-year shift, None when it is not a prior-year measure as
    a whole (see the module text)."""
    text = _without_comments(str(dax or ""))
    if not _MAY_SHIFT.search(text):
        return None
    split = _split(text)
    if split is None:
        return None
    variables, result = split
    names = [name for name, _ in variables]
    bodies = {name.lower(): body for name, body in variables}
    used: list[str] = []
    pending = _variables_in(result, names)
    while pending:
        name = pending.pop()
        if name not in used:
            used.append(name)
            pending += _variables_in(bodies[name.lower()], names)
    calendars: set[tuple[str, str]] = set()
    parts: dict[str, _Part] = {}
    for name, body in [(n, bodies[n.lower()]) for n in names if n in used] + [
        ("", result)
    ]:
        taken, masked = _take_out(body, calendars)
        if taken is None or masked is None:
            return None
        parts[name.lower()] = _Part(taken, masked)
    if len(calendars) != 1:
        return None

    def shifted(name: str, seen: frozenset[str] = frozenset()) -> bool:
        """Whether everything this expression reads is read one year back."""
        part = parts[name]
        inner = [v.lower() for v in _variables_in(part.masked, names)]
        if "[" in part.masked or name in seen or any(v in seen for v in inner):
            return False
        if not inner and _SHIFTED not in part.masked:
            return False  # a constant: nothing is shifted here
        return all(shifted(v, seen | {name}) for v in inner)

    if not shifted(""):
        return None
    calendar, column = _original_case(text, next(iter(calendars)))
    ordered = [n for n in names if n in used]
    if len(ordered) == 1 and parts[""].text.strip().lower() == ordered[0].lower():
        rewritten = parts[ordered[0].lower()].text  # VAR a = <x> RETURN a
    elif ordered:
        lines = [f"VAR {n} = {parts[n.lower()].text}" for n in ordered]
        rewritten = "\n".join([*lines, f"RETURN {parts[''].text}"])
    else:
        rewritten = parts[""].text
    return PriorYear(rewritten.strip(), calendar, column)


def _original_case(dax: str, calendar: tuple[str, str]) -> tuple[str, str]:
    """The calendar table and column as the measure spells them."""
    for match in re.finditer(_COLUMN, dax):
        table = _table(match, 1)
        if (table.lower(), match.group(3).lower()) == calendar:
            return table, match.group(3)
    return calendar
