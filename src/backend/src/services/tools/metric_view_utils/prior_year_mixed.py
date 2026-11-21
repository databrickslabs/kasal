"""Prior-year calls inside a measure that also reads this year.

``prior_year_dax.unshifted`` takes a measure only when all of its result is one year back.
A measure that mixes the two is not taken, and it stays untranslatable::

    var a = CALCULATE([X], SAMEPERIODLASTYEAR(C[d])) var b = [X] return (b - a) * 100
    IF(ISBLANK([X]), BLANK(), CALCULATE([X], SAMEPERIODLASTYEAR(C[d])))
    CALCULATE(DIVIDE(...), SAMEPERIODLASTYEAR(C[d])) * [W]

Each shifted call is replaced by a reference to a prior-year measure, and the measure is then
plain DAX over this year's measures and those references:

- a measure of the model that is exactly that shifted call (the same body, the same calendar)
  is referenced as it is;
- otherwise a helper measure is added, named after the Power BI measure it shifts
  (``X PY``, or ``<measure> (prior year)`` when the body is not one measure). The helper
  holds the shifted call, so it is translated and windowed like any prior-year measure
  (``prior_year.py``). It is marked ``helper_of`` and reconciliation does not compare it.

Runs before ``prior_year.take_out_shifts``. A measure whose shift cannot be split this way
(a call that uses a variable of the measure, a call the unshift cannot take) is left as it is.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from . import prior_year
from .data_classes import MetricViewSpec
from .prior_year_dax import may_shift, shifted_calls, unshifted

HELPER_OF = prior_year.HELPER_OF
_MEASURE_CALL = re.compile(r"\bMEASURE\(\s*`?(\w+)`?\s*\)", re.IGNORECASE)
_VARIABLE = re.compile(r"\bVAR\s+(\w+)", re.IGNORECASE)
_NAMED = re.compile(r"\[[^\]]*\]|'[^']*'|\"[^\"]*\"")
_SINGLE_REF = re.compile(r"\[([^\]]+)\]")


def _key(name: object) -> str:
    return "".join(str(name or "").split()).lower()


def _name_of(entry: Mapping[str, object]) -> str:
    return str(entry.get("original_name") or entry.get("measure_name") or "")


def _fresh_name(wanted: str, taken: set[str]) -> str:
    """``wanted``, or ``wanted 2``, ``wanted 3``... that no measure of the model has."""
    name, number = wanted, 2
    while _key(name) in taken:
        name = f"{wanted} {number}"
        number += 1
    taken.add(_key(name))
    return name


def _uses_variable(call: str, variables: set[str]) -> bool:
    bare = _NAMED.sub(" ", call).lower()
    return any(re.search(rf"\b{re.escape(v)}\b", bare) for v in variables)


def _existing_prior_year(mapping: list[dict], call: str) -> str | None:
    """The name of a measure whose whole DAX is the same shifted call, if the model has one."""
    wanted = unshifted(call)
    if wanted is None:
        return None
    for other in mapping:
        if not may_shift(other.get("dax_expression")):
            continue
        if unshifted(other.get("dax_expression")) == wanted:
            return _name_of(other)
    return None


def _helper(entry: dict, call: str, taken: set[str]) -> dict:
    outer = _name_of(entry)
    body = unshifted(call)
    single = _SINGLE_REF.fullmatch(body.dax.strip()) if body else None
    wanted = f"{single.group(1)} PY" if single else f"{outer} (prior year)"
    name = _fresh_name(wanted, taken)
    home = entry.get("proposed_allocation")
    return {
        "measure_name": name,
        "original_name": name,
        "dax_expression": call,
        HELPER_OF: outer,
        "proposed_allocation": home,
        "all_allocations": [{"table": home, "role": "primary"}] if home else [],
    }


def _split_one(entry: dict, mapping: list[dict], taken: set[str]) -> list[dict] | None:
    """Rewrite ``entry``'s DAX to reference its prior-year helpers; the helpers to add, or None
    when the measure is left as it is."""
    dax = entry.get("dax_expression")
    if not dax or not may_shift(dax) or unshifted(dax) is not None:
        return None
    text, spans = shifted_calls(dax)
    if not spans:
        return None
    variables = {v.lower() for v in _VARIABLE.findall(text)}
    pieces: list[str] = []
    helpers: list[dict] = []
    last = 0
    for start, end in spans:
        call = text[start:end]
        if _uses_variable(call, variables) or unshifted(call) is None:
            return None
        ref = _existing_prior_year(mapping + helpers, call)
        if ref is None:
            helper = _helper(entry, call, taken)
            helpers.append(helper)
            ref = helper["original_name"]
        pieces += [text[last:start], f"[{ref}]"]
        last = end
    pieces.append(text[last:])
    rewritten = "".join(pieces)
    if may_shift(rewritten):
        return None
    entry["dax_expression"] = rewritten
    return helpers


def drop_unused_helpers(
    specs: Mapping[str, MetricViewSpec], mapping: list[dict]
) -> int:
    """Remove the helpers no translated measure of their view reads (their outer measure was
    not translated): an unread helper is a measure nobody asked for, and an untranslatable one
    would be a TODO with no cause. Returns how many were removed."""
    helpers = {_key(e.get("original_name")) for e in mapping if e.get(HELPER_OF)}
    if not helpers:
        return 0
    dropped = 0
    for spec in specs.values():
        read = {
            _key(name)
            for m in spec.measures
            for name in _MEASURE_CALL.findall(str(m.sql_expr or ""))
        }
        for group in (spec.measures, spec.untranslatable):
            kept = [
                m
                for m in group
                if not (
                    _key(m.original_name) in helpers
                    and _key(m.measure_name) not in read
                )
            ]
            dropped += len(group) - len(kept)
            group[:] = kept
    return dropped


def split_mixed_shifts(mapping: list[dict], config: Mapping[str, object]) -> int:
    """Split every mixed prior-year measure in ``mapping`` (helpers are appended to it).
    Returns how many measures were split."""
    if not prior_year.enabled(config):
        return 0
    taken = {
        _key(entry.get(field))
        for entry in mapping
        for field in ("original_name", "measure_name")
        if entry.get(field)
    }
    split = 0
    for entry in list(mapping):
        helpers = _split_one(entry, mapping, taken)
        if helpers is None:
            continue
        mapping.extend(helpers)
        split += 1
    return split
