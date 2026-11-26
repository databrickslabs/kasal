"""Checks a scope table before it routes anything: vocabulary, links, and stale rows.

A signed-off scope table drives the semantic sync, so a wrong row hides a change:
a decision outside the vocabulary, a ``mirrored_as`` that names no deployed
measure, a dropped measure that is still mirrored, a kept measure or table with
no mirror, or a row for an object the model no longer has. ``review`` is allowed
in a draft only. Prints object names and counts only.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from src.services.powerbi.scope.decisions import DECISIONS

SIGNED_OFF = tuple(d for d in DECISIONS if d != "review")
_NEEDS_MIRROR = ("keep", "snapshot")
_LINKED_TYPES = ("measure", "table")


def _label(row: Mapping[str, str]) -> str:
    kind, table, name = row.get("object_type", ""), row.get("pbi_table", ""), row.get("name", "")
    if kind == "table":
        return f"table {table}"
    return f"{kind} {table}[{name}]" if table else f"{kind} {name}"


def _targets(row: Mapping[str, str]) -> list[str]:
    return [t.strip() for t in str(row.get("mirrored_as") or "").split(";") if t.strip()]


def check_scope(
    rows: Iterable[Mapping[str, str]],
    *,
    draft: bool = False,
    deployed_measures: set[str] | None = None,
    model_measures: set[str] | None = None,
    model_tables: set[str] | None = None,
) -> list[str]:
    """Problems of a scope table, one line each.

    ``deployed_measures``: ``<view>.<measure>`` of the deployed metric views (when
    known); ``model_measures`` / ``model_tables``: today's Power BI model (when known).
    """
    allowed = DECISIONS if draft else SIGNED_OFF
    problems = []
    for row in rows:
        label, decision, targets = _label(row), row.get("decision", ""), _targets(row)
        kind = row.get("object_type", "")
        if decision not in allowed:
            problems.append(f"{label}: decision {decision!r} not in {', '.join(allowed)}")
        if kind == "measure" and deployed_measures is not None:
            problems += [
                f"{label}: mirrored_as {t!r} is not a measure of the deployed metric views"
                for t in targets
                if "." in t and t not in deployed_measures and t.split(".")[0] in {d.split(".")[0] for d in deployed_measures}
            ]
        if kind == "measure" and decision == "drop" and targets:
            problems.append(f"{label}: dropped, but mirrored as {'; '.join(targets)}")
        if not draft and kind in _LINKED_TYPES and decision in _NEEDS_MIRROR and not targets:
            problems.append(f"{label}: {decision} without mirrored_as")
        if kind == "measure" and model_measures is not None and row.get("name") not in model_measures:
            problems.append(f"{label}: the model no longer has this measure")
        if kind == "table" and model_tables is not None and row.get("pbi_table") not in model_tables:
            problems.append(f"{label}: the model no longer has this table")
    return problems
