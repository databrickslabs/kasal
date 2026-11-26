"""DAX calculated columns of a source-layer view: translated to SQL, or kept as ``todo``.

A Power BI calculated column exists only in the model. ``calc_column_sql`` translates a
row-level formula to SQL over the table's own columns; this module decides, per table, which
translations become real columns of the source view:

* only with the switch ``calculated_columns_in_source_layer`` on (default on; see the
  ``calculated_column_views_enabled`` docstring);
* only when the view's real columns are known (``source_columns_json``): a column name that is
  not known is never assumed, so a formula is never written against a guessed name;
* only when the translation has no refusal. A refused formula stays a ``todo`` with the reason,
  as it did before this module existed.

The translated names are reported to the rest of the pipeline (``translated_calc_names``) so the
fact's known columns, the join checks and the source-column guard all see them as real columns.

Pure functions: no I/O.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

from .calc_column_sql import CalcColumnSql, translate_calculated_column
from .source_layer_emitter import CalcColumn

_PLAIN_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_OLD_TODO = "DAX calculated column — verify/translate before use"

Config = Mapping[str, object]


def _as_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def calculated_column_views_enabled(config: Config | None) -> bool:
    """The switch ``calculated_columns_in_source_layer`` (default true).

    On, a calculated column whose formula translates is a real column of its table's source
    view. Off, every calculated column stays a ``todo`` as before. A model with no calculated
    columns produces the same output either way, because there is nothing to translate.
    """
    raw = _as_mapping(config).get("calculated_columns_in_source_layer", True)
    if isinstance(raw, str):
        return raw.strip().lower() not in ("0", "false", "no", "off")
    return bool(raw)


def _raw_entries(config: Config | None, key: str) -> list[dict[str, object]]:
    """The table's calculated-column dicts that have a name (the pre-existing filter)."""
    by_table = _as_mapping(_as_mapping(config).get("calculated_columns"))
    return [
        cc
        for cc in _as_list(by_table.get(key))
        if isinstance(cc, dict) and cc.get("name")
    ]


def plan_calc_columns(
    key: str,
    config: Config | None,
    exposed: Sequence[str] | None,
) -> list[CalcColumn]:
    """The ``CalcColumn`` entries of one view: translated, or ``todo`` with the reason.

    ``exposed`` is the view's real column names (``None`` when they are not known). Every
    calculated column of the table appears, in the order of ``config['calculated_columns']``.
    """
    entries = _raw_entries(config, key)
    if not entries:
        return []
    if not calculated_column_views_enabled(config) or not exposed:
        # The pre-existing behaviour, unchanged: the source view keeps a todo.
        return [
            CalcColumn(
                name=str(cc.get("name", "")),
                sql_expr=str(cc.get("expression", "")),
                translation_status="todo",
                comment=_OLD_TODO,
            )
            for cc in entries
        ]
    return [_planned(key, cc, exposed, config) for cc in entries]


def _planned(
    key: str,
    entry: dict[str, object],
    exposed: Sequence[str],
    config: Config | None,
) -> CalcColumn:
    name = str(entry["name"])
    expression = str(entry.get("expression") or "")
    if not expression.strip():
        return _todo(name, expression, "the model has no formula for it")
    if not _PLAIN_NAME.fullmatch(name):
        return _todo(
            name,
            expression,
            "its name is not a plain identifier, so it cannot be a column without quoting",
        )
    if name.casefold() in {c.casefold() for c in exposed}:
        return _todo(
            name, expression, f"the source view already has a column named {name}"
        )
    result = _translate(key, entry, exposed, config)
    if not result.translated:
        return _todo(name, expression, result.refusal)
    return CalcColumn(name=name, sql_expr=result.sql, translation_status="translated")


def _todo(name: str, expression: str, reason: str) -> CalcColumn:
    return CalcColumn(
        name=name,
        sql_expr=expression,
        translation_status="todo",
        comment=f"DAX calculated column not translated: {reason}",
    )


def _translate(
    key: str,
    entry: dict[str, object],
    exposed: Sequence[str],
    config: Config | None,
) -> CalcColumnSql:
    other_calc = [
        str(cc["name"])
        for cc in _raw_entries(config, key)
        if str(cc["name"]) != str(entry["name"])
    ]
    return translate_calculated_column(
        str(entry["expression"]),
        table=key,
        columns=list(exposed),
        column_kinds=_kinds_for(key, exposed, config),
        calculated_columns=other_calc,
    )


def _kinds_for(
    key: str, exposed: Sequence[str], config: Config | None
) -> dict[str, str]:
    """The DAX kinds of the view's columns, from the scan, for columns the view exposes."""
    kinds = _as_mapping(
        _as_mapping(_as_mapping(config).get("calculated_column_kinds")).get(key)
    )
    exposed_lower = {c.casefold(): c for c in exposed}
    out: dict[str, str] = {}
    for column, kind in kinds.items():
        match = exposed_lower.get(str(column).casefold())
        if match is not None and isinstance(kind, str):
            out[match] = kind
    return out


def translated_calc_names(
    config: Config | None,
    exposed_by_key: Mapping[str, Sequence[str]] | None,
) -> dict[str, list[str]]:
    """``{table key: [names of the calculated columns translated into its view]}``."""
    out: dict[str, list[str]] = {}
    for key, exposed in (exposed_by_key or {}).items():
        if not exposed:
            continue
        planned = plan_calc_columns(key, config, exposed)
        names = [c.name for c in planned if c.translation_status == "translated"]
        if names:
            out[key] = names
    return out
