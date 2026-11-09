"""Row-level security checks across the views: the fact-needs-the-RLS-column rule,
and best-effort ``value_access`` suggestions for report-scope ``rls_filter`` rows.

Why the fact check exists: a row filter on a dimension view does not filter the
fact view that joins to it, so each fact (many side of a relationship) must carry
the same ``value_access`` field itself. Relationships come from ``relationships_json``
(Kasal shape ``from_table/from_column/to_table/to_column``, from_table = many side)
and from active ``relationship`` rows of the report scope.

Suggestions are never applied: they are review material for the row filters.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from src.services.tools.metric_view_utils.source_layer_emitter import RowFilter
from src.services.tools.ucmv_source_layer_utils.inputs import (
    load_json,
    parse_json_list,
    row_filter_for,
)
from src.services.tools.ucmv_source_layer_utils.row_filter_shorthand import (
    VALUE_ACCESS,
)


@dataclass(frozen=True)
class Relationship:
    """``from_table[from_column]`` (many side) -> ``to_table[to_column]`` (one side)."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str


_SCOPE_RELATIONSHIP = re.compile(r"^(.+?)\[([^\]]+)\]\s*->\s*(.+?)\[([^\]]+)\]$")
_REQUIRED = ("from_table", "from_column", "to_table", "to_column")


def parse_relationships(raw: object) -> list[Relationship]:
    """``relationships_json``: a list of ``{from_table, from_column, to_table, to_column}``."""
    relationships: list[Relationship] = []
    for index, item in enumerate(parse_json_list(raw, "relationships_json")):
        values = [str(item.get(key) or "").strip() for key in _REQUIRED]
        if not all(values):
            raise ValueError(
                f"relationships_json entry #{index + 1} needs {', '.join(_REQUIRED)}"
            )
        relationships.append(Relationship(*values))
    return relationships


def relationships_from_scope(
    scope_rows: Iterable[Mapping[str, str]],
) -> list[Relationship]:
    """Active ``relationship`` rows of the report scope (``ft[fc] -> tt[tc]``)."""
    found: list[Relationship] = []
    for row in scope_rows:
        if row.get("object_type") != "relationship" or row.get("decision") != "active":
            continue
        match = _SCOPE_RELATIONSHIP.match(row.get("name", "").strip())
        if match:
            found.append(
                Relationship(
                    match.group(1).strip(),
                    match.group(2),
                    match.group(3).strip(),
                    match.group(4),
                )
            )
    return found


def value_access_field(row_filter: RowFilter | None) -> str | None:
    """The rule-type field of a value_access filter, None for any other filter."""
    if row_filter is None or not row_filter.constants:
        return None
    if row_filter.function.strip().split(".")[-1].strip("`") != VALUE_ACCESS:
        return None
    return str(row_filter.constants[0])


def fact_rls_findings(
    relationships: Iterable[Relationship],
    filters: Mapping[str, RowFilter],
    tables: set[str],
) -> list[str]:
    """Warnings: a dimension filtered by a field whose related fact is not, too."""
    findings: list[str] = []
    seen: set[tuple[str, str]] = set()
    for rel in relationships:
        dim, fact = rel.to_table, rel.from_table
        if dim not in tables or fact not in tables:
            continue
        field = value_access_field(row_filter_for(dim, dict(filters)))
        if field is None:
            continue
        fact_field = value_access_field(row_filter_for(fact, dict(filters)))
        if fact_field is not None and fact_field.lower() == field.lower():
            continue
        if (fact, field.lower()) in seen:
            continue
        seen.add((fact, field.lower()))
        findings.append(
            f"{fact} is not filtered by {field}: a filter on {dim} does not reach it; "
            "add the column to the fact MV and a value_access filter"
        )
    return findings


def parse_rls_dax(raw: object) -> dict[str, str]:
    """``rls_dax_json``: ``{"<PBI table>": "<DAX of its RLS role expression>"}``."""
    value = load_json(raw, "rls_dax_json")
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("rls_dax_json must be a JSON object: {PBI table: DAX}")
    return {str(k): str(v) for k, v in value.items()}


_COLUMN = r"(?:'[^']*'|\w+)?\[([^\]]+)\]"
_QUOTED_LIST = r'\s*"[^"]*"(?:\s*,\s*"[^"]*")*\s*'
_EQUALS = re.compile(rf'^\s*{_COLUMN}\s*=\s*(?:"([^"]*)"|(-?\d+(?:\.\d+)?))\s*$')
_IN = re.compile(rf"^\s*{_COLUMN}\s+IN\s*\{{({_QUOTED_LIST})\}}\s*$", re.IGNORECASE)
_USER = re.compile(rf"^\s*{_COLUMN}\s*=\s*USERPRINCIPALNAME\(\)\s*$", re.IGNORECASE)
_QUOTED = re.compile(r'"([^"]*)"')


def rls_suggestion(dax: str, roles: str) -> dict[str, object] | None:
    """A ``value_access`` suggestion for a simple RLS expression, else None.

    Recognised: ``Table[col] = "x"`` (or a number), ``Table[col] IN {"x", "y"}`` and
    ``Table[col] = USERPRINCIPALNAME()``. Anything else (LOOKUPVALUE, measures,
    multi-column logic) is left to a person.
    """
    text = (dax or "").strip()
    if not text:
        return None
    user = _USER.match(text)
    if user:
        return _suggestion(
            user.group(1),
            "user",
            [],
            roles,
            (
                "PBI matches the signed-in user through USERPRINCIPALNAME(); Unity Catalog "
                "matches account groups, so map each group to its values in access_rows"
            ),
        )
    equals = _EQUALS.match(text)
    if equals:
        value = equals.group(2) if equals.group(2) is not None else equals.group(3)
        return _suggestion(equals.group(1), "equals", [value], roles, _VALUES_NOTE)
    member = _IN.match(text)
    if member:
        values = _QUOTED.findall(member.group(2))
        return _suggestion(member.group(1), "in", values, roles, _VALUES_NOTE)
    return None


_VALUES_NOTE = (
    "suggestion only: the column must exist in the view under this name (check column_map), "
    "and the groups that may see each value go into access_rows"
)


def _suggestion(
    column: str, match: str, values: list[str], roles: str, note: str
) -> dict[str, object]:
    return {
        "match": match,
        "row_filter": {VALUE_ACCESS: {"field": column, "column": column}},
        "values": values,
        "access_rows_template": [
            {
                "group_name": "<account group>",
                "field": column,
                "value": value,
                "reason": f"Power BI role {roles}".strip(),
            }
            for value in values
        ],
        "note": note,
    }
