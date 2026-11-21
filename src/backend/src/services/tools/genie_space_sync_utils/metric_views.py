"""What is deployed in Unity Catalog for a table or metric view (pure parsing).

``DESCRIBE TABLE EXTENDED <name> AS JSON`` returns the columns and, for a metric
view, its YAML in ``view_text``. Names are lower-cased on the way in, since Spark
identifiers are case-insensitive. Failures are recorded per table, not raised, so
one unreadable table does not hide the findings for the others.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import yaml

from src.services.tools.genie_space_sync_utils.sql_checks import quote_name
from src.services.tools.genie_space_sync_utils.sql_results import (
    SqlFn,
    error_of,
    first_cell,
)

_VIEW_MARKERS = ("version:", "measures:")


@dataclass(frozen=True)
class ViewInfo:
    name: str
    columns: frozenset[str]
    dimensions: frozenset[str]
    measures: frozenset[str]
    percent_measures: frozenset[str]
    is_metric_view: bool


def describe_statement(table: str) -> str:
    return f"DESCRIBE TABLE EXTENDED {quote_name(table)} AS JSON"


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("expected a mapping")
    return {str(key): item for key, item in value.items()}


def _named_items(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _view_definition(view_text: str) -> dict[str, object]:
    start = view_text.index("version:")
    try:
        loaded: object = yaml.safe_load(view_text[start:])
    except yaml.YAMLError as exc:
        raise ValueError("view YAML does not parse") from exc
    return _as_dict(loaded)


def _lower_names(items: list[dict[str, object]]) -> set[str]:
    names = {str(item["name"]).lower() for item in items if item.get("name")}
    return names


def parse_describe(table: str, describe_text: str) -> ViewInfo:
    """A ``ViewInfo`` from the DESCRIBE JSON text. Raises ValueError when malformed."""
    document = _as_dict(json.loads(describe_text))
    columns = _named_items(document.get("columns"))
    column_names = _lower_names(columns)
    view_text = document.get("view_text")
    if not (
        isinstance(view_text, str)
        and all(marker in view_text for marker in _VIEW_MARKERS)
    ):
        measures = {
            str(col["name"]).lower()
            for col in columns
            if col.get("is_measure") and col.get("name")
        }
        return ViewInfo(
            name=table.lower(),
            columns=frozenset(column_names),
            dimensions=frozenset(column_names - measures),
            measures=frozenset(measures),
            percent_measures=frozenset(),
            is_metric_view=bool(measures),
        )
    definition = _view_definition(view_text)
    dimension_items = _named_items(definition.get("dimensions"))
    measure_items = _named_items(definition.get("measures"))
    percent = {
        str(item["name"]).lower()
        for item in measure_items
        if item.get("name")
        and isinstance(item.get("format"), dict)
        and _as_dict(item["format"]).get("type") == "percentage"
    }
    dimensions = _lower_names(dimension_items)
    measures = _lower_names(measure_items)
    return ViewInfo(
        name=table.lower(),
        columns=frozenset(column_names | dimensions | measures),
        dimensions=frozenset(dimensions),
        measures=frozenset(measures),
        percent_measures=frozenset(percent),
        is_metric_view=True,
    )


@dataclass
class ViewCatalog:
    """Deployed objects of a space, each DESCRIBE run once."""

    sql_fn: SqlFn
    _views: dict[str, ViewInfo | None] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)

    def view(self, table: str) -> ViewInfo | None:
        key = table.lower()
        if key not in self._views:
            self._views[key] = self._load(table)
        return self._views[key]

    def _load(self, table: str) -> ViewInfo | None:
        try:
            statement = describe_statement(table)
        except ValueError as exc:
            self.errors[table] = str(exc)
            return None
        result = self.sql_fn(statement)
        if not result.get("success"):
            self.errors[table] = error_of(result)
            return None
        text = first_cell(result)
        if text is None:
            self.errors[table] = "DESCRIBE returned no document"
            return None
        try:
            return parse_describe(table, text)
        except ValueError as exc:
            self.errors[table] = str(exc)
            return None

    def percent_measures(self, tables: list[str]) -> frozenset[str]:
        found: set[str] = set()
        for table in tables:
            info = self.view(table)
            if info is not None:
                found |= info.percent_measures
        return frozenset(found)

    def known_names(self, tables: list[str]) -> set[str]:
        """Every column, dimension, measure and bare table name of the given tables."""
        known: set[str] = set()
        for table in tables:
            known.add(table.split(".")[-1].lower())
            info = self.view(table)
            if info is not None:
                known |= info.columns | info.measures | info.dimensions
        return known
