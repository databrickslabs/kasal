"""Reconciliation mapping files checked against the deployed metric views.

A metric-view change is a change in three places: the view YAML, the Genie space
and the reconciliation mappings. This checks the last one: every measure,
dimension and time column a mapping names must exist in the deployed view.
The mapping YAML is parsed in memory with Kasal's own loader; a path is never read.
"""

from __future__ import annotations

import json

import yaml

from src.services.tools.genie_space_sync_utils.metric_views import ViewCatalog
from src.services.tools.genie_space_sync_utils.verdicts import (
    FAIL,
    PASS,
    REVIEW,
    Verdict,
    short,
)
from src.services.tools.metric_view_utils.reconciliation.mapping import (
    UCMVMapping,
    load_mapping_dict,
)


def parse_mapping_text(text: str) -> UCMVMapping:
    """Parse one mapping from YAML text. Raises ValueError on any malformed input."""
    try:
        loaded: object = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError("mapping is not valid YAML") from exc
    if not isinstance(loaded, dict):
        raise ValueError("mapping must be a YAML mapping")
    try:
        return load_mapping_dict(loaded)
    except (KeyError, TypeError, yaml.YAMLError) as exc:
        raise ValueError(f"mapping does not match the schema: {short(exc)}") from exc


def parse_mapping_set(raw: object) -> dict[str, str]:
    """A JSON object (or dict) of mapping file name -> YAML text."""
    decoded: object = json.loads(raw) if isinstance(raw, str) and raw.strip() else raw
    if decoded in (None, "", {}):
        return {}
    if not isinstance(decoded, dict):
        raise ValueError(
            "reconciliation_mappings must be a JSON object of name -> YAML"
        )
    if not all(isinstance(text, str) for text in decoded.values()):
        raise ValueError("every reconciliation mapping must be YAML text")
    return {str(name): str(text) for name, text in decoded.items()}


def _problems(
    mapping: UCMVMapping, view_measures: frozenset[str], view_dimensions: frozenset[str]
) -> list[str]:
    binding = mapping.binding
    missing_measures = sorted(
        m.ucmv_measure
        for m in mapping.measures
        if m.ucmv_measure.lower() not in view_measures
    )
    columns = [dim.ucmv_column for dim in binding.dimensions.values()]
    time_column = binding.time_dimension.ucmv_column
    if time_column:
        columns.append(time_column)
    missing_columns = sorted(
        column for column in columns if column.lower() not in view_dimensions
    )
    problems: list[str] = []
    if missing_measures:
        problems.append(f"measures {missing_measures}")
    if missing_columns:
        problems.append(f"dimensions or time column {missing_columns}")
    return problems


def _check_one(name: str, text: str, catalog: ViewCatalog) -> tuple[Verdict, str]:
    """The verdict for one mapping, and the view it names ("" when it did not parse)."""
    try:
        mapping = parse_mapping_text(text)
    except ValueError as exc:
        return Verdict("mapping", name, FAIL, short(exc)), ""
    table = mapping.binding.ucmv_table
    view = catalog.view(table)
    if view is None:
        detail = f"{table} is not readable: {catalog.errors.get(table, 'unknown')}"
        return Verdict("mapping", name, FAIL, detail), table
    if not view.is_metric_view:
        return Verdict("mapping", name, FAIL, f"{table} is not a metric view"), table
    problems = _problems(mapping, view.measures, view.dimensions)
    if problems:
        return Verdict(
            "mapping", name, FAIL, "missing in " + "; ".join(problems)
        ), table
    return Verdict("mapping", name, PASS, table), table


def check_mappings(
    mappings: dict[str, str], catalog: ViewCatalog, space_tables: list[str]
) -> list[Verdict]:
    """Each mapping against its view, plus metric views of the space with no mapping."""
    verdicts: list[Verdict] = []
    mapped: set[str] = set()
    for name, text in sorted(mappings.items()):
        verdict, table = _check_one(name, text, catalog)
        verdicts.append(verdict)
        if table:
            mapped.add(table.lower())
    if not mappings:
        return verdicts
    for table in space_tables:
        view = catalog.view(table)
        if view is not None and view.is_metric_view and table.lower() not in mapped:
            verdicts.append(
                Verdict(
                    "mapping_coverage", table, REVIEW, "metric view has no mapping file"
                )
            )
    return verdicts
