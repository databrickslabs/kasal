"""Parsing of the tool's inputs. Each parser accepts the JSON/CSV text the task
form sends, or an already-parsed value, and raises ValueError with a message
that says what to fix."""

from __future__ import annotations

import csv
import io
import json
import re

from src.services.tools.metric_view_utils.source_layer_emitter import (
    RowFilter,
    row_filter_problems,
)
from src.services.tools.ucmv_source_layer_utils.names import is_identifier

DEFAULT_ROW_FILTER_KEY = "*"
_UUID = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def load_json(raw: object, label: str) -> object:
    """JSON text -> value; None / blank -> None; other values pass through."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{label} is not valid JSON: {exc.msg}") from exc
    return raw


def parse_json_list(raw: object, label: str) -> list[dict[str, object]]:
    value = load_json(raw, label)
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(i, dict) for i in value):
        raise ValueError(f"{label} must be a JSON list of objects")
    return [dict(item) for item in value]


def parse_names(raw: object) -> list[str]:
    """A list, or comma / newline separated text, of names (blanks dropped)."""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return [part.strip() for part in re.split(r"[,\n]", str(raw)) if part.strip()]


def parse_column_map(raw: object) -> dict[str, str]:
    """``{"PBI column": "uc_column"}``; keys keep their case."""
    value = load_json(raw, "column_map")
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("column_map must be a JSON object: {PBI name: UC name}")
    result: dict[str, str] = {}
    for pbi_name, uc_name in value.items():
        if not isinstance(uc_name, str) or not is_identifier(uc_name):
            raise ValueError(
                f"column_map: UC name for {pbi_name!r} is not an identifier"
            )
        result[str(pbi_name)] = uc_name
    return result


def parse_scope_rows(raw: object) -> list[dict[str, str]]:
    """The report scope table: JSON rows, or CSV text with a header line."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return []
    value: object = None
    if isinstance(raw, str) and raw.lstrip()[:1] in ("[", "{"):
        value = load_json(raw, "report_scope")
    elif not isinstance(raw, str):
        value = raw
    if isinstance(value, dict):
        value = value.get("rows", [value])
    if isinstance(value, list):
        return [
            {str(k): str(v) if v is not None else "" for k, v in row.items()}
            for row in value
            if isinstance(row, dict)
        ]
    reader = csv.DictReader(io.StringIO(str(raw)))
    return [{str(k): str(v or "") for k, v in row.items() if k} for row in reader]


def parse_row_filters(raw: object) -> dict[str, RowFilter]:
    """``{"<PBI table or view or '*'>": {"function", "columns", "constants"}}``.

    ``"*"`` is the default for every table without its own entry. Every problem
    in the config is reported together.
    """
    value = load_json(raw, "row_filters")
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("row_filters must be a JSON object keyed by table name")
    result: dict[str, RowFilter] = {}
    problems: list[str] = []
    for key, spec in value.items():
        if not isinstance(spec, dict):
            problems.append(f"{key}: entry must be an object")
            continue
        columns = spec.get("columns") or []
        constants = spec.get("constants") or []
        if not isinstance(columns, list) or not isinstance(constants, list):
            problems.append(f"{key}: columns and constants must be lists")
            continue
        row_filter = RowFilter(
            function=str(spec.get("function") or ""),
            columns=[str(c) for c in columns],
            constants=list(constants),
        )
        for problem in row_filter_problems(row_filter):
            problems.append(f"{key}: {problem}")
        result[str(key)] = row_filter
    if problems:
        raise ValueError("row_filters: " + "; ".join(problems))
    return result


def row_filter_for(table: str, filters: dict[str, RowFilter]) -> RowFilter | None:
    """The table's own filter, else the ``"*"`` default, else None (no override merge)."""
    return filters.get(table) or filters.get(DEFAULT_ROW_FILTER_KEY)


def check_uuid(value: str, label: str) -> str:
    if not _UUID.match(value or ""):
        raise ValueError(f"{label} must be a GUID")
    return value
