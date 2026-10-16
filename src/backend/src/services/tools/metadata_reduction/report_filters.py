"""Report filters and slicer selections as structured values, and their DAX.

The Fetcher used to cache a filter as display text (``"= 'Italy'"``,
``"IN (A, B)"``, ``"has filter (unknown type)"``) and every DAX writer pasted
that text into ``TREATAS({"= 'Italy'"}, ...)``, so answers came back blank.
A filter is now a spec::

    {"operator": "in", "values": ["Italy"]}
    {"operator": ">=", "values": [2020]}
    {"operator": "not_blank", "values": []}

and ``filter_to_dax`` is the one place a filter becomes DAX: strings quoted,
numbers bare, dates as ``DATE(...)``. ``parse_filter_value`` also reads the
legacy display strings and the UI's plain values, so caches written before this
change keep working without a refetch. A filter that has no value to apply
(a filter-pane card with nothing selected, an advanced AND/OR filter) is
``None`` and is skipped rather than turned into a filter on its description.
"""

from __future__ import annotations

import json
import logging
import re
from typing import TypedDict, Union

logger = logging.getLogger(__name__)

FilterValue = Union[str, int, float, bool]


class FilterSpec(TypedDict, total=False):
    operator: str
    values: list[FilterValue]
    value_type: str  # "date" when the values are ISO dates
    warning: str


_CMP_OPERATORS = ("=", "<>", ">", ">=", "<", "<=")
_LIST_OPERATORS = ("in", "not_in")
_TEXT_OPERATORS = ("starts_with", "not_starts_with", "contains")
_OPERATORS = (*_CMP_OPERATORS, *_LIST_OPERATORS, *_TEXT_OPERATORS, "not_blank")
# Power BI QueryComparisonKind (semantic query JSON).
_COMPARISON_KIND = {0: "=", 1: ">", 2: ">=", 3: "<", 4: "<="}
_UNUSABLE = re.compile(r"^has (complex )?filter\b", re.IGNORECASE)
_MISMATCH = re.compile(r"\s*⚠️\s*DATATYPE MISMATCH.*$")


def _pbi_literal(raw: object) -> tuple[FilterValue | None, str]:
    """``(value, value_type)`` from a semantic-query literal such as
    ``'Italy'``, ``2020L``, ``1.5D``, ``true`` or ``datetime'2020-01-01T00:00:00'``."""
    text = str(raw).strip()
    m = re.fullmatch(r"(?:datetime|date)'(\d{4}-\d{2}-\d{2})(?:T[\d:.]+)?'", text)
    if m:
        return m.group(1), "date"
    if len(text) >= 2 and text[0] == text[-1] == "'":
        return text[1:-1].replace("''", "'"), ""
    if text in ("true", "false"):
        return text == "true", ""
    if text == "null":
        return None, ""
    if re.fullmatch(r"-?\d+L?", text):
        return int(text.rstrip("L")), ""
    if re.fullmatch(r"-?\d+\.\d+[DM]?|-?\d+[DM]", text):
        return float(text.rstrip("DM")), ""
    return text.strip("\"'"), ""


def _literals(values: object) -> tuple[list[FilterValue | None], str]:
    out: list[FilterValue | None] = []
    vtype = ""
    for row in values if isinstance(values, list) else []:
        for item in row if isinstance(row, list) else [row]:
            lit = item.get("Literal", {}) if isinstance(item, dict) else {}
            value, t = _pbi_literal(lit.get("Value", ""))
            out.append(value)
            vtype = vtype or t
    return out, vtype


def _spec(operator: str, values: list[FilterValue | None], vtype: str) -> FilterSpec:
    spec: FilterSpec = {
        "operator": operator,
        "values": [v for v in values if v is not None],
    }
    if vtype:
        spec["value_type"] = vtype
    return spec


def spec_from_pbi_condition(condition: object) -> FilterSpec | None:
    """Structured spec from one Power BI filter ``Where[].Condition``."""
    if not isinstance(condition, dict):
        return None
    if "Not" in condition:
        inner = (condition["Not"] or {}).get("Expression", {})
        if "In" in inner:
            values, vtype = _literals(inner["In"].get("Values"))
            if values and all(v is None for v in values):
                return _spec("not_blank", [], "")
            return _spec("not_in", values, vtype) if values else None
        if "StartsWith" in inner:
            value, _ = _pbi_literal(
                inner["StartsWith"].get("Right", {}).get("Literal", {}).get("Value", "")
            )
            return _spec("not_starts_with", [value], "")
        if "Comparison" in inner and inner["Comparison"].get("ComparisonKind", 0) == 0:
            value, vtype = _pbi_literal(
                inner["Comparison"].get("Right", {}).get("Literal", {}).get("Value", "")
            )
            return _spec("<>", [value], vtype)
        return None
    if "In" in condition:
        values, vtype = _literals(condition["In"].get("Values"))
        return (
            _spec("in", values, vtype) if any(v is not None for v in values) else None
        )
    for key, operator in (("StartsWith", "starts_with"), ("Contains", "contains")):
        if key in condition:
            value, _ = _pbi_literal(
                condition[key].get("Right", {}).get("Literal", {}).get("Value", "")
            )
            return _spec(operator, [value], "")
    if "Comparison" in condition:
        comparison = condition["Comparison"]
        op = _COMPARISON_KIND.get(comparison.get("ComparisonKind", 0))
        value, vtype = _pbi_literal(
            comparison.get("Right", {}).get("Literal", {}).get("Value", "")
        )
        if op is None or value is None:
            return None
        return _spec("in" if op == "=" else op, [value], vtype)
    return None  # And / Or / Between: an advanced filter this does not model


def spec_from_filter_definition(
    filter_def: object,
) -> tuple[str | None, FilterSpec | None]:
    """``("Table[Column]", spec)`` from a report / page / visual filter entry."""
    if not isinstance(filter_def, dict):
        return None, None
    column = (filter_def.get("expression") or {}).get("Column") or {}
    table = ((column.get("Expression") or {}).get("SourceRef") or {}).get("Entity", "")
    prop = column.get("Property", "")
    if not table or not prop:
        return None, None
    where = (filter_def.get("filter") or {}).get("Where") or []
    if not where or not isinstance(where[0], dict):
        return f"{table}[{prop}]", None  # a card with nothing selected
    return f"{table}[{prop}]", spec_from_pbi_condition(where[0].get("Condition"))


def slicer_selection_spec(vis_data: object, parsed_config: object) -> FilterSpec | None:
    """The pre-set selection of a slicer visual, from its visual-level filters."""
    raw = None
    for source in (vis_data, parsed_config):
        if isinstance(source, dict) and source.get("filters"):
            raw = source["filters"]
            break
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    for entry in raw if isinstance(raw, list) else []:
        if not isinstance(entry, dict):
            continue
        where = (entry.get("filter") or {}).get("Where") or []
        if where and isinstance(where[0], dict):
            spec = spec_from_pbi_condition(where[0].get("Condition"))
            if spec:
                return spec
    return None


def _legacy_value(text: str, numeric: bool) -> FilterValue:
    """A value from a legacy display string. The old text stripped a literal's
    quotes but kept its type suffix (``'2020L'``), so the suffix decides."""
    inner = text.strip()
    if len(inner) >= 2 and inner[0] == inner[-1] and inner[0] in "'\"":
        inner = inner[1:-1]
    if re.fullmatch(r"-?\d+L|-?\d+(\.\d+)?[DM]|true|false", inner):
        value, _ = _pbi_literal(inner)
        if value is not None:
            return value
    if numeric and re.fullmatch(r"-?\d+(\.\d+)?", inner):
        return float(inner) if "." in inner else int(inner)
    return inner


def _from_spec_dict(value: dict) -> FilterSpec | None:
    operator, values = value.get("operator"), value.get("values")
    if operator not in _OPERATORS or not isinstance(values, list):
        return None
    spec: FilterSpec = {
        "operator": str(operator),
        "values": [v for v in values if isinstance(v, (str, int, float, bool))],
    }
    if isinstance(value.get("value_type"), str) and value["value_type"]:
        spec["value_type"] = value["value_type"]
    return spec


def parse_filter_value(value: object) -> FilterSpec | None:
    """Spec from any stored filter value: a spec, a UI list, a plain value, or
    a legacy display string. ``None`` when there is nothing to apply."""
    if isinstance(value, dict):
        return _from_spec_dict(value)
    if isinstance(value, list):
        values = [v for v in value if isinstance(v, (str, int, float, bool))]
        return {"operator": "in", "values": values} if values else None
    if isinstance(value, (bool, int, float)):
        return {"operator": "in", "values": [value]}
    if not isinstance(value, str):
        return None
    text = _MISMATCH.sub("", value).strip()
    if not text or _UNUSABLE.match(text):
        return None
    up = text.upper()
    if up == "NOT NULL":
        return {"operator": "not_blank", "values": []}
    for prefix, operator in (
        ("NOT STARTS WITH", "not_starts_with"),
        ("STARTS WITH", "starts_with"),
    ):
        if up.startswith(prefix):
            return {
                "operator": operator,
                "values": [text[len(prefix) :].strip().strip("'\"")],
            }
    m = re.fullmatch(r"(NOT\s+)?IN\s*\((.*)\)", text, re.IGNORECASE | re.DOTALL)
    if m:
        items = [_legacy_value(i, False) for i in m.group(2).split(",") if i.strip()]
        return (
            {"operator": "not_in" if m.group(1) else "in", "values": items}
            if items
            else None
        )
    m = re.fullmatch(r"(!=|<>|>=|<=|>|<|=)\s*(.+)", text, re.DOTALL)
    if m:
        op = {"!=": "<>", "=": "in"}.get(m.group(1), m.group(1))
        return {"operator": op, "values": [_legacy_value(m.group(2), op != "in")]}
    return {"operator": "in", "values": [text]}


def dax_column_ref(filter_key: str) -> str | None:
    """``Table[Column]`` -> DAX column reference (table quoted when needed)."""
    m = re.fullmatch(r"'?([^'\[]+)'?\[(.+)\]", filter_key.strip())
    if not m:
        return None
    table = m.group(1).strip()
    quoted = (
        table
        if re.fullmatch(r"[A-Za-z_]\w*", table)
        else "'" + table.replace("'", "''") + "'"
    )
    return f"{quoted}[{m.group(2)}]"


def _dax_literal(value: FilterValue, value_type: str) -> str:
    if isinstance(value, bool):
        return "TRUE()" if value else "FALSE()"
    if isinstance(value, (int, float)):
        return repr(value)
    if value_type == "date":
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", value)
        if m:
            return f"DATE({int(m.group(1))}, {int(m.group(2))}, {int(m.group(3))})"
    return '"' + str(value).replace('"', '""') + '"'


def filter_to_dax(filter_key: str, value: object) -> str | None:
    """The DAX table filter for one filter, usable in SUMMARIZECOLUMNS or
    CALCULATETABLE. ``None`` when the filter has no value to apply."""
    spec = parse_filter_value(value)
    col = dax_column_ref(filter_key)
    if not spec or not col:
        return None
    op = spec["operator"]
    vtype = spec.get("value_type", "")
    values = [_dax_literal(v, vtype) for v in spec.get("values", [])]
    if op == "not_blank":
        return f"FILTER(ALL({col}), NOT ISBLANK({col}))"
    if not values:
        return None
    if op in ("in", "="):
        return f"TREATAS({{{', '.join(values)}}}, {col})"
    if op == "not_in":
        return f"FILTER(ALL({col}), NOT {col} IN {{{', '.join(values)}}})"
    if op in _CMP_OPERATORS:
        return f"FILTER(ALL({col}), {col} {op} {values[0]})"
    if op in ("starts_with", "not_starts_with"):
        prefix = str(spec["values"][0])
        cmp = "=" if op == "starts_with" else "<>"
        return f"FILTER(ALL({col}), LEFT({col}, {len(prefix)}) {cmp} {values[0]})"
    if op == "contains":
        return f"FILTER(ALL({col}), CONTAINSSTRING({col}, {values[0]}))"
    return None


def describe_filter(value: object) -> str:
    """Human-readable text for logs, prompts and slicer listings."""
    spec = parse_filter_value(value)
    if not spec:
        return "no value to apply"
    op = spec["operator"]
    vtype = spec.get("value_type", "")
    values = [_dax_literal(v, vtype) for v in spec.get("values", [])]
    if op == "not_blank":
        return "NOT NULL"
    if op == "in":
        return f"= {values[0]}" if len(values) == 1 else f"IN ({', '.join(values)})"
    if op == "not_in":
        return f"NOT IN ({', '.join(values)})"
    words = {
        "starts_with": "STARTS WITH",
        "not_starts_with": "NOT STARTS WITH",
        "contains": "CONTAINS",
    }
    return f"{words.get(op, op)} {', '.join(values)}"


# Power BI DataType enum -> name, for the datatype check below.
PBI_DTYPE_NAMES = {
    "1": "whole_number",
    "2": "decimal",
    "3": "currency",
    "4": "date",
    "5": "boolean",
    "6": "string",
    "7": "binary",
    "8": "datetime",
    "9": "time",
    "10": "duration",
}


def check_spec_datatype(spec: FilterSpec, column_dtype: str) -> FilterSpec:
    """Coerce numeric-looking values on a numeric column; on a mismatch keep
    the values and record a ``warning`` (never written into a value)."""
    if column_dtype not in ("1", "2", "3"):
        return spec
    values: list[FilterValue] = []
    bad = False
    for v in spec.get("values", []):
        if isinstance(v, str) and re.fullmatch(r"-?\d+(\.\d+)?", v):
            values.append(float(v) if "." in v else int(v))
        else:
            bad = bad or isinstance(v, str)
            values.append(v)
    checked: FilterSpec = {**spec, "values": values}
    if bad:
        checked["warning"] = f"text value on a {PBI_DTYPE_NAMES[column_dtype]} column"
    return checked


def validate_filter_datatype(
    filter_name: str, value: object, column_dtype: str
) -> object:
    """Check a filter against its column type.

    A structured spec gets ``check_spec_datatype``. A legacy text description
    keeps its old behaviour: a mismatch is appended to the text, which
    ``parse_filter_value`` strips again before the value is used.
    """
    spec = _from_spec_dict(value) if isinstance(value, dict) else None
    if spec is not None:
        return check_spec_datatype(spec, column_dtype)
    text = str(value)
    has_quoted = "'" in text and text not in ("has filter", "has complex filter")
    mismatch = (
        (column_dtype in ("1", "2", "3") and has_quoted)
        or (
            column_dtype in ("4", "8", "9")
            and has_quoted
            and not re.search(r"\d{4}-\d{2}-\d{2}", text)
        )
        or (column_dtype == "6" and bool(re.match(r"^[><=!]+ \d+\.?\d*$", text)))
    )
    if not mismatch:
        return text
    dtype_name = PBI_DTYPE_NAMES.get(column_dtype, column_dtype)
    logger.warning(
        "[Filter Validation] DATATYPE MISMATCH: %s (column type: %s) has filter: %s",
        filter_name,
        dtype_name,
        text,
    )
    return f"{text} ⚠️ DATATYPE MISMATCH (column is {dtype_name})"
