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
UI's plain values and the legacy display strings. From legacy text it accepts
only ``=``, ``IN``, ``NOT IN``, ``NOT NULL`` and ``STARTS WITH``: the old
extractor shifted comparison operators by one (it wrote ``>=`` for ``<``), so
a cached comparison is dropped with a warning until the filters are refetched.
A filter that cannot be applied exactly (a card with nothing selected, an
advanced AND/OR filter, a relative date, a multi-column tuple) is ``None`` and
is skipped rather than turned into a filter on its description.
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
    # Set (alone) on a report filter that exists but cannot be applied exactly.
    # Kept in the cache so the answer can say which filters it ignored.
    unapplied: str


_CMP_OPERATORS = ("=", "<>", ">", ">=", "<", "<=")
_LIST_OPERATORS = ("in", "not_in")
_TEXT_OPERATORS = ("starts_with", "not_starts_with", "contains")
_OPERATORS = (
    *_CMP_OPERATORS,
    *_LIST_OPERATORS,
    *_TEXT_OPERATORS,
    "blank",
    "not_blank",
)
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


def _literal_node(node: object) -> tuple[FilterValue | None, str] | None:
    """A ``{"Literal": {...}}`` node's value; ``None`` for anything that is not
    a literal (a relative date, a measure, another column)."""
    if not isinstance(node, dict) or not isinstance(node.get("Literal"), dict):
        return None
    return _pbi_literal(node["Literal"].get("Value", ""))


def _in_values(in_expr: object) -> tuple[list[FilterValue | None], str] | None:
    """Values of a single-column ``In``; ``None`` for a multi-column tuple
    ``In`` or any item that is not a literal (never flattened onto one column)."""
    if not isinstance(in_expr, dict) or len(in_expr.get("Expressions") or [0]) > 1:
        return None
    out: list[FilterValue | None] = []
    vtype = ""
    for row in in_expr.get("Values") or []:
        lit = _literal_node(row[0]) if isinstance(row, list) and len(row) == 1 else None
        if lit is None:
            return None
        out.append(lit[0])
        vtype = vtype or lit[1]
    return (out, vtype) if out else None


def _text_spec(operator: str, node: object) -> FilterSpec | None:
    """StartsWith / Contains: only a non-empty text literal is applied."""
    lit = _literal_node(node.get("Right") if isinstance(node, dict) else None)
    if lit is None or not isinstance(lit[0], str) or not lit[0]:
        return None
    return _spec(operator, [lit[0]], "")


def _spec(operator: str, values: list[FilterValue | None], vtype: str) -> FilterSpec:
    spec: FilterSpec = {
        "operator": operator,
        "values": [v for v in values if v is not None],
    }
    if vtype:
        spec["value_type"] = vtype
    return spec


def spec_from_pbi_condition(condition: object) -> FilterSpec | None:
    """Structured spec from one Power BI filter ``Where[].Condition``.

    ``None`` (not applied, and logged by the caller) for anything that cannot
    be applied exactly: a non-literal right side, a tuple ``In``, a list that
    mixes blank with values, And / Or / Between.
    """
    if not isinstance(condition, dict):
        return None
    negated = isinstance(condition.get("Not"), dict)
    inner = condition["Not"].get("Expression", {}) if negated else condition
    if not isinstance(inner, dict):
        return None
    if "In" in inner:
        parsed = _in_values(inner["In"])
        if parsed is None:
            return None
        values, vtype = parsed
        if all(v is None for v in values):  # In [null] is "is blank"
            return _spec("not_blank" if negated else "blank", [], "")
        if any(v is None for v in values):
            return None  # values plus blank: not modelled
        return _spec("not_in" if negated else "in", values, vtype)
    if "StartsWith" in inner:
        return _text_spec(
            "not_starts_with" if negated else "starts_with", inner["StartsWith"]
        )
    if "Contains" in inner and not negated:
        return _text_spec("contains", inner["Contains"])
    comparison = inner.get("Comparison")
    if not isinstance(comparison, dict):
        return None
    op = _COMPARISON_KIND.get(comparison.get("ComparisonKind", 0))
    if negated:
        op = "<>" if op == "=" else None  # there is no not-equal kind
    lit = _literal_node(comparison.get("Right")) if op else None
    if lit is None or lit[0] is None:
        return None
    return _spec("in" if op == "=" else str(op), [lit[0]], lit[1])


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


def refusal_reason(condition: object) -> str:
    """Why ``spec_from_pbi_condition`` refused a condition, for the answer."""
    if not isinstance(condition, dict) or not condition:
        return "no value selected"
    if {"And", "Or"} & set(condition):
        return "advanced AND/OR filter"
    if "Between" in condition:
        return "range filter"
    negated = isinstance(condition.get("Not"), dict)
    inner = condition["Not"].get("Expression", {}) if negated else condition
    if isinstance(inner, dict) and "In" in inner:
        if _in_values(inner["In"]) is None:
            return "multi-column or computed values"
        return "values combined with blank"
    if isinstance(inner, dict) and "Comparison" in inner:
        return "negated comparison" if negated else "relative or computed value"
    if isinstance(inner, dict) and ({"StartsWith", "Contains"} & set(inner)):
        return "empty or computed text"
    return "filter type not supported"


def filter_from_definition(
    filter_def: object,
) -> tuple[str | None, FilterSpec | None]:
    """Like ``spec_from_filter_definition``, but a filter that exists and
    cannot be applied comes back as ``{"unapplied": reason}`` (not ``None``),
    so it stays visible downstream and the answer can report it."""
    name, spec = spec_from_filter_definition(filter_def)
    if name is None or spec is not None or not isinstance(filter_def, dict):
        return name, spec
    where = (filter_def.get("filter") or {}).get("Where") or []
    first = where[0] if where and isinstance(where[0], dict) else {}
    return name, {"unapplied": refusal_reason(first.get("Condition"))}


def slicer_selection_spec(vis_data: object, parsed_config: object) -> FilterSpec | None:
    """The pre-set selection of a slicer visual, from its visual-level filters.

    A selection that exists but cannot be applied exactly comes back as
    ``{"unapplied": reason}``; ``None`` means the slicer has no selection.
    """
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
    refused: FilterSpec | None = None
    for entry in raw if isinstance(raw, list) else []:
        if not isinstance(entry, dict):
            continue
        where = (entry.get("filter") or {}).get("Where") or []
        if where and isinstance(where[0], dict):
            condition = where[0].get("Condition")
            spec = spec_from_pbi_condition(condition)
            if spec:
                return spec
            refused = refused or {"unapplied": refusal_reason(condition)}
    return refused


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
    if re.match(r"(!=|<>|>=|<=|>|<)", text):
        _warn_once(
            text,
            "Not applying legacy report filter text %r: the old extractor shifted "
            "comparison operators by one (it wrote '>=' for '<'), so its direction "
            "is unknown. Refetch the report filters (Tool 79) to apply it.",
        )
        return None
    m = re.fullmatch(r"=\s*(.+)", text, re.DOTALL)
    if m:
        return {"operator": "in", "values": [_legacy_value(m.group(1), False)]}
    return {"operator": "in", "values": [text]}


def unapplied_reason(value: object) -> str:
    """Why a stored filter value has nothing to apply (for the answer)."""
    if isinstance(value, dict) and isinstance(value.get("unapplied"), str):
        return str(value["unapplied"])
    text = _MISMATCH.sub("", value).strip() if isinstance(value, str) else ""
    if re.match(r"(!=|<>|>=|<=|>|<)", text):
        return "cached comparison of unknown direction; refetch the report filters"
    if _UNUSABLE.match(text) or not text:
        return "no exact value; refetch the report filters"
    return "value could not be read"


def unapplied_filters(filters: object) -> list[tuple[str, str]]:
    """``(column, reason)`` for every filter in ``filters`` that is not applied."""
    if not isinstance(filters, dict):
        return []
    return [
        (str(key), unapplied_reason(value))
        for key, value in filters.items()
        if not str(key).startswith("_") and parse_filter_value(value) is None
    ]


_WARNED: set[str] = set()


def _warn_once(text: str, message: str) -> None:
    if text not in _WARNED:
        _WARNED.add(text)
        logger.warning(message, text)


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
    if op in ("blank", "not_blank"):
        neg = "NOT " if op == "not_blank" else ""
        return f"FILTER(ALL({col}), {neg}ISBLANK({col}))"
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
        return f"not applied: {unapplied_reason(value)}"
    op = spec["operator"]
    vtype = spec.get("value_type", "")
    values = [_dax_literal(v, vtype) for v in spec.get("values", [])]
    if op in ("blank", "not_blank"):
        return "NOT NULL" if op == "not_blank" else "IS BLANK"
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
    if isinstance(value, dict):
        spec = _from_spec_dict(value)
        return value if spec is None else check_spec_datatype(spec, column_dtype)
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
