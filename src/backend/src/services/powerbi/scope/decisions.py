"""Draft scope decisions per measure, calculation item, table and column.

Heuristics over names and DAX; every decision is a DRAFT a person signs off.
The vocabulary is fixed so downstream checks (and the semantic sync) can rely on
it:

    keep      own measure / table (materialized view) / column
    snapshot  table copied from the published model (no Databricks source)
    fold      part of another object's SQL or measure
    genie     answered by a Genie query pattern (time intelligence), no own object
    drop      not mirrored
    n/a       container rows (measure tables, calculation groups, report tables)
    review    open while drafting; the reason says what is proposed
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.services.powerbi.scope.closure import (
    CALC_COLUMN,
    CALC_GROUP,
    CALC_TABLE,
    POWER_QUERY,
    Closure,
    Seeds,
)
from src.services.powerbi.scope.tmdl_model import (
    DATA_SOURCES,
    OTHER_MODEL,
    REPORT_TABLE,
    Model,
    Table,
    source_class,
)

DECISIONS = ("keep", "snapshot", "fold", "genie", "drop", "n/a", "review")
MIRRORED = ("keep", "snapshot", "fold")

_SELECTED_MEASURE = re.compile(r"SELECTEDMEASURE\s*\(", re.I)
_DISPLAY_DAX = re.compile(
    r"FORMAT\s*\(|UNICHAR|\"#[0-9A-F]{6}\"|CONCATENATEX|SELECTEDVALUE\([^)]*\)\s*&|&\s*\"\s",
    re.I,
)
_DISPLAY_NAME = re.compile(
    r"title|colou?r|label|text|arrow|icon|tooltip|banner|header|subtitle", re.I
)
_FORMATTED_VALUE = re.compile(r"FORMAT\s*\(\s*(\w+|\[[^\]]+\])\s*[,(]", re.I)
_SELECTOR = re.compile(
    r"SWITCH\s*\(\s*(TRUE\s*\(\s*\)\s*,\s*)?(SELECTEDVALUE|VALUES|MAX|MIN|ISFILTERED|HASONEVALUE)\b",
    re.I,
)
_SELECTED_COLUMN = re.compile(
    r"SELECTEDVALUE\s*\(\s*('(?:[^']|'')+'|[A-Z_]\w*)\s*\[", re.I
)
_TI_DAX = re.compile(
    r"SAMEPERIODLASTYEAR|DATEADD|DATESYTD|DATESMTD|TOTALYTD|TOTALMTD|PARALLELPERIOD|PREVIOUSYEAR|PREVIOUSMONTH",
    re.I,
)
_TI_NAME = re.compile(
    r"(^|[_\s(])(PY|LY|YTD|MTD|YTG|PM|PW|LW|L4W|L13W|prev\w*)($|[_\s)])", re.I
)
_SEGMENT = re.compile(
    r"\bby\b[^\n]*?\bgroups?(?![a-z])|\btop ?n\b|\branges?(?![a-z])", re.I
)
_WINDOW = re.compile(
    r"\(PW\s*&\s*PW-\d+\)|\brolling\b|\bL\d+[WM]\b|\binv(?:entory)?\.? days(?![a-z])",
    re.I,
)
_SLICER = re.compile(r"^slicer", re.I)
_SELECTOR_TABLE = re.compile(r"selector|parameter|slicer", re.I)
_DESCRIPTOR = re.compile(
    r"(_txt|txtmd|txtsh|txtlg|_desc|desc$|name|text|label|region|country|brand|category|categ|"
    r"cluster|flavour|pack|business_unit|bu_|city|type$)",
    re.I,
)
_TRAILING_INDEX = re.compile(r"\.\d+$")


def strip_comments(expression: str) -> str:
    return re.sub(r"/\*.*?\*/|//[^\n]*|--[^\n]*", " ", expression or "", flags=re.S)


def _unquote(name: str) -> str:
    if len(name) > 1 and name.startswith("'") and name.endswith("'"):
        return name[1:-1].replace("''", "'")
    return name.strip()


def selector_tables(model: Model) -> set[str]:
    """Disconnected tables with columns and no data source: slicer / selector / what-if tables."""
    connected = model.connected()
    return {
        t.name
        for t in model.tables.values()
        if t.name not in connected
        and t.columns
        and source_class(t, model) not in DATA_SOURCES
    }


def _is_display_name(name: str) -> bool:
    return bool(_DISPLAY_NAME.search(name)) or name.lower().endswith("_display")


def is_sort_helper(column: str) -> bool:
    return "sequence_id" in column.lower() or bool(_TRAILING_INDEX.search(column))


def _selects_from_selector(expression: str, selectors: set[str]) -> bool:
    return any(_unquote(m) in selectors for m in _SELECTED_COLUMN.findall(expression))


def category(name: str, expression: str, selectors: set[str]) -> str:
    text = strip_comments(expression)
    if _is_display_name(name) or _DISPLAY_DAX.search(text):
        return "display"
    if _SELECTOR.search(text) or (
        "SWITCH" in text.upper() and _selects_from_selector(text, selectors)
    ):
        return "selector-switch"
    if _TI_NAME.search(name) or _TI_DAX.search(text):
        return "time-variant"
    return "base/kpi"


def _formatted_kpi(name: str, text: str, selectors: set[str]) -> bool:
    """A card KPI that exists only as FORMAT() text of a computed value."""
    if (
        _is_display_name(name)
        or _SEGMENT.search(name)
        or _selects_from_selector(text, selectors)
    ):
        return False
    return bool(_FORMATTED_VALUE.search(text))


def measure_decision(
    name: str, expression: str, reach: str, cat: str, selectors: set[str]
) -> tuple[str, str]:
    if reach == "unreached":
        return (
            "drop",
            "not reachable (no reachable page, not in the lineage of a reachable measure)",
        )
    text = strip_comments(expression)
    if cat == "display" and reach == "drawn" and _formatted_kpi(name, text, selectors):
        return (
            "keep",
            "drawn as FORMAT() text of a computed value: mirror the number, the format goes to the metric view",
        )
    if cat == "display":
        return "drop", "display helper (format / colour / label)"
    if cat == "selector-switch":
        return (
            "drop",
            "selector plumbing (SWITCH over a selector); its branches are mirrored on their own",
        )
    if _SLICER.search(name):
        return "drop", "slicer visibility helper"
    if cat == "time-variant" and _WINDOW.search(name):
        return (
            "review",
            "KPI defined over a fixed window (MTD / YTD / last weeks): own measure or Genie pattern",
        )
    if cat == "time-variant":
        return (
            "genie",
            "time intelligence (PY / YTD / MTD / prev. week ...): Genie pattern on the base measure (verify)",
        )
    if _SEGMENT.search(name):
        return (
            "review",
            "segmentation / Top-N variant of a base measure: dimension or Genie pattern",
        )
    reasons = {"drawn": "drawn", "filter": "visual / page filter"}
    return "keep", reasons.get(reach, "building block of a reachable measure")


def calc_item_decision(
    name: str, expression: str, reach: str, selectors: set[str]
) -> tuple[str, str]:
    """A calculation item is a measure definition too, unless it rewrites whichever measure is shown."""
    text = strip_comments(expression)
    if reach == "unreached":
        return measure_decision(name, expression, reach, "", selectors)
    if not _SELECTED_MEASURE.search(text):
        return measure_decision(
            name, expression, reach, category(name, expression, selectors), selectors
        )
    if _TI_NAME.search(name) or _TI_DAX.search(text):
        return (
            "genie",
            "calculation item: time intelligence on the selected measure (Genie pattern on the base measures)",
        )
    return (
        "review",
        "calculation item applied to SELECTEDMEASURE(): measure variant or Genie pattern",
    )


@dataclass
class TableContext:
    """What the table decisions need from the measure decisions and the closure."""

    readers: dict[str, set[str]]  # table -> reader labels
    kept: set[str]  # measures not dropped
    segment: set[str]  # measures in review as segmentation / Top-N variants
    live_columns: set[str]  # calculated-column labels something kept uses


def _live(label: str, connected: bool, ctx: TableContext) -> bool:
    if label == "on canvas" or label in ctx.kept:
        return True
    if label == "slicer/filter":
        return connected  # a slicer on a related table filters the facts; a disconnected one only selects
    if label.startswith(CALC_COLUMN):
        return label in ctx.live_columns
    return label.startswith((CALC_TABLE, CALC_GROUP, POWER_QUERY))


def _holds_only_measures(table: Table) -> bool:
    return not [
        c for c, dax in table.columns.items() if dax is None and not table.hidden.get(c)
    ]


def table_decision(
    table: Table, model: Model, closure: Closure, ctx: TableContext
) -> tuple[str, str]:
    source = source_class(table, model)
    homed = table.measures.keys() & closure.measures
    connected = table.name in model.connected()
    readers = ctx.readers.get(table.name, set())
    if table.kind == REPORT_TABLE:
        return "n/a", "report-level measures (defined in the report, not in the model)"
    if table.calc_items:
        return "n/a", "calculation group (its items are calculation_item rows)"
    if homed and _holds_only_measures(table):
        return "n/a", "measure table (holds measures only)"
    if not connected and (readers | homed) & ctx.segment:
        return (
            "fold",
            "band / what-if table of segmentation / Top-N measures: CASE in SQL or Genie pattern",
        )
    if homed and source not in DATA_SOURCES:
        return "n/a", "measure table (holds measures and selector keys)"
    if table.name not in closure.tables:
        return "drop", "not reached"
    if source == "calculated" and "NAMEOF" in table.source.upper():
        return (
            "drop",
            "field parameter (switches the fields of a visual; the fields themselves are followed)",
        )
    if source != "databricks" and _SELECTOR_TABLE.search(table.name):
        return (
            "drop",
            "selector / parameter table (a Genie question names the period / unit / KPI instead)",
        )
    live = {r for r in readers if _live(r, connected, ctx)}
    if not live:
        return (
            "drop",
            "only slicers / filters on a disconnected table or dropped helper measures use it",
        )
    if source == "databricks" and all(r.startswith(CALC_COLUMN) for r in live):
        names = ", ".join(sorted(live))[:120]
        return (
            "fold",
            f"read only by calculated column(s) of other tables ({names}): fold into their SQL",
        )
    if all(r.startswith(POWER_QUERY) for r in live):
        return _merged_decision(
            source, sorted(r[len(POWER_QUERY) :] for r in live), model
        )
    return decision_by_source(source)


def _merged_decision(source: str, mergers: list[str], model: Model) -> tuple[str, str]:
    """A table only other tables' Power Query reads (merge, append, reference)."""
    names = ", ".join(mergers)[:120]
    if source in ("databricks", OTHER_MODEL):
        return (
            "fold",
            f"read in Power Query by {names} (merge / reference / connection): part of their SQL",
        )
    selectors = selector_tables(model)
    rebuilt = [
        m for m in mergers if source_class(model.tables[m], model) == "databricks"
    ]
    if rebuilt or all(m in selectors or _SELECTOR_TABLE.search(m) for m in mergers):
        decision, reason = decision_by_source(source)
        return decision, f"{reason}; merged in Power Query into {names}"
    return (
        "fold",
        f"merged in Power Query into {names}: their snapshot already holds the merged data",
    )


def decision_by_source(source: str) -> tuple[str, str]:
    decisions = {
        "databricks": ("keep", "materialized view"),
        "external": (
            "snapshot",
            "external data (SharePoint / Excel / SQL, no Databricks source): snapshot table",
        ),
        "typed": (
            "snapshot",
            "typed-in data in the model: snapshot table (or CASE in SQL if tiny)",
        ),
        "derived": (
            "fold",
            "derived from another table in Power Query: fold into that table's SQL",
        ),
        "calculated": (
            "fold",
            "calculated table: derive in SQL (fold into an MV / a column)",
        ),
        OTHER_MODEL: (
            "keep",
            "table of another semantic model: read from that model's schema",
        ),
    }
    return decisions.get(source, ("drop", "no data source (display helper)"))


def column_decision(
    key: tuple[str, str], closure: Closure, seeds: Seeds, rel_keys: set[tuple[str, str]]
) -> tuple[str, str]:
    flags = []
    node = ("c", *key)
    if node in seeds.drawn or node in seeds.filters:
        flags.append("drawn/filter")
    if key in closure.columns and closure.used_by.get(node):
        flags.append("read by a reachable measure / calc column")
    if key in rel_keys:
        flags.append("relationship key")
    if flags:
        return "keep", "; ".join(flags)
    if is_sort_helper(key[1]):
        return (
            "drop",
            "sort helper / duplicate column (metric views expose no sort helpers or duplicates)",
        )
    if _DESCRIPTOR.search(key[1]):
        return (
            "keep",
            "descriptive attribute (name / text / category) kept for Genie; unused by the report",
        )
    return "drop", "technical / unused field"
