"""A Power BI semantic model read from its TMDL parts: tables, measures, columns, relationships, roles.

Only what the scope analysis and the semantic sync need: per table the partition
kind and source (M query, DAX of a calculated table, or another model's entity),
columns with the DAX of calculated ones, measures, calculation items; plus
relationships, user-defined functions (``functions.tmdl``), RLS table permissions
(``roles/*.tmdl``) and shared Power Query expressions (``expressions.tmdl``).
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from src.services.powerbi.scope.parts import PartTree, arr, text_at


def _named_object(keyword: str, depth: int) -> re.Pattern:
    """``<tabs>keyword Name = ...`` / ``<tabs>keyword 'Quoted Name' = ...``; no ``=``: an empty expression."""
    return re.compile(
        "^" + "\t" * depth + keyword + r" ('(?:[^']|'')+'|[^\s=']+)\s*(?:=(.*))?$"
    )


MEASURE = _named_object("measure", 1)
CALC_ITEM = _named_object("calculationItem", 2)
FUNCTION = _named_object("function", 0)
EXPRESSION = _named_object("expression", 0)
_TABLE_PERMISSION = _named_object("tablePermission", 1)
_ROLE = re.compile(r"^role ('(?:[^']|'')+'|[^\s=']+)", re.M)
_PARTITION_KIND = re.compile(r"^\tpartition .*? = (\w+)", re.M)
_COLUMN = re.compile(r"^\tcolumn ('?)(.+?)\1( =.*)?$", re.M)
_PARTITION = "\tpartition"
_TABLE_LINE = "table "
_AUTO_DATE_TABLES = ("LocalDateTable_", "DateTableTemplate_")

# M source of an import table
_DATABRICKS = re.compile(r"Databricks\.(Catalogs|Query)|Value\.NativeQuery", re.I)
_EXTERNAL = re.compile(
    r"SharePoint\.|Excel\.Workbook|Sql\.Databases?|Web\.Contents|Odbc\.|Csv\.Document|File\.Contents",
    re.I,
)
_TYPED = re.compile(r"Binary\.FromText|Table\.FromRows", re.I)
_M_SOURCE = re.compile(r"^\s*Source\s*=\s*(#\"[^\"]+\"|[A-Za-z_][\w.]*)", re.M)
_M_NAME = re.compile(r"#\"([^\"\n]+)\"|\b([A-Za-z_]\w*)\b")
_M_STEP = re.compile(r"^[ \t]*,?[ \t]*(?:#\"([^\"\n]+)\"|([A-Za-z_]\w*))[ \t]*=", re.M)
# one pass: a #"quoted name" is kept; text values, comments and [field] access are blanked
_M_TOKEN = re.compile(
    r'(#"(?:[^"]|"")*")|"(?:[^"]|"")*"|/\*.*?\*/|//[^\n]*|\[[^\]\n]*\]', re.S
)

OTHER_MODEL = "other model"
REPORT_TABLE = "report"  # Table.kind of an entity that only holds report-level measures
DATA_SOURCES = ("databricks", "external", OTHER_MODEL)

Relationship = tuple[
    str, str, str, str, bool
]  # from table, from column, to table, to column, active


@dataclass
class Table:
    name: str
    kind: str  # partition kind: m / calculated / entity / "-"
    source: str  # partition block (M query, DAX of a calculated table, or another model's entity)
    columns: dict[str, str | None] = field(default_factory=dict)  # column -> calc DAX
    hidden: dict[str, bool] = field(default_factory=dict)
    measures: dict[str, str] = field(default_factory=dict)  # measure -> DAX
    calc_items: dict[str, str] = field(default_factory=dict)  # calculation groups only
    format_strings: dict[str, str] = field(
        default_factory=dict
    )  # measure -> formatString


@dataclass
class Model:
    tables: dict[str, Table]
    relationships: list[Relationship]
    functions: dict[str, str] = field(default_factory=dict)
    roles: list[tuple[str, str, str]] = field(
        default_factory=list
    )  # (role, table, DAX)
    expressions: dict[str, str] = field(default_factory=dict)  # shared Power Query
    home: dict[str, str] = field(default_factory=dict)  # measure -> table holding it
    linked: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.index_measures()
        self.linked = self._data_connected()

    def index_measures(self) -> None:
        """Rebuild ``home``; after report-level measures were added."""
        self.home = {m: t.name for t in self.tables.values() for m in t.measures}

    def measure(self, name: str) -> str:
        return self.tables[self.home[name]].measures[name]

    def connected(self) -> set[str]:
        """Tables linked through relationships to a table with a data source. Selector tables related only
        to each other are not."""
        return self.linked

    def _data_connected(self) -> set[str]:
        neighbours: dict[str, set[str]] = defaultdict(set)
        for rel in self.relationships:
            neighbours[rel[0]].add(rel[2])
            neighbours[rel[2]].add(rel[0])
        related = set(neighbours)
        todo = [
            t.name
            for t in self.tables.values()
            if source_class(t, self) in DATA_SOURCES
        ]
        found: set[str] = set()
        while todo:
            name = todo.pop()
            if name not in found:
                found.add(name)
                todo += neighbours.get(name, set()) - found
        return found & related

    def relationship_keys(self) -> set[tuple[str, str]]:
        return {
            k
            for rel in self.relationships
            for k in ((rel[0], rel[1]), (rel[2], rel[3]))
        }


# --- TMDL text --------------------------------------------------------------------------------------------------


def unquote(name: str) -> str:
    """``'Region '`` -> ``Region `` (a quoted name keeps its inner spaces), ``Region`` -> ``Region``."""
    if len(name) > 1 and name.startswith("'") and name.endswith("'"):
        return name[1:-1].replace("''", "'")
    return name.strip()


def named_expressions(tmdl: str, pattern: re.Pattern, depth: int) -> dict[str, str]:
    """name -> body of every object ``pattern`` matches at ``depth`` tabs (one-line, indented or fenced)."""
    lines = tmdl.replace("\r", "").split("\n")
    continuation = "\t" * (depth + 2)
    found: dict[str, str] = {}
    i = 0
    while i < len(lines):
        match = pattern.match(lines[i])
        i += 1
        if match:
            first = (match.group(2) or "").strip()
            found[unquote(match.group(1))], i = _expression_body(
                lines, i, first, continuation
            )
    return found


def _expression_body(
    lines: list[str], i: int, first: str, continuation: str
) -> tuple[str, int]:
    if first.startswith("```"):
        body = []
        while i < len(lines) and not lines[i].strip().startswith("```"):
            body.append(lines[i])
            i += 1
        return "\n".join(body), i + 1
    body = [first] if first else []
    while i < len(lines) and (
        lines[i].startswith(continuation) or not lines[i].strip()
    ):
        body.append(lines[i])
        i += 1
    return "\n".join(body).strip(), i


def measures_of(tmdl: str) -> dict[str, str]:
    return named_expressions(tmdl, MEASURE, 1)


_FORMAT_STRING = re.compile(r"^\t\tformatString:\s*(.+)$")


def measure_format_strings(tmdl: str) -> dict[str, str]:
    """measure -> its ``formatString`` property (the display format Power BI shows)."""
    found: dict[str, str] = {}
    current: str | None = None
    for line in tmdl.replace("\r", "").split("\n"):
        match = MEASURE.match(line)
        if match:
            current = unquote(match.group(1))
        elif current and (fmt := _FORMAT_STRING.match(line)):
            found[current] = fmt.group(1).strip()
        elif line.startswith("\t") and not line.startswith("\t\t") and line.strip():
            current = None
    return found


def columns(tmdl: str) -> list[tuple[str, str, str | None, str | None, bool]]:
    """[(name, data type, source column, DAX of a calculated column or None, hidden)]."""
    found = []
    for match in _COLUMN.finditer(tmdl):
        expression = match.group(3)
        props, dax_lines = _column_body(
            tmdl[match.end() :].split("\n")[1:], bool(expression)
        )
        first_line = (expression or "")[2:].strip()
        dax = (first_line + " " + " ".join(line for line in dax_lines if line)).strip()
        found.append(
            (
                match.group(2),
                props.get("dataType", ""),
                props.get("sourceColumn"),
                dax.strip("`").strip() if expression else None,
                "isHidden" in props,
            )
        )
    return found


def _column_body(
    lines: list[str], calculated: bool
) -> tuple[dict[str, str], list[str]]:
    props: dict[str, str] = {}
    dax_lines: list[str] = []
    for line in lines:
        if line.startswith("\t\t\t") or (calculated and not line.strip() and not props):
            dax_lines.append(line.strip())
        elif line.startswith("\t\t"):
            key, _, value = line.strip().partition(": ")
            props[key] = value
        elif line.strip():
            break
    return props, dax_lines


def partition_source(tmdl: str) -> str:
    """The table's partition block (M query, DAX of a calculated table, or another model's entity)."""
    if _PARTITION not in tmdl:
        return ""
    block = tmdl.split(_PARTITION, 1)[1].split("\tannotation")[0]
    return re.sub(r"^\t\t\t\t", "", block, flags=re.M).strip()


def table_name(tmdl: str) -> str:
    line = next(
        (ln for ln in tmdl.splitlines() if ln.startswith(_TABLE_LINE)), _TABLE_LINE
    )
    return unquote(line[len(_TABLE_LINE) :].strip())


def parse_table(tmdl: str) -> Table:
    tmdl = tmdl.replace("\r", "")
    kinds = _PARTITION_KIND.findall(tmdl)
    table = Table(
        table_name(tmdl),
        ",".join(kinds) or "-",
        partition_source(tmdl),
        measures=measures_of(tmdl),
        calc_items=named_expressions(tmdl, CALC_ITEM, 2),
        format_strings=measure_format_strings(tmdl),
    )
    for name, _type, _source, dax, hidden in columns(tmdl):
        table.columns[name.replace("''", "'")] = dax
        table.hidden[name.replace("''", "'")] = hidden
    return table


def parse_roles(tmdl: str) -> list[tuple[str, str, str]]:
    """(role, table, row filter DAX) for every tablePermission of one role file."""
    match = _ROLE.search(tmdl)
    if not match:
        return []
    role = unquote(match.group(1))
    return [
        (role, table, dax)
        for table, dax in named_expressions(tmdl, _TABLE_PERMISSION, 1).items()
    ]


_REL_START = re.compile(r"^relationship\s+(.+?)\s*$")
_REL_PROP = re.compile(r"^\t(fromColumn|toColumn|isActive):\s*(.+?)\s*$")
_QUALIFIED = re.compile(r"^('(?:[^']|'')+'|[^.']+)\.('(?:[^']|'')+'|.+)$")


def _split_column_ref(ref: str) -> tuple[str, str]:
    match = _QUALIFIED.match(ref.strip())
    if not match:
        return "", ref.strip()
    return unquote(match.group(1)), unquote(match.group(2))


def parse_relationships(tmdl: str) -> list[Relationship]:
    """Relationships of ``relationships.tmdl`` (``fromColumn: Table.Column``, ``isActive: false``)."""
    found: list[Relationship] = []
    current: dict[str, str] | None = None

    def flush() -> None:
        if current and "fromColumn" in current and "toColumn" in current:
            ft, fc = _split_column_ref(current["fromColumn"])
            tt, tc = _split_column_ref(current["toColumn"])
            found.append((ft, fc, tt, tc, current.get("isActive", "true") != "false"))

    for line in tmdl.replace("\r", "").split("\n"):
        if _REL_START.match(line):
            flush()
            current = {}
        elif current is not None and (prop := _REL_PROP.match(line)):
            current[prop.group(1)] = prop.group(2)
    flush()
    return found


def relationships_from_rows(rows: Iterable[Mapping[str, object]]) -> list[Relationship]:
    """Kasal's REST relationship rows (``from_table`` ...) or DAX ``INFO.VIEW.RELATIONSHIPS`` rows."""
    found: list[Relationship] = []
    for row in rows:

        def get(*keys: str) -> object:
            return next((row[k] for k in keys if k in row), "")

        found.append(
            (
                str(get("from_table", "fromTable", "[FromTable]")),
                str(get("from_column", "fromColumn", "[FromColumn]")),
                str(get("to_table", "toTable", "[ToTable]")),
                str(get("to_column", "toColumn", "[ToColumn]")),
                bool(
                    get("is_active", "isActive", "[IsActive]")
                    not in (False, "false", 0)
                ),
            )
        )
    return found


def load_model(
    tree: PartTree, relationships: list[Relationship] | None = None
) -> Model:
    """The model of a TMDL part tree; ``relationships`` override ``relationships.tmdl`` when given."""
    tables: dict[str, Table] = {}
    for path in tree.glob("tables/*.tmdl"):
        table = parse_table(tree.text(path) or "")
        if table.name and not table.name.startswith(_AUTO_DATE_TABLES):
            tables[table.name] = table
    if relationships is None:
        relationships = parse_relationships(tree.text("relationships.tmdl") or "")
    functions = named_expressions(tree.text("functions.tmdl") or "", FUNCTION, 0)
    roles = [
        r
        for path in tree.glob("roles/*.tmdl")
        for r in parse_roles(tree.text(path) or "")
    ]
    expressions = named_expressions(tree.text("expressions.tmdl") or "", EXPRESSION, 0)
    return Model(
        tables, relationships, functions=functions, roles=roles, expressions=expressions
    )


def add_report_measures(model: Model, report: PartTree) -> None:
    """Measures defined in the report (``reportExtensions.json``) join the model under their entity."""
    for entity in arr(report.json("reportExtensions.json"), "entities"):
        name = text_at(entity, "name")
        if not name:
            continue
        table = model.tables.setdefault(name, Table(name, REPORT_TABLE, ""))
        for measure in arr(entity, "measures"):
            if measure_name := text_at(measure, "name"):
                table.measures[measure_name] = text_at(measure, "expression") or ""
    model.index_measures()


# --- Power Query sources ----------------------------------------------------------------------------------------


def resolved_source(source: str, expressions: Mapping[str, str]) -> str:
    """An M query plus every shared query it reads, directly or through another shared query."""
    parts, todo, seen = [source], [source], set()
    while todo:
        for quoted, bare in _M_NAME.findall(todo.pop()):
            name = quoted or bare
            if name in expressions and name not in seen:
                seen.add(name)
                parts.append(expressions[name])
                todo.append(expressions[name])
    return "\n".join(parts)


def power_query_tables(table: Table, model: Model) -> set[str]:
    """Other model tables the M query of ``table`` reads (merge, append, reference)."""
    if table.kind != "m":
        return set()
    source = _M_TOKEN.sub(
        lambda m: m.group(1) or " ", resolved_source(table.source, model.expressions)
    )
    steps = {quoted or bare for quoted, bare in _M_STEP.findall(source)}
    names = {quoted or bare for quoted, bare in _M_NAME.findall(source)}
    return {n for n in names - steps if n in model.tables and n != table.name}


def source_class(table: Table, model: Model) -> str:
    """databricks / external / typed / derived / calculated / other model / local / none."""
    if table.kind == "entity":
        return OTHER_MODEL
    if table.kind == "calculated":
        return "calculated"
    source = resolved_source(table.source, model.expressions)
    if _DATABRICKS.search(source):
        return "databricks"
    if _EXTERNAL.search(source):
        return "external"
    if _TYPED.search(source):
        return "typed"
    first = _M_SOURCE.search(table.source)
    if first and first.group(1).strip('#"') in model.tables:
        return "derived"
    return "local" if table.kind != "-" else "none"
