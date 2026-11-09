"""Source-layer naming and metric-view binding (``read_from_source_layer``).

Two decisions live here, once, so the source-layer DDL, the metric-view YAML and
the binding report cannot disagree:

* **Naming** - ``source_layer_naming`` picks the leaf name of a PBI table's
  source-layer view. ``"src_prefix"`` gives ``src_<snake>`` (the default);
  ``"table_name"`` gives ``<snake>``, the PBI table name in snake_case.
* **Binding** - with ``read_from_source_layer`` on, a metric view's ``source:`` is
  its fact's source-layer view and each join's ``source:`` is the dimension's
  source-layer view. The view-level SQL and filter are cleared: the source-layer
  view already folds them in, so applying them again would count them twice. A
  fact without a usable view keeps the raw binding and the reason is recorded.

Everything here is pure. The pipeline calls these functions and owns the state.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace

from .data_classes import MetricViewSpec, TableInfo
from .utils import to_snake_case

SOURCE_LAYER_NAMING_MODES: tuple[str, ...] = ("src_prefix", "table_name")
DEFAULT_SOURCE_LAYER_NAMING = "src_prefix"

_SIMPLE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_COLUMN_REF = re.compile(r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b")
_TRUE_STRINGS = frozenset({"1", "true", "yes", "on"})


# ── Config readers ───────────────────────────────────────────────────────────


def read_from_source_layer(config: dict | None) -> bool:
    """Whether metric views should read the source-layer views (default off)."""
    raw = (config or {}).get("read_from_source_layer", False)
    if isinstance(raw, str):
        return raw.strip().lower() in _TRUE_STRINGS
    return bool(raw)


def source_layer_naming(config: dict | None) -> str:
    """The configured naming mode. Raises ``ValueError`` on an unknown value, so a
    typo is reported instead of silently producing differently named views."""
    raw = (config or {}).get("source_layer_naming") or DEFAULT_SOURCE_LAYER_NAMING
    if raw not in SOURCE_LAYER_NAMING_MODES:
        raise ValueError(
            "source_layer_naming must be one of "
            f"{', '.join(SOURCE_LAYER_NAMING_MODES)}; got {raw!r}"
        )
    return str(raw)


def calc_columns_by_key(config: dict | None) -> dict[str, frozenset[str]]:
    """DAX calculated-column names per PBI table. The source layer carries these
    only as TODO, so a metric view that reads one must be flagged."""
    raw = (config or {}).get("calculated_columns") or {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, frozenset[str]] = {}
    for key, columns in raw.items():
        if not isinstance(columns, list):
            continue
        out[str(key)] = frozenset(
            str(cc["name"]) for cc in columns if isinstance(cc, dict) and cc.get("name")
        )
    return out


# ── Naming (the one place the source-layer name is decided) ──────────────────


def source_layer_leaf_name(table_key: str, naming: str) -> str:
    """Leaf (unqualified) name of the source-layer view for a PBI table."""
    snake = to_snake_case(table_key)
    if naming == "table_name":
        return snake
    if naming == "src_prefix":
        return f"src_{snake}"
    raise ValueError(f"unknown source_layer_naming mode {naming!r}")


def quote_uc_name(name: str) -> str:
    """Back-quote each dotted part of a UC name (``a.b-c.d`` -> ``a``.``b-c``.``d``)."""
    parts = [p for p in str(name).split(".") if p]
    return ".".join(f"`{p.strip('`')}`" for p in parts)


def source_layer_ddl_name(catalog: str, schema: str, leaf: str) -> str:
    """Back-quoted three-part name the source-layer DDL creates."""
    return ".".join(quote_uc_name(part) for part in (catalog, schema, leaf))


def source_layer_yaml_name(catalog: str, schema: str, leaf: str) -> str:
    """Three-part name as written into metric-view YAML. Bare when every part is a
    plain identifier, back-quoted per part otherwise (e.g. a hyphenated catalog)."""
    parts = [p.strip("`") for p in (catalog, schema, leaf)]
    return ".".join(p if _SIMPLE_IDENTIFIER.match(p) else f"`{p}`" for p in parts)


def normalized_name(name: str) -> str:
    """Lower-cased dotted name with back-quotes and blank parts removed."""
    parts = [p.strip().strip("`").lower() for p in str(name or "").split(".")]
    return ".".join(p for p in parts if p)


# ── Raw-source index (what each PBI table reads today) ───────────────────────


def raw_source_by_key(
    all_specs: Mapping[str, MetricViewSpec],
    mquery_tables: Mapping[str, TableInfo],
) -> dict[str, str]:
    """The raw three-part source each PBI table reads, keyed by PBI table."""
    out: dict[str, str] = {}
    for key in [*all_specs, *(k for k in mquery_tables if k not in all_specs)]:
        spec = all_specs.get(key)
        info = mquery_tables.get(key)
        raw = (spec.source_table if spec is not None else "") or (
            info.source_table if info is not None else ""
        )
        if raw:
            out[key] = raw
    return out


def resolve_join_target(source: str, raw_by_key: Mapping[str, str]) -> str | None:
    """The PBI table a join's ``source`` reads, or ``None``.

    A join source that is inline SQL (a dedup subquery, a native query) is not a
    table reference and resolves to nothing. A plain name resolves when exactly
    one PBI table's raw source equals it, or ends with it (a shorter, dotted form).
    """
    text = (source or "").strip()
    if not text or "(" in text or any(ch.isspace() for ch in text):
        return None
    want = normalized_name(text)
    hits = [
        key
        for key, raw in raw_by_key.items()
        if (name := normalized_name(raw)) == want or name.endswith("." + want)
    ]
    return hits[0] if len(hits) == 1 else None


def usable_source_layer_keys(
    source_layer: Mapping[str, Mapping[str, object]],
) -> set[str]:
    """PBI tables whose source-layer view emitted DDL without error."""
    return {
        key
        for key, entry in source_layer.items()
        if entry.get("ddl") and not entry.get("error")
    }


# ── Binding report ───────────────────────────────────────────────────────────


@dataclass
class SourceLayerBinding:
    """What read mode did, for the output and the migration report."""

    naming: str
    disabled_reason: str = ""
    views: dict[str, str] = field(default_factory=dict)
    bound_facts: list[str] = field(default_factory=list)
    fallback_facts: dict[str, str] = field(default_factory=dict)
    bound_joins: dict[str, list[str]] = field(default_factory=dict)
    fallback_joins: dict[str, dict[str, str]] = field(default_factory=dict)
    folded_facts: list[str] = field(default_factory=list)
    untranslated_columns: list[str] = field(default_factory=list)

    @classmethod
    def disabled(cls, reason: str) -> SourceLayerBinding:
        """Read mode was requested but could not run; every view stays raw."""
        return cls(naming="", disabled_reason=reason)

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": "read_from_source_layer",
            "naming": self.naming,
            "disabled_reason": self.disabled_reason,
            "views": dict(self.views),
            "bound_facts": list(self.bound_facts),
            "fallback_facts": dict(self.fallback_facts),
            "bound_joins": {k: list(v) for k, v in self.bound_joins.items()},
            "fallback_joins": {k: dict(v) for k, v in self.fallback_joins.items()},
            "folded_facts": list(self.folded_facts),
            "untranslated_columns": list(self.untranslated_columns),
        }


def binding_report_lines(report: SourceLayerBinding) -> list[str]:
    """Markdown section appended to the migration report."""
    lines = ["", "## Source-Layer Binding (read_from_source_layer)", ""]
    if report.disabled_reason:
        lines += [f"Read mode was requested but not applied: {report.disabled_reason}"]
        return lines + [""]
    lines += [
        f"Metric views read the source-layer views (naming: `{report.naming}`). "
        "View-level filters and native SQL are folded into those views, so the "
        "metric views carry no `filter:` and no `source_sql`.",
        "",
        f"- Facts bound to a source-layer view: {len(report.bound_facts)}",
        f"- Facts reading the raw table (fallback): {len(report.fallback_facts)}",
    ]
    lines += [
        f"  - `{fact}`: {why}" for fact, why in sorted(report.fallback_facts.items())
    ]
    for fact, aliases in sorted(report.fallback_joins.items()):
        lines += [
            f"- Joins of `{fact}` reading the raw table: {', '.join(sorted(aliases))}"
        ]
    if report.untranslated_columns:
        lines += [
            "- Columns referenced but only TODO in the source layer (DAX calculated "
            "columns, not translated; the metric view will not resolve them until "
            "they are translated):"
        ]
        lines += [f"  - `{col}`" for col in sorted(report.untranslated_columns)]
    return lines + [""]


# ── Rebinding ────────────────────────────────────────────────────────────────


def _fallback_reason(key: str, source_layer: Mapping[str, Mapping[str, object]]) -> str:
    entry = source_layer.get(key)
    if entry is None:
        return "no source-layer view planned (no resolvable source table, or live connection)"
    if entry.get("error"):
        return f"source-layer view failed to emit: {entry.get('error')}"
    return "source-layer view produced no DDL"


def _join_fallback_reason(target: str | None) -> str:
    if target is None:
        return (
            "join source is not a plain PBI table reference (inline SQL or unmatched)"
        )
    return f"`{target}` has no usable source-layer view"


def _rebind_joins(
    spec: MetricViewSpec,
    views: Mapping[str, str],
    raw_by_key: Mapping[str, str],
    report: SourceLayerBinding,
) -> tuple[list[dict], dict[str, str]]:
    """Point each join at its dimension's view. Returns the joins and an
    alias -> PBI-table map for the joins that were rebound."""
    joins: list[dict] = []
    bound_aliases: dict[str, str] = {}
    bound: list[str] = []
    fallback: dict[str, str] = {}
    for join in spec.joins:
        alias = str(join.get("name", ""))
        source = str(join.get("source", ""))
        target = resolve_join_target(source, raw_by_key)
        if target is not None and target in views:
            joins.append({**join, "source": views[target]})
            bound_aliases[alias] = target
            bound.append(alias)
        else:
            joins.append(dict(join))
            fallback[alias] = _join_fallback_reason(target)
    if bound:
        report.bound_joins[spec.fact_table_key] = bound
    if fallback:
        report.fallback_joins[spec.fact_table_key] = fallback
    return joins, bound_aliases


def _flag_untranslated(
    spec: MetricViewSpec,
    alias_to_key: Mapping[str, str],
    views: Mapping[str, str],
    calc_columns: Mapping[str, frozenset[str]],
    report: SourceLayerBinding,
) -> None:
    """Record references to DAX calculated columns: the source layer keeps those as
    TODO, so the metric view would reference a column that does not exist yet."""
    texts = [str(d.get("expr", "")) for d in spec.dimensions]
    texts += [m.sql_expr or "" for m in spec.measures]
    for text in texts:
        for alias, column in _COLUMN_REF.findall(text):
            key = alias_to_key.get(alias)
            if key is None or column not in calc_columns.get(key, frozenset()):
                continue
            ref = f"{views[key]}.{column}"
            if ref not in report.untranslated_columns:
                report.untranslated_columns.append(ref)


def _bind_spec(
    key: str,
    spec: MetricViewSpec,
    *,
    source_layer: Mapping[str, Mapping[str, object]],
    views: Mapping[str, str],
    raw_by_key: Mapping[str, str],
    calc_columns: Mapping[str, frozenset[str]],
    report: SourceLayerBinding,
) -> MetricViewSpec:
    joins, alias_to_key = _rebind_joins(spec, views, raw_by_key, report)
    if key not in views:
        report.fallback_facts[key] = _fallback_reason(key, source_layer)
        bound = replace(spec, joins=joins)
    else:
        if spec.source_sql or spec.source_filter:
            report.folded_facts.append(key)
        report.bound_facts.append(key)
        alias_to_key["source"] = key
        # The view already folds the native SQL and the view-level filter in.
        bound = replace(
            spec,
            joins=joins,
            source_table=views[key],
            source_sql="",
            source_filter="",
        )
    _flag_untranslated(bound, alias_to_key, views, calc_columns, report)
    return bound


def bind_metric_views(
    all_specs: Mapping[str, MetricViewSpec],
    source_layer: Mapping[str, Mapping[str, object]],
    *,
    catalog: str,
    schema: str,
    naming: str,
    raw_by_key: Mapping[str, str],
    calc_columns: Mapping[str, frozenset[str]],
) -> tuple[dict[str, MetricViewSpec], SourceLayerBinding]:
    """Rebind every metric view onto the source-layer views.

    Returns ``(bound specs by table key, report)``. The input specs are not
    mutated. A fact without a usable view is returned unchanged (raw binding), and
    its joins are still rebound wherever their dimension has a usable view.
    """
    usable = usable_source_layer_keys(source_layer)
    views = {
        key: source_layer_yaml_name(
            catalog, schema, source_layer_leaf_name(key, naming)
        )
        for key in sorted(usable)
    }
    report = SourceLayerBinding(naming=naming, views=dict(views))
    bound: dict[str, MetricViewSpec] = {}
    for key, spec in all_specs.items():
        bound[key] = _bind_spec(
            key,
            spec,
            source_layer=source_layer,
            views=views,
            raw_by_key=raw_by_key,
            calc_columns=calc_columns,
            report=report,
        )
    return bound, report
