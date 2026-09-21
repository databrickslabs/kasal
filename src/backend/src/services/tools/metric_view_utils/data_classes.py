"""Data classes used across the metric-view generation pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TranslationResult:
    """Result of translating a single DAX measure to SQL."""

    measure_name: str  # snake_case
    original_name: str  # PBI name
    sql_expr: str | None  # SQL or None if untranslatable
    is_translatable: bool
    skip_reason: str
    dax_expression: str
    confidence: str
    category: str  # "single_table" | "cross_table" | "unassigned" | "base"
    window_spec: dict | None = (
        None  # UC Metric View window: {order, range, semiadditive}
    )
    # Translation provenance/quality label from the LLM-first translator's
    # 7-category framework (translatable_direct | composed | filtered |
    # architecture_change | display_layer | unsupported | out_of_scope).
    # Distinct from `category`, which drives yaml_emitter output routing — never
    # overload category with this. Reporting/telemetry only.
    dax_class: str | None = None
    # How many OTHER measures reference this one (DAX dependency in-degree).
    # Surfaced on TODOs + the UCMV overview so reviewers prioritize high-impact
    # gaps. Populated from config['measure_usage']; measure→measure refs only
    # (not dashboard/visual usage). 0 = nothing references it.
    referenced_by: int = 0
    # The LLM's one-line justification for the translation (or the decline
    # reason). Surfaced as a provenance comment on the emitted measure so a
    # reviewer sees HOW/WHY each best-effort measure was produced. Reporting only.
    explanation: str | None = None
    # Business-usage signal (PROP-8): which report page(s)/visual(s) actually
    # draw or filter on this measure, e.g. [{"page": "OTC Scorecard",
    # "visual_type": "pivotTable", "role": "drawn"}]. Populated from
    # `visual_usage_annotator.annotate_visual_usage` (config['visual_usage_index'],
    # itself from `services.powerbi.visual_usage.derive_visual_usage_index`) —
    # an EMPTY list means "no visual reference found," not "not yet checked";
    # distinct from `referenced_by`, which is measure→measure DAX in-degree,
    # not dashboard usage.
    used_in_visuals: list = field(default_factory=list)
    # PBI-reconciliation provenance (priority 3): how this measure's SQL was
    # derived from the PBI side, ONLY set for switch/fx-resolved measures
    # (`switch_decomposition.py`/`custom_function_resolution.py` — see their
    # entry dicts' `pbi_kind`/`pbi_sources`/`pbi_operator` keys, threaded
    # through by `pipeline.py`'s `_build_switch_measure`). None for an
    # ordinarily-translated measure — `pbi_ucmv_mapping.py` treats that as
    # `pbi_kind="direct"` against `original_name` itself, the correct default
    # for the vast majority of measures, without needing it stamped here.
    # pbi_kind: "direct" | "composite" | "dimension_conditional" | None.
    pbi_kind: str | None = None
    # Plain measure-name strings for a switch resolution (e.g. ["ACT", "BP"]),
    # or {"kind": "raw_column", "table", "column", "value_column",
    # "filter_value"} dicts for an fx_* code-filtered EAV resolution — never
    # a mix within one list.
    pbi_sources: list = field(default_factory=list)
    pbi_operator: str | None = None  # "passthrough" | "add" | "subtract" | "divide"


@dataclass
class TableInfo:
    """Structure extracted from a single MQuery transpiled SQL."""

    table_name: str  # PBI table name
    source_table: str  # Databricks 3-level name from FROM clause
    aggregate_columns: list[dict]  # [{name, source_col}]
    group_by_columns: list[str]  # column names from GROUP BY
    calculated_columns: list[dict]  # [{name, expr}]
    is_fact: bool  # True if has aggregate columns
    full_sql: str  # original transpiled SQL
    raw_transpiled_sql: str = ""  # full transpiled SQL for scan enrichment
    dim_source_tables: dict[str, str] = field(default_factory=dict)
    static_filters: list[str] = field(default_factory=list)


@dataclass
class MetricViewSpec:
    """Complete UC Metric View specification for one fact table."""

    fact_table_key: str
    source_table: str
    view_name: str
    comment: str
    joins: list[dict]
    dimensions: list[dict]
    measures: list[TranslationResult]
    untranslatable: list[TranslationResult]
    base_measure_count: int = 0
    dax_measure_count: int = 0
    switch_measure_count: int = 0
    source_filter: str = ""  # MQuery WHERE → UC MV filter: key
    source_sql: str = ""  # Inline SQL for source: |-


@dataclass
class MStep:
    """A single M transform step from PBI scan data."""

    step_type: str  # "SelectRows" | "ReplaceValue" | "AddColumn" | etc.
    raw_expression: str  # original M code for this step


@dataclass
class ScanTableInfo:
    """Extracted info from a PBI scan table with NativeQuery."""

    pbi_table_name: str
    raw_m_expression: str
    native_sql: str  # SQL extracted from Value.NativeQuery(...)
    m_steps: list  # list[MStep]
    has_union: bool
    pbi_columns: list  # columns from scan (name, dataType, columnType, expression)
    storage_mode: str = ""  # 'Import', 'DirectQuery', 'Dual', etc.
