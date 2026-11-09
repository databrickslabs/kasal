"""Typed schema + loader for a UCMV-to-PBI reconciliation MAPPING.

This is the INPUT to the reconciliation layer: for every UCMV measure it says
which PBI measure/column it maps to and how to reproduce its value on the PBI
side. The taxonomy mirrors the concepts proven in the customer's own
reconciliation (``pbi_kind`` ∈ direct / raw_column / switch /
dimension_conditional / composite / unresolved) and the shape Kasal's own
``pbi_ucmv_mapping.py`` emits — no dependency on either, concepts only.

Five resolvable ``pbi_kind`` values:

- ``direct``: a plain named PBI measure (``pbi_measure``).
- ``raw_column``: no named PBI measure exists — the value is a raw fact-table
  column aggregated directly (PBI's implicit "SummarizeBy" behaviour).
  ``pbi_column`` is a ``"Table[Column]"`` ref; ``pbi_aggregation`` (default
  ``Sum``) is which of the 6 SummarizeBy functions PBI uses.
- ``switch``: the value is selected inside a PBI ``SWITCH()`` by
  ``SELECTEDVALUE()`` over a selector table — reproduced with ``TREATAS`` on the
  selector column. Optional second, independent selector for a compound switch.
- ``dimension_conditional``: the PBI measure branches on whether a dimension is
  present in the query grouping (``ISFILTERED``/``HASONEVALUE``) — resolved by
  presence, not by a selector value.
- ``composite``: an arithmetic combination (``subtract``/``add``) of two
  independently-filtered raw sums of a column, each operand its own
  ``pbi_column`` + optional ``extra_filter``.

``unresolved`` measures carry a ``raw_hint`` and are excluded from querying.

Time dimension (``time_dimension``):

- ``grain``: ``month`` | ``quarter`` | ``year`` | ``snapshot`` | ``week``. Week
  keys ``YYYYWW``, ``WW.YYYY`` and ``YYYY-WW`` all compare as ``YYYYWW``; every
  week before the view's latest (open) week is compared.
- ``pbi_column`` (the PBI period column), or with month grain
  ``pbi_year_column`` + ``pbi_month_column`` (integer calendar columns; the
  period key becomes ``YYYY0MM``). Needed by time-intelligence measures that
  come back empty under a fiscper grouping.
- ``year_filter_column``: the UCMV column the reference years filter on.
- ``ucmv.mode`` / ``ucmv.name`` / ``ucmv.from``; ``ucmv.group_by`` (columns
  grouped but not selected, e.g. a window order that must be 1:1 with the
  period); ``ucmv.period_format: date_to_fiscper`` (a DATE or ``YYYY-MM``
  period column becomes ``YYYY0MM`` in SQL).

Measure options: ``value_format`` also takes ``raw_number_hundred`` (a PBI
number on the 0-100 scale for a UCMV fraction; divided by 100);
``pbi_chunk_by: period | member`` (split a heavy PBI query without trying it
whole first); ``extra_group_by`` entries are ``"Table[Column]"`` or
``{pbi: "Table[Column]", ucmv: column}`` for a calendar label the measure reads
(a period whose label is NULL in the metric view is not queried or compared).

Loader normalisation: ``raw_column``/composite operands may arrive either as a
combined ``pbi_column: "Table[Column]"`` (reference convention) or as separate
``pbi_table`` + bare ``pbi_column`` (Kasal's emitter convention). Both are
normalised to the combined form so the DAX builders never need to know which
was used.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Optional

import yaml

from ..raw_fields import (
    DimensionValue,
    RawMap,
    as_list,
    as_map,
    opt_bool,
    opt_float,
    opt_scalar_list,
    opt_str,
    opt_str_list,
    str_or,
)

_VALID_PBI_KINDS = {
    "direct",
    "raw_column",
    "switch",
    "dimension_conditional",
    "composite",
    "unresolved",
}
_VALID_VALUE_FORMATS = {
    "raw_number",
    "raw_number_hundred",
    "percent_string",
    "percent_string_unscaled",
}
_VALID_TOLERANCE_KINDS = {"absolute", "relative"}
_VALID_GRAINS = {"month", "quarter", "year", "snapshot", "week"}
_VALID_TIME_MODES = {"column", "reconstruct"}
# Per-measure split of a PBI query that times out (see pbi_retry.py).
_VALID_CHUNK_BY = {"period", "member"}
# UCMV period column -> "YYYYNNN" key: a DATE or 'YYYY-MM[-DD]' text column.
_VALID_UCMV_PERIOD_FORMATS = {"date_to_fiscper"}
_VALID_COMPOSITE_OPERATORS = {"subtract", "add"}
# PBI's 6 real SummarizeBy values ("None" excluded — not aggregatable).
_VALID_PBI_AGGREGATIONS = {"Sum", "Average", "Count", "Min", "Max", "DistinctCount"}


def _normalize_pbi_column(
    pbi_column: Optional[str], pbi_table: Optional[str]
) -> Optional[str]:
    """Return a combined ``"Table[Column]"`` ref from either convention.

    - ``pbi_column`` already combined (contains ``[``) -> passed through.
    - separate ``pbi_table`` + bare ``pbi_column`` -> combined.
    - bare ``pbi_column`` with no table -> returned as-is (best effort).
    """
    if not pbi_column:
        return pbi_column
    if "[" in pbi_column:
        return pbi_column
    if pbi_table:
        return f"{pbi_table}[{pbi_column}]"
    return pbi_column


def _normalize_operand(operand: Optional[RawMap]) -> Optional[RawMap]:
    """Normalise a composite operand's ``pbi_column`` to combined form."""
    if not operand:
        return operand
    out: dict[str, object] = dict(operand)
    out["pbi_column"] = _normalize_pbi_column(
        opt_str(operand.get("pbi_column")), opt_str(operand.get("pbi_table"))
    )
    return out


@dataclass
class TimeDimension:
    grain: str  # "month" | "quarter" | "year" | "snapshot" | "week"
    ucmv_mode: str  # "column" | "reconstruct"
    # PBI period column. Optional only when pbi_year_column + pbi_month_column
    # are given instead (month grain).
    pbi_column: Optional[str] = None
    ucmv_column: Optional[str] = None  # required when ucmv_mode == "column"
    ucmv_reconstruct_from: Optional[list[str]] = None  # required when "reconstruct"
    year_filter_column: str = "fiscal_year"
    # Table pbi_column lives on, if not the fact table (reachable via a model
    # relationship in SUMMARIZECOLUMNS).
    pbi_table: Optional[str] = None
    # Month grain: group PBI by the calendar's integer year and month columns
    # (period key YYYY0MM) instead of one period column. Needed for time-
    # intelligence measures that come back empty under a fiscper grouping.
    pbi_year_column: Optional[str] = None
    pbi_month_column: Optional[str] = None
    # UCMV columns grouped (not selected) next to the period, e.g. the order
    # dimension of a window measure. Each must be 1:1 with the period.
    ucmv_group_by: Optional[list[str]] = None
    # "date_to_fiscper": the UCMV period is a DATE or 'YYYY-MM[-DD]' text
    # column, turned into YYYY0MM in SQL. None: already "YYYYNNN" shaped.
    ucmv_period_format: Optional[str] = None

    @property
    def pbi_year_month(self) -> bool:
        """True when PBI groups by the calendar's year and month columns."""
        return bool(self.pbi_year_column)

    def __post_init__(self) -> None:
        if self.grain not in _VALID_GRAINS:
            raise ValueError(
                f"time_dimension.grain must be one of {_VALID_GRAINS}, got {self.grain!r}"
            )
        if self.ucmv_mode not in _VALID_TIME_MODES:
            raise ValueError(
                f"time_dimension.ucmv.mode must be one of {_VALID_TIME_MODES}, got {self.ucmv_mode!r}"
            )
        if self.grain == "snapshot" and self.ucmv_mode != "column":
            raise ValueError(
                "Snapshot reconciliation requires a date column, not period reconstruction."
            )
        if self.grain == "snapshot" and not self.pbi_column:
            raise ValueError(
                "Snapshot reconciliation requires time_dimension.pbi_column."
            )
        if self.ucmv_mode == "column" and not self.ucmv_column:
            raise ValueError("time_dimension.ucmv.mode='column' requires ucmv.name")
        if self.ucmv_mode == "reconstruct" and not self.ucmv_reconstruct_from:
            raise ValueError(
                "time_dimension.ucmv.mode='reconstruct' requires ucmv.from"
            )
        self._validate_pbi_time_columns()
        if self.ucmv_period_format is not None:
            if self.ucmv_period_format not in _VALID_UCMV_PERIOD_FORMATS:
                raise ValueError(
                    f"time_dimension.ucmv.period_format must be one of "
                    f"{_VALID_UCMV_PERIOD_FORMATS}, got {self.ucmv_period_format!r}"
                )
            if self.ucmv_mode != "column":
                raise ValueError(
                    "time_dimension.ucmv.period_format needs ucmv.mode='column'"
                )

    def _validate_pbi_time_columns(self) -> None:
        if bool(self.pbi_year_column) != bool(self.pbi_month_column):
            raise ValueError(
                "time_dimension.pbi_year_column and pbi_month_column go together"
            )
        if self.pbi_year_month and self.grain != "month":
            raise ValueError(
                "time_dimension.pbi_year_column / pbi_month_column need grain 'month'"
            )
        if not self.pbi_column and not self.pbi_year_month:
            raise ValueError(
                "time_dimension needs pbi_column, or pbi_year_column + pbi_month_column"
            )


@dataclass
class Dimension:
    name: str
    ucmv_column: str
    pbi_dim_table: str
    pbi_dim_column: str
    include_values: Optional[list[DimensionValue]] = None
    exclude_values: Optional[list[DimensionValue]] = None

    def select_values(self, values: Sequence[DimensionValue]) -> list[DimensionValue]:
        selected: set[DimensionValue] = set(values)
        if self.include_values is not None:
            selected &= set(self.include_values)
        return sorted(selected - set(self.exclude_values or []), key=str)


@dataclass
class Measure:
    ucmv_measure: str
    pbi_kind: str
    pbi_measure: Optional[str] = None

    # raw_column
    pbi_column: Optional[str] = None  # "Table[Column]"
    pbi_aggregation: str = "Sum"

    # switch
    switch_selector_table: Optional[str] = None
    switch_selector_column: Optional[str] = None
    switch_selector_value: Optional[str] = None
    switch_selector2_table: Optional[str] = None
    switch_selector2_column: Optional[str] = None
    switch_selector2_value: Optional[str] = None

    # dimension_conditional
    context_dimension: Optional[str] = None
    context_included: Optional[bool] = None

    # per-measure dimension override
    dimension: Optional[str] = None

    value_format: str = "raw_number"
    tolerance_kind: str = "absolute"
    tolerance_value: float = 0.001
    notes: str = ""
    raw_hint: Optional[str] = None

    # extra grouping columns ("Table[Column]") and fixed filters
    pbi_extra_group_by: Optional[list[str]] = None
    pbi_extra_filter: Optional[list[RawMap]] = None
    # extra_group_by entries given as {pbi, ucmv}: PBI label column -> the UCMV
    # column holding the same label per period. A period whose label is NULL in
    # the metric view is not queried in PBI and not compared.
    ucmv_context_columns: Optional[dict[str, str]] = None
    # Split of a PBI query that times out, set per measure: "period" | "member".
    # Unset: one query, and only a timeout triggers the retry ladder.
    pbi_chunk_by: Optional[str] = None

    # composite
    composite_operator: Optional[str] = None
    composite_a: Optional[RawMap] = None
    composite_b: Optional[RawMap] = None

    # visual-usage override
    pbi_visual_field: Optional[str] = None

    # snapshot-only null/zero equivalence
    null_equals_zero: bool = False

    @property
    def pbi_visibility_field_name(self) -> Optional[str]:
        return self.pbi_visual_field or self.pbi_field_name

    @property
    def pbi_field_name(self) -> Optional[str]:
        """The PBI field name this measure/column is known by, or None when
        there is no single coherent field (composite of two different columns,
        or unresolved)."""
        if self.pbi_measure:
            return self.pbi_measure
        if self.pbi_column:
            return self.pbi_column.partition("[")[2].rstrip("]") or None
        if self.pbi_kind == "composite" and self.composite_a and self.composite_b:
            col_a = opt_str(self.composite_a.get("pbi_column"))
            col_b = opt_str(self.composite_b.get("pbi_column"))
            if col_a and col_a == col_b:
                return col_a.partition("[")[2].rstrip("]") or None
        return None

    def __post_init__(self) -> None:
        # Normalise operand/column shapes up front so builders are convention-agnostic.
        self.pbi_column = _normalize_pbi_column(self.pbi_column, None)
        self.composite_a = _normalize_operand(self.composite_a)
        self.composite_b = _normalize_operand(self.composite_b)

        if self.pbi_kind not in _VALID_PBI_KINDS:
            raise ValueError(
                f"{self.ucmv_measure}: pbi_kind must be one of {_VALID_PBI_KINDS}, got {self.pbi_kind!r}"
            )
        if self.value_format not in _VALID_VALUE_FORMATS:
            raise ValueError(
                f"{self.ucmv_measure}: value_format must be one of {_VALID_VALUE_FORMATS}"
            )
        if self.tolerance_kind not in _VALID_TOLERANCE_KINDS:
            raise ValueError(
                f"{self.ucmv_measure}: tolerance_kind must be one of {_VALID_TOLERANCE_KINDS}"
            )
        if not isinstance(self.null_equals_zero, bool):
            raise ValueError(
                f"{self.ucmv_measure}: null_equals_zero must be true or false"
            )
        if self.pbi_chunk_by is not None and self.pbi_chunk_by not in _VALID_CHUNK_BY:
            raise ValueError(
                f"{self.ucmv_measure}: pbi_chunk_by must be one of {_VALID_CHUNK_BY}"
            )
        unmapped = set(self.ucmv_context_columns or {}) - set(
            self.pbi_extra_group_by or []
        )
        if unmapped:
            raise ValueError(
                f"{self.ucmv_measure}: extra_group_by mapping for {sorted(unmapped)} "
                "is not in extra_group_by"
            )

        validator = {
            "direct": self._validate_direct,
            "raw_column": self._validate_raw_column,
            "switch": self._validate_switch,
            "dimension_conditional": self._validate_dimension_conditional,
            "composite": self._validate_composite,
        }.get(self.pbi_kind)
        if validator:
            validator()

    def _validate_direct(self) -> None:
        if not self.pbi_measure:
            raise ValueError(
                f"{self.ucmv_measure}: pbi_kind='direct' requires pbi_measure"
            )

    def _validate_raw_column(self) -> None:
        if not self.pbi_column:
            raise ValueError(
                f"{self.ucmv_measure}: pbi_kind='raw_column' requires pbi_column"
            )
        if self.pbi_aggregation not in _VALID_PBI_AGGREGATIONS:
            raise ValueError(
                f"{self.ucmv_measure}: pbi_aggregation must be one of {_VALID_PBI_AGGREGATIONS}, "
                f"got {self.pbi_aggregation!r}"
            )

    def _validate_switch(self) -> None:
        missing = [
            f
            for f in (
                "pbi_measure",
                "switch_selector_table",
                "switch_selector_column",
                "switch_selector_value",
            )
            if not getattr(self, f)
        ]
        if missing:
            raise ValueError(
                f"{self.ucmv_measure}: pbi_kind='switch' missing {missing}"
            )

    def _validate_dimension_conditional(self) -> None:
        missing = [
            f for f in ("pbi_measure", "context_dimension") if not getattr(self, f)
        ]
        if missing:
            raise ValueError(
                f"{self.ucmv_measure}: pbi_kind='dimension_conditional' missing {missing}"
            )
        if self.context_included is None:
            raise ValueError(
                f"{self.ucmv_measure}: pbi_kind='dimension_conditional' requires context_included (true/false)"
            )
        if (
            self.context_included
            and self.dimension
            and self.dimension != self.context_dimension
        ):
            raise ValueError(
                f"{self.ucmv_measure}: dimension_conditional with context_included=true requires "
                f"dimension == context_dimension ({self.context_dimension!r})"
            )

    def _validate_composite(self) -> None:
        if self.composite_operator not in _VALID_COMPOSITE_OPERATORS:
            raise ValueError(
                f"{self.ucmv_measure}: pbi_kind='composite' requires composite_operator in "
                f"{_VALID_COMPOSITE_OPERATORS}"
            )
        for operand_name, operand in (
            ("composite_a", self.composite_a),
            ("composite_b", self.composite_b),
        ):
            if not operand or not operand.get("pbi_column"):
                raise ValueError(
                    f"{self.ucmv_measure}: pbi_kind='composite' requires {operand_name}.pbi_column"
                )


@dataclass
class Binding:
    ucmv_table: str
    pbi_semantic_model_id: str
    pbi_fact_table: str
    time_dimension: TimeDimension
    default_dimension: str
    dimensions: dict[str, Dimension] = field(default_factory=dict)
    pbi_workspace_id: Optional[str] = None
    pbi_report_id: Optional[str] = None
    report: Optional[str] = None

    def __post_init__(self) -> None:
        if self.default_dimension not in self.dimensions:
            raise ValueError(
                f"default_dimension={self.default_dimension!r} is not one of the declared "
                f"dimensions: {list(self.dimensions)}"
            )


@dataclass
class UCMVMapping:
    binding: Binding
    measures: list[Measure]

    def measure(self, ucmv_measure: str) -> Measure:
        for m in self.measures:
            if m.ucmv_measure == ucmv_measure:
                return m
        raise KeyError(
            f"No measure {ucmv_measure!r} in this mapping "
            f"(have: {[m.ucmv_measure for m in self.measures]})"
        )

    def resolved_measures(self) -> list[Measure]:
        """Measures ready to query — excludes anything pbi_kind='unresolved'."""
        return [m for m in self.measures if m.pbi_kind != "unresolved"]


def _build_time_dimension(td: RawMap) -> TimeDimension:
    ucmv_td = as_map(td.get("ucmv"))
    group_by_raw = ucmv_td.get("group_by")
    group_by = opt_str_list(group_by_raw)
    if group_by_raw is not None and group_by is None:
        raise ValueError("time_dimension.ucmv.group_by must be a list of UCMV columns")
    return TimeDimension(
        grain=str(td["grain"]),
        ucmv_mode=str_or(ucmv_td.get("mode"), "column"),
        pbi_column=opt_str(td.get("pbi_column")),
        ucmv_column=opt_str(ucmv_td.get("name")),
        ucmv_reconstruct_from=opt_str_list(ucmv_td.get("from")),
        year_filter_column=str_or(td.get("year_filter_column"), "fiscal_year"),
        pbi_table=opt_str(td.get("pbi_table")),
        pbi_year_column=opt_str(td.get("pbi_year_column")),
        pbi_month_column=opt_str(td.get("pbi_month_column")),
        ucmv_group_by=group_by,
        ucmv_period_format=opt_str(ucmv_td.get("period_format")),
    )


def _parse_extra_group_by(
    raw: object,
) -> tuple[Optional[list[str]], Optional[dict[str, str]]]:
    """``extra_group_by`` entries: "Table[Column]" (PBI grouping column, the
    period is derived) or ``{pbi: "Table[Column]", ucmv: column}`` (a calendar
    label: the UCMV column holds the same label per period)."""
    if not isinstance(raw, list):
        return None, None
    refs: list[str] = []
    context: dict[str, str] = {}
    for entry in raw:
        if isinstance(entry, Mapping):
            pbi_ref = opt_str(entry.get("pbi"))
            ucmv_col = opt_str(entry.get("ucmv"))
            if not pbi_ref or not ucmv_col:
                raise ValueError(
                    "extra_group_by entries are 'Table[Column]' or {pbi: ..., ucmv: ...}"
                )
            refs.append(pbi_ref)
            context[pbi_ref] = ucmv_col
        elif isinstance(entry, str) and entry:
            refs.append(entry)
        else:
            raise ValueError(
                f"extra_group_by entries are 'Table[Column]' or {{pbi, ucmv}}, got {entry!r}"
            )
    return refs or None, context or None


_DEFAULT_TOLERANCE: RawMap = {"kind": "absolute", "value": 0.001}


def _filter_list(value: object) -> Optional[list[RawMap]]:
    """Filter specs as mappings, or None when the section is absent."""
    if not isinstance(value, list):
        return None
    return [as_map(item) for item in value]


def _build_measure(m: RawMap) -> Measure:
    tol = as_map(m["tolerance"]) if "tolerance" in m else _DEFAULT_TOLERANCE
    tolerance_value = opt_float(tol.get("value"))
    null_equals_zero = m.get("null_equals_zero", False)
    if not isinstance(null_equals_zero, bool):
        raise ValueError(
            f"{m.get('ucmv_measure')}: null_equals_zero must be true or false"
        )
    extra_group_by, context_columns = _parse_extra_group_by(m.get("extra_group_by"))
    measure = Measure(
        ucmv_measure=str(m["ucmv_measure"]),
        pbi_kind=str(m["pbi_kind"]),
        pbi_measure=opt_str(m.get("pbi_measure")),
        pbi_column=opt_str(m.get("pbi_column")),
        pbi_aggregation=str_or(m.get("pbi_aggregation"), "Sum"),
        switch_selector_table=opt_str(m.get("switch_selector_table")),
        switch_selector_column=opt_str(m.get("switch_selector_column")),
        switch_selector_value=opt_str(m.get("switch_selector_value")),
        switch_selector2_table=opt_str(m.get("switch_selector2_table")),
        switch_selector2_column=opt_str(m.get("switch_selector2_column")),
        switch_selector2_value=opt_str(m.get("switch_selector2_value")),
        context_dimension=opt_str(m.get("context_dimension")),
        context_included=opt_bool(m.get("context_included")),
        dimension=opt_str(m.get("dimension")),
        value_format=str_or(m.get("value_format"), "raw_number"),
        tolerance_kind=str_or(tol.get("kind"), "absolute"),
        tolerance_value=0.001 if tolerance_value is None else tolerance_value,
        notes=str_or(m.get("notes"), ""),
        raw_hint=opt_str(m.get("raw_hint")),
        pbi_extra_group_by=extra_group_by,
        ucmv_context_columns=context_columns,
        pbi_chunk_by=opt_str(m.get("pbi_chunk_by")),
        pbi_extra_filter=_filter_list(m.get("extra_filter")),
        composite_operator=opt_str(m.get("composite_operator")),
        composite_a=_optional_map(m.get("composite_a")),
        composite_b=_optional_map(m.get("composite_b")),
        pbi_visual_field=opt_str(m.get("visual_field")),
        null_equals_zero=null_equals_zero,
    )
    # The Kasal emitter puts a bare column in pbi_column + a sibling pbi_table;
    # combine them here (Measure.__post_init__ can't see the sibling key).
    sibling_table = opt_str(m.get("pbi_table"))
    if sibling_table and measure.pbi_column and "[" not in measure.pbi_column:
        measure.pbi_column = _normalize_pbi_column(measure.pbi_column, sibling_table)
    return measure


def _optional_map(value: object) -> Optional[RawMap]:
    """A mapping section, or None when absent."""
    if isinstance(value, Mapping):
        return value
    return None


def load_mapping_dict(raw: RawMap) -> UCMVMapping:
    """Build a UCMVMapping from an already-parsed mapping dict.

    Accepts both the flat reference shape (``ucmv_table``/``pbi_fact_table`` at
    top level) and a nested ``binding:`` header (Kasal's emitter) — the binding
    keys are read from whichever is present.
    """
    binding_src = as_map(raw.get("binding")) or raw
    time_dimension = _build_time_dimension(as_map(raw["time_dimension"]))

    dimensions = {
        name: _build_dimension(name, as_map(d_raw))
        for name, d_raw in as_map(raw.get("dimensions")).items()
    }

    binding = Binding(
        ucmv_table=_first_text(binding_src.get("ucmv_table"), binding_src.get("ucmv")),
        pbi_semantic_model_id=_first_text(
            raw.get("pbi_semantic_model_id"),
            binding_src.get("pbi_semantic_model_id"),
        ),
        pbi_fact_table=str_or(binding_src.get("pbi_fact_table"), ""),
        time_dimension=time_dimension,
        default_dimension=_first_text(
            binding_src.get("default_dimension"), raw.get("default_dimension")
        ),
        dimensions=dimensions,
        pbi_workspace_id=_first_opt(raw, binding_src, "pbi_workspace_id"),
        pbi_report_id=_first_opt(raw, binding_src, "pbi_report_id"),
        report=_first_opt(raw, binding_src, "report"),
    )

    measures = [_build_measure(as_map(m)) for m in as_list(raw.get("measures"))]
    return UCMVMapping(binding=binding, measures=measures)


def _build_dimension(name: str, d: RawMap) -> Dimension:
    return Dimension(
        name=name,
        ucmv_column=str(d["ucmv_column"]),
        pbi_dim_table=str(d["pbi_dim_table"]),
        pbi_dim_column=str(d["pbi_dim_column"]),
        include_values=opt_scalar_list(d.get("include_values")),
        exclude_values=opt_scalar_list(d.get("exclude_values")),
    )


def _first_text(*lookups: object) -> str:
    """Text of the first truthy lookup value, else ``""``. Mirrors the
    ``a.get(k) or b.get(k)`` fallbacks of the flat-vs-nested binding header."""
    for value in lookups:
        if value:
            return str(value)
    return ""


def _first_opt(raw: RawMap, binding_src: RawMap, key: str) -> str | None:
    """``raw[key] or binding_src[key]`` as optional text."""
    return opt_str(raw.get(key) or binding_src.get(key))


def load_mapping(source: str) -> UCMVMapping:
    """Load a mapping from a file path OR a YAML/JSON text string."""
    text: str
    if "\n" not in source and source.strip().endswith((".yml", ".yaml", ".json")):
        with open(source, encoding="utf-8") as f:
            text = f.read()
    else:
        text = source
    raw = yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError("Mapping source did not parse to a mapping object.")
    return load_mapping_dict(as_map(raw))
