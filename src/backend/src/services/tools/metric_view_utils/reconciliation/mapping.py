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

Loader normalisation: ``raw_column``/composite operands may arrive either as a
combined ``pbi_column: "Table[Column]"`` (reference convention) or as separate
``pbi_table`` + bare ``pbi_column`` (Kasal's emitter convention). Both are
normalised to the combined form so the DAX builders never need to know which
was used.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import yaml

_VALID_PBI_KINDS = {
    "direct",
    "raw_column",
    "switch",
    "dimension_conditional",
    "composite",
    "unresolved",
}
_VALID_VALUE_FORMATS = {"raw_number", "percent_string", "percent_string_unscaled"}
_VALID_TOLERANCE_KINDS = {"absolute", "relative"}
_VALID_GRAINS = {"month", "quarter", "year", "snapshot"}
_VALID_TIME_MODES = {"column", "reconstruct"}
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


def _normalize_operand(operand: Optional[dict]) -> Optional[dict]:
    """Normalise a composite operand's ``pbi_column`` to combined form."""
    if not operand:
        return operand
    out = dict(operand)
    out["pbi_column"] = _normalize_pbi_column(
        operand.get("pbi_column"), operand.get("pbi_table")
    )
    return out


@dataclass
class TimeDimension:
    grain: str  # "month" | "quarter" | "year" | "snapshot"
    ucmv_mode: str  # "column" | "reconstruct"
    pbi_column: str
    ucmv_column: Optional[str] = None  # required when ucmv_mode == "column"
    ucmv_reconstruct_from: Optional[list] = None  # required when "reconstruct"
    year_filter_column: str = "fiscal_year"
    # Table pbi_column lives on, if not the fact table (reachable via a model
    # relationship in SUMMARIZECOLUMNS).
    pbi_table: Optional[str] = None

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
        if self.ucmv_mode == "column" and not self.ucmv_column:
            raise ValueError("time_dimension.ucmv.mode='column' requires ucmv.name")
        if self.ucmv_mode == "reconstruct" and not self.ucmv_reconstruct_from:
            raise ValueError(
                "time_dimension.ucmv.mode='reconstruct' requires ucmv.from"
            )


@dataclass
class Dimension:
    name: str
    ucmv_column: str
    pbi_dim_table: str
    pbi_dim_column: str
    include_values: Optional[list] = None
    exclude_values: Optional[list] = None

    def select_values(self, values: list) -> list:
        selected = set(values)
        if self.include_values is not None:
            selected &= set(self.include_values)
        return sorted(selected - set(self.exclude_values or []))


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
    pbi_extra_group_by: Optional[list] = None
    pbi_extra_filter: Optional[list] = None

    # composite
    composite_operator: Optional[str] = None
    composite_a: Optional[dict] = None
    composite_b: Optional[dict] = None

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
            col_a = self.composite_a.get("pbi_column")
            col_b = self.composite_b.get("pbi_column")
            if col_a and col_a == col_b:
                return col_a.partition("[")[2].rstrip("]") or None
        return None

    def __post_init__(self) -> None:
        # Normalise operand/column shapes up front so builders are convention-agnostic.
        self.pbi_column = _normalize_pbi_column(
            self.pbi_column, getattr(self, "pbi_table", None)
        )
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
        for operand_name in ("composite_a", "composite_b"):
            operand = getattr(self, operand_name)
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
    dimensions: dict = field(default_factory=dict)  # name -> Dimension
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
    measures: list  # list[Measure]

    def measure(self, ucmv_measure: str) -> Measure:
        for m in self.measures:
            if m.ucmv_measure == ucmv_measure:
                return m
        raise KeyError(
            f"No measure {ucmv_measure!r} in this mapping "
            f"(have: {[m.ucmv_measure for m in self.measures]})"
        )

    def resolved_measures(self) -> list:
        """Measures ready to query — excludes anything pbi_kind='unresolved'."""
        return [m for m in self.measures if m.pbi_kind != "unresolved"]


def _build_time_dimension(td: dict) -> TimeDimension:
    ucmv_td = td.get("ucmv", {}) or {}
    return TimeDimension(
        grain=td["grain"],
        ucmv_mode=ucmv_td.get("mode", "column"),
        pbi_column=td["pbi_column"],
        ucmv_column=ucmv_td.get("name"),
        ucmv_reconstruct_from=ucmv_td.get("from"),
        year_filter_column=td.get("year_filter_column", "fiscal_year"),
        pbi_table=td.get("pbi_table"),
    )


def _build_measure(m: dict) -> Measure:
    tol = m.get("tolerance", {"kind": "absolute", "value": 0.001})
    measure = Measure(
        ucmv_measure=m["ucmv_measure"],
        pbi_kind=m["pbi_kind"],
        pbi_measure=m.get("pbi_measure"),
        pbi_column=m.get("pbi_column"),
        pbi_aggregation=m.get("pbi_aggregation", "Sum"),
        switch_selector_table=m.get("switch_selector_table"),
        switch_selector_column=m.get("switch_selector_column"),
        switch_selector_value=m.get("switch_selector_value"),
        switch_selector2_table=m.get("switch_selector2_table"),
        switch_selector2_column=m.get("switch_selector2_column"),
        switch_selector2_value=m.get("switch_selector2_value"),
        context_dimension=m.get("context_dimension"),
        context_included=m.get("context_included"),
        dimension=m.get("dimension"),
        value_format=m.get("value_format", "raw_number"),
        tolerance_kind=tol.get("kind", "absolute"),
        tolerance_value=tol.get("value", 0.001),
        notes=m.get("notes", ""),
        raw_hint=m.get("raw_hint"),
        pbi_extra_group_by=m.get("extra_group_by"),
        pbi_extra_filter=m.get("extra_filter"),
        composite_operator=m.get("composite_operator"),
        composite_a=m.get("composite_a"),
        composite_b=m.get("composite_b"),
        pbi_visual_field=m.get("visual_field"),
        null_equals_zero=m.get("null_equals_zero", False),
    )
    # The Kasal emitter puts a bare column in pbi_column + a sibling pbi_table;
    # combine them here (Measure.__post_init__ can't see the sibling key).
    if m.get("pbi_table") and measure.pbi_column and "[" not in measure.pbi_column:
        measure.pbi_column = _normalize_pbi_column(
            measure.pbi_column, m.get("pbi_table")
        )
    return measure


def load_mapping_dict(raw: dict) -> UCMVMapping:
    """Build a UCMVMapping from an already-parsed mapping dict.

    Accepts both the flat reference shape (``ucmv_table``/``pbi_fact_table`` at
    top level) and a nested ``binding:`` header (Kasal's emitter) — the binding
    keys are read from whichever is present.
    """
    binding_src = raw.get("binding") or raw
    time_dimension = _build_time_dimension(raw["time_dimension"])

    dimensions = {
        name: Dimension(
            name=name,
            ucmv_column=d["ucmv_column"],
            pbi_dim_table=d["pbi_dim_table"],
            pbi_dim_column=d["pbi_dim_column"],
            include_values=d.get("include_values"),
            exclude_values=d.get("exclude_values"),
        )
        for name, d in (raw.get("dimensions") or {}).items()
    }

    binding = Binding(
        ucmv_table=binding_src.get("ucmv_table") or binding_src.get("ucmv"),
        pbi_semantic_model_id=raw.get("pbi_semantic_model_id")
        or binding_src.get("pbi_semantic_model_id"),
        pbi_fact_table=binding_src.get("pbi_fact_table"),
        time_dimension=time_dimension,
        default_dimension=binding_src.get("default_dimension")
        or raw.get("default_dimension"),
        dimensions=dimensions,
        pbi_workspace_id=raw.get("pbi_workspace_id")
        or binding_src.get("pbi_workspace_id"),
        pbi_report_id=raw.get("pbi_report_id") or binding_src.get("pbi_report_id"),
        report=raw.get("report") or binding_src.get("report"),
    )

    measures = [_build_measure(m) for m in (raw.get("measures") or [])]
    return UCMVMapping(binding=binding, measures=measures)


def load_mapping(source: str) -> UCMVMapping:
    """Load a mapping from a file path OR a YAML/JSON text string."""
    text: str
    if "\n" not in source and source.strip().endswith((".yml", ".yaml", ".json")):
        with open(source, encoding="utf-8") as f:
            text = f.read()
    else:
        text = source
    raw = yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ValueError("Mapping source did not parse to a mapping object.")
    return load_mapping_dict(raw)
