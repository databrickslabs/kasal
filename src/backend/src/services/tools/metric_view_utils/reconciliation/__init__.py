"""Deterministic reconciliation core for PBI -> UC Metric View migration.

Given a reconciliation MAPPING, this package builds the UCMV-side SQL and the
PBI-side DAX, parses PBI display values back into numbers, joins the two sides
cell-by-cell within tolerance, and classifies mismatches — all deterministically
(no LLM in the comparison path). The live orchestration (running the queries
against a Databricks SQL warehouse + the PBI Execute Queries API) lives in
``src/services/tools/ucmv_reconciliation_tool.py``; everything here is either
pure or executor-injected so it can be unit-tested with no network.
"""

from __future__ import annotations

from .classify import MismatchStats, classify
from .compare import (
    LONG_FORMAT_COLUMNS,
    compare,
    summarize,
    summarize_by_dimension,
    summarize_by_period,
    to_long_format,
)
from .mapping import (
    Binding,
    Dimension,
    Measure,
    TimeDimension,
    UCMVMapping,
    load_mapping,
    load_mapping_dict,
)
from .pbi_query import (
    build_dimension_scope_query,
    build_direct_context_queries,
    build_direct_queries,
    build_direct_query,
    build_switch_queries,
    filter_expression,
    measure_dax_expr,
    partition_measures_by_shape,
    validate_dimension_conditional_usage,
)
from .periods import (
    current_period,
    is_closed_period,
    matches_reference_years,
    normalize_snapshot_date,
)
from .loop import (
    CycleResult,
    LoopResult,
    build_refinement_feedback,
    feedback_to_prompt_map,
    run_reconciliation_loop,
)
from .runner import run_pbi_queries, run_ucmv_query
from .ucmv_query import build_ucmv_query
from .values import parse_value, strip_percent

__all__ = [
    # mapping
    "Binding",
    "Dimension",
    "Measure",
    "TimeDimension",
    "UCMVMapping",
    "load_mapping",
    "load_mapping_dict",
    # ucmv query
    "build_ucmv_query",
    # pbi query
    "build_direct_query",
    "build_direct_queries",
    "build_direct_context_queries",
    "build_switch_queries",
    "build_dimension_scope_query",
    "filter_expression",
    "measure_dax_expr",
    "partition_measures_by_shape",
    "validate_dimension_conditional_usage",
    # values
    "parse_value",
    "strip_percent",
    # periods
    "current_period",
    "is_closed_period",
    "matches_reference_years",
    "normalize_snapshot_date",
    # compare
    "compare",
    "to_long_format",
    "summarize",
    "summarize_by_period",
    "summarize_by_dimension",
    "LONG_FORMAT_COLUMNS",
    # classify
    "classify",
    "MismatchStats",
    # runner
    "run_pbi_queries",
    "run_ucmv_query",
    # iterative loop
    "run_reconciliation_loop",
    "build_refinement_feedback",
    "feedback_to_prompt_map",
    "CycleResult",
    "LoopResult",
]
