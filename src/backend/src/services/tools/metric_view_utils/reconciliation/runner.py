"""Executor-driven orchestration: run the built queries and assemble results.

This is the only module in the package that performs "I/O" — and it does so
through an INJECTED executor (a duck-typed object with an ``execute`` method),
never a concrete client. The tool wrapper supplies a real Databricks/PBI
executor; unit tests supply a fake. Everything else here is the batching, merge,
and switch-slicing logic that ties the pure query builders in ``pbi_query.py`` to
whatever executor is given.

Executor contracts:

- PBI: ``executor.execute(semantic_model_id, dax_query) -> (DataFrame, err)``.
  A non-None ``err`` with a non-empty df is a warning (e.g. an all-null column),
  not fatal; ``df.empty`` is the only fatal signal for direct measures.
- UCMV: ``executor.execute(sql) -> (DataFrame, err)``.
"""

from __future__ import annotations

import concurrent.futures
from typing import Optional

import pandas as pd

from . import pbi_query as pq
from .mapping import UCMVMapping
from .periods import normalize_snapshot_date
from .values import parse_value

# Max concurrent PBI network queries issued WITHIN one view. A view's
# per-measure-group DAX queries are independent network I/O, so they run on a
# bounded thread pool (the executor is synchronous) instead of one-at-a-time.
# Post-processing (parse/merge) stays sequential and in input order, so results
# are byte-for-byte identical to a serial run — only wall-clock changes.
_RECON_QUERY_CONCURRENCY = 6


def _execute_many(executor, model_id, daxes: list) -> list:
    """``executor.execute(model_id, dax)`` for each dax, concurrently (bounded),
    returning ``(df, err)`` tuples in the SAME order as ``daxes``.

    A single query runs inline (no pool). Exceptions propagate exactly as a serial
    run would raise them, preserving the callers' fatal/fail-open semantics.
    """
    if not daxes:
        return []
    if len(daxes) == 1:
        return [executor.execute(model_id, daxes[0])]
    max_workers = min(_RECON_QUERY_CONCURRENCY, len(daxes))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(lambda dax: executor.execute(model_id, dax), daxes))


def _rename_first_two_cols(df: pd.DataFrame, dimension_name: str) -> pd.DataFrame:
    df = df.rename(columns={c: pq.strip_table_prefix(c) for c in df.columns})
    df = df.rename(columns={df.columns[0]: dimension_name, df.columns[1]: "period"})
    df["period"] = df["period"].astype(str)
    return df


def _merge_on_dimension(
    a: Optional[pd.DataFrame], b: Optional[pd.DataFrame], dimension_name: str
) -> Optional[pd.DataFrame]:
    if a is None:
        return b
    if b is None:
        return a
    return a.merge(b, on=(dimension_name, "period"), how="outer")


def run_ucmv_query(executor, sql: str, dimension_name: str) -> pd.DataFrame:
    """Run one UCMV SQL query and return its result frame. The executor yields a
    frame already keyed by (dimension_name, period, <measure columns>) — the
    column names ``build_ucmv_query`` aliased them to."""
    df, err = executor.execute(sql)
    if df is None or df.empty:
        raise RuntimeError(f"UCMV query returned no rows: {err}")
    if "period" in df.columns:
        df["period"] = df["period"].astype(str)
    return df


def _run_direct_queries(
    executor, model_id, mapping, dimension_name, dimension_values, direct
):
    result: Optional[pd.DataFrame] = None
    filter_groups: dict = {}
    for m in direct:
        filter_groups.setdefault(pq._extra_filter_key(m), []).append(m)
    # Build every group's DAX first, then run the (non-empty) queries concurrently;
    # process results in the SAME order the groups merged serially.
    pending = []
    for group_measures in filter_groups.values():
        dax = pq.build_direct_query(
            mapping, dimension_name, dimension_values, group_measures
        )
        if dax:
            pending.append((dax, group_measures))
    outcomes = _execute_many(executor, model_id, [dax for dax, _ in pending])
    strict = mapping.binding.time_dimension.grain == "snapshot"
    for (dax, group_measures), (df, err) in zip(pending, outcomes):
        if df is None or df.empty:
            raise RuntimeError(f"DAX query failed (direct measures): {err}")
        df = _rename_first_two_cols(df, dimension_name)
        for m in group_measures:
            df[m.ucmv_measure] = parse_value(
                df[m.ucmv_measure], m.value_format, strict=strict
            )
        result = _merge_on_dimension(result, df, dimension_name)
    return result


def _run_direct_context_queries(
    executor,
    model_id,
    mapping,
    dimension_name,
    dimension_values,
    context_periods,
    direct_context,
):
    result: Optional[pd.DataFrame] = None
    ctx_filter_groups: dict = {}
    for m in direct_context:
        ctx_filter_groups.setdefault(pq._extra_filter_key(m), []).append(m)
    groups = list(ctx_filter_groups.values())
    # Flatten every (group, period) query, run them all concurrently, then
    # re-group the results per group IN ORDER so the merge is identical to serial.
    flat: list = []  # (group_index, period, dax)
    for gi, ctx_group in enumerate(groups):
        for dax, period in pq.build_direct_context_queries(
            mapping, dimension_name, dimension_values, context_periods, ctx_group
        ):
            flat.append((gi, period, dax))
    outcomes = _execute_many(executor, model_id, [dax for _, _, dax in flat])
    rows_by_group: dict = {gi: [] for gi in range(len(groups))}
    for (gi, period, _dax), (df, _err) in zip(flat, outcomes):
        rows_by_group[gi].append((period, df))
    for gi, ctx_group in enumerate(groups):
        per_period_rows = []
        for period, df in rows_by_group[gi]:
            if df is None or df.empty:
                continue
            df = df.rename(columns={c: pq.strip_table_prefix(c) for c in df.columns})
            df = df.rename(columns={df.columns[0]: dimension_name})
            df["period"] = period
            per_period_rows.append(df)
        if per_period_rows:
            ctx_df = pd.concat(per_period_rows, ignore_index=True)
            for m in ctx_group:
                ctx_df[m.ucmv_measure] = parse_value(
                    ctx_df[m.ucmv_measure], m.value_format
                )
            result = _merge_on_dimension(result, ctx_df, dimension_name)
    return result


def _switch_group_result(
    df, group_measures, sel_col_out, sel2_col_out, dimension_name, strict=False
):
    """Slice a switch-group frame into one column per measure by matching its
    selector value(s), case-insensitively (matching DAX's text semantics)."""
    result: Optional[pd.DataFrame] = None
    for m in group_measures:
        mask = (
            df[sel_col_out].astype(str).str.casefold()
            == str(m.switch_selector_value).casefold()
        )
        if sel2_col_out:
            mask &= (
                df[sel2_col_out].astype(str).str.casefold()
                == str(m.switch_selector2_value).casefold()
            )
        sliced = df[mask][[dimension_name, "period", "value"]].copy()
        sliced = sliced.rename(columns={"value": m.ucmv_measure})
        sliced[m.ucmv_measure] = parse_value(
            sliced[m.ucmv_measure], m.value_format, strict=strict
        )
        result = _merge_on_dimension(result, sliced, dimension_name)
    return result


def _run_switch_queries(
    executor, model_id, mapping, dimension_name, dimension_values, switch
):
    result: Optional[pd.DataFrame] = None
    failed_measures: list = []
    strict = mapping.binding.time_dimension.grain == "snapshot"
    specs = list(
        pq.build_switch_queries(mapping, dimension_name, dimension_values, switch)
    )
    outcomes = _execute_many(executor, model_id, [dax for dax, *_ in specs])
    for (dax, group_measures, sel_col, sel2_col), (df, _err) in zip(specs, outcomes):
        if df is None or df.empty:
            failed_measures.extend(m.ucmv_measure for m in group_measures)
            continue
        df = _rename_first_two_cols(df, dimension_name)
        sel_col_out = pq.strip_table_prefix(sel_col)
        sel2_col_out = pq.strip_table_prefix(sel2_col) if sel2_col else None
        df = df.rename(columns={c: pq.strip_table_prefix(c) for c in df.columns})
        group_result = _switch_group_result(
            df, group_measures, sel_col_out, sel2_col_out, dimension_name, strict=strict
        )
        result = _merge_on_dimension(result, group_result, dimension_name)
    return result, failed_measures


class _SnapshotExecutor:
    """Wrap a PBI executor to scope every query to the snapshot load date(s)."""

    def __init__(self, executor, mapping, periods):
        if not periods:
            raise ValueError(
                "Snapshot reconciliation requires at least one UCMV load date."
            )
        self.executor = executor
        td = mapping.binding.time_dimension
        table = (td.pbi_table or mapping.binding.pbi_fact_table).replace("'", "''")
        column = td.pbi_column.replace("]", "]]")
        dates = [normalize_snapshot_date(p) for p in periods]
        literals = [
            "DATE(" + ", ".join(str(int(v)) for v in p.split("-")) + ")"
            for p in sorted(set(dates))
        ]
        self.filter = f"TREATAS({{{', '.join(literals)}}}, '{table}'[{column}])"

    def execute(self, model_id, query):
        if not query.lstrip().startswith("EVALUATE"):
            raise ValueError("Snapshot queries must be EVALUATE table expressions.")
        expression = query.lstrip()[len("EVALUATE") :].strip()
        df, err = self.executor.execute(
            model_id, f"EVALUATE CALCULATETABLE({expression}, {self.filter})"
        )
        if df is None or df.empty:
            raise RuntimeError(f"Snapshot DAX query returned no verifiable rows: {err}")
        return df, err


def run_pbi_queries(
    executor,
    semantic_model_id: str,
    mapping: UCMVMapping,
    dimension_name: str,
    dimension_values: list,
    measures: Optional[list] = None,
    context_periods: Optional[list] = None,
) -> pd.DataFrame:
    """Run whatever queries ``measures`` needs (default: every resolved measure)
    and return one wide frame, one column per ucmv_measure, keyed on
    (dimension_name, period)."""
    measures = measures if measures is not None else mapping.resolved_measures()
    pq.validate_dimension_conditional_usage(measures, dimension_name)

    dimension = mapping.binding.dimensions[dimension_name]
    if dimension.include_values is not None or dimension.exclude_values:
        dimension_values = dimension.select_values(
            dimension_values or dimension.include_values or []
        )
        if not dimension_values:
            raise ValueError(
                "No dimension values remain inside this measure's applicability."
            )

    if mapping.binding.time_dimension.grain == "snapshot":
        if any(m.pbi_extra_group_by for m in measures):
            raise ValueError(
                "Snapshot extra grouping requires a reviewed date-context implementation."
            )
        executor = _SnapshotExecutor(executor, mapping, context_periods)

    direct, direct_context, switch = pq.partition_measures_by_shape(measures)
    if direct_context and not context_periods:
        raise ValueError(
            "context_periods is required for measures with pbi_extra_group_by: "
            + ", ".join(m.ucmv_measure for m in direct_context)
        )

    result = _run_direct_queries(
        executor, semantic_model_id, mapping, dimension_name, dimension_values, direct
    )
    context_result = _run_direct_context_queries(
        executor,
        semantic_model_id,
        mapping,
        dimension_name,
        dimension_values,
        context_periods,
        direct_context,
    )
    result = _merge_on_dimension(result, context_result, dimension_name)
    switch_result, failed = _run_switch_queries(
        executor, semantic_model_id, mapping, dimension_name, dimension_values, switch
    )
    result = _merge_on_dimension(result, switch_result, dimension_name)

    result = (
        result
        if result is not None
        else pd.DataFrame(columns=[dimension_name, "period"])
    )
    for measure_name in failed:
        result[measure_name] = pd.NA
    return result
