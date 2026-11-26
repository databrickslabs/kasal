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
  not fatal; ``df.empty`` is the only fatal signal for direct measures. An error
  that reads as a timeout is recovered by the retry ladder (``pbi_retry.py``):
  the measure is retried alone, then in chunks, and only if the chunks time out
  too is it reported in :class:`PbiRunReport` as not compared.
- UCMV: ``executor.execute(sql) -> (DataFrame, err)``.
"""

from __future__ import annotations

import concurrent.futures
import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Optional, Protocol

import pandas as pd

from ..raw_fields import DimensionValue
from . import pbi_query as pq
from .context_labels import ContextLabels
from .mapping import Measure, TimeDimension, UCMVMapping
from .pbi_retry import BuildFn, PbiExecutor, Recovery, TimeoutLadder, is_timeout
from .periods import normalize_snapshot_date
from .values import parse_value
from .weeks import week_key

# Max concurrent PBI network queries issued WITHIN one view. A view's
# per-measure-group DAX queries are independent network I/O, so they run on a
# bounded thread pool (the executor is synchronous) instead of one-at-a-time.
# Post-processing (parse/merge) stays sequential and in input order, so results
# are byte-for-byte identical to a serial run — only wall-clock changes.
_RECON_QUERY_CONCURRENCY = 6

# (frame or None, error detail) — the shape both executors return.
QueryOutcome = tuple[Optional[pd.DataFrame], object]


class UcmvExecutor(Protocol):
    """Runs one Spark SQL query against the UC Metric View side."""

    def execute(self, sql: str) -> QueryOutcome: ...


@dataclass
class PbiRunReport:
    """What a PBI run could not compare.

    ``timed_out``: measures whose queries timed out even after the retry ladder.
    ``skipped_periods``: ``{measure: periods}`` not queried because the metric
    view has no calendar label for them (NULL, e.g. future weeks).
    """

    timed_out: set[str] = field(default_factory=set)
    skipped_periods: dict[str, list[str]] = field(default_factory=dict)


def _execute_many(
    executor: PbiExecutor, model_id: str, daxes: list[str]
) -> list[QueryOutcome]:
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


def _period_text(value: object, grain: str) -> str:
    """Period key as text. Week grain: every spelling -> YYYYWW. Otherwise a
    whole float (a blank calendar row makes the column float) loses its ".0"."""
    if grain == "week":
        return week_key(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _is_blank(value: object) -> bool:
    """None, NA or NaN: a blank cell of a grouped result."""
    if value is None or value is pd.NA:
        return True
    return isinstance(value, float) and math.isnan(value)


def _year_month_key(year: object, month: object) -> str:
    """ "YYYY0MM" from the calendar's year and month numbers; "nan" for the blank
    calendar row, like a blank period."""
    if _is_blank(year) or _is_blank(month):
        return "nan"
    try:
        return f"{int(float(str(year))):04d}{int(float(str(month))):03d}"
    except ValueError:
        raise ValueError(
            f"time_dimension.pbi_year_column / pbi_month_column must hold numbers, "
            f"got {year!r} / {month!r}"
        ) from None


def _keyed_frame(
    df: pd.DataFrame, dimension_name: str, time_dimension: TimeDimension
) -> pd.DataFrame:
    """A grouped result with its dimension and period as the first columns.
    With year and month grouping the two calendar columns become one period key."""
    df = df.rename(columns={c: pq.strip_table_prefix(c) for c in df.columns})
    if time_dimension.pbi_year_month:
        year_col, month_col = df.columns[1], df.columns[2]
        period = [_year_month_key(y, m) for y, m in zip(df[year_col], df[month_col])]
        df = df.drop(columns=[year_col, month_col])
        df.insert(1, "period", period)
        return df.rename(columns={df.columns[0]: dimension_name})
    df = df.rename(columns={df.columns[0]: dimension_name, df.columns[1]: "period"})
    grain = time_dimension.grain
    df["period"] = df["period"].map(lambda v: _period_text(v, grain))
    return df


def _parse_measures(
    df: pd.DataFrame, measures: Iterable[Measure], strict: bool = False
) -> pd.DataFrame:
    for m in measures:
        if m.ucmv_measure in df.columns:
            df[m.ucmv_measure] = parse_value(
                df[m.ucmv_measure], m.value_format, strict=strict
            )
    return df


def _merge_on_dimension(
    a: Optional[pd.DataFrame], b: Optional[pd.DataFrame], dimension_name: str
) -> Optional[pd.DataFrame]:
    if a is None:
        return b
    if b is None:
        return a
    return a.merge(b, on=(dimension_name, "period"), how="outer")


def _stack_by_key(
    frames: Sequence[pd.DataFrame], dimension_name: str
) -> Optional[pd.DataFrame]:
    """Stack frames that share (dimension, period) keys and carry different
    measure columns: one row per key, each measure taken from the frame that has
    it (``first`` skips the NaN of the other frames)."""
    if not frames:
        return None
    if len(frames) == 1:
        return frames[0]
    stacked = pd.concat(frames, ignore_index=True)
    return stacked.groupby(
        [dimension_name, "period"], as_index=False, sort=False
    ).first()


def _absorb(
    result: Optional[pd.DataFrame],
    recovery: Recovery,
    to_frame: Callable[[pd.DataFrame], pd.DataFrame],
    dimension_name: str,
    report: PbiRunReport,
) -> Optional[pd.DataFrame]:
    """Merge the frames a timeout recovery produced, and record what it gave up on."""
    report.timed_out.update(recovery.timed_out)
    for raw in recovery.frames:
        result = _merge_on_dimension(result, to_frame(raw), dimension_name)
    return result


def run_ucmv_query(
    executor: UcmvExecutor, sql: str, dimension_name: str
) -> pd.DataFrame:
    """Run one UCMV SQL query and return its result frame. The executor yields a
    frame already keyed by (dimension_name, period, <measure columns>) — the
    column names ``build_ucmv_query`` aliased them to."""
    df, err = executor.execute(sql)
    if df is None or df.empty:
        raise RuntimeError(f"UCMV query returned no rows: {err}")
    if "period" in df.columns:
        df["period"] = df["period"].astype(str)
    return df


def _direct_build(mapping: UCMVMapping, dimension_name: str) -> BuildFn:
    def build(
        measures: Sequence[Measure],
        values: Sequence[DimensionValue],
        period: str | None,
    ) -> str | None:
        return pq.build_direct_query(mapping, dimension_name, values, measures, period)

    return build


def _switch_build(mapping: UCMVMapping, dimension_name: str) -> BuildFn:
    def build(
        measures: Sequence[Measure],
        values: Sequence[DimensionValue],
        period: str | None,
    ) -> str | None:
        queries = pq.build_switch_queries(
            mapping, dimension_name, values, measures, period
        )
        return queries[0][0] if queries else None

    return build


def _context_build(
    mapping: UCMVMapping, dimension_name: str, labels: ContextLabels | None
) -> BuildFn:
    def build(
        measures: Sequence[Measure],
        values: Sequence[DimensionValue],
        period: str | None,
    ) -> str | None:
        if period is None:
            return None
        queries = pq.build_direct_context_queries(
            mapping, dimension_name, values, [period], measures, labels
        )
        return queries[0][0] if queries else None

    return build


def _run_direct_queries(
    executor: PbiExecutor,
    model_id: str,
    mapping: UCMVMapping,
    dimension_name: str,
    dimension_values: list[DimensionValue],
    direct: list[Measure],
    ladder: TimeoutLadder,
    report: PbiRunReport,
) -> Optional[pd.DataFrame]:
    """One SUMMARIZECOLUMNS call per distinct extra_filter group (a fixed TREATAS
    filter applies to the whole query, so measures needing different fixed filters
    cannot share one call). A query that times out goes to the retry ladder."""
    td = mapping.binding.time_dimension
    strict = td.grain == "snapshot"
    build = _direct_build(mapping, dimension_name)

    def frames_for(group: list[Measure]) -> Callable[[pd.DataFrame], pd.DataFrame]:
        def convert(raw: pd.DataFrame) -> pd.DataFrame:
            return _parse_measures(_keyed_frame(raw, dimension_name, td), group, strict)

        return convert

    groups: dict[tuple[tuple[str, ...], Optional[str]], list[Measure]] = {}
    for m in direct:
        groups.setdefault((pq._extra_filter_key(m), m.pbi_chunk_by), []).append(m)

    result: Optional[pd.DataFrame] = None
    batched = [
        (ms, build(ms, dimension_values, None))
        for (_, chunk), ms in groups.items()
        if chunk is None
    ]
    pending = [(ms, dax) for ms, dax in batched if dax is not None]
    outcomes = _execute_many(executor, model_id, [dax for _, dax in pending])
    for (ms, _), (df, err) in zip(pending, outcomes):
        if df is not None and not df.empty:
            result = _merge_on_dimension(result, frames_for(ms)(df), dimension_name)
        elif is_timeout(err):
            recovery = ladder.recover(ms, build, None)
            result = _absorb(result, recovery, frames_for(ms), dimension_name, report)
        else:
            raise RuntimeError(f"DAX query failed (direct measures): {err}")
    for (_, chunk_by), ms in groups.items():
        if chunk_by is None:
            continue
        recovery = ladder.recover(ms, build, chunk_by)
        result = _absorb(result, recovery, frames_for(ms), dimension_name, report)
    return result


def _run_direct_context_queries(
    executor: PbiExecutor,
    model_id: str,
    mapping: UCMVMapping,
    dimension_name: str,
    dimension_values: list[DimensionValue],
    context_periods: list[str],
    direct_context: list[Measure],
    context_labels: ContextLabels | None,
    ladder: TimeoutLadder,
    report: PbiRunReport,
) -> Optional[pd.DataFrame]:
    """One SUMMARIZECOLUMNS call per (extra_filter group, period) for measures that
    read a calendar label in single-value context. A period without a label in
    the metric view is not queried; it is recorded in ``report.skipped_periods``."""
    build = _context_build(mapping, dimension_name, context_labels)
    result: Optional[pd.DataFrame] = None
    filter_groups: dict[tuple[tuple[str, ...], Optional[str]], list[Measure]] = {}
    for m in direct_context:
        filter_groups.setdefault((pq._extra_filter_key(m), m.pbi_chunk_by), []).append(
            m
        )
    for ms in filter_groups.values():
        built = pq.build_direct_context_queries(
            mapping,
            dimension_name,
            dimension_values,
            context_periods,
            ms,
            context_labels,
        )
        skipped = sorted(
            set(context_periods) - {period for _, period in built}, key=str
        )
        for m in ms:
            if skipped:
                report.skipped_periods[m.ucmv_measure] = skipped
        result = _merge_on_dimension(
            result,
            _context_group_frame(
                executor, model_id, ms, built, build, dimension_name, ladder, report
            ),
            dimension_name,
        )
    return result


def _context_group_frame(
    executor: PbiExecutor,
    model_id: str,
    measures: list[Measure],
    built: list[tuple[str, str]],
    build: BuildFn,
    dimension_name: str,
    ladder: TimeoutLadder,
    report: PbiRunReport,
) -> Optional[pd.DataFrame]:
    """The per-period queries of one group, stacked into one frame (None: no period
    returned rows). A period that times out is retried by the ladder, per measure
    and then per member."""
    chunk_by = next((m.pbi_chunk_by for m in measures if m.pbi_chunk_by), None)
    if chunk_by is None:
        outcomes = _execute_many(executor, model_id, [dax for dax, _ in built])
    else:
        outcomes = [(None, None)] * len(built)
    strict = False
    rows: list[pd.DataFrame] = []
    for (_, period), (df, err) in zip(built, outcomes):
        if chunk_by is None and df is not None and not df.empty:
            rows.append(_context_frame(df, dimension_name, period, measures, strict))
        elif chunk_by is not None or is_timeout(err):
            recovery = ladder.recover_period(measures, build, period, chunk_by)
            report.timed_out.update(recovery.timed_out)
            rows.extend(
                _context_frame(raw, dimension_name, period, measures, strict)
                for raw in recovery.frames
            )
    return _stack_by_key(rows, dimension_name)


def _context_frame(
    raw: pd.DataFrame,
    dimension_name: str,
    period: str,
    measures: list[Measure],
    strict: bool,
) -> pd.DataFrame:
    """A per-period context result: the dimension, and the period it was queried for."""
    df = raw.rename(columns={c: pq.strip_table_prefix(c) for c in raw.columns})
    df = df.rename(columns={df.columns[0]: dimension_name})
    df["period"] = period
    return _parse_measures(df, measures, strict)


def _run_switch_queries(
    executor: PbiExecutor,
    model_id: str,
    mapping: UCMVMapping,
    dimension_name: str,
    dimension_values: list[DimensionValue],
    switch: list[Measure],
    ladder: TimeoutLadder,
    report: PbiRunReport,
) -> tuple[Optional[pd.DataFrame], list[str]]:
    """Run every switch query group. Returns (merged_result, failed_measure_names):
    a group that comes back empty is not fatal to the run; its measures are
    returned so the caller can keep a (blank) column for them."""
    result: Optional[pd.DataFrame] = None
    failed_measures: list[str] = []
    td = mapping.binding.time_dimension
    strict = td.grain == "snapshot"
    build = _switch_build(mapping, dimension_name)
    specs = list(
        pq.build_switch_queries(mapping, dimension_name, dimension_values, switch)
    )
    batch_index = [
        i for i, (_, group, _, _) in enumerate(specs) if group[0].pbi_chunk_by is None
    ]
    outcomes = dict(
        zip(
            batch_index,
            _execute_many(executor, model_id, [specs[i][0] for i in batch_index]),
        )
    )
    for i, (_dax, group, sel_col, sel2_col) in enumerate(specs):
        if i in outcomes:
            df, err = outcomes[i]
            if df is not None and not df.empty:
                recovery = Recovery(frames=[df])
            elif is_timeout(err):
                recovery = ladder.recover(group, build, None)
            else:
                failed_measures.extend(m.ucmv_measure for m in group)
                continue
        else:
            recovery = ladder.recover(group, build, group[0].pbi_chunk_by)

        result = _absorb(
            result,
            recovery,
            _switch_converter(dimension_name, td, group, sel_col, sel2_col, strict),
            dimension_name,
            report,
        )
    return result, failed_measures


def _switch_group_result(
    df: pd.DataFrame,
    group_measures: list[Measure],
    sel_col_out: str,
    sel2_col_out: Optional[str],
    dimension_name: str,
    strict: bool = False,
) -> Optional[pd.DataFrame]:
    """Slice a switch-group frame into one column per measure by matching its
    selector value(s), case-insensitively (matching DAX's text semantics). A
    measure with no row in this frame gets no column."""
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
        if sliced.empty:
            continue
        sliced = sliced.rename(columns={"value": m.ucmv_measure})
        sliced[m.ucmv_measure] = parse_value(
            sliced[m.ucmv_measure], m.value_format, strict=strict
        )
        result = _merge_on_dimension(result, sliced, dimension_name)
    return result


def _switch_converter(
    dimension_name: str,
    time_dimension: TimeDimension,
    group: list[Measure],
    sel_col: str,
    sel2_col: str | None,
    strict: bool,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """The per-frame conversion of one switch group (see :func:`_switch_frame`)."""

    def convert(raw: pd.DataFrame) -> pd.DataFrame:
        return _switch_frame(
            raw, dimension_name, time_dimension, group, sel_col, sel2_col, strict
        )

    return convert


def _switch_frame(
    raw: pd.DataFrame,
    dimension_name: str,
    time_dimension: TimeDimension,
    group: list[Measure],
    sel_col: str,
    sel2_col: str | None,
    strict: bool,
) -> pd.DataFrame:
    """One switch-group frame sliced into one column per measure of the group.
    Measures with no row in this frame get no column (another frame may hold them)."""
    df = _keyed_frame(raw, dimension_name, time_dimension)
    sel_col_out = pq.strip_table_prefix(sel_col)
    sel2_col_out = pq.strip_table_prefix(sel2_col) if sel2_col else None
    sliced = _switch_group_result(
        df, group, sel_col_out, sel2_col_out, dimension_name, strict=strict
    )
    return (
        sliced
        if sliced is not None
        else pd.DataFrame(columns=[dimension_name, "period"])
    )


class _SnapshotExecutor:
    """Wrap a PBI executor to scope every query to the snapshot load date(s)."""

    def __init__(
        self, executor: PbiExecutor, mapping: UCMVMapping, periods: Sequence[str]
    ) -> None:
        if not periods:
            raise ValueError(
                "Snapshot reconciliation requires at least one UCMV load date."
            )
        self.executor = executor
        td = mapping.binding.time_dimension
        if not td.pbi_column:
            raise ValueError(
                "Snapshot reconciliation requires time_dimension.pbi_column."
            )
        table = (td.pbi_table or mapping.binding.pbi_fact_table).replace("'", "''")
        column = td.pbi_column.replace("]", "]]")
        dates = [normalize_snapshot_date(p) for p in periods]
        literals = [
            "DATE(" + ", ".join(str(int(v)) for v in p.split("-")) + ")"
            for p in sorted(set(dates))
        ]
        self.filter = f"TREATAS({{{', '.join(literals)}}}, '{table}'[{column}])"

    def execute(self, model_id: str, query: str) -> QueryOutcome:
        if not query.lstrip().startswith("EVALUATE"):
            raise ValueError("Snapshot queries must be EVALUATE table expressions.")
        expression = query.lstrip()[len("EVALUATE") :].strip()
        df, err = self.executor.execute(
            model_id, f"EVALUATE CALCULATETABLE({expression}, {self.filter})"
        )
        if df is None or df.empty:
            if is_timeout(err):
                return df, err  # the retry ladder decides
            raise RuntimeError(f"Snapshot DAX query returned no verifiable rows: {err}")
        return df, err


def run_pbi_queries(
    executor: PbiExecutor,
    semantic_model_id: str,
    mapping: UCMVMapping,
    dimension_name: str,
    dimension_values: list[DimensionValue],
    measures: Optional[list[Measure]] = None,
    context_periods: Optional[list[str]] = None,
    context_labels: ContextLabels | None = None,
) -> pd.DataFrame:
    """The wide frame of :func:`run_pbi_queries_reported` (the report is dropped)."""
    frame, _report = run_pbi_queries_reported(
        executor,
        semantic_model_id,
        mapping,
        dimension_name,
        dimension_values,
        measures,
        context_periods,
        context_labels,
    )
    return frame


def run_pbi_queries_reported(
    executor: PbiExecutor,
    semantic_model_id: str,
    mapping: UCMVMapping,
    dimension_name: str,
    dimension_values: list[DimensionValue],
    measures: Optional[list[Measure]] = None,
    context_periods: Optional[list[str]] = None,
    context_labels: ContextLabels | None = None,
) -> tuple[pd.DataFrame, PbiRunReport]:
    """Run whatever queries ``measures`` needs (default: every resolved measure)
    and return one wide frame, one column per ucmv_measure, keyed on
    (dimension_name, period), plus the :class:`PbiRunReport` of what could not
    be compared.

    ``context_periods``: required for measures with ``pbi_extra_group_by`` (the
    periods they are queried one by one for) and the chunks of a timed-out query.
    ``context_labels``: the calendar label per period (see ``labels_from_ucmv``).
    """
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

    td = mapping.binding.time_dimension
    if td.grain == "snapshot":
        if any(m.pbi_extra_group_by for m in measures):
            raise ValueError(
                "Snapshot extra grouping requires a reviewed date-context implementation."
            )
        executor = _SnapshotExecutor(executor, mapping, context_periods or [])

    direct, direct_context, switch = pq.partition_measures_by_shape(measures)
    if direct_context and not context_periods:
        raise ValueError(
            "context_periods is required for measures with pbi_extra_group_by: "
            + ", ".join(m.ucmv_measure for m in direct_context)
        )

    ladder = TimeoutLadder(
        executor,
        semantic_model_id,
        dimension_values,
        context_periods or [],
        period_chunks=td.grain == "month",
    )
    report = PbiRunReport()
    result = _run_direct_queries(
        executor,
        semantic_model_id,
        mapping,
        dimension_name,
        dimension_values,
        direct,
        ladder,
        report,
    )
    context_result = _run_direct_context_queries(
        executor,
        semantic_model_id,
        mapping,
        dimension_name,
        dimension_values,
        context_periods or [],
        direct_context,
        context_labels,
        ladder,
        report,
    )
    result = _merge_on_dimension(result, context_result, dimension_name)
    switch_result, failed = _run_switch_queries(
        executor,
        semantic_model_id,
        mapping,
        dimension_name,
        dimension_values,
        switch,
        ladder,
        report,
    )
    result = _merge_on_dimension(result, switch_result, dimension_name)

    result = (
        result
        if result is not None
        else pd.DataFrame(columns=[dimension_name, "period"])
    )
    for measure_name in failed + sorted(report.timed_out - set(result.columns)):
        result[measure_name] = pd.NA
    return result, report
