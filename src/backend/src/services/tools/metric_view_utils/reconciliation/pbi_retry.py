"""Timeout recovery for Power BI Execute Queries (pure control flow).

A DAX query that times out does not stop the run. The ladder, per measure:

1. the all-at-once query (done by the caller, concurrently with its siblings);
2. after a timeout, each measure alone;
3. still timing out, chunks: one query per period (all members) when the grain
   is month, otherwise one per dimension member.

A measure whose chunks still time out is reported as timed out and left out of
the comparison. A measure with ``pbi_chunk_by`` skips step 1 and goes straight
to the chunks it names.

The executor is injected (``execute(model_id, dax) -> (DataFrame, err)``), so
this module makes no network calls and is unit-tested with fakes.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Optional, Protocol

import pandas as pd

from ..raw_fields import DimensionValue
from .mapping import Measure

# Builds the DAX for (measures, dimension members, period or None = all periods).
# None means there is nothing to query (no measures, or no label for a period).
BuildFn = Callable[
    [Sequence[Measure], Sequence[DimensionValue], Optional[str]], Optional[str]
]
# (dimension members, period or None): the unit of one chunk query.
Unit = tuple[list[DimensionValue], Optional[str]]

_TIMEOUT_MARKERS = ("timed out", "timeout", "time out")


class PbiExecutor(Protocol):
    """Runs one DAX query against a semantic model."""

    def execute(
        self, semantic_model_id: str, dax_query: str
    ) -> tuple[Optional[pd.DataFrame], object]: ...


def is_timeout(error: object) -> bool:
    """True for an executor error that means the query ran out of time."""
    if error is None:
        return False
    text = str(error).lower()
    return any(marker in text for marker in _TIMEOUT_MARKERS)


@dataclass
class Recovery:
    """Result frames of a recovered query (raw, not yet renamed or parsed) and the
    measures that still timed out after every retry."""

    frames: list[pd.DataFrame] = field(default_factory=list)
    timed_out: list[str] = field(default_factory=list)

    def absorb(self, other: "Recovery") -> None:
        self.frames.extend(other.frames)
        self.timed_out.extend(other.timed_out)


class TimeoutLadder:
    """Runs the retry steps for one reconciliation view against one executor."""

    def __init__(
        self,
        executor: PbiExecutor,
        semantic_model_id: str,
        dimension_values: Sequence[DimensionValue],
        periods: Sequence[str],
        period_chunks: bool,
    ) -> None:
        self.executor = executor
        self.semantic_model_id = semantic_model_id
        self.dimension_values = list(dimension_values)
        self.periods = list(periods)
        # Period chunks need the "YYYYNNN" period key in the PBI filter (month
        # grain). Other grains chunk per dimension member instead.
        self.period_chunks = period_chunks

    def _attempt(
        self,
        build: BuildFn,
        measures: Sequence[Measure],
        values: Sequence[DimensionValue],
        period: Optional[str],
    ) -> tuple[Optional[pd.DataFrame], object]:
        """One query: its non-empty frame (or None) and the executor's error."""
        dax = build(measures, values, period)
        if dax is None:
            return None, None
        df, err = self.executor.execute(self.semantic_model_id, dax)
        return (df if df is not None and not df.empty else None), err

    def units(self, chunk_by: str) -> list[Unit]:
        """The chunks of a query over all periods: per period (all members), or
        per dimension member (all periods)."""
        if chunk_by == "period" and self.period_chunks and self.periods:
            return [(self.dimension_values, period) for period in self.periods]
        return [([value], None) for value in self.dimension_values]

    def recover(
        self,
        measures: Sequence[Measure],
        build: BuildFn,
        chunk_by: Optional[str],
    ) -> Recovery:
        """Recover a query over ``measures`` that timed out (``chunk_by`` None), or
        run its chunks straight away (``chunk_by`` set by the mapping)."""
        if chunk_by is None and len(measures) > 1:
            out = Recovery()
            for measure in measures:
                out.absorb(self._alone(measure, build))
            return out
        return self._chunks(measures, build, self.units(chunk_by or "period"))

    def recover_period(
        self,
        measures: Sequence[Measure],
        build: BuildFn,
        period: str,
        chunk_by: Optional[str] = None,
    ) -> Recovery:
        """Recover a per-period query (calendar-label measures) that timed out:
        each measure alone at that period, then per member. ``chunk_by`` set by
        the mapping skips the attempt and goes straight to the member chunks."""
        member_units = [([value], period) for value in self.dimension_values]
        if chunk_by is not None or len(measures) == 1:
            return self._chunks(measures, build, member_units)
        out = Recovery()
        for measure in measures:
            frame, err = self._attempt(build, [measure], self.dimension_values, period)
            if frame is not None:
                out.frames.append(frame)
            elif is_timeout(err):
                out.absorb(self._chunks([measure], build, member_units))
        return out

    def _alone(self, measure: Measure, build: BuildFn) -> Recovery:
        frame, err = self._attempt(build, [measure], self.dimension_values, None)
        if frame is not None:
            return Recovery(frames=[frame])
        if not is_timeout(err):
            return Recovery()  # no rows for this measure alone: compared as missing
        return self._chunks([measure], build, self.units("period"))

    def _chunks(
        self,
        measures: Sequence[Measure],
        build: BuildFn,
        units: Sequence[Unit],
    ) -> Recovery:
        frames: list[pd.DataFrame] = []
        timed_out = False
        for values, period in units:
            frame, err = self._attempt(build, measures, values, period)
            if frame is not None:
                frames.append(frame)
            elif is_timeout(err):
                timed_out = True
        out = Recovery()
        # One stacked frame per measure group: its chunks share the columns and
        # have disjoint keys, so they stack instead of merging.
        if frames:
            out.frames.append(pd.concat(frames, ignore_index=True))
        if timed_out:
            out.timed_out = [m.ucmv_measure for m in measures]
        return out
