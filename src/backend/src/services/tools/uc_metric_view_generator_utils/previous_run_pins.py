"""Reuse of a previous run's translations, measure by measure (rerun pins).

``previous_output`` is the JSON a generator run returned. A measure is PINNED to the SQL it had
in that run when all of these hold: it sat in a view that was deployed and compared with Power BI,
its comparison reached the lock threshold on enough cells, it was not removed by deploy healing,
and its DAX is unchanged. A pin is a manual override (``manual_overrides`` in the pipeline config,
Step 5a-override of ``table_processor``), so the LLM never sees the measure. Everything else is
translated as usual.

A pin can be wrong after the views are built, and two checks run then:

- the new view no longer has what the pinned SQL reads (a column of the fact, a join alias, a
  measure), or a prior-year measure's window is not the one it had: the pin is dropped and the
  pipeline runs once more without it, so the measure is translated normally;
- the rules run before the overrides are injected, so a rule translation wins over a pin: the
  measure keeps the rule's SQL and is not reported as reused.

Pipeline config keys: ``rerun_lock_pct`` (default 99) and ``rerun_min_cells`` (default 10; a
measure compared on fewer cells proves little). Pins are OFF unless ``previous_output`` is given.

A prior-year pin carries no window: its SQL is the unshifted one, and ``prior_year.add_windows``
adds the window and its key dimension from the DAX, which is unchanged. A window set on the pin
would make ``add_windows`` demote the measure ("prior year of a measure that is a window already").
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from src.services.tools.metric_view_utils.dax_preclean import preclean_dax
from src.services.tools.metric_view_utils.drift.fingerprint import dax_fingerprint
from src.services.tools.metric_view_utils.raw_fields import (
    as_int,
    as_list,
    as_map,
    opt_float,
)

if TYPE_CHECKING:
    from src.services.tools.metric_view_utils.data_classes import (
        MetricViewSpec,
        TranslationResult,
    )

logger = logging.getLogger(__name__)

PIN_COMMENT = "reused from the previous run (reconciled at or above the lock threshold)"
DEFAULT_LOCK_PCT = 99.0
DEFAULT_MIN_CELLS = 10

# ``alias.column`` (or ``alias.`column```) in a measure's SQL, and MEASURE(name) references.
_QUALIFIED = re.compile(r"\b([A-Za-z_]\w*)\.`?([A-Za-z_]\w*)`?")
_MEASURE_CALL = re.compile(r"\bMEASURE\(\s*`?(\w+)`?\s*\)", re.IGNORECASE)

Key = tuple[str, str]  # (table key, measure name)


@dataclass(frozen=True)
class PinSettings:
    lock_pct: float = DEFAULT_LOCK_PCT
    min_cells: int = DEFAULT_MIN_CELLS


def _positive(value: object) -> float | None:
    if value is None or isinstance(value, bool) or value == "":
        return None
    try:
        number = float(str(value))
    except ValueError:
        return None
    return number if number > 0 else None


def pin_settings(config: Mapping[str, object]) -> PinSettings:
    """``rerun_lock_pct`` and ``rerun_min_cells`` from the pipeline config, or the defaults."""
    lock = _positive(config.get("rerun_lock_pct"))
    cells = _positive(config.get("rerun_min_cells"))
    return PinSettings(
        lock_pct=lock if lock is not None else DEFAULT_LOCK_PCT,
        min_cells=int(cells) if cells is not None else DEFAULT_MIN_CELLS,
    )


@dataclass
class PinPlan:
    """The pins for this run, the windows the previous run gave them, and why the rest were not pinned."""

    # (table key, measure) -> the SQL pinned
    pins: dict[Key, str] = field(default_factory=dict)
    # (table key, measure) -> the window the previous run gave it (None when it had none)
    windows: dict[Key, object] = field(default_factory=dict)
    # table key -> manual_overrides entries (the pipeline config shape)
    overrides: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    # reason -> number of measures that qualified for no pin
    not_pinned: dict[str, int] = field(default_factory=dict)
    note: str = ""

    def without(self, dropped: Iterable[Key]) -> PinPlan:
        """This plan minus ``dropped`` pins."""
        drop = set(dropped)
        kept = PinPlan(note=self.note, not_pinned=dict(self.not_pinned))
        for key, sql in self.pins.items():
            if key not in drop:
                kept.pins[key] = sql
                kept.windows[key] = self.windows.get(key)
        kept.overrides = _overrides_of(kept.pins, self.overrides)
        return kept


def _overrides_of(
    pins: Mapping[Key, str], source: Mapping[str, list[dict[str, object]]]
) -> dict[str, list[dict[str, object]]]:
    """The override entries for ``pins``, taken from ``source`` in their order."""
    out: dict[str, list[dict[str, object]]] = {}
    for table, entries in source.items():
        for entry in entries:
            if (table, str(entry.get("name"))) in pins:
                out.setdefault(table, []).append(entry)
    return out


def _as_dict(value: object) -> dict[str, object]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            logger.warning("[UCMV] previous_output is not valid JSON; no pins")
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _current_dax(measures: object) -> dict[str, tuple[int, set[str]]]:
    """Per PBI measure name of this run: how many distinct DAX texts it has, and the fingerprints
    the translated DAX may carry. The translator stores the DAX after ``preclean_dax`` (variables
    inlined), and prior-year measures store the raw text, so both forms are accepted."""
    raw: dict[str, set[str]] = {}
    for m in as_list(measures):
        entry = as_map(m)
        name = str(
            entry.get("original_name")
            or entry.get("measure_name")
            or entry.get("name")
            or ""
        )
        text = str(entry.get("dax_expression") or entry.get("expression") or "")
        if name and text.strip():
            raw.setdefault(name, set()).add(text)
    out: dict[str, tuple[int, set[str]]] = {}
    for name, texts in raw.items():
        forms: set[str] = set()
        for text in texts:
            forms.add(dax_fingerprint(text))
            forms.add(dax_fingerprint(preclean_dax(text)))
        out[name] = (len(texts), forms - {""})
    return out


def _healed_names(previous: Mapping[str, object]) -> dict[str, set[str]]:
    """Measures that deploy healing removed from a view, in any cycle of the previous run."""
    out: dict[str, set[str]] = {}
    for cycle in as_list(previous.get("cycles")):
        for view, items in as_map(as_map(cycle).get("deploy_healed")).items():
            for item in as_list(items):
                text = str(item)
                if text.startswith("measure "):
                    out.setdefault(str(view), set()).add(
                        text.split(" ", 1)[1].split(" (", 1)[0]
                    )
    return out


def _excluded_keys(feedback: object) -> set[tuple[object, object]]:
    """(view, measure) pairs this run's comparison failed (the refinement feedback)."""
    items = feedback
    if isinstance(feedback, str):
        try:
            items = json.loads(feedback)
        except json.JSONDecodeError:
            return set()
    if isinstance(items, Mapping):
        return {(None, str(name)) for name in items}
    return {
        (item.get("view"), str(item.get("measure")))
        for item in map(as_map, as_list(items))
        if item.get("measure")
    }


class _PreviousRun:
    """The fields of a previous generator output the pins are read from."""

    def __init__(self, previous: Mapping[str, object], settings: PinSettings) -> None:
        self.settings = settings
        self.views = as_map(as_map(previous.get("reconciliation")).get("views"))
        self.final_yaml = as_map(previous.get("yaml"))
        self.summaries = as_map(previous.get("specs_summary"))
        self.best = as_map(previous.get("best_by_view"))
        self.cycles = len(as_list(previous.get("cycles")))
        self.healed = _healed_names(previous)

    def reason(
        self,
        table: str,
        row: Mapping[str, object],
        current: Mapping[str, tuple[int, set[str]]],
        excluded: set[tuple[object, object]],
    ) -> tuple[str | None, int]:
        """Why ``row`` is not pinned (None when it is), and its cells compared."""
        name = str(row.get("measure_name") or "")
        view = str(as_map(self.summaries.get(table)).get("view_name") or "")
        if table not in self.views or table not in self.final_yaml:
            return "view not compared in the previous run", 0
        best_cycle = as_map(self.best.get(table)).get("cycle")
        if best_cycle is not None and self.cycles and as_int(best_cycle) != self.cycles:
            return "view's final YAML is from an earlier cycle than its SQL", 0
        measure = as_map(as_map(self.views[table]).get("measures")).get(name)
        if measure is None:
            return "measure not compared in the previous run", 0
        info = as_map(measure)
        cells = as_int(info.get("cells_total"))
        pct = opt_float(info.get("pct_aligned"))
        if pct is None:
            return "not scored in the previous run", 0
        if cells < self.settings.min_cells:
            return f"compared on fewer than {self.settings.min_cells} cells", cells
        if pct < self.settings.lock_pct:
            return f"below the {self.settings.lock_pct:g}% lock", cells
        if name in self.healed.get(table, set()):
            return "removed by deploy healing in the previous run", cells
        if (
            (table, name) in excluded
            or (view, name) in excluded
            or (None, name) in excluded
        ):
            return "failed in this run's comparison (retranslated)", cells
        if not str(row.get("sql_expr") or "").strip():
            return "no SQL in the previous output", cells
        original = str(row.get("original_name") or name)
        fp = dax_fingerprint(str(row.get("dax_expression") or ""))
        if not fp:
            return "no DAX recorded in the previous output", cells
        if original not in current:
            return "measure not in this run's input", cells
        count, forms = current[original]
        if count != 1:
            return "DAX changed (two texts under one name)", cells
        if fp not in forms:
            return "DAX changed", cells
        return None, cells


def plan_previous_run_pins(
    previous: object,
    measures: object,
    config: Mapping[str, object],
    feedback: object = None,
) -> PinPlan:
    """The pins a previous generator output allows for this run's ``measures``.

    ``feedback``: this run's refinement feedback (the loop's cycle N-1 failures, JSON or
    list). Its measures failed in this run's comparison, so they are retranslated, never
    pinned again.
    """
    plan = PinPlan()
    prev = _as_dict(previous)
    if not prev:
        return plan
    if not prev.get("reconciliation"):
        plan.note = "previous output has no reconciliation: nothing to pin"
        return plan
    run = _PreviousRun(prev, pin_settings(config))
    current = _current_dax(measures)
    skip = _excluded_keys(feedback)
    for table, rows in as_map(prev.get("resolved_measures_by_table")).items():
        for raw in as_list(rows):
            row = as_map(raw)
            name = str(row.get("measure_name") or "")
            why, _cells = run.reason(str(table), row, current, skip)
            if why is not None:
                plan.not_pinned[why] = plan.not_pinned.get(why, 0) + 1
                continue
            key = (str(table), name)
            plan.pins[key] = str(row.get("sql_expr"))
            plan.windows[key] = row.get("window")
            plan.overrides.setdefault(str(table), []).append(
                {
                    "name": name,
                    "original_name": row.get("original_name") or name,
                    "expr": str(row.get("sql_expr")),
                    "comment": PIN_COMMENT,
                }
            )
    return plan


def _in_effect(
    spec: MetricViewSpec | None, name: str, sql: str
) -> TranslationResult | None:
    """The measure of ``spec`` that carries the pinned SQL (None when a rule or nothing has it)."""
    if spec is None:
        return None
    for measure in spec.measures:
        if (
            measure.measure_name == name
            and (measure.sql_expr or "").strip() == sql.strip()
        ):
            return measure
    return None


def _missing_reads(spec: MetricViewSpec, sql: str) -> list[str]:
    """What ``sql`` reads that the view does not have (empty when everything is there)."""
    fact_key = spec.fact_table_key.lower()
    fact_columns = {c.lower() for c in spec.fact_columns}
    aliases = {str(j.get("name")).lower() for j in spec.joins if j.get("name")}
    measures = {m.measure_name.lower() for m in spec.measures}
    missing: list[str] = []
    for m in _QUALIFIED.finditer(sql):
        qualifier, column = m.group(1), m.group(2)
        if qualifier.lower() in ("source", fact_key):
            # The fact's columns are known only when its native query lists them.
            if fact_columns and column.lower() not in fact_columns:
                missing.append(f"column {qualifier}.{column}")
        elif qualifier.lower() not in aliases:
            missing.append(f"join alias {qualifier}")
    for m in _MEASURE_CALL.finditer(sql):
        if m.group(1).lower() not in measures:
            missing.append(f"measure {m.group(1)}")
    return missing


def _window_missing(spec: MetricViewSpec, window: object) -> str | None:
    """A window whose ordering dimension the view does not have."""
    if not isinstance(window, Mapping):
        return None
    order = str(window.get("order") or "")
    if order and order not in {str(d.get("name")) for d in spec.dimensions}:
        return f"window dimension {order}"
    return None


def stale_pins(specs: Mapping[str, MetricViewSpec], plan: PinPlan) -> dict[Key, str]:
    """The pins whose SQL reads what the new view does not have, or whose window differs.

    The reads are checked whether or not the pin landed: a pin the pipeline dropped for an
    unknown alias or column is exactly the case to retranslate. The window is checked only
    for a pin that landed (its window is the one the view gave it)."""
    stale: dict[Key, str] = {}
    for key, sql in plan.pins.items():
        table, name = key
        spec = specs.get(table)
        if spec is None:
            continue  # no such view in this run: nothing to check it against
        missing = _missing_reads(spec, sql)
        measure = _in_effect(spec, name, sql)
        if measure is not None:
            window_gone = _window_missing(spec, plan.windows.get(key))
            if window_gone:
                missing.append(window_gone)
            if measure.window_spec != plan.windows.get(key):
                missing.append("the window the previous run had")
        if missing:
            stale[key] = "the view has no " + ", ".join(missing)
    return stale


@dataclass
class PinOutcome:
    """What the pins did in the final pipeline run."""

    planned: int
    reused: dict[str, list[str]]
    invalidated: dict[str, dict[str, str]]
    not_pinned: dict[str, int]

    def as_output(self) -> dict[str, object]:
        return {
            "reused_from_previous_run": self.reused,
            "previous_run_pins": {
                "planned": self.planned,
                "reused": sum(len(v) for v in self.reused.values()),
                "invalidated": self.invalidated,
                "not_pinned": self.not_pinned,
            },
        }


def pin_outcome(
    specs: Mapping[str, MetricViewSpec],
    plan: PinPlan,
    invalidated: Mapping[Key, str],
) -> PinOutcome:
    """The reused pins of the final run (in effect, same SQL) and the invalidated ones."""
    reused: dict[str, list[str]] = {}
    for (table, name), sql in plan.pins.items():
        if _in_effect(specs.get(table), name, sql) is not None:
            reused.setdefault(table, []).append(name)
    dropped: dict[str, dict[str, str]] = {}
    for (table, name), reason in invalidated.items():
        dropped.setdefault(table, {})[name] = reason
    return PinOutcome(
        planned=len(plan.pins),
        reused={t: sorted(names) for t, names in sorted(reused.items())},
        invalidated=dropped,
        not_pinned=dict(plan.not_pinned),
    )


def pin_report(pipeline: object) -> dict[str, object]:
    """The output keys for the pins of a finished pipeline; empty when no pins were planned."""
    outcome = getattr(pipeline, "pin_outcome", None)
    return outcome.as_output() if isinstance(outcome, PinOutcome) else {}
