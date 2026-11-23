"""Power BI selector measures → metric view parameters (config switch ``slicer_parameters``).

A measure that dispatches on a disconnected selector (``SWITCH(SELECTEDVALUE(T[c]), ...)``)
has no value of its own in a metric view: the view has no slicer. With the switch on,
each such measure gets a second, parameterised measure ``<name>__selector`` in the view
of the fact its branches read. The selector becomes a string parameter of the view and the
measure a ``CASE`` over that parameter, whose branches are the translated branch
expressions. The per-option measures of the SWITCH decomposition are left as they are.

Runs as a post-pass over the built specs (like ``prior_year.add_windows``). With the
switch off it returns at once and nothing changes. The DAX shape is in
``slicer_parameter_dax``; the YAML is in ``slicer_parameter_yaml``.

Not verified against a live warehouse: the grammar follows the metric view documentation
for parameters (DBR 18.2 or above, no materialization of a view with parameters).
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from src.services.powerbi.switch_decomposition import (
    _resolve_switch_branch,
    reaches_external_measure,
)

from .data_classes import MetricViewSpec, TranslationResult
from .slicer_parameter_dax import (
    DispatchDecline,
    SelectorDispatch,
    SelectorRef,
    parse_selector_dispatch,
    reads_query_grouping,
    selector_refs,
)
from .sql_measure_sanitizer import sanitize_measure_name
from .utils import to_snake_case

logger = logging.getLogger(__name__)

PBI_KIND = "slicer_parameter"
MEASURE_SUFFIX = "__selector"
_TRUE_STRINGS = frozenset({"true", "1", "yes", "on"})
# Option values are written into SQL literals and comments; anything that could end a
# literal or a comment is refused rather than escaped.
_UNSAFE_VALUE = re.compile(r"[\\;\r\n]|--|/\*")


def slicer_parameters_enabled(config: Mapping[str, object] | None) -> bool:
    """Whether the ``slicer_parameters`` switch is on (default off)."""
    raw = (config or {}).get("slicer_parameters", False)
    if isinstance(raw, str):
        return raw.strip().lower() in _TRUE_STRINGS
    return bool(raw)


@dataclass
class SlicerReport:
    """What the pass did, for the run's limitations: counts by reason and the translated
    measures. ``candidates`` counts measures with a disconnected SELECTEDVALUE."""

    candidates: int = 0
    translated: list[dict] = field(default_factory=list)
    declined: Counter = field(default_factory=Counter)  # grouped by reason kind
    declined_measures: list[dict] = field(default_factory=list)  # {measure, reason}

    def as_dict(self) -> dict:
        return {
            "candidates": self.candidates,
            "translated_count": len(self.translated),
            "translated": self.translated,
            "declined": dict(self.declined),
            "declined_measures": self.declined_measures,
        }


def _connected_tables(relationships: Iterable[Mapping[str, object]]) -> set[str]:
    connected: set[str] = set()
    for rel in relationships:
        for key in ("from_table", "to_table"):
            if rel.get(key):
                connected.add(str(rel[key]))
    return connected


def _normalised_rows(mapping: list[dict]) -> list[dict]:
    """Mapping rows as the DAX resolver reads them (``expression``, ``table_name``)."""
    rows = []
    for row in mapping:
        name = row.get("measure_name") or ""
        if not name:
            continue
        rows.append(
            {
                "measure_name": name,
                "expression": row.get("dax_expression") or row.get("expression") or "",
                "table_name": row.get("proposed_allocation")
                or row.get("table_name")
                or "",
            }
        )
    return rows


def add_slicer_parameters(
    specs: dict[str, MetricViewSpec],
    mapping: list[dict],
    config: Mapping[str, object] | None,
    *,
    relationships: list[dict],
) -> dict:
    """Attach the parameter measures to ``specs``. Returns the report (empty when off)."""
    if not slicer_parameters_enabled(config):
        return {}
    connected = _connected_tables(relationships)
    report = SlicerReport()
    if not connected:
        # Without the relationships every table looks disconnected; refuse to guess.
        report.declined["no relationships supplied"] += 1
        return report.as_dict()
    rows = _normalised_rows(mapping)
    measure_by_name = {row["measure_name"]: row for row in rows}
    fact_tables = set(specs)
    seen: set[str] = set()
    for row in mapping:
        name = row.get("original_name") or row.get("measure_name") or ""
        dax = row.get("dax_expression") or row.get("expression") or ""
        if not name or name in seen:
            continue
        if not any(ref.table not in connected for ref in selector_refs(dax)):
            continue
        seen.add(name)
        report.candidates += 1
        try:
            _attach_one(
                specs, name, dax, measure_by_name, fact_tables, connected, report
            )
        except DispatchDecline as exc:
            report.declined[_reason_key(str(exc))] += 1
            report.declined_measures.append({"measure": name, "reason": str(exc)})
            logger.info("[SlicerParameters] %s not translated: %s", name, exc)
    return report.as_dict()


def _reason_key(reason: str) -> str:
    """Group reasons by their kind; the branch name is not part of the grouping."""
    return re.sub(r'"[^"]*"', '"…"', reason)[:120]


def _attach_one(
    specs: dict[str, MetricViewSpec],
    name: str,
    dax: str,
    measure_by_name: dict[str, dict],
    fact_tables: set[str],
    connected: set[str],
    report: SlicerReport,
) -> None:
    if reads_query_grouping(dax):
        raise DispatchDecline(
            "reads the query's grouping (ISFILTERED/HASONEVALUE/ISINSCOPE), not a selector"
        )
    if reaches_external_measure(dax, measure_by_name):
        raise DispatchDecline("reads an EXTERNALMEASURE")
    dispatch = parse_selector_dispatch(dax)
    if dispatch.selector.table in connected:
        raise DispatchDecline("the selector table is connected to the model")
    branches = _translate_branches(dispatch, measure_by_name, fact_tables)
    target = branches.table
    spec = specs.get(target)
    if spec is None:
        raise DispatchDecline(f"branches read {target!r}, which has no metric view")
    _check_not_already_translated(spec, name)
    param = _new_parameter(spec, dispatch.selector, dispatch)
    case_sql, measure_refs = _case_expression(param["name"], branches, spec)
    measure = _new_measure(spec, name, dax, case_sql, param)
    _record_measure(spec, name, measure, param)
    report.translated.append(
        {
            "view": target,
            "measure": measure.measure_name,
            "parameter": param["name"],
            "allowed_values": param["allowed_values"],
            "default": param["default"],
            "branch_measure_refs": measure_refs,
        }
    )


@dataclass(frozen=True)
class _ResolvedBranch:
    """One branch as SQL, the table it reads, and the measure it names when it is one."""

    sql: str
    table: str
    measure_ref: str | None  # sanitized measure name when the branch is one measure


@dataclass
class _Branches:
    table: str
    options: list[tuple[str, _ResolvedBranch]]
    otherwise: _ResolvedBranch | None


def _translate_branches(
    dispatch: SelectorDispatch,
    measure_by_name: dict[str, dict],
    fact_tables: set[str],
) -> _Branches:
    """Each option (and the otherwise branch) as SQL; all must read one fact table."""
    tables: set[str] = set()
    options: list[tuple[str, _ResolvedBranch]] = []
    for option in dispatch.options:
        branch = _branch(
            option.result, f'option "{option.value}"', measure_by_name, fact_tables
        )
        options.append((option.value, branch))
        tables.add(branch.table)
    otherwise = None
    if dispatch.otherwise is not None:
        otherwise = _branch(
            dispatch.otherwise, "the otherwise branch", measure_by_name, fact_tables
        )
        tables.add(otherwise.table)
    if len(tables) != 1:
        raise DispatchDecline(f"branches read different tables: {sorted(tables)}")
    return _Branches(next(iter(tables)), options, otherwise)


def _branch(
    result: str, label: str, measure_by_name: dict[str, dict], fact_tables: set[str]
) -> _ResolvedBranch:
    resolved = _resolve_switch_branch(result.strip(), measure_by_name, fact_tables)
    if resolved is None:
        raise DispatchDecline(
            f"{label} is not a measure reference or a two-measure composite: "
            f"{result.strip()[:80]}"
        )
    ref = None
    if resolved["kind"] == "direct":
        ref = sanitize_measure_name(to_snake_case(resolved["sources"][0]))
    return _ResolvedBranch(resolved["sql"], resolved["table"], ref)


def _check_not_already_translated(spec: MetricViewSpec, name: str) -> None:
    for measure in spec.measures:
        if measure.original_name == name and measure.category != "switch_decomposition":
            raise DispatchDecline(
                "the view already carries a translation of this measure"
            )


def _new_parameter(
    spec: MetricViewSpec, selector: SelectorRef, dispatch: SelectorDispatch
) -> dict:
    taken = _taken_names(spec)
    base = re.sub(
        r"[^a-z0-9_]", "_", to_snake_case(f"{selector.table}_{selector.column}")
    )
    base = base.strip("_") or "selector"
    if base[0].isdigit():
        base = f"p_{base}"
    name = base
    suffix = 2
    while name in taken:
        name = f"{base}_{suffix}"
        suffix += 1
    values = [option.value for option in dispatch.options]
    default = selector.alternate if selector.alternate is not None else values[0]
    return {
        "name": name,
        "data_type": "string",
        "default": default,
        "allowed_values": values,
        "selector": f"{selector.table}[{selector.column}]",
    }


def _taken_names(spec: MetricViewSpec) -> set[str]:
    names = {m.measure_name for m in spec.measures}
    names |= {m.measure_name for m in spec.untranslatable}
    names |= {str(d.get("name", "")) for d in spec.dimensions}
    names |= set(spec.fact_columns)
    names |= {str(p.get("name", "")) for p in spec.parameters}
    return names


def _case_expression(
    param: str, branches: _Branches, spec: MetricViewSpec
) -> tuple[str, int]:
    """``CASE param WHEN 'A' THEN ... END``. Returns the SQL and how many branches became
    ``MEASURE(...)`` references to a measure of this view."""
    view_measures = {m.measure_name for m in spec.measures if m.sql_expr}
    refs = 0

    def render(branch: _ResolvedBranch) -> str:
        nonlocal refs
        if branch.measure_ref and branch.measure_ref in view_measures:
            refs += 1
            return f"MEASURE({branch.measure_ref})"
        return branch.sql

    parts = [f"CASE {param}"]
    for value, branch in branches.options:
        if _UNSAFE_VALUE.search(value):
            raise DispatchDecline(
                f'option value "{value}" cannot be written as a literal'
            )
        parts.append(f"WHEN {_sql_string(value)} THEN {render(branch)}")
    if branches.otherwise is not None:
        parts.append(f"ELSE {render(branches.otherwise)}")
    parts.append("END")
    return " ".join(parts), refs


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _new_measure(
    spec: MetricViewSpec,
    name: str,
    dax: str,
    case_sql: str,
    param: dict,
) -> TranslationResult:
    base = sanitize_measure_name(to_snake_case(name))
    taken = {m.measure_name for m in spec.measures} | {
        m.measure_name for m in spec.untranslatable
    }
    measure_name = f"{base}{MEASURE_SUFFIX}"
    suffix = 2
    while measure_name in taken:
        measure_name = f"{base}{MEASURE_SUFFIX}_{suffix}"
        suffix += 1
    options = ", ".join(f"'{v}'" for v in param["allowed_values"])
    comment = (
        f"Slicer parameter {param['name']} ({param['selector']}): options {options}, "
        f"default '{param['default']}'. The per-option measures carry each option."
    )
    return TranslationResult(
        measure_name=measure_name,
        original_name=name,
        sql_expr=case_sql,
        is_translatable=True,
        skip_reason=comment,
        dax_expression=dax,
        confidence="medium",
        category="switch_decomposition",
        pbi_kind=PBI_KIND,
        pbi_sources=[],
        pbi_operator=None,
    )


def _record_measure(
    spec: MetricViewSpec, name: str, measure: TranslationResult, param: dict
) -> None:
    spec.measures.append(measure)
    spec.parameters.append(param)
    # The PBI measure is translated now: it is no longer an untranslatable item of this view.
    spec.untranslatable[:] = [u for u in spec.untranslatable if u.original_name != name]
