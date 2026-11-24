"""Selector measures → metric view parameters (config switch ``slicer_parameters``, default off).

A measure that reads a disconnected slicer selector (``SELECTEDVALUE(T[c])`` with no
relationship to the model) has no value of its own in a metric view: a view has no slicer.
With the switch on, the selector is treated as a constant of the view, and the measure is
translated like any other:

1. ``plan_view`` (before ``process_table``) rewrites each selector read to the placeholder
   literal of a string parameter (``slicer_parameter_text``), registers the parameter
   (default = the SELECTEDVALUE alternate, else the first option the DAX compares with) and
   hands the rewritten measure to ``process_table``: the rule translator and, when it is on,
   the DAX LLM. Those see the literal and the view's parameter lines in the table context.
2. ``finish_view`` (after ``process_table``) takes the result of each such measure:
   * a measure whose result is a choice on the parameter (``SWITCH``/``IF``) is rebuilt as
     ``CASE <param> WHEN 'A' THEN ... END`` over its branches, each translated by the
     translator (rule path). This replaces whatever ``process_table`` produced;
   * otherwise the translated SQL gets the bare parameter name in place of the literal;
   * a result that does not carry the parameter name where the selector was read is refused.

The per-option SWITCH decomposition (``switch_decomposition``) is not changed: the parameter
measure is an additional measure, named ``<measure>__selector`` only when a per-option
measure already has the plain name.

Deployed on a warehouse (DBR 18.2 or above; a view with parameters cannot be materialized).
The parameter measures are not compared with Power BI yet.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from src.services.powerbi.switch_decomposition import reaches_external_measure

from .data_classes import MetricViewSpec, TranslationResult
from .slicer_parameter_dax import (
    EXTERNAL_MEASURE,
    CaseShape,
    DispatchDecline,
    SelectorRef,
    expanded_body,
    parse_case,
    reads_query_grouping,
    rewrite_selectors,
    selector_refs,
)
from .slicer_branch import branch_sql
from .slicer_parameter_text import TOKEN_PREFIX, substitute, token_names
from .sql_measure_sanitizer import sanitize_measure_name
from .utils import to_snake_case

if TYPE_CHECKING:
    from .dax_translator import DaxTranslator

logger = logging.getLogger(__name__)

PBI_KIND = "slicer_parameter"
MEASURE_SUFFIX = "__selector"
_TRUE_STRINGS = frozenset({"true", "1", "yes", "on"})
# Option values are written into SQL literals; anything that could end a literal or a
# comment is refused rather than escaped.
_UNSAFE_VALUE = re.compile(r"[\\;\r\n]|--|/\*")
# A comparison of a parameter token with a text literal, either side.
_EQ_LITERAL = re.compile(
    rf'"{TOKEN_PREFIX}(\w+)"\s*=\s*"([^"]*)"|"([^"]*)"\s*=\s*"{TOKEN_PREFIX}(\w+)"'
)
_WHOLE_REF = re.compile(r"\[([^\]]+)\]")
_TABLE_REF = re.compile(r"(?:'([^']+)'|(\w+))\s*\[")
_SELECTOR_CALL = re.compile(r"\bSELECTEDVALUE\s*\([^()]*\)", re.IGNORECASE)

# Outcome per measure, best first: a measure seen in several views keeps the best.
_RANK = {"rule": 0, "translated": 1, "llm": 2, "declined": 3}


def slicer_parameters_enabled(config: Mapping[str, object] | None) -> bool:
    """Whether the ``slicer_parameters`` switch is on (default off)."""
    raw = (config or {}).get("slicer_parameters", False)
    if isinstance(raw, str):
        return raw.strip().lower() in _TRUE_STRINGS
    return bool(raw)


def connected_tables(relationships: Iterable[Mapping[str, object]]) -> set[str]:
    """Every table that a relationship names (either end)."""
    connected: set[str] = set()
    for rel in relationships:
        for key in ("from_table", "to_table"):
            if rel.get(key):
                connected.add(str(rel[key]))
    return connected


@dataclass
class SelectorParameter:
    """A string parameter of one view, read from one disconnected selector."""

    name: str
    selector: str  # "Table[column]"
    values: list[str] = field(
        default_factory=list
    )  # the literals the DAX compares with
    alternates: list[str] = field(default_factory=list)  # SELECTEDVALUE alternates

    @property
    def default(self) -> str | None:
        if self.alternates:
            return self.alternates[0]
        return self.values[0] if self.values else None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "data_type": "string",
            "default": self.default,
            "allowed_values": list(self.values),
            "selector": self.selector,
        }


@dataclass
class _Candidate:
    """A measure whose selector reads are rewritten to tokens."""

    name: str  # Power BI measure name
    rewritten: str  # DAX with the parameter tokens
    params: list[str]  # the parameter names whose token the DAX writes
    original: str = ""  # the Power BI DAX, as shown in the YAML comments
    shape: CaseShape | None = None
    shape_reason: str = ""


@dataclass
class ViewSlicerPlan:
    """What ``plan_view`` decided for one view, applied by ``finish_view``."""

    table_key: str
    llm_enabled: bool
    trivial_only: bool
    measure_rows: dict[
        str, dict
    ]  # every model measure by its name, for branch references
    connected: set[str] = field(default_factory=set)
    taken: set[str] = field(default_factory=set)  # names a parameter must not take
    params: dict[str, SelectorParameter] = field(
        default_factory=dict
    )  # by parameter name
    candidates: dict[str, _Candidate] = field(default_factory=dict)  # by Power BI name
    declined: dict[str, str] = field(
        default_factory=dict
    )  # by Power BI name: hard reasons
    process_rows: list[dict] = field(default_factory=list)

    def public_parameters(self) -> list[dict[str, object]]:
        """The parameters the LLM context names (those with a default, so usable)."""
        return [p.to_dict() for p in self.params.values() if p.default is not None]

    def register(self, ref: SelectorRef) -> str:
        """The parameter of a selector (created on first use); returns its name."""
        key = f"{ref.table}[{ref.column}]"
        param = next((p for p in self.params.values() if p.selector == key), None)
        if param is None:
            name = _param_name(ref.table, ref.column, self.taken | set(self.params))
            param = SelectorParameter(name=name, selector=key)
            self.params[name] = param
        if ref.alternate is not None and ref.alternate not in param.alternates:
            param.alternates.append(ref.alternate)
        return param.name

    def reference_dax(self, name: str) -> str:
        """The DAX of a referenced measure with its selectors written as parameter tokens.

        A measure that reads a selector is rewritten here, and its literals join the
        parameter's values. Raises ``DispatchDecline`` when it cannot be read as one."""
        row = self.measure_rows.get(name)
        if row is None:
            raise DispatchDecline(f"branch [{name}] is not a measure of this model")
        dax = _dax(row)
        if not selector_refs(dax):
            return dax
        reason = _hard_decline(dax, self.connected, {})
        if reason:
            raise DispatchDecline(f"branch [{name}] reads a selector: {reason}")
        rewritten = rewrite_selectors(dax, self.register)
        cand = _Candidate(name, rewritten, sorted(token_names(rewritten)))
        _collect_values(self, cand)
        unusable = [p for p in cand.params if self.params[p].default is None]
        if unusable:
            raise DispatchDecline(
                f"branch [{name}] reads a selector with no known option"
            )
        return rewritten


@dataclass
class SlicerReport:
    """Outcome per Power BI measure over the whole run: one of ``rule`` (the CASE of the
    rule path), ``translated`` (the translator's own SQL, LLM when it is on), ``llm`` (not
    translated by the rules, handed to the LLM path), ``declined`` (with the reason)."""

    best: dict[str, tuple[str, str]] = field(default_factory=dict)

    def record(self, name: str, outcome: str, reason: str = "") -> None:
        current = self.best.get(name)
        if current is None or _RANK[outcome] < _RANK[current[0]]:
            self.best[name] = (outcome, reason)

    def as_dict(self) -> dict[str, object]:
        counts = Counter(outcome for outcome, _ in self.best.values())
        reasons = Counter(
            f"{outcome}: {_group(reason)}"
            for outcome, reason in self.best.values()
            if outcome in ("llm", "declined") and reason
        )
        return {
            "candidates": len(self.best),
            "counts": dict(counts),
            "reasons": dict(reasons),
            "by_measure": {
                name: [outcome, reason[:160]]
                for name, (outcome, reason) in sorted(self.best.items())
            },
        }


def _group(reason: str) -> str:
    """A reason with its quoted names and bracketed references removed, to count kinds."""
    return re.sub(r'"[^"]*"|\[[^\]]*\]', "…", reason)[:140]


def _pbi_name(row: Mapping[str, object]) -> str:
    return str(row.get("original_name") or row.get("measure_name") or "")


def _dax(row: Mapping[str, object]) -> str:
    return str(row.get("dax_expression") or row.get("expression") or "")


def _selects_disconnected(dax: str, connected: set[str]) -> bool:
    return any(ref.table not in connected for ref in selector_refs(dax))


def _hard_decline(
    dax: str, connected: set[str], dax_by_name: Mapping[str, dict[str, str]]
) -> str:
    """The reason a candidate is never translated as a parameter ("" when it may be)."""
    if reads_query_grouping(dax):
        return "reads the query's grouping (ISFILTERED/HASONEVALUE/ISINSCOPE), not a selector value"
    if any(ref.table in connected for ref in selector_refs(dax)):
        return "a selector of this measure has a relationship to the model"
    if EXTERNAL_MEASURE.search(dax) or reaches_external_measure(dax, dax_by_name):
        return "reads an EXTERNALMEASURE"
    return ""


def _param_name(table: str, column: str, taken: set[str]) -> str:
    base = re.sub(r"[^a-z0-9_]", "_", to_snake_case(f"{table}_{column}")).strip("_")
    base = base or "selector"
    if base[0].isdigit():
        base = f"p_{base}"
    name, suffix = base, 2
    while name in taken:
        name = f"{base}_{suffix}"
        suffix += 1
    return name


def plan_view(
    table_key: str,
    rows: list[dict],
    all_rows: list[dict],
    *,
    connected: set[str],
    taken_names: set[str],
    llm_enabled: bool,
    trivial_only: bool,
) -> ViewSlicerPlan:
    """Rewrite the selector reads of the view's measures and register their parameters.

    ``rows`` are the view's measures as ``process_table`` receives them; the returned
    ``process_rows`` replace them. ``all_rows`` is the whole model (branch references).
    """
    measure_rows: dict[str, dict] = {}
    for row in all_rows:
        for key in ("measure_name", "original_name"):
            if row.get(key):
                measure_rows.setdefault(str(row[key]), row)
    dax_by_name = {
        name: {"expression": _dax(row)} for name, row in measure_rows.items()
    }
    plan = ViewSlicerPlan(
        table_key,
        llm_enabled,
        trivial_only,
        measure_rows,
        connected=connected,
        taken=set(taken_names),
    )
    dispositions: list[tuple[dict, _Candidate | None]] = []

    for row in rows:
        name = _pbi_name(row)
        dax = _dax(row)
        if not name or not _selects_disconnected(dax, connected):
            dispositions.append((row, None))
            continue
        reason = _hard_decline(dax, connected, dax_by_name)
        if reason:
            plan.declined[name] = reason
            dispositions.append((row, None))
            continue
        try:
            rewritten = rewrite_selectors(dax, plan.register)
        except DispatchDecline as exc:
            plan.declined[name] = str(exc)
            dispositions.append((row, None))
            continue
        dispositions.append(
            (
                row,
                _Candidate(
                    name, rewritten, sorted(token_names(rewritten)), original=dax
                ),
            )
        )

    candidates = [cand for _, cand in dispositions if cand is not None]
    for cand in candidates:
        _collect_values(plan, cand)
    for row, cand in dispositions:
        if cand is None:
            plan.process_rows.append(row)
            continue
        unusable = [p for p in cand.params if plan.params[p].default is None]
        if unusable:
            plan.declined[cand.name] = (
                "no text literal is compared with the selector and it has no text alternate "
                "(a numeric selector is not a text parameter)"
            )
            plan.process_rows.append(row)
            continue
        plan.candidates.setdefault(cand.name, cand)
        plan.process_rows.append({**row, "dax_expression": cand.rewritten})
    return plan


def _collect_values(plan: ViewSlicerPlan, cand: _Candidate) -> None:
    """The literals the rewritten DAX compares each parameter with, in order of appearance."""
    try:
        cand.shape = parse_case(cand.rewritten)
    except DispatchDecline as exc:
        cand.shape_reason = str(exc)
    if cand.shape is not None:
        if cand.shape.param not in plan.params:
            cand.shape = None
            cand.shape_reason = (
                "the choice reads a parameter this view does not register"
            )
        else:
            for value, _ in cand.shape.options:
                _add_value(plan.params[cand.shape.param], value)
    for match in _EQ_LITERAL.finditer(cand.rewritten):
        if match.group(1):
            _add_value(plan.params[match.group(1)], match.group(2))
        else:
            _add_value(plan.params[match.group(4)], match.group(3))


def _add_value(param: SelectorParameter, value: str) -> None:
    if value not in param.values:
        param.values.append(value)


def finish_view(
    plan: ViewSlicerPlan,
    spec: MetricViewSpec,
    translator: DaxTranslator,
    report: SlicerReport,
) -> None:
    """Apply the plan to the view ``process_table`` built: CASE rebuilds, bare parameter
    names, refusals, outcome per measure, and the parameters of the view."""
    switch_names = {
        m.measure_name for m in spec.measures if m.category == "switch_decomposition"
    }
    reserved = {str(d.get("name", "")) for d in spec.dimensions} | set(
        spec.fact_columns
    )
    for name, reason in plan.declined.items():
        # Their rows went through process_table unchanged: a translation they have stays.
        for entry in spec.untranslatable:
            if _is_entry(entry, name):
                entry.skip_reason = f"slicer parameter: {reason}"
        report.record(name, "declined", reason)
    used: set[str] = set()
    for name, cand in plan.candidates.items():
        clash = [p for p in cand.params if plan.params[p].name in reserved]
        if clash:
            reason = f"parameter {plan.params[clash[0]].name} has the name of a column or dimension"
            _mark_untranslatable(spec, name, f"slicer parameter: {reason}")
            report.record(name, "declined", reason)
            continue
        used |= _finish_candidate(plan, spec, cand, translator, report, switch_names)
    for param in plan.params.values():
        if param.name in used and param.default is not None:
            spec.parameters.append(param.to_dict())


def _finish_candidate(
    plan: ViewSlicerPlan,
    spec: MetricViewSpec,
    cand: _Candidate,
    translator: DaxTranslator,
    report: SlicerReport,
    switch_names: set[str],
) -> set[str]:
    """Settle one candidate; returns the parameter names its final translation reads."""
    rule_sql, rule_reason = _case_rule(plan, cand, translator)
    if rule_sql is not None:
        _remove_entries(spec, cand.name)
        name = _final_name(spec, cand.name, switch_names)
        spec.measures.append(
            TranslationResult(
                measure_name=name,
                original_name=cand.name,
                sql_expr=rule_sql,
                is_translatable=True,
                skip_reason=_comment(plan, cand, "a CASE over the parameter"),
                dax_expression=cand.original,
                confidence="medium",
                category="single_table",
                pbi_kind=PBI_KIND,
            )
        )
        report.record(cand.name, "rule", "CASE over the parameter")
        return {p for p in plan.params if _carries(rule_sql, p)}
    translated = [m for m in spec.measures if _is_entry(m, cand.name) and m.sql_expr]
    if translated:
        return _adopt_translations(plan, spec, cand, translated, switch_names, report)
    entries = [u for u in spec.untranslatable if _is_entry(u, cand.name)]
    if not entries:
        report.record(cand.name, "declined", "not emitted in this view")
        return set()
    detail = rule_reason or cand.shape_reason or "no rule covers the expression"
    handed = "LLM attempted" if plan.llm_enabled else "LLM not enabled: needs the LLM"
    for entry in entries:
        entry.dax_expression = cand.original or entry.dax_expression
        entry.skip_reason = f"slicer parameter, not translated by rules ({detail}); {handed}: {entry.skip_reason}"
    report.record(cand.name, "llm", detail)
    return set()


def _adopt_translations(
    plan: ViewSlicerPlan,
    spec: MetricViewSpec,
    cand: _Candidate,
    translated: list[TranslationResult],
    switch_names: set[str],
    report: SlicerReport,
) -> set[str]:
    """A translation from the translator (or the LLM): the bare names, or a refusal."""
    names = set(plan.params)
    for measure in translated:
        sql = substitute(measure.sql_expr, names) or ""
        dropped = [p for p in cand.params if not _carries(sql, p)]
        if dropped:
            _to_untranslatable(
                spec,
                measure,
                f"the translation drops the selector parameter {dropped[0]}",
            )
            report.record(
                cand.name, "declined", "the translation drops a selector parameter"
            )
            continue
        measure.sql_expr = sql
        measure.pbi_kind = PBI_KIND
        measure.skip_reason = measure.skip_reason or _comment(
            plan, cand, "the translator"
        )
        if measure.measure_name in switch_names:
            measure.measure_name = f"{measure.measure_name}{MEASURE_SUFFIX}"
        report.record(cand.name, "translated", "translated by the translator")
    return {
        p
        for measure in translated
        if measure.sql_expr
        for p in plan.params
        if _carries(measure.sql_expr, p)
    }


def _carries(sql: str, param: str) -> bool:
    return (
        param in token_names(sql)
        or re.search(rf"\b{re.escape(param)}\b", sql) is not None
    )


def _case_rule(
    plan: ViewSlicerPlan, cand: _Candidate, translator: DaxTranslator
) -> tuple[str | None, str]:
    """``(sql, "")`` for the CASE over the parameter, or ``(None, reason)``."""
    if cand.shape is None:
        return None, cand.shape_reason
    shape = cand.shape
    try:
        pieces = []
        for value, branch in shape.options:
            if _UNSAFE_VALUE.search(value):
                raise DispatchDecline(
                    f'option "{value}" cannot be written as a literal'
                )
            pieces.append(
                f"WHEN {_sql_string(value)} THEN {_branch_sql(plan, branch, translator)}"
            )
        if shape.otherwise is not None:
            pieces.append(f"ELSE {_branch_sql(plan, shape.otherwise, translator)}")
    except DispatchDecline as exc:
        return None, str(exc)
    sql = " ".join([f"CASE {shape.param}", *pieces, "END"])
    for param in cand.params:
        if not _carries(sql, param):
            return None, f"the selector {param} is not carried into the translation"
    return substitute(sql, set(plan.params)), ""


_MAX_NESTING = 8


def _branch_sql(
    plan: ViewSlicerPlan, branch: str, translator: DaxTranslator, depth: int = 0
) -> str:
    """The SQL of one branch: a measure reference (its own branch, recursively), a
    composition of references (``slicer_branch``), or the translator's SQL of the text.
    """
    if depth > _MAX_NESTING:
        raise DispatchDecline("a branch refers to measures too deeply")
    branch = expanded_body(branch)  # VARs inlined: a referenced measure often has them

    def resolve_ref(name: str) -> str:
        return _branch_sql(plan, plan.reference_dax(name), translator, depth + 1)

    def translate_text(text: str) -> str:
        if EXTERNAL_MEASURE.search(text):
            raise DispatchDecline("a branch reads an EXTERNALMEASURE")
        result = translator.translate(
            {
                "measure_name": "slicer_branch",
                "original_name": "slicer_branch",
                "dax_expression": text,
                "proposed_allocation": plan.table_key,
            },
            plan.table_key,
            trivial_only=plan.trivial_only,
        )
        if not result.is_translatable or not result.sql_expr:
            raise DispatchDecline(
                f"branch {text[:60]!r} is not translated by the rules: "
                f"{(result.skip_reason or '')[:80]}"
            )
        sql = str(result.sql_expr)
        lost = sorted(token_names(text) - token_names(sql))
        if lost:
            raise DispatchDecline(
                f"the translation of a branch drops the selector {lost[0]}"
            )
        return sql

    return branch_sql(branch, resolve_ref, translate_text)


def rehome_candidates(
    rows: list[dict], fact_keys: set[str], connected: set[str]
) -> dict[str, str]:
    """Move each selector measure held by a table with no view to the view it reads.

    The holder tables of a model (``Measures``, ``Calculation`` and the like) have no
    warehouse source, so a measure allocated there is in no view. The view is the first
    fact its DAX reads as ``Table[col]``, else the view of the first measure it references
    as ``[Name]``. Returns the moved measures and their new view."""
    by_name = {_pbi_name(row): row for row in rows if _pbi_name(row)}
    moved: dict[str, str] = {}
    for row in rows:
        name = _pbi_name(row)
        dax = _dax(row)
        if not name or not _selects_disconnected(dax, connected):
            continue
        if str(row.get("proposed_allocation") or "") in fact_keys:
            continue
        target = _fact_home(dax, by_name, fact_keys, {name})
        if target is not None:
            row["proposed_allocation"] = target
            moved[name] = target
    return moved


def _fact_home(
    dax: str,
    by_name: Mapping[str, dict],
    fact_keys: set[str],
    seen: set[str],
    depth: int = 0,
) -> str | None:
    """The view a DAX reads: its first fact table reference, else (up to four measure hops)
    the view of the first measure it references that has one."""
    others = False
    for match in _TABLE_REF.finditer(_SELECTOR_CALL.sub(" ", dax)):
        table = match.group(1) or match.group(2)
        if table in fact_keys:
            return table
        others = True
    # A measure that reads a table with no view of its own (a table the model has but the
    # warehouse does not) is not moved to another measure's view: its columns are not there.
    if others or depth >= 4:
        return None
    for ref in _WHOLE_REF.finditer(dax):
        name = ref.group(1)
        row = by_name.get(name)
        if row is None or name in seen:
            continue
        seen.add(name)
        home = str(row.get("proposed_allocation") or "")
        if home in fact_keys:
            return home
        found = _fact_home(_dax(row), by_name, fact_keys, seen, depth + 1)
        if found is not None:
            return found
    return None


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _comment(plan: ViewSlicerPlan, cand: _Candidate, how: str) -> str:
    parts = []
    for param_name in cand.params:
        param = plan.params[param_name]
        options = ", ".join(f"'{v}'" for v in param.values)
        parts.append(
            f"{param.name} ({param.selector}): options {options}, default '{param.default}'"
        )
    return f"Slicer parameter, read by {how}: " + "; ".join(parts)


def _is_entry(measure: TranslationResult, name: str) -> bool:
    return measure.original_name == name and measure.category != "switch_decomposition"


def _remove_entries(spec: MetricViewSpec, name: str) -> None:
    spec.measures[:] = [m for m in spec.measures if not _is_entry(m, name)]
    spec.untranslatable[:] = [u for u in spec.untranslatable if not _is_entry(u, name)]


def _final_name(spec: MetricViewSpec, original: str, switch_names: set[str]) -> str:
    """The plain measure name, or ``<name>__selector`` when a per-option measure has it."""
    base = sanitize_measure_name(to_snake_case(original))
    taken = {m.measure_name for m in spec.measures} | {
        u.measure_name for u in spec.untranslatable
    }
    name = base
    if name in switch_names or name in taken:
        name = f"{base}{MEASURE_SUFFIX}"
    return name


def _mark_untranslatable(spec: MetricViewSpec, name: str, reason: str) -> None:
    """Annotate the measure's entries with ``reason``; translated entries are moved out."""
    for measure in [m for m in spec.measures if _is_entry(m, name) and m.sql_expr]:
        _to_untranslatable(spec, measure, reason)
    for entry in spec.untranslatable:
        if _is_entry(entry, name):
            entry.skip_reason = reason


def _to_untranslatable(
    spec: MetricViewSpec, measure: TranslationResult, reason: str
) -> None:
    if measure in spec.measures:
        spec.measures.remove(measure)
    measure.is_translatable = False
    measure.sql_expr = None
    measure.skip_reason = reason
    spec.untranslatable.append(measure)


def unplaced_candidates(
    all_rows: list[dict], connected: set[str], placed: set[str]
) -> list[str]:
    """Power BI names of the measures that read a disconnected selector but no view carried."""
    names = []
    for row in all_rows:
        name = _pbi_name(row)
        if name and name not in placed and _selects_disconnected(_dax(row), connected):
            names.append(name)
    return sorted(set(names))
