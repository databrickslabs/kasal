"""Translate ONLY the drifted measures, reusing the UC Metric View Generator.

The generator is the one place that knows how to place a measure on its fact, resolve
physical columns from the M source, translate DAX (regex first, LLM fallback) and emit
UC-valid YAML. Re-implementing any of that here would fork the transpiler, so instead
the generator is run in JSON mode on a SUBSET of today's extraction:

  * the target measures (new / changed / possibly changed), plus
  * every measure they reference (transitively) — measure-to-measure references are
    resolved from the measures list, so dropping a dependency would break the
    translation of the measure that uses it.

Only the target measures are taken from the result; dependencies are context. Each
target yields the measure's full YAML entry and any join it needs that the baseline
does not have yet.

The generator is injected (``generate_fn(measures_subset) -> output dict``) so this
module is pure and testable.
"""

import re
from dataclasses import dataclass, field
from typing import Callable, Optional

import yaml

from src.services.tools.metric_view_utils.drift.fingerprint import extract_pbi_name
from src.services.tools.metric_view_utils.drift.matcher import PbiMeasure
from src.services.tools.metric_view_utils.utils import to_snake_case

GenerateFn = Callable[[list[dict]], dict]

_MEASURE_REF_RE = re.compile(r"\[([^\[\]]+)\]")
_ALIAS_RE = re.compile(r"\b([A-Za-z_]\w*)\.[A-Za-z_`]")


@dataclass
class Candidate:
    original_name: str
    table_key: str
    entry: dict  # the measure's YAML mapping, as the generator emitted it
    joins: list[dict] = field(default_factory=list)  # every join of that fresh view


@dataclass
class CandidateSet:
    by_name: dict[str, list[Candidate]]  # original_name -> one per fresh view
    untranslated: dict[str, str]  # original_name -> reason
    generator_warning: Optional[str] = None


def dependency_closure(
    targets: list[PbiMeasure], all_measures: list[PbiMeasure]
) -> list[PbiMeasure]:
    """Targets plus every measure their DAX references, transitively."""
    by_lower = {m.original_name.lower(): m for m in all_measures}
    out: dict[str, PbiMeasure] = {}
    stack = list(targets)
    while stack:
        m = stack.pop()
        key = m.original_name.lower()
        if key in out:
            continue
        out[key] = m
        for ref in _MEASURE_REF_RE.findall(m.dax or ""):
            dep = by_lower.get(ref.strip().lower())
            if dep and dep.original_name.lower() not in out:
                stack.append(dep)
    return list(out.values())


def referenced_aliases(expr: str) -> set[str]:
    """Join aliases a SQL expression uses (``alias.column``), excluding ``source``."""
    return {a for a in _ALIAS_RE.findall(expr or "") if a.lower() != "source"}


def build_candidates(
    targets: list[PbiMeasure],
    all_measures: list[PbiMeasure],
    generate_fn: GenerateFn,
) -> CandidateSet:
    if not targets:
        return CandidateSet({}, {})
    subset = dependency_closure(targets, all_measures)
    output = generate_fn([m.raw for m in subset]) or {}
    target_names = {t.original_name for t in targets}

    by_name: dict[str, list[Candidate]] = {}
    for table_key, text in (output.get("yaml") or {}).items():
        if table_key == "none_allocated_measures" or not isinstance(text, str):
            continue
        try:
            spec = yaml.safe_load(text)
        except yaml.YAMLError:
            continue
        if not isinstance(spec, dict):
            continue
        names = _name_to_original(output, table_key)
        joins = [j for j in (spec.get("joins") or []) if isinstance(j, dict)]
        for entry in spec.get("measures") or []:
            if not isinstance(entry, dict) or not entry.get("name"):
                continue
            original = (
                names.get(entry["name"])
                or extract_pbi_name(str(entry.get("comment") or ""))
                or _match_snake(entry["name"], target_names)
            )
            if original and original in target_names:
                by_name.setdefault(original, []).append(
                    Candidate(original, table_key, entry, joins)
                )

    untranslated: dict[str, str] = {}
    reasons = _untranslated_reasons(output)
    for t in targets:
        if t.original_name not in by_name:
            untranslated[t.original_name] = reasons.get(
                t.original_name.lower(),
                "the generator produced no SQL for this measure",
            )
    warning = output.get("llm_translation_warning") or output.get("error")
    return CandidateSet(by_name, untranslated, str(warning) if warning else None)


def _name_to_original(output: dict, table_key: str) -> dict[str, str]:
    rows = (output.get("resolved_measures_by_table") or {}).get(table_key) or []
    return {
        str(r["measure_name"]): str(r["original_name"])
        for r in rows
        if isinstance(r, dict) and r.get("measure_name") and r.get("original_name")
    }


def _match_snake(name: str, originals: set[str]) -> Optional[str]:
    for o in originals:
        if to_snake_case(o) == name:
            return o
    return None


def _untranslated_reasons(output: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in output.get("untranslatable_items") or []:
        if not isinstance(item, dict):
            continue
        name = item.get("original_name") or item.get("measure_name")
        reason = item.get("skip_reason") or item.get("reason") or item.get("category")
        if name and reason:
            out[str(name).lower()] = str(reason)
    return out


def pick_candidate(
    candidates: list[Candidate], allocations: list[str], baseline_aliases: set[str]
) -> Optional[Candidate]:
    """The fresh-view translation that best fits the target baseline view.

    Prefer the fact the measure is allocated to (in allocation order); among the rest,
    the one that needs the fewest joins the baseline does not already have.
    """
    if not candidates:
        return None
    rank = {to_snake_case(a): i for i, a in enumerate(allocations)}

    def _key(c: Candidate) -> tuple:
        missing = referenced_aliases(str(c.entry.get("expr") or "")) - baseline_aliases
        return (rank.get(to_snake_case(c.table_key), len(rank)), len(missing))

    return sorted(candidates, key=_key)[0]
