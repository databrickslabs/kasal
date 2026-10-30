"""Match today's Power BI measures to the measures of the deployed metric views.

Matching keys, strongest first:
  1. the ``PBI: <original name>`` tag the generator writes into a measure comment
  2. the measure's YAML ``name`` against ``to_snake_case(<PBI name>)`` — the rule the
     generator itself uses to name measures (so it also covers measures whose PBI
     name was already snake_case and therefore carry no tag)
  3. ``display_name`` against the PBI name

A PBI measure allocated to several fact tables legitimately matches several views.

Which view a NEW measure belongs to is decided by allocation voting: every matched
measure says "fact table <T> (per today's allocation) lives in view <V>". A new
measure allocated to <T> goes to the view <T> voted for. When a fact has no matched
measures yet, the view whose name or source table equals the fact's snake name is
used. Anything still unresolved is reported as unassigned — never guessed.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Optional

from src.services.tools.metric_view_utils.drift.baseline import BaselineView
from src.services.tools.metric_view_utils.drift.fingerprint import (
    extract_fingerprint,
    extract_pbi_name,
)
from src.services.tools.metric_view_utils.utils import to_snake_case


@dataclass
class BaselineMeasure:
    view: str  # full view name
    index: int  # position in the view's measures list
    name: str
    expr: str
    comment: str
    display_name: str
    pbi_tag: Optional[str]
    fingerprint: Optional[str]


@dataclass
class PbiMeasure:
    original_name: str
    measure_name: str
    dax: str
    allocations: list[str]  # fact tables, primary first
    raw: dict = field(default_factory=dict)


@dataclass
class MatchResult:
    # original_name -> baseline measures it matched (one per view)
    matched: dict[str, list[BaselineMeasure]]
    unmatched_pbi: list[PbiMeasure]
    # baseline measures no PBI measure claimed
    unclaimed_baseline: list[BaselineMeasure]
    # fact table -> full view name
    fact_to_view: dict[str, str]


def index_baseline(view: BaselineView) -> list[BaselineMeasure]:
    out: list[BaselineMeasure] = []
    for i, m in enumerate(view.spec.get("measures") or []):
        if not isinstance(m, dict) or not m.get("name"):
            continue
        comment = str(m.get("comment") or "")
        out.append(
            BaselineMeasure(
                view=view.full_name,
                index=i,
                name=str(m["name"]),
                expr=str(m.get("expr") or ""),
                comment=comment,
                display_name=str(m.get("display_name") or ""),
                pbi_tag=extract_pbi_name(comment),
                fingerprint=extract_fingerprint(comment),
            )
        )
    return out


def to_pbi_measures(measures_json: list) -> list[PbiMeasure]:
    """Config-gen ``measures_json`` → PbiMeasure, dropping rows without a name."""
    out: list[PbiMeasure] = []
    seen: set[str] = set()
    for m in measures_json or []:
        if not isinstance(m, dict):
            continue
        original = str(m.get("original_name") or m.get("measure_name") or "").strip()
        if not original or original.lower() in seen:
            continue
        seen.add(original.lower())
        allocs = [
            str(a.get("table"))
            for a in (m.get("all_allocations") or [])
            if isinstance(a, dict) and a.get("table")
        ]
        primary = m.get("proposed_allocation")
        if primary and primary != "__unassigned__" and primary not in allocs:
            allocs.insert(0, str(primary))
        out.append(
            PbiMeasure(
                original_name=original,
                measure_name=str(m.get("measure_name") or original),
                dax=str(m.get("dax_expression") or m.get("expression") or ""),
                allocations=allocs,
                raw=m,
            )
        )
    return out


def match_measures(views: list[BaselineView], pbi: list[PbiMeasure]) -> MatchResult:
    baseline: list[BaselineMeasure] = []
    for v in views:
        if v.ok:
            baseline.extend(index_baseline(v))

    by_tag: dict[str, list[BaselineMeasure]] = defaultdict(list)
    by_name: dict[str, list[BaselineMeasure]] = defaultdict(list)
    by_display: dict[str, list[BaselineMeasure]] = defaultdict(list)
    for b in baseline:
        if b.pbi_tag:
            by_tag[b.pbi_tag.lower()].append(b)
        by_name[b.name.lower()].append(b)
        if b.display_name:
            by_display[b.display_name.lower()].append(b)

    claimed: set[tuple[str, int]] = set()
    matched: dict[str, list[BaselineMeasure]] = {}
    unmatched: list[PbiMeasure] = []
    for p in pbi:
        hits: dict[str, BaselineMeasure] = {}
        for pool, key in (
            (by_tag, p.original_name.lower()),
            (by_name, to_snake_case(p.original_name)),
            (by_name, p.measure_name.lower()),
            (by_display, p.original_name.lower()),
        ):
            for b in pool.get(key, []):
                # One hit per view; the strongest key wins. A measure another PBI
                # measure already claimed by tag is never re-claimed by name.
                if b.view in hits or (b.view, b.index) in claimed:
                    continue
                hits[b.view] = b
        if hits:
            matched[p.original_name] = list(hits.values())
            claimed.update((b.view, b.index) for b in hits.values())
        else:
            unmatched.append(p)

    unclaimed = [b for b in baseline if (b.view, b.index) not in claimed]
    fact_to_view = _vote_fact_views(views, pbi, matched)
    return MatchResult(matched, unmatched, unclaimed, fact_to_view)


def _vote_fact_views(
    views: list[BaselineView],
    pbi: list[PbiMeasure],
    matched: dict[str, list[BaselineMeasure]],
) -> dict[str, str]:
    votes: dict[str, Counter] = defaultdict(Counter)
    for p in pbi:
        for b in matched.get(p.original_name, []):
            for fact in p.allocations:
                votes[fact][b.view] += 1
    fact_to_view = {fact: c.most_common(1)[0][0] for fact, c in votes.items() if c}

    # Facts no matched measure voted for: fall back to name / source equality.
    all_facts = {f for p in pbi for f in p.allocations}
    for fact in all_facts - set(fact_to_view):
        snake = to_snake_case(fact)
        for v in views:
            if not v.ok:
                continue
            source = str(v.spec.get("source") or "")
            source_tail = (
                to_snake_case(source.split(".")[-1].strip("`")) if source else ""
            )
            if snake and snake in (to_snake_case(v.name), source_tail):
                fact_to_view[fact] = v.full_name
                break
    return fact_to_view
