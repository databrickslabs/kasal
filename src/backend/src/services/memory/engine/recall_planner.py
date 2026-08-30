"""Deep recall — the LLM-assisted half of ``Memory.recall``, driven by the
Memory Tuning knobs (Configuration > Memory).

Shallow recall is one vector search with the caller's query. Two things make
it miss what the store plainly holds:

* The query is a task description — 200+ chars of instructions ("Search for
  and gather the latest news stories from Switzerland published today.
  Identify the top 5-7 most significant stories ...") — while the record is
  the report that task produced. Measured on the local embedder: 0.72 cosine,
  under every floor. A distilled query ("latest Switzerland news today") is
  what the store was written for. ``query_analysis_threshold`` (chars) decides
  when that one distillation call runs.
* The store holds the answer under another phrasing or as a sub-topic.
  ``exploration_budget`` rounds of alternative queries find it — tried only
  when the shallow result is not convincing (``confidence_threshold_low`` /
  ``confidence_threshold_high``) or the query asks for several things at once
  (``complex_query_threshold``).

Every LLM call here is optional and best-effort: no LLM, an empty store, a
malformed reply — each degrades to the shallow search, never to a failed
recall. The plain search is always run too, so analysis can only ADD hits.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .analyze import extract_json_object
from .types import MemoryRecord

logger = logging.getLogger(__name__)

SearchFn = Callable[[str], list[MemoryRecord]]

# How much of a long query the analysis call sees, and how many alternative
# queries one exploration round may try.
QUERY_CHAR_CAP = 4000
MAX_ALTERNATIVES = 3
_SNIPPET_CHARS = 200

_ANALYSIS_SYSTEM_PROMPT = (
    "You turn an AI agent's task or question into search queries for its "
    "long-term memory store. The store holds short notes: task outputs, facts "
    "the user stated, earlier answers. Reply with ONLY a JSON object:\n"
    '{"query": "<ONE short search query, at most 15 words, keeping the '
    'concrete topic, entities, places and dates>",\n'
    ' "alternatives": ["<up to %d different short queries: another phrasing, '
    'a sub-topic, or the likely title of a stored note>"],\n'
    ' "complexity": <0.0-1.0: how many distinct things the task asks for; '
    "0.1 for one plain question, 0.9 for a multi-part brief>}"
) % MAX_ALTERNATIVES

_ALTERNATIVES_SYSTEM_PROMPT = (
    "An AI agent searched its long-term memory and is not confident it found "
    "what it needs. Propose up to %d NEW short search queries (at most 15 "
    "words each) that could reach notes the tried queries missed: other "
    "phrasings, narrower sub-topics, the likely title of a stored note. Do "
    "not repeat a tried query. Reply with ONLY a JSON object: "
    '{"alternatives": ["...", "..."]}'
) % MAX_ALTERNATIVES


@dataclass(frozen=True)
class RecallPlan:
    """What the analysis call decided; ``analyzed`` False means it never ran."""

    query: str
    alternatives: list[str] = field(default_factory=list)
    complexity: float = 0.0
    analyzed: bool = False


@dataclass
class RecallOutcome:
    records: list[MemoryRecord]
    plan: RecallPlan
    rounds: int = 0
    best_score: float | None = None


def _today_line() -> str:
    """The date, for a prompt that has to write dates.

    The planner wrote "Lebanon news today 2025" in 2026: without this it uses
    the year it remembers, and a memory query carrying the wrong year misses
    the notes it was written to find. Same wording as the agents' own date
    block, kept short.
    """
    today = datetime.now(timezone.utc)
    return (
        f"\nToday is {today:%Y-%m-%d}. This is later than your training data: "
        f"when a query needs a year, use {today.year} or omit the year — never "
        "a year you recall."
    )


def _llm_call(llm: Any, system: str, user: str) -> Any:
    call = getattr(llm, "call", None)
    if not callable(call):
        return None
    return call(
        [
            {"role": "system", "content": system + _today_line()},
            {"role": "user", "content": user},
        ]
    )


def _clean_queries(values: Any, *, exclude: set[str], cap: int) -> list[str]:
    out: list[str] = []
    if not isinstance(values, list):
        return out
    for value in values:
        text = " ".join(str(value or "").split())
        key = text.lower()
        if text and key not in exclude and key not in {q.lower() for q in out}:
            out.append(text)
        if len(out) >= cap:
            break
    return out


def analyze_query(llm: Any, query: str) -> RecallPlan:
    """Distil ``query`` into a search query plus alternatives. Never raises."""
    fallback = RecallPlan(query=query)
    try:
        raw = _llm_call(llm, _ANALYSIS_SYSTEM_PROMPT, query[:QUERY_CHAR_CAP])
        payload = extract_json_object(str(raw or ""))
        if not isinstance(payload, dict):
            logger.debug("memory query analysis returned no JSON: %.200r", raw)
            return fallback
        distilled = " ".join(str(payload.get("query") or "").split())
        try:
            complexity = min(1.0, max(0.0, float(payload.get("complexity") or 0.0)))
        except (TypeError, ValueError):
            complexity = 0.0
        exclude = {query.lower(), distilled.lower()}
        alternatives = _clean_queries(
            payload.get("alternatives"), exclude=exclude, cap=MAX_ALTERNATIVES
        )
        return RecallPlan(
            query=distilled or query,
            alternatives=alternatives,
            complexity=complexity,
            analyzed=True,
        )
    except Exception:  # noqa: BLE001 — analysis must never break a recall
        logger.warning("memory query analysis failed; searching as-is", exc_info=True)
        return fallback


def propose_alternatives(
    llm: Any, query: str, tried: set[str], found: list[MemoryRecord]
) -> list[str]:
    """Ask for new queries given what was tried and what (little) was found."""
    snippets = [
        " ".join(str(getattr(r, "content", "") or "").split())[:_SNIPPET_CHARS]
        for r in found[:3]
    ]
    user = (
        f"Task:\n{query[:QUERY_CHAR_CAP]}\n\nQueries already tried:\n"
        + "\n".join(f"- {q}" for q in sorted(tried))
        + "\n\nBest notes found so far:\n"
        + ("\n".join(f"- {s}" for s in snippets) if snippets else "- (nothing)")
    )
    try:
        raw = _llm_call(llm, _ALTERNATIVES_SYSTEM_PROMPT, user)
        payload = extract_json_object(str(raw or ""))
        if not isinstance(payload, dict):
            return []
        return _clean_queries(
            payload.get("alternatives"), exclude=tried, cap=MAX_ALTERNATIVES
        )
    except Exception:  # noqa: BLE001
        logger.warning("memory exploration proposal failed", exc_info=True)
        return []


def score_of(record: MemoryRecord) -> float | None:
    """The blended score the storage stamped on a hit (advisory metadata)."""
    value = (getattr(record, "metadata", None) or {}).get("similarity")
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def merge_hits(into: list[MemoryRecord], new: list[MemoryRecord]) -> list[MemoryRecord]:
    """Union by id, keeping the copy with the higher score."""
    by_id: dict[str, MemoryRecord] = {}
    merged: list[MemoryRecord] = []
    for record in [*into, *new]:
        rid = getattr(record, "id", None)
        if not rid:
            merged.append(record)
            continue
        current = by_id.get(rid)
        if current is None:
            by_id[rid] = record
            merged.append(record)
        elif (score_of(record) or 0.0) > (score_of(current) or 0.0):
            merged[merged.index(current)] = record
            by_id[rid] = record
    return merged


def rank(records: list[MemoryRecord], limit: int) -> list[MemoryRecord]:
    """Highest score first; unscored hits keep their order at the end."""
    ordered = sorted(
        records, key=lambda r: (score_of(r) is None, -(score_of(r) or 0.0))
    )
    return ordered[:limit] if limit and limit > 0 else ordered


def best_score(records: list[MemoryRecord]) -> float | None:
    scores = [s for s in (score_of(r) for r in records) if s is not None]
    return max(scores) if scores else None


def needs_exploration(memory: Any, best: float | None, complexity: float) -> bool:
    high = float(getattr(memory, "confidence_threshold_high", 0.8))
    low = float(getattr(memory, "confidence_threshold_low", 0.5))
    complex_at = float(getattr(memory, "complex_query_threshold", 0.7))
    if best is None:
        return True
    if best >= high:
        return False
    return best < low or complexity >= complex_at


def deep_recall(
    memory: Any,
    query: str,
    *,
    limit: int,
    search: SearchFn,
    scope: str | None = None,
) -> RecallOutcome:
    """Shallow search, then — per the knobs — distillation and exploration.

    ``search`` is the storage search already bound to scope, limit and floor;
    ``scope`` is that same scope, for the emptiness probe.
    """
    llm = getattr(memory, "llm", None)
    llm_ok = callable(getattr(llm, "call", None))
    threshold = getattr(memory, "query_analysis_threshold", None)
    budget = int(getattr(memory, "exploration_budget", 0) or 0)

    # Nothing to find → not worth a single model call. The probe is the
    # adapter's cached COUNT; storages without one count as non-empty.
    probe = getattr(getattr(memory, "storage", None), "has_records", None)
    store_empty = callable(probe) and not probe(
        scope or getattr(memory, "root_scope", None)
    )

    plan = RecallPlan(query=query)
    if (
        llm_ok
        and not store_empty
        and threshold is not None
        and int(threshold) >= 0
        and len(query) >= int(threshold)
    ):
        plan = analyze_query(llm, query)

    hits = merge_hits([], search(plan.query))
    tried = {plan.query.lower()}
    if plan.analyzed and plan.query.lower() != query.lower():
        # The plain search too — analysis may only ever ADD.
        hits = merge_hits(hits, search(query))
        tried.add(query.lower())

    best = best_score(hits)
    rounds = 0
    pending = [a for a in plan.alternatives if a.lower() not in tried]
    while (
        rounds < budget
        and llm_ok
        and not store_empty
        and needs_exploration(memory, best, plan.complexity)
    ):
        if not pending:
            pending = propose_alternatives(llm, query, tried, rank(hits, 3))
            if not pending:
                break
        rounds += 1
        for alt in pending[:MAX_ALTERNATIVES]:
            tried.add(alt.lower())
            hits = merge_hits(hits, search(alt))
        pending = []
        best = best_score(hits)

    return RecallOutcome(
        records=rank(hits, limit), plan=plan, rounds=rounds, best_score=best
    )
