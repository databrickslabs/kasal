"""Background memory maintenance — Kasal's "sleep-time compute".

Runs off the critical path (chat/crew/flow end, after the write flush). Four
passes, cheapest first:

1. :func:`consolidate_memory` — LLM-free exact-content dedupe (one bounded
   listing, targeted deletes). Repeated chat turns and re-run crews otherwise
   accumulate identical records that crowd the recall top-k with copies.
2. :func:`merge_similar_memories` — ONE bounded LLM call that spots
   near-duplicate/fragmented records and replaces each cluster with a single
   merged record. Only runs when the scope is big enough to matter and the
   memory has an LLM configured; ``KASAL_MEMORY_LLM_CONSOLIDATION=false``
   disables it entirely.
3. :func:`supersede_outdated_facts` (in ``supersession.py``) — retires facts a
   newer record contradicts. The first two make the store SMALLER; this one
   makes it TRUER, and they pull in opposite directions on the same input, which
   is why the merge prompt is explicitly told to leave contradictions alone.
4. :func:`forget_expired_memories` (in ``forgetting.py``) — deletes what is past
   its retention rule. OPT-IN (``KASAL_MEMORY_FORGETTING=true``): it is the only
   pass that removes something a user might still want.

:func:`run_memory_maintenance` orchestrates all four. Everything is
best-effort — maintenance must never break (or outlive) a run: failures log and
no-op.

**Every execution path must reach this.** It used to be called only from the
crew path, so a workspace that only used chat (or only flows) never consolidated
its memory at all and accumulated duplicates forever. The three paths differ in
how often they finish, so they enter through different doors:

* crew / flow — one run is a coarse boundary, so they call
  :func:`run_memory_maintenance` (flows via
  :func:`run_registered_memory_maintenance`, since a flow's crews build their
  memory deep inside the flow and the subprocess teardown has no handle on it).
* chat — a turn is seconds long, and an unthrottled pass would put a bounded
  listing (and, past ``_MERGE_MIN_RECORDS``, an LLM call) on EVERY turn. Chat
  goes through :func:`maybe_run_memory_maintenance` /
  :func:`run_maintenance_after_writes`, which claim a per-scope slot at most
  once per ``KASAL_MEMORY_MAINTENANCE_INTERVAL`` seconds.

The throttle is process-local and deliberately so: it is a rate limiter for a
long-lived server, not the durable per-scope watermark a scheduled sweep would
need (see the memory gap analysis, M9.b).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import threading
import time
from typing import Any

from src.services.memory.engine import (
    KIND_EPISODIC,
    KIND_PROCEDURAL,
    KIND_SEMANTIC,
)
from src.services.memory.forgetting import forget_expired_memories
from src.services.memory.supersession import supersede_outdated_facts

logger = logging.getLogger(__name__)

# Newest records scanned per pass. Bounded so a huge scope can never turn the
# end-of-run maintenance into real work; older history gets covered across runs.
_SCAN_LIMIT = 500

# LLM merge pass: bounded input, gated on scope size so small workspaces never
# pay a model call for nothing.
_MERGE_SCAN_LIMIT = 60
_MERGE_MIN_RECORDS = 25
_MERGE_SNIPPET_CHARS = 300

_MERGE_PROMPT = """You maintain an AI agent's long-term memory store.
Below are memory records, one per line, formatted as "N: text".

Identify groups of records that state the SAME fact or are fragments of the
same information (near-duplicates, partial overlaps). For each group, write
one merged record that preserves every distinct detail.

Reply with ONLY a JSON array (no prose, no fences). Each element:
{{"merge": [record numbers], "text": "the merged record"}}

Rules:
- Only include groups of 2+ records that genuinely describe the same fact.
- Never merge records about different topics. When unsure, leave them out.
- CONTRADICTIONS ARE NOT FRAGMENTS. If two records about the same subject
  cannot both be true now — one states a value that the other replaced — leave
  them out entirely. Do NOT write a merged record that preserves both, and do
  not silently pick a winner. Retiring outdated facts is a separate pass that
  records which one replaced which; merging them here destroys that.
- If nothing should be merged, reply [].

Records:
{records}"""


def _merged_kind(members: list[Any]) -> str:
    """The kind a merged record should carry: the most durable of its members.

    Fragments of one preference are often a mix — the classifier saw the same
    subject in a transcript (episodic) and in a bare statement (semantic).
    Taking the most durable reading keeps the merged result a fact.
    """
    kinds = {getattr(record, "kind", KIND_EPISODIC) for record in members}
    for kind in (KIND_PROCEDURAL, KIND_SEMANTIC):
        if kind in kinds:
            return kind
    return KIND_EPISODIC


def _content_key(record: Any) -> str:
    normalized = " ".join(str(getattr(record, "content", "") or "").split()).lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def consolidate_memory(memory: Any, scope: str | None = None) -> dict[str, int]:
    """Delete exact-content duplicates within ``scope``, keeping the newest.

    Operates through the unified ``Memory`` surface (list_records + storage
    delete), so it works identically on the local, Lakebase, and Databricks
    backends. Best-effort: any failure aborts the pass without raising.

    Returns ``{"scanned": n, "deleted": n}``.
    """
    stats = {"scanned": 0, "deleted": 0}
    if memory in (None, True, False):
        return stats
    try:
        records = memory.list_records(scope=scope, limit=_SCAN_LIMIT)
    except Exception as exc:  # noqa: BLE001 — maintenance must never break a run
        logger.debug("Memory consolidation listing failed: %s", exc)
        return stats

    stats["scanned"] = len(records)
    seen: dict[str, Any] = {}
    duplicates: list[Any] = []
    # list_records returns newest-first on every backend; the first record per
    # content key is the keeper.
    for record in records:
        key = _content_key(record)
        if not key:
            continue
        if key in seen:
            duplicates.append(record)
        else:
            seen[key] = record

    storage = getattr(memory, "storage", None)
    if not callable(getattr(storage, "delete", None)):
        return stats
    for record in duplicates:
        try:
            if storage.delete(str(record.id)):
                stats["deleted"] += 1
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "Memory consolidation delete failed for %s: %s", record.id, exc
            )

    if stats["deleted"]:
        logger.info(
            "Memory consolidation: removed %d duplicate record(s) "
            "(scanned %d, scope=%s)",
            stats["deleted"],
            stats["scanned"],
            scope or "root",
        )
    return stats


def _extract_json_array(raw: str) -> list:
    """Pull the first JSON array out of an LLM reply (tolerates fences/prose)."""
    text = str(raw or "").strip()
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        return []
    try:
        parsed = json.loads(match.group(0))
    except (ValueError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def merge_similar_memories(memory: Any, scope: str | None = None) -> dict[str, int]:
    """Replace clusters of near-duplicate records with one merged record each.

    One bounded LLM call over the newest ``_MERGE_SCAN_LIMIT`` records, using
    the memory's own configured LLM (``Memory.llm`` — the crew's model, never
    an implicit OpenAI default). Gated: skips entirely when disabled via
    ``KASAL_MEMORY_LLM_CONSOLIDATION=false``, when no LLM is configured, or
    when the scope holds fewer than ``_MERGE_MIN_RECORDS`` records.

    Returns ``{"scanned": n, "merged_clusters": k, "records_replaced": m}``.
    """
    stats = {"scanned": 0, "merged_clusters": 0, "records_replaced": 0}
    if memory in (None, True, False):
        return stats
    if os.environ.get("KASAL_MEMORY_LLM_CONSOLIDATION", "true").lower() == "false":
        return stats
    llm = getattr(memory, "llm", None)
    call = getattr(llm, "call", None)
    if not callable(call):
        return stats
    try:
        records = memory.list_records(scope=scope, limit=_MERGE_SCAN_LIMIT)
    except Exception as exc:  # noqa: BLE001 — maintenance must never break a run
        logger.debug("Memory merge listing failed: %s", exc)
        return stats
    stats["scanned"] = len(records)
    if len(records) < _MERGE_MIN_RECORDS:
        return stats

    lines = []
    for index, record in enumerate(records):
        content = " ".join(str(getattr(record, "content", "") or "").split())
        lines.append(f"{index}: {content[:_MERGE_SNIPPET_CHARS]}")
    try:
        reply = call(_MERGE_PROMPT.format(records="\n".join(lines)))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Memory merge LLM call failed: %s", exc)
        return stats

    storage = getattr(memory, "storage", None)
    if not callable(getattr(storage, "delete", None)):
        return stats

    consumed: set[int] = set()
    for cluster in _extract_json_array(reply):
        if not isinstance(cluster, dict):
            continue
        merged_text = " ".join(str(cluster.get("text", "") or "").split())
        indices = cluster.get("merge")
        if not merged_text or not isinstance(indices, list) or len(indices) < 2:
            continue
        members = []
        for index in indices:
            if (
                isinstance(index, int)
                and 0 <= index < len(records)
                and index not in consumed
            ):
                members.append(records[index])
        if len(members) < 2:
            continue
        try:
            categories = sorted({c for r in members for c in (r.categories or [])})
            memory.remember(
                merged_text[:4000],
                categories=categories or None,
                importance=max(r.importance for r in members),
                source="consolidation",
                metadata={"merged_from": len(members)},
                # Inherit the kind rather than re-classifying: passing it
                # explicitly is what keeps this path free of a second LLM call
                # per cluster. Merging fragments of a fact must not demote the
                # result to episodic (which would make it decay and stop it
                # ever being superseded), so any durable member wins.
                kind=_merged_kind(members),
            )
            for record in members:
                storage.delete(str(record.id))
        except Exception as exc:  # noqa: BLE001 — skip the cluster, keep going
            logger.debug("Memory merge cluster failed: %s", exc)
            continue
        consumed.update(index for index in indices if isinstance(index, int))
        stats["merged_clusters"] += 1
        stats["records_replaced"] += len(members)

    if stats["merged_clusters"]:
        logger.info(
            "Memory merge: %d cluster(s), %d record(s) consolidated (scope=%s)",
            stats["merged_clusters"],
            stats["records_replaced"],
            scope or "root",
        )
    return stats


def run_memory_maintenance(memory: Any, scope: str | None = None) -> dict[str, int]:
    """Full end-of-run maintenance, cheapest pass first.

    1. exact dedupe (no LLM), 2. the gated near-duplicate merge, 3. retiring
    facts a newer record contradicts, 4. forgetting what is past its retention
    rule (opt-in).

    The order is not arbitrary. Supersession runs after the two shrinking passes
    because they cut what it has to read — a scope full of identical copies
    would otherwise spend the pass comparing a fact against itself. Forgetting
    runs last because it is the only pass that consumes supersession's output:
    a record retired in step 3 starts its retention window here.
    """
    stats = dict(consolidate_memory(memory, scope=scope))
    merge_stats = merge_similar_memories(memory, scope=scope)
    supersede_stats = supersede_outdated_facts(memory, scope=scope)
    forget_stats = forget_expired_memories(memory, scope=scope)
    stats.update(
        merged_clusters=merge_stats["merged_clusters"],
        records_replaced=merge_stats["records_replaced"],
        superseded=supersede_stats["superseded"],
        forgotten=forget_stats["forgotten"],
    )
    return stats


# ----------------------------------------------------------------------
# Throttle — for callers that finish often (chat turns)
# ----------------------------------------------------------------------

_SKIPPED: dict[str, int] = {
    "scanned": 0,
    "deleted": 0,
    "merged_clusters": 0,
    "records_replaced": 0,
    "superseded": 0,
    "forgotten": 0,
    "skipped": 1,
}

_last_maintenance: dict[str, float] = {}
_throttle_lock = threading.Lock()


def _default_interval() -> float:
    """Minimum seconds between passes over one scope (0 disables throttling)."""
    try:
        return float(os.environ.get("KASAL_MEMORY_MAINTENANCE_INTERVAL", "900"))
    except (TypeError, ValueError):
        return 900.0


def _scope_key(memory: Any, scope: str | None) -> str:
    return scope or str(getattr(memory, "root_scope", None) or "/")


def _claim_maintenance_slot(key: str, min_interval_s: float) -> bool:
    """Reserve the next pass for ``key``. ``False`` when one ran too recently.

    The timestamp is stamped on CLAIM, not on completion: chat turns land
    concurrently, and marking afterwards would let every turn that arrives
    during a pass claim its own.
    """
    if min_interval_s <= 0:
        return True
    now = time.monotonic()
    with _throttle_lock:
        previous = _last_maintenance.get(key)
        if previous is not None and (now - previous) < min_interval_s:
            return False
        _last_maintenance[key] = now
        return True


def reset_maintenance_throttle() -> None:
    """Forget every claimed slot. For tests and process reuse."""
    with _throttle_lock:
        _last_maintenance.clear()


def maybe_run_memory_maintenance(
    memory: Any,
    scope: str | None = None,
    min_interval_s: float | None = None,
) -> dict[str, int]:
    """:func:`run_memory_maintenance`, at most once per scope per interval.

    For callers whose runs finish far more often than the store needs tidying —
    chat, where an unthrottled pass would add a listing (and eventually an LLM
    merge call) to every turn. Returns the usual stats, or ``{"skipped": 1}``
    when this scope was maintained too recently.
    """
    if memory in (None, True, False):
        return dict(_SKIPPED)
    interval = _default_interval() if min_interval_s is None else float(min_interval_s)
    if not _claim_maintenance_slot(_scope_key(memory, scope), interval):
        return dict(_SKIPPED)
    return run_memory_maintenance(memory, scope=scope)


async def run_maintenance_after_writes(
    memory: Any,
    scope: str | None = None,
    flush_timeout: float = 15.0,
    min_interval_s: float | None = None,
) -> dict[str, int]:
    """Throttled maintenance that first waits for in-flight writes to land.

    The async entry point for the in-process chat path: schedule it with
    ``asyncio.create_task`` after the turn's answer is produced. Persistence is
    fire-and-forget, so the flush is what makes this turn's record visible to
    the dedupe pass. The throttle is checked BEFORE the flush, so a skipped turn
    costs nothing at all. Never raises — a maintenance failure must not surface
    on a chat turn.
    """
    if memory in (None, True, False):
        return dict(_SKIPPED)
    interval = _default_interval() if min_interval_s is None else float(min_interval_s)
    if not _claim_maintenance_slot(_scope_key(memory, scope), interval):
        return dict(_SKIPPED)
    try:
        from src.services.memory.hooks import flush_memory_writes

        await asyncio.to_thread(flush_memory_writes, flush_timeout)
        return await asyncio.to_thread(run_memory_maintenance, memory, scope)
    except Exception as exc:  # noqa: BLE001 — maintenance never breaks a turn
        logger.debug("Memory maintenance after writes skipped: %s", exc)
        return dict(_SKIPPED)


# asyncio only holds a WEAK reference to a running task, so a bare
# ``create_task(...)`` whose result nobody awaits can be collected mid-flight —
# which would silently reintroduce the "chat never consolidates" bug it is here
# to fix. Hold a strong reference until the task settles.
_scheduled_tasks: set[asyncio.Task] = set()


def schedule_maintenance_after_writes(memory: Any, scope: str | None = None) -> Any:
    """Start :func:`run_maintenance_after_writes` in the background.

    For the in-process chat path: returns immediately, so the turn's answer is
    never delayed by maintenance. Returns the task, or ``None`` when there is
    nothing to do or no running loop.
    """
    if memory in (None, True, False):
        return None
    try:
        task = asyncio.get_running_loop().create_task(
            run_maintenance_after_writes(memory, scope=scope)
        )
    except RuntimeError:  # no running loop — nothing to schedule onto
        return None
    _scheduled_tasks.add(task)
    task.add_done_callback(_scheduled_tasks.discard)
    return task


# ----------------------------------------------------------------------
# Registry — for callers that cannot reach their own Memory at teardown
# ----------------------------------------------------------------------

# A flow builds a Memory per crew, deep inside the flow's own wiring, and the
# subprocess teardown that drains the write pool has no reference to any of
# them. Registering at build time is what lets the teardown maintain them.
# Bounded, and only the flow path registers, so the long-lived server process
# (which maintains chat memory directly) never accumulates entries here.
_REGISTRY_LIMIT = 16
_registered_memories: list[Any] = []
_registry_lock = threading.Lock()


def register_memory_for_maintenance(memory: Any) -> None:
    """Record ``memory`` so a later teardown can maintain it. Never raises."""
    if memory in (None, True, False):
        return
    with _registry_lock:
        if any(candidate is memory for candidate in _registered_memories):
            return
        _registered_memories.append(memory)
        while len(_registered_memories) > _REGISTRY_LIMIT:
            del _registered_memories[0]


def clear_registered_memories() -> None:
    """Drop every registered Memory. For tests and process reuse."""
    with _registry_lock:
        _registered_memories.clear()


def run_registered_memory_maintenance() -> dict[str, int]:
    """Maintain each registered Memory once, one pass per distinct root scope.

    Several crews in one flow usually share a backend and a root scope; running
    the same scope repeatedly would re-scan the same records for nothing.
    """
    with _registry_lock:
        memories = list(_registered_memories)
    totals = {
        "scanned": 0,
        "deleted": 0,
        "merged_clusters": 0,
        "records_replaced": 0,
        "superseded": 0,
        "forgotten": 0,
    }
    seen_scopes: set[str] = set()
    for memory in memories:
        key = _scope_key(memory, None)
        if key in seen_scopes:
            continue
        seen_scopes.add(key)
        try:
            stats = run_memory_maintenance(memory)
        except Exception as exc:  # noqa: BLE001 — best-effort, per memory
            logger.debug("Memory maintenance failed for scope %s: %s", key, exc)
            continue
        for field in totals:
            totals[field] += int(stats.get(field, 0) or 0)
    return totals
