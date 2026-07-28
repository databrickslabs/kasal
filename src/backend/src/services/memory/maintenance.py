"""Background memory maintenance — Kasal's "sleep-time compute".

Runs off the critical path (crew/flow end, after the write flush). Two passes,
cheapest first:

1. :func:`consolidate_memory` — LLM-free exact-content dedupe (one bounded
   listing, targeted deletes). Repeated chat turns and re-run crews otherwise
   accumulate identical records that crowd the recall top-k with copies.
2. :func:`merge_similar_memories` — ONE bounded LLM call that spots
   near-duplicate/fragmented records and replaces each cluster with a single
   merged record. Only runs when the scope is big enough to matter and the
   memory has an LLM configured; ``KASAL_MEMORY_LLM_CONSOLIDATION=false``
   disables it entirely.

:func:`run_memory_maintenance` orchestrates both. Everything is best-effort —
maintenance must never break (or outlive) a run: failures log and no-op.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from typing import Any

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
- If nothing should be merged, reply [].

Records:
{records}"""


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
    delete = getattr(storage, "delete", None)
    if not callable(delete):
        return stats
    for record in duplicates:
        try:
            if delete(str(record.id)):
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
    delete = getattr(storage, "delete", None)
    if not callable(delete):
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
            )
            for record in members:
                delete(str(record.id))
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
    """Full end-of-run maintenance: exact dedupe, then the gated LLM merge."""
    stats = dict(consolidate_memory(memory, scope=scope))
    merge_stats = merge_similar_memories(memory, scope=scope)
    stats.update(
        merged_clusters=merge_stats["merged_clusters"],
        records_replaced=merge_stats["records_replaced"],
    )
    return stats
