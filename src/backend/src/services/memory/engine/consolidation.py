"""Save-time consolidation — fold a new memory into a near-duplicate the store
already holds (Memory Tuning: consolidation threshold / consolidation limit).

Two runs of one task write two records that say the same thing. The
write-time gate in ``hooks._already_remembered`` catches the literal case
(token overlap ≥ 0.9, no model call); this pass catches the SEMANTIC one — the
``consolidation_limit`` nearest records are fetched, and when the best of them
scores at or above ``consolidation_threshold`` the new text is merged INTO it
instead of being inserted beside it. The merge is a rewrite by the memory LLM
that keeps every fact from both notes — so without an LLM the pass is skipped
and the record is inserted: silently dropping a note that merely RESEMBLES an
old one (yesterday's report vs today's) would lose data, and the literal
duplicate case is already caught, model-free, by the write-time gate.

The similarity compared is the semantic component when the storage reports it
(``metadata["semantic"]``), else the blended score it stamps on every hit.

Never raises: any failure means "insert normally".
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from .analyze import extract_json_object
from .types import MemoryRecord

logger = logging.getLogger(__name__)

# Maintenance's own merged output must never be folded back into one of its
# members — that member is about to be deleted.
SOURCE_CONSOLIDATION = "consolidation"
_MERGE_CHAR_CAP = 4000
# Scoring artefacts the storage stamps on hits; never written back.
_ADVISORY_KEYS = ("similarity", "semantic")

_MERGE_SYSTEM_PROMPT = (
    "You maintain an AI agent's long-term memory. Two notes below say nearly "
    "the same thing. Rewrite them as ONE note that keeps every distinct fact, "
    "figure, name and date from both, prefers the newer note where they "
    "disagree, and drops nothing. Keep the style of the notes. Reply with "
    'ONLY a JSON object: {"content": "<the merged note>"}'
)


def similarity_of(record: MemoryRecord) -> float | None:
    metadata = getattr(record, "metadata", None) or {}
    for key in _ADVISORY_KEYS[::-1]:  # semantic first, then blended
        value = metadata.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def find_duplicate(
    memory: Any, record: MemoryRecord, scope: str | None
) -> tuple[MemoryRecord, float] | None:
    """The stored record ``record`` should be folded into, if any."""
    threshold = float(getattr(memory, "consolidation_threshold", 0) or 0)
    limit = int(getattr(memory, "consolidation_limit", 0) or 0)
    if threshold <= 0 or limit <= 0:
        return None
    if getattr(record, "source", None) == SOURCE_CONSOLIDATION:
        return None
    text = " ".join((record.content or "").split())
    if not text:
        return None
    candidates = memory.storage.search(
        text[:2000], limit=limit, scope=scope, score_threshold=0.0
    )
    best: tuple[MemoryRecord, float] | None = None
    for candidate in candidates or []:
        if getattr(candidate, "id", None) == record.id:
            continue
        score = similarity_of(candidate)
        if score is None:
            continue
        if best is None or score > best[1]:
            best = (candidate, score)
    if best is not None and best[1] >= threshold:
        return best
    return None


def merge_contents(llm: Any, existing: str, new: str) -> str | None:
    """One rewrite with the memory LLM; ``None`` when it cannot be done safely
    (no LLM, a reply without content, a failed call) — the caller inserts."""
    call = getattr(llm, "call", None)
    if not callable(call):
        return None
    try:
        raw = call(
            [
                {"role": "system", "content": _MERGE_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Existing note:\n{existing[:_MERGE_CHAR_CAP]}\n\n"
                        f"Newer note:\n{new[:_MERGE_CHAR_CAP]}"
                    ),
                },
            ]
        )
        payload = extract_json_object(str(raw or ""))
        merged = (
            " ".join(str(payload.get("content") or "").split())
            if isinstance(payload, dict)
            else ""
        )
        if not merged:
            logger.debug("memory merge reply carried no content: %.200r", raw)
            return None
        return merged[:_MERGE_CHAR_CAP]
    except Exception:  # noqa: BLE001 — a failed merge means "insert normally"
        logger.warning("memory merge failed; inserting the note", exc_info=True)
        return None


def consolidate_on_save(
    memory: Any, record: MemoryRecord, scope: str | None
) -> MemoryRecord | None:
    """Fold ``record`` into its near-duplicate. ``None`` → insert normally."""
    try:
        llm = getattr(memory, "llm", None)
        if not callable(getattr(llm, "call", None)):
            return None  # no LLM → no safe merge → insert
        hit = find_duplicate(memory, record, scope)
        if hit is None:
            return None
        existing, score = hit
        merged = merge_contents(llm, existing.content, record.content)
        if merged is None:
            return None
        metadata = {
            k: v
            for k, v in (existing.metadata or {}).items()
            if k not in _ADVISORY_KEYS
        }
        metadata["consolidated_writes"] = (
            int(metadata.get("consolidated_writes", 0) or 0) + 1
        )
        metadata["consolidation_similarity"] = round(float(score), 4)
        changes: dict[str, Any] = {
            "content": merged,
            "categories": sorted(set(existing.categories) | set(record.categories)),
            "metadata": metadata,
            "importance": max(existing.importance, record.importance),
            "last_accessed": datetime.now(timezone.utc),
        }
        if merged != existing.content:
            # Content changed → the stored vector is stale; a None embedding
            # makes the backend re-embed on save.
            changes["embedding"] = None
        updated = memory.storage.update(existing.id, **changes)
        logger.info(
            "memory consolidated on save: folded into %s (similarity %.3f%s)",
            existing.id,
            score,
            ", merged" if merged != existing.content else ", kept",
        )
        return updated or existing
    except Exception:  # noqa: BLE001 — consolidation must never lose a memory
        logger.warning(
            "memory save-time consolidation failed; inserting", exc_info=True
        )
        return None
