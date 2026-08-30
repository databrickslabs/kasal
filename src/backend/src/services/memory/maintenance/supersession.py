"""Truth maintenance — retiring facts that stopped being true.

Consolidation (``maintenance.py``) makes the store SMALLER: it removes exact
duplicates and merges fragments of one fact. This pass makes it TRUER, and the
distinction matters because the two want opposite treatment of the same input:

    "The project deadline is 15 June."
    "The deadline moved to 30 July."

Consolidation's merge prompt is told to *preserve every distinct detail*, which
is correct for fragments and wrong here — it keeps both, and recall then returns
whichever the blended score happens to favour that day. As soon as the correct
fact is older than the incorrect one, ordering can invert. That is the most
user-visible failure a memory system has.

So contradictions are resolved rather than merged: the older record's
``valid_to`` is closed and its ``superseded_by`` points at the winner. Nothing is
deleted — recall filters to currently-valid records, but the history remains, so
"what did we believe on 3 March" stays answerable and a wrong retirement is
reversible.

Scope and cost, deliberately bounded like every other maintenance pass:

* **Semantic records only.** Episodic records are a log of what happened; two
  accounts of different moments are both true and must never supersede one
  another.
* **One LLM call**, over the newest ``_SCAN_LIMIT`` semantic records in a scope,
  gated on the memory having an LLM configured. Same shape as the merge pass.
* **Best-effort.** Any failure logs and no-ops; a run never breaks because truth
  maintenance did.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from src.services.memory.engine import KIND_EPISODIC

logger = logging.getLogger(__name__)

# Newest semantic records considered per pass. Smaller than the dedupe scan:
# every one of these goes into the prompt, and contradictions are overwhelmingly
# between recent statements and the fact they replace.
_SCAN_LIMIT = 40
_MIN_RECORDS = 2
_SNIPPET_CHARS = 300

_PROMPT = """You maintain an AI agent's long-term memory of FACTS.
Below are memory records, one per line, formatted as "N: text". They are ordered
NEWEST FIRST.

Find records that CONTRADICT a newer record — statements about the same subject
that cannot both be true now, because the newer one replaced the older one.

Reply with ONLY a JSON array (no prose, no fences). Each element:
{{"current": N, "outdated": [N, ...]}}

Rules:
- "current" must be the NEWEST record of the group (the lowest number).
- Only group records that genuinely describe the SAME subject.
- Records that differ but can both be true are NOT contradictions. Two people's
  preferences, two different projects, or extra detail about one thing must be
  left alone.
- When unsure, leave them out. Retiring a true fact is worse than keeping a
  stale one.
- If nothing is outdated, reply [].

Records:
{records}"""


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


def _durable(record: Any) -> bool:
    """True for records that assert something still-true (never episodic)."""
    return getattr(record, "kind", KIND_EPISODIC) != KIND_EPISODIC


def supersede_outdated_facts(memory: Any, scope: str | None = None) -> dict[str, int]:
    """Retire facts contradicted by a newer one. ``{"scanned", "superseded"}``.

    Disable with ``KASAL_MEMORY_SUPERSESSION=false``.
    """
    stats = {"scanned": 0, "superseded": 0}
    if memory in (None, True, False):
        return stats
    if os.environ.get("KASAL_MEMORY_SUPERSESSION", "true").lower() == "false":
        return stats
    call = getattr(getattr(memory, "llm", None), "call", None)
    if not callable(call):
        return stats

    try:
        # Over-fetch: list_records cannot filter by kind, and in a young store
        # almost everything is episodic, so a bare _SCAN_LIMIT would often
        # contain no facts at all.
        listed = memory.list_records(scope=scope, limit=_SCAN_LIMIT * 5)
    except Exception as exc:  # noqa: BLE001 — maintenance must never break a run
        logger.debug("Supersession listing failed: %s", exc)
        return stats

    # Newest first, and only currently-valid facts: an already-retired record
    # must not be re-retired, nor act as the winner of a new group.
    records = [
        record
        for record in listed
        if _durable(record) and getattr(record, "valid_to", None) is None
    ][:_SCAN_LIMIT]
    stats["scanned"] = len(records)
    if len(records) < _MIN_RECORDS:
        return stats

    lines = [
        f"{index}: {' '.join(str(getattr(record, 'content', '') or '').split())[:_SNIPPET_CHARS]}"
        for index, record in enumerate(records)
    ]
    try:
        reply = call(_PROMPT.format(records="\n".join(lines)))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Supersession LLM call failed: %s", exc)
        return stats

    retired: set[int] = set()
    for group in _extract_json_array(reply):
        if not isinstance(group, dict):
            continue
        current = group.get("current")
        outdated = group.get("outdated")
        if not isinstance(current, int) or not isinstance(outdated, list):
            continue
        if not 0 <= current < len(records):
            continue
        winner = records[current]
        for index in outdated:
            if not isinstance(index, int) or not 0 <= index < len(records):
                continue
            # A record cannot supersede itself, and one already retired in this
            # pass must not be re-pointed at a different winner.
            if index == current or index in retired:
                continue
            loser = records[index]
            # The model was told newest-first, but a model that mixes up the
            # direction would retire the CURRENT fact and keep the stale one —
            # the exact failure this pass exists to prevent. Trust the
            # timestamps, not the ordering claim.
            if getattr(loser, "created_at", None) and getattr(
                winner, "created_at", None
            ):
                if loser.created_at > winner.created_at:
                    continue
            if _retire(memory, loser, winner):
                retired.add(index)
                stats["superseded"] += 1

    if stats["superseded"]:
        logger.info(
            "Memory supersession: retired %d outdated fact(s) (scanned %d, scope=%s)",
            stats["superseded"],
            stats["scanned"],
            scope or "root",
        )
    return stats


def _retire(memory: Any, loser: Any, winner: Any) -> bool:
    """Close ``loser``'s validity window, pointing at ``winner``. Never raises."""
    try:
        memory.update(
            str(loser.id),
            valid_to=datetime.now(timezone.utc),
            superseded_by=str(winner.id),
        )
        return True
    except Exception as exc:  # noqa: BLE001 — skip this one, keep going
        logger.debug("Could not retire record %s: %s", getattr(loser, "id", "?"), exc)
        return False
