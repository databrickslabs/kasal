"""The pending overlay — records handed to the writer but not yet durable.

Persistence is fire-and-forget and the next task's recall races it (measured:
the read won by 1.23s, all of it the labelling LLM call on the write thread).
Rather than wait, a submitted record is readable from process memory the
moment it is submitted and dropped the moment the durable write lands, so it
is visible through exactly one of the two paths and never both. Scoped by the
writing memory's root scope, so the overlay honours the tenant boundary."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# Records handed to the writer but not yet durable, readable RIGHT NOW.
#
# Persistence is fire-and-forget, and recall runs when the next task assembles
# its context, so the write and the read raced — and the read won. Measured on
# one run: task 2's recall returned 0 results at 21:07:31.217 and task 1's
# record only landed at 21:07:32.448, 1.23s later. The task immediately after a
# write could therefore NEVER see it; only later runs benefited.
#
# Waiting for the write instead would cost that 1.23s at every task boundary,
# and it would buy nothing in storage terms: the local backend holds ONE SQLite
# connection, so a committed row is already instantly visible to the next read
# (the store is not even in WAL mode). The delay is not the insert — 1.23s of it
# is the memory-labelling LLM call on the write thread. So the fix is neither a
# barrier nor a storage-engine change: keep the durable write asynchronous and
# make the value legible from process memory the moment it is submitted.
#
# Entries are dropped as soon as the durable write finishes, from which point
# storage.search returns them, so a record is visible through exactly one of the
# two paths and never both.
_MAX_PENDING_RECORDS = 64


@dataclass(eq=False)  # eq=False: dropped by identity, and content repeats
class _PendingMemory:
    """Quacks like a MemoryRecord for the two attributes recall formatting uses."""

    content: str
    source: str | None
    agent_role: str | None
    scope: str | None


_PENDING_RECORDS: list[_PendingMemory] = []


_PENDING_RECORDS_LOCK = threading.Lock()


def _add_pending(entry: _PendingMemory) -> None:
    with _PENDING_RECORDS_LOCK:
        _PENDING_RECORDS.append(entry)
        # Bounded: this process is long-lived and serves chat turns too, so a
        # wedged writer must not turn the overlay into a leak.
        while len(_PENDING_RECORDS) > _MAX_PENDING_RECORDS:
            del _PENDING_RECORDS[0]


def _drop_pending(entry: _PendingMemory) -> None:
    with _PENDING_RECORDS_LOCK:
        for index, candidate in enumerate(_PENDING_RECORDS):
            if candidate is entry:
                del _PENDING_RECORDS[index]
                return


def pending_memory_for(read_scope: str | None) -> list[_PendingMemory]:
    """In-flight records visible to a reader at ``read_scope``.

    Mirrors the backends' ``scope LIKE '<read_scope>%'`` containment so the
    overlay cannot widen what a tenant can see.

    Both sides must be a real scope string, and anything else yields nothing.
    This buffer is process-global — one server process serves several groups —
    and scope is the tenant boundary, so "no scope" is treated as "cannot prove
    it belongs to this reader" rather than as a wildcard. Production always sets
    one (``root_scope = f"/{group_id}"``), so nothing legitimate is lost.
    """
    if not isinstance(read_scope, str) or not read_scope:
        return []
    with _PENDING_RECORDS_LOCK:
        snapshot = list(_PENDING_RECORDS)
    return [e for e in snapshot if e.scope and e.scope.startswith(read_scope)]


def clear_pending_memory() -> None:
    """Drop the overlay. For tests and for a process reusing the writer pool."""
    with _PENDING_RECORDS_LOCK:
        _PENDING_RECORDS.clear()
