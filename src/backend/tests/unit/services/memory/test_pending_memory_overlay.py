"""A task must see what the task before it just remembered.

Persistence is fire-and-forget and recall runs when the next task assembles its
context, so the two raced and the read won. Measured on run 89d7d143:

    21:07:31.214706  task 2 recall starts
    21:07:31.214938  task 1 persist starts
    21:07:31.217255  task 2 recall returns 0 results
    21:07:32.448496  task 1's record lands in SQLite  (1.23s too late)

So the task immediately after a write could never see it; only later runs could.
Waiting for the write would cost that 1.23s at every task boundary and would fix
nothing in storage terms — the local backend holds one SQLite connection, so a
committed row is already instantly visible, and 1.23s of the delay is the
memory-labelling LLM call, not the insert. Hence an in-process overlay: readable
at submit, dropped once durable.
"""

import threading
import time
from unittest.mock import MagicMock

import pytest

from src.services.memory import hooks
from src.services.memory.engine import MemoryRecord
from src.services.memory.hooks import (
    build_memory_preamble,
    clear_pending_memory,
    flush_memory_writes,
    pending_memory_for,
    remember_async,
)

SCOPE = "/group_a"


@pytest.fixture(autouse=True)
def _clean_overlay():
    clear_pending_memory()
    yield
    flush_memory_writes(timeout=5.0)
    clear_pending_memory()


def _memory(scope=SCOPE, records=None, on_write=None):
    """A Memory whose write blocks until released, so 'in flight' is testable."""
    memory = MagicMock()
    memory.root_scope = scope
    memory.recall.return_value = list(records or [])
    if on_write is not None:
        memory.remember.side_effect = on_write
    return memory


class TestTheRaceItself:
    def test_a_record_is_readable_before_it_is_durable(self):
        """The regression, stated directly: the write is still in flight and the
        reader must already see it."""
        release = threading.Event()
        memory = _memory(on_write=lambda *a, **k: release.wait(5))

        remember_async(memory, "task 1 found four listings", source="crew_task")
        try:
            # Still in the overlay ⇒ the durable write has not finished. Asserting
            # the writer THREAD had started would be flaky instead of stricter:
            # the pool has two workers, so under load the write may not have begun
            # — which makes the record less durable, not more.
            assert pending_memory_for(SCOPE), "precondition: write not yet durable"
            block = build_memory_preamble(memory, "what did task 1 find?")
            assert "task 1 found four listings" in block
        finally:
            release.set()

    def test_without_the_overlay_the_reader_would_see_nothing(self):
        """Pins the cause: storage returns nothing while the write is in flight,
        so the block can only come from the overlay."""
        release = threading.Event()
        memory = _memory(records=[], on_write=lambda *a, **k: release.wait(5))
        remember_async(memory, "in flight", source="crew_task")
        try:
            assert memory.recall.return_value == []
            assert "in flight" in build_memory_preamble(memory, "q")
        finally:
            release.set()

    def test_it_costs_no_waiting(self):
        """The reason this is an overlay and not a barrier. A barrier would have
        paid the write's full 1.23s here."""
        release = threading.Event()
        memory = _memory(on_write=lambda *a, **k: release.wait(5))
        remember_async(memory, "slow to persist", source="crew_task")
        try:
            start = time.perf_counter()
            build_memory_preamble(memory, "q")
            assert (time.perf_counter() - start) < 0.5
        finally:
            release.set()


class TestNoDoubleVision:
    def test_the_entry_is_dropped_once_durable(self):
        memory = _memory()
        remember_async(memory, "persisted fine", source="crew_task")
        assert flush_memory_writes(timeout=5.0) == 0
        assert pending_memory_for(SCOPE) == []

    def test_a_record_in_storage_is_not_shown_twice(self):
        """Overlay and storage can briefly hold the same content; the reader must
        see one line, not two."""
        release = threading.Event()
        memory = _memory(
            records=[MemoryRecord(content="the same note", scope=SCOPE)],
            on_write=lambda *a, **k: release.wait(5),
        )
        remember_async(memory, "the same note", source="crew_task")
        try:
            block = build_memory_preamble(memory, "q")
            assert block.count("the same note") == 1
        finally:
            release.set()

    def test_storage_records_still_come_through(self):
        memory = _memory(records=[MemoryRecord(content="from storage", scope=SCOPE)])
        assert "from storage" in build_memory_preamble(memory, "q")

    def test_pending_is_listed_before_storage(self):
        """The freshest thing the crew produced is the reason the reader is here,
        and the char cap trims from the end."""
        release = threading.Event()
        memory = _memory(
            records=[MemoryRecord(content="older stored note", scope=SCOPE)],
            on_write=lambda *a, **k: release.wait(5),
        )
        remember_async(memory, "just written", source="crew_task")
        try:
            block = build_memory_preamble(memory, "q")
            assert block.index("just written") < block.index("older stored note")
        finally:
            release.set()


class TestTenantIsolation:
    def test_another_group_cannot_see_it(self):
        """Scope is the tenant boundary and this buffer is process-global."""
        release = threading.Event()
        writer = _memory(scope="/group_a", on_write=lambda *a, **k: release.wait(5))
        remember_async(writer, "group a secret", source="crew_task")
        try:
            reader = _memory(scope="/group_b", records=[])
            assert build_memory_preamble(reader, "q") == ""
        finally:
            release.set()

    def test_a_narrower_reader_scope_still_matches_by_prefix(self):
        """Mirrors the backends' scope LIKE '<read_scope>%' containment."""
        release = threading.Event()
        writer = _memory(
            scope="/group_a/crew_7", on_write=lambda *a, **k: release.wait(5)
        )
        remember_async(writer, "crew 7 note", source="crew_task")
        try:
            assert len(pending_memory_for("/group_a")) == 1
            assert pending_memory_for("/group_b") == []
        finally:
            release.set()

    def test_an_unscoped_write_is_unreadable_rather_than_global(self):
        """A missing scope means "cannot prove it belongs to this reader". The
        inverse default would leak one tenant's memory into another's prompt."""
        release = threading.Event()
        writer = _memory(scope=None, on_write=lambda *a, **k: release.wait(5))
        remember_async(writer, "unscoped note", source="crew_task")
        try:
            assert pending_memory_for("/group_a") == []
            assert pending_memory_for(None) == []
        finally:
            release.set()

    def test_a_mock_shaped_scope_is_not_a_scope(self):
        """MagicMock auto-creates root_scope, and str(mock).startswith(mock)
        would raise. Non-strings are treated as absent."""
        release = threading.Event()
        writer = MagicMock()
        writer.remember.side_effect = lambda *a, **k: release.wait(5)
        remember_async(writer, "mock scoped", source="crew_task")
        try:
            assert pending_memory_for("/group_a") == []
        finally:
            release.set()


class TestItStaysBounded:
    def test_the_buffer_cannot_grow_without_limit(self):
        """A wedged writer must not turn the overlay into a leak in the
        long-lived server process."""
        release = threading.Event()
        memory = _memory(on_write=lambda *a, **k: release.wait(10))
        try:
            for i in range(hooks._MAX_PENDING_RECORDS + 20):
                remember_async(memory, f"note {i}", source="crew_task")
            assert len(pending_memory_for(SCOPE)) <= hooks._MAX_PENDING_RECORDS
        finally:
            release.set()

    def test_a_failed_write_does_not_wedge_the_overlay(self):
        memory = _memory(on_write=MagicMock(side_effect=RuntimeError("backend down")))
        remember_async(memory, "will fail", source="crew_task")
        flush_memory_writes(timeout=5.0)
        assert pending_memory_for(SCOPE) == []

    def test_recall_failure_still_shows_pending(self):
        """Memory is best-effort, but a broken backend should not also hide what
        the previous task just produced."""
        release = threading.Event()
        memory = _memory(on_write=lambda *a, **k: release.wait(5))
        memory.recall.side_effect = RuntimeError("storage unavailable")
        remember_async(memory, "still visible", source="crew_task")
        try:
            assert "still visible" in build_memory_preamble(memory, "q")
        finally:
            release.set()
