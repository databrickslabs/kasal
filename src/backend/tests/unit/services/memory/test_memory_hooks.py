"""Tests for memory_hooks — recall preambles, async persist, crew wiring."""

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

from kasal_engine.memory import MemoryRecord
from src.services.memory.hooks import (
    MEMORY_BLOCK_HEADER,
    build_memory_preamble,
    flush_memory_writes,
    format_turn_for_memory,
    inject_task_memory,
    register_task_output_persistence,
    remember_async,
)


def _memory_with(records):
    memory = MagicMock()
    memory.recall.return_value = records
    return memory


class TestBuildMemoryPreamble:
    def test_formats_records_with_header_and_source(self):
        memory = _memory_with(
            [
                MemoryRecord(content="Paris is the capital", source="chat"),
                MemoryRecord(content="Report used Q2 numbers"),
            ]
        )
        block = build_memory_preamble(memory, "capital of France?")
        assert block.startswith(MEMORY_BLOCK_HEADER)
        assert "- [chat] Paris is the capital" in block
        assert "- Report used Q2 numbers" in block

    def test_empty_when_no_records_or_sentinel_memory(self):
        assert build_memory_preamble(_memory_with([]), "q") == ""
        assert build_memory_preamble(None, "q") == ""
        assert build_memory_preamble(True, "q") == ""
        assert build_memory_preamble(False, "q") == ""
        assert (
            build_memory_preamble(_memory_with([MemoryRecord(content="x")]), " ") == ""
        )

    def test_recall_failure_returns_empty(self):
        memory = MagicMock()
        memory.recall.side_effect = RuntimeError("backend down")
        assert build_memory_preamble(memory, "q") == ""

    def test_char_cap_is_enforced(self):
        records = [MemoryRecord(content=("word " * 200)) for _ in range(50)]
        block = build_memory_preamble(_memory_with(records), "q", char_cap=1000)
        assert len(block) <= 1000

    def test_query_is_trimmed_before_recall(self):
        memory = _memory_with([MemoryRecord(content="x")])
        build_memory_preamble(memory, "  padded query  ")
        assert memory.recall.call_args.args[0] == "padded query"


class TestRememberAsync:
    def _blocking_memory(self):
        done = threading.Event()
        memory = MagicMock()
        memory.remember.side_effect = lambda *a, **k: done.set()
        return memory, done

    def test_writes_off_the_caller_thread(self):
        memory, done = self._blocking_memory()
        remember_async(memory, "note to keep", source="chat", agent_role="Analyst")
        assert done.wait(timeout=5), "write never happened"
        kwargs = memory.remember.call_args.kwargs
        assert kwargs["source"] == "chat"
        assert kwargs["agent_role"] == "Analyst"

    def test_noop_on_sentinel_or_empty(self):
        memory = MagicMock()
        remember_async(None, "x")
        remember_async(True, "x")
        remember_async(memory, "   ")
        time.sleep(0.05)
        memory.remember.assert_not_called()

    def test_write_failure_is_swallowed(self):
        done = threading.Event()

        def _boom(*a, **k):
            done.set()
            raise RuntimeError("db locked")

        memory = MagicMock()
        memory.remember.side_effect = _boom
        remember_async(memory, "content")
        assert done.wait(timeout=5)  # raised inside the pool, never propagated


class TestFlushMemoryWrites:
    """The crew subprocess exits right after kickoff — the flush is what keeps
    the last task's save (and its trace span) from dying with the process."""

    def test_flush_waits_for_inflight_write(self):
        finished = threading.Event()

        def _slow_remember(*a, **k):
            time.sleep(0.3)
            finished.set()

        memory = MagicMock()
        memory.remember.side_effect = _slow_remember

        remember_async(memory, "final task output")
        still_pending = flush_memory_writes(timeout=5.0)

        assert still_pending == 0
        assert finished.is_set(), "flush returned before the write completed"

    def test_flush_reports_writes_exceeding_timeout(self):
        release = threading.Event()
        memory = MagicMock()
        memory.remember.side_effect = lambda *a, **k: release.wait(timeout=5)

        remember_async(memory, "very slow write")
        try:
            assert flush_memory_writes(timeout=0.1) == 1
        finally:
            release.set()  # unblock the writer thread
            flush_memory_writes(timeout=5.0)  # drain before the next test

    def test_flush_with_nothing_pending_is_zero(self):
        assert flush_memory_writes(timeout=0.1) == 0


class TestFormatTurn:
    def test_compacts_and_truncates(self):
        text = format_turn_for_memory("q " * 1000, "a " * 2000)
        assert text.startswith("User: ")
        assert "\nAssistant: " in text
        assert len(text) <= 2100


class TestInjectTaskMemory:
    def test_appends_block_to_descriptions(self):
        memory = _memory_with([MemoryRecord(content="prior learning")])
        task = SimpleNamespace(description="Analyze churn")
        assert inject_task_memory(memory, [task]) == 1
        assert task.description.startswith("Analyze churn\n\n")
        assert "prior learning" in task.description

    def test_skips_when_nothing_recalled(self):
        task = SimpleNamespace(description="Analyze churn")
        assert inject_task_memory(_memory_with([]), [task]) == 0
        assert task.description == "Analyze churn"

    def test_sentinel_memory_is_noop(self):
        assert inject_task_memory(None, [SimpleNamespace(description="d")]) == 0
        assert inject_task_memory(False, [SimpleNamespace(description="d")]) == 0


class TestTaskOutputPersistence:
    def _crew(self, memory):
        task = SimpleNamespace(id="task-1", name="research", description="Find facts")
        return SimpleNamespace(memory=memory, tasks=[task]), task

    def test_persists_completed_task_output(self):
        from kasal_engine.events import TaskCompletedEvent, crewai_event_bus

        done = threading.Event()
        memory = MagicMock()
        memory.remember.side_effect = lambda *a, **k: done.set()
        crew, task = self._crew(memory)

        unregister = register_task_output_persistence(crew)
        try:
            crewai_event_bus.emit(
                task,
                TaskCompletedEvent(
                    output=SimpleNamespace(raw="42 facts found", agent="Researcher"),
                    task=task,
                ),
            )
            assert done.wait(timeout=5), "task output never persisted"
        finally:
            unregister()
        content = memory.remember.call_args.args[0]
        assert "research" in content and "42 facts found" in content
        assert memory.remember.call_args.kwargs["agent_role"] == "Researcher"

    def test_foreign_task_is_ignored(self):
        from kasal_engine.events import TaskCompletedEvent, crewai_event_bus

        memory = MagicMock()
        crew, _task = self._crew(memory)
        foreign = SimpleNamespace(id="other-task", name="other", description="")

        unregister = register_task_output_persistence(crew)
        try:
            crewai_event_bus.emit(
                foreign,
                TaskCompletedEvent(
                    output=SimpleNamespace(raw="leak", agent="X"), task=foreign
                ),
            )
            time.sleep(0.05)
        finally:
            unregister()
        memory.remember.assert_not_called()

    def test_sentinel_memory_returns_noop_unregister(self):
        crew = SimpleNamespace(memory=None, tasks=[])
        unregister = register_task_output_persistence(crew)
        unregister()  # no raise
