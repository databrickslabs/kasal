"""Tests for memory_hooks — recall preambles, async persist, crew wiring."""

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.services.memory.engine import MemoryRecord
from src.services.memory.run.persist import (
    flush_memory_writes,
    format_turn_for_memory,
    register_task_output_persistence,
    remember_async,
)
from src.services.memory.run.recall import (
    MEMORY_BLOCK_HEADER,
    build_memory_preamble,
    inject_task_memory,
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

    def test_strips_run_grounding_scaffold(self):
        """Every run shares the grounding wrapper; storing it made every record
        embed close to every scaffolded query (the off-topic-recall bug). The
        record must carry the REQUEST alone."""
        prompt = (
            "Respond directly and helpfully to the user's request. "
            "USER REQUEST — this run exists to answer it:\n"
            "create a diagram around genie ontology "
            "MCP data sources attached — query them for data questions."
        )
        text = format_turn_for_memory(prompt, "done")
        assert text == "User: create a diagram around genie ontology\nAssistant: done"

    def test_strips_expected_output_and_tool_hint(self):
        prompt = (
            "provide me swiss news : browser Expected output: "
            "A helpful, complete answer to the user's request."
        )
        text = format_turn_for_memory(prompt, "done")
        assert text == "User: provide me swiss news\nAssistant: done"


class TestRecallRelevanceCliff:
    """_select_records drops storage candidates far below the recall's best."""

    def _rec(self, content, sim):
        record = MemoryRecord(content=content, source="chat")
        record.metadata["similarity"] = sim
        return record

    def test_drops_the_far_tail_keeps_the_cluster(self):
        records = [
            self._rec("top match", 0.90),
            self._rec("close second", 0.85),
            self._rec("filler A", 0.60),
            self._rec("filler B", 0.58),
        ]
        block = build_memory_preamble(_memory_with(records), "the query")
        assert "top match" in block
        assert "close second" in block
        assert "filler A" not in block
        assert "filler B" not in block

    def test_unscored_records_pass_untouched(self):
        plain = MemoryRecord(content="no similarity stamp", source="chat")
        block = build_memory_preamble(
            _memory_with([self._rec("top match", 0.90), plain]), "the query"
        )
        assert "no similarity stamp" in block

    def test_cliff_width_is_env_tunable(self, monkeypatch):
        monkeypatch.setenv("KASAL_MEMORY_RECALL_MAX_DROP", "0.5")
        records = [self._rec("top match", 0.90), self._rec("filler A", 0.60)]
        block = build_memory_preamble(_memory_with(records), "the query")
        assert "filler A" in block  # 0.6 >= 0.9 - 0.5


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
        from src.core.events import TaskCompletedEvent, event_bus

        done = threading.Event()
        memory = MagicMock()
        memory.remember.side_effect = lambda *a, **k: done.set()
        crew, task = self._crew(memory)

        unregister = register_task_output_persistence(crew)
        try:
            event_bus.emit(
                task,
                TaskCompletedEvent(
                    output=SimpleNamespace(raw="42 facts found", agent="Researcher"),
                    task=task,
                ),
            )
            assert done.wait(timeout=5), "task output never persisted"
        finally:
            unregister()
        # The record is the ANSWER, alone. It used to be
        # "[crew task: X] {description}\nResult: {answer}" — the retrieval key
        # inside the retrieved document, so a task matched its own prior answers
        # at ~0.98 and was handed them back every run.
        content = memory.remember.call_args.args[0]
        assert content == "42 facts found"
        assert "research" not in content.lower()
        metadata = memory.remember.call_args.kwargs["metadata"]
        assert metadata["task_name"] == "research"
        assert metadata["task_description"] == "Find facts"
        assert memory.remember.call_args.kwargs["agent_role"] == "Researcher"

    def _sink_write(self, memory, **sink_kwargs):
        from src.services.memory.run.persist import make_memory_output_sink

        done = threading.Event()
        memory.remember.side_effect = lambda *a, **k: done.set()
        sink = make_memory_output_sink(memory, **sink_kwargs)
        _crew, task = self._crew(memory)
        sink(task=task, output=SimpleNamespace(raw="42 facts found", agent="R"))
        assert done.wait(timeout=5), "task output never persisted"
        return memory.remember.call_args.kwargs["metadata"]

    def test_record_is_stamped_with_the_run_that_wrote_it(self):
        # The wiring site passes the id (both crew paths do) …
        metadata = self._sink_write(MagicMock(), execution_id="job-42")
        assert metadata["execution_id"] == "job-42"

    def test_run_stamp_falls_back_to_the_execution_context(self):
        # … and the flow path is covered by the subprocess's execution
        # context, which the bootstrap sets before any crew runs.
        from src.services.execution.logs.context import execution_logging_context

        with execution_logging_context("job-ctx"):
            metadata = self._sink_write(MagicMock())
        assert metadata["execution_id"] == "job-ctx"

    def test_no_run_stamp_outside_a_run(self):
        metadata = self._sink_write(MagicMock())
        assert "execution_id" not in metadata
        assert metadata["task_name"] == "research"

    def test_foreign_task_is_ignored(self):
        from src.core.events import TaskCompletedEvent, event_bus

        memory = MagicMock()
        crew, _task = self._crew(memory)
        foreign = SimpleNamespace(id="other-task", name="other", description="")

        unregister = register_task_output_persistence(crew)
        try:
            event_bus.emit(
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


class TestRecallDefaultFloor:
    """Memory.recall applies the calibrated floor when the caller passes none."""

    def _memory(self):
        from unittest.mock import MagicMock

        from src.services.memory.engine.memory import Memory

        storage = MagicMock()
        storage.search.return_value = []
        return Memory(storage=storage), storage

    def test_default_floor_is_calibrated_and_env_tunable(self, monkeypatch):
        memory, storage = self._memory()
        memory.recall("q")
        assert storage.search.call_args.kwargs["score_threshold"] == 0.75

        monkeypatch.setenv("KASAL_MEMORY_RECALL_MIN_SCORE", "0.6")
        memory.recall("q")
        assert storage.search.call_args.kwargs["score_threshold"] == 0.6

    def test_explicit_zero_disables_the_floor(self):
        memory, storage = self._memory()
        memory.recall("q", score_threshold=0.0)
        assert storage.search.call_args.kwargs["score_threshold"] == 0.0


class TestCrewAIMatchSurface:
    """MemoryRecord serves as its own crewai MemoryMatch — the CrewAI engine
    attaches Kasal's Memory to a LiteAgent whose recall consumers read
    ``m.record.*`` and ``m.format()``. Without the surface, recalled memory
    raised inside crewai's try/except and was silently never injected."""

    def test_lite_agent_injection_expression_works(self):
        r = MemoryRecord(content="Rony Fahed is a basketball player", source="chat")
        r.metadata["similarity"] = 0.83
        # crewai.lite_agent._inject_memory_context, verbatim shape:
        block = "Relevant memories:\n" + "\n".join(f"- {m.record.content}" for m in [r])
        assert "basketball player" in block

    def test_memory_tools_dedupe_and_format(self):
        r = MemoryRecord(content="fact", source="chat", categories=["sports"])
        assert r.record.id == r.id  # tools/memory_tools dedupe key
        formatted = r.format()
        assert formatted.startswith("- (score=0.00) fact")
        assert "sports" in formatted
