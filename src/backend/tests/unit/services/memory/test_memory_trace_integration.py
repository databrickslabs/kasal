"""Memory events must reach the trace timeline (hooks → bus → OTel bridge).

End-to-end through real components: memory_hooks drive a real ``Memory`` over
the local SQLite backend; the real ``OTelEventBridge`` listens on the real
engine event bus; spans land in an in-memory OTel exporter. Asserts the span
names / event types the db_exporter + frontend timeline consume, and the task
attribution that keeps rows out of the "Unassigned" bucket.
"""

import time
from types import SimpleNamespace

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from src.services.memory.engine import Memory
from src.services.memory.run.persist import register_task_output_persistence
from src.services.memory.run.recall import inject_task_memory
from src.services.memory.storage.adapter import EngineStorageAdapter
from src.services.memory.storage.local import LocalStorageBackend
from src.services.otel_tracing.event_bridge import OTelEventBridge


def _embedder(texts):
    return [[1.0, 0.0] for _ in texts]


@pytest.fixture
def bus():
    """Global engine bus with handler snapshot/restore (bridge has no off())."""
    from src.core.events import event_bus

    snapshot = {k: list(v) for k, v in event_bus._handlers.items()}
    yield event_bus
    event_bus._handlers = snapshot


@pytest.fixture
def spans(bus):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    bridge = OTelEventBridge(provider.get_tracer("test-bridge"), "job-1", None)
    bridge.register(bus)
    return exporter


def _memory(tmp_path) -> Memory:
    backend = LocalStorageBackend(tmp_path / "m.db", embedder=_embedder)
    return Memory(storage=EngineStorageAdapter(backend), root_scope="/g1")


def _wait_for_span(exporter, name, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        matches = [s for s in exporter.get_finished_spans() if s.name == name]
        if matches:
            return matches
        time.sleep(0.05)
    raise AssertionError(
        f"span {name!r} never exported; got "
        f"{[s.name for s in exporter.get_finished_spans()]}"
    )


class TestRecallReachesTrace:
    def test_task_injection_emits_attributed_query_spans(self, tmp_path, spans):
        memory = _memory(tmp_path)
        stored = memory.remember("previous run learned the Swiss market grew 4%")
        task = SimpleNamespace(
            id="task-123",
            name="collect news",
            description="Collect Swiss market news",
            agent=SimpleNamespace(role="News Specialist"),
        )

        assert inject_task_memory(memory, [task]) == 1

        completed = _wait_for_span(spans, "kasal.memory.query_completed")
        attrs = dict(completed[0].attributes)
        assert attrs["kasal.event_type"] == "memory_retrieval"
        # Attribution: pre-kickoff recall still lands under ITS task.
        assert attrs["kasal.extra.task_id"] == "task-123"
        assert attrs["kasal.task_name"] == "collect news"
        assert attrs["kasal.agent_name"] == "News Specialist"
        # The ids of what was recalled, structured — the run's "Recalled" view
        # resolves on these rather than parsing the capped content.
        assert attrs["kasal.extra.results_count"] == 1
        assert list(attrs["kasal.extra.record_ids"]) == [stored.id]


class TestPersistReachesTrace:
    def test_task_completion_emits_attributed_save_spans(self, tmp_path, spans):
        memory = _memory(tmp_path)
        task = SimpleNamespace(
            id="task-9",
            name="summarize",
            description="Summarize findings",
            agent=SimpleNamespace(role="Writer"),
        )
        crew = SimpleNamespace(memory=memory, tasks=[task])
        saved: list = []
        memory.add_save_hook(lambda records: saved.extend(records))

        from src.core.events import TaskCompletedEvent, event_bus

        unregister = register_task_output_persistence(crew)
        try:
            event_bus.emit(
                task,
                TaskCompletedEvent(
                    output=SimpleNamespace(raw="Summary: growth 4%", agent="Writer"),
                    task=task,
                ),
            )
            completed = _wait_for_span(spans, "kasal.memory.save_completed")
        finally:
            unregister()

        attrs = dict(completed[0].attributes)
        assert attrs["kasal.event_type"] == "memory_write"
        # Attribution survives BOTH thread hops (writer pool + Memory's own
        # save pool) via contextvars copies.
        assert attrs["kasal.extra.task_id"] == "task-9"
        assert attrs["kasal.agent_name"] == "Writer"
        # The stored record's id rides on the Memory Write row: it is what the
        # run's "Saved" view resolves on (crew and flow runs have no other
        # trace writer than this bridge).
        assert len(saved) == 1
        assert attrs["kasal.extra.record_id"] == saved[0].id

    def test_flush_guarantees_write_before_subprocess_exit(self, tmp_path, spans):
        """The crew subprocess flushes after kickoff: once flush returns, the
        record is in storage and the Memory Write span is exported — nothing
        left to die with the interpreter."""
        from src.core.events import TaskCompletedEvent, event_bus
        from src.services.memory.run.persist import flush_memory_writes

        backend = LocalStorageBackend(tmp_path / "m.db", embedder=_embedder)
        memory = Memory(storage=EngineStorageAdapter(backend), root_scope="/g1")
        task = SimpleNamespace(
            id="task-final",
            name="final task",
            description="Wrap up",
            agent=SimpleNamespace(role="Closer"),
        )
        crew = SimpleNamespace(memory=memory, tasks=[task])

        unregister = register_task_output_persistence(crew)
        try:
            event_bus.emit(
                task,
                TaskCompletedEvent(
                    output=SimpleNamespace(raw="done and dusted", agent="Closer"),
                    task=task,
                ),
            )
            assert flush_memory_writes(timeout=10.0) == 0
        finally:
            unregister()

        # No polling: flush IS the synchronization barrier.
        assert backend.count("/g1") == 1
        names = [s.name for s in spans.get_finished_spans()]
        assert "kasal.memory.save_completed" in names

    def test_record_is_stamped_with_the_subprocess_execution(self, tmp_path, spans):
        """Through the REAL write path (hooks → writer pool → Memory → SQLite):
        a task output persisted inside a run's execution context lands with
        that run's id in its metadata, and the same id-less rows a pruned trace
        would leave behind still say which run wrote them."""
        from src.core.events import TaskCompletedEvent, event_bus
        from src.services.execution.logs.context import execution_logging_context
        from src.services.memory.run.persist import flush_memory_writes

        backend = LocalStorageBackend(tmp_path / "m.db", embedder=_embedder)
        memory = Memory(storage=EngineStorageAdapter(backend), root_scope="/g1")
        saved: list = []
        memory.add_save_hook(lambda records: saved.extend(records))
        task = SimpleNamespace(
            id="task-ctx",
            name="stamped task",
            description="Prove the stamp",
            agent=SimpleNamespace(role="Writer"),
        )
        crew = SimpleNamespace(memory=memory, tasks=[task])

        unregister = register_task_output_persistence(crew)
        try:
            with execution_logging_context("job-e2e-1"):
                event_bus.emit(
                    task,
                    TaskCompletedEvent(
                        output=SimpleNamespace(raw="stamped output", agent="Writer"),
                        task=task,
                    ),
                )
            assert flush_memory_writes(timeout=10.0) == 0
        finally:
            unregister()

        assert len(saved) == 1
        assert saved[0].metadata["execution_id"] == "job-e2e-1"
        assert saved[0].metadata["task_name"] == "stamped task"
        stored = backend.get_record(saved[0].id)
        assert stored is not None
        assert stored.metadata["execution_id"] == "job-e2e-1"
