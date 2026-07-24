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

from kasal_engine.memory import Memory
from src.engines.kasal.memory.engine_storage_adapter import EngineStorageAdapter
from src.engines.kasal.memory.local_storage_backend import LocalMemoryStorage
from src.engines.kasal.memory.memory_hooks import (
    inject_task_memory,
    register_task_output_persistence,
)
from src.services.otel_tracing.event_bridge import OTelEventBridge


def _embedder(texts):
    return [[1.0, 0.0] for _ in texts]


@pytest.fixture
def bus():
    """Global engine bus with handler snapshot/restore (bridge has no off())."""
    from kasal_engine.events import crewai_event_bus

    snapshot = {k: list(v) for k, v in crewai_event_bus._handlers.items()}
    yield crewai_event_bus
    crewai_event_bus._handlers = snapshot


@pytest.fixture
def spans(bus):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    bridge = OTelEventBridge(provider.get_tracer("test-bridge"), "job-1", None)
    bridge.register(bus)
    return exporter


def _memory(tmp_path) -> Memory:
    backend = LocalMemoryStorage(tmp_path / "m.db", embedder=_embedder)
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
        memory.remember("previous run learned the Swiss market grew 4%")
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

        from kasal_engine.events import TaskCompletedEvent, crewai_event_bus

        unregister = register_task_output_persistence(crew)
        try:
            crewai_event_bus.emit(
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

    def test_flush_guarantees_write_before_subprocess_exit(self, tmp_path, spans):
        """The crew subprocess flushes after kickoff: once flush returns, the
        record is in storage and the Memory Write span is exported — nothing
        left to die with the interpreter."""
        from kasal_engine.events import TaskCompletedEvent, crewai_event_bus
        from src.engines.kasal.memory.memory_hooks import flush_memory_writes

        backend = LocalMemoryStorage(tmp_path / "m.db", embedder=_embedder)
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
            crewai_event_bus.emit(
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
