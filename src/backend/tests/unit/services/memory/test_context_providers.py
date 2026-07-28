"""Tests for the engine's Crew.context_providers seam + the memory provider.

The seam is the P2 replacement for build-time task-description injection:
providers run when each task's CONTEXT is assembled (i.e., after prior tasks
completed), so memory recall queries can blend the static description with
what the crew has actually produced so far.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from src.services.execution.runtime.crew import Crew
from src.services.memory.engine import MemoryRecord
from src.services.memory.hooks import (
    MEMORY_BLOCK_HEADER,
    make_memory_context_provider,
)


def _crew_with_provider(provider):
    crew = Crew(agents=[], tasks=[])
    crew.context_providers.append(provider)
    return crew


class TestCrewContextProviders:
    def test_provider_output_is_appended_to_context(self):
        provider = MagicMock(return_value="MEMORY BLOCK")
        crew = _crew_with_provider(provider)
        task = SimpleNamespace(description="analyze", context=None, agent="agent-1")

        # task.context None → base context None → provider output stands alone.
        result = crew._context_for(task, completed=[], futures={})

        assert result == "MEMORY BLOCK"
        provider.assert_called_once_with(task=task, agent="agent-1", context=None)

    def test_provider_receives_prior_outputs_as_context(self):
        provider = MagicMock(return_value="RECALLED")
        crew = _crew_with_provider(provider)
        # NOT_SPECIFIED sentinel → all prior outputs form the base context.
        task = SimpleNamespace(
            description="analyze", context="NOT_SPECIFIED", agent=None
        )
        prior = (
            SimpleNamespace(),
            SimpleNamespace(raw="previous task said X"),
        )

        result = crew._context_for(task, completed=[prior], futures={})

        assert "previous task said X" in result
        assert "RECALLED" in result
        assert provider.call_args.kwargs["context"] == "previous task said X"

    def test_empty_provider_output_leaves_context_unchanged(self):
        crew = _crew_with_provider(MagicMock(return_value=""))
        task = SimpleNamespace(description="analyze", context=None, agent=None)
        assert crew._context_for(task, completed=[], futures={}) is None

    def test_provider_error_never_breaks_the_run(self):
        crew = _crew_with_provider(MagicMock(side_effect=RuntimeError("backend down")))
        task = SimpleNamespace(
            description="analyze", context="NOT_SPECIFIED", agent=None
        )
        prior = (SimpleNamespace(), SimpleNamespace(raw="prior output"))

        result = crew._context_for(task, completed=[prior], futures={})

        assert result == "prior output"

    def test_no_providers_is_zero_overhead_passthrough(self):
        crew = Crew(agents=[], tasks=[])
        task = SimpleNamespace(description="analyze", context=None, agent=None)
        assert crew._context_for(task, completed=[], futures={}) is None

    def test_copy_carries_providers(self):
        provider = MagicMock(return_value="X")
        crew = _crew_with_provider(provider)
        assert provider in crew.copy().context_providers


class TestMakeMemoryContextProvider:
    def test_none_for_sentinel_memory(self):
        assert make_memory_context_provider(None) is None
        assert make_memory_context_provider(True) is None
        assert make_memory_context_provider(False) is None

    def test_query_blends_description_and_context_tail(self):
        memory = MagicMock()
        memory.recall.return_value = [MemoryRecord(content="learned before")]
        provider = make_memory_context_provider(memory)
        task = SimpleNamespace(
            id="t1",
            name="analyze",
            description="Analyze churn",
            agent=SimpleNamespace(role="Analyst"),
        )

        block = provider(task=task, agent=task.agent, context="C" * 1000)

        assert block.startswith(MEMORY_BLOCK_HEADER)
        query = memory.recall.call_args.args[0]
        assert query.startswith("Analyze churn")
        # Only the TAIL of the runtime context rides along (last 500 chars).
        assert query.endswith("C" * 500)
        assert len(query) < 1000

    def test_task_without_description_recalls_nothing(self):
        memory = MagicMock()
        provider = make_memory_context_provider(memory)
        assert provider(task=SimpleNamespace(description=""), context=None) == ""
        memory.recall.assert_not_called()


class TestCrewOutputSinks:
    def test_sink_invoked_on_finish_task(self):
        sink = MagicMock()
        crew = Crew(agents=[], tasks=[])
        crew.output_sinks.append(sink)
        task = SimpleNamespace(description="analyze")
        output = SimpleNamespace(raw="result text", agent="Analyst")

        crew._finish_task(task, output)

        sink.assert_called_once_with(task=task, output=output)

    def test_sink_error_never_breaks_the_run(self):
        crew = Crew(agents=[], tasks=[])
        crew.output_sinks.append(MagicMock(side_effect=RuntimeError("db down")))
        crew._finish_task(SimpleNamespace(), SimpleNamespace(raw="x"))  # no raise

    def test_copy_carries_sinks(self):
        sink = MagicMock()
        crew = Crew(agents=[], tasks=[])
        crew.output_sinks.append(sink)
        assert sink in crew.copy().output_sinks


class TestMakeMemoryOutputSink:
    def test_none_for_sentinel_memory(self):
        from src.services.memory.hooks import make_memory_output_sink

        assert make_memory_output_sink(None) is None
        assert make_memory_output_sink(False) is None
        assert make_memory_output_sink(True) is None

    def test_sink_persists_task_output(self):
        import threading

        from src.services.memory.hooks import make_memory_output_sink

        done = threading.Event()
        memory = MagicMock()
        memory.recall.return_value = []
        memory.remember.side_effect = lambda *a, **k: done.set()
        sink = make_memory_output_sink(memory)
        task = SimpleNamespace(
            id="t1",
            name="research",
            description="Find facts",
            agent=SimpleNamespace(role="Researcher"),
        )

        sink(task=task, output=SimpleNamespace(raw="42 facts", agent="Researcher"))

        assert done.wait(timeout=5), "sink never persisted"
        content = memory.remember.call_args.args[0]
        assert "research" in content and "42 facts" in content

    def test_sink_skips_empty_output(self):
        import time as _time

        from src.services.memory.hooks import make_memory_output_sink

        memory = MagicMock()
        sink = make_memory_output_sink(memory)
        sink(task=SimpleNamespace(), output=SimpleNamespace(raw="  "))
        _time.sleep(0.05)
        memory.remember.assert_not_called()
