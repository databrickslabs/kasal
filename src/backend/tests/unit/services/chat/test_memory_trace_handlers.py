"""The chat path's memory trace rows — same shape as the crew/flow bridge rows.

What matters: the row carries the recall QUERY (so the timeline can show what
was asked), the retrieved record ids, and — for writes — the record id, all
scoped to THIS run's Memory instance.
"""

from types import SimpleNamespace

from src.services.chat.memory_trace_handlers import MemoryTraceHandlers, cap_text


def _harness(memory=None):
    traces: list = []
    logs: list = []

    def base_trace(event_type, output, tool_name):
        return {
            "event_type": event_type,
            "output": output,
            "trace_metadata": {"tool_name": tool_name},
        }

    h = MemoryTraceHandlers(
        agent_memory=memory,
        base_trace=base_trace,
        schedule_trace=traces.append,
        log=logs.append,
    )
    return h, traces, logs


class TestMemoryQuery:
    def test_row_carries_query_count_time_and_record_ids(self):
        memory = object()
        h, traces, logs = _harness(memory)
        results = [SimpleNamespace(id="r-1", content="x"), SimpleNamespace(id="r-2")]
        h.on_memory_query(
            memory,
            SimpleNamespace(
                query="latest news from Switzerland",
                results=results,
                query_time_ms=12.5,
            ),
        )
        assert len(traces) == 1
        td = traces[0]
        assert td["event_type"] == "memory_retrieval"
        extra = td["output"]["extra_data"]
        assert extra["query"] == "latest news from Switzerland"
        assert extra["results_count"] == 2
        assert extra["query_time_ms"] == 12.5
        assert extra["record_ids"] == ["r-1", "r-2"]
        # Mirrored into trace_metadata — where the timeline and pane read it.
        for key in ("query", "results_count", "record_ids"):
            assert td["trace_metadata"][key] == extra[key]
        assert logs == ["Memory read: 2 result(s)"]

    def test_query_is_capped_like_the_bridge(self):
        memory = object()
        h, traces, _ = _harness(memory)
        h.on_memory_query(memory, SimpleNamespace(query="q" * 900, results=[]))
        query = traces[0]["output"]["extra_data"]["query"]
        assert query.startswith("q" * 500) and query.endswith("…[truncated]")

    def test_empty_results_record_no_ids_but_still_the_query(self):
        memory = object()
        h, traces, _ = _harness(memory)
        h.on_memory_query(memory, SimpleNamespace(query="anything", results=[]))
        extra = traces[0]["output"]["extra_data"]
        assert extra["results_count"] == 0
        assert extra["query"] == "anything"
        assert "record_ids" not in extra

    def test_other_runs_memory_is_ignored(self):
        h, traces, _ = _harness(object())
        h.on_memory_query(object(), SimpleNamespace(query="x", results=[]))
        assert traces == []

    def test_never_raises(self):
        memory = object()
        h, traces, _ = _harness(memory)

        class Boom:
            @property
            def results(self):
                raise RuntimeError("bad event")

        h.on_memory_query(memory, Boom())
        assert traces == []


class TestRecordsSaved:
    def test_one_row_per_record_with_its_id(self):
        h, traces, logs = _harness(object())
        h.on_records_saved(
            [
                SimpleNamespace(
                    id="11111111-2222-3333-4444-555555555555", content="Remember A"
                ),
                SimpleNamespace(id=None, content="no id yet"),
            ]
        )
        assert [t["event_type"] for t in traces] == ["memory_write", "memory_write"]
        assert traces[0]["trace_metadata"]["record_id"] == (
            "11111111-2222-3333-4444-555555555555"
        )
        assert traces[0]["output"]["content"] == "Remember A"
        assert "record_id" not in traces[1]["trace_metadata"]
        assert logs == ["Memory write", "Memory write"]

    def test_none_is_a_noop(self):
        h, traces, _ = _harness(object())
        h.on_records_saved(None)
        assert traces == []


class TestMemoryRetrieval:
    def test_placeholder_when_nothing_matched(self):
        memory = object()
        h, traces, _ = _harness(memory)
        h.on_memory_retrieval(
            memory, SimpleNamespace(memory_content="", retrieval_time_ms=3.0)
        )
        td = traces[0]
        assert td["event_type"] == "memory_retrieval_completed"
        assert td["output"]["content"] == "(no memories matched the query)"
        assert td["trace_metadata"]["retrieval_time_ms"] == 3.0


def test_cap_text_marks_truncation():
    assert cap_text("abc", 8000) == "abc"
    assert cap_text("x" * 10, 4) == "xxxx…[truncated]"


class TestRecallPlanStamps:
    def test_distilled_query_and_rounds_ride_on_the_read_row(self):
        memory = object()
        h, traces, _ = _harness(memory)
        h.on_memory_query(
            memory,
            SimpleNamespace(
                query="x" * 300,
                distilled_query="latest Switzerland news today",
                exploration_rounds=1,
                results=[],
            ),
        )
        extra = traces[0]["output"]["extra_data"]
        assert extra["distilled_query"] == "latest Switzerland news today"
        assert extra["exploration_rounds"] == 1
        assert (
            traces[0]["trace_metadata"]["distilled_query"] == extra["distilled_query"]
        )
