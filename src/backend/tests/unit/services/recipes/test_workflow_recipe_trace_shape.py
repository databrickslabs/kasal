"""What a run DID, read from the trace the engine actually writes.

These tests exist because the reader and its fixtures previously agreed with
each other and disagreed with reality: ``tool_name`` was read from ``output``,
where it never is, so every recipe in a real workspace recorded
``tool_names=[]``, ``tool_call_count=0`` and ``error_span_count=0`` — including
runs with 59 tool calls, 3 tool failures and a guardrail that rejected the
output three times.

So the fixtures here are shaped like real spans, verified against
``execution_trace`` rows from live runs:

    event_type      'tool_usage'                      (start AND completion)
    span_name       'CrewAI.tool.execute' | '.complete' | 'kasal.guardrail.failed'
    trace_metadata  {'tool_name': ..., 'tool_args': {...}}
    output          {'content': '...', 'duration_ms': ..., 'extra_data': {...}}

A tool FAILURE is text inside ``output.content`` ("Tool error: ..."), not an
event type — the engine returns it to the model as a string rather than raising.
"""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.services.recipes.recipes import WorkflowRecipeService


async def _factory():
    from src.models.execution_trace import ExecutionTrace

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(ExecutionTrace.__table__.create)
    return async_sessionmaker(engine, expire_on_commit=False)


def _tool_span(job, name, args=None, *, complete=False, content="ok"):
    from src.models.execution_trace import ExecutionTrace

    return ExecutionTrace(
        job_id=job,
        event_source="agent",
        event_context="Analyst",
        event_type="tool_usage",
        span_name="CrewAI.tool.complete" if complete else "CrewAI.tool.execute",
        trace_metadata={"tool_name": name, "tool_args": args or {}},
        output={"content": content, "duration_ms": 1.0, "extra_data": {}},
    )


def _guardrail_span(job, failed=False):
    from src.models.execution_trace import ExecutionTrace

    return ExecutionTrace(
        job_id=job,
        event_source="crew",
        event_context="crew",
        # NOTE: a pass and a rejection carry the SAME event_type. Only the span
        # name distinguishes them, which is why span_name has to be projected.
        event_type="llm_guardrail",
        span_name="kasal.guardrail.failed" if failed else "kasal.guardrail.completed",
        output={"content": "verdict"},
    )


async def _shape(spans, job="job-1"):
    factory = await _factory()
    async with factory() as session:
        for span in spans:
            session.add(span)
        await session.commit()
    async with factory() as session:
        return await WorkflowRecipeService(session)._trace_shape(job)


class TestToolExtraction:
    @pytest.mark.asyncio
    async def test_tool_names_come_from_trace_metadata(self):
        """The bug in one assertion: read from ``output`` and this is empty."""
        shape = await _shape(
            [
                _tool_span("job-1", "GenieTool", {"q": "x"}),
                _tool_span("job-1", "database_execute_sql", {"sql": "SELECT 1"}),
            ]
        )
        assert shape["tool_names"] == ["GenieTool", "database_execute_sql"]
        assert shape["tool_call_count"] == 2

    @pytest.mark.asyncio
    async def test_a_call_is_counted_once_not_twice(self):
        """Every call emits a start and a completion span, both typed
        'tool_usage'. Counting both doubles every number in the recipe."""
        shape = await _shape(
            [
                _tool_span("job-1", "GenieTool"),
                _tool_span("job-1", "GenieTool", complete=True),
            ]
        )
        assert shape["tool_call_count"] == 1

    @pytest.mark.asyncio
    async def test_repeated_identical_calls_show_up_as_looping(self):
        """59 calls of which 12 are distinct is a crew that could not converge.
        The ratio is the signal; neither number alone says it."""
        spans = [
            _tool_span("job-1", "describe_table", {"t": "listings"}) for _ in range(5)
        ]
        spans.append(_tool_span("job-1", "describe_table", {"t": "other"}))
        shape = await _shape(spans)
        assert shape["tool_call_count"] == 6
        assert shape["distinct_tool_call_count"] == 2

    @pytest.mark.asyncio
    async def test_argument_key_order_does_not_invent_distinct_calls(self):
        shape = await _shape(
            [
                _tool_span("job-1", "t", {"a": 1, "b": 2}),
                _tool_span("job-1", "t", {"b": 2, "a": 1}),
            ]
        )
        assert shape["distinct_tool_call_count"] == 1

    @pytest.mark.asyncio
    async def test_other_jobs_are_not_mixed_in(self):
        shape = await _shape(
            [_tool_span("job-1", "Mine"), _tool_span("other-job", "Theirs")]
        )
        assert shape["tool_names"] == ["Mine"]


class TestErrorCounting:
    @pytest.mark.asyncio
    async def test_a_failing_tool_is_counted(self):
        """The failure is TEXT in the completion's content — the engine hands
        the model a string instead of raising, so there is no error span."""
        shape = await _shape(
            [
                _tool_span("job-1", "Parse_call_endpoint"),
                _tool_span(
                    "job-1",
                    "Parse_call_endpoint",
                    complete=True,
                    content='Tool error: {"ok": false, "error": {"status_code": 503}}',
                ),
            ]
        )
        assert shape["error_span_count"] == 1

    @pytest.mark.asyncio
    async def test_a_succeeding_tool_is_not_counted(self):
        shape = await _shape(
            [
                _tool_span("job-1", "GenieTool"),
                _tool_span("job-1", "GenieTool", complete=True, content='{"rows": []}'),
            ]
        )
        assert shape["error_span_count"] == 0

    @pytest.mark.asyncio
    async def test_a_guardrail_rejection_is_counted(self):
        shape = await _shape([_guardrail_span("job-1", failed=True)])
        assert shape["guardrail_failure_count"] == 1
        assert shape["error_span_count"] == 1

    @pytest.mark.asyncio
    async def test_a_guardrail_pass_is_not(self):
        """Pass and rejection share an event_type; only span_name separates
        them. Getting this wrong would reject every run that HAS a guardrail."""
        shape = await _shape([_guardrail_span("job-1", failed=False)])
        assert shape["guardrail_failure_count"] == 0
        assert shape["error_span_count"] == 0


class TestSignalsAreNotColumns:
    @pytest.mark.asyncio
    async def test_the_decision_signals_are_present_for_the_caller(self):
        """They inform mine-or-skip; the mine loop strips them before writing,
        so adding one never needs a migration."""
        shape = await _shape([_tool_span("job-1", "T")])
        assert "distinct_tool_call_count" in shape
        assert "guardrail_failure_count" in shape

    @pytest.mark.asyncio
    async def test_persisted_keys_are_all_real_columns(self):
        """Guards the other direction: a persisted key that is not a column
        fails silently on setattr and is never written."""
        from src.models.workflow_recipe import WorkflowRecipe
        from src.services.recipes.recipes import _SHAPE_SIGNALS

        shape = await _shape([_tool_span("job-1", "T")])
        columns = {c.name for c in WorkflowRecipe.__table__.columns}
        for key in shape:
            if key in _SHAPE_SIGNALS:
                continue
            assert key in columns, f"{key} is written but is not a column"
