"""Engine guardrail events — regression suite for the invisible-guardrails bug.

After the crewAI migration, guardrails executed but emitted nothing: the
LLMGuardrail*Event classes were never ported, ``Task._apply_guardrails`` was
uninstrumented, and the OTel bridge's subscriptions silently skipped. These
tests pin the emission contract the bridge and trace UI consume.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.events import (
    LLMGuardrailCompletedEvent,
    LLMGuardrailFailedEvent,
    LLMGuardrailStartedEvent,
    event_bus,
)
from src.services.execution.runtime.guardrail import LLMGuardrail
from src.services.execution.runtime.task import Task
from src.services.execution.runtime.types import TaskOutput


@pytest.fixture
def captured_events():
    """Capture guardrail events; snapshot/restore the global bus handlers."""
    snapshot = {k: list(v) for k, v in event_bus._handlers.items()}
    events = []

    def _capture(source, event):
        events.append(event)

    for event_cls in (
        LLMGuardrailStartedEvent,
        LLMGuardrailCompletedEvent,
        LLMGuardrailFailedEvent,
    ):
        event_bus.register_handler(event_cls, _capture)
    yield events
    event_bus._handlers = snapshot


def _task(guardrail, max_retries=None):
    return Task(
        description="Collect Swiss news",
        expected_output="A report",
        name="collect news",
        guardrail=guardrail,
        max_retries=max_retries,
    )


def _output(raw="the report"):
    return TaskOutput(description="Collect Swiss news", agent="Reporter", raw=raw)


class TestEmissionOnPass:
    def test_started_and_completed_success(self, captured_events):
        task = _task(lambda output: (True, output.raw))

        task._apply_guardrails(_output(), agent=MagicMock(), context=None, tools=None)

        types = [type(e).__name__ for e in captured_events]
        assert types == ["LLMGuardrailStartedEvent", "LLMGuardrailCompletedEvent"]
        started, completed = captured_events
        assert started.retry_count == 0
        assert completed.success is True
        assert completed.error is None
        # Task attribution stamped explicitly (bridge groups rows under the task).
        assert started.task_id == str(task.id)
        assert started.task_name == "collect news"


class TestEmissionOnRetry:
    def test_fail_then_pass_emits_two_windows(self, captured_events):
        verdicts = iter([(False, "too short"), (True, None)])
        agent = MagicMock()
        agent.execute_task.return_value = "a longer report"
        task = _task(lambda output: next(verdicts))

        result = task._apply_guardrails(
            _output(), agent=agent, context=None, tools=None
        )

        types = [type(e).__name__ for e in captured_events]
        assert types == [
            "LLMGuardrailStartedEvent",
            "LLMGuardrailCompletedEvent",  # success=False
            "LLMGuardrailStartedEvent",
            "LLMGuardrailCompletedEvent",  # success=True
        ]
        assert captured_events[1].success is False
        assert captured_events[1].error == "too short"
        assert captured_events[2].retry_count == 1
        assert captured_events[3].success is True
        agent.execute_task.assert_called_once()
        assert result.raw == "a longer report"


class TestEmissionOnExhaustion:
    def test_failed_event_then_raise(self, captured_events):
        agent = MagicMock()
        agent.execute_task.return_value = "still bad"
        task = _task(lambda output: (False, "nope"), max_retries=1)

        with pytest.raises(ValueError, match="guardrail failed"):
            task._apply_guardrails(_output(), agent=agent, context=None, tools=None)

        failed = [e for e in captured_events if isinstance(e, LLMGuardrailFailedEvent)]
        assert len(failed) == 1
        assert failed[0].error == "nope"
        assert failed[0].retry_count == 1

    def test_max_retries_zero_fails_on_first_attempt(self, captured_events):
        agent = MagicMock()
        task = _task(lambda output: (False, "rejected"), max_retries=0)

        with pytest.raises(ValueError):
            task._apply_guardrails(_output(), agent=agent, context=None, tools=None)

        agent.execute_task.assert_not_called()
        types = [type(e).__name__ for e in captured_events]
        assert types == [
            "LLMGuardrailStartedEvent",
            "LLMGuardrailCompletedEvent",
            "LLMGuardrailFailedEvent",
        ]


class TestGuardrailLabel:
    def test_llm_guardrail_uses_description(self):
        guardrail = LLMGuardrail(
            description="Must include 5-7 headlines", llm=MagicMock()
        )
        assert Task._guardrail_label(guardrail) == "Must include 5-7 headlines"

    def test_wrapper_bound_method_uses_inner_type(self):
        class CompanyCountGuardrail:
            pass

        wrapper = SimpleNamespace(guardrail=CompanyCountGuardrail())
        bound = SimpleNamespace(__self__=wrapper)
        assert Task._guardrail_label(bound) == "CompanyCountGuardrail"

    def test_plain_function_uses_qualname(self):
        def my_check(output):
            return True, None

        assert "my_check" in Task._guardrail_label(my_check)

    def test_no_guardrail_no_events(self, captured_events):
        task = Task(description="d", expected_output="e")
        task._apply_guardrails(_output(), agent=MagicMock(), context=None, tools=None)
        assert captured_events == []


class TestRetryOnFailMapping:
    """task_builder maps the app's retry_on_fail flag onto engine retries."""

    @pytest.mark.asyncio
    async def test_explicit_false_disables_retries(self):
        from src.services.execution.kernel.task_builder import build_task_args

        args = await build_task_args(
            {"description": "D", "expected_output": "E", "retry_on_fail": False},
            MagicMock(),
            [],
        )
        assert args["max_retries"] == 0
        assert "retry_on_fail" not in args

    @pytest.mark.asyncio
    async def test_true_or_absent_keeps_engine_retries(self):
        from src.services.execution.kernel.task_builder import build_task_args

        for config in (
            {"description": "D", "expected_output": "E"},
            {"description": "D", "expected_output": "E", "retry_on_fail": True},
        ):
            args = await build_task_args(config, MagicMock(), [])
            assert args["max_retries"] == 3
            assert "retry_on_fail" not in args

    @pytest.mark.asyncio
    async def test_explicit_max_retries_wins(self):
        from src.services.execution.kernel.task_builder import build_task_args

        args = await build_task_args(
            {"description": "D", "expected_output": "E", "max_retries": 5},
            MagicMock(),
            [],
        )
        assert args["max_retries"] == 5
