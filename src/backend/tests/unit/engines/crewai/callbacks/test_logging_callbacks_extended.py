"""Extended unit tests for logging_callbacks module.

AgentTraceEventListener is a thin shell -- the OTel bridge handles all real
event subscriptions. Tests verify shell behaviour: initialization validation,
attribute storage, no-op setup_listeners, and the deprecated
TaskCompletionEventListener.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.engines.crewai.callbacks.logging_callbacks import (
    AgentTraceEventListener,
    TaskCompletionEventListener,
)


# ===========================================================================
# AgentTraceEventListener
# ===========================================================================

class TestAgentTraceEventListenerInit:
    """Initialization and parameter storage."""

    def test_stores_job_id(self):
        listener = AgentTraceEventListener(job_id="j1", group_context=None)
        assert listener.job_id == "j1"

    def test_stores_group_context(self):
        ctx = MagicMock()
        ctx.primary_group_id = "grp"
        listener = AgentTraceEventListener(job_id="j2", group_context=ctx)
        assert listener.group_context is ctx

    def test_stores_task_event_queue(self):
        queue = MagicMock()
        listener = AgentTraceEventListener(
            job_id="j3", group_context=None, task_event_queue=queue
        )
        assert listener._task_event_queue is queue

    def test_task_event_queue_defaults_to_none(self):
        listener = AgentTraceEventListener(job_id="j4", group_context=None)
        assert listener._task_event_queue is None

    def test_sets_init_time(self):
        before = datetime.now(timezone.utc)
        listener = AgentTraceEventListener(job_id="j5", group_context=None)
        after = datetime.now(timezone.utc)
        assert before <= listener._init_time <= after

    def test_register_global_events_accepted(self):
        # Parameter accepted without error even though it is unused
        listener = AgentTraceEventListener(
            job_id="j6", group_context=None, register_global_events=True
        )
        assert listener.job_id == "j6"


class TestAgentTraceEventListenerValidation:
    """Input validation for job_id."""

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            AgentTraceEventListener(job_id="", group_context=None)

    def test_rejects_none(self):
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            AgentTraceEventListener(job_id=None, group_context=None)

    def test_rejects_non_string(self):
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            AgentTraceEventListener(job_id=123, group_context=None)


class TestAgentTraceEventListenerSetupListeners:
    """setup_listeners is a no-op (OTel bridge handles events)."""

    def test_is_noop(self):
        listener = AgentTraceEventListener(job_id="noop1", group_context=None)
        mock_bus = MagicMock()
        listener.setup_listeners(mock_bus)
        # No handlers registered
        mock_bus.on.assert_not_called()

    def test_does_not_register_any_events(self):
        listener = AgentTraceEventListener(job_id="noop2", group_context=None)

        registered = {}

        def mock_on(event_class):
            def decorator(func):
                registered[event_class] = func
                return func
            return decorator

        bus = MagicMock()
        bus.on = mock_on
        listener.setup_listeners(bus)
        assert len(registered) == 0

    def test_callable_multiple_times(self):
        listener = AgentTraceEventListener(job_id="multi", group_context=None)
        bus = MagicMock()
        listener.setup_listeners(bus)
        listener.setup_listeners(bus)
        # Still no registrations
        bus.on.assert_not_called()


# ===========================================================================
# TaskCompletionEventListener
# ===========================================================================

class TestTaskCompletionEventListenerInit:
    """Initialization and parameter storage."""

    def test_stores_job_id(self):
        listener = TaskCompletionEventListener(job_id="t1", group_context=None)
        assert listener.job_id == "t1"

    def test_stores_group_context(self):
        ctx = MagicMock()
        listener = TaskCompletionEventListener(job_id="t2", group_context=ctx)
        assert listener.group_context is ctx

    def test_none_group_context(self):
        listener = TaskCompletionEventListener(job_id="t3", group_context=None)
        assert listener.group_context is None


class TestTaskCompletionEventListenerSetupListeners:
    """setup_listeners is a no-op."""

    def test_is_noop(self):
        listener = TaskCompletionEventListener(job_id="tn1", group_context=None)
        bus = MagicMock()
        listener.setup_listeners(bus)
        bus.on.assert_not_called()

    def test_returns_none(self):
        listener = TaskCompletionEventListener(job_id="tn2", group_context=None)
        result = listener.setup_listeners(MagicMock())
        assert result is None


# ===========================================================================
# CrewAI event imports (used by the OTel bridge)
# ===========================================================================

class TestCrewAIEventImports:
    """Verify that event types referenced by the OTel bridge are importable."""

    def test_core_events(self):
        from crewai.events import (
            AgentExecutionCompletedEvent,
            CrewKickoffStartedEvent,
            CrewKickoffCompletedEvent,
        )
        assert AgentExecutionCompletedEvent is not None
        assert CrewKickoffStartedEvent is not None
        assert CrewKickoffCompletedEvent is not None

    def test_base_event_listener(self):
        from crewai.events import BaseEventListener
        assert BaseEventListener is not None

    def test_agent_trace_inherits_base(self):
        from crewai.events import BaseEventListener
        assert issubclass(AgentTraceEventListener, BaseEventListener)

    def test_task_completion_inherits_base(self):
        from crewai.events import BaseEventListener
        assert issubclass(TaskCompletionEventListener, BaseEventListener)
