"""
Extended unit tests for logging_callbacks module.

AgentTraceEventListener is now a thin shell — OTel bridge handles all event
subscriptions, context tracking, and trace persistence. Tests verify the shell
behaviour: initialization, no-op setup_listeners, and that event types are
importable from crewai.events (used by the OTel bridge directly).
"""

import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone


class TestAgentTraceEventListenerShell:
    """Tests for the thin-shell AgentTraceEventListener."""

    def test_stores_job_id(self):
        """Test listener stores job_id."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        listener = AgentTraceEventListener(job_id="test_job_id", group_context=None)
        assert listener.job_id == "test_job_id"

    def test_stores_group_context(self):
        """Test listener stores group_context."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        group_context = MagicMock()
        group_context.primary_group_id = "group-123"

        listener = AgentTraceEventListener(job_id="test_job_id", group_context=group_context)
        assert listener.group_context == group_context

    def test_stores_task_event_queue(self):
        """Test listener stores task_event_queue parameter."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        queue = MagicMock()
        listener = AgentTraceEventListener(
            job_id="test_job_id", group_context=None, task_event_queue=queue
        )
        assert listener._task_event_queue == queue

    def test_sets_init_time(self):
        """Test listener sets _init_time on creation."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        before = datetime.now(timezone.utc)
        listener = AgentTraceEventListener(job_id="test_job_id", group_context=None)
        after = datetime.now(timezone.utc)

        assert before <= listener._init_time <= after

    def test_rejects_empty_job_id(self):
        """Test that empty job_id raises ValueError."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            AgentTraceEventListener(job_id="", group_context=None)

    def test_rejects_none_job_id(self):
        """Test that None job_id raises ValueError."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            AgentTraceEventListener(job_id=None, group_context=None)

    def test_setup_listeners_is_noop(self):
        """Test that setup_listeners registers no handlers (OTel bridge handles events)."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        listener = AgentTraceEventListener(job_id="noop_test", group_context=None)

        mock_event_bus = MagicMock()
        listener.setup_listeners(mock_event_bus)

        mock_event_bus.on.assert_not_called()

    def test_setup_listeners_does_not_register_agent_execution(self):
        """Test that AgentExecutionCompletedEvent is NOT registered (OTel handles it)."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        listener = AgentTraceEventListener(job_id="no_reg_test", group_context=None)

        registered = {}

        def mock_on(event_class):
            def decorator(func):
                registered[event_class] = func
                return func
            return decorator

        mock_event_bus = MagicMock()
        mock_event_bus.on = mock_on

        listener.setup_listeners(mock_event_bus)

        assert len(registered) == 0

    def test_setup_listeners_does_not_register_crew_kickoff(self):
        """Test that crew kickoff events are NOT registered (OTel handles them)."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        listener = AgentTraceEventListener(job_id="no_kickoff_test", group_context=None)

        registered = {}

        def mock_on(event_class):
            def decorator(func):
                registered[event_class] = func
                return func
            return decorator

        mock_event_bus = MagicMock()
        mock_event_bus.on = mock_on

        listener.setup_listeners(mock_event_bus)

        assert len(registered) == 0


class TestTaskCompletionEventListenerShell:
    """Tests for the deprecated TaskCompletionEventListener shell."""

    def test_stores_attributes(self):
        """Test initialization stores job_id and group_context."""
        from src.engines.crewai.callbacks.logging_callbacks import TaskCompletionEventListener

        group_context = MagicMock()
        listener = TaskCompletionEventListener(job_id="task_test", group_context=group_context)

        assert listener.job_id == "task_test"
        assert listener.group_context == group_context

    def test_setup_listeners_is_noop(self):
        """Test that setup_listeners registers no handlers."""
        from src.engines.crewai.callbacks.logging_callbacks import TaskCompletionEventListener

        listener = TaskCompletionEventListener(job_id="noop_test", group_context=None)

        mock_event_bus = MagicMock()
        listener.setup_listeners(mock_event_bus)

        mock_event_bus.on.assert_not_called()


class TestLLMStreamHandling:
    """Verify LLM stream handler is NOT registered by the shell (OTel handles it)."""

    def test_llm_stream_handler_not_registered(self):
        """LLMStreamChunkEvent should NOT be registered by the listener shell."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        try:
            from crewai.events import LLMStreamChunkEvent  # noqa: F401
        except ImportError:
            pytest.skip("LLMStreamChunkEvent not available in this CrewAI version")

        listener = AgentTraceEventListener(job_id="stream_test", group_context=None)

        registered = {}

        def mock_on(event_class):
            def decorator(func):
                registered[event_class] = func
                return func
            return decorator

        mock_event_bus = MagicMock()
        mock_event_bus.on = mock_on

        listener.setup_listeners(mock_event_bus)

        assert LLMStreamChunkEvent not in registered


class TestCrewAIEventImports:
    """Verify CrewAI event types used by the OTel bridge are importable."""

    def test_core_events_importable(self):
        """Test core CrewAI events can be imported."""
        from crewai.events import (
            AgentExecutionCompletedEvent,
            CrewKickoffStartedEvent,
            CrewKickoffCompletedEvent,
        )
        assert AgentExecutionCompletedEvent is not None
        assert CrewKickoffStartedEvent is not None
        assert CrewKickoffCompletedEvent is not None

    def test_task_events_importable(self):
        """Test task events can be imported."""
        try:
            from crewai.events import TaskStartedEvent, TaskCompletedEvent
            assert TaskStartedEvent is not None
            assert TaskCompletedEvent is not None
        except ImportError:
            pytest.skip("Task events not available in this CrewAI version")

    def test_llm_events_importable(self):
        """Test LLM call events can be imported."""
        try:
            from crewai.events import LLMCallStartedEvent, LLMCallCompletedEvent
            assert LLMCallStartedEvent is not None
            assert LLMCallCompletedEvent is not None
        except ImportError:
            pytest.skip("LLM call events not available in this CrewAI version")

    def test_guardrail_events_importable(self):
        """Test guardrail event types can be imported."""
        try:
            from crewai.events.types.llm_guardrail_events import (
                LLMGuardrailStartedEvent,
                LLMGuardrailCompletedEvent,
                LLMGuardrailFailedEvent,
            )
            assert LLMGuardrailStartedEvent is not None
            assert LLMGuardrailCompletedEvent is not None
            assert LLMGuardrailFailedEvent is not None
        except ImportError:
            pytest.skip("LLM Guardrail events not available in this CrewAI version")

    def test_memory_events_importable(self):
        """Test memory events can be imported."""
        try:
            from crewai.events import (
                ShortTermMemoryUpdatedEvent,
                LongTermMemoryUpdatedEvent,
            )
            assert ShortTermMemoryUpdatedEvent is not None
            assert LongTermMemoryUpdatedEvent is not None
        except ImportError:
            pytest.skip("Memory events not available in this CrewAI version")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
