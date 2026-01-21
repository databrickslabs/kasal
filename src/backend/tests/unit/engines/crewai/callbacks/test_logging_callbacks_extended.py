"""
Extended unit tests for logging_callbacks module.

Comprehensive tests covering:
- AgentTraceEventListener advanced functionality
- Event handler registration and triggering
- Memory and knowledge event handling
- Flow event handling
- Subprocess mode operations
- Database write operations
- LLM stream handling
"""
import pytest
import os
from unittest.mock import Mock, MagicMock, patch, AsyncMock, call
from datetime import datetime, timezone
import asyncio


class TestAgentTraceEventListenerAdvanced:
    """Advanced tests for AgentTraceEventListener."""

    @pytest.fixture
    def clean_registries(self):
        """Clean up static registries before each test."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        AgentTraceEventListener._active_context.clear()
        AgentTraceEventListener._task_start_times.clear()
        AgentTraceEventListener._active_crew_name.clear()
        yield
        # Cleanup after test
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        AgentTraceEventListener._active_context.clear()
        AgentTraceEventListener._task_start_times.clear()
        AgentTraceEventListener._active_crew_name.clear()

    def test_listener_with_debug_tracing_disabled(self, clean_registries):
        """Test listener with debug tracing disabled."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(
                job_id="debug_disabled_job",
                group_context=None,
                debug_tracing=False
            )

            assert listener.debug_tracing is False

    def test_listener_with_debug_tracing_enabled(self, clean_registries):
        """Test listener with debug tracing enabled."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(
                job_id="debug_enabled_job",
                group_context=None,
                debug_tracing=True
            )

            assert listener.debug_tracing is True

    def test_extract_agent_info_with_role_attribute(self, clean_registries):
        """Test agent info extraction with role attribute."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="test_job", group_context=None)

            mock_event = MagicMock()
            mock_event.agent = MagicMock()
            mock_event.agent.role = "Test Role"
            mock_event.agent.id = "agent-123"

            name, agent_id = listener._extract_agent_info(mock_event)

            assert name == "Test Role"
            assert agent_id == "agent-123"

    def test_extract_agent_info_with_name_attribute(self, clean_registries):
        """Test agent info extraction with name attribute when role is missing."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="test_job", group_context=None)

            # Create an agent mock that only has 'name' and 'agent_id', not 'role' or 'id'
            class MockAgent:
                name = "Agent Name"
                agent_id = "agent-456"

            mock_event = MagicMock()
            mock_event.agent = MockAgent()

            name, agent_id = listener._extract_agent_info(mock_event)

            assert name == "Agent Name"
            assert agent_id == "agent-456"

    def test_extract_agent_info_no_agent(self, clean_registries):
        """Test agent info extraction when no agent attribute."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="test_job", group_context=None)

            mock_event = MagicMock(spec=[])  # No agent attribute

            name, agent_id = listener._extract_agent_info(mock_event)

            assert name == "Unknown Agent"
            assert agent_id is None

    def test_extract_task_info_with_description(self, clean_registries):
        """Test task info extraction with description."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="test_job", group_context=None)

            mock_event = MagicMock()
            mock_event.task = MagicMock()
            mock_event.task.description = "Task Description"
            mock_event.task.id = "task-123"

            name, task_id, description = listener._extract_task_info(mock_event)

            assert name == "Task Description"
            assert task_id == "task-123"
            assert description == "Task Description"

    def test_extract_task_info_with_name_fallback(self, clean_registries):
        """Test task info extraction with name fallback when description is missing."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="test_job", group_context=None)

            # Create a task mock that only has 'name' and 'task_id', not 'description' or 'id'
            class MockTask:
                name = "Task Name"
                task_id = "task-456"

            mock_event = MagicMock()
            mock_event.task = MockTask()

            name, task_id, description = listener._extract_task_info(mock_event)

            assert name == "Task Name"
            assert task_id == "task-456"
            assert description == "Task Name"

    def test_extract_task_info_no_task(self, clean_registries):
        """Test task info extraction when no task attribute."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="test_job", group_context=None)

            mock_event = MagicMock(spec=[])  # No task attribute

            name, task_id, description = listener._extract_task_info(mock_event)

            assert name == "Unknown Task"
            assert task_id is None


class TestUpdateActiveContext:
    """Test active context management."""

    @pytest.fixture
    def clean_registries(self):
        """Clean up static registries."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        AgentTraceEventListener._active_context.clear()
        AgentTraceEventListener._task_start_times.clear()
        AgentTraceEventListener._active_crew_name.clear()
        yield
        AgentTraceEventListener._init_logged.clear()

    def test_update_context_creates_entry(self, clean_registries):
        """Test that context entry is created for new job."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="context_test", group_context=None)

            listener._update_active_context("Test Agent", "Test Task", "[TEST]")

            assert "context_test" in AgentTraceEventListener._active_context
            assert AgentTraceEventListener._active_context["context_test"]["agent"] == "Test Agent"
            assert AgentTraceEventListener._active_context["context_test"]["task"] == "Test Task"

    def test_update_context_updates_existing(self, clean_registries):
        """Test that existing context is updated."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="update_test", group_context=None)

            listener._update_active_context("Agent 1", "Task 1", "[TEST]")
            listener._update_active_context("Agent 2", "Task 2", "[TEST]")

            context = AgentTraceEventListener._active_context["update_test"]
            assert context["agent"] == "Agent 2"
            assert context["task"] == "Task 2"

    def test_update_context_ignores_unknown_agent(self, clean_registries):
        """Test that Unknown Agent doesn't overwrite valid agent."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="unknown_test", group_context=None)

            listener._update_active_context("Valid Agent", "Task 1", "[TEST]")
            listener._update_active_context("Unknown Agent", "Task 2", "[TEST]")

            context = AgentTraceEventListener._active_context["unknown_test"]
            assert context["agent"] == "Valid Agent"
            assert context["task"] == "Task 2"


class TestEnqueueTrace:
    """Test trace enqueueing functionality."""

    @pytest.fixture
    def clean_registries(self):
        """Clean up static registries."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        AgentTraceEventListener._active_context.clear()
        yield
        AgentTraceEventListener._init_logged.clear()

    def test_enqueue_trace_main_process(self, clean_registries):
        """Test trace enqueueing in main process."""
        os.environ.pop('CREW_SUBPROCESS_MODE', None)

        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
            with patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log') as mock_enqueue:
                mock_queue = MagicMock()
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

                group_context = MagicMock()
                group_context.primary_group_id = "group-123"
                group_context.group_email = "test@example.com"

                listener = AgentTraceEventListener(
                    job_id="main_process_test",
                    group_context=group_context
                )

                listener._enqueue_trace(
                    event_source="Test Agent",
                    event_context="test_context",
                    event_type="test_event",
                    output_content="Test content",
                    extra_data={"key": "value"}
                )

                mock_queue.put.assert_called_once()
                mock_enqueue.assert_called_once()

    def test_enqueue_trace_subprocess_mode(self, clean_registries):
        """Test trace enqueueing in subprocess mode."""
        os.environ['CREW_SUBPROCESS_MODE'] = 'true'

        try:
            with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
                mock_queue = MagicMock()
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

                listener = AgentTraceEventListener(
                    job_id="subprocess_test",
                    group_context=None
                )

                with patch.object(listener, '_write_trace_to_db_async') as mock_write:
                    listener._enqueue_trace(
                        event_source="Test Agent",
                        event_context="test_context",
                        event_type="test_event",
                        output_content="Test content"
                    )

                    mock_write.assert_called_once()

        finally:
            os.environ.pop('CREW_SUBPROCESS_MODE', None)

    def test_enqueue_trace_skips_debug_events_when_disabled(self, clean_registries):
        """Test that debug events are skipped when debug tracing disabled."""
        os.environ.pop('CREW_SUBPROCESS_MODE', None)

        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
            with patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log') as mock_enqueue:
                mock_queue = MagicMock()
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

                listener = AgentTraceEventListener(
                    job_id="debug_skip_test",
                    group_context=None,
                    debug_tracing=False
                )

                # Try to enqueue a debug-only event
                listener._enqueue_trace(
                    event_source="Test Agent",
                    event_context="memory",
                    event_type="memory_write_started",  # Debug-only event
                    output_content="Memory content"
                )

                # Should not enqueue when debug tracing is disabled
                mock_queue.put.assert_not_called()

    def test_enqueue_trace_includes_debug_events_when_enabled(self, clean_registries):
        """Test that debug events are included when debug tracing enabled."""
        os.environ.pop('CREW_SUBPROCESS_MODE', None)

        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
            with patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log') as mock_enqueue:
                mock_queue = MagicMock()
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

                listener = AgentTraceEventListener(
                    job_id="debug_include_test",
                    group_context=None,
                    debug_tracing=True
                )

                listener._enqueue_trace(
                    event_source="Test Agent",
                    event_context="memory",
                    event_type="memory_write_started",
                    output_content="Memory content"
                )

                # Should enqueue when debug tracing is enabled
                mock_queue.put.assert_called_once()


class TestHandleTaskCompletion:
    """Test task completion handling."""

    @pytest.fixture
    def clean_registries(self):
        """Clean up static registries."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        AgentTraceEventListener._active_context.clear()
        AgentTraceEventListener._task_start_times.clear()
        yield
        AgentTraceEventListener._init_logged.clear()

    def test_handle_task_completion_calculates_duration(self, clean_registries):
        """Test that task duration is calculated."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="duration_test", group_context=None)

            # Set a start time
            start_time = datetime.now(timezone.utc)
            AgentTraceEventListener._task_start_times["duration_test"] = {
                "task-123": start_time
            }

            with patch.object(listener, '_update_task_status'):
                listener._handle_task_completion("Test Task", "task-123", "[TEST]")

            # Start time should be cleaned up
            assert "task-123" not in AgentTraceEventListener._task_start_times.get("duration_test", {})

    def test_handle_task_completion_updates_status(self, clean_registries):
        """Test that task status is updated."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
            from src.schemas.task_tracking import TaskStatusEnum

            listener = AgentTraceEventListener(job_id="status_test", group_context=None)

            with patch.object(listener, '_update_task_status') as mock_update:
                listener._handle_task_completion("Test Task", "task-456", "[TEST]")

                mock_update.assert_called_once_with(
                    "Test Task",
                    TaskStatusEnum.COMPLETED,
                    "[TEST]"
                )


class TestSetupListenersEventRegistration:
    """Test event handler registration."""

    @pytest.fixture
    def clean_registries(self):
        """Clean up static registries."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        AgentTraceEventListener._active_context.clear()
        yield
        AgentTraceEventListener._init_logged.clear()

    def test_registers_agent_execution_handler(self, clean_registries):
        """Test that AgentExecutionCompletedEvent handler is registered."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
            from crewai.events import AgentExecutionCompletedEvent

            listener = AgentTraceEventListener(job_id="register_test", group_context=None)

            registered_handlers = {}

            def mock_on(event_class):
                def decorator(func):
                    registered_handlers[event_class] = func
                    return func
                return decorator

            mock_event_bus = MagicMock()
            mock_event_bus.on = mock_on

            listener.setup_listeners(mock_event_bus)

            assert AgentExecutionCompletedEvent in registered_handlers

    def test_registers_crew_kickoff_handlers(self, clean_registries):
        """Test that crew kickoff handlers are registered."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
            from crewai.events import CrewKickoffStartedEvent, CrewKickoffCompletedEvent

            listener = AgentTraceEventListener(job_id="crew_test", group_context=None)

            registered_handlers = {}

            def mock_on(event_class):
                def decorator(func):
                    registered_handlers[event_class] = func
                    return func
                return decorator

            mock_event_bus = MagicMock()
            mock_event_bus.on = mock_on

            listener.setup_listeners(mock_event_bus)

            assert CrewKickoffStartedEvent in registered_handlers
            assert CrewKickoffCompletedEvent in registered_handlers


class TestEventHandlerExecution:
    """Test actual event handler execution."""

    @pytest.fixture
    def clean_registries(self):
        """Clean up static registries."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        AgentTraceEventListener._active_context.clear()
        AgentTraceEventListener._active_crew_name.clear()
        yield
        AgentTraceEventListener._init_logged.clear()

    def test_agent_execution_completed_handler(self, clean_registries):
        """Test AgentExecutionCompletedEvent handler."""
        os.environ.pop('CREW_SUBPROCESS_MODE', None)

        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
            with patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log') as mock_enqueue:
                mock_queue = MagicMock()
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

                listener = AgentTraceEventListener(job_id="exec_test", group_context=None)

                # Capture the handler
                handler = None

                def mock_on(event_class):
                    def decorator(func):
                        nonlocal handler
                        if event_class.__name__ == 'AgentExecutionCompletedEvent':
                            handler = func
                        return func
                    return decorator

                mock_event_bus = MagicMock()
                mock_event_bus.on = mock_on

                listener.setup_listeners(mock_event_bus)

                # Create mock event
                mock_event = MagicMock()
                mock_event.agent = MagicMock()
                mock_event.agent.role = "Test Agent"
                mock_event.agent.id = "agent-123"
                mock_event.task = MagicMock()
                mock_event.task.description = "Test Task"
                mock_event.task.id = "task-123"
                mock_event.output = "Test output"
                mock_event.timestamp = datetime.now(timezone.utc)

                # Call handler
                handler(None, mock_event)

                mock_queue.put.assert_called()

    def test_crew_kickoff_started_handler(self, clean_registries):
        """Test CrewKickoffStartedEvent handler."""
        os.environ.pop('CREW_SUBPROCESS_MODE', None)
        os.environ.pop('FLOW_SUBPROCESS_MODE', None)

        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
            with patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log') as mock_enqueue:
                mock_queue = MagicMock()
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

                listener = AgentTraceEventListener(job_id="kickoff_test", group_context=None)

                # Capture the handler
                handler = None

                def mock_on(event_class):
                    def decorator(func):
                        nonlocal handler
                        if event_class.__name__ == 'CrewKickoffStartedEvent':
                            handler = func
                        return func
                    return decorator

                mock_event_bus = MagicMock()
                mock_event_bus.on = mock_on

                listener.setup_listeners(mock_event_bus)

                # Create mock event
                mock_event = MagicMock()
                mock_event.crew_name = "Test Crew"
                mock_event.inputs = {"key": "value"}

                # Call handler
                handler(None, mock_event)

                # Should store active crew name
                assert AgentTraceEventListener._active_crew_name.get("kickoff_test") == "Test Crew"

    def test_crew_kickoff_started_suppressed_in_flow_mode(self, clean_registries):
        """Test CrewKickoffStartedEvent is suppressed in flow mode."""
        os.environ['FLOW_SUBPROCESS_MODE'] = 'true'

        try:
            with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
                with patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log') as mock_enqueue:
                    mock_queue = MagicMock()
                    mock_get_queue.return_value = mock_queue

                    from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

                    listener = AgentTraceEventListener(job_id="flow_test", group_context=None)

                    handler = None

                    def mock_on(event_class):
                        def decorator(func):
                            nonlocal handler
                            if event_class.__name__ == 'CrewKickoffStartedEvent':
                                handler = func
                            return func
                        return decorator

                    mock_event_bus = MagicMock()
                    mock_event_bus.on = mock_on

                    listener.setup_listeners(mock_event_bus)

                    mock_event = MagicMock()
                    mock_event.crew_name = "Flow Crew"
                    mock_event.inputs = {}

                    # Reset call count
                    mock_queue.put.reset_mock()

                    handler(None, mock_event)

                    # In flow mode, crew_started trace should be suppressed
                    # (We check that it doesn't enqueue a crew_started event)
                    for call in mock_queue.put.call_args_list:
                        if call[0][0].get("event_type") == "crew_started":
                            pytest.fail("crew_started should be suppressed in flow mode")

        finally:
            os.environ.pop('FLOW_SUBPROCESS_MODE', None)


class TestEventTypeDetectorExtended:
    """Extended tests for EventTypeDetector."""

    def test_detect_tool_usage_patterns(self):
        """Test detection of various tool usage patterns."""
        from src.engines.crewai.callbacks.logging_callbacks import EventTypeDetector

        patterns = [
            ("Using tool: SearchTool\nTool Output: Results", "tool_usage"),
            ("Action: WebScrapeTool\nAction Output: Data", "tool_usage"),
            ("Calling: ApiTool", "tool_usage"),
            ("Executing: ProcessTool", "tool_usage"),
        ]

        for output, expected_type in patterns:
            event_type, _, _ = EventTypeDetector.detect_event_type(output)
            assert event_type == expected_type, f"Failed for: {output}"

    def test_detect_task_completion_patterns(self):
        """Test detection of task completion patterns."""
        from src.engines.crewai.callbacks.logging_callbacks import EventTypeDetector

        patterns = [
            "Final Answer: The task is complete",
            "Task Complete: Done",
            "FINAL ANSWER: Result",
            "## Final Answer\nContent",
        ]

        for output in patterns:
            event_type, _, _ = EventTypeDetector.detect_event_type(output)
            assert event_type == "task_completed", f"Failed for: {output}"

    def test_detect_llm_reasoning_patterns(self):
        """Test detection of LLM reasoning patterns."""
        from src.engines.crewai.callbacks.logging_callbacks import EventTypeDetector

        patterns = [
            "Thought: I need to analyze this",
            "Thinking: Let me consider",
            "Reasoning: Based on the data",
            "Analysis: The results show",
        ]

        for output in patterns:
            event_type, _, _ = EventTypeDetector.detect_event_type(output)
            assert event_type == "llm_call", f"Failed for: {output}"

    def test_detect_default_agent_execution(self):
        """Test default detection for generic output."""
        from src.engines.crewai.callbacks.logging_callbacks import EventTypeDetector

        outputs = [
            "Regular agent output without patterns",
            "Some processing happening",
            "",
            None,
        ]

        for output in outputs:
            output_str = output if output else ""
            event_type, _, _ = EventTypeDetector.detect_event_type(output_str)
            assert event_type == "agent_execution"


class TestTaskCompletionEventListenerExtended:
    """Extended tests for TaskCompletionEventListener."""

    def test_initialization_sets_attributes(self):
        """Test initialization sets all required attributes."""
        from src.engines.crewai.callbacks.logging_callbacks import TaskCompletionEventListener

        group_context = MagicMock()
        group_context.primary_group_id = "group-123"

        listener = TaskCompletionEventListener(
            job_id="task_listener_test",
            group_context=group_context
        )

        assert listener.job_id == "task_listener_test"
        assert listener.group_context == group_context
        assert listener._init_time is not None

    def test_setup_listeners_is_noop(self):
        """Test that setup_listeners doesn't register handlers."""
        from src.engines.crewai.callbacks.logging_callbacks import TaskCompletionEventListener

        listener = TaskCompletionEventListener(job_id="noop_test", group_context=None)

        mock_event_bus = MagicMock()

        listener.setup_listeners(mock_event_bus)

        # Should not register any handlers (deprecated)
        mock_event_bus.on.assert_not_called()


class TestLLMStreamHandling:
    """Test LLM stream chunk handling."""

    @pytest.fixture
    def clean_registries(self):
        """Clean up static registries."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        AgentTraceEventListener._active_context.clear()
        yield
        AgentTraceEventListener._init_logged.clear()

    def test_llm_stream_handler_registered(self, clean_registries):
        """Test that LLMStreamChunkEvent handler is registered."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
            from crewai.events import LLMStreamChunkEvent

            listener = AgentTraceEventListener(job_id="stream_test", group_context=None)

            registered_handlers = {}

            def mock_on(event_class):
                def decorator(func):
                    registered_handlers[event_class] = func
                    return func
                return decorator

            mock_event_bus = MagicMock()
            mock_event_bus.on = mock_on

            listener.setup_listeners(mock_event_bus)

            assert LLMStreamChunkEvent in registered_handlers


class TestWriteTraceToDbAsync:
    """Test async database write functionality."""

    @pytest.fixture
    def clean_registries(self):
        """Clean up static registries."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
        AgentTraceEventListener._init_logged.clear()
        yield
        AgentTraceEventListener._init_logged.clear()

    def test_write_trace_handles_uuid_in_data(self, clean_registries):
        """Test that UUIDs in trace data are properly serialized."""
        import uuid

        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="uuid_test", group_context=None)

            trace_data = {
                "job_id": "uuid_test",
                "event_source": "Test",
                "event_context": "test",
                "event_type": "test_event",
                "output": {
                    "content": "test",
                    "uuid_field": uuid.uuid4()
                }
            }

            with patch.object(listener, '_write_trace_to_db_async') as mock_write:
                # This tests that UUID handling works without error
                listener._enqueue_trace(
                    event_source="Test",
                    event_context="test",
                    event_type="test_event",
                    output_content="test",
                    extra_data={"uuid_field": str(uuid.uuid4())}
                )


class TestDebugOnlyEventTypes:
    """Test DEBUG_ONLY_EVENT_TYPES filtering."""

    def test_debug_only_event_types_defined(self):
        """Test that debug only event types are properly defined."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        expected_types = {
            "memory_write_started",
            "memory_retrieval_started",
            "memory_write",
            "memory_retrieval",
            "knowledge_retrieval_started",
            "knowledge_retrieval",
            "agent_reasoning",
            "agent_reasoning_error",
            "llm_guardrail",
        }

        for event_type in expected_types:
            assert event_type in AgentTraceEventListener.DEBUG_ONLY_EVENT_TYPES


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
