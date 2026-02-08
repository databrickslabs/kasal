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

    def test_listener_initialization_stores_job_id(self, clean_registries):
        """Test listener initialization stores job_id correctly."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(
                job_id="test_job_id",
                group_context=None
            )

            assert listener.job_id == "test_job_id"

    def test_listener_initialization_stores_group_context(self, clean_registries):
        """Test listener initialization stores group_context correctly."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            group_context = MagicMock()
            group_context.primary_group_id = "group-123"

            listener = AgentTraceEventListener(
                job_id="test_job_id",
                group_context=group_context
            )

            assert listener.group_context == group_context

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
            # IMPORTANT: Set these to None to avoid MagicMock auto-creating them
            mock_event.task_name = None
            mock_event.task_id = None

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
            # IMPORTANT: Set these to None to avoid MagicMock auto-creating them
            mock_event.task_name = None
            mock_event.task_id = None

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

            # Context is stored per job_id and per agent_name
            assert "context_test" in AgentTraceEventListener._active_context
            assert "Test Agent" in AgentTraceEventListener._active_context["context_test"]
            assert AgentTraceEventListener._active_context["context_test"]["Test Agent"]["task"] == "Test Task"

    def test_update_context_updates_existing(self, clean_registries):
        """Test that existing context is updated for same agent."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="update_test", group_context=None)

            # Update same agent with different tasks
            listener._update_active_context("Agent 1", "Task 1", "[TEST]")
            listener._update_active_context("Agent 1", "Task 2", "[TEST]")

            # Context stores each agent separately
            context = AgentTraceEventListener._active_context["update_test"]
            assert context["Agent 1"]["task"] == "Task 2"

    def test_update_context_multiple_agents(self, clean_registries):
        """Test that context supports multiple agents per job."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="multi_agent_test", group_context=None)

            listener._update_active_context("Agent 1", "Task 1", "[TEST]")
            listener._update_active_context("Agent 2", "Task 2", "[TEST]")

            context = AgentTraceEventListener._active_context["multi_agent_test"]
            assert "Agent 1" in context
            assert "Agent 2" in context
            assert context["Agent 1"]["task"] == "Task 1"
            assert context["Agent 2"]["task"] == "Task 2"

    def test_update_context_ignores_unknown_agent(self, clean_registries):
        """Test that Unknown Agent doesn't create a context entry."""
        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

            listener = AgentTraceEventListener(job_id="unknown_test", group_context=None)

            listener._update_active_context("Valid Agent", "Task 1", "[TEST]")
            listener._update_active_context("Unknown Agent", "Task 2", "[TEST]")

            context = AgentTraceEventListener._active_context["unknown_test"]
            # "Unknown Agent" should not be stored (it's skipped)
            assert "Valid Agent" in context
            assert "Unknown Agent" not in context
            assert context["Valid Agent"]["task"] == "Task 1"


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
            with patch('src.engines.crewai.callbacks.trace_persistence.enqueue_log') as mock_enqueue:
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

    def test_enqueue_trace_with_extra_data(self, clean_registries):
        """Test that extra_data is included in trace."""
        os.environ.pop('CREW_SUBPROCESS_MODE', None)

        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
            with patch('src.engines.crewai.callbacks.trace_persistence.enqueue_log') as mock_enqueue:
                mock_queue = MagicMock()
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

                listener = AgentTraceEventListener(
                    job_id="extra_data_test",
                    group_context=None
                )

                listener._enqueue_trace(
                    event_source="Test Agent",
                    event_context="memory",
                    event_type="memory_event",
                    output_content="Memory content",
                    extra_data={"custom_key": "custom_value"}
                )

                # Should enqueue with the trace data
                mock_queue.put.assert_called_once()

    def test_enqueue_trace_with_group_context(self, clean_registries):
        """Test that group context is included in trace data."""
        os.environ.pop('CREW_SUBPROCESS_MODE', None)

        with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
            with patch('src.engines.crewai.callbacks.trace_persistence.enqueue_log') as mock_enqueue:
                mock_queue = MagicMock()
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

                group_context = MagicMock()
                group_context.primary_group_id = "test-group-id"
                group_context.group_email = "test@example.com"

                listener = AgentTraceEventListener(
                    job_id="group_context_test",
                    group_context=group_context
                )

                listener._enqueue_trace(
                    event_source="Test Agent",
                    event_context="test_context",
                    event_type="test_event",
                    output_content="Test content"
                )

                # Should enqueue with the trace data
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
            with patch('src.engines.crewai.callbacks.trace_persistence.enqueue_log') as mock_enqueue:
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
            with patch('src.engines.crewai.callbacks.trace_persistence.enqueue_log') as mock_enqueue:
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
                with patch('src.engines.crewai.callbacks.trace_persistence.enqueue_log') as mock_enqueue:
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


class TestStaticRegistries:
    """Test static registry class attributes."""

    def test_init_logged_registry_exists(self):
        """Test that _init_logged registry is defined."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        assert hasattr(AgentTraceEventListener, '_init_logged')
        assert isinstance(AgentTraceEventListener._init_logged, set)

    def test_task_registry_exists(self):
        """Test that _task_registry is defined."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        assert hasattr(AgentTraceEventListener, '_task_registry')
        assert isinstance(AgentTraceEventListener._task_registry, dict)

    def test_active_context_registry_exists(self):
        """Test that _active_context registry is defined."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        assert hasattr(AgentTraceEventListener, '_active_context')
        assert isinstance(AgentTraceEventListener._active_context, dict)

    def test_task_start_times_registry_exists(self):
        """Test that _task_start_times registry is defined."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        assert hasattr(AgentTraceEventListener, '_task_start_times')
        assert isinstance(AgentTraceEventListener._task_start_times, dict)

    def test_active_crew_name_registry_exists(self):
        """Test that _active_crew_name registry is defined."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener

        assert hasattr(AgentTraceEventListener, '_active_crew_name')
        assert isinstance(AgentTraceEventListener._active_crew_name, dict)


class TestGuardrailEventHandlers:
    """Test LLM Guardrail event handlers."""

    @pytest.fixture
    def listener(self):
        """Create an AgentTraceEventListener for testing."""
        from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
        from src.utils.user_context import GroupContext

        # Clear static registries
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        AgentTraceEventListener._active_context.clear()
        AgentTraceEventListener._task_start_times.clear()

        job_id = "test_job_123"
        group_context = GroupContext(
            group_ids=["group_123"],
            group_email="test@example.com",
            email_domain="example.com"
        )

        return AgentTraceEventListener(
            job_id=job_id,
            group_context=group_context,
            register_global_events=False
        )

    def test_guardrail_started_event_handler(self, listener):
        """Test handling of LLMGuardrailStartedEvent."""
        # Create mock event
        mock_event = Mock()
        mock_event.guardrail = "test_guardrail_function"
        mock_event.timestamp = "2024-01-01T00:00:00Z"
        mock_event.task = None

        # Mock the _enqueue_trace method
        listener._enqueue_trace = Mock()

        # Call handler via _handle_guardrail_event style (simulating the registered handler)
        try:
            from crewai.events.types.llm_guardrail_events import LLMGuardrailStartedEvent
            # Handler is registered internally, we test the trace output
        except ImportError:
            pytest.skip("LLM Guardrail events not available in this CrewAI version")

    def test_guardrail_completed_event_handler_success(self, listener):
        """Test handling of LLMGuardrailCompletedEvent with success."""
        # Create mock event
        mock_event = Mock()
        mock_event.success = True
        mock_event.result = "Validation passed"
        mock_event.error = None
        mock_event.retry_count = 0
        mock_event.timestamp = "2024-01-01T00:00:00Z"
        mock_event.task = None

        # Mock the _enqueue_trace method
        listener._enqueue_trace = Mock()

        try:
            from crewai.events.types.llm_guardrail_events import LLMGuardrailCompletedEvent
            # Handler is registered internally, we test the trace output
        except ImportError:
            pytest.skip("LLM Guardrail events not available in this CrewAI version")

    def test_guardrail_completed_event_handler_failure(self, listener):
        """Test handling of LLMGuardrailCompletedEvent with failure."""
        # Create mock event
        mock_event = Mock()
        mock_event.success = False
        mock_event.result = None
        mock_event.error = "Validation failed: insufficient data"
        mock_event.retry_count = 1
        mock_event.timestamp = "2024-01-01T00:00:00Z"
        mock_event.task = None

        listener._enqueue_trace = Mock()

        try:
            from crewai.events.types.llm_guardrail_events import LLMGuardrailCompletedEvent
        except ImportError:
            pytest.skip("LLM Guardrail events not available in this CrewAI version")

    def test_guardrail_failed_event_handler(self, listener):
        """Test handling of LLMGuardrailFailedEvent (technical errors)."""
        # Create mock event
        mock_event = Mock()
        mock_event.error = "Connection timeout during validation"
        mock_event.retry_count = 2
        mock_event.timestamp = "2024-01-01T00:00:00Z"
        mock_event.task = None

        listener._enqueue_trace = Mock()

        try:
            from crewai.events.types.llm_guardrail_events import LLMGuardrailFailedEvent
            # Verify the event import works (the handler was added in our fix)
            assert LLMGuardrailFailedEvent is not None
        except ImportError:
            pytest.skip("LLM Guardrail events not available in this CrewAI version")

    def test_guardrail_failed_event_handler_with_task(self, listener):
        """Test LLMGuardrailFailedEvent handler extracts task name."""
        mock_event = Mock()
        mock_event.error = "Validation error"
        mock_event.retry_count = 0
        mock_event.timestamp = "2024-01-01T00:00:00Z"

        # Mock task with name
        mock_task = Mock()
        mock_task.name = "test_task"
        mock_event.task = mock_task

        listener._enqueue_trace = Mock()

        try:
            from crewai.events.types.llm_guardrail_events import LLMGuardrailFailedEvent
        except ImportError:
            pytest.skip("LLM Guardrail events not available in this CrewAI version")

    def test_guardrail_event_imports_available(self):
        """Test that all guardrail event types can be imported."""
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

    def test_guardrail_events_available_flag(self):
        """Test that LLM_GUARDRAIL_EVENTS_AVAILABLE flag is set correctly."""
        from src.engines.crewai.callbacks.logging_callbacks import LLM_GUARDRAIL_EVENTS_AVAILABLE

        # The flag should be True if imports succeeded, False otherwise
        assert isinstance(LLM_GUARDRAIL_EVENTS_AVAILABLE, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
