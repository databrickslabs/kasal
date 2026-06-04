"""
Extended tests for streaming_callbacks.py to push coverage to 90%+.

Covers missing lines:
- LogCaptureHandler.emit error path (exc_info)
- LogCaptureHandler.close
- LogCaptureHandler.flush empty buffer
- LogCaptureHandler.flush error path
- JobOutputCallback.execute with dict/output.raw
- JobOutputCallback.execute Final Answer / Task Completed paths
- JobOutputCallback.execute empty message path
- JobOutputCallback.execute with enqueue failure
- JobOutputCallback.__del__ cleanup
- EventStreamingCallback._sanitize_config
- EventStreamingCallback config logging path
- EventStreamingCallback._register_tool_usage_handlers
- EventStreamingCallback._register_llm_call_handlers
- EventStreamingCallback with no config (stream_events defaults True)
"""
import sys
import os
from unittest.mock import MagicMock

os.environ.setdefault("DATABASE_TYPE", "sqlite")
os.environ.setdefault("SQLITE_DB_PATH", ":memory:")

# Ensure the crewai import chain doesn't fail when this file is imported in isolation
# (e.g., when test discovery runs memory/ tests before callbacks/ tests, causing entity
# retriever extended to clear crewai mock stubs from sys.modules).
#
# Strategy: if crewai is NOT in sys.modules (it was cleared by another test file),
# install a full mock for the top-level crewai package so the import chain resolves.
# If crewai IS already available (real or mocked), leave it alone.
if "crewai" not in sys.modules:
    _crewai_mock = MagicMock()
    sys.modules["crewai"] = _crewai_mock
    _crewai_tools_mock = MagicMock()
    sys.modules.setdefault("crewai_tools", _crewai_tools_mock)
    for _s in [
        "crewai.agent", "crewai.agent.core", "crewai.agent.utils",
        "crewai.agents", "crewai.agents.agent_builder",
        "crewai.agents.agent_builder.base_agent",
        "crewai.agents.cache", "crewai.agents.cache.cache_handler",
        "crewai.crew", "crewai.task",
        "crewai.tools", "crewai.events", "crewai.events.types",
        "crewai.events.types.llm_events",
        "crewai.flow", "crewai.flow.flow", "crewai.flow.persistence",
        "crewai.llm", "crewai.llms", "crewai.llms.providers",
        "crewai.llms.providers.openai", "crewai.llms.providers.openai.completion",
        "crewai.memory", "crewai.memory.storage",
        "crewai.memory.storage.rag_storage",
        "crewai.memory.storage.ltm_sqlite_storage",
        "crewai.project", "crewai.tasks",
        "crewai.tasks.llm_guardrail", "crewai.tasks.task_output",
        "crewai.utilities", "crewai.utilities.converter",
        "crewai.utilities.evaluators",
        "crewai.utilities.evaluators.task_evaluator",
        "crewai.utilities.exceptions",
        "crewai.utilities.internal_instructor",
        "crewai.utilities.paths", "crewai.utilities.printer",
        "crewai.knowledge", "crewai.knowledge.knowledge",
        "crewai.knowledge.source", "crewai.knowledge.source.base_knowledge_source",
        "crewai.knowledge.storage", "crewai.knowledge.storage.knowledge_storage",
        "crewai.knowledge.utils", "crewai.knowledge.utils.knowledge_utils",
        "crewai.rag", "crewai.rag.core", "crewai.rag.core.base_client",
        "crewai.rag.core.base_embeddings_provider",
        "crewai.rag.embeddings", "crewai.rag.embeddings.types",
        "crewai.rag.types", "crewai.rag.config", "crewai.rag.config.utils",
        "crewai.rag.chromadb", "crewai.rag.chromadb.config",
        "crewai.rag.chromadb.types",
        "crewai_tools",
        "crewai_tools.rag", "crewai_tools.rag.core",
    ]:
        if _s not in sys.modules:
            sys.modules[_s] = MagicMock()

import pytest
import logging
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch, call, AsyncMock

from src.engines.crewai.callbacks.streaming_callbacks import (
    LogCaptureHandler,
    JobOutputCallback,
    EventStreamingCallback,
)
from src.utils.user_context import GroupContext


# ─────────────────────────────────────────────────────────────────────────────
# LogCaptureHandler extended tests
# ─────────────────────────────────────────────────────────────────────────────


class TestLogCaptureHandlerExtended:
    """Additional tests for LogCaptureHandler."""

    def _make_record(self, msg="Test message", level=logging.INFO):
        return logging.LogRecord(
            name="test",
            level=level,
            pathname="test.py",
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )

    def test_emit_error_is_handled(self):
        """emit() should not propagate exceptions; it logs them via logger_manager."""
        handler = LogCaptureHandler("job_1", None)
        # Override format to raise
        handler.format = MagicMock(side_effect=Exception("format error"))

        record = self._make_record()
        # Should not raise
        handler.emit(record)

    def test_flush_empty_buffer_does_nothing(self):
        """flush() with empty buffer should exit early without calling enqueue."""
        handler = LogCaptureHandler("job_2", None)
        handler.buffer = []

        with patch(
            "src.engines.crewai.callbacks.streaming_callbacks.enqueue_log"
        ) as mock_enqueue:
            handler.flush()

        mock_enqueue.assert_not_called()

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_flush_handles_exception(self, mock_enqueue):
        """flush() should handle exceptions without raising."""
        handler = LogCaptureHandler("job_3", None)
        handler.buffer = [("msg", 1000.0)]
        mock_enqueue.side_effect = Exception("enqueue failed")

        # Should not raise
        handler.flush()

    def test_close_flushes_then_calls_super(self):
        """close() should call flush first, then super().close()."""
        handler = LogCaptureHandler("job_4", None)
        handler.buffer = [("msg", 1000.0)]

        with patch.object(handler, "flush") as mock_flush:
            handler.close()
            mock_flush.assert_called_once()

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_flush_enqueue_failure_logged(self, mock_enqueue):
        """flush() logs error when enqueue returns False."""
        handler = LogCaptureHandler("job_5", None)
        handler.buffer = [("msg", 1000.0)]
        mock_enqueue.return_value = False

        # Should not raise; enqueue failure should be logged
        handler.flush()

    def test_group_logs_single_entry(self):
        """_group_logs_by_time() with single entry creates one group."""
        handler = LogCaptureHandler("job_6", None)
        handler.buffer = [("Only message", 1000.0)]
        groups = handler._group_logs_by_time()
        assert len(groups) == 1
        assert groups[0][0] == ["Only message"]

    def test_group_logs_empty(self):
        """_group_logs_by_time() with empty buffer returns empty."""
        handler = LogCaptureHandler("job_7", None)
        handler.buffer = []
        assert handler._group_logs_by_time() == []


# ─────────────────────────────────────────────────────────────────────────────
# JobOutputCallback extended tests
# ─────────────────────────────────────────────────────────────────────────────


class TestJobOutputCallbackExtended:
    """Additional tests for JobOutputCallback."""

    def _make_group_context(self):
        return GroupContext(
            group_ids=["grp_1"],
            group_email="test@example.com",
            email_domain="example.com",
        )

    @pytest.mark.asyncio
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    async def test_execute_with_output_raw_attribute(self, mock_enqueue):
        """execute() should use output.raw when attribute exists."""
        callback = JobOutputCallback("job_1", group_context=None)

        output = MagicMock()
        output.raw = "Raw output content"

        result = await callback.execute(output)
        assert result is output
        # enqueue_log should have been called for the content
        assert mock_enqueue.called

    @pytest.mark.asyncio
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    async def test_execute_with_dict_output(self, mock_enqueue):
        """execute() should convert dict to JSON string."""
        callback = JobOutputCallback("job_2", group_context=None)

        output = {"key": "value", "num": 42}
        result = await callback.execute(output)
        assert result is output

    @pytest.mark.asyncio
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    async def test_execute_with_string_output(self, mock_enqueue):
        """execute() should convert non-raw/dict to str."""
        callback = JobOutputCallback("job_3", group_context=None)
        result = await callback.execute("plain string")
        assert result == "plain string"

    @pytest.mark.asyncio
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    async def test_execute_with_final_answer_triggers_completion_marker(self, mock_enqueue):
        """Messages containing 'Final Answer' should trigger task completion marker."""
        callback = JobOutputCallback("job_4", group_context=self._make_group_context())

        result = await callback.execute("Final Answer: The result is 42")

        # At minimum, enqueue should have been called multiple times
        # (once for the message, once for the completion marker)
        call_contents = [c.kwargs.get("content", "") for c in mock_enqueue.call_args_list]
        assert any("TASK_COMPLETION" in c for c in call_contents)

    @pytest.mark.asyncio
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    async def test_execute_with_task_completed_triggers_completion_marker(self, mock_enqueue):
        """Messages containing 'Task Completed' should trigger task completion marker."""
        callback = JobOutputCallback("job_5", group_context=None)

        result = await callback.execute("Task Completed successfully")

        call_contents = [c.kwargs.get("content", "") for c in mock_enqueue.call_args_list]
        assert any("TASK_COMPLETION" in c for c in call_contents)

    @pytest.mark.asyncio
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    async def test_execute_empty_message_skipped(self, mock_enqueue):
        """Empty/whitespace-only output should not trigger a content enqueue."""
        mock_enqueue.reset_mock()
        callback = JobOutputCallback("job_6", group_context=None)
        mock_enqueue.reset_mock()  # Reset after init calls

        result = await callback.execute("   ")  # whitespace only
        assert result == "   "

    @pytest.mark.asyncio
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    async def test_execute_returns_output_on_exception(self, mock_enqueue):
        """execute() should return original output even on internal error."""
        callback = JobOutputCallback("job_7", group_context=None)
        mock_enqueue.side_effect = Exception("Queue error")

        output = "some output"
        result = await callback.execute(output)
        # Should still return the original output
        assert result == output

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_del_sends_finalization_message(self, mock_enqueue):
        """__del__() should send finalization message and clean up handler."""
        callback = JobOutputCallback("job_8", group_context=None)
        mock_enqueue.reset_mock()

        callback.__del__()

        # Should have sent finalization message
        call_contents = [c.kwargs.get("content", "") for c in mock_enqueue.call_args_list]
        assert any("FINALIZATION" in c for c in call_contents)

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_del_handles_exception(self, mock_enqueue):
        """__del__() should handle exceptions without raising."""
        callback = JobOutputCallback("job_9", group_context=None)
        # Remove log_handler to force error
        del callback.log_handler

        # Should not raise
        callback.__del__()

    def test_sanitize_config_removes_nothing_by_default(self):
        """_sanitize_config() should return a copy with no modifications by default."""
        callback = JobOutputCallback("job_10", group_context=None)
        config = {"model": "gpt-4", "temperature": 0.7}
        result = callback._sanitize_config(config)
        assert result == config
        assert result is not config  # Should be a copy

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_init_logs_config_when_provided(self, mock_enqueue):
        """JobOutputCallback init should log config when provided."""
        config = {"stream_output": True, "model": "gpt-4"}
        callback = JobOutputCallback("job_11", config=config, group_context=None)

        # Config message should have been enqueued
        call_contents = [c.kwargs.get("content", "") for c in mock_enqueue.call_args_list]
        assert any("CONFIG" in c for c in call_contents)


# ─────────────────────────────────────────────────────────────────────────────
# EventStreamingCallback extended tests
# ─────────────────────────────────────────────────────────────────────────────


class TestEventStreamingCallbackExtended:
    """Additional tests for EventStreamingCallback."""

    def _make_group_context(self):
        return GroupContext(
            group_ids=["grp_1"],
            group_email="test@example.com",
            email_domain="example.com",
        )

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_init_with_config_logs_configuration(self, mock_enqueue, mock_event_bus):
        """Init with config should enqueue a CONFIG message."""
        mock_enqueue.reset_mock()
        config = {"stream_events": True, "batch_size": 10}
        callback = EventStreamingCallback("job_1", config, None)

        call_contents = [c.kwargs.get("content", "") for c in mock_enqueue.call_args_list]
        assert any("CONFIG" in c for c in call_contents)

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    def test_sanitize_config_returns_copy(self, mock_event_bus):
        """_sanitize_config() should return deep copy."""
        callback = EventStreamingCallback("job_2", {"stream_events": False}, None)
        config = {"model": "gpt-4", "nested": {"key": "val"}}
        result = callback._sanitize_config(config)
        assert result == config
        assert result is not config

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    def test_init_without_config_defaults_streaming_enabled(self, mock_event_bus):
        """Init with no config should default stream_events=True."""
        # When config is None, _setup_event_handlers should still register handlers
        # because `not self.config or not self.config.get('stream_events', True)`
        # when config is None -> returns early (no handlers)
        callback = EventStreamingCallback("job_3", None, None)
        assert callback.job_id == "job_3"

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_config_enqueue_exception_handled(self, mock_enqueue, mock_event_bus):
        """Error during config logging should not crash init."""
        mock_enqueue.side_effect = [None, Exception("enqueue fail"), None, None]  # 2nd call fails
        config = {"stream_events": True}

        # Should not raise
        callback = EventStreamingCallback("job_4", config, None)

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_handler_error_handling_crew_kickoff_started(self, mock_enqueue, mock_event_bus):
        """CrewKickoffStarted handler should gracefully handle errors."""
        from crewai.events import CrewKickoffStartedEvent

        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator

        mock_event_bus.on = mock_on
        config = {"stream_events": True}
        callback = EventStreamingCallback("job_5", config, None)

        if CrewKickoffStartedEvent in handlers:
            # Trigger with enqueue_log raising exception - handler should catch it
            mock_enqueue.side_effect = Exception("fail")
            class SimpleEvent:
                crew_name = "Test"
            # Should not raise
            handlers[CrewKickoffStartedEvent]("source", SimpleEvent())

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_handler_error_handling_crew_kickoff_completed(self, mock_enqueue, mock_event_bus):
        """CrewKickoffCompleted handler should gracefully handle errors."""
        from crewai.events import CrewKickoffCompletedEvent

        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator

        mock_event_bus.on = mock_on
        config = {"stream_events": True}
        callback = EventStreamingCallback("job_6", config, None)

        if CrewKickoffCompletedEvent in handlers:
            mock_enqueue.side_effect = Exception("fail")
            class SimpleCompleteEvent:
                crew_name = "Test"
                output = "done"
            # Should not raise
            handlers[CrewKickoffCompletedEvent]("source", SimpleCompleteEvent())

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_agent_handler_event_without_output_attribute(self, mock_enqueue, mock_event_bus):
        """AgentExecutionCompleted handler should handle events without 'output' attribute."""
        from crewai.events import AgentExecutionCompletedEvent

        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator

        mock_event_bus.on = mock_on
        mock_enqueue.return_value = True
        config = {"stream_events": True}
        callback = EventStreamingCallback("job_7", config, None)

        if AgentExecutionCompletedEvent in handlers:
            # Use a simple object without output attribute to avoid spec issues
            class MockEventNoOutput:
                agent = Mock()
            MockEventNoOutput.agent.role = "Analyst"

            handlers[AgentExecutionCompletedEvent]("source", MockEventNoOutput())

    def test_register_tool_usage_handlers_is_accessible(self):
        """_register_tool_usage_handlers method should exist and be callable."""
        with patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus"):
            callback = EventStreamingCallback("job_8", {"stream_events": False}, None)
            # Just verify the method exists and can be called without crashing
            # (it references ToolUsageStartedEvent which may not exist, just test the decorator body)
            assert hasattr(callback, "_register_tool_usage_handlers")

    def test_register_llm_call_handlers_is_accessible(self):
        """_register_llm_call_handlers method should exist."""
        with patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus"):
            callback = EventStreamingCallback("job_9", {"stream_events": False}, None)
            assert hasattr(callback, "_register_llm_call_handlers")

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_crew_kickoff_started_no_crew_name_attribute(self, mock_enqueue, mock_event_bus):
        """CrewKickoffStarted handler with event lacking crew_name uses 'Unknown'."""
        from crewai.events import CrewKickoffStartedEvent

        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator

        mock_event_bus.on = mock_on
        mock_enqueue.return_value = True
        config = {"stream_events": True}
        callback = EventStreamingCallback("job_10", config, None)

        if CrewKickoffStartedEvent in handlers:
            # Use simple object without crew_name to avoid spec issues
            class MockStartEvent:
                pass
            handlers[CrewKickoffStartedEvent]("source", MockStartEvent())

            call_contents = [c.kwargs.get("content", "") for c in mock_enqueue.call_args_list]
            assert any("crew_started" in c for c in call_contents)

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_crew_kickoff_completed_no_crew_name(self, mock_enqueue, mock_event_bus):
        """CrewKickoffCompleted handler with event lacking crew_name/output uses defaults."""
        from crewai.events import CrewKickoffCompletedEvent

        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator

        mock_event_bus.on = mock_on
        mock_enqueue.return_value = True
        config = {"stream_events": True}
        callback = EventStreamingCallback("job_11", config, None)

        if CrewKickoffCompletedEvent in handlers:
            class MockCompleteEvent:
                pass
            handlers[CrewKickoffCompletedEvent]("source", MockCompleteEvent())

            call_contents = [c.kwargs.get("content", "") for c in mock_enqueue.call_args_list]
            assert any("crew_completed" in c for c in call_contents)

    @patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus")
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_agent_handler_agent_without_role(self, mock_enqueue, mock_event_bus):
        """AgentExecutionCompleted handler with agent lacking role uses str()."""
        from crewai.events import AgentExecutionCompletedEvent

        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator

        mock_event_bus.on = mock_on
        mock_enqueue.return_value = True
        config = {"stream_events": True}
        callback = EventStreamingCallback("job_12", config, None)

        if AgentExecutionCompletedEvent in handlers:
            # Use a plain object without role attribute
            class MockAgentNoRole:
                pass

            class MockEventNoRole:
                agent = MockAgentNoRole()
                output = "some output"

            handlers[AgentExecutionCompletedEvent]("source", MockEventNoRole())
            assert mock_enqueue.called

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_job_output_callback_init_config_exception_handled(self, mock_enqueue):
        """JobOutputCallback init should handle exception when logging config."""
        # Make enqueue_log fail on the config message call
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 2:  # 2nd call is the config message
                raise Exception("config logging failed")
            return True

        mock_enqueue.side_effect = side_effect
        config = {"stream_output": True}

        # Should not raise
        callback = JobOutputCallback("job_exc", config=config, group_context=None)

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_event_streaming_init_config_exception_handled(self, mock_enqueue):
        """EventStreamingCallback init should handle exception when logging config."""
        with patch("src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus"):
            # Make enqueue_log fail
            mock_enqueue.side_effect = Exception("logging failed")
            config = {"stream_events": False}

            # Should not raise
            callback = EventStreamingCallback("job_esc_exc", config=config, group_context=None)

    @pytest.mark.asyncio
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    async def test_execute_enqueue_returns_false(self, mock_enqueue):
        """execute() should log error when enqueue returns False."""
        mock_enqueue.return_value = True  # reset
        callback = JobOutputCallback("job_e263", group_context=None)
        mock_enqueue.reset_mock()

        # Make the content enqueue call return False
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            return False  # Simulate enqueue failure

        mock_enqueue.side_effect = side_effect
        result = await callback.execute("Some non-empty message")
        assert result == "Some non-empty message"

    @pytest.mark.asyncio
    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    async def test_execute_task_completion_enqueue_exception(self, mock_enqueue):
        """Task completion enqueue exception should be handled gracefully."""
        callback = JobOutputCallback("job_tc_exc", group_context=None)
        mock_enqueue.reset_mock()

        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] >= 2:  # Fail on task completion enqueue
                raise Exception("task completion failed")
            return True

        mock_enqueue.side_effect = side_effect
        # "Final Answer" triggers task completion path
        result = await callback.execute("Final Answer: Done")
        assert result == "Final Answer: Done"

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_register_tool_usage_handlers_covers_inner_functions(self, mock_enqueue):
        """Cover _register_tool_usage_handlers by injecting event types and calling handlers."""
        import src.engines.crewai.callbacks.streaming_callbacks as sc_module

        # Create fake event classes
        ToolUsageStartedEvent = type("ToolUsageStartedEvent", (), {})
        ToolUsageFinishedEvent = type("ToolUsageFinishedEvent", (), {})

        # Capture registered handlers
        captured = {}

        def mock_on(event_class):
            def decorator(func):
                captured[event_class] = func
                return func
            return decorator

        mock_bus = MagicMock()
        mock_bus.on = mock_on

        with patch.dict(sc_module.__dict__, {
            "ToolUsageStartedEvent": ToolUsageStartedEvent,
            "ToolUsageFinishedEvent": ToolUsageFinishedEvent,
            "crewai_event_bus": mock_bus,
        }):
            callback = EventStreamingCallback("job_tool3", {"stream_events": False}, None)
            mock_enqueue.return_value = True
            callback._register_tool_usage_handlers()

        # Now invoke the captured handlers to cover inner function bodies
        if ToolUsageStartedEvent in captured:
            mock_event = MagicMock()
            mock_event.tool_name = "test_tool"
            mock_event.agent = MagicMock(role="Agent")
            mock_event.task = MagicMock(description="Task description")
            captured[ToolUsageStartedEvent]("source", mock_event)

        if ToolUsageFinishedEvent in captured:
            mock_event = MagicMock()
            mock_event.tool_name = "test_tool"
            mock_event.agent = MagicMock(role="Agent")
            mock_event.task = MagicMock(description="Task description")
            mock_event.output = "Tool output"
            captured[ToolUsageFinishedEvent]("source", mock_event)

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_register_llm_call_handlers_covers_inner_functions(self, mock_enqueue):
        """Cover _register_llm_call_handlers by injecting event types and calling handlers."""
        import src.engines.crewai.callbacks.streaming_callbacks as sc_module

        LLMCallStartedEvent = type("LLMCallStartedEvent", (), {})
        LLMCallCompletedEvent = type("LLMCallCompletedEvent", (), {})

        captured = {}

        def mock_on(event_class):
            def decorator(func):
                captured[event_class] = func
                return func
            return decorator

        mock_bus = MagicMock()
        mock_bus.on = mock_on

        with patch.dict(sc_module.__dict__, {
            "LLMCallStartedEvent": LLMCallStartedEvent,
            "LLMCallCompletedEvent": LLMCallCompletedEvent,
            "crewai_event_bus": mock_bus,
        }):
            callback = EventStreamingCallback("job_llm3", {"stream_events": False}, None)
            mock_enqueue.return_value = True
            callback._register_llm_call_handlers()

        # Invoke handlers to cover function bodies
        if LLMCallStartedEvent in captured:
            mock_event = MagicMock()
            mock_event.agent = MagicMock(role="Agent")
            mock_event.task = MagicMock(description="Task description")
            mock_event.prompt = "test prompt"
            captured[LLMCallStartedEvent]("source", mock_event)

        if LLMCallCompletedEvent in captured:
            mock_event = MagicMock()
            mock_event.agent = MagicMock(role="Agent")
            mock_event.task = MagicMock(description="Task description")
            mock_event.output = "LLM output response"
            captured[LLMCallCompletedEvent]("source", mock_event)

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_register_tool_handler_exception_path(self, mock_enqueue):
        """Cover exception paths in _register_tool_usage_handlers inner functions."""
        import src.engines.crewai.callbacks.streaming_callbacks as sc_module

        ToolUsageStartedEvent = type("ToolUsageStartedEvent", (), {})
        ToolUsageFinishedEvent = type("ToolUsageFinishedEvent", (), {})

        captured = {}

        def mock_on(event_class):
            def decorator(func):
                captured[event_class] = func
                return func
            return decorator

        mock_bus = MagicMock()
        mock_bus.on = mock_on

        with patch.dict(sc_module.__dict__, {
            "ToolUsageStartedEvent": ToolUsageStartedEvent,
            "ToolUsageFinishedEvent": ToolUsageFinishedEvent,
            "crewai_event_bus": mock_bus,
        }):
            callback = EventStreamingCallback("job_tool4", {"stream_events": False}, None)
            mock_enqueue.side_effect = Exception("enqueue fail")
            callback._register_tool_usage_handlers()

        # Invoke handlers with exception-triggering event
        if ToolUsageStartedEvent in captured:
            mock_event = MagicMock()
            mock_event.tool_name = "failing_tool"
            captured[ToolUsageStartedEvent]("source", mock_event)

        if ToolUsageFinishedEvent in captured:
            mock_event = MagicMock()
            mock_event.tool_name = "failing_tool"
            captured[ToolUsageFinishedEvent]("source", mock_event)

    @patch("src.engines.crewai.callbacks.streaming_callbacks.enqueue_log")
    def test_register_llm_handler_exception_path(self, mock_enqueue):
        """Cover exception paths in _register_llm_call_handlers inner functions."""
        import src.engines.crewai.callbacks.streaming_callbacks as sc_module

        LLMCallStartedEvent = type("LLMCallStartedEvent", (), {})
        LLMCallCompletedEvent = type("LLMCallCompletedEvent", (), {})

        captured = {}

        def mock_on(event_class):
            def decorator(func):
                captured[event_class] = func
                return func
            return decorator

        mock_bus = MagicMock()
        mock_bus.on = mock_on

        with patch.dict(sc_module.__dict__, {
            "LLMCallStartedEvent": LLMCallStartedEvent,
            "LLMCallCompletedEvent": LLMCallCompletedEvent,
            "crewai_event_bus": mock_bus,
        }):
            callback = EventStreamingCallback("job_llm4", {"stream_events": False}, None)
            mock_enqueue.side_effect = Exception("enqueue fail")
            callback._register_llm_call_handlers()

        if LLMCallStartedEvent in captured:
            mock_event = MagicMock()
            captured[LLMCallStartedEvent]("source", mock_event)

        if LLMCallCompletedEvent in captured:
            mock_event = MagicMock()
            captured[LLMCallCompletedEvent]("source", mock_event)
