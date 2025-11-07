"""
Unit tests for the rewritten logging callbacks module.

Tests the event-driven logging architecture for CrewAI 0.177+ which consolidates
event handling through AgentExecutionCompletedEvent and other core events.
"""

import pytest
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch, call, AsyncMock, ANY
import queue
import json
import os

from src.engines.crewai.callbacks.logging_callbacks import (
    AgentTraceEventListener,
    TaskCompletionEventListener,
    EventTypeDetector
)
from src.utils.user_context import GroupContext


class TestEventTypeDetector:
    """Test suite for EventTypeDetector utility class."""
    
    def test_detect_tool_usage_with_using_tool_pattern(self):
        """Test detection of tool usage with 'Using tool:' pattern."""
        output = "Using tool: SearchTool\nTool Output: Results found"
        event_type, context, extra_data = EventTypeDetector.detect_event_type(output)
        
        assert event_type == "tool_usage"
        assert context == "SearchTool"
        assert extra_data["tool_name"] == "SearchTool"
        assert extra_data["tool_output"] == "Results found"  # Only the content after 'Tool Output:'
    
    def test_detect_tool_usage_with_action_pattern(self):
        """Test detection of tool usage with 'Action:' pattern."""
        output = "Action: WebScrapeTool\nAction Output: Page content extracted"
        event_type, context, extra_data = EventTypeDetector.detect_event_type(output)
        
        assert event_type == "tool_usage"
        assert context == "WebScrapeTool"
        assert extra_data["tool_name"] == "WebScrapeTool"
        assert extra_data["tool_output"] == "Page content extracted"  # Content after 'Action Output:'
    
    def test_detect_task_completion_with_final_answer(self):
        """Test detection of task completion with 'Final Answer:' pattern."""
        output = "Final Answer: The task has been completed successfully"
        event_type, context, extra_data = EventTypeDetector.detect_event_type(output)
        
        assert event_type == "task_completed"
        assert context == "task_completion"
        assert extra_data["final_answer"] == "The task has been completed successfully"  # Content after 'Final Answer:'
    
    def test_detect_llm_reasoning_patterns(self):
        """Test detection of LLM reasoning patterns."""
        output = "Thought: I need to analyze this problem step by step"
        event_type, context, extra_data = EventTypeDetector.detect_event_type(output)
        
        assert event_type == "llm_call"
        assert context == "reasoning"
        assert extra_data["pattern_matched"] == "Thought:"
    
    def test_detect_default_agent_execution(self):
        """Test default detection returns agent_execution."""
        output = "Some regular output without special patterns"
        event_type, context, extra_data = EventTypeDetector.detect_event_type(output)
        
        assert event_type == "agent_execution"
        assert context is None
        assert extra_data is None
    
    def test_empty_output_returns_agent_execution(self):
        """Test that empty output returns agent_execution."""
        event_type, context, extra_data = EventTypeDetector.detect_event_type("")
        
        assert event_type == "agent_execution"
        assert context is None
        assert extra_data is None


class TestAgentTraceEventListener:
    """Test suite for AgentTraceEventListener."""
    
    @pytest.fixture
    def setup(self):
        """Set up test environment."""
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
        
        return job_id, group_context
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_initialization(self, mock_get_queue, setup):
        """Test listener initialization."""
        job_id, group_context = setup
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(job_id, group_context)
        
        assert listener.job_id == job_id
        assert listener.group_context == group_context
        assert listener._queue == mock_queue
        assert listener._init_time is not None
        assert job_id in AgentTraceEventListener._task_registry
        assert job_id in AgentTraceEventListener._task_start_times
    
    def test_initialization_with_invalid_job_id(self, setup):
        """Test that initialization fails with invalid job_id."""
        _, group_context = setup
        
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            AgentTraceEventListener("", group_context)
        
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            AgentTraceEventListener(None, group_context)
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_extract_agent_info(self, mock_get_queue, setup):
        """Test extraction of agent information from event."""
        job_id, group_context = setup
        mock_get_queue.return_value = MagicMock()
        
        listener = AgentTraceEventListener(job_id, group_context)
        
        # Create mock event with agent
        mock_event = Mock()
        mock_event.agent = Mock()
        mock_event.agent.role = "Research Agent"
        mock_event.agent.id = "agent_001"
        
        agent_name, agent_id = listener._extract_agent_info(mock_event)
        
        assert agent_name == "Research Agent"
        assert agent_id == "agent_001"
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_extract_task_info(self, mock_get_queue, setup):
        """Test extraction of task information from event."""
        job_id, group_context = setup
        mock_get_queue.return_value = MagicMock()
        
        listener = AgentTraceEventListener(job_id, group_context)
        
        # Create mock event with task
        mock_event = Mock()
        mock_event.task = Mock()
        mock_event.task.description = "Research the latest AI trends"
        mock_event.task.id = "task_001"
        
        task_name, task_id, task_description = listener._extract_task_info(mock_event)
        
        assert task_name == "Research the latest AI trends"
        assert task_id == "task_001"
        assert task_description == "Research the latest AI trends"
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_enqueue_trace_main_process(self, mock_get_queue, mock_enqueue_log, setup):
        """Test trace enqueueing in main process mode."""
        job_id, group_context = setup
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        # Ensure we're not in subprocess mode
        os.environ.pop('CREW_SUBPROCESS_MODE', None)
        
        listener = AgentTraceEventListener(job_id, group_context)
        
        listener._enqueue_trace(
            event_source="Test Agent",
            event_context="test_context",
            event_type="test_event",
            output_content="Test output",
            extra_data={"key": "value"}
        )
        
        # Verify trace was put on queue
        mock_queue.put.assert_called_once()
        trace_data = mock_queue.put.call_args[0][0]
        
        assert trace_data["job_id"] == job_id
        assert trace_data["event_source"] == "Test Agent"
        assert trace_data["event_context"] == "test_context"
        assert trace_data["event_type"] == "test_event"
        assert trace_data["output"]["content"] == "Test output"
        assert trace_data["output"]["extra_data"]["key"] == "value"
        assert trace_data["group_id"] == "group_123"
        assert trace_data["group_email"] == "test@example.com"
        
        # Verify log was enqueued
        mock_enqueue_log.assert_called_once_with(
            job_id,
            {"content": "Test output", "timestamp": ANY}
        )
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.crewai_event_bus')
    def test_setup_listeners_registers_handlers(self, mock_event_bus, mock_get_queue, setup):
        """Test that setup_listeners registers all event handlers."""
        job_id, group_context = setup
        mock_get_queue.return_value = MagicMock()
        
        listener = AgentTraceEventListener(job_id, group_context)
        
        # Track registered handlers
        registered_handlers = []
        
        def mock_on(event_class):
            def decorator(func):
                registered_handlers.append((event_class, func))
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Verify core handlers were registered
        event_types = [handler[0] for handler in registered_handlers]
        
        # These should always be registered (only import events that exist in CrewAI 0.177+)
        from crewai.events import (
            AgentExecutionCompletedEvent,
            CrewKickoffStartedEvent,
            CrewKickoffCompletedEvent
        )
        # LLMStreamChunkEvent import separately as it may not always be available
        try:
            from crewai.events import LLMStreamChunkEvent
        except ImportError:
            LLMStreamChunkEvent = None
        
        assert AgentExecutionCompletedEvent in event_types
        assert CrewKickoffStartedEvent in event_types
        assert CrewKickoffCompletedEvent in event_types
        # Only check for LLMStreamChunkEvent if it's available
        if LLMStreamChunkEvent is not None:
            assert LLMStreamChunkEvent in event_types
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_update_active_context(self, mock_get_queue, setup):
        """Test updating active context for agent and task."""
        job_id, group_context = setup
        mock_get_queue.return_value = MagicMock()
        
        listener = AgentTraceEventListener(job_id, group_context)
        
        # Update context
        listener._update_active_context("Research Agent", "Analyze data", "[Test]")
        
        # Verify context was updated
        assert job_id in listener._active_context
        assert listener._active_context[job_id]['agent'] == "Research Agent"
        assert listener._active_context[job_id]['task'] == "Analyze data"
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.TaskTrackingService')
    def test_update_task_status_in_subprocess(self, mock_task_service, setup):
        """Test task status update in subprocess mode."""
        job_id, group_context = setup

        # Set subprocess mode
        os.environ['CREW_SUBPROCESS_MODE'] = 'true'

        try:
            # Mock TaskTrackingService (kept for interface stability)
            mock_service_instance = AsyncMock()
            mock_task_service.return_value = mock_service_instance

            with patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue') as mock_get_queue:
                mock_get_queue.return_value = MagicMock()
                listener = AgentTraceEventListener(job_id, group_context)

                # This would normally be called in a thread
                from src.schemas.task_tracking import TaskStatusEnum
                listener._update_task_status("Test Task", TaskStatusEnum.COMPLETED, "[Test]")

                # The actual update happens in a thread, so we can't easily verify it
                # Just ensure no exceptions are raised

        finally:
            # Clean up
            os.environ.pop('CREW_SUBPROCESS_MODE', None)


class TestTaskCompletionEventListener:
    """Test suite for TaskCompletionEventListener."""
    
    def test_initialization(self):
        """Test TaskCompletionEventListener initialization."""
        job_id = "test_job_456"
        group_context = MagicMock()
        
        listener = TaskCompletionEventListener(job_id, group_context)
        
        assert listener.job_id == job_id
        assert listener.group_context == group_context
        assert listener._init_time is not None
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.crewai_event_bus')
    def test_setup_listeners_is_noop(self, mock_event_bus):
        """Test that setup_listeners is a no-op (deprecated class)."""
        listener = TaskCompletionEventListener("test_job", None)
        
        # Should not raise any errors
        listener.setup_listeners(mock_event_bus)
        
        # Should not register any handlers (deprecated)
        mock_event_bus.on.assert_not_called()


class TestIntegrationScenarios:
    """Integration tests for event handling scenarios."""
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.crewai_event_bus')
    def test_agent_execution_with_tool_usage(self, mock_event_bus, mock_get_queue, mock_enqueue_log):
        """Test handling of agent execution that includes tool usage."""
        job_id = "test_job_789"
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(job_id, None)
        
        # Capture the handler
        handler = None
        def mock_on(event_class):
            def decorator(func):
                nonlocal handler
                if event_class.__name__ == 'AgentExecutionCompletedEvent':
                    handler = func
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event with tool usage output
        mock_event = Mock()
        mock_event.agent = Mock(role="Research Agent")
        mock_event.task = Mock(description="Search for information")
        mock_event.output = "Using tool: SearchTool\nTool Output: Found 5 results"
        mock_event.timestamp = datetime.now(timezone.utc)
        
        # Call the handler
        if handler:
            handler(None, mock_event)
        
        # Verify trace was enqueued with tool usage detection
        mock_queue.put.assert_called()
        trace_data = mock_queue.put.call_args[0][0]
        
        assert trace_data["event_type"] == "tool_usage"
        assert "tool:" in trace_data["event_context"]
        assert trace_data["output"]["extra_data"]["tool_name"] == "SearchTool"
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.crewai_event_bus')
    def test_crew_kickoff_lifecycle(self, mock_event_bus, mock_get_queue, mock_enqueue_log):
        """Test handling of crew kickoff start and completion events."""
        job_id = "test_job_crew"
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(job_id, None)
        
        # Capture handlers
        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class.__name__] = func
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        listener.setup_listeners(mock_event_bus)
        
        # Test kickoff started
        if 'CrewKickoffStartedEvent' in handlers:
            mock_start_event = Mock()
            mock_start_event.crew_name = "Research Crew"
            mock_start_event.inputs = {"topic": "AI"}
            
            handlers['CrewKickoffStartedEvent'](None, mock_start_event)
            
            # Verify trace for crew start
            trace_data = mock_queue.put.call_args_list[0][0][0]
            assert trace_data["event_type"] == "crew_started"
            assert trace_data["event_source"] == "crew"
        
        # Test kickoff completed
        if 'CrewKickoffCompletedEvent' in handlers:
            mock_complete_event = Mock()
            mock_complete_event.crew_name = "Research Crew"
            mock_complete_event.output = "Research completed successfully"
            mock_complete_event.total_tokens = 1500
            
            handlers['CrewKickoffCompletedEvent'](None, mock_complete_event)
            
            # Verify trace for crew completion
            trace_data = mock_queue.put.call_args_list[-1][0][0]
            assert trace_data["event_type"] == "crew_completed"
            assert trace_data["output"]["extra_data"]["total_tokens"] == 1500


if __name__ == "__main__":
    pytest.main([__file__, "-v"])