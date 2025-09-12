"""
Unit tests for streaming callbacks module.

Tests the real-time streaming of CrewAI events and logs for live monitoring
of agent execution progress in the updated CrewAI 0.177+ architecture.
"""

import pytest
import logging
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch, call, AsyncMock, ANY
import asyncio
import queue

from src.engines.crewai.callbacks.streaming_callbacks import (
    LogCaptureHandler,
    JobOutputCallback,
    EventStreamingCallback
)
from src.utils.user_context import GroupContext
# Import available events from CrewAI 0.177
from crewai.events import (
    AgentExecutionCompletedEvent,
    CrewKickoffStartedEvent,
    CrewKickoffCompletedEvent
)


class TestLogCaptureHandler:
    """Test suite for LogCaptureHandler."""
    
    def test_initialization(self):
        """Test LogCaptureHandler initialization."""
        job_id = "test_job_123"
        group_context = MagicMock()
        
        handler = LogCaptureHandler(job_id, group_context)
        
        assert handler.job_id == job_id
        assert handler.group_context == group_context
        assert handler.buffer == []
        assert handler.buffer_size == 50
    
    def test_emit_with_message(self):
        """Test emitting a log record."""
        handler = LogCaptureHandler("test_job", None)
        
        # Create a log record
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test log message",
            args=(),
            exc_info=None
        )
        
        handler.emit(record)
        
        # Check buffer contains the message
        assert len(handler.buffer) == 1
        assert "Test log message" in handler.buffer[0][0]
    
    def test_emit_with_empty_message(self):
        """Test that empty messages are not added to buffer."""
        handler = LogCaptureHandler("test_job", None)
        
        # Create a log record with empty message
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="   ",  # Whitespace only
            args=(),
            exc_info=None
        )
        
        handler.emit(record)
        
        # Buffer should remain empty
        assert len(handler.buffer) == 0
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LogCaptureHandler.flush')
    def test_auto_flush_on_buffer_full(self, mock_flush):
        """Test that buffer automatically flushes when full."""
        handler = LogCaptureHandler("test_job", None)
        handler.buffer_size = 3  # Small buffer for testing
        
        # Add messages to fill buffer
        for i in range(4):
            record = logging.LogRecord(
                name="test_logger",
                level=logging.INFO,
                pathname="test.py",
                lineno=i,
                msg=f"Message {i}",
                args=(),
                exc_info=None
            )
            handler.emit(record)
        
        # Flush should have been called
        mock_flush.assert_called()
    
    def test_group_logs_by_time(self):
        """Test grouping logs by time window."""
        handler = LogCaptureHandler("test_job", None)
        
        # Add logs with different timestamps
        base_time = 1000.0
        handler.buffer = [
            ("Log 1", base_time),
            ("Log 2", base_time + 0.5),  # Within 2 second window
            ("Log 3", base_time + 1.5),  # Within 2 second window
            ("Log 4", base_time + 3.0),  # Outside window, new group
            ("Log 5", base_time + 3.5),  # Within new window
        ]
        
        grouped = handler._group_logs_by_time()
        
        # Should have 2 groups
        assert len(grouped) == 2
        # First group should have 3 logs (grouped[0][0] is a list of messages)
        assert len(grouped[0][0]) == 3
        # Second group should have 2 logs
        assert len(grouped[1][0]) == 2


class TestJobOutputCallback:
    """Test suite for JobOutputCallback."""
    
    @pytest.fixture
    def setup(self):
        """Set up test environment."""
        job_id = "test_job_456"
        config = {"stream_output": True}
        group_context = GroupContext(
            group_ids=["group_123"],
            group_email="test@example.com",
            email_domain="example.com"
        )
        return job_id, config, group_context
    
    def test_initialization(self, setup):
        """Test JobOutputCallback initialization."""
        job_id, config, group_context = setup
        
        callback = JobOutputCallback(job_id, config=config, group_context=group_context)
        
        assert callback.job_id == job_id
        assert callback.config == config
        assert callback.group_context == group_context
        # JobOutputCallback no longer has output_buffer or last_flush_time attributes
        # It uses log_handler instead
        assert hasattr(callback, 'log_handler')
        assert callback.log_handler is not None
    
    @pytest.mark.asyncio
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    async def test_execute(self, mock_enqueue_log, setup):
        """Test execute method (replaced on_output)."""
        job_id, config, group_context = setup
        callback = JobOutputCallback(job_id, config=config, group_context=group_context)
        
        # Test execute method which replaced on_output
        output = "Test output message"
        result = await callback.execute(output)
        
        # Check that output is returned unchanged
        assert result == output
        
        # Check that enqueue_log was called
        assert mock_enqueue_log.called
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_log_handler_flush(self, mock_enqueue_log, setup):
        """Test flushing logs through log handler."""
        job_id, config, group_context = setup
        callback = JobOutputCallback(job_id, config=config, group_context=group_context)
        
        # Add some logs to the handler buffer
        callback.log_handler.buffer = [
            ("Message 1", 1000.0),
            ("Message 2", 1001.0)
        ]
        
        # Flush the log handler
        callback.log_handler.flush()
        
        # Check enqueue was called (logs are grouped and sent together)
        assert mock_enqueue_log.called
        
        # Buffer should be cleared
        assert len(callback.log_handler.buffer) == 0
    
    def test_log_handler_auto_flush(self, setup):
        """Test automatic flush when buffer is full."""
        job_id, config, group_context = setup
        callback = JobOutputCallback(job_id, config=config, group_context=group_context)
        
        # Set a small buffer size for testing
        callback.log_handler.buffer_size = 2
        
        with patch.object(callback.log_handler, 'flush') as mock_flush:
            # Add logs to exceed buffer size
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="test.py",
                lineno=1,
                msg="Test message",
                args=(),
                exc_info=None
            )
            
            # Emit multiple records
            callback.log_handler.emit(record)
            callback.log_handler.emit(record)
            callback.log_handler.emit(record)  # This should trigger flush
            
            # Verify flush was called
            mock_flush.assert_called()
    
    def test_cleanup(self, setup):
        """Test cleanup through destructor."""
        job_id, config, group_context = setup
        callback = JobOutputCallback(job_id, config=config, group_context=group_context)
        
        # Add some logs to the handler
        callback.log_handler.buffer = [("Test message", 1000.0)]
        
        with patch.object(callback.log_handler, 'flush') as mock_flush:
            # Trigger cleanup through destructor
            callback.__del__()
            
            # Should flush remaining logs (may be called multiple times in cleanup)
            assert mock_flush.called


class TestEventStreamingCallback:
    """Test suite for EventStreamingCallback."""
    
    @pytest.fixture
    def setup(self):
        """Set up test environment."""
        job_id = "test_job_789"
        config = {"stream_events": True}
        group_context = GroupContext(
            group_ids=["group_123"],
            group_email="test@example.com",
            email_domain="example.com"
        )
        return job_id, config, group_context
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_initialization(self, mock_event_bus, setup):
        """Test EventStreamingCallback initialization."""
        job_id, config, group_context = setup
        
        callback = EventStreamingCallback(job_id, config, group_context)
        
        assert callback.job_id == job_id
        assert callback.config == config
        assert callback.group_context == group_context
        # With streaming enabled, handlers should be registered
        assert len(callback.handlers) == 3  # 3 event types registered
        
        # Should register handlers if streaming is enabled
        assert mock_event_bus.on.called
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_initialization_without_streaming(self, mock_event_bus):
        """Test initialization when streaming is disabled."""
        job_id = "test_job"
        config = {"stream_events": False}  # Disabled
        
        callback = EventStreamingCallback(job_id, config, None)
        
        # Should not register handlers
        mock_event_bus.on.assert_not_called()
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_register_handlers(self, mock_event_bus, mock_enqueue_log, setup):
        """Test handler registration."""
        job_id, config, group_context = setup
        
        # Track registered handlers
        registered_handlers = {}
        
        def mock_on(event_class):
            def decorator(func):
                registered_handlers[event_class] = func
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        
        callback = EventStreamingCallback(job_id, config, group_context)
        
        # Should register handlers for available events
        assert AgentExecutionCompletedEvent in registered_handlers
        assert CrewKickoffStartedEvent in registered_handlers
        assert CrewKickoffCompletedEvent in registered_handlers
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_agent_execution_handler(self, mock_event_bus, mock_enqueue_log, setup):
        """Test handling of AgentExecutionCompletedEvent."""
        job_id, config, group_context = setup
        
        # Capture handlers
        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        
        callback = EventStreamingCallback(job_id, config, group_context)
        
        # Create mock event
        mock_event = Mock(spec=AgentExecutionCompletedEvent)
        mock_event.agent = Mock()
        mock_event.agent.role = "Test Agent"
        mock_event.output = "Agent completed task successfully"
        
        # Trigger handler
        if AgentExecutionCompletedEvent in handlers:
            handlers[AgentExecutionCompletedEvent]("source", mock_event)
        
        # Should have enqueued a log
        mock_enqueue_log.assert_called()
        # Check keyword arguments
        call_kwargs = mock_enqueue_log.call_args.kwargs
        assert call_kwargs['execution_id'] == job_id
        assert "Test Agent" in call_kwargs['content']
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_crew_kickoff_started_handler(self, mock_event_bus, mock_enqueue_log, setup):
        """Test handling of CrewKickoffStartedEvent."""
        job_id, config, group_context = setup
        
        # Capture handlers
        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        
        callback = EventStreamingCallback(job_id, config, group_context)
        
        # Create mock event
        mock_event = Mock(spec=CrewKickoffStartedEvent)
        mock_event.crew_name = "Test Crew"
        
        # Trigger handler
        if CrewKickoffStartedEvent in handlers:
            handlers[CrewKickoffStartedEvent]("source", mock_event)
        
        # Should have enqueued a log
        mock_enqueue_log.assert_called()
        # Check keyword arguments
        call_kwargs = mock_enqueue_log.call_args.kwargs
        assert call_kwargs['execution_id'] == job_id
        assert "crew_started" in call_kwargs['content']
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_crew_kickoff_completed_handler(self, mock_event_bus, mock_enqueue_log, setup):
        """Test handling of CrewKickoffCompletedEvent."""
        job_id, config, group_context = setup
        
        # Capture handlers
        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        
        callback = EventStreamingCallback(job_id, config, group_context)
        
        # Create mock event
        mock_event = Mock(spec=CrewKickoffCompletedEvent)
        mock_event.crew_name = "Test Crew"
        mock_event.output = "Crew execution completed"
        
        # Trigger handler
        if CrewKickoffCompletedEvent in handlers:
            handlers[CrewKickoffCompletedEvent]("source", mock_event)
        
        # Should have enqueued a log
        mock_enqueue_log.assert_called()
        # Check keyword arguments
        call_kwargs = mock_enqueue_log.call_args.kwargs
        assert call_kwargs['execution_id'] == job_id
        assert "crew_completed" in call_kwargs['content']
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_cleanup(self, mock_event_bus, setup):
        """Test cleanup method."""
        job_id, config, group_context = setup
        
        # Track handlers
        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        
        callback = EventStreamingCallback(job_id, config, group_context)
        
        # Store handlers
        callback.handlers = handlers.copy()
        
        # Cleanup
        callback.cleanup()
        
        # Handlers should be cleared
        assert callback.handlers == {}
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.logger_manager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_error_handling_in_event_handler(self, mock_event_bus, mock_enqueue_log, mock_logger, setup):
        """Test error handling when event processing fails."""
        job_id, config, group_context = setup
        
        # Capture handlers
        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        
        callback = EventStreamingCallback(job_id, config, group_context)
        
        # Create mock event that will cause an error
        mock_event = Mock(spec=AgentExecutionCompletedEvent)
        # Set up a mock agent that raises AttributeError when accessing role
        mock_agent = Mock()
        mock_agent.role.side_effect = AttributeError("No role attribute")
        mock_event.agent = mock_agent
        
        # Trigger handler - should not raise exception
        if AgentExecutionCompletedEvent in handlers:
            handlers[AgentExecutionCompletedEvent]("source", mock_event)
        
        # Should have logged the error
        mock_logger.system.error.assert_called()


class TestIntegration:
    """Integration tests for streaming callbacks."""
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_full_event_flow(self, mock_event_bus, mock_enqueue_log):
        """Test complete event flow from crew start to completion."""
        job_id = "test_integration"
        config = {"stream_events": True}
        
        # Capture handlers
        handlers = {}
        def mock_on(event_class):
            def decorator(func):
                handlers[event_class] = func
                return func
            return decorator
        
        mock_event_bus.on = mock_on
        
        callback = EventStreamingCallback(job_id, config, None)
        
        # Simulate crew kickoff
        if CrewKickoffStartedEvent in handlers:
            mock_start = Mock(spec=CrewKickoffStartedEvent)
            mock_start.crew_name = "Integration Test Crew"
            handlers[CrewKickoffStartedEvent]("source", mock_start)
        
        # Simulate agent execution
        if AgentExecutionCompletedEvent in handlers:
            mock_exec = Mock(spec=AgentExecutionCompletedEvent)
            mock_exec.agent = Mock(role="Test Agent")
            mock_exec.output = "Processing task"
            handlers[AgentExecutionCompletedEvent]("source", mock_exec)
        
        # Simulate crew completion
        if CrewKickoffCompletedEvent in handlers:
            mock_complete = Mock(spec=CrewKickoffCompletedEvent)
            mock_complete.crew_name = "Integration Test Crew"
            mock_complete.output = "All tasks completed"
            handlers[CrewKickoffCompletedEvent]("source", mock_complete)
        
        # Verify events were logged
        assert mock_enqueue_log.call_count >= 3
        
        # Cleanup
        callback.cleanup()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])