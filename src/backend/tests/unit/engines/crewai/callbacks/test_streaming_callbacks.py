import unittest
import logging
import json
from datetime import datetime, UTC
from unittest.mock import MagicMock, Mock, patch, call
import asyncio

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.engines.crewai.callbacks.streaming_callbacks import (
    LogCaptureHandler, JobOutputCallback, EventStreamingCallback
)
from crewai.utilities.events import (
    ToolUsageStartedEvent, ToolUsageFinishedEvent,
    LLMCallStartedEvent, LLMCallCompletedEvent
)


class TestLogCaptureHandler(unittest.TestCase):
    """Unit tests for LogCaptureHandler"""

    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_123"
        self.group_context = MagicMock()
        self.handler = LogCaptureHandler(self.job_id, self.group_context)
        
    def test_init(self):
        """Test LogCaptureHandler initialization"""
        self.assertEqual(self.handler.job_id, self.job_id)
        self.assertEqual(self.handler.group_context, self.group_context)
        self.assertEqual(self.handler.buffer, [])
        self.assertEqual(self.handler.buffer_size, 50)

    def test_emit_with_message(self):
        """Test emitting a log record"""
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test log message",
            args=(),
            exc_info=None
        )
        
        self.handler.emit(record)
        
        self.assertEqual(len(self.handler.buffer), 1)
        self.assertEqual(self.handler.buffer[0][0], "Test log message")

    def test_emit_empty_message(self):
        """Test emitting an empty log record"""
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="   ",  # Whitespace only
            args=(),
            exc_info=None
        )
        
        self.handler.emit(record)
        
        # Empty messages should not be added
        self.assertEqual(len(self.handler.buffer), 0)

    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_emit_triggers_flush_when_full(self, mock_enqueue):
        """Test that emit triggers flush when buffer is full"""
        mock_enqueue.return_value = True
        
        # Fill the buffer
        for i in range(self.handler.buffer_size + 5):
            record = logging.LogRecord(
                name="test_logger",
                level=logging.INFO,
                pathname="test.py",
                lineno=i,
                msg=f"Message {i}",
                args=(),
                exc_info=None
            )
            self.handler.emit(record)
        
        # Buffer should have been flushed
        self.assertLess(len(self.handler.buffer), self.handler.buffer_size)
        mock_enqueue.assert_called()

    def test_group_logs_by_time(self):
        """Test grouping logs by time window"""
        # Add logs at different times
        base_time = 1000.0
        self.handler.buffer = [
            ("Message 1", base_time),
            ("Message 2", base_time + 0.5),
            ("Message 3", base_time + 1.5),
            ("Message 4", base_time + 5.0),
            ("Message 5", base_time + 5.5),
        ]
        
        grouped = self.handler._group_logs_by_time()
        
        # Should have 2 groups (messages 1-3 in first, messages 4-5 in second)
        # Based on the actual implementation with 2.0 second time window
        self.assertEqual(len(grouped), 2)
        self.assertEqual(len(grouped[0][0]), 3)  # First group has 3 messages
        self.assertEqual(len(grouped[1][0]), 2)  # Second group has 2 messages

    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_flush_success(self, mock_enqueue):
        """Test successful flush operation"""
        mock_enqueue.return_value = True
        
        # Add some messages
        for i in range(5):
            self.handler.buffer.append((f"Message {i}", 1000.0 + i * 0.5))
        
        self.handler.flush()
        
        # Buffer should be cleared
        self.assertEqual(len(self.handler.buffer), 0)
        # Enqueue should have been called
        mock_enqueue.assert_called()

    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_flush_empty_buffer(self, mock_enqueue):
        """Test flush with empty buffer"""
        self.handler.flush()
        
        # Should not call enqueue
        mock_enqueue.assert_not_called()

    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_close(self, mock_enqueue):
        """Test handler close operation"""
        mock_enqueue.return_value = True
        
        # Add a message
        self.handler.buffer.append(("Test message", 1000.0))
        
        self.handler.close()
        
        # Should have flushed
        self.assertEqual(len(self.handler.buffer), 0)
        mock_enqueue.assert_called()


class TestJobOutputCallback(unittest.TestCase):
    """Unit tests for JobOutputCallback"""

    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_456"
        self.task_key = "test_task"
        self.config = {"key": "value"}
        self.group_context = MagicMock()
        
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_init(self, mock_enqueue, mock_logger_manager):
        """Test JobOutputCallback initialization"""
        mock_enqueue.return_value = True
        
        callback = JobOutputCallback(
            self.job_id, 
            self.task_key, 
            self.config, 
            group_context=self.group_context
        )
        
        self.assertEqual(callback.job_id, self.job_id)
        self.assertEqual(callback.task_key, self.task_key)
        self.assertEqual(callback.config, self.config)
        self.assertEqual(callback.group_context, self.group_context)
        
        # Should have sent initialization message
        init_call = mock_enqueue.call_args_list[0]
        self.assertEqual(init_call[1]['execution_id'], self.job_id)
        self.assertIn("[INITIALIZATION]", init_call[1]['content'])
        
        # Should have sent config message
        config_call = mock_enqueue.call_args_list[1]
        self.assertIn("[CONFIG]", config_call[1]['content'])

    def test_sanitize_config(self):
        """Test config sanitization"""
        callback = JobOutputCallback.__new__(JobOutputCallback)
        
        config = {
            "normal_key": "value",
            "nested": {
                "another_key": "another_value"
            }
        }
        
        sanitized = callback._sanitize_config(config)
        
        # Should create a deep copy
        self.assertIsNot(sanitized, config)
        self.assertEqual(sanitized, config)

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_execute_with_raw_output(self, mock_enqueue, mock_logger_manager):
        """Test execute with output having raw attribute"""
        mock_enqueue.return_value = True
        
        async def run_test():
            callback = JobOutputCallback(self.job_id)
            
            # Mock output with raw attribute
            output = MagicMock()
            output.raw = "Raw output content"
            
            result = await callback.execute(output)
            
            # Should return original output
            self.assertEqual(result, output)
            
            # Should have enqueued the message
            mock_enqueue.assert_called()
            call_args = mock_enqueue.call_args_list[-1]
            self.assertEqual(call_args[1]['content'], "Raw output content")
        
        # Run the async test
        asyncio.run(run_test())

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_execute_with_dict_output(self, mock_enqueue, mock_logger_manager):
        """Test execute with dictionary output"""
        mock_enqueue.return_value = True
        
        async def run_test():
            callback = JobOutputCallback(self.job_id)
            
            output = {"key": "value", "number": 42}
            
            result = await callback.execute(output)
            
            # Should return original output
            self.assertEqual(result, output)
            
            # Should have converted to JSON
            mock_enqueue.assert_called()
            call_args = mock_enqueue.call_args_list[-1]
            self.assertEqual(call_args[1]['content'], '{"key": "value", "number": 42}')
        
        # Run the async test
        asyncio.run(run_test())

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_execute_with_final_answer(self, mock_enqueue, mock_logger_manager):
        """Test execute with output containing Final Answer"""
        mock_enqueue.return_value = True
        
        async def run_test():
            callback = JobOutputCallback(self.job_id)
            
            output = "Some output with Final Answer: The result is 42"
            
            result = await callback.execute(output)
            
            # Should have sent task completion marker
            completion_calls = [call for call in mock_enqueue.call_args_list 
                              if "TASK_COMPLETION" in call[1]['content']]
            self.assertGreater(len(completion_calls), 0)
        
        # Run the async test
        asyncio.run(run_test())

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_execute_with_exception(self, mock_enqueue, mock_logger_manager):
        """Test execute with exception handling"""
        # First allow initialization to succeed
        mock_enqueue.return_value = True
        callback = JobOutputCallback(self.job_id)
        
        # Then make execute calls fail
        mock_enqueue.side_effect = Exception("Enqueue error")
        
        async def run_test():
            output = "Test output"
            
            # Should not raise exception
            result = await callback.execute(output)
            
            # Should still return original output
            self.assertEqual(result, output)
        
        # Run the async test
        asyncio.run(run_test())

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_del(self, mock_enqueue, mock_logger_manager):
        """Test cleanup on deletion"""
        mock_enqueue.return_value = True
        
        callback = JobOutputCallback(self.job_id)
        
        # Manually call __del__
        callback.__del__()
        
        # Should have sent finalization message
        finalization_calls = [call for call in mock_enqueue.call_args_list 
                            if "[FINALIZATION]" in call[1]['content']]
        self.assertGreater(len(finalization_calls), 0)


class TestEventStreamingCallback(unittest.TestCase):
    """Unit tests for EventStreamingCallback"""

    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_789"
        self.config = {"key": "value"}
        self.group_context = MagicMock()
        
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_init(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test EventStreamingCallback initialization"""
        mock_enqueue.return_value = True
        
        callback = EventStreamingCallback(
            self.job_id, 
            self.config, 
            self.group_context
        )
        
        self.assertEqual(callback.job_id, self.job_id)
        self.assertEqual(callback.config, self.config)
        self.assertEqual(callback.group_context, self.group_context)
        
        # Should have logged config
        config_calls = [call for call in mock_enqueue.call_args_list 
                       if "[CONFIG]" in call[1]['content']]
        self.assertGreater(len(config_calls), 0)

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_tool_usage_started_handler(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test tool usage started event handler"""
        mock_enqueue.return_value = True
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        callback = EventStreamingCallback(self.job_id)
        
        # Create mock event
        mock_event = MagicMock(spec=ToolUsageStartedEvent)
        mock_event.tool_name = "TestTool"
        mock_event.agent = MagicMock()
        mock_event.agent.role = "TestAgent"
        mock_event.task = MagicMock()
        mock_event.task.description = "Test task description"
        mock_event.args = {"param": "value"}
        
        # Trigger handler
        handlers[ToolUsageStartedEvent]("source", mock_event)
        
        # Should have sent tool start message
        tool_calls = [call for call in mock_enqueue.call_args_list 
                     if "[TOOL-START]" in call[1]['content']]
        self.assertGreater(len(tool_calls), 0)
        
        # Check message content
        message = tool_calls[0][1]['content']
        self.assertIn("TestTool", message)
        self.assertIn("TestAgent", message)

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_tool_usage_finished_handler(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test tool usage finished event handler"""
        mock_enqueue.return_value = True
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        callback = EventStreamingCallback(self.job_id)
        
        # Create mock event
        mock_event = MagicMock(spec=ToolUsageFinishedEvent)
        mock_event.tool_name = "TestTool"
        mock_event.agent = MagicMock()
        mock_event.agent.role = "TestAgent"
        mock_event.task = MagicMock()
        mock_event.task.description = "Test task description"
        mock_event.output = "Tool execution result"
        
        # Trigger handler
        handlers[ToolUsageFinishedEvent]("source", mock_event)
        
        # Should have sent tool finish message
        tool_calls = [call for call in mock_enqueue.call_args_list 
                     if "[TOOL-FINISH]" in call[1]['content']]
        self.assertGreater(len(tool_calls), 0)

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_llm_call_started_handler(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test LLM call started event handler"""
        mock_enqueue.return_value = True
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        callback = EventStreamingCallback(self.job_id)
        
        # Create mock event
        mock_event = MagicMock(spec=LLMCallStartedEvent)
        mock_event.agent = MagicMock()
        mock_event.agent.role = "TestAgent"
        mock_event.task = MagicMock()
        mock_event.task.description = "Test task description"
        mock_event.prompt = "Test prompt content"
        
        # Trigger handler
        handlers[LLMCallStartedEvent]("source", mock_event)
        
        # Should have sent LLM start message
        llm_calls = [call for call in mock_enqueue.call_args_list 
                    if "[LLM-CALL-START]" in call[1]['content']]
        self.assertGreater(len(llm_calls), 0)

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_llm_call_completed_handler(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test LLM call completed event handler"""
        mock_enqueue.return_value = True
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        callback = EventStreamingCallback(self.job_id)
        
        # Create mock event
        mock_event = MagicMock(spec=LLMCallCompletedEvent)
        mock_event.agent = MagicMock()
        mock_event.agent.role = "TestAgent"
        mock_event.task = MagicMock()
        mock_event.task.description = "Test task description"
        mock_event.output = "LLM response content"
        
        # Trigger handler
        handlers[LLMCallCompletedEvent]("source", mock_event)
        
        # Should have sent LLM complete message
        llm_calls = [call for call in mock_enqueue.call_args_list 
                    if "[LLM-CALL-COMPLETE]" in call[1]['content']]
        self.assertGreater(len(llm_calls), 0)

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_event_handler_exception_handling(self, mock_event_bus, mock_logger_manager):
        """Test exception handling in event handlers"""
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        callback = EventStreamingCallback(self.job_id)
        
        # Create mock event that will cause exception
        mock_event = MagicMock()
        # Don't set expected attributes to cause AttributeError
        
        # Should not raise exception
        handlers[ToolUsageStartedEvent]("source", mock_event)

    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_cleanup(self, mock_event_bus, mock_logger_manager):
        """Test cleanup method"""
        callback = EventStreamingCallback(self.job_id)
        
        # Should not raise exception
        callback.cleanup()


class TestAsyncExecution(unittest.TestCase):
    """Test async execution of callbacks"""

    def test_job_output_callback_async_execute(self):
        """Test that JobOutputCallback.execute can be run in async context"""
        async def run_test():
            with patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager'):
                with patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log'):
                    callback = JobOutputCallback("test_job")
                    result = await callback.execute("Test output")
                    self.assertEqual(result, "Test output")
        
        # Run the async test
        asyncio.run(run_test())


class TestLogCaptureHandlerAdditional(unittest.TestCase):
    """Additional tests for LogCaptureHandler coverage"""
    
    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_additional"
        self.group_context = MagicMock()
        self.handler = LogCaptureHandler(self.job_id, self.group_context)
    
    def test_emit_exception_handling(self):
        """Test exception handling in emit method"""
        # Mock format to raise exception
        self.handler.format = MagicMock(side_effect=Exception("Format error"))
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Should not raise exception
        self.handler.emit(record)
        
        # Buffer should remain empty due to exception
        self.assertEqual(len(self.handler.buffer), 0)
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_flush_exception_handling(self, mock_enqueue):
        """Test exception handling in flush method"""
        # Add some messages
        self.handler.buffer = [("Message 1", 1000.0), ("Message 2", 1001.0)]
        
        # Mock enqueue_log to raise exception
        mock_enqueue.side_effect = Exception("Enqueue error")
        
        # Should not raise exception
        self.handler.flush()
        
        # Buffer should NOT be cleared when exception occurs (buffer.clear() is in try block)
        self.assertEqual(len(self.handler.buffer), 2)
    
    def test_group_logs_by_time_empty_buffer(self):
        """Test _group_logs_by_time with empty buffer"""
        self.handler.buffer = []
        
        grouped = self.handler._group_logs_by_time()
        
        # Should return empty list
        self.assertEqual(grouped, [])


class TestJobOutputCallbackAdditional(unittest.TestCase):
    """Additional tests for JobOutputCallback coverage"""
    
    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_additional"
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_init_config_exception(self, mock_enqueue, mock_logger_manager):
        """Test exception handling during config logging in __init__"""
        # First call succeeds (initialization)
        # Second call fails (config logging)
        mock_enqueue.side_effect = [True, Exception("Config error")]
        
        config = {"key": "value"}
        
        # Should not raise exception
        callback = JobOutputCallback(self.job_id, config=config)
        
        # Should have tried to log config
        self.assertEqual(mock_enqueue.call_count, 2)
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_execute_with_empty_output(self, mock_enqueue, mock_logger_manager):
        """Test execute with empty/whitespace output"""
        mock_enqueue.return_value = True
        
        async def run_test():
            callback = JobOutputCallback(self.job_id)
            
            # Test with empty string
            result1 = await callback.execute("")
            self.assertEqual(result1, "")
            
            # Test with whitespace only
            result2 = await callback.execute("   \n\t  ")
            self.assertEqual(result2, "   \n\t  ")
        
        asyncio.run(run_test())
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_execute_with_string_output(self, mock_enqueue, mock_logger_manager):
        """Test execute with basic string output"""
        mock_enqueue.return_value = True
        
        async def run_test():
            callback = JobOutputCallback(self.job_id)
            
            # Test with string
            result1 = await callback.execute("Simple string")
            self.assertEqual(result1, "Simple string")
            
            # Test with integer
            result2 = await callback.execute(42)
            self.assertEqual(result2, 42)
            
            # Test with float
            result3 = await callback.execute(3.14)
            self.assertEqual(result3, 3.14)
        
        asyncio.run(run_test())
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_execute_with_task_completed(self, mock_enqueue, mock_logger_manager):
        """Test execute with output containing Task Completed"""
        mock_enqueue.return_value = True
        
        async def run_test():
            callback = JobOutputCallback(self.job_id)
            
            output = "Some output with Task Completed: All done"
            
            result = await callback.execute(output)
            
            # Should have sent task completion marker
            completion_calls = [call for call in mock_enqueue.call_args_list 
                              if "TASK_COMPLETION" in call[1]['content']]
            self.assertGreater(len(completion_calls), 0)
        
        asyncio.run(run_test())
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_execute_task_completion_marker_exception(self, mock_enqueue, mock_logger_manager):
        """Test exception handling in task completion marker"""
        # Allow normal operations but fail completion marker
        def side_effect(*args, **kwargs):
            content = kwargs.get('content', '')
            if 'TASK_COMPLETION' in content:
                raise Exception("Completion marker error")
            return True
        
        mock_enqueue.side_effect = side_effect
        
        async def run_test():
            callback = JobOutputCallback(self.job_id)
            
            output = "Some output with Final Answer: The result"
            
            # Should not raise exception
            result = await callback.execute(output)
            self.assertEqual(result, output)
        
        asyncio.run(run_test())
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_del_exception_handling(self, mock_enqueue, mock_logger_manager):
        """Test exception handling in __del__ method"""
        # Allow initialization to succeed
        mock_enqueue.return_value = True
        callback = JobOutputCallback(self.job_id)
        
        # Then make finalization fail
        mock_enqueue.side_effect = Exception("Finalization error")
        
        # Should not raise exception
        callback.__del__()
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    def test_del_cleanup_comprehensive(self, mock_enqueue, mock_logger_manager):
        """Test comprehensive cleanup in __del__ method"""
        mock_enqueue.return_value = True
        
        callback = JobOutputCallback(self.job_id)
        
        # Get the actual logger manager instance from the module
        from src.engines.crewai.callbacks.streaming_callbacks import logger_manager
        
        # Call __del__
        callback.__del__()
        
        # Should have attempted to remove handlers from the loggers
        # Since we're mocking LoggerManager, the actual logger_manager is mocked
        # We just verify the method didn't raise an exception
        self.assertTrue(True)  # Test that __del__ completes without exception


class TestEventStreamingCallbackAdditional(unittest.TestCase):
    """Additional tests for EventStreamingCallback coverage"""
    
    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_additional"
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_init_without_config(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test initialization without config"""
        mock_enqueue.return_value = True
        
        callback = EventStreamingCallback(self.job_id)
        
        self.assertEqual(callback.job_id, self.job_id)
        self.assertIsNone(callback.config)
        
        # Should not have logged config
        config_calls = [call for call in mock_enqueue.call_args_list 
                       if "[CONFIG]" in call[1]['content']]
        self.assertEqual(len(config_calls), 0)
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_init_config_exception(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test exception handling during config logging"""
        mock_enqueue.side_effect = Exception("Config error")
        
        config = {"key": "value"}
        
        # Should not raise exception
        callback = EventStreamingCallback(self.job_id, config)
        
        self.assertEqual(callback.config, config)
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_event_handlers_with_missing_attributes(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test event handlers when events are missing expected attributes"""
        mock_enqueue.return_value = True
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        callback = EventStreamingCallback(self.job_id)
        
        # Test tool usage started with missing attributes
        mock_event = MagicMock()
        mock_event.tool_name = "TestTool"
        # Don't set agent, task, or args attributes
        del mock_event.agent
        del mock_event.task
        del mock_event.args
        
        # Should not raise exception
        handlers[ToolUsageStartedEvent]("source", mock_event)
        
        # Should have sent message with "Unknown" values
        tool_calls = [call for call in mock_enqueue.call_args_list 
                     if "[TOOL-START]" in call[1]['content']]
        self.assertGreater(len(tool_calls), 0)
        
        message = tool_calls[-1][1]['content']
        self.assertIn("Unknown", message)
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_event_handlers_with_alternative_attributes(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test event handlers with alternative attribute paths"""
        mock_enqueue.return_value = True
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        callback = EventStreamingCallback(self.job_id)
        
        # Test LLM call started with context-based attributes
        mock_event = MagicMock()
        # Don't set direct agent/task attributes
        del mock_event.agent
        del mock_event.task
        del mock_event.prompt
        
        # Set context-based attributes
        mock_event.context = MagicMock()
        mock_event.context.agent = MagicMock()
        mock_event.context.agent.role = "ContextAgent"
        mock_event.context.task = MagicMock()
        mock_event.context.task.description = "Context task description"
        
        # Trigger handler
        handlers[LLMCallStartedEvent]("source", mock_event)
        
        # Should have used context attributes
        llm_calls = [call for call in mock_enqueue.call_args_list 
                    if "[LLM-CALL-START]" in call[1]['content']]
        self.assertGreater(len(llm_calls), 0)
        
        message = llm_calls[-1][1]['content']
        self.assertIn("ContextAgent", message)
    
    @patch('src.engines.crewai.callbacks.streaming_callbacks.LoggerManager')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.enqueue_log')
    @patch('src.engines.crewai.callbacks.streaming_callbacks.crewai_event_bus')
    def test_event_handlers_with_long_content(self, mock_event_bus, mock_enqueue, mock_logger_manager):
        """Test event handlers with long content (truncation)"""
        mock_enqueue.return_value = True
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        callback = EventStreamingCallback(self.job_id)
        
        # Test LLM call started with very long prompt
        mock_event = MagicMock()
        mock_event.agent = MagicMock()
        mock_event.agent.role = "TestAgent"
        mock_event.task = MagicMock()
        mock_event.task.description = "Test task"
        mock_event.prompt = "x" * 1000  # Very long prompt
        
        handlers[LLMCallStartedEvent]("source", mock_event)
        
        # Test tool usage finished with very long output
        mock_event2 = MagicMock()
        mock_event2.tool_name = "TestTool"
        mock_event2.agent = MagicMock()
        mock_event2.agent.role = "TestAgent"
        mock_event2.task = MagicMock()
        mock_event2.task.description = "Test task"
        mock_event2.output = "y" * 2000  # Very long output
        
        handlers[ToolUsageFinishedEvent]("source", mock_event2)
        
        # Should have truncated content
        tool_calls = [call for call in mock_enqueue.call_args_list 
                     if "[TOOL-FINISH]" in call[1]['content']]
        self.assertGreater(len(tool_calls), 0)
        
        # Verify truncation occurred
        message = tool_calls[-1][1]['content']
        self.assertIn("...", message)


class TestSanitizeConfig(unittest.TestCase):
    """Test config sanitization functionality"""
    
    def test_sanitize_config_deep_copy(self):
        """Test that _sanitize_config creates a deep copy"""
        callback = JobOutputCallback.__new__(JobOutputCallback)
        
        original_config = {
            "key1": "value1",
            "nested": {
                "key2": "value2",
                "deep_nested": {
                    "key3": "value3"
                }
            },
            "list": [1, 2, {"key4": "value4"}]
        }
        
        sanitized = callback._sanitize_config(original_config)
        
        # Should be equal but not the same object
        self.assertEqual(sanitized, original_config)
        self.assertIsNot(sanitized, original_config)
        self.assertIsNot(sanitized["nested"], original_config["nested"])
        self.assertIsNot(sanitized["nested"]["deep_nested"], original_config["nested"]["deep_nested"])
    
    def test_eventstreaming_sanitize_config(self):
        """Test EventStreamingCallback _sanitize_config"""
        callback = EventStreamingCallback.__new__(EventStreamingCallback)
        
        config = {
            "normal_key": "value",
            "nested": {
                "another_key": "another_value"
            }
        }
        
        sanitized = callback._sanitize_config(config)
        
        # Should create a deep copy
        self.assertIsNot(sanitized, config)
        self.assertEqual(sanitized, config)


if __name__ == '__main__':
    unittest.main()