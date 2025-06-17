import unittest
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch, call
import queue
import uuid
import inspect

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.engines.crewai.callbacks.logging_callbacks import (
    AgentTraceEventListener, TaskCompletionLogger, DetailedOutputLogger, 
    CrewLoggerHandler, AgentTraceCallback
)
from crewai.utilities.events import (
    AgentExecutionCompletedEvent, ToolUsageFinishedEvent,
    LLMCallCompletedEvent, TaskStartedEvent, TaskCompletedEvent,
    CrewKickoffStartedEvent, CrewKickoffCompletedEvent
)


class TestAgentTraceEventListener(unittest.TestCase):
    """Unit tests for AgentTraceEventListener"""

    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_123"
        self.group_context = MagicMock()
        self.group_context.primary_group_id = "group_123"
        self.group_context.group_email = "test@example.com"
        
        # Clear the init logged set to allow reinitialization
        AgentTraceEventListener._init_logged.clear()
        # Clear task registry
        AgentTraceEventListener._task_registry.clear()
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_init(self, mock_get_queue):
        """Test initialization of AgentTraceEventListener"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id, self.group_context)
        
        self.assertEqual(listener.job_id, self.job_id)
        self.assertEqual(listener.group_context, self.group_context)
        self.assertEqual(listener._queue, mock_queue)
        self.assertIsNotNone(listener._init_time)
        self.assertIn(self.job_id, AgentTraceEventListener._task_registry)

    def test_init_invalid_job_id(self):
        """Test initialization with invalid job_id"""
        with self.assertRaises(ValueError) as cm:
            AgentTraceEventListener("", self.group_context)
        self.assertIn("job_id must be a non-empty string", str(cm.exception))
        
        with self.assertRaises(ValueError):
            AgentTraceEventListener(None, self.group_context)

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.SessionLocal')
    def test_get_or_create_task_id_found_in_db(self, mock_session, mock_get_queue):
        """Test task ID retrieval when task exists in database"""
        mock_get_queue.return_value = MagicMock()
        listener = AgentTraceEventListener(self.job_id)
        
        # Mock database session and task
        mock_db = MagicMock()
        mock_task = MagicMock()
        mock_task.id = "existing_task_id"
        mock_task.name = "Test Task"
        
        mock_session.return_value.__enter__.return_value = mock_db
        mock_db.query.return_value.filter.return_value.first.return_value = mock_task
        
        task_id = listener._get_or_create_task_id("Test Task", "original_123")
        
        self.assertEqual(task_id, "existing_task_id")
        # Should be cached in registry
        registry_key = "Test Task:original_123"
        self.assertEqual(listener._task_registry[self.job_id][registry_key], "existing_task_id")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.SessionLocal')
    @patch('src.engines.crewai.callbacks.logging_callbacks.uuid.uuid4')
    def test_get_or_create_task_id_not_found(self, mock_uuid, mock_session, mock_get_queue):
        """Test task ID creation when task not in database"""
        mock_get_queue.return_value = MagicMock()
        mock_uuid.return_value = "new_uuid_123"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Mock database session returning no task
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db
        mock_db.query.return_value.filter.return_value.first.return_value = None
        mock_db.query.return_value.all.return_value = []
        
        task_id = listener._get_or_create_task_id("New Task", "original_456")
        
        self.assertEqual(task_id, "new_uuid_123")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_get_or_create_task_id_from_registry(self, mock_get_queue):
        """Test task ID retrieval from registry cache"""
        mock_get_queue.return_value = MagicMock()
        listener = AgentTraceEventListener(self.job_id)
        
        # Prepopulate registry
        registry_key = "Cached Task:original_789"
        listener._task_registry[self.job_id][registry_key] = "cached_id_789"
        
        task_id = listener._get_or_create_task_id("Cached Task", "original_789")
        
        self.assertEqual(task_id, "cached_id_789")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_enqueue_trace_success(self, mock_get_queue):
        """Test successful trace enqueueing"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id, self.group_context)
        
        listener._enqueue_trace(
            event_source="TestAgent",
            event_context="Test Task",
            event_type="test_event",
            output_content="Test output",
            extra_data={"key": "value"}
        )
        
        # Verify queue put was called
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["job_id"], self.job_id)
        self.assertEqual(trace_data["event_source"], "TestAgent")
        self.assertEqual(trace_data["event_context"], "Test Task")
        self.assertEqual(trace_data["event_type"], "test_event")
        self.assertEqual(trace_data["output_content"], "Test output")
        self.assertEqual(trace_data["extra_data"], {"key": "value"})
        self.assertEqual(trace_data["group_id"], "group_123")
        self.assertEqual(trace_data["group_email"], "test@example.com")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_enqueue_trace_queue_full(self, mock_get_queue):
        """Test trace enqueueing when queue is full"""
        mock_queue = MagicMock()
        mock_queue.put_nowait.side_effect = queue.Full()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Should not raise exception
        listener._enqueue_trace(
            event_source="TestAgent",
            event_context="Test Task",
            event_type="test_event",
            output_content="Test output"
        )

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_on_agent_execution_completed(self, mock_extract_agent, mock_get_queue):
        """Test handling of agent execution completed event"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.description = "Test Task Description"
        mock_event.task.markdown = True
        mock_event.output = "Agent execution output"
        
        # Trigger the handler
        handlers[AgentExecutionCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["event_source"], "TestAgent")
        self.assertEqual(trace_data["event_context"], "Test Task Description")
        self.assertEqual(trace_data["event_type"], "agent_execution")
        self.assertEqual(trace_data["output_content"], "Agent execution output")
        self.assertEqual(trace_data["extra_data"], {"markdown": True})

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_on_tool_usage_finished(self, mock_extract_agent, mock_get_queue):
        """Test handling of tool usage finished event"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.tool_name = "TestTool"
        mock_event.output = "Tool output data"
        mock_event.context = MagicMock()
        mock_event.context.task = MagicMock()
        mock_event.context.task.description = "Test Task"
        
        # Trigger the handler
        handlers[ToolUsageFinishedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["event_source"], "TestTool")
        self.assertEqual(trace_data["event_context"], "TestAgent")
        self.assertEqual(trace_data["event_type"], "tool_usage")
        self.assertEqual(trace_data["output_content"], "Tool output data")
        self.assertEqual(trace_data["extra_data"]["agent_role"], "TestAgent")
        self.assertEqual(trace_data["extra_data"]["task"], "Test Task")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_on_crew_kickoff_started(self, mock_get_queue):
        """Test handling of crew kickoff started event"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.crew_name = "TestCrew"
        
        # Trigger the handler
        handlers[CrewKickoffStartedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["event_source"], "crew")
        self.assertEqual(trace_data["event_context"], "TestCrew")
        self.assertEqual(trace_data["event_type"], "crew_started")
        self.assertIn("Crew 'TestCrew' execution started", trace_data["output_content"])
        
        # Verify task registry was reset
        self.assertEqual(listener._task_registry[self.job_id], {})

    def test_backward_compatibility_alias(self):
        """Test that AgentTraceCallback alias exists"""
        self.assertEqual(AgentTraceCallback, AgentTraceEventListener)

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.SessionLocal')
    def test_get_or_create_task_id_description_match(self, mock_session, mock_get_queue):
        """Test task ID retrieval by description match"""
        mock_get_queue.return_value = MagicMock()
        listener = AgentTraceEventListener(self.job_id)
        
        # Mock database session
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db
        
        # Mock task not found by name
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        # Mock task found by description
        mock_task = MagicMock()
        mock_task.id = "desc_task_id"
        mock_task.description = "This is a test task description with long text"
        mock_db.query.return_value.all.return_value = [mock_task]
        
        task_id = listener._get_or_create_task_id("test task description", "original_123")
        
        self.assertEqual(task_id, "desc_task_id")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.SessionLocal')
    def test_get_or_create_task_id_db_error(self, mock_session, mock_get_queue):
        """Test task ID creation when database error occurs"""
        mock_get_queue.return_value = MagicMock()
        listener = AgentTraceEventListener(self.job_id)
        
        # Mock database session to raise error
        mock_session.return_value.__enter__.side_effect = Exception("Database error")
        
        with patch('src.engines.crewai.callbacks.logging_callbacks.uuid.uuid4') as mock_uuid:
            mock_uuid.return_value = "error_uuid"
            task_id = listener._get_or_create_task_id("Error Task", "original_error")
            
            self.assertEqual(task_id, "error_uuid")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.SessionLocal')
    def test_get_or_create_task_id_table_missing(self, mock_session, mock_get_queue):
        """Test task ID creation when table is missing"""
        mock_get_queue.return_value = MagicMock()
        listener = AgentTraceEventListener(self.job_id)
        
        # Mock database session to raise 'no such table' error
        mock_session.return_value.__enter__.side_effect = Exception("no such table")
        
        with patch('src.engines.crewai.callbacks.logging_callbacks.uuid.uuid4') as mock_uuid:
            mock_uuid.return_value = "table_missing_uuid"
            task_id = listener._get_or_create_task_id("Missing Table Task", "original_missing")
            
            self.assertEqual(task_id, "table_missing_uuid")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_on_llm_call_completed(self, mock_extract_agent, mock_get_queue):
        """Test handling of LLM call completed event"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event with different output attributes
        mock_event = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.description = "Test LLM Task"
        mock_event.output = "LLM output"
        mock_event.call_type = "completion"
        
        # Trigger the handler
        handlers[LLMCallCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["event_source"], "llm")
        self.assertEqual(trace_data["event_context"], "completion")
        self.assertEqual(trace_data["event_type"], "llm_call")
        self.assertEqual(trace_data["output_content"], "LLM output")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_on_llm_call_completed_various_outputs(self, mock_extract_agent, mock_get_queue):
        """Test LLM call event with various output attribute names"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Test different output attributes
        test_cases = [
            {"response": "response_content"},
            {"result": "result_content"},
            {"completion": "completion_content"},
            {"content": "content_content"},
            {}  # No output attributes
        ]
        
        for i, attrs in enumerate(test_cases):
            # Create a simple object with only the intended attributes
            class MockEvent:
                def __init__(self, **kwargs):
                    for k, v in kwargs.items():
                        setattr(self, k, v)
                
                def __getattr__(self, name):
                    # Only return None for known output attributes that aren't set
                    if name in ['output', 'response', 'result', 'completion', 'content']:
                        return None
                    return MagicMock()
            
            mock_event = MockEvent(**attrs)
            
            # Trigger the handler
            handlers[LLMCallCompletedEvent]("source", mock_event)
            
            # Verify trace was enqueued
            call_args = mock_queue.put_nowait.call_args_list[i]
            trace_data = call_args[0][0]
            
            if attrs:
                expected_content = list(attrs.values())[0]
            else:
                expected_content = "LLM call completed (no output data available)"
            
            self.assertEqual(trace_data["output_content"], expected_content)

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_on_task_completed(self, mock_extract_agent, mock_get_queue):
        """Test handling of task completed event"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.description = "Test Completed Task"
        mock_event.task.id = "task_123"
        mock_event.output = "Task completed output"
        
        # Mock the task ID lookup
        with patch.object(listener, '_get_or_create_task_id', return_value="resolved_task_id"):
            # Trigger the handler
            handlers[TaskCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["event_source"], "task")
        self.assertEqual(trace_data["event_context"], "Test Completed Task")
        self.assertEqual(trace_data["event_type"], "task_completed")
        self.assertEqual(trace_data["output_content"], "Task completed output")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_on_task_started(self, mock_extract_agent, mock_get_queue):
        """Test handling of task started event"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.description = "Test Started Task"
        mock_event.task.id = "task_456"
        
        # Mock the task ID lookup
        with patch.object(listener, '_get_or_create_task_id', return_value="resolved_task_id"):
            # Trigger the handler
            handlers[TaskStartedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["event_source"], "task")
        self.assertEqual(trace_data["event_context"], "Test Started Task")
        self.assertEqual(trace_data["event_type"], "task_started")
        self.assertIn("Task 'Test Started Task' started by agent 'TestAgent'", trace_data["output_content"])

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_on_crew_kickoff_completed(self, mock_get_queue):
        """Test handling of crew kickoff completed event"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.crew_name = "TestCrew"
        mock_event.output = "Crew execution completed"
        
        # Trigger the handler
        handlers[CrewKickoffCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["event_source"], "crew")
        self.assertEqual(trace_data["event_context"], "TestCrew")
        self.assertEqual(trace_data["event_type"], "crew_completed")
        self.assertEqual(trace_data["output_content"], "Crew execution completed")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_enqueue_trace_without_group_context(self, mock_get_queue):
        """Test trace enqueueing without group context"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)  # No group context
        
        listener._enqueue_trace(
            event_source="TestAgent",
            event_context="Test Task",
            event_type="test_event",
            output_content="Test output"
        )
        
        # Verify queue put was called
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        # Should not have group_id or group_email
        self.assertNotIn("group_id", trace_data)
        self.assertNotIn("group_email", trace_data)

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_enqueue_trace_exception(self, mock_get_queue):
        """Test trace enqueueing when general exception occurs"""
        mock_queue = MagicMock()
        mock_queue.put_nowait.side_effect = Exception("General error")
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Should not raise exception
        listener._enqueue_trace(
            event_source="TestAgent",
            event_context="Test Task",
            event_type="test_event",
            output_content="Test output"
        )

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_setup_listeners_with_inspect(self, mock_get_queue):
        """Test setup_listeners method with inspect debugging"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        
        # Should not raise exception
        listener.setup_listeners(mock_event_bus)

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_agent_execution_event_context_variations(self, mock_extract_agent, mock_get_queue):
        """Test agent execution event with different context variations"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Test with context.task.markdown
        mock_event = MagicMock()
        mock_event.task = None
        mock_event.context = MagicMock()
        mock_event.context.task = MagicMock()
        mock_event.context.task.markdown = True
        mock_event.output = "Agent execution output"
        
        # Trigger the handler
        handlers[AgentExecutionCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued with markdown flag
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        self.assertEqual(trace_data["extra_data"], {"markdown": True})

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_tool_usage_event_variations(self, mock_extract_agent, mock_get_queue):
        """Test tool usage event with different attribute arrangements"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Test with event.task instead of event.context.task (no context attribute)
        class MockEvent:
            def __init__(self):
                self.tool_name = "TestTool"
                self.output = "Tool output data"
                self.task = MagicMock()
                self.task.description = "Direct Task"
                
            def __getattr__(self, name):
                if name == 'context':
                    raise AttributeError(f"'{type(self).__name__}' object has no attribute 'context'")
                return super().__getattribute__(name)
        
        mock_event = MockEvent()
        
        # Trigger the handler
        handlers[ToolUsageFinishedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["extra_data"]["task"], "Direct Task")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_task_events_context_variations(self, mock_extract_agent, mock_get_queue):
        """Test task events with different context arrangements"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Test with event.context.task (no event.task attribute)
        class MockEvent:
            def __init__(self):
                self.context = MagicMock()
                self.context.task = MagicMock()
                self.context.task.description = "Context Task"
                self.context.task.id = "context_task_id"
                self.output = "Task output"
                
            def __getattr__(self, name):
                if name == 'task':
                    raise AttributeError(f"'{type(self).__name__}' object has no attribute 'task'")
                return super().__getattribute__(name)
        
        mock_event = MockEvent()
        
        # Mock the task ID lookup
        with patch.object(listener, '_get_or_create_task_id', return_value="resolved_task_id"):
            # Trigger the handler
            handlers[TaskCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["event_context"], "Context Task")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_llm_call_with_context_task(self, mock_extract_agent, mock_get_queue):
        """Test LLM call event with context.task"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event with context.task
        mock_event = MagicMock()
        mock_event.task = None
        mock_event.context = MagicMock()
        mock_event.context.task = MagicMock()
        mock_event.context.task.description = "Context LLM Task"
        mock_event.output = "LLM output"
        
        # Trigger the handler
        handlers[LLMCallCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["event_source"], "llm")
        self.assertEqual(trace_data["output_content"], "LLM output")

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_init_exception_handling(self, mock_get_queue):
        """Test initialization exception handling"""
        mock_get_queue.return_value = MagicMock()
        
        # Mock super().__init__ to raise an exception
        with patch('src.engines.crewai.callbacks.logging_callbacks.BaseEventListener.__init__', 
                  side_effect=Exception("Init error")):
            with self.assertRaises(Exception):
                AgentTraceEventListener(self.job_id)


class TestTaskCompletionLogger(unittest.TestCase):
    """Unit tests for TaskCompletionLogger"""

    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_456"
        
    def test_init(self):
        """Test TaskCompletionLogger initialization"""
        logger = TaskCompletionLogger(self.job_id)
        self.assertEqual(logger.job_id, self.job_id)

    def test_on_task_start(self):
        """Test task start logging"""
        logger = TaskCompletionLogger(self.job_id)
        
        # Mock task and agent
        mock_task = MagicMock()
        mock_task.id = "task_123"
        mock_task.description = "Test Task Description"
        
        mock_agent = MagicMock()
        mock_agent.role = "TestAgent"
        
        # Should not raise exception
        logger.on_task_start(mock_task, mock_agent)

    def test_on_task_end(self):
        """Test task end logging"""
        logger = TaskCompletionLogger(self.job_id)
        
        # Mock task and output
        mock_task = MagicMock()
        mock_task.id = "task_123"
        
        mock_output = "Task completed successfully"
        
        # Should not raise exception
        logger.on_task_end(mock_output, mock_task)

    def test_setup_listeners(self):
        """Test event listener setup"""
        logger = TaskCompletionLogger(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        logger.setup_listeners(mock_event_bus)
        
        # Verify handlers were registered
        self.assertIn(TaskStartedEvent, handlers)
        self.assertIn(TaskCompletedEvent, handlers)
        
        # Test event handlers
        mock_event = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.id = "test_task"
        mock_event.task.description = "Test Task"
        mock_event.agent = MagicMock()
        mock_event.agent.role = "TestAgent"
        mock_event.output = "Task output"
        
        # Test task started handler
        handlers[TaskStartedEvent]("source", mock_event)
        
        # Test task completed handler
        handlers[TaskCompletedEvent]("source", mock_event)
        
    def test_setup_listeners_event_handler_exceptions(self):
        """Test exception handling in event handlers"""
        logger = TaskCompletionLogger(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        logger.setup_listeners(mock_event_bus)
        
        # Test with malformed event that will cause exceptions
        mock_event = MagicMock()
        mock_event.task = None  # This will cause attribute errors
        mock_event.context = None
        
        # Should not raise exceptions
        handlers[TaskStartedEvent]("source", mock_event)
        handlers[TaskCompletedEvent]("source", mock_event)
        
    def test_on_task_start_with_none_agent(self):
        """Test task start with None agent"""
        logger = TaskCompletionLogger(self.job_id)
        
        mock_task = MagicMock()
        mock_task.id = "task_123"
        mock_task.description = "Test Task"
        
        # Should handle None agent gracefully
        logger.on_task_start(mock_task, None)
        
    def test_on_task_start_with_missing_attributes(self):
        """Test task start with missing attributes"""
        logger = TaskCompletionLogger(self.job_id)
        
        # Mock task without attributes
        mock_task = MagicMock()
        del mock_task.id
        del mock_task.description
        
        mock_agent = MagicMock()
        del mock_agent.role
        
        # Should handle missing attributes gracefully
        logger.on_task_start(mock_task, mock_agent)
        
    def test_on_task_end_exception_handling(self):
        """Test task end with exception in logging"""
        logger = TaskCompletionLogger(self.job_id)
        
        mock_task = MagicMock()
        mock_task.id = "task_123"
        
        # Mock output that will cause exception when repr'd
        class BadOutput:
            def __repr__(self):
                raise Exception("Repr error")
        
        bad_output = BadOutput()
        
        # Should handle exception gracefully
        logger.on_task_end(bad_output, mock_task)


class TestDetailedOutputLogger(unittest.TestCase):
    """Unit tests for DetailedOutputLogger"""

    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_789"
        
    def test_init(self):
        """Test DetailedOutputLogger initialization"""
        logger = DetailedOutputLogger(self.job_id)
        self.assertEqual(logger.job_id, self.job_id)
        
    def test_init_without_job_id(self):
        """Test DetailedOutputLogger initialization without job_id"""
        logger = DetailedOutputLogger()
        self.assertIsNone(logger.job_id)

    def test_on_agent_step(self):
        """Test agent step logging"""
        logger = DetailedOutputLogger(self.job_id)
        
        # Mock agent output, agent, and task
        mock_output = MagicMock()
        mock_output.output = "Detailed output content"
        
        mock_agent = MagicMock()
        mock_task = MagicMock()
        mock_task.id = "task_123"
        
        # Should return the original output
        result = logger.on_agent_step(mock_output, mock_agent, mock_task)
        self.assertEqual(result, mock_output)

    def test_setup_listeners(self):
        """Test event listener setup"""
        logger = DetailedOutputLogger(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        logger.setup_listeners(mock_event_bus)
        
        # Verify handler was registered
        self.assertIn(AgentExecutionCompletedEvent, handlers)
        
        # Test event handler
        mock_event = MagicMock()
        mock_event.agent = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.id = "test_task"
        mock_event.output = MagicMock()
        mock_event.output.output = "Test output"
        
        # Should not raise exception
        handlers[AgentExecutionCompletedEvent]("source", mock_event)
        
    def test_setup_listeners_exception_handling(self):
        """Test exception handling in setup listeners"""
        logger = DetailedOutputLogger(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        logger.setup_listeners(mock_event_bus)
        
        # Test with malformed event
        mock_event = MagicMock()
        mock_event.agent = None
        mock_event.task = None
        mock_event.output = None
        
        # Should not raise exception
        handlers[AgentExecutionCompletedEvent]("source", mock_event)
        
    def test_on_agent_step_exception_handling(self):
        """Test on_agent_step with exception in analysis"""
        logger = DetailedOutputLogger(self.job_id)
        
        # Mock objects that will cause exceptions during analysis
        mock_output = MagicMock()
        mock_output.output = MagicMock()
        mock_output.output.__class__ = type('BadType', (), {'__name__': 'BadType'})
        
        mock_agent = MagicMock()
        mock_task = MagicMock()
        mock_task.id = "task_123"
        
        # Should return the original output even if analysis fails
        result = logger.on_agent_step(mock_output, mock_agent, mock_task)
        self.assertEqual(result, mock_output)
        
    def test_on_agent_step_without_output_attribute(self):
        """Test on_agent_step when agent_output doesn't have output attribute"""
        logger = DetailedOutputLogger(self.job_id)
        
        # Mock output without output attribute
        mock_output = "Direct string output"
        
        mock_agent = MagicMock()
        mock_task = MagicMock()
        mock_task.id = "task_123"
        
        # Should return the original output
        result = logger.on_agent_step(mock_output, mock_agent, mock_task)
        self.assertEqual(result, mock_output)


class TestCrewLoggerHandler(unittest.TestCase):
    """Unit tests for CrewLoggerHandler"""

    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_999"
        self.group_context = MagicMock()
        
    def test_init(self):
        """Test CrewLoggerHandler initialization"""
        handler = CrewLoggerHandler(self.job_id, self.group_context)
        
        self.assertEqual(handler.job_id, self.job_id)
        self.assertEqual(handler.group_context, self.group_context)

    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    def test_emit_success(self, mock_enqueue_log):
        """Test successful log emission"""
        handler = CrewLoggerHandler(self.job_id, self.group_context)
        
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
        
        # Verify enqueue_log was called
        mock_enqueue_log.assert_called_once_with(
            execution_id=self.job_id,
            content="Test log message",
            group_context=self.group_context
        )

    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    @patch('sys.stderr')
    def test_emit_failure(self, mock_stderr, mock_enqueue_log):
        """Test log emission with error"""
        mock_enqueue_log.side_effect = Exception("Enqueue error")
        mock_stderr.closed = False
        
        handler = CrewLoggerHandler(self.job_id, self.group_context)
        
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
        
        # Should not raise exception
        handler.emit(record)
        
        # Verify error was written to stderr
        mock_stderr.write.assert_called()
        mock_stderr.flush.assert_called()

    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    def test_emit_with_formatter(self, mock_enqueue_log):
        """Test log emission with custom formatter"""
        handler = CrewLoggerHandler(self.job_id, self.group_context)
        
        # Set a formatter
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
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
        
        # Verify formatted message was enqueued
        mock_enqueue_log.assert_called_once_with(
            execution_id=self.job_id,
            content="INFO - Test log message",
            group_context=self.group_context
        )
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    @patch('sys.stderr')
    def test_emit_stderr_closed(self, mock_stderr, mock_enqueue_log):
        """Test log emission when stderr is closed"""
        mock_enqueue_log.side_effect = Exception("Enqueue error")
        mock_stderr.closed = True
        
        handler = CrewLoggerHandler(self.job_id, self.group_context)
        
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
        
        # Should not raise exception even if stderr is closed
        handler.emit(record)
        
        # stderr.write should not be called if closed
        mock_stderr.write.assert_not_called()
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    @patch('sys.stderr')
    def test_emit_stderr_exception(self, mock_stderr, mock_enqueue_log):
        """Test log emission when stderr write fails"""
        mock_enqueue_log.side_effect = Exception("Enqueue error")
        mock_stderr.closed = False
        mock_stderr.write.side_effect = Exception("Write error")
        
        handler = CrewLoggerHandler(self.job_id, self.group_context)
        
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
        
        # Should not raise exception even if stderr write fails
        handler.emit(record)


class TestEventHandlerExceptions(unittest.TestCase):
    """Test exception handling in event handlers"""

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_event_handler_exception_handling(self, mock_extract_agent, mock_get_queue):
        """Test that exceptions in event handlers are caught and logged"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.side_effect = Exception("Extract error")
        
        listener = AgentTraceEventListener("test_job")
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        
        # Trigger handler - should not raise exception
        handlers[AgentExecutionCompletedEvent]("source", mock_event)
        
        # Queue should not have been called due to early exception
        mock_queue.put_nowait.assert_not_called()
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_all_event_handlers_exception_coverage(self, mock_extract_agent, mock_get_queue):
        """Test exception handling for all event handlers"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.side_effect = Exception("Extract error")
        
        listener = AgentTraceEventListener("test_job")
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        
        # Test event handlers that use extract_agent_name_from_event (these should fail silently)
        event_types_with_agent_extraction = [
            AgentExecutionCompletedEvent,
            ToolUsageFinishedEvent,
            LLMCallCompletedEvent,
            TaskCompletedEvent,
            TaskStartedEvent
        ]
        
        for event_type in event_types_with_agent_extraction:
            # Should not raise exception but also should not enqueue due to early exception
            handlers[event_type]("source", mock_event)
        
        # Test crew events that don't use extract_agent_name_from_event (these should succeed)
        crew_event_types = [
            CrewKickoffStartedEvent,
            CrewKickoffCompletedEvent
        ]
        
        for event_type in crew_event_types:
            # Should not raise exception and should successfully enqueue
            handlers[event_type]("source", mock_event)
        
        # Queue should have been called exactly twice (for the two crew events)
        self.assertEqual(mock_queue.put_nowait.call_count, 2)


class TestAdditionalCoverage(unittest.TestCase):
    """Additional tests for edge cases and full coverage"""
    
    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_coverage"
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_crew_kickoff_with_none_crew_name(self, mock_get_queue):
        """Test crew kickoff events with None crew name"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Test crew kickoff started with None crew_name
        mock_event = MagicMock()
        mock_event.crew_name = None
        
        handlers[CrewKickoffStartedEvent]("source", mock_event)
        
        # Verify trace was enqueued with default crew name
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        self.assertEqual(trace_data["event_context"], "crew")
        
        # Reset mock for next test
        mock_queue.reset_mock()
        
        # Test crew kickoff completed with None crew_name
        mock_event.output = "Completed output"
        handlers[CrewKickoffCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued with default crew name
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        self.assertEqual(trace_data["event_context"], "crew")
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.SessionLocal')
    def test_get_or_create_task_id_short_task_name(self, mock_session, mock_get_queue):
        """Test task ID creation with short task name (<=20 chars)"""
        mock_get_queue.return_value = MagicMock()
        listener = AgentTraceEventListener(self.job_id)
        
        # Mock database session returning no task
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        with patch('src.engines.crewai.callbacks.logging_callbacks.uuid.uuid4') as mock_uuid:
            mock_uuid.return_value = "short_name_uuid"
            
            # Short task name should not try description matching
            task_id = listener._get_or_create_task_id("Short", "original_short")
            
            self.assertEqual(task_id, "short_name_uuid")
            # Should not call all() for description matching
            mock_db.query.return_value.all.assert_not_called()
            
    def test_task_completion_logger_init_without_job_id(self):
        """Test TaskCompletionLogger initialization without job_id"""
        logger = TaskCompletionLogger()
        self.assertIsNone(logger.job_id)
        
    def test_detailed_output_logger_init_without_job_id(self):
        """Test DetailedOutputLogger initialization without job_id"""
        logger = DetailedOutputLogger()
        self.assertIsNone(logger.job_id)
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_enqueue_trace_qsize_debug(self, mock_get_queue):
        """Test enqueue trace with queue size debugging"""
        mock_queue = MagicMock()
        mock_queue.qsize.return_value = 10
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)
        
        listener._enqueue_trace(
            event_source="TestAgent",
            event_context="Test Task",
            event_type="debug_test",
            output_content="Debug output"
        )
        
        # Verify qsize was called for debugging
        mock_queue.qsize.assert_called()
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_init_logging_path_coverage(self, mock_get_queue):
        """Test initialization logging path coverage"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        # Test that init logging happens only once per job_id
        listener1 = AgentTraceEventListener(self.job_id)
        listener2 = AgentTraceEventListener(self.job_id)
        
        # Verify the job_id was added to init_logged
        self.assertIn(self.job_id, AgentTraceEventListener._init_logged)
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_init_with_existing_task_registry(self, mock_get_queue):
        """Test initialization when task registry already exists"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        # Pre-populate task registry
        AgentTraceEventListener._task_registry[self.job_id] = {"existing": "task"}
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Verify existing registry is preserved
        self.assertEqual(AgentTraceEventListener._task_registry[self.job_id], {"existing": "task"})
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.SessionLocal')
    def test_get_or_create_task_id_all_paths(self, mock_session, mock_get_queue):
        """Test all code paths in _get_or_create_task_id"""
        mock_get_queue.return_value = MagicMock()
        listener = AgentTraceEventListener(self.job_id)
        
        # Mock database session
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db
        
        # Test path 1: Task found by name
        mock_task = MagicMock()
        mock_task.id = "found_by_name"
        mock_db.query.return_value.filter.return_value.first.return_value = mock_task
        
        task_id = listener._get_or_create_task_id("Found Task", "original_id")
        self.assertEqual(task_id, "found_by_name")
        
        # Test path 2: Task not found by name but found by description
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        mock_task_by_desc = MagicMock()
        mock_task_by_desc.id = "found_by_description"
        mock_task_by_desc.description = "This is a long task description that contains the search term"
        mock_db.query.return_value.all.return_value = [mock_task_by_desc]
        
        task_id = listener._get_or_create_task_id("long task description", "original_id2")
        self.assertEqual(task_id, "found_by_description")
        
        # Test path 3: No task found, create new UUID
        mock_db.query.return_value.all.return_value = []
        
        with patch('src.engines.crewai.callbacks.logging_callbacks.uuid.uuid4') as mock_uuid:
            mock_uuid.return_value = "new_uuid_created"
            task_id = listener._get_or_create_task_id("Not Found Task", "original_id3")
            self.assertEqual(task_id, "new_uuid_created")
            
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_enqueue_trace_full_coverage(self, mock_get_queue):
        """Test _enqueue_trace method with all parameters"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        group_context = MagicMock()
        group_context.primary_group_id = "group_123"
        group_context.group_email = "test@example.com"
        
        listener = AgentTraceEventListener(self.job_id, group_context)
        
        # Test with all parameters
        listener._enqueue_trace(
            event_source="test_source",
            event_context="test_context",
            event_type="test_type",
            output_content="test_output",
            extra_data={"key": "value"}
        )
        
        # Verify the trace data structure
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        self.assertEqual(trace_data["job_id"], self.job_id)
        self.assertEqual(trace_data["event_source"], "test_source")
        self.assertEqual(trace_data["event_context"], "test_context")
        self.assertEqual(trace_data["event_type"], "test_type")
        self.assertEqual(trace_data["output_content"], "test_output")
        self.assertEqual(trace_data["extra_data"], {"key": "value"})
        self.assertEqual(trace_data["group_id"], "group_123")
        self.assertEqual(trace_data["group_email"], "test@example.com")
        self.assertIn("timestamp", trace_data)
        self.assertIn("time_since_init", trace_data)
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_event_handlers_no_task_coverage(self, mock_extract_agent, mock_get_queue):
        """Test event handlers when task information is missing"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Test agent execution event without task
        class MockEventNoTask:
            def __init__(self):
                self.output = "test output"
                
            def __getattr__(self, name):
                if name in ['task', 'context']:
                    raise AttributeError(f"No attribute {name}")
                return super().__getattribute__(name)
        
        mock_event = MockEventNoTask()
        handlers[AgentExecutionCompletedEvent]("source", mock_event)
        
        # Verify that the exception was caught and no trace was enqueued
        # (due to the exception in the event handler being caught by try-catch)
        mock_queue.put_nowait.assert_not_called()
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_llm_call_without_call_type(self, mock_extract_agent, mock_get_queue):
        """Test LLM call event without call_type attribute"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Test LLM call event without call_type
        class MockLLMEvent:
            def __init__(self):
                self.output = "LLM response"
                
            def __getattr__(self, name):
                if name in ['call_type', 'task', 'context']:
                    raise AttributeError(f"No attribute {name}")
                return super().__getattribute__(name)
        
        mock_event = MockLLMEvent()
        handlers[LLMCallCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued with default context
        mock_queue.put_nowait.assert_called()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        self.assertEqual(trace_data["event_context"], "call")
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_task_completion_logger_event_edge_cases(self, mock_get_queue):
        """Test TaskCompletionLogger with edge case events"""
        mock_get_queue.return_value = MagicMock()
        logger = TaskCompletionLogger(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        logger.setup_listeners(mock_event_bus)
        
        # Test with event that has context but no direct task/agent
        class MockEventWithContext:
            def __init__(self):
                self.context = MagicMock()
                self.context.task = MagicMock()
                self.context.task.id = "context_task_id"
                self.context.task.description = "Context task"
                self.context.agent = MagicMock()
                self.context.agent.role = "Context agent"
                self.output = "Task output"
                
            def __getattr__(self, name):
                if name in ['task', 'agent']:
                    raise AttributeError(f"No attribute {name}")
                return super().__getattribute__(name)
        
        mock_event = MockEventWithContext()
        
        # Should handle both task started and completed events
        handlers[TaskStartedEvent]("source", mock_event)
        handlers[TaskCompletedEvent]("source", mock_event)
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_detailed_output_logger_edge_cases(self, mock_get_queue):
        """Test DetailedOutputLogger with edge case events"""
        mock_get_queue.return_value = MagicMock()
        logger = DetailedOutputLogger(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        logger.setup_listeners(mock_event_bus)
        
        # Test with minimal event data
        mock_event = MagicMock()
        mock_event.agent = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.id = "test_task"
        mock_event.output = "simple output"
        
        # Should handle agent execution completed event
        handlers[AgentExecutionCompletedEvent]("source", mock_event)
    
    def test_imports_and_module_level_coverage(self):
        """Test imports and module-level declarations"""
        # Test that all classes are importable and module level constants exist
        from src.engines.crewai.callbacks.logging_callbacks import (
            AgentTraceEventListener, TaskCompletionLogger, DetailedOutputLogger, 
            CrewLoggerHandler, AgentTraceCallback
        )
        
        # Test module level constants
        self.assertTrue(hasattr(AgentTraceEventListener, '_init_logged'))
        self.assertTrue(hasattr(AgentTraceEventListener, '_task_registry'))
        
        # Test alias
        self.assertEqual(AgentTraceCallback, AgentTraceEventListener)
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.SessionLocal')
    def test_get_or_create_task_id_description_no_match(self, mock_session, mock_get_queue):
        """Test _get_or_create_task_id when description search finds no match"""
        mock_get_queue.return_value = MagicMock()
        listener = AgentTraceEventListener(self.job_id)
        
        # Mock database session
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db
        
        # Mock task not found by name
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        # Mock tasks with descriptions that don't match
        mock_task1 = MagicMock()
        mock_task1.description = "Completely different task description"
        mock_task2 = MagicMock()
        mock_task2.description = "Another unrelated description"
        mock_db.query.return_value.all.return_value = [mock_task1, mock_task2]
        
        with patch('src.engines.crewai.callbacks.logging_callbacks.uuid.uuid4') as mock_uuid:
            mock_uuid.return_value = "no_match_uuid"
            task_id = listener._get_or_create_task_id("search term not found anywhere", "original_id")
            self.assertEqual(task_id, "no_match_uuid")
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_task_started_no_such_table_error(self, mock_extract_agent, mock_get_queue):
        """Test task started event with 'no such table' error"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.description = "Test Task"
        mock_event.task.id = "task_123"
        
        # Mock task ID lookup to raise "no such table" error
        with patch.object(listener, '_get_or_create_task_id', side_effect=Exception("no such table: tasks")):
            handlers[TaskStartedEvent]("source", mock_event)
        
        # Verify trace was NOT enqueued due to early database error
        mock_queue.put_nowait.assert_not_called()
        
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_task_completed_no_such_table_error(self, mock_extract_agent, mock_get_queue):
        """Test task completed event with 'no such table' error"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.description = "Test Task"
        mock_event.task.id = "task_123"
        mock_event.output = "Task output"
        
        # Mock task ID lookup to raise "no such table" error
        with patch.object(listener, '_get_or_create_task_id', side_effect=Exception("no such table: tasks")):
            handlers[TaskCompletedEvent]("source", mock_event)
        
        # Verify trace was NOT enqueued due to early database error
        mock_queue.put_nowait.assert_not_called()
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_crew_events_with_output_none(self, mock_extract_agent, mock_get_queue):
        """Test crew events when output is None"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Test crew kickoff completed with None output
        mock_event = MagicMock()
        mock_event.crew_name = "TestCrew"
        mock_event.output = None
        
        handlers[CrewKickoffCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued with "None" as output content
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        self.assertEqual(trace_data["output_content"], "None")
    
    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    def test_crew_logger_handler_with_formatter(self, mock_enqueue_log):
        """Test CrewLoggerHandler emit method with custom formatter"""
        group_context = MagicMock()
        handler = CrewLoggerHandler("test_job", group_context)
        
        # Set custom formatter
        formatter = logging.Formatter('[%(levelname)s] %(message)s')
        handler.setFormatter(formatter)
        
        # Create log record
        record = logging.LogRecord(
            name="test_logger",
            level=logging.WARNING,
            pathname="test.py",
            lineno=42,
            msg="Warning message",
            args=(),
            exc_info=None
        )
        
        handler.emit(record)
        
        # Verify formatted message was enqueued
        mock_enqueue_log.assert_called_once_with(
            execution_id="test_job",
            content="[WARNING] Warning message",
            group_context=group_context
        )


class TestMissingCoverageLines(unittest.TestCase):
    """Tests specifically targeting uncovered lines for 100% coverage"""

    def setUp(self):
        """Set up test environment"""
        self.job_id = "test_job_missing_coverage"
        AgentTraceEventListener._init_logged.clear()
        AgentTraceEventListener._task_registry.clear()

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_llm_call_with_only_context_task(self, mock_extract_agent, mock_get_queue):
        """Test LLM call event with only context.task (covers line 238)"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event with only context.task (no event.task) - covers line 238
        class MockLLMEventContextOnly:
            def __init__(self):
                self.output = "LLM response"
                self.context = MagicMock()
                self.context.task = MagicMock()
                self.context.task.description = "Context LLM Task"
                
            def __getattr__(self, name):
                if name == 'task':
                    raise AttributeError(f"'{type(self).__name__}' object has no attribute 'task'")
                return super().__getattribute__(name)
        
        mock_event = MockLLMEventContextOnly()
        
        # Trigger the handler
        handlers[LLMCallCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_task_completed_with_only_context_task(self, mock_extract_agent, mock_get_queue):
        """Test task completed event with only context.task (covers lines 286-287)"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event with only context.task (no event.task) - covers lines 286-287
        class MockTaskEventContextOnly:
            def __init__(self):
                self.output = "Task output"
                self.context = MagicMock()
                self.context.task = MagicMock()
                self.context.task.description = "Context Task"
                self.context.task.id = "context_task_id"
                
            def __getattr__(self, name):
                if name == 'task':
                    raise AttributeError(f"'{type(self).__name__}' object has no attribute 'task'")
                return super().__getattribute__(name)
        
        mock_event = MockTaskEventContextOnly()
        
        # Mock the task ID lookup to work
        with patch.object(listener, '_get_or_create_task_id', return_value="task_id_123"):
            # Trigger the handler
            handlers[TaskCompletedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_task_started_with_only_context_task(self, mock_extract_agent, mock_get_queue):
        """Test task started event with only context.task (covers lines 366-371)"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event with only context.task (no event.task) - covers lines 366-371
        class MockTaskStartedEventContextOnly:
            def __init__(self):
                self.context = MagicMock()
                self.context.task = MagicMock()
                self.context.task.description = "Started Context Task"
                self.context.task.id = "started_context_task_id"
                
            def __getattr__(self, name):
                if name == 'task':
                    raise AttributeError(f"'{type(self).__name__}' object has no attribute 'task'")
                return super().__getattribute__(name)
        
        mock_event = MockTaskStartedEventContextOnly()
        
        # Mock the task ID lookup to work
        with patch.object(listener, '_get_or_create_task_id', return_value="task_id_456"):
            # Trigger the handler
            handlers[TaskStartedEvent]("source", mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_task_completion_logger_init_optional_job_id(self, mock_get_queue):
        """Test TaskCompletionLogger.__init__ without job_id parameter (covers lines 479-480)"""
        mock_get_queue.return_value = MagicMock()
        
        # Test the path where job_id is not provided 
        logger = TaskCompletionLogger()
        self.assertIsNone(logger.job_id)

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_detailed_output_logger_init_optional_job_id(self, mock_get_queue):
        """Test DetailedOutputLogger.__init__ without job_id parameter (covers lines 489-490)"""
        mock_get_queue.return_value = MagicMock()
        
        # Test the path where job_id is not provided
        logger = DetailedOutputLogger()
        self.assertIsNone(logger.job_id)

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_crew_logger_handler_no_group_context(self, mock_get_queue):
        """Test CrewLoggerHandler without group_context (covers lines 538-539)"""
        mock_get_queue.return_value = MagicMock()
        
        # Test initialization without group_context
        handler = CrewLoggerHandler("test_job")
        self.assertEqual(handler.job_id, "test_job")
        self.assertIsNone(handler.group_context)

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.enqueue_log')
    def test_crew_logger_handler_emit_no_formatter(self, mock_enqueue_log, mock_get_queue):
        """Test CrewLoggerHandler emit without formatter (covers lines 552-553)"""
        mock_get_queue.return_value = MagicMock()
        
        handler = CrewLoggerHandler("test_job")
        
        # Create log record without setting formatter
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        handler.emit(record)
        
        # Verify raw message was enqueued (no formatting applied)
        mock_enqueue_log.assert_called_once_with(
            execution_id="test_job",
            content="Test message",
            group_context=None
        )

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_task_completed_database_error_handling(self, mock_extract_agent, mock_get_queue):
        """Test task completed with logging error handling (covers lines 308-313)"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.description = "Test Task"
        mock_event.task.id = "task_123"
        mock_event.output = "Task output"
        
        # Mock successful task ID creation and enqueue_trace to succeed
        with patch.object(listener, '_get_or_create_task_id', return_value="task_id_123"):
            with patch.object(listener, '_enqueue_trace'):
                with patch('src.engines.crewai.callbacks.logging_callbacks.logger') as mock_logger:
                    # Make the logger.info call in the try block raise "no such table" error
                    mock_logger.info.side_effect = Exception("no such table: tasks")
                    mock_logger.error = MagicMock()  # For the outer exception handler
                    
                    # Trigger the handler
                    handlers[TaskCompletedEvent]("source", mock_event)
                    
                    # Verify the outer error handler was called
                    mock_logger.error.assert_called_with(f"[AgentTraceEventListener][{self.job_id}] Error in on_task_completed: no such table: tasks", exc_info=True)

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_crew_kickoff_started_error_handling(self, mock_get_queue):
        """Test crew kickoff started with error handling (covers lines 335-336)"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event that will cause an error
        mock_event = MagicMock()
        mock_event.crew_name = "TestCrew"
        
        # Mock _enqueue_trace to raise an exception
        with patch.object(listener, '_enqueue_trace', side_effect=Exception("Enqueue error")):
            with patch('src.engines.crewai.callbacks.logging_callbacks.logger') as mock_logger:
                mock_logger.error = MagicMock()
                
                # Trigger the handler - should catch and log error
                handlers[CrewKickoffStartedEvent]("source", mock_event)
                
                # Verify error was logged
                mock_logger.error.assert_called()

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    def test_crew_kickoff_completed_error_handling(self, mock_get_queue):
        """Test crew kickoff completed with error handling (covers lines 353-354)"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event that will cause an error
        mock_event = MagicMock()
        mock_event.crew_name = "TestCrew"
        mock_event.output = "Crew output"
        
        # Mock _enqueue_trace to raise an exception
        with patch.object(listener, '_enqueue_trace', side_effect=Exception("Enqueue error")):
            with patch('src.engines.crewai.callbacks.logging_callbacks.logger') as mock_logger:
                mock_logger.error = MagicMock()
                
                # Trigger the handler - should catch and log error
                handlers[CrewKickoffCompletedEvent]("source", mock_event)
                
                # Verify error was logged
                mock_logger.error.assert_called()

    @patch('src.engines.crewai.callbacks.logging_callbacks.get_trace_queue')
    @patch('src.engines.crewai.callbacks.logging_callbacks.extract_agent_name_from_event')
    def test_task_started_database_error_handling(self, mock_extract_agent, mock_get_queue):
        """Test task started with logging error handling (covers lines 390-395)"""
        mock_queue = MagicMock()
        mock_get_queue.return_value = mock_queue
        mock_extract_agent.return_value = "TestAgent"
        
        listener = AgentTraceEventListener(self.job_id)
        
        # Create mock event bus
        mock_event_bus = MagicMock()
        handlers = {}
        
        def on_decorator(event_type):
            def decorator(func):
                handlers[event_type] = func
                return func
            return decorator
        
        mock_event_bus.on = on_decorator
        
        # Setup listeners
        listener.setup_listeners(mock_event_bus)
        
        # Create mock event
        mock_event = MagicMock()
        mock_event.task = MagicMock()
        mock_event.task.description = "Test Task"
        mock_event.task.id = "task_123"
        
        # Mock successful task ID creation and enqueue_trace to succeed
        with patch.object(listener, '_get_or_create_task_id', return_value="task_id_123"):
            with patch.object(listener, '_enqueue_trace'):
                with patch('src.engines.crewai.callbacks.logging_callbacks.logger') as mock_logger:
                    # Make the logger.info call in the try block raise "no such table" error
                    mock_logger.info.side_effect = Exception("no such table: tasks")
                    mock_logger.error = MagicMock()  # For the outer exception handler
                    
                    # Trigger the handler
                    handlers[TaskStartedEvent]("source", mock_event)
                    
                    # Verify the outer error handler was called
                    mock_logger.error.assert_called_with(f"[AgentTraceEventListener][{self.job_id}] Error in on_task_started: no such table: tasks", exc_info=True)



if __name__ == '__main__':
    unittest.main()