"""
Unit tests for LLM Event Router module.

Tests the singleton router that captures CrewAI LLM events and routes them
to appropriate executions based on agent context.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timezone
import threading

from src.engines.crewai.callbacks.llm_event_router import (
    LLMEventRouter,
    register_execution_for_llm_events,
    unregister_execution_from_llm_events
)
from src.utils.user_context import GroupContext


class TestLLMEventRouter:
    """Test suite for LLMEventRouter."""
    
    def setup_method(self):
        """Reset singleton state before each test."""
        LLMEventRouter._instance = None
        LLMEventRouter._initialized = False
        LLMEventRouter._active_executions = {}
    
    def test_singleton_pattern(self):
        """Test that LLMEventRouter follows singleton pattern."""
        router1 = LLMEventRouter()
        router2 = LLMEventRouter()
        assert router1 is router2
    
    def test_singleton_thread_safety(self):
        """Test singleton creation is thread-safe."""
        routers = []
        
        def create_router():
            routers.append(LLMEventRouter())
        
        threads = [threading.Thread(target=create_router) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All should be the same instance
        assert all(r is routers[0] for r in routers)
    
    @patch('src.engines.crewai.callbacks.llm_event_router.get_trace_queue')
    def test_register_execution(self, mock_get_trace_queue):
        """Test registering an execution with agents."""
        mock_queue = Mock()
        mock_get_trace_queue.return_value = mock_queue
        
        # Create mock crew with agents
        mock_crew = Mock()
        mock_agent1 = Mock(role="researcher")
        mock_agent2 = Mock(role="writer")
        mock_crew.agents = [mock_agent1, mock_agent2]
        
        # Create group context
        group_context = GroupContext(
            group_ids=["group123"],
            group_email="test@example.com",
            email_domain="example.com"
        )
        
        # Register execution
        LLMEventRouter.register_execution("exec123", mock_crew, group_context)
        
        # Verify execution was registered
        assert "exec123" in LLMEventRouter._active_executions
        exec_data = LLMEventRouter._active_executions["exec123"]
        assert exec_data['agents'] == {"researcher", "writer"}
        assert exec_data['group_context'] == group_context
        assert exec_data['trace_queue'] == mock_queue
    
    @patch('src.engines.crewai.callbacks.llm_event_router.get_trace_queue')
    def test_register_execution_without_agents(self, mock_get_trace_queue):
        """Test registering an execution with no agents."""
        mock_queue = Mock()
        mock_get_trace_queue.return_value = mock_queue
        
        # Register with None crew
        LLMEventRouter.register_execution("exec124", None, None)
        
        # Verify execution was registered with empty agents
        assert "exec124" in LLMEventRouter._active_executions
        exec_data = LLMEventRouter._active_executions["exec124"]
        assert exec_data['agents'] == set()
        assert exec_data['group_context'] is None
    
    @patch('src.engines.crewai.callbacks.llm_event_router.get_trace_queue')
    def test_register_execution_with_invalid_crew(self, mock_get_trace_queue):
        """Test registering an execution with crew without agents attribute."""
        mock_queue = Mock()
        mock_get_trace_queue.return_value = mock_queue
        
        mock_crew = Mock(spec=[])  # No agents attribute
        
        LLMEventRouter.register_execution("exec125", mock_crew, None)
        
        assert "exec125" in LLMEventRouter._active_executions
        exec_data = LLMEventRouter._active_executions["exec125"]
        assert exec_data['agents'] == set()
    
    @patch('src.engines.crewai.callbacks.llm_event_router.get_trace_queue')
    @patch.object(LLMEventRouter, '_setup_global_handler')
    def test_register_execution_initializes_handler_once(self, mock_setup, mock_get_trace_queue):
        """Test that global handler is set up only once."""
        mock_get_trace_queue.return_value = Mock()
        
        mock_crew = Mock()
        mock_crew.agents = []
        
        # Register multiple executions
        LLMEventRouter.register_execution("exec1", mock_crew, None)
        LLMEventRouter.register_execution("exec2", mock_crew, None)
        LLMEventRouter.register_execution("exec3", mock_crew, None)
        
        # Global handler should be set up only once
        mock_setup.assert_called_once()
        assert LLMEventRouter._initialized is True
    
    def test_unregister_execution(self):
        """Test unregistering an execution."""
        # Manually add an execution
        LLMEventRouter._active_executions["exec126"] = {
            'agents': {"agent1", "agent2"},
            'group_context': None,
            'trace_queue': Mock()
        }
        
        # Unregister it
        LLMEventRouter.unregister_execution("exec126")
        
        # Verify it was removed
        assert "exec126" not in LLMEventRouter._active_executions
    
    def test_unregister_nonexistent_execution(self):
        """Test unregistering an execution that doesn't exist."""
        # Should not raise an error
        LLMEventRouter.unregister_execution("nonexistent")
        assert "nonexistent" not in LLMEventRouter._active_executions
    
    def test_get_active_execution_count(self):
        """Test getting the count of active executions."""
        # Add some executions
        LLMEventRouter._active_executions = {
            "exec1": {'agents': set(), 'group_context': None, 'trace_queue': Mock()},
            "exec2": {'agents': set(), 'group_context': None, 'trace_queue': Mock()},
            "exec3": {'agents': set(), 'group_context': None, 'trace_queue': Mock()}
        }
        
        assert LLMEventRouter.get_active_execution_count() == 3
    
    def test_get_active_agents(self):
        """Test getting all active agent roles."""
        # Add executions with different agents
        LLMEventRouter._active_executions = {
            "exec1": {'agents': {"agent1", "agent2"}, 'group_context': None, 'trace_queue': Mock()},
            "exec2": {'agents': {"agent2", "agent3"}, 'group_context': None, 'trace_queue': Mock()},
            "exec3": {'agents': {"agent4"}, 'group_context': None, 'trace_queue': Mock()}
        }
        
        active_agents = LLMEventRouter.get_active_agents()
        assert active_agents == {"agent1", "agent2", "agent3", "agent4"}
    
    @patch('src.engines.crewai.callbacks.llm_event_router.crewai_event_bus')
    def test_setup_global_handler(self, mock_event_bus):
        """Test setting up the global event handler."""
        router = LLMEventRouter()
        router._setup_global_handler()
        
        # Verify event handler was registered
        mock_event_bus.on.assert_called_once()
    
    @patch('src.engines.crewai.callbacks.llm_event_router.datetime')
    @patch('src.engines.crewai.callbacks.llm_event_router.crewai_event_bus')
    def test_llm_event_handling(self, mock_event_bus, mock_datetime):
        """Test handling of LLM events."""
        # Setup
        mock_now = Mock()
        mock_now.isoformat.return_value = "2024-01-01T00:00:00Z"
        mock_datetime.now.return_value = mock_now
        
        mock_queue = Mock()
        
        # Add an execution
        LLMEventRouter._active_executions["exec127"] = {
            'agents': {"researcher"},
            'group_context': GroupContext(
                group_ids=["group123"],
                group_email="test@example.com",
                email_domain="example.com"
            ),
            'trace_queue': mock_queue
        }
        
        # Setup event handler capture
        handler_func = None
        def capture_handler(event_class):
            def decorator(func):
                nonlocal handler_func
                handler_func = func
                return func
            return decorator
        
        mock_event_bus.on = capture_handler
        
        # Setup global handler
        router = LLMEventRouter()
        router._setup_global_handler()
        
        # Create mock LLM event
        mock_event = Mock()
        mock_event.agent_role = "researcher"
        mock_event.response = "LLM response text"
        mock_event.model = "gpt-4"
        mock_event.call_type = Mock(value="completion")
        
        # Call the handler
        if handler_func:
            handler_func(None, mock_event)
        
        # Verify trace was enqueued
        mock_queue.put_nowait.assert_called_once()
        trace_data = mock_queue.put_nowait.call_args[0][0]
        
        assert trace_data['job_id'] == "exec127"
        assert trace_data['event_source'] == "researcher"
        assert trace_data['event_type'] == "llm_call"
        assert trace_data['output_content'] == "LLM response text"
        assert trace_data['group_id'] == "group123"
        assert trace_data['group_email'] == "test@example.com"
        assert trace_data['extra_data']['agent_role'] == "researcher"
        assert trace_data['extra_data']['model'] == "gpt-4"
    
    @patch('src.engines.crewai.callbacks.llm_event_router.crewai_event_bus')
    def test_llm_event_handling_no_agent_role(self, mock_event_bus):
        """Test that events without agent_role are skipped."""
        mock_queue = Mock()
        
        # Add an execution
        LLMEventRouter._active_executions["exec128"] = {
            'agents': {"researcher"},
            'group_context': None,
            'trace_queue': mock_queue
        }
        
        # Setup event handler capture
        handler_func = None
        def capture_handler(event_class):
            def decorator(func):
                nonlocal handler_func
                handler_func = func
                return func
            return decorator
        
        mock_event_bus.on = capture_handler
        
        # Setup global handler
        router = LLMEventRouter()
        router._setup_global_handler()
        
        # Create mock LLM event without agent_role
        mock_event = Mock(spec=[])  # No agent_role attribute
        
        # Call the handler
        if handler_func:
            handler_func(None, mock_event)
        
        # Verify trace was NOT enqueued
        mock_queue.put_nowait.assert_not_called()
    
    @patch('src.engines.crewai.callbacks.llm_event_router.logger')
    @patch('src.engines.crewai.callbacks.llm_event_router.crewai_event_bus')
    def test_llm_event_handling_error(self, mock_event_bus, mock_logger):
        """Test error handling in LLM event handler."""
        mock_queue = Mock()
        mock_queue.put_nowait.side_effect = Exception("Queue error")
        
        # Add an execution
        LLMEventRouter._active_executions["exec129"] = {
            'agents': {"researcher"},
            'group_context': None,
            'trace_queue': mock_queue
        }
        
        # Setup event handler capture
        handler_func = None
        def capture_handler(event_class):
            def decorator(func):
                nonlocal handler_func
                handler_func = func
                return func
            return decorator
        
        mock_event_bus.on = capture_handler
        
        # Setup global handler
        router = LLMEventRouter()
        router._setup_global_handler()
        
        # Create mock LLM event
        mock_event = Mock()
        mock_event.agent_role = "researcher"
        
        # Call the handler
        if handler_func:
            handler_func(None, mock_event)
        
        # Verify error was logged
        mock_logger.error.assert_called()


class TestConvenienceFunctions:
    """Test suite for convenience functions."""
    
    def setup_method(self):
        """Reset singleton state before each test."""
        LLMEventRouter._instance = None
        LLMEventRouter._initialized = False
        LLMEventRouter._active_executions = {}
    
    @patch.object(LLMEventRouter, 'register_execution')
    def test_register_execution_for_llm_events(self, mock_register):
        """Test the convenience function for registering executions."""
        mock_crew = Mock()
        group_context = Mock()
        
        register_execution_for_llm_events("exec130", mock_crew, group_context)
        
        mock_register.assert_called_once_with("exec130", mock_crew, group_context)
    
    @patch.object(LLMEventRouter, 'unregister_execution')
    def test_unregister_execution_from_llm_events(self, mock_unregister):
        """Test the convenience function for unregistering executions."""
        unregister_execution_from_llm_events("exec131")
        
        mock_unregister.assert_called_once_with("exec131")