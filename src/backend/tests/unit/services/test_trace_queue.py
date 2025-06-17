"""
Unit tests for TraceQueue.

Tests the functionality of the singleton trace queue including
singleton behavior, queue operations, and utility functions.
"""
import pytest
import queue
from unittest.mock import patch, MagicMock

from src.services.trace_queue import TraceQueue, get_trace_queue


class TestTraceQueue:
    """Test cases for TraceQueue."""
    
    def teardown_method(self):
        """Reset singleton state after each test."""
        TraceQueue._instance = None
        TraceQueue._queue = None
    
    def test_trace_queue_singleton_behavior(self):
        """Test that TraceQueue implements singleton pattern correctly."""
        # First instance
        queue1 = TraceQueue()
        
        # Second instance should be the same
        queue2 = TraceQueue()
        
        assert queue1 is queue2
        assert id(queue1) == id(queue2)
    
    def test_trace_queue_initialization(self):
        """Test that TraceQueue is properly initialized."""
        trace_queue = TraceQueue()
        
        assert trace_queue is not None
        assert trace_queue._queue is not None
        assert isinstance(trace_queue._queue, queue.Queue)
    
    def test_get_queue_returns_queue_instance(self):
        """Test that get_queue returns the internal queue."""
        trace_queue = TraceQueue()
        result_queue = trace_queue.get_queue()
        
        assert result_queue is trace_queue._queue
        assert isinstance(result_queue, queue.Queue)
    
    def test_get_queue_consistent_across_instances(self):
        """Test that get_queue returns same queue for different instances."""
        queue1 = TraceQueue().get_queue()
        queue2 = TraceQueue().get_queue()
        
        assert queue1 is queue2
    
    def test_queue_functionality(self):
        """Test basic queue operations work correctly."""
        trace_queue = TraceQueue()
        q = trace_queue.get_queue()
        
        # Test putting and getting items
        test_item = {"trace": "test_data"}
        q.put(test_item)
        
        assert not q.empty()
        retrieved_item = q.get()
        assert retrieved_item == test_item
        assert q.empty()
    
    def test_queue_multiple_items(self):
        """Test queue handles multiple items correctly."""
        trace_queue = TraceQueue()
        q = trace_queue.get_queue()
        
        items = ["item1", "item2", "item3"]
        
        # Put multiple items
        for item in items:
            q.put(item)
        
        # Verify queue size
        assert q.qsize() == 3
        
        # Get items in FIFO order
        for expected_item in items:
            assert q.get() == expected_item
        
        assert q.empty()
    
    def test_get_trace_queue_function(self):
        """Test the utility function get_trace_queue."""
        result_queue = get_trace_queue()
        
        assert isinstance(result_queue, queue.Queue)
        
        # Should return the same queue as direct access
        trace_queue = TraceQueue()
        assert result_queue is trace_queue.get_queue()
    
    def test_get_trace_queue_function_singleton_behavior(self):
        """Test that get_trace_queue returns same queue across calls."""
        queue1 = get_trace_queue()
        queue2 = get_trace_queue()
        
        assert queue1 is queue2
    
    def test_singleton_persistence_across_calls(self):
        """Test that singleton state persists across multiple calls."""
        # Get initial instance
        first_instance = TraceQueue()
        first_queue = first_instance.get_queue()
        
        # Put some data
        test_data = {"test": "persistence"}
        first_queue.put(test_data)
        
        # Get another instance
        second_instance = TraceQueue()
        second_queue = second_instance.get_queue()
        
        # Should be same instance and queue
        assert first_instance is second_instance
        assert first_queue is second_queue
        
        # Data should persist
        assert not second_queue.empty()
        assert second_queue.get() == test_data
    
    @patch('queue.Queue')
    def test_queue_creation_called_once(self, mock_queue_class):
        """Test that queue.Queue() is called only once during singleton creation."""
        mock_queue_instance = MagicMock()
        mock_queue_class.return_value = mock_queue_instance
        
        # Create multiple instances
        TraceQueue()
        TraceQueue()
        TraceQueue()
        
        # queue.Queue() should be called only once
        mock_queue_class.assert_called_once()
    
    def test_class_attributes_initial_state(self):
        """Test that class attributes are properly initialized."""
        # Before any instance creation
        assert TraceQueue._instance is None
        assert TraceQueue._queue is None
        
        # After instance creation
        instance = TraceQueue()
        assert TraceQueue._instance is not None
        assert instance._queue is not None  # _queue is an instance attribute
    
    def test_new_method_returns_correct_instance(self):
        """Test that __new__ method returns the correct instance."""
        instance1 = TraceQueue()
        instance2 = TraceQueue()
        
        # Both should be the same instance
        assert instance1 is instance2
        assert instance1 is TraceQueue._instance
        assert instance2 is TraceQueue._instance
    
    def test_queue_thread_safety_basic(self):
        """Test basic queue thread safety features."""
        trace_queue = TraceQueue()
        q = trace_queue.get_queue()
        
        # Queue should support basic thread-safe operations
        assert hasattr(q, 'put')
        assert hasattr(q, 'get')
        assert hasattr(q, 'empty')
        assert hasattr(q, 'qsize')
        
        # Test non-blocking operations
        q.put("test", block=False)
        assert q.get(block=False) == "test"
    
    def test_queue_exception_handling(self):
        """Test queue exception handling for empty queue."""
        trace_queue = TraceQueue()
        q = trace_queue.get_queue()
        
        # Getting from empty queue with timeout should raise exception
        with pytest.raises(queue.Empty):
            q.get(block=False)
    
    def test_integration_with_utility_function(self):
        """Test integration between class and utility function."""
        # Use utility function first
        util_queue = get_trace_queue()
        util_queue.put("utility_data")
        
        # Use class directly
        class_instance = TraceQueue()
        class_queue = class_instance.get_queue()
        
        # Should access same queue
        assert class_queue.get() == "utility_data"
        
        # Put data via class
        class_queue.put("class_data")
        
        # Access via utility function
        util_queue_2 = get_trace_queue()
        assert util_queue_2.get() == "class_data"