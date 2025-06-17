"""
Unit tests for ExecutionLogsQueue.

Tests the functionality of the job output queue including
singleton behavior, log enqueueing, and group context handling.
"""
import pytest
import queue
from datetime import datetime
from unittest.mock import patch, MagicMock

from src.services.execution_logs_queue import JobOutputQueue, get_job_output_queue, enqueue_log
from src.utils.user_context import GroupContext


class TestJobOutputQueue:
    """Test cases for JobOutputQueue."""
    
    def teardown_method(self):
        """Reset singleton state after each test."""
        JobOutputQueue._instance = None
        JobOutputQueue._queue = None
    
    def test_job_output_queue_singleton_behavior(self):
        """Test that JobOutputQueue implements singleton pattern correctly."""
        # First instance
        queue1 = JobOutputQueue()
        
        # Second instance should be the same
        queue2 = JobOutputQueue()
        
        assert queue1 is queue2
        assert id(queue1) == id(queue2)
    
    def test_job_output_queue_initialization(self):
        """Test that JobOutputQueue is properly initialized."""
        job_queue = JobOutputQueue()
        
        assert job_queue is not None
        assert job_queue._queue is not None
        assert isinstance(job_queue._queue, queue.Queue)
    
    def test_get_queue_returns_queue_instance(self):
        """Test that get_queue returns the internal queue."""
        job_queue = JobOutputQueue()
        result_queue = job_queue.get_queue()
        
        assert result_queue is job_queue._queue
        assert isinstance(result_queue, queue.Queue)
    
    def test_get_queue_consistent_across_instances(self):
        """Test that get_queue returns same queue for different instances."""
        queue1 = JobOutputQueue().get_queue()
        queue2 = JobOutputQueue().get_queue()
        
        assert queue1 is queue2
    
    def test_get_job_output_queue_function(self):
        """Test the utility function get_job_output_queue."""
        result_queue = get_job_output_queue()
        
        assert isinstance(result_queue, queue.Queue)
        
        # Should return the same queue as direct access
        job_queue = JobOutputQueue()
        assert result_queue is job_queue.get_queue()
    
    def test_get_job_output_queue_function_singleton_behavior(self):
        """Test that get_job_output_queue returns same queue across calls."""
        queue1 = get_job_output_queue()
        queue2 = get_job_output_queue()
        
        assert queue1 is queue2
    
    @patch('queue.Queue')
    def test_queue_creation_called_once(self, mock_queue_class):
        """Test that queue.Queue() is called only once during singleton creation."""
        mock_queue_instance = MagicMock()
        mock_queue_class.return_value = mock_queue_instance
        
        # Create multiple instances
        JobOutputQueue()
        JobOutputQueue()
        JobOutputQueue()
        
        # queue.Queue() should be called only once
        mock_queue_class.assert_called_once()


class TestEnqueueLog:
    """Test cases for enqueue_log function."""
    
    def teardown_method(self):
        """Reset singleton state after each test."""
        JobOutputQueue._instance = None
        JobOutputQueue._queue = None
    
    def test_enqueue_log_success(self):
        """Test successful log enqueueing."""
        execution_id = "test-exec-123"
        content = "Test log message"
        
        result = enqueue_log(execution_id, content)
        
        assert result is True
        
        # Verify the log was added to queue
        job_queue = get_job_output_queue()
        assert not job_queue.empty()
        
        log_data = job_queue.get()
        assert log_data["job_id"] == execution_id
        assert log_data["content"] == content
        assert "timestamp" in log_data
        assert isinstance(log_data["timestamp"], datetime)
    
    def test_enqueue_log_with_custom_timestamp(self):
        """Test log enqueueing with custom timestamp."""
        execution_id = "test-exec-456"
        content = "Test log with timestamp"
        custom_timestamp = datetime(2023, 1, 1, 12, 0, 0)
        
        result = enqueue_log(execution_id, content, timestamp=custom_timestamp)
        
        assert result is True
        
        # Verify the custom timestamp was used
        job_queue = get_job_output_queue()
        log_data = job_queue.get()
        assert log_data["timestamp"] == custom_timestamp
    
    def test_enqueue_log_with_group_context(self):
        """Test log enqueueing with group context."""
        execution_id = "test-exec-789"
        content = "Test log with group context"
        group_context = GroupContext(
            group_ids=["group-123"],
            group_email="test@example.com",
            email_domain="example.com"
        )
        
        result = enqueue_log(execution_id, content, group_context=group_context)
        
        assert result is True
        
        # Verify group context was added
        job_queue = get_job_output_queue()
        log_data = job_queue.get()
        assert log_data["group_id"] == "group-123"
        assert log_data["group_email"] == "test@example.com"
        assert log_data["job_id"] == execution_id
        assert log_data["content"] == content
    
    def test_enqueue_log_without_group_context(self):
        """Test log enqueueing without group context."""
        execution_id = "test-exec-000"
        content = "Test log without group context"
        
        result = enqueue_log(execution_id, content)
        
        assert result is True
        
        # Verify no group context fields are present
        job_queue = get_job_output_queue()
        log_data = job_queue.get()
        assert "group_id" not in log_data
        assert "group_email" not in log_data
        assert log_data["job_id"] == execution_id
        assert log_data["content"] == content
    
    @patch('src.services.execution_logs_queue.get_job_output_queue')
    def test_enqueue_log_queue_full_exception(self, mock_get_queue):
        """Test log enqueueing when queue is full."""
        mock_queue = MagicMock()
        mock_queue.put_nowait.side_effect = queue.Full("Queue is full")
        mock_get_queue.return_value = mock_queue
        
        result = enqueue_log("test-exec", "test content")
        
        assert result is False
        mock_queue.put_nowait.assert_called_once()
    
    @patch('src.services.execution_logs_queue.get_job_output_queue')
    def test_enqueue_log_general_exception(self, mock_get_queue):
        """Test log enqueueing with general exception."""
        mock_queue = MagicMock()
        mock_queue.put_nowait.side_effect = Exception("Unexpected error")
        mock_get_queue.return_value = mock_queue
        
        result = enqueue_log("test-exec", "test content")
        
        assert result is False
        mock_queue.put_nowait.assert_called_once()
    
    def test_enqueue_log_multiple_logs(self):
        """Test enqueueing multiple logs."""
        logs = [
            ("exec-1", "Log message 1"),
            ("exec-2", "Log message 2"),
            ("exec-3", "Log message 3")
        ]
        
        # Enqueue all logs
        for execution_id, content in logs:
            result = enqueue_log(execution_id, content)
            assert result is True
        
        # Verify all logs are in queue
        job_queue = get_job_output_queue()
        assert job_queue.qsize() == 3
        
        # Verify FIFO order
        for expected_execution_id, expected_content in logs:
            log_data = job_queue.get()
            assert log_data["job_id"] == expected_execution_id
            assert log_data["content"] == expected_content
    
    @patch('src.services.execution_logs_queue.datetime')
    def test_enqueue_log_default_timestamp(self, mock_datetime):
        """Test that default timestamp uses datetime.now()."""
        mock_now = datetime(2023, 6, 15, 10, 30, 45)
        mock_datetime.now.return_value = mock_now
        
        # Keep datetime class for isinstance check
        mock_datetime.datetime = datetime
        
        result = enqueue_log("test-exec", "test content")
        
        assert result is True
        mock_datetime.now.assert_called_once()
        
        # Verify the timestamp was set correctly
        job_queue = get_job_output_queue()
        log_data = job_queue.get()
        assert log_data["timestamp"] == mock_now
    
    def test_enqueue_log_data_structure(self):
        """Test the structure of enqueued log data."""
        execution_id = "test-structure"
        content = "Structure test content"
        timestamp = datetime(2023, 5, 1, 14, 30, 0)
        group_context = GroupContext(
            group_ids=["struct-group"],
            group_email="struct@test.com",
            email_domain="test.com"
        )
        
        result = enqueue_log(execution_id, content, timestamp, group_context)
        
        assert result is True
        
        # Verify complete data structure
        job_queue = get_job_output_queue()
        log_data = job_queue.get()
        
        expected_keys = {"job_id", "content", "timestamp", "group_id", "group_email"}
        assert set(log_data.keys()) == expected_keys
        
        assert log_data["job_id"] == execution_id
        assert log_data["content"] == content
        assert log_data["timestamp"] == timestamp
        assert log_data["group_id"] == "struct-group"
        assert log_data["group_email"] == "struct@test.com"
    
    def test_enqueue_log_empty_content(self):
        """Test enqueueing log with empty content."""
        result = enqueue_log("test-exec", "")
        
        assert result is True
        
        # Empty content should still be enqueued
        job_queue = get_job_output_queue()
        log_data = job_queue.get()
        assert log_data["content"] == ""
    
    def test_enqueue_log_none_group_context(self):
        """Test enqueueing log with None group context."""
        result = enqueue_log("test-exec", "test content", group_context=None)
        
        assert result is True
        
        # Should behave same as no group context
        job_queue = get_job_output_queue()
        log_data = job_queue.get()
        assert "group_id" not in log_data
        assert "group_email" not in log_data
    
    def test_integration_with_job_output_queue(self):
        """Test integration between enqueue_log and JobOutputQueue."""
        # Use enqueue_log to add data
        enqueue_log("integration-test", "Integration content")
        
        # Access via JobOutputQueue directly
        job_queue = JobOutputQueue()
        direct_queue = job_queue.get_queue()
        
        # Should be the same queue
        util_queue = get_job_output_queue()
        assert direct_queue is util_queue
        
        # Data should be accessible
        log_data = direct_queue.get()
        assert log_data["job_id"] == "integration-test"
        assert log_data["content"] == "Integration content"