"""
Basic tests for execution callback functionality.

Simple tests to verify core callback functionality without complex mocking.
"""
import pytest
from unittest.mock import patch, MagicMock

from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks


class TestBasicCallbackFunctionality:
    """Basic tests for callback functionality."""
    
    def test_callback_creation(self):
        """Test that callbacks can be created successfully."""
        job_id = "test_job"
        config = {"model": "test"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue"), \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):
            
            step_callback, task_callback = create_execution_callbacks(job_id, config, None)
            
            # Verify callbacks are callable
            assert callable(step_callback)
            assert callable(task_callback)
    
    def test_different_callbacks_for_different_jobs(self):
        """Test that different job IDs get different callback instances."""
        job_1 = "job_1"
        job_2 = "job_2"
        config = {"model": "test"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue"), \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):
            
            step_1, task_1 = create_execution_callbacks(job_1, config, None)
            step_2, task_2 = create_execution_callbacks(job_2, config, None)
            
            # Verify different instances
            assert step_1 is not step_2
            assert task_1 is not task_2
    
    def test_callback_handles_missing_attributes(self):
        """Test that callbacks handle missing attributes gracefully."""
        job_id = "test_job"
        config = {"model": "test"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):
            
            mock_queue.return_value = MagicMock()
            
            step_callback, _ = create_execution_callbacks(job_id, config, None)
            
            # Test with output that has no agent attribute
            mock_output = MagicMock()
            mock_output.output = "test output"
            # Don't set agent attribute
            
            # Should not raise exception
            step_callback(mock_output)
    
    def test_callbacks_with_group_context(self):
        """Test callbacks work with group context."""
        job_id = "test_job"
        config = {"model": "test"}
        
        mock_group = MagicMock()
        mock_group.primary_group_id = "group_123"
        mock_group.group_email = "test@group.com"
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):
            
            mock_queue.return_value = MagicMock()
            
            step_callback, _ = create_execution_callbacks(job_id, config, mock_group)
            
            # Should not raise exception
            mock_output = MagicMock()
            mock_output.output = "test output"
            step_callback(mock_output)