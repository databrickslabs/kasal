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
        step_callback, task_callback = create_execution_callbacks(
            "test_job", {"model": "test"}, None
        )

        assert callable(step_callback)
        assert callable(task_callback)

    def test_different_callbacks_for_different_jobs(self):
        """Test that different job IDs get different callback instances."""
        step_1, task_1 = create_execution_callbacks("job_1", {"model": "test"}, None)
        step_2, task_2 = create_execution_callbacks("job_2", {"model": "test"}, None)

        assert step_1 is not step_2
        assert task_1 is not task_2

    def test_callback_handles_missing_attributes(self):
        """Test that callbacks handle missing attributes gracefully."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks("test_job", {}, None)

            # Output with only 'output' attribute (standard MagicMock)
            mock_output = MagicMock()
            mock_output.output = "test output"
            step_callback(mock_output)

            mock_enqueue.assert_called_once()

    def test_callbacks_with_group_context(self):
        """Test callbacks work with group context."""
        mock_group = MagicMock()
        mock_group.primary_group_id = "group_123"
        mock_group.group_email = "test@group.com"

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks(
                "test_job", {"model": "test"}, mock_group
            )

            mock_output = MagicMock()
            mock_output.output = "test output"
            step_callback(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert kwargs["group_context"] == mock_group
