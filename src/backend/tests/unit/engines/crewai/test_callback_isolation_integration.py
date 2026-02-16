"""
Integration tests for callback isolation system.

Tests the complete flow from callback creation through log processing
to ensure proper isolation between concurrent executions.

NOTE: The execution_callback module now only creates execution logs via
enqueue_log().  Trace creation is handled by the event bus handlers
(logging_callbacks.py) and the OTel pipeline.
"""
import pytest
from unittest.mock import patch, MagicMock

from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.primary_group_id = "group_123"
    context.group_email = "test@example.com"
    return context


class TestCallbackIsolationIntegration:
    """Integration tests for the complete callback isolation system."""

    def test_callback_creation_isolation(self, mock_group_context):
        """Test that callback creation produces isolated callbacks for different executions."""
        job_id_1 = "execution_1"
        job_id_2 = "execution_2"
        config = {"model": "test-model"}

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_1, task_1 = create_execution_callbacks(job_id_1, config, mock_group_context)
            step_2, task_2 = create_execution_callbacks(job_id_2, config, mock_group_context)

            # Verify callbacks are different instances
            assert step_1 is not step_2
            assert task_1 is not task_2

            # Test that callbacks produce logs with different execution IDs
            mock_output_1 = MagicMock()
            mock_output_1.output = "output from execution 1"
            mock_output_2 = MagicMock()
            mock_output_2.output = "output from execution 2"

            step_1(mock_output_1)
            step_2(mock_output_2)

            assert mock_enqueue.call_count == 2
            calls = mock_enqueue.call_args_list
            assert calls[0][1]["execution_id"] == job_id_1
            assert calls[1][1]["execution_id"] == job_id_2

    def test_concurrent_callback_execution(self, mock_group_context):
        """Test that concurrent callback execution maintains proper isolation."""
        configs = [
            {"job_id": "concurrent_1", "model": "model_1"},
            {"job_id": "concurrent_2", "model": "model_2"},
            {"job_id": "concurrent_3", "model": "model_3"},
        ]

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            callbacks = []
            for config in configs:
                job_id = config["job_id"]
                step_callback, task_callback = create_execution_callbacks(
                    job_id=job_id, config=config, group_context=mock_group_context
                )
                callbacks.append((job_id, step_callback, task_callback))

            # Simulate concurrent execution of step callbacks
            for job_id, step_callback, _ in callbacks:
                mock_output = MagicMock()
                mock_output.output = f"concurrent output for {job_id}"
                step_callback(mock_output)

            assert mock_enqueue.call_count == len(configs)

            # Verify all logs have correct execution IDs
            execution_ids = set()
            for call in mock_enqueue.call_args_list:
                execution_ids.add(call[1]["execution_id"])

            expected_ids = {c["job_id"] for c in configs}
            assert execution_ids == expected_ids

    def test_error_isolation_between_executions(self, mock_group_context):
        """Test that errors in one execution don't affect others."""
        job_id_1 = "execution_1"
        job_id_2 = "execution_2"
        config = {"model": "test-model"}

        call_count = {"count": 0}

        def selective_failure(**kwargs):
            call_count["count"] += 1
            if kwargs.get("execution_id") == job_id_1:
                raise Exception("Enqueue failed for execution 1")

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            mock_enqueue.side_effect = selective_failure

            step_1, _ = create_execution_callbacks(job_id_1, config, mock_group_context)
            step_2, _ = create_execution_callbacks(job_id_2, config, mock_group_context)

            mock_output_1 = MagicMock()
            mock_output_1.output = "output for execution 1"
            mock_output_2 = MagicMock()
            mock_output_2.output = "output for execution 2"

            # First should handle error gracefully, second should succeed
            step_1(mock_output_1)  # Should not raise
            step_2(mock_output_2)  # Should succeed

            # Both attempted to enqueue
            assert mock_enqueue.call_count == 2

    def test_group_context_isolation(self):
        """Test that different group contexts are properly isolated."""
        config = {"model": "test-model"}

        group_1 = MagicMock()
        group_1.primary_group_id = "group_1"
        group_1.group_email = "group1@example.com"

        group_2 = MagicMock()
        group_2.primary_group_id = "group_2"
        group_2.group_email = "group2@example.com"

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_1, _ = create_execution_callbacks("job_1", config, group_1)
            step_2, _ = create_execution_callbacks("job_2", config, group_2)

            out_1 = MagicMock()
            out_1.output = "output for group 1"
            out_2 = MagicMock()
            out_2.output = "output for group 2"

            step_1(out_1)
            step_2(out_2)

            assert mock_enqueue.call_count == 2
            calls = mock_enqueue.call_args_list
            assert calls[0][1]["group_context"] == group_1
            assert calls[1][1]["group_context"] == group_2

    def test_step_callback_creates_execution_log(self, mock_group_context):
        """Test that step callbacks create execution logs."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks(
                "execution_1", {"model": "test"}, mock_group_context
            )

            mock_output = MagicMock()
            mock_output.output = "regular agent output"
            step_callback(mock_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "execution_1"
            assert "[STEP]" in kwargs["content"]

    def test_task_callback_creates_execution_log(self, mock_group_context):
        """Test that task callbacks create execution logs."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            _, task_callback = create_execution_callbacks(
                "execution_1", {"model": "test"}, mock_group_context
            )

            mock_task_output = MagicMock()
            mock_task_output.raw = "task result"
            mock_task_output.description = "test task"
            task_callback(mock_task_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "execution_1"
            assert "TASK COMPLETED" in kwargs["content"]
