"""
Unit tests for the logging callbacks module.

AgentTraceEventListener is now a thin shell — OTel bridge handles all event
subscriptions and trace persistence. Tests verify initialization, validation,
and that setup_listeners is a no-op.
"""

import pytest
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock

from src.engines.crewai.callbacks.logging_callbacks import (
    AgentTraceEventListener,
    TaskCompletionEventListener,
)
from src.utils.user_context import GroupContext


class TestAgentTraceEventListener:
    """Test suite for AgentTraceEventListener (thin shell)."""

    @pytest.fixture
    def group_context(self):
        return GroupContext(
            group_ids=["group_123"],
            group_email="test@example.com",
            email_domain="example.com",
        )

    def test_initialization(self, group_context):
        """Test listener stores job_id, group_context, and init_time."""
        listener = AgentTraceEventListener("test_job_123", group_context)

        assert listener.job_id == "test_job_123"
        assert listener.group_context == group_context
        assert isinstance(listener._init_time, datetime)

    def test_initialization_with_task_event_queue(self, group_context):
        """Test listener stores task_event_queue when provided."""
        queue = MagicMock()
        listener = AgentTraceEventListener(
            "test_job_123", group_context, task_event_queue=queue
        )

        assert listener._task_event_queue == queue

    def test_initialization_with_invalid_job_id(self, group_context):
        """Test that initialization fails with invalid job_id."""
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            AgentTraceEventListener("", group_context)

        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            AgentTraceEventListener(None, group_context)

    def test_initialization_without_group_context(self):
        """Test listener works with None group_context."""
        listener = AgentTraceEventListener("test_job_123", None)
        assert listener.group_context is None

    def test_setup_listeners_is_noop(self, group_context):
        """Test that setup_listeners does not register any handlers."""
        listener = AgentTraceEventListener("test_job_123", group_context)

        mock_event_bus = MagicMock()
        listener.setup_listeners(mock_event_bus)

        mock_event_bus.on.assert_not_called()


class TestTaskCompletionEventListener:
    """Test suite for TaskCompletionEventListener (deprecated shell)."""

    def test_initialization(self):
        """Test TaskCompletionEventListener initialization."""
        group_context = MagicMock()
        listener = TaskCompletionEventListener("test_job_456", group_context)

        assert listener.job_id == "test_job_456"
        assert listener.group_context == group_context

    def test_setup_listeners_is_noop(self):
        """Test that setup_listeners is a no-op."""
        mock_event_bus = MagicMock()
        listener = TaskCompletionEventListener("test_job", None)

        listener.setup_listeners(mock_event_bus)

        mock_event_bus.on.assert_not_called()


class TestPydanticOutputSerialization:
    """Test that structured pydantic outputs are serialized as JSON
    instead of Python repr when stored as trace output.

    Note: These tests verify a serialization pattern used by the OTel
    bridge, not the logging_callbacks module directly.
    """

    def test_pydantic_output_serialized_as_json(self):
        """When event.task.output.pydantic has model_dump_json, it should be used."""
        mock_pydantic = MagicMock()
        mock_pydantic.model_dump_json.return_value = (
            '{"list_of_plans_per_task":[{"task_number":1,"task":"Test","plan":"Step 1"}]}'
        )

        mock_task_output = MagicMock()
        mock_task_output.pydantic = mock_pydantic

        mock_task = MagicMock()
        mock_task.output = mock_task_output

        mock_event = MagicMock()
        mock_event.output = "list_of_plans_per_task=[PlanPerTask(...)]"
        mock_event.task = mock_task

        output_content = ""
        if mock_event.output is not None:
            task_output = getattr(mock_event.task, "output", None)
            pydantic_output = getattr(task_output, "pydantic", None) if task_output else None
            if pydantic_output and hasattr(pydantic_output, "model_dump_json"):
                try:
                    output_content = pydantic_output.model_dump_json()
                except Exception:
                    output_content = str(mock_event.output)
            else:
                output_content = str(mock_event.output)

        assert output_content == '{"list_of_plans_per_task":[{"task_number":1,"task":"Test","plan":"Step 1"}]}'
        parsed = json.loads(output_content)
        assert "list_of_plans_per_task" in parsed

    def test_fallback_to_str_when_no_pydantic(self):
        """When event.task.output.pydantic is None, fall back to str(event.output)."""
        mock_task_output = MagicMock()
        mock_task_output.pydantic = None

        mock_task = MagicMock()
        mock_task.output = mock_task_output

        mock_event = MagicMock()
        mock_event.output = "Final Answer: The result is 42"
        mock_event.task = mock_task

        output_content = ""
        if mock_event.output is not None:
            task_output = getattr(mock_event.task, "output", None)
            pydantic_output = getattr(task_output, "pydantic", None) if task_output else None
            if pydantic_output and hasattr(pydantic_output, "model_dump_json"):
                try:
                    output_content = pydantic_output.model_dump_json()
                except Exception:
                    output_content = str(mock_event.output)
            else:
                output_content = str(mock_event.output)

        assert output_content == "Final Answer: The result is 42"

    def test_fallback_to_str_when_model_dump_json_fails(self):
        """When model_dump_json raises, fall back to str(event.output)."""
        mock_pydantic = MagicMock()
        mock_pydantic.model_dump_json.side_effect = RuntimeError("serialization error")

        mock_task_output = MagicMock()
        mock_task_output.pydantic = mock_pydantic

        mock_task = MagicMock()
        mock_task.output = mock_task_output

        mock_event = MagicMock()
        mock_event.output = "list_of_plans_per_task=[PlanPerTask(...)]"
        mock_event.task = mock_task

        output_content = ""
        if mock_event.output is not None:
            task_output = getattr(mock_event.task, "output", None)
            pydantic_output = getattr(task_output, "pydantic", None) if task_output else None
            if pydantic_output and hasattr(pydantic_output, "model_dump_json"):
                try:
                    output_content = pydantic_output.model_dump_json()
                except Exception:
                    output_content = str(mock_event.output)
            else:
                output_content = str(mock_event.output)

        assert output_content == "list_of_plans_per_task=[PlanPerTask(...)]"

    def test_empty_output_when_event_output_is_none(self):
        """When event.output is None, output_content should be empty."""
        mock_event = MagicMock()
        mock_event.output = None

        output_content = ""
        if mock_event.output is not None:
            output_content = str(mock_event.output)

        assert output_content == ""

    def test_fallback_when_task_has_no_output_attribute(self):
        """When event.task has no output attribute, fall back to str()."""
        mock_task = MagicMock(spec=[])
        mock_event = MagicMock()
        mock_event.output = "Some regular output"
        mock_event.task = mock_task

        output_content = ""
        if mock_event.output is not None:
            task_output = getattr(mock_event.task, "output", None)
            pydantic_output = getattr(task_output, "pydantic", None) if task_output else None
            if pydantic_output and hasattr(pydantic_output, "model_dump_json"):
                try:
                    output_content = pydantic_output.model_dump_json()
                except Exception:
                    output_content = str(mock_event.output)
            else:
                output_content = str(mock_event.output)

        assert output_content == "Some regular output"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
