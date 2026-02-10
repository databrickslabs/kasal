"""
Extended unit tests for execution_callback module.

Comprehensive tests covering the simplified execution-scoped callbacks that
handle execution log streaming only.  Trace creation is delegated to the
event bus handlers (logging_callbacks.py) and the OTel pipeline.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestCreateExecutionCallbacksExtended:
    """Extended tests for create_execution_callbacks function."""

    @pytest.fixture
    def mock_group_context(self):
        """Create mock group context."""
        context = MagicMock()
        context.primary_group_id = "group_test_123"
        context.group_email = "test@example.com"
        return context

    @pytest.fixture
    def mock_crew(self):
        """Create mock crew with agents and tasks."""
        crew = MagicMock()
        agent1 = MagicMock()
        agent1.role = "Research Agent"
        agent1.tools = []
        agent2 = MagicMock()
        agent2.role = "Writer Agent"
        agent2.tools = []
        crew.agents = [agent1, agent2]
        crew.name = "Test Crew"
        task1 = MagicMock()
        task1.description = "Research the topic"
        task1.agent = agent1
        task2 = MagicMock()
        task2.description = "Write the content"
        task2.agent = agent2
        crew.tasks = [task1, task2]
        return crew

    def test_callbacks_accept_crew_parameter(self, mock_group_context, mock_crew):
        """Test that crew parameter is accepted for API compatibility."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        step_cb, task_cb = create_execution_callbacks(
            job_id="test_job",
            config={},
            group_context=mock_group_context,
            crew=mock_crew,
        )
        assert callable(step_cb)
        assert callable(task_cb)

    def test_callbacks_work_without_crew(self, mock_group_context):
        """Test that callbacks work without crew parameter."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        step_cb, task_cb = create_execution_callbacks(
            job_id="test_job",
            config={},
            group_context=mock_group_context,
        )
        assert callable(step_cb)
        assert callable(task_cb)

    def test_callbacks_work_without_config(self, mock_group_context):
        """Test that callbacks work with None config."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        step_cb, task_cb = create_execution_callbacks(
            job_id="test_job",
            config=None,
            group_context=mock_group_context,
        )
        assert callable(step_cb)
        assert callable(task_cb)


class TestStepCallbackExtended:
    """Extended tests for step callback functionality."""

    def _create_step_callback(self, job_id="test_job", group_context=None):
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        step_cb, _ = create_execution_callbacks(
            job_id=job_id, config={}, group_context=group_context
        )
        return step_cb

    def test_step_callback_handles_output_attribute(self):
        """Test step callback reads 'output' attribute."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_cb = self._create_step_callback()
            mock_output = MagicMock()
            mock_output.output = "Agent step output"
            step_cb(mock_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert "[STEP]" in kwargs["content"]
            assert "Agent step output" in kwargs["content"]

    def test_step_callback_handles_raw_attribute(self):
        """Test step callback reads 'raw' attribute when 'output' is missing."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_cb = self._create_step_callback()
            mock_output = MagicMock(spec=[])
            mock_output.raw = "Raw content"
            step_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert "Raw content" in kwargs["content"]

    def test_step_callback_handles_log_attribute(self):
        """Test step callback reads 'log' attribute as fallback."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_cb = self._create_step_callback()
            mock_output = MagicMock(spec=[])
            mock_output.log = "Log content"
            step_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert "Log content" in kwargs["content"]

    def test_step_callback_handles_string_output(self):
        """Test step callback handles plain string output."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_cb = self._create_step_callback()
            step_cb("Simple string output")

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert "Simple string output" in kwargs["content"]

    def test_step_callback_truncates_long_content(self):
        """Test step callback truncates content longer than 500 chars."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_cb = self._create_step_callback()
            mock_output = MagicMock()
            mock_output.output = "x" * 1000
            step_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert kwargs["content"].endswith("...")
            assert len(kwargs["content"]) < 600

    def test_step_callback_handles_exception_gracefully(self):
        """Test step callback handles exceptions without crashing."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            mock_enqueue.side_effect = Exception("Log error")
            step_cb = self._create_step_callback()
            # Should not raise
            step_cb("test output")

    def test_step_callback_includes_job_id(self):
        """Test step callback passes correct execution_id."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_cb = self._create_step_callback(job_id="my_job_42")
            mock_output = MagicMock()
            mock_output.output = "test"
            step_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "my_job_42"

    def test_step_callback_includes_group_context(self):
        """Test step callback passes group_context."""
        gc = MagicMock()
        gc.primary_group_id = "g1"
        gc.group_email = "g@test.com"

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_cb = self._create_step_callback(group_context=gc)
            mock_output = MagicMock()
            mock_output.output = "test"
            step_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert kwargs["group_context"] == gc


class TestTaskCallbackExtended:
    """Extended tests for task callback functionality."""

    def _create_task_callback(self, job_id="test_job", group_context=None):
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        _, task_cb = create_execution_callbacks(
            job_id=job_id, config={}, group_context=group_context
        )
        return task_cb

    def test_task_callback_extracts_description(self):
        """Test task callback extracts description from task_output.description."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            task_cb = self._create_task_callback()
            mock_output = MagicMock()
            mock_output.description = "My Task Description"
            mock_output.raw = "result"
            task_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert "My Task Description" in kwargs["content"]
            assert "TASK COMPLETED" in kwargs["content"]

    def test_task_callback_extracts_description_from_task_attr(self):
        """Test task callback extracts description from task_output.task.description."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            task_cb = self._create_task_callback()
            mock_output = MagicMock(spec=[])
            mock_output.task = MagicMock()
            mock_output.task.description = "Nested task desc"
            mock_output.output = "result"
            task_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert "Nested task desc" in kwargs["content"]

    def test_task_callback_extracts_raw_content(self):
        """Test task callback extracts content from raw attribute."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            task_cb = self._create_task_callback()
            mock_output = MagicMock()
            mock_output.description = "task"
            mock_output.raw = "Raw task result"
            task_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert "Raw task result" in kwargs["content"]

    def test_task_callback_extracts_output_content(self):
        """Test task callback extracts content from output attribute when raw is missing."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            task_cb = self._create_task_callback()
            mock_output = MagicMock(spec=[])
            mock_output.output = "Output task result"
            task_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert "Output task result" in kwargs["content"]

    def test_task_callback_falls_back_to_str_when_no_raw_or_output(self):
        """Test task callback uses str(task_output) when neither raw nor output exist.

        Covers line 95: content = str(task_output) -- the else branch in task_callback
        when the task_output object has neither .raw nor .output attributes.
        """
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            task_cb = self._create_task_callback()

            # Use a plain object that has description but NOT raw or output
            class BareTaskOutput:
                def __init__(self):
                    self.description = "Fallback task"

                def __str__(self):
                    return "stringified-task-output"

            mock_output = BareTaskOutput()
            task_cb(mock_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert "stringified-task-output" in kwargs["content"]
            assert "TASK COMPLETED" in kwargs["content"]

    def test_task_callback_handles_exception_gracefully(self):
        """Test task callback handles exceptions without crashing."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            mock_enqueue.side_effect = Exception("Log error")
            task_cb = self._create_task_callback()
            mock_output = MagicMock()
            mock_output.raw = "result"
            mock_output.description = "task"
            # Should not raise
            task_cb(mock_output)

    def test_task_callback_truncates_long_description(self):
        """Test task callback truncates descriptions longer than 100 chars."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            task_cb = self._create_task_callback()
            mock_output = MagicMock()
            mock_output.description = "D" * 200
            mock_output.raw = "result"
            task_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            # The description part should be truncated
            assert "..." in kwargs["content"]

    def test_task_callback_includes_job_id(self):
        """Test task callback passes correct execution_id."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            task_cb = self._create_task_callback(job_id="my_job_99")
            mock_output = MagicMock()
            mock_output.description = "task"
            mock_output.raw = "result"
            task_cb(mock_output)

            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "my_job_99"


class TestCrewCallbacksExtended:
    """Extended tests for crew lifecycle callbacks."""

    def test_create_crew_callbacks_returns_callbacks(self):
        """Test create_crew_callbacks returns callback functions."""
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        callbacks = create_crew_callbacks(
            job_id="test_job",
            group_context=MagicMock(),
        )
        assert isinstance(callbacks, dict)
        assert "on_start" in callbacks
        assert "on_complete" in callbacks
        assert "on_error" in callbacks

    def test_log_crew_initialization_logs_config(self):
        """Test log_crew_initialization logs configuration."""
        from src.engines.crewai.callbacks.execution_callback import log_crew_initialization

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            log_crew_initialization(
                job_id="test_job",
                config={"agents": [{"role": "Test Agent"}]},
                group_context=MagicMock(),
            )
            mock_enqueue.assert_called_once()

    def test_log_crew_initialization_sanitizes_config(self):
        """Test log_crew_initialization removes sensitive data."""
        from src.engines.crewai.callbacks.execution_callback import log_crew_initialization

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            log_crew_initialization(
                job_id="test_job",
                config={
                    "model": "test-model",
                    "api_keys": {"secret": "hidden"},
                    "tokens": {"access_token": "secret"},
                    "passwords": {"db_pass": "secret"},
                    "normal_field": "visible",
                },
                group_context=MagicMock(),
            )
            content = mock_enqueue.call_args[1]["content"]
            assert "test-model" in content
            assert "visible" in content
            assert "secret" not in content
            assert "hidden" not in content


    def test_on_crew_start_handles_exception_gracefully(self):
        """Test on_crew_start exception handler catches and logs errors.

        Covers lines 152-153: except Exception handler in on_crew_start.
        """
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            mock_enqueue.side_effect = RuntimeError("Queue unavailable")
            callbacks = create_crew_callbacks(
                job_id="err_job",
                group_context=MagicMock(),
            )
            # Should not raise -- the exception handler catches and logs the error
            callbacks["on_start"]()

    def test_on_crew_complete_handles_exception_gracefully(self):
        """Test on_crew_complete exception handler catches and logs errors.

        Covers lines 173-174: except Exception handler in on_crew_complete.
        """
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            mock_enqueue.side_effect = RuntimeError("Queue unavailable")
            callbacks = create_crew_callbacks(
                job_id="err_job",
                group_context=MagicMock(),
            )
            # Should not raise -- the exception handler catches and logs the error
            callbacks["on_complete"]("some result")

    def test_on_crew_error_handles_exception_gracefully(self):
        """Test on_crew_error exception handler catches and logs errors.

        Covers lines 193-194: except Exception handler in on_crew_error.
        """
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            mock_enqueue.side_effect = RuntimeError("Queue unavailable")
            callbacks = create_crew_callbacks(
                job_id="err_job",
                group_context=MagicMock(),
            )
            # Should not raise -- the exception handler catches and logs the error
            callbacks["on_error"](Exception("Original crew error"))


class TestMultiAgentExecution:
    """Tests for multi-agent execution log isolation."""

    def test_sequential_step_callbacks_isolated(self):
        """Test that step callbacks from different jobs produce separate logs."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_1, _ = create_execution_callbacks("job_1", {}, None)
            step_2, _ = create_execution_callbacks("job_2", {}, None)

            out1 = MagicMock()
            out1.output = "Output from job 1"
            out2 = MagicMock()
            out2.output = "Output from job 2"

            step_1(out1)
            step_2(out2)

            assert mock_enqueue.call_count == 2
            calls = mock_enqueue.call_args_list
            assert calls[0][1]["execution_id"] == "job_1"
            assert calls[1][1]["execution_id"] == "job_2"

    def test_sequential_task_callbacks_isolated(self):
        """Test that task callbacks from different jobs produce separate logs."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            _, task_1 = create_execution_callbacks("job_1", {}, None)
            _, task_2 = create_execution_callbacks("job_2", {}, None)

            out1 = MagicMock()
            out1.description = "Task 1"
            out1.raw = "Result 1"
            out2 = MagicMock()
            out2.description = "Task 2"
            out2.raw = "Result 2"

            task_1(out1)
            task_2(out2)

            assert mock_enqueue.call_count == 2
            calls = mock_enqueue.call_args_list
            assert calls[0][1]["execution_id"] == "job_1"
            assert calls[1][1]["execution_id"] == "job_2"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
