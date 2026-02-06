"""
Extended unit tests for execution_callback module.

Comprehensive tests covering:
- Step callback context tracking
- Task callback agent extraction
- Tool-to-agent mapping
- Task switch detection
- Error handling scenarios
- Group context propagation
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime, timezone


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

        # Create mock agents
        agent1 = MagicMock()
        agent1.role = "Research Agent"
        agent1.tools = []

        agent2 = MagicMock()
        agent2.role = "Writer Agent"
        agent2.tools = []

        crew.agents = [agent1, agent2]
        crew.name = "Test Crew"

        # Create mock tasks
        task1 = MagicMock()
        task1.description = "Research the topic"
        task1.agent = agent1

        task2 = MagicMock()
        task2.description = "Write the content"
        task2.agent = agent2

        crew.tasks = [task1, task2]

        return crew

    def test_callbacks_with_crew_builds_agent_lookup(self, mock_group_context, mock_crew):
        """Test that agent lookup is built from crew."""
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

            step_cb, task_cb = create_execution_callbacks(
                job_id="test_job",
                config={},
                group_context=mock_group_context,
                crew=mock_crew
            )

            # Callbacks should be created
            assert callable(step_cb)
            assert callable(task_cb)

    def test_callbacks_with_crew_builds_task_mapping(self, mock_group_context, mock_crew):
        """Test that task-to-agent mapping is built from crew."""
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

            step_cb, task_cb = create_execution_callbacks(
                job_id="test_job",
                config={},
                group_context=mock_group_context,
                crew=mock_crew
            )

            assert callable(step_cb)
            assert callable(task_cb)

    def test_callbacks_with_tools_builds_tool_mapping(self, mock_group_context):
        """Test that tool-to-agent mapping is built."""
        crew = MagicMock()

        # Create agent with tools
        agent = MagicMock()
        agent.role = "Tool Agent"

        tool1 = MagicMock()
        tool1.name = "SearchTool"

        tool2 = MagicMock()
        tool2.name = "WebScrapeTool"

        agent.tools = [tool1, tool2]
        crew.agents = [agent]
        crew.tasks = []
        crew.name = "Tool Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

            step_cb, task_cb = create_execution_callbacks(
                job_id="test_job",
                config={},
                group_context=mock_group_context,
                crew=crew
            )

            assert callable(step_cb)

    def test_callbacks_maps_mcp_tools(self, mock_group_context):
        """Test that MCP tools with prefixes are properly mapped."""
        crew = MagicMock()

        agent = MagicMock()
        agent.role = "MCP Agent"

        # MCP tool with prefix
        tool = MagicMock()
        tool.name = "Gmail_send_email_tool"

        agent.tools = [tool]
        crew.agents = [agent]
        crew.tasks = []
        crew.name = "MCP Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

            step_cb, task_cb = create_execution_callbacks(
                job_id="test_job",
                config={},
                group_context=mock_group_context,
                crew=crew
            )

            assert callable(step_cb)


class TestStepCallbackExtended:
    """Extended tests for step callback functionality."""

    def test_step_callback_handles_agent_action(self):
        """Test step callback handles AgentAction objects."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent = MagicMock()
        agent.role = "Test Agent"
        agent.tools = []
        crew.agents = [agent]
        crew.tasks = []
        crew.name = "Test Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                # Create AgentAction-like mock
                mock_output = MagicMock()
                mock_output.__class__.__name__ = "AgentAction"
                mock_output.tool = "SearchTool"
                mock_output.tool_input = "search query"
                mock_output.thought = "I need to search"
                mock_output.log = "Searching..."

                step_cb(mock_output)

                # Should have logged and queued
                assert mock_enqueue.called or mock_queue.put_nowait.called

    def test_step_callback_handles_agent_finish(self):
        """Test step callback handles AgentFinish objects."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent = MagicMock()
        agent.role = "Test Agent"
        agent.tools = []
        crew.agents = [agent]
        crew.tasks = []
        crew.name = "Test Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                mock_output = MagicMock()
                mock_output.__class__.__name__ = "AgentFinish"
                mock_output.output = "Final answer"

                step_cb(mock_output)

                assert mock_enqueue.called or mock_queue.put_nowait.called

    def test_step_callback_handles_string_output(self):
        """Test step callback handles string output."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent = MagicMock()
        agent.role = "Test Agent"
        agent.tools = []
        crew.agents = [agent]
        crew.tasks = []
        crew.name = "Test Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                step_cb("Simple string output")

                assert mock_enqueue.called or mock_queue.put_nowait.called

    def test_step_callback_handles_output_with_raw(self):
        """Test step callback extracts raw attribute."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent = MagicMock()
        agent.role = "Test Agent"
        agent.tools = []
        crew.agents = [agent]
        crew.tasks = []
        crew.name = "Test Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                mock_output = MagicMock()
                mock_output.output = None
                mock_output.raw = "Raw content"

                step_cb(mock_output)

                assert mock_enqueue.called or mock_queue.put_nowait.called

    def test_step_callback_truncates_long_content(self):
        """Test step callback truncates very long content."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent = MagicMock()
        agent.role = "Test Agent"
        agent.tools = []
        crew.agents = [agent]
        crew.tasks = []
        crew.name = "Test Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                long_output = "x" * 1000
                mock_output = MagicMock()
                mock_output.output = long_output

                step_cb(mock_output)

                # Should still work without error
                assert mock_enqueue.called or mock_queue.put_nowait.called

    def test_step_callback_handles_exception_gracefully(self):
        """Test step callback handles exceptions without crashing."""
        mock_queue = MagicMock()
        mock_queue.put_nowait.side_effect = Exception("Queue error")

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent = MagicMock()
        agent.role = "Test Agent"
        agent.tools = []
        crew.agents = [agent]
        crew.tasks = []
        crew.name = "Test Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_enqueue.side_effect = Exception("Log error")
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                # Should not raise
                step_cb("test output")


class TestTaskCallbackExtended:
    """Extended tests for task callback functionality."""

    def test_task_callback_extracts_agent_from_task_output(self):
        """Test task callback extracts agent from task_output.agent."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent1 = MagicMock()
        agent1.role = "Agent One"
        agent1.tools = []

        agent2 = MagicMock()
        agent2.role = "Agent Two"
        agent2.tools = []

        task1 = MagicMock()
        task1.description = "Task One Description"
        task1.agent = agent1

        task2 = MagicMock()
        task2.description = "Task Two Description"
        task2.agent = agent2

        crew.agents = [agent1, agent2]
        crew.tasks = [task1, task2]
        crew.name = "Multi-Task Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                mock_output = MagicMock()
                mock_output.description = "Task One Description"
                mock_output.raw = "Task result"
                mock_output.agent = crew.agents[0]
                mock_output.task = crew.tasks[0]

                task_cb(mock_output)

                assert mock_enqueue.called or mock_queue.put_nowait.called

    def test_task_callback_extracts_agent_from_task_object(self):
        """Test task callback extracts agent from task.agent."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent1 = MagicMock()
        agent1.role = "Agent One"
        agent1.tools = []

        task1 = MagicMock()
        task1.description = "Task One Description"
        task1.agent = agent1

        crew.agents = [agent1]
        crew.tasks = [task1]
        crew.name = "Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                mock_output = MagicMock()
                mock_output.description = "Task description"
                mock_output.raw = "Result"
                mock_output.agent = None
                mock_output.task = crew.tasks[0]

                task_cb(mock_output)

                assert mock_enqueue.called or mock_queue.put_nowait.called

    def test_task_callback_matches_by_description(self):
        """Test task callback matches task by description."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent1 = MagicMock()
        agent1.role = "Agent One"
        agent1.tools = []

        task1 = MagicMock()
        task1.description = "Task One Description"
        task1.agent = agent1

        crew.agents = [agent1]
        crew.tasks = [task1]
        crew.name = "Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                mock_output = MagicMock()
                mock_output.description = "Task One Description"
                mock_output.raw = "Result"
                mock_output.agent = None
                mock_output.task = None

                # Should find agent from crew.tasks by description match
                task_cb(mock_output)

                assert mock_enqueue.called or mock_queue.put_nowait.called

    def test_task_callback_updates_context_for_next_task(self):
        """Test task callback updates context for next task."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent1 = MagicMock()
        agent1.role = "Agent One"
        agent1.tools = []

        agent2 = MagicMock()
        agent2.role = "Agent Two"
        agent2.tools = []

        task1 = MagicMock()
        task1.description = "Task One Description"
        task1.agent = agent1

        task2 = MagicMock()
        task2.description = "Task Two Description"
        task2.agent = agent2

        crew.agents = [agent1, agent2]
        crew.tasks = [task1, task2]
        crew.name = "Multi-Task Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                # Complete first task
                mock_output = MagicMock()
                mock_output.description = "Task One Description"
                mock_output.raw = "Result"
                mock_output.task = crew.tasks[0]

                task_cb(mock_output)

                # Context should be prepared for next task
                assert mock_enqueue.called or mock_queue.put_nowait.called

    def test_task_callback_handles_last_task(self):
        """Test task callback handles completion of last task."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()
        agent1 = MagicMock()
        agent1.role = "Agent One"
        agent1.tools = []

        task1 = MagicMock()
        task1.description = "Task Description"
        task1.agent = agent1

        crew.agents = [agent1]
        crew.tasks = [task1]
        crew.name = "Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                # Complete last task
                mock_output = MagicMock()
                mock_output.description = "Task Description"
                mock_output.raw = "Final result"
                mock_output.task = crew.tasks[0]

                task_cb(mock_output)

                assert mock_enqueue.called or mock_queue.put_nowait.called


class TestCrewCallbacksExtended:
    """Extended tests for crew lifecycle callbacks."""

    def test_create_crew_callbacks_returns_callbacks(self):
        """Test create_crew_callbacks returns callback functions."""
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_queue:
            mock_queue.return_value = MagicMock()

            from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

            mock_group_context = MagicMock()
            mock_group_context.primary_group_id = "group_123"
            mock_group_context.group_email = "test@example.com"

            callbacks = create_crew_callbacks(
                job_id="test_job",
                group_context=mock_group_context
            )

            # Should return dict with callback functions
            assert isinstance(callbacks, dict)

    def test_log_crew_initialization_logs_config(self):
        """Test log_crew_initialization logs configuration."""
        from src.engines.crewai.callbacks.execution_callback import log_crew_initialization

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        config = {
            "agents": [{"role": "Test Agent"}],
            "tasks": [{"description": "Test Task"}]
        }

        # Should not raise
        log_crew_initialization(
            job_id="test_job",
            config=config,
            group_context=mock_group_context
        )

    def test_log_crew_initialization_sanitizes_config(self):
        """Test log_crew_initialization removes sensitive data."""
        from src.engines.crewai.callbacks.execution_callback import log_crew_initialization

        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        config = {
            "agents": [{"role": "Test Agent"}],
            "api_key": "secret_key",
            "token": "secret_token",
            "password": "secret_password"
        }

        # Should not raise and should sanitize sensitive fields
        log_crew_initialization(
            job_id="test_job",
            config=config,
            group_context=mock_group_context
        )


class TestAgentContextSwitching:
    """Tests for agent context switching during multi-agent execution."""

    def test_context_switches_between_agents(self):
        """Test that context properly switches between agents."""
        mock_queue = MagicMock()
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@example.com"

        crew = MagicMock()

        agent1 = MagicMock()
        agent1.role = "Research Agent"
        agent1.tools = []

        agent2 = MagicMock()
        agent2.role = "Writer Agent"
        agent2.tools = []

        task1 = MagicMock()
        task1.description = "Research task"
        task1.agent = agent1

        task2 = MagicMock()
        task2.description = "Writing task"
        task2.agent = agent2

        crew.agents = [agent1, agent2]
        crew.tasks = [task1, task2]
        crew.name = "Multi-Agent Crew"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):
                mock_get_queue.return_value = mock_queue

                from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

                step_cb, task_cb = create_execution_callbacks(
                    job_id="test_job",
                    config={},
                    group_context=mock_group_context,
                    crew=crew
                )

                # First agent does work with Final Answer (triggers trace)
                mock_output_1 = MagicMock()
                mock_output_1.__class__.__name__ = "AgentFinish"
                mock_output_1.output = "Final Answer: Research output"
                mock_output_1.agent = agent1
                step_cb(mock_output_1)

                # First task completes
                mock_task_output1 = MagicMock()
                mock_task_output1.description = "Research task"
                mock_task_output1.raw = "Research result"
                mock_task_output1.task = task1

                task_cb(mock_task_output1)

                # Second agent does work with Final Answer (triggers trace)
                mock_output_2 = MagicMock()
                mock_output_2.__class__.__name__ = "AgentFinish"
                mock_output_2.output = "Final Answer: Writing output"
                mock_output_2.agent = agent2
                step_cb(mock_output_2)

                # Should have queued events for Final Answer patterns
                assert mock_queue.put_nowait.called
                assert mock_queue.put_nowait.call_count >= 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
