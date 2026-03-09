"""
Unit tests for specific flow changes.

Tests the three specific changes made to the flow execution system:
1. flow_methods.py - planning_llm fallback logic
2. flow_execution_runner.py - result parameter propagation
3. flow_runner_service.py - fresh session usage for post-execution updates
"""

import asyncio
import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch


class MockAsyncSession:
    """Mock that acts as both a session and an async context manager (like AsyncSession)."""
    def __init__(self, name="mock_session"):
        self._name = name

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def __repr__(self):
        return f"MockAsyncSession({self._name!r})"


def make_async_session_factory(*sessions):
    """Create an async_session_factory mock that returns MockAsyncSession objects.

    The sessions must be MockAsyncSession instances (or objects with __aenter__/__aexit__).
    """
    call_idx = [0]

    def factory():
        idx = min(call_idx[0], len(sessions) - 1)
        call_idx[0] += 1
        return sessions[idx]

    mock = MagicMock(side_effect=factory)
    return mock


class TestFlowMethodsPlanningLLM:
    """Test planning_llm fallback logic in FlowMethodFactory."""

    @pytest.mark.asyncio
    async def test_planning_llm_explicit_configuration(self):
        """Test planning_llm uses explicit crew configuration when available."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        # Mock crew data with explicit planning_llm
        mock_crew_data = MagicMock()
        mock_crew_data.planning = True
        mock_crew_data.planning_llm = "databricks-dbrx-instruct"
        mock_crew_data.memory = True
        mock_crew_data.process = "sequential"
        mock_crew_data.verbose = True
        mock_crew_data.reasoning = False

        # Mock agent with LLM
        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"
        mock_agent.llm = MagicMock()
        mock_agent.llm.model = "gpt-4"
        mock_agent.tools = []
        mock_agent._kasal_memory_disabled = False

        # Mock task with agent
        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"
        mock_task.expected_output = "Test output"
        mock_task.context = None

        # Mock LLMManager to return a planning LLM
        mock_planning_llm = MagicMock()

        # Crew IS a module-level import; LLMManager is a lazy import inside the closure
        with patch('src.engines.crewai.flow.modules.flow_methods.Crew') as mock_crew_class, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager:

            mock_llm_manager.get_llm = AsyncMock(return_value=mock_planning_llm)
            mock_crew_instance = MagicMock()
            mock_crew_class.return_value = mock_crew_instance

            # Create the starting point method
            method = FlowMethodFactory.create_starting_point_crew_method(
                method_name="test_start",
                task_list=[mock_task],
                crew_name="Test Crew",
                callbacks={"job_id": "test-job"},
                group_context=None,
                create_execution_callbacks=MagicMock(return_value=(MagicMock(), MagicMock())),
                crew_data=mock_crew_data
            )

            # Create mock flow instance (simulates 'self' in the @start decorated method)
            mock_flow = MagicMock()
            mock_flow.state = {}

            # Mock crew.kickoff_async to avoid actual execution
            mock_crew_instance.kickoff_async = AsyncMock(return_value=MagicMock(raw="Test result"))

            # Execute the method (._meth is the inner function before @start wrapping)
            await method._meth(mock_flow)

            # Verify LLMManager was called with the explicit planning_llm
            mock_llm_manager.get_llm.assert_called_once_with("databricks-dbrx-instruct")

            # Verify Crew was created with planning_llm from LLMManager
            crew_call_kwargs = mock_crew_class.call_args[1]
            assert crew_call_kwargs['planning'] is True
            assert crew_call_kwargs['planning_llm'] == mock_planning_llm

    @pytest.mark.asyncio
    async def test_planning_llm_fallback_to_agent(self):
        """Test planning_llm falls back to first agent's LLM when not configured."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        # Mock crew data WITHOUT planning_llm
        mock_crew_data = MagicMock()
        mock_crew_data.planning = True
        mock_crew_data.planning_llm = None  # No explicit planning_llm
        mock_crew_data.memory = True
        mock_crew_data.process = "sequential"
        mock_crew_data.verbose = True
        mock_crew_data.reasoning = False

        # Mock agent with LLM
        mock_agent_llm = MagicMock()
        mock_agent_llm.model = "gpt-4"

        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"
        mock_agent.llm = mock_agent_llm
        mock_agent.tools = []
        mock_agent._kasal_memory_disabled = False

        # Mock task
        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"
        mock_task.expected_output = "Test output"
        mock_task.context = None

        with patch('src.engines.crewai.flow.modules.flow_methods.Crew') as mock_crew_class:
            mock_crew_instance = MagicMock()
            mock_crew_class.return_value = mock_crew_instance
            mock_crew_instance.kickoff_async = AsyncMock(return_value=MagicMock(raw="Test result"))

            method = FlowMethodFactory.create_starting_point_crew_method(
                method_name="test_start",
                task_list=[mock_task],
                crew_name="Test Crew",
                callbacks={"job_id": "test-job"},
                group_context=None,
                create_execution_callbacks=MagicMock(return_value=(MagicMock(), MagicMock())),
                crew_data=mock_crew_data
            )

            mock_flow = MagicMock()
            mock_flow.state = {}

            await method._meth(mock_flow)

            # Verify Crew was created with agent's LLM as planning_llm
            crew_call_kwargs = mock_crew_class.call_args[1]
            assert crew_call_kwargs['planning'] is True
            assert crew_call_kwargs['planning_llm'] == mock_agent_llm

    @pytest.mark.asyncio
    async def test_planning_llm_no_fallback_available(self):
        """Test planning_llm handles case when no LLM is available."""
        from src.engines.crewai.flow.modules.flow_methods import FlowMethodFactory

        # Mock crew data WITHOUT planning_llm
        mock_crew_data = MagicMock()
        mock_crew_data.planning = True
        mock_crew_data.planning_llm = None
        mock_crew_data.memory = True
        mock_crew_data.process = "sequential"
        mock_crew_data.verbose = True
        mock_crew_data.reasoning = False

        # Mock agent WITHOUT LLM
        mock_agent = MagicMock()
        mock_agent.role = "Test Agent"
        mock_agent.llm = None  # No LLM available
        mock_agent.tools = []
        mock_agent._kasal_memory_disabled = False

        # Mock task
        mock_task = MagicMock()
        mock_task.agent = mock_agent
        mock_task.description = "Test task"
        mock_task.expected_output = "Test output"
        mock_task.context = None

        with patch('src.engines.crewai.flow.modules.flow_methods.Crew') as mock_crew_class:
            mock_crew_instance = MagicMock()
            mock_crew_class.return_value = mock_crew_instance
            mock_crew_instance.kickoff_async = AsyncMock(return_value=MagicMock(raw="Test result"))

            method = FlowMethodFactory.create_starting_point_crew_method(
                method_name="test_start",
                task_list=[mock_task],
                crew_name="Test Crew",
                callbacks={"job_id": "test-job"},
                group_context=None,
                create_execution_callbacks=MagicMock(return_value=(MagicMock(), MagicMock())),
                crew_data=mock_crew_data
            )

            mock_flow = MagicMock()
            mock_flow.state = {}

            await method._meth(mock_flow)

            # Verify Crew was created with planning=True but no planning_llm
            crew_call_kwargs = mock_crew_class.call_args[1]
            assert crew_call_kwargs['planning'] is True
            # planning_llm should not be in kwargs when no LLM available
            assert 'planning_llm' not in crew_call_kwargs or crew_call_kwargs['planning_llm'] is None


class TestFlowExecutionRunnerResultPropagation:
    """Test result parameter propagation in flow_execution_runner.py."""

    @pytest.mark.asyncio
    async def test_update_execution_status_with_result(self):
        """Test update_execution_status_with_retry accepts and passes result parameter."""
        from src.engines.crewai.flow.flow_execution_runner import update_execution_status_with_retry

        execution_id = "test-exec-123"
        status = "COMPLETED"
        message = "Flow completed successfully"
        result = {"output": "Test output", "metrics": {"duration": 45}}

        # ExecutionStatusService is a lazy import inside the function
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
            mock_service.update_status = AsyncMock(return_value=True)

            success = await update_execution_status_with_retry(
                execution_id=execution_id,
                status=status,
                message=message,
                result=result
            )

            mock_service.update_status.assert_called_once_with(
                job_id=execution_id,
                status=status,
                message=message,
                result=result
            )
            assert success is True

    @pytest.mark.asyncio
    async def test_update_execution_status_without_result(self):
        """Test update_execution_status_with_retry works without result parameter."""
        from src.engines.crewai.flow.flow_execution_runner import update_execution_status_with_retry

        execution_id = "test-exec-456"
        status = "RUNNING"
        message = "Flow is running"

        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_service:
            mock_service.update_status = AsyncMock(return_value=True)

            success = await update_execution_status_with_retry(
                execution_id=execution_id,
                status=status,
                message=message
            )

            mock_service.update_status.assert_called_once_with(
                job_id=execution_id,
                status=status,
                message=message,
                result=None
            )
            assert success is True

    @pytest.mark.asyncio
    async def test_run_flow_in_process_propagates_final_result(self):
        """Test run_flow_in_process propagates final_result to status update."""
        from src.engines.crewai.flow.flow_execution_runner import run_flow_in_process

        execution_id = "test-exec-789"
        config = {"flow_id": "test-flow", "inputs": {}}
        running_jobs = {}

        mock_result_data = {"final_output": "Flow completed", "stats": {"tasks": 3}}

        # process_flow_executor IS at module level
        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:

            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'COMPLETED',
                'result': mock_result_data
            })
            mock_update.return_value = True

            await run_flow_in_process(
                execution_id=execution_id,
                config=config,
                running_jobs=running_jobs,
                group_context=None,
                user_token=None
            )

            mock_update.assert_called_once()
            call_kwargs = mock_update.call_args[1]
            assert call_kwargs['execution_id'] == execution_id
            assert call_kwargs['status'] == 'COMPLETED'
            assert call_kwargs['result'] == mock_result_data

    @pytest.mark.asyncio
    async def test_run_flow_in_process_no_result_for_failed(self):
        """Test run_flow_in_process does not pass result for failed executions."""
        from src.engines.crewai.flow.flow_execution_runner import run_flow_in_process

        execution_id = "test-exec-failed"
        config = {"flow_id": "test-flow", "inputs": {}}
        running_jobs = {}

        with patch('src.engines.crewai.flow.flow_execution_runner.process_flow_executor') as mock_executor, \
             patch('src.engines.crewai.flow.flow_execution_runner.update_execution_status_with_retry') as mock_update:

            mock_executor.run_flow_isolated = AsyncMock(return_value={
                'status': 'FAILED',
                'error': 'Test error'
            })
            mock_update.return_value = True

            # Mock the lazy-imported ExecutionStatusService for status check
            with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service:
                mock_execution = MagicMock()
                mock_execution.status = 'RUNNING'
                mock_status_service.get_status = AsyncMock(return_value=mock_execution)

                await run_flow_in_process(
                    execution_id=execution_id,
                    config=config,
                    running_jobs=running_jobs,
                    group_context=None,
                    user_token=None
                )

            mock_update.assert_called_once()
            call_kwargs = mock_update.call_args[1]
            assert call_kwargs['execution_id'] == execution_id
            assert call_kwargs['status'] == 'FAILED'
            assert call_kwargs['result'] is None


class TestFlowRunnerServiceFreshSession:
    """Test fresh session usage for post-execution updates in flow_runner_service.py."""

    @pytest.mark.asyncio
    async def test_dynamic_flow_uses_fresh_session_on_success(self):
        """Test _run_dynamic_flow uses fresh session for post-execution DB updates."""
        from src.engines.crewai.flow.flow_runner_service import FlowRunnerService

        mock_session = MagicMock()

        # Patch FlowExecutionService at module level (used in __init__)
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_fes_class:
            mock_fes_class.return_value = MagicMock()
            service = FlowRunnerService(mock_session)

        execution_id = 1
        job_id = "test-job-123"
        config = {
            "nodes": [{"id": "node1", "type": "crew"}],
            "edges": [],
            "flow_config": {"startingPoints": ["node1"], "listeners": []},
            "group_id": "test-group"
        }

        # Track which sessions are used for FlowExecutionService
        session_tracker = []

        def track_fes(sess):
            mock_svc = MagicMock()
            mock_svc.update_execution_status = AsyncMock()
            session_tracker.append(sess)
            return mock_svc

        # Set up two distinct sessions (must be async context managers like AsyncSession)
        mock_initial_session = MockAsyncSession("initial_session")
        mock_post_session = MockAsyncSession("post_session")
        mock_factory = make_async_session_factory(mock_initial_session, mock_post_session)

        # BackendFlow is re-imported inside method — patch at source module
        with patch('src.engines.crewai.flow.flow_runner_service._smart_db_session', mock_factory), \
             patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService', side_effect=track_fes), \
             patch('src.engines.crewai.flow.flow_runner_service.ApiKeysService') as mock_api_keys, \
             patch('src.engines.crewai.flow.backend_flow.BackendFlow') as mock_backend_flow_class:

            mock_api_keys.get_provider_api_key = AsyncMock(return_value=None)

            # Mock BackendFlow instance
            mock_flow_instance = MagicMock()
            mock_flow_instance.kickoff = AsyncMock(return_value={
                "success": True,
                "result": {"output": "Test output"}
            })
            mock_backend_flow_class.return_value = mock_flow_instance

            result = await service._run_dynamic_flow(execution_id, job_id, config)

            # Verify two sessions were created (initial + fresh post-execution)
            assert mock_factory.call_count == 2

            # Verify both sessions were used to create FlowExecutionService
            assert len(session_tracker) >= 2
            assert session_tracker[0] is mock_initial_session
            assert session_tracker[-1] is mock_post_session

            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_fresh_session_used_for_failed_flow(self):
        """Test fresh session is used even for failed flow executions."""
        from src.engines.crewai.flow.flow_runner_service import FlowRunnerService

        mock_session = MagicMock()

        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_fes_class:
            mock_fes_class.return_value = MagicMock()
            service = FlowRunnerService(mock_session)

        execution_id = 3
        job_id = "test-job-failed"
        config = {
            "nodes": [{"id": "node1"}],
            "edges": [],
            "flow_config": {"startingPoints": ["node1"]},
            "group_id": "test-group"
        }

        mock_initial_session = MockAsyncSession("initial_session")
        mock_post_session = MockAsyncSession("post_session")
        mock_factory = make_async_session_factory(mock_initial_session, mock_post_session)

        session_tracker = []

        def track_fes(sess):
            mock_svc = MagicMock()
            mock_svc.update_execution_status = AsyncMock()
            session_tracker.append(sess)
            return mock_svc

        with patch('src.engines.crewai.flow.flow_runner_service._smart_db_session', mock_factory), \
             patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService', side_effect=track_fes), \
             patch('src.engines.crewai.flow.flow_runner_service.ApiKeysService') as mock_api_keys, \
             patch('src.engines.crewai.flow.backend_flow.BackendFlow') as mock_backend_flow_class:

            mock_api_keys.get_provider_api_key = AsyncMock(return_value=None)

            # Mock BackendFlow to return failure
            mock_flow_instance = MagicMock()
            mock_flow_instance.kickoff = AsyncMock(return_value={
                "success": False,
                "error": "Test error"
            })
            mock_backend_flow_class.return_value = mock_flow_instance

            result = await service._run_dynamic_flow(execution_id, job_id, config)

            # Verify fresh sessions: _safe_session + post_session (error span uses OTel pipeline)
            assert mock_factory.call_count == 2
            assert result["success"] is False
            assert result["error"] == "Test error"

    @pytest.mark.asyncio
    async def test_fresh_session_prevents_stale_connection_error(self):
        """Test fresh session pattern prevents stale SQLite connection errors."""
        from src.engines.crewai.flow.flow_runner_service import FlowRunnerService

        mock_session = MagicMock()

        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_fes_class:
            mock_fes_class.return_value = MagicMock()
            service = FlowRunnerService(mock_session)

        execution_id = 4
        job_id = "test-job-stale"
        config = {
            "nodes": [{"id": "node1"}],
            "edges": [],
            "flow_config": {"startingPoints": ["node1"]},
            "group_id": "test-group"
        }

        mock_initial_session = MockAsyncSession("initial_session")
        mock_post_session = MockAsyncSession("post_session")
        mock_factory = make_async_session_factory(mock_initial_session, mock_post_session)

        session_tracker = []

        def track_fes(sess):
            mock_svc = MagicMock()
            mock_svc.update_execution_status = AsyncMock()
            session_tracker.append(sess)
            return mock_svc

        with patch('src.engines.crewai.flow.flow_runner_service._smart_db_session', mock_factory), \
             patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService', side_effect=track_fes), \
             patch('src.engines.crewai.flow.flow_runner_service.ApiKeysService') as mock_api_keys, \
             patch('src.engines.crewai.flow.backend_flow.BackendFlow') as mock_backend_flow_class:

            mock_api_keys.get_provider_api_key = AsyncMock(return_value=None)

            # Simulate long-running flow
            mock_flow_instance = MagicMock()

            async def long_running_kickoff():
                await asyncio.sleep(0.01)  # Simulate long execution
                return {"success": True, "result": {"output": "Done"}}

            mock_flow_instance.kickoff = long_running_kickoff
            mock_backend_flow_class.return_value = mock_flow_instance

            result = await service._run_dynamic_flow(execution_id, job_id, config)

            # Key assertion: two sessions created (initial + fresh post-execution)
            assert mock_factory.call_count == 2, "Should create two sessions: initial and post-execution"

            # The post-execution update used the FRESH session, not the original
            assert len(session_tracker) >= 2
            # Last FlowExecutionService was created with the fresh session
            assert session_tracker[-1] is mock_post_session

            assert result["success"] is True


class TestEmitErrorSpan:
    """Test _emit_error_span OTel error emission in FlowRunnerService."""

    def _make_service(self):
        """Create a FlowRunnerService with mocked dependencies."""
        from src.engines.crewai.flow.flow_runner_service import FlowRunnerService
        with patch('src.engines.crewai.flow.flow_runner_service.FlowExecutionService') as mock_fes:
            mock_fes.return_value = MagicMock()
            return FlowRunnerService(MagicMock())

    @pytest.mark.asyncio
    async def test_emit_error_span_basic(self):
        """Test _emit_error_span creates an OTel span with correct attributes."""
        from src.engines.crewai.flow.flow_runner_service import FlowRunnerService

        service = self._make_service()

        mock_provider = MagicMock()
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
        mock_provider.get_tracer.return_value = mock_tracer

        with patch('src.services.otel_tracing.otel_config.create_kasal_tracer_provider', return_value=mock_provider), \
             patch('src.services.otel_tracing.db_exporter.KasalDBSpanExporter'):

            await service._emit_error_span("job-123", "Something failed", group_id="grp-1")

        mock_span.set_attribute.assert_any_call("kasal.event_type", "flow_execution_failed")
        mock_span.set_attribute.assert_any_call("kasal.job_id", "job-123")
        mock_span.set_attribute.assert_any_call("kasal.group_id", "grp-1")
        mock_provider.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_emit_error_span_with_group_email(self):
        """Test _emit_error_span sets group_email attribute when provided."""
        from src.engines.crewai.flow.flow_runner_service import FlowRunnerService

        service = self._make_service()

        mock_provider = MagicMock()
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
        mock_provider.get_tracer.return_value = mock_tracer

        with patch('src.services.otel_tracing.otel_config.create_kasal_tracer_provider', return_value=mock_provider), \
             patch('src.services.otel_tracing.db_exporter.KasalDBSpanExporter'):

            await service._emit_error_span(
                "job-456", "Error msg",
                group_id="grp-2", group_email="user@example.com"
            )

        mock_span.set_attribute.assert_any_call("kasal.group_email", "user@example.com")
        mock_provider.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_emit_error_span_exception_handling(self):
        """Test _emit_error_span catches and logs exceptions without raising."""
        from src.engines.crewai.flow.flow_runner_service import FlowRunnerService

        service = self._make_service()

        with patch('src.services.otel_tracing.otel_config.create_kasal_tracer_provider',
                   side_effect=RuntimeError("OTel init failed")):

            # Should NOT raise — the method catches all exceptions
            await service._emit_error_span("job-789", "Error msg")

    @pytest.mark.asyncio
    async def test_emit_error_span_no_group_context(self):
        """Test _emit_error_span without group_id or group_email."""
        from src.engines.crewai.flow.flow_runner_service import FlowRunnerService

        service = self._make_service()

        mock_provider = MagicMock()
        mock_span = MagicMock()
        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
        mock_provider.get_tracer.return_value = mock_tracer

        with patch('src.services.otel_tracing.otel_config.create_kasal_tracer_provider', return_value=mock_provider), \
             patch('src.services.otel_tracing.db_exporter.KasalDBSpanExporter') as mock_exporter:

            await service._emit_error_span("job-no-group", "Error without group")

        # group_context should be None — KasalDBSpanExporter gets None
        mock_exporter.assert_called_once_with("job-no-group", None)
        # group_id and group_email should NOT be set
        set_attr_calls = [c[0] for c in mock_span.set_attribute.call_args_list]
        assert ("kasal.group_id",) not in [(c[0],) for c in set_attr_calls if len(c) >= 1]
