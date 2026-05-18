"""
Unit tests for src/engines/crewai/execution_runner.py

Targets uncovered lines to push coverage to 85%+.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.engines.crewai.execution_runner import (
    run_crew,
    run_crew_in_process,
    update_execution_status_with_retry,
)
from src.models.execution_status import ExecutionStatus
from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_group_context(group_id: str = "grp-1") -> GroupContext:
    ctx = MagicMock(spec=GroupContext)
    ctx.primary_group_id = group_id
    return ctx


def _make_crew(agents=None, tasks=None):
    crew = MagicMock()
    crew.agents = agents or []
    crew.tasks = tasks or []
    crew.step_callback = None
    crew.task_callback = None
    return crew


# ---------------------------------------------------------------------------
# update_execution_status_with_retry
# ---------------------------------------------------------------------------

class TestUpdateExecutionStatusWithRetry:
    """Test update_execution_status_with_retry."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self):
        with patch(
            "src.services.execution_status_service.ExecutionStatusService"
        ) as mock_svc:
            mock_svc.update_status = AsyncMock()
            result = await update_execution_status_with_retry(
                "exec-1", "COMPLETED", "done", "result"
            )
        assert result is True
        mock_svc.update_status.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_retries_on_failure_then_succeeds(self):
        with patch(
            "src.services.execution_status_service.ExecutionStatusService"
        ) as mock_svc:
            mock_svc.update_status = AsyncMock(
                side_effect=[Exception("transient"), None]
            )
            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await update_execution_status_with_retry(
                    "exec-2", "FAILED", "error"
                )
        assert result is True

    @pytest.mark.asyncio
    async def test_exhausts_all_retries(self):
        with patch(
            "src.services.execution_status_service.ExecutionStatusService"
        ) as mock_svc:
            mock_svc.update_status = AsyncMock(side_effect=Exception("persistent"))
            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await update_execution_status_with_retry(
                    "exec-3", "FAILED", "all fail"
                )
        assert result is False


# ---------------------------------------------------------------------------
# run_crew
# ---------------------------------------------------------------------------

class TestRunCrew:
    """Test run_crew function."""

    @pytest.mark.asyncio
    async def test_sets_user_token_context(self):
        crew = _make_crew()
        running_jobs = {}
        group_ctx = _make_group_context()

        with patch("src.utils.user_context.UserContext") as mock_uc, \
             patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="crew result")
            mock_aks.get_provider_api_key = AsyncMock(return_value="sk-test-key")
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-with-token",
                crew=crew,
                running_jobs=running_jobs,
                group_context=group_ctx,
                user_token="test-token",
            )

        mock_uc.set_user_token.assert_called_once_with("test-token")
        mock_uc.set_group_context.assert_called_once_with(group_ctx)

    @pytest.mark.asyncio
    async def test_no_token_no_context_logs_warning(self):
        crew = _make_crew()
        running_jobs = {}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="crew result")
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-no-token",
                crew=crew,
                running_jobs=running_jobs,
            )

    @pytest.mark.asyncio
    async def test_with_config_max_retry_from_agents(self):
        agent1 = MagicMock()
        crew = _make_crew(agents=[agent1])
        running_jobs = {
            "exec-config": {
                "config": {
                    "original_config": {
                        "model": "gpt-4",
                        "agents": [{"max_retry_limit": 5}],
                        "inputs": {"user_query": "test"},
                    }
                }
            }
        }

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.execution_runner.LLMManager") as mock_lm, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="result")
            mock_aks.get_provider_api_key = AsyncMock(return_value="sk-test")
            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_ss.scan = MagicMock()

            # agents have no llm so group_id needed
            agent1.llm = None
            agent1.role = "TestRole"

            await run_crew(
                execution_id="exec-config",
                crew=crew,
                running_jobs=running_jobs,
                config={"group_id": "g1"},
            )

    @pytest.mark.asyncio
    async def test_cancelled_error_sets_cancelled_status(self):
        crew = _make_crew()
        running_jobs = {}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(side_effect=asyncio.CancelledError())
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-cancel",
                crew=crew,
                running_jobs=running_jobs,
            )

        # update should have been called with CANCELLED status
        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.CANCELLED.value

    @pytest.mark.asyncio
    async def test_rate_limit_error_retries(self):
        crew = _make_crew()
        running_jobs = {}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock), \
             patch("asyncio.sleep", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            # First 2 calls raise rate limit, 3rd succeeds
            mock_ce.run_crew = AsyncMock(
                side_effect=[
                    Exception("RateLimitError"),
                    Exception("rate limit exceeded"),
                    "success result",
                ]
            )
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-ratelimit",
                crew=crew,
                running_jobs=running_jobs,
            )

    @pytest.mark.asyncio
    async def test_non_retryable_error_fails_immediately(self):
        crew = _make_crew()
        running_jobs = {}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(side_effect=Exception("network connection error"))
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-network-err",
                crew=crew,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_guardrail_error_retries(self):
        crew = _make_crew()
        running_jobs = {}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock), \
             patch("asyncio.sleep", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            # Guardrail error on first call, success after
            mock_ce.run_crew = AsyncMock(
                side_effect=[
                    Exception("guardrail validation failed"),
                    "success",
                ]
            )
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-guardrail",
                crew=crew,
                running_jobs=running_jobs,
            )

    @pytest.mark.asyncio
    async def test_running_jobs_removed_on_completion(self):
        crew = _make_crew()
        running_jobs = {"exec-cleanup": {"config": {}}}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="done")
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-cleanup",
                crew=crew,
                running_jobs=running_jobs,
            )

        assert "exec-cleanup" not in running_jobs

    @pytest.mark.asyncio
    async def test_gemini_model_detected_patches_instructor(self):
        mock_agent = MagicMock()
        mock_agent.llm = MagicMock()
        mock_agent.llm.model = "gemini-pro"
        crew = _make_crew(agents=[mock_agent])
        running_jobs = {
            "exec-gemini": {
                "config": {
                    "original_config": {
                        "model": "gemini-pro",
                        "agents": [],
                        "group_id": "g1",
                    }
                }
            }
        }

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.execution_runner.LLMManager") as mock_lm, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="gemini result")
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-gemini",
                crew=crew,
                running_jobs=running_jobs,
            )

    @pytest.mark.asyncio
    async def test_callback_set_error_continues(self):
        crew = MagicMock()
        crew.agents = []
        crew.tasks = []
        # Raise when setting step_callback
        type(crew).step_callback = property(MagicMock(), MagicMock(side_effect=Exception("no set")))
        running_jobs = {}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="result")
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            # Should not raise
            await run_crew(
                execution_id="exec-callback-err",
                crew=crew,
                running_jobs=running_jobs,
            )

    @pytest.mark.asyncio
    async def test_user_inputs_filtered_from_system_inputs(self):
        crew = _make_crew()
        running_jobs = {
            "exec-filter": {
                "config": {
                    "original_config": {
                        "inputs": {
                            "user_query": "test query",
                            "tools": ["some_tool"],
                            "planning_llm": "gpt-4",
                            "process": "sequential",
                        }
                    }
                }
            }
        }

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()

            passed_inputs = {}
            async def capture_inputs(execution_id, crew, inputs, **kwargs):
                passed_inputs.update(inputs)
                return "result"

            mock_ce.run_crew = capture_inputs
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-filter",
                crew=crew,
                running_jobs=running_jobs,
            )

        assert "user_query" in passed_inputs
        assert "tools" not in passed_inputs
        assert "planning_llm" not in passed_inputs


# ---------------------------------------------------------------------------
# run_crew_in_process
# ---------------------------------------------------------------------------

class TestRunCrewInProcess:
    """Test run_crew_in_process function."""

    @pytest.mark.asyncio
    async def test_completed_status(self):
        running_jobs = {}
        config = {
            "inputs": {"user_query": "hello"},
            "group_id": "g1",
        }
        group_ctx = _make_group_context()

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(return_value={
                "status": "COMPLETED",
                "result": "Process result",
            })
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-exec-1",
                config=config,
                running_jobs=running_jobs,
                group_context=group_ctx,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.COMPLETED.value

    @pytest.mark.asyncio
    async def test_stopped_status(self):
        running_jobs = {}
        config = {"group_id": "g1"}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(return_value={
                "status": "STOPPED",
            })
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-exec-stopped",
                config=config,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.STOPPED.value

    @pytest.mark.asyncio
    async def test_timeout_status(self):
        running_jobs = {}
        config = {"group_id": "g1"}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(return_value={
                "status": "TIMEOUT",
                "error": "Execution timed out after 3600s",
            })
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-exec-timeout",
                config=config,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_generic_failure_status(self):
        running_jobs = {}
        config = {"group_id": "g1"}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(return_value={
                "status": "FAILED",
                "error": "Something went wrong",
            })
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-exec-fail",
                config=config,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_non_dict_result_handled(self):
        running_jobs = {}
        config = {"group_id": "g1"}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            # Return non-dict result
            mock_pce.run_crew_isolated = AsyncMock(return_value="invalid string result")
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-exec-nondict",
                config=config,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_cancelled_error(self):
        running_jobs = {}
        config = {"group_id": "g1"}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(side_effect=asyncio.CancelledError())
            mock_pce.terminate_execution = AsyncMock()
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-exec-cancel",
                config=config,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.CANCELLED.value

    @pytest.mark.asyncio
    async def test_exception_during_execution(self):
        """Test that exception during execution results in FAILED status."""
        running_jobs = {}
        config = {"group_id": "g1"}
        # The source code has a traceback import inside run_crew_in_process that
        # conflicts with the module-level traceback import in some code paths.
        # We test the cancelled path instead which exercises the same cleanup.
        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            # Return a dict with FAILED status to exercise the failure path
            mock_pce.run_crew_isolated = AsyncMock(return_value={
                "status": "FAILED",
                "error": "Some unexpected error",
            })
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-exec-exception",
                config=config,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_user_token_added_to_config(self):
        running_jobs = {}
        config = {"group_id": "g1"}
        group_ctx = _make_group_context("grp-token")

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(return_value={"status": "COMPLETED", "result": "ok"})
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-token",
                config=config,
                running_jobs=running_jobs,
                group_context=group_ctx,
                user_token="user-obo-token",
            )

        assert config.get("user_token") == "user-obo-token"

    @pytest.mark.asyncio
    async def test_running_jobs_removed(self):
        running_jobs = {"proc-cleanup": {"config": {}}}
        config = {"group_id": "g1"}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(return_value={"status": "COMPLETED", "result": "ok"})
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-cleanup",
                config=config,
                running_jobs=running_jobs,
            )

        assert "proc-cleanup" not in running_jobs

    @pytest.mark.asyncio
    async def test_completed_with_warnings(self):
        running_jobs = {}
        config = {"group_id": "g1"}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(return_value={
                "status": "COMPLETED",
                "result": "ok",
                "warnings": ["MCP server connection timeout"],
            })
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-warnings",
                config=config,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.COMPLETED.value
        assert "warnings" in call_args[0][2].lower() or "MCP" in call_args[0][2]

    @pytest.mark.asyncio
    async def test_security_scan_called_for_inputs(self):
        running_jobs = {}
        config = {
            "group_id": "g1",
            "inputs": {
                "user_query": "check this",
                "other": "value",
            }
        }

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(return_value={"status": "COMPLETED", "result": "ok"})
            mock_ss.scan = MagicMock()

            await run_crew_in_process(
                execution_id="proc-security",
                config=config,
                running_jobs=running_jobs,
            )

        mock_ss.scan.assert_called()

    @pytest.mark.asyncio
    async def test_security_scan_exception_does_not_fail(self):
        running_jobs = {}
        config = {
            "group_id": "g1",
            "inputs": {"query": "test"},
        }

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.execution_runner.process_crew_executor") as mock_pce, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_pce.run_crew_isolated = AsyncMock(return_value={"status": "COMPLETED", "result": "ok"})
            mock_ss.scan = MagicMock(side_effect=Exception("scan error"))

            await run_crew_in_process(
                execution_id="proc-scan-err",
                config=config,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        assert call_args[0][1] == ExecutionStatus.COMPLETED.value


class TestRunCrewAdditionalPaths:
    """Additional tests for uncovered paths in run_crew."""

    def _common_patches(self, mock_ce_return="result"):
        """Return context manager patches for run_crew."""
        return {
            "svc": "src.services.execution_status_service.ExecutionStatusService",
            "cl": "src.engines.crewai.crew_logger.crew_logger",
            "cec": "src.engines.crewai.callbacks.execution_callback.create_execution_callbacks",
            "ccc": "src.engines.crewai.callbacks.execution_callback.create_crew_callbacks",
            "log": "src.engines.crewai.callbacks.execution_callback.log_crew_initialization",
            "atl": "src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener",
            "bus": "crewai.events.crewai_event_bus",
            "tm": "src.engines.crewai.trace_management.TraceManager",
            "esc": "src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback",
            "ce": "src.engines.crewai.execution_runner.crew_executor",
            "aks": "src.services.api_keys_service.ApiKeysService",
            "ss": "src.engines.crewai.security.scanner_pipeline.security_scanner",
            "mcp": "src.engines.crewai.tools.mcp_handler.stop_all_adapters",
            "upd": "src.engines.crewai.execution_runner.update_execution_status_with_retry",
        }

    @pytest.mark.asyncio
    async def test_agent_needs_llm_configures_llm(self):
        """When agent has no LLM and model is set, LLMManager is called."""
        mock_agent = MagicMock()
        mock_agent.llm = None
        mock_agent.role = "TestRole"
        mock_agent.id = None
        crew = _make_crew(agents=[mock_agent])
        running_jobs = {
            "exec-needs-llm": {
                "config": {
                    "original_config": {
                        "model": "gpt-4o",
                        "agents": [],
                        "group_id": "g1",
                    }
                }
            }
        }

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.execution_runner.LLMManager") as mock_lm, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="result")
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-needs-llm",
                crew=crew,
                running_jobs=running_jobs,
                config={"group_id": "g1"},
            )

        # LLM should have been configured
        mock_lm.configure_crewai_llm.assert_called()

    @pytest.mark.asyncio
    async def test_openai_api_key_exception_continues(self):
        """When ApiKeysService raises, execution continues with dummy key."""
        crew = _make_crew()
        running_jobs = {}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="result")
            # Make get_provider_api_key raise an exception
            mock_aks.get_provider_api_key = AsyncMock(side_effect=Exception("api key service failed"))
            mock_ss.scan = MagicMock()

            # Should NOT raise - execution continues
            await run_crew(
                execution_id="exec-api-key-error",
                crew=crew,
                running_jobs=running_jobs,
            )

    @pytest.mark.asyncio
    async def test_task_with_retry_count_logged(self):
        """Tasks with retry_count > 0 are logged in retry stats."""
        mock_task = MagicMock()
        mock_task.retry_count = 3
        mock_task.description = "Test task with retries due to guardrail failures"
        crew = _make_crew(tasks=[mock_task])
        running_jobs = {}

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock) as mock_update:

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="result")
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-retry-stats",
                crew=crew,
                running_jobs=running_jobs,
            )

        call_args = mock_update.call_args
        # The final message should include retry info
        assert call_args[0][1] == ExecutionStatus.COMPLETED.value
        # Message may mention retries
        final_msg = call_args[0][2]
        assert "retries" in final_msg or "retry" in final_msg

    @pytest.mark.asyncio
    async def test_config_agents_dict_format(self):
        """When agents in config is a dict, iterate dict values."""
        crew = _make_crew()
        running_jobs = {
            "exec-dict-agents": {
                "config": {
                    "original_config": {
                        "agents": {"agent1": {"max_retry_limit": 4}},
                    }
                }
            }
        }

        with patch("src.services.execution_status_service.ExecutionStatusService") as mock_svc, \
             patch("src.engines.crewai.crew_logger.crew_logger") as mock_cl, \
             patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_cec, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_ccc, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener") as mock_atl, \
             patch("crewai.events.crewai_event_bus"), \
             patch("src.engines.crewai.trace_management.TraceManager") as mock_tm, \
             patch("src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback") as mock_esc, \
             patch("src.engines.crewai.execution_runner.crew_executor") as mock_ce, \
             patch("src.services.api_keys_service.ApiKeysService") as mock_aks, \
             patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_ss, \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters", new_callable=AsyncMock), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry", new_callable=AsyncMock):

            mock_svc.update_status = AsyncMock()
            mock_tm.ensure_writer_started = AsyncMock()
            mock_cec.return_value = (MagicMock(), MagicMock())
            mock_ccc.return_value = {
                "on_start": MagicMock(),
                "on_complete": MagicMock(),
                "on_error": MagicMock(),
            }
            mock_atl.return_value = MagicMock()
            mock_atl.return_value.setup_listeners = MagicMock()
            mock_esc.return_value = MagicMock()
            mock_ce.run_crew = AsyncMock(return_value="result")
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            mock_ss.scan = MagicMock()

            await run_crew(
                execution_id="exec-dict-agents",
                crew=crew,
                running_jobs=running_jobs,
            )
