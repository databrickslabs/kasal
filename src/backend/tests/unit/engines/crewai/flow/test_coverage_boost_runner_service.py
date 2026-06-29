"""
Coverage-boosting tests for FlowRunnerService.

Targets uncovered lines:
 229, 241-242, 265-268, 285-287, 311-325, 348-349, 355-356, 367, 381-383,
 429-431, 436-437, 524-534, 553, 577-617, 642-660, 688, 698, 707, 790,
 794-804, 807-808, 832-881, 914, 922, 959-969, 988, 999-1093
"""

import pytest
import uuid
from unittest.mock import MagicMock, AsyncMock, patch, Mock, call
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from src.engines.crewai.paths.flow.flow_runner_service import FlowRunnerService
from src.schemas.flow_execution import FlowExecutionStatus
from src.engines.crewai.paths.flow.exceptions import FlowPausedForApprovalException


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_service(db=None):
    """Return a FlowRunnerService with all heavy deps patched out."""
    db = db or MagicMock(spec=AsyncSession)
    with patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService"):
        with patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"):
            with patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"):
                with patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"):
                    with patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"):
                        with patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"):
                            return FlowRunnerService(db)


def _make_execution(exec_id=1, flow_id=None):
    ex = MagicMock()
    ex.id = exec_id
    ex.flow_id = flow_id
    ex.status = "PENDING"
    return ex


# ---------------------------------------------------------------------------
# _emit_error_span
# ---------------------------------------------------------------------------

class TestEmitErrorSpan:
    """Tests for _emit_error_span helper (lines 78-120)."""

    @pytest.mark.asyncio
    async def test_emit_error_span_success(self):
        svc = _make_service()
        with patch("src.services.otel_tracing.otel_config.create_kasal_tracer_provider") as mock_prov, \
             patch("src.services.otel_tracing.db_exporter.KasalDBSpanExporter"), \
             patch("opentelemetry.sdk.trace.export.SimpleSpanProcessor"):
            mock_tracer = MagicMock()
            mock_span = MagicMock()
            mock_tracer.start_as_current_span.return_value.__enter__ = lambda s: mock_span
            mock_tracer.start_as_current_span.return_value.__exit__ = MagicMock(return_value=False)
            provider_instance = MagicMock()
            provider_instance.get_tracer.return_value = mock_tracer
            mock_prov.return_value = provider_instance
            # Should not raise
            await svc._emit_error_span("job-1", "some error", group_id="g1")

    @pytest.mark.asyncio
    async def test_emit_error_span_with_group_email(self):
        svc = _make_service()
        with patch("src.services.otel_tracing.otel_config.create_kasal_tracer_provider") as mock_prov, \
             patch("src.services.otel_tracing.db_exporter.KasalDBSpanExporter"), \
             patch("opentelemetry.sdk.trace.export.SimpleSpanProcessor"):
            provider_instance = MagicMock()
            mock_prov.return_value = provider_instance
            await svc._emit_error_span("job-2", "err", group_id="g1", group_email="u@example.com")

    @pytest.mark.asyncio
    async def test_emit_error_span_exception_swallowed(self):
        svc = _make_service()
        with patch("src.services.otel_tracing.otel_config.create_kasal_tracer_provider", side_effect=RuntimeError("oops")):
            # Should not raise - exception is caught internally
            await svc._emit_error_span("job-3", "error msg")


# ---------------------------------------------------------------------------
# create_flow_execution
# ---------------------------------------------------------------------------

class TestCreateFlowExecution:
    """Tests for create_flow_execution (lines ~135-183)."""

    @pytest.mark.asyncio
    async def test_create_flow_execution_success(self):
        svc = _make_service()
        mock_ex = _make_execution(exec_id=99, flow_id=uuid.uuid4())
        svc.flow_execution_service.create_execution = AsyncMock(return_value=mock_ex)

        result = await svc.create_flow_execution(
            flow_id=str(uuid.uuid4()),
            job_id="job-x",
            config={"group_id": "g1"}
        )
        assert result["success"] is True
        assert result["execution_id"] == 99

    @pytest.mark.asyncio
    async def test_create_flow_execution_value_error(self):
        svc = _make_service()
        svc.flow_execution_service.create_execution = AsyncMock(side_effect=ValueError("bad uuid"))

        result = await svc.create_flow_execution(flow_id="not-a-uuid", job_id="job-y")
        assert result["success"] is False
        assert "bad uuid" in result["error"]

    @pytest.mark.asyncio
    async def test_create_flow_execution_generic_error(self):
        svc = _make_service()
        svc.flow_execution_service.create_execution = AsyncMock(side_effect=RuntimeError("db down"))

        result = await svc.create_flow_execution(flow_id=str(uuid.uuid4()), job_id="job-z")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# run_flow – top-level routing
# ---------------------------------------------------------------------------

class TestRunFlow:
    """Tests for run_flow covering uncovered branches."""

    def _service_with_mocks(self):
        svc = _make_service()
        svc.flow_execution_service.create_execution = AsyncMock(return_value=_make_execution(1))
        svc.flow_execution_service.update_execution_status = AsyncMock()
        return svc

    @pytest.mark.asyncio
    async def test_run_flow_invalid_uuid_string(self):
        svc = self._service_with_mocks()
        with pytest.raises(HTTPException) as exc_info:
            await svc.run_flow(flow_id="not-valid-uuid", job_id="job-1")
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_run_flow_flow_id_from_config(self):
        """flow_id is None but config has flow_id – should extract UUID."""
        svc = self._service_with_mocks()
        fid = uuid.uuid4()
        svc.flow_repo.get = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await svc.run_flow(
                flow_id=None,
                job_id="job-2",
                config={"flow_id": str(fid), "group_id": "g"}
            )
        # 404 because the flow wasn't found
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_run_flow_invalid_flow_id_in_config(self):
        """flow_id in config is invalid UUID – should be ignored (warning logged)."""
        svc = self._service_with_mocks()
        # No valid nodes and no valid flow_id → 400
        with pytest.raises(HTTPException) as exc_info:
            await svc.run_flow(
                flow_id=None,
                job_id="job-3",
                config={"flow_id": "bad-uuid", "group_id": "g"}
            )
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_run_flow_no_nodes_flow_not_found(self):
        svc = self._service_with_mocks()
        svc.flow_repo.get = AsyncMock(return_value=None)
        fid = uuid.uuid4()
        with pytest.raises(HTTPException) as exc_info:
            await svc.run_flow(flow_id=fid, job_id="job-4", config={})
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_run_flow_access_denied_by_group(self):
        svc = self._service_with_mocks()
        flow = MagicMock()
        flow.group_id = "group-B"
        flow.nodes = [{"id": "n1"}]
        flow.edges = []
        flow.flow_config = {}
        svc.flow_repo.get = AsyncMock(return_value=flow)

        group_ctx = MagicMock()
        group_ctx.group_ids = ["group-A"]

        fid = uuid.uuid4()
        with pytest.raises(HTTPException) as exc_info:
            await svc.run_flow(
                flow_id=fid,
                job_id="job-5",
                config={"group_context": group_ctx, "group_id": "group-A"}
            )
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_run_flow_no_nodes_db_error(self):
        svc = self._service_with_mocks()
        svc.flow_repo.get = AsyncMock(side_effect=RuntimeError("db error"))
        fid = uuid.uuid4()
        with pytest.raises(HTTPException) as exc_info:
            await svc.run_flow(flow_id=fid, job_id="job-6", config={})
        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_run_flow_resume_execution_not_found(self):
        """resume_from_execution_id set but record not found → 404."""
        svc = self._service_with_mocks()
        svc.db.commit = AsyncMock()

        with patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository") as MockRepo:
            repo_instance = MagicMock()
            # resume_from_execution_id is resolved by job_id first, then int PK fallback.
            repo_instance.get_execution_by_job_id = AsyncMock(return_value=None)
            repo_instance.get_execution_by_id = AsyncMock(return_value=None)
            MockRepo.return_value = repo_instance

            fid = uuid.uuid4()
            with pytest.raises(HTTPException) as exc_info:
                await svc.run_flow(
                    flow_id=None,
                    job_id="job-7",
                    config={
                        "nodes": [{"id": "n1"}],
                        "edges": [],
                        "resume_from_execution_id": 115,
                    }
                )
            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_run_flow_resume_by_uuid_job_id(self):
        """resume_from_execution_id as a UUID job_id (HITL path) resolves by job_id, not int()."""
        svc = self._service_with_mocks()
        svc.db.commit = AsyncMock()

        with patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository") as MockRepo:
            repo_instance = MagicMock()
            existing = MagicMock()
            existing.id = 42
            repo_instance.get_execution_by_job_id = AsyncMock(return_value=existing)
            repo_instance.get_execution_by_id = AsyncMock(return_value=None)
            MockRepo.return_value = repo_instance

            job_uuid = "e089f9fd-d6ea-4565-96ee-f039d5925992"
            svc._run_dynamic_flow = AsyncMock(return_value={"success": True, "result": "ok"})

            result = await svc.run_flow(
                flow_id=None,
                job_id="job-8",
                config={
                    "nodes": [{"id": "n1"}],
                    "edges": [],
                    "resume_from_execution_id": job_uuid,
                },
            )

            # Looked up by job_id (UUID), and int() fallback never attempted.
            repo_instance.get_execution_by_job_id.assert_awaited_once_with(job_uuid)
            repo_instance.get_execution_by_id.assert_not_awaited()
            assert result["status"] == FlowExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_run_flow_hitl_pause_result_returned(self):
        """When flow result has hitl_paused=True, it must be returned as-is."""
        svc = self._service_with_mocks()
        hitl_result = {
            "success": True,
            "hitl_paused": True,
            "approval_id": "approval-1",
            "gate_node_id": "gate-node",
        }

        with patch.object(svc, "_run_dynamic_flow", new=AsyncMock(return_value=hitl_result)):
            result = await svc.run_flow(
                flow_id=None,
                job_id="job-8",
                config={"nodes": [{"id": "n1"}], "edges": []}
            )
        assert result["hitl_paused"] is True
        assert result["approval_id"] == "approval-1"

    @pytest.mark.asyncio
    async def test_run_flow_success_with_flow_uuid(self):
        """Successful run with flow_uuid included in return dict."""
        svc = self._service_with_mocks()
        flow_result = {"success": True, "result": {"data": "ok"}, "flow_uuid": "uuid-xyz"}

        with patch.object(svc, "_run_dynamic_flow", new=AsyncMock(return_value=flow_result)):
            result = await svc.run_flow(
                flow_id=None,
                job_id="job-9",
                config={"nodes": [{"id": "n1"}], "edges": []}
            )
        assert result["status"] == FlowExecutionStatus.COMPLETED
        assert result["flow_uuid"] == "uuid-xyz"

    @pytest.mark.asyncio
    async def test_run_flow_failed_result(self):
        """When flow returns success=False, status should be FAILED."""
        svc = self._service_with_mocks()
        flow_result = {"success": False, "error": "Something broke"}

        with patch.object(svc, "_run_dynamic_flow", new=AsyncMock(return_value=flow_result)):
            result = await svc.run_flow(
                flow_id=None,
                job_id="job-10",
                config={"nodes": [{"id": "n1"}], "edges": []}
            )
        assert result["status"] == FlowExecutionStatus.FAILED
        assert "Something broke" in result["error"]

    @pytest.mark.asyncio
    async def test_run_flow_none_result(self):
        """When flow returns None, error message is set."""
        svc = self._service_with_mocks()
        with patch.object(svc, "_run_dynamic_flow", new=AsyncMock(return_value=None)):
            result = await svc.run_flow(
                flow_id=None,
                job_id="job-11",
                config={"nodes": [{"id": "n1"}], "edges": []}
            )
        assert result["status"] == FlowExecutionStatus.FAILED

    @pytest.mark.asyncio
    async def test_run_flow_existing_flow_dispatched(self):
        """flow_id present → _run_flow_execution should be called."""
        svc = self._service_with_mocks()
        flow_result = {"success": True, "result": "done"}

        with patch.object(svc, "_run_flow_execution", new=AsyncMock(return_value=flow_result)):
            fid = uuid.uuid4()
            result = await svc.run_flow(
                flow_id=fid,
                job_id="job-12",
                config={"nodes": [{"id": "n1"}], "edges": []}
            )
        assert result["status"] == FlowExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_run_flow_unexpected_exception_raises_http500(self):
        svc = self._service_with_mocks()
        svc.flow_execution_service.create_execution = AsyncMock(side_effect=RuntimeError("boom"))
        with pytest.raises(HTTPException) as exc_info:
            await svc.run_flow(
                flow_id=None,
                job_id="job-13",
                config={"nodes": [{"id": "n1"}], "edges": []}
            )
        assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# _get_required_providers
# ---------------------------------------------------------------------------

class TestGetRequiredProviders:
    """Tests for _get_required_providers (lines 662-728)."""

    @pytest.mark.asyncio
    async def test_no_models_returns_empty(self):
        svc = _make_service()
        result = await svc._get_required_providers(MagicMock(), {})
        assert result == []

    @pytest.mark.asyncio
    async def test_single_model_with_provider(self):
        svc = _make_service()
        with patch("src.services.model_config_service.ModelConfigService") as MockSvc:
            instance = MagicMock()
            instance.get_model_config = AsyncMock(return_value={"provider": "openai"})
            MockSvc.return_value = instance
            result = await svc._get_required_providers(
                MagicMock(), {"model": "gpt-4"}
            )
        assert "OPENAI" in result

    @pytest.mark.asyncio
    async def test_crew_config_models_extracted(self):
        svc = _make_service()
        with patch("src.services.model_config_service.ModelConfigService") as MockSvc:
            instance = MagicMock()
            instance.get_model_config = AsyncMock(return_value={"provider": "anthropic"})
            MockSvc.return_value = instance
            config = {
                "crew": {
                    "planning_llm": "claude-3",
                    "reasoning_llm": "claude-2",
                    "manager_llm": "claude-1",
                }
            }
            result = await svc._get_required_providers(MagicMock(), config)
        assert "ANTHROPIC" in result

    @pytest.mark.asyncio
    async def test_top_level_llm_keys(self):
        svc = _make_service()
        with patch("src.services.model_config_service.ModelConfigService") as MockSvc:
            instance = MagicMock()
            instance.get_model_config = AsyncMock(return_value={"provider": "perplexity"})
            MockSvc.return_value = instance
            config = {
                "planning_llm": "pplx-1",
                "reasoning_llm": "pplx-2",
                "manager_llm": "pplx-3",
            }
            result = await svc._get_required_providers(MagicMock(), config)
        assert "PERPLEXITY" in result

    @pytest.mark.asyncio
    async def test_model_config_not_found(self):
        svc = _make_service()
        with patch("src.services.model_config_service.ModelConfigService") as MockSvc:
            instance = MagicMock()
            instance.get_model_config = AsyncMock(return_value=None)
            MockSvc.return_value = instance
            result = await svc._get_required_providers(MagicMock(), {"model": "unknown-model"})
        assert result == []

    @pytest.mark.asyncio
    async def test_model_without_provider_key(self):
        svc = _make_service()
        with patch("src.services.model_config_service.ModelConfigService") as MockSvc:
            instance = MagicMock()
            instance.get_model_config = AsyncMock(return_value={})
            MockSvc.return_value = instance
            result = await svc._get_required_providers(MagicMock(), {"model": "some-model"})
        assert result == []

    @pytest.mark.asyncio
    async def test_model_config_service_raises(self):
        svc = _make_service()
        with patch("src.services.model_config_service.ModelConfigService") as MockSvc:
            instance = MagicMock()
            instance.get_model_config = AsyncMock(side_effect=Exception("service down"))
            MockSvc.return_value = instance
            # Should not raise, just warn
            result = await svc._get_required_providers(MagicMock(), {"model": "some-model"})
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_empty_model_name_skipped(self):
        svc = _make_service()
        with patch("src.services.model_config_service.ModelConfigService") as MockSvc:
            instance = MagicMock()
            instance.get_model_config = AsyncMock()
            MockSvc.return_value = instance
            result = await svc._get_required_providers(MagicMock(), {"model": ""})
        # Empty model name is skipped
        instance.get_model_config.assert_not_called()


# ---------------------------------------------------------------------------
# get_flow_execution
# ---------------------------------------------------------------------------

class TestGetFlowExecution:
    """Tests for get_flow_execution (lines 1095-1152)."""

    @pytest.mark.asyncio
    async def test_execution_found(self):
        svc = _make_service()
        ex = _make_execution(5)
        ex.flow_id = uuid.uuid4()
        ex.job_id = "job-abc"
        ex.result = {"data": "ok"}
        ex.error = None
        ex.created_at = datetime.now()
        ex.updated_at = datetime.now()
        ex.completed_at = None

        svc.flow_execution_service.get_execution = AsyncMock(return_value=ex)
        svc.flow_execution_service.get_node_executions = AsyncMock(return_value=[])

        result = await svc.get_flow_execution(5)
        assert result["success"] is True
        assert result["execution"]["id"] == 5

    @pytest.mark.asyncio
    async def test_execution_not_found(self):
        svc = _make_service()
        svc.flow_execution_service.get_execution = AsyncMock(return_value=None)
        result = await svc.get_flow_execution(999)
        assert result["success"] is False
        assert "999" in result["error"]

    @pytest.mark.asyncio
    async def test_execution_service_raises(self):
        svc = _make_service()
        svc.flow_execution_service.get_execution = AsyncMock(side_effect=RuntimeError("db err"))
        result = await svc.get_flow_execution(1)
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_execution_with_nodes(self):
        svc = _make_service()
        ex = _make_execution(6)
        ex.flow_id = uuid.uuid4()
        ex.job_id = "j"
        ex.result = {}
        ex.error = None
        ex.created_at = datetime.now()
        ex.updated_at = datetime.now()
        ex.completed_at = None

        node = MagicMock()
        node.id = 10
        node.node_id = "n1"
        node.status = "completed"
        node.agent_id = None
        node.task_id = None
        node.result = {}
        node.error = None
        node.created_at = datetime.now()
        node.updated_at = datetime.now()
        node.completed_at = None

        svc.flow_execution_service.get_execution = AsyncMock(return_value=ex)
        svc.flow_execution_service.get_node_executions = AsyncMock(return_value=[node])

        result = await svc.get_flow_execution(6)
        assert result["success"] is True
        assert len(result["execution"]["nodes"]) == 1


# ---------------------------------------------------------------------------
# get_flow_executions_by_flow
# ---------------------------------------------------------------------------

class TestGetFlowExecutionsByFlow:
    """Tests for get_flow_executions_by_flow (lines 1154-1187)."""

    @pytest.mark.asyncio
    async def test_returns_executions(self):
        svc = _make_service()
        fid = uuid.uuid4()
        ex = MagicMock()
        ex.id = 1
        ex.job_id = "j1"
        ex.status = "COMPLETED"
        ex.created_at = datetime.now()
        ex.completed_at = None

        svc.flow_execution_service.get_executions_by_flow = AsyncMock(return_value=[ex])
        result = await svc.get_flow_executions_by_flow(fid)
        assert result["success"] is True
        assert len(result["executions"]) == 1

    @pytest.mark.asyncio
    async def test_service_raises(self):
        svc = _make_service()
        svc.flow_execution_service.get_executions_by_flow = AsyncMock(side_effect=RuntimeError("err"))
        result = await svc.get_flow_executions_by_flow(uuid.uuid4())
        assert result["success"] is False


# ---------------------------------------------------------------------------
# _run_dynamic_flow – internal helper
# ---------------------------------------------------------------------------

def _make_safe_session_patch(mock_session=None):
    """Create a proper async context manager for patching _safe_session staticmethod."""
    from contextlib import asynccontextmanager

    if mock_session is None:
        mock_session = MagicMock(spec=AsyncSession)

    @staticmethod
    @asynccontextmanager
    async def _safe_ctx():
        yield mock_session

    return _safe_ctx


def _make_smart_session_patch(mock_session=None):
    """Create a proper async context manager for patching _smart_db_session."""
    from contextlib import asynccontextmanager

    if mock_session is None:
        mock_session = MagicMock(spec=AsyncSession)

    @asynccontextmanager
    async def _smart_ctx():
        yield mock_session

    return _smart_ctx


class TestRunDynamicFlow:
    """Tests for _run_dynamic_flow internals."""

    @pytest.mark.asyncio
    async def test_dynamic_flow_success(self):
        svc = _make_service()
        kickoff_result = {"success": True, "result": {"text": "done"}, "flow_uuid": "u1"}

        mock_session = MagicMock(spec=AsyncSession)

        with patch.object(FlowRunnerService, "_safe_session", new=_make_safe_session_patch(mock_session)), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.backend_flow.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_make_smart_session_patch(mock_session)):

            flow_svc_instance = MagicMock()
            flow_svc_instance.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc_instance

            bf_instance = MagicMock()
            bf_instance.kickoff = AsyncMock(return_value=kickoff_result)
            bf_instance.config = {}
            bf_instance._flow_data = None
            MockBF.return_value = bf_instance

            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)

            with patch("src.services.execution_history_service.ExecutionHistoryService") as MockHist:
                hist_instance = MagicMock()
                hist_instance.set_checkpoint_active = AsyncMock()
                MockHist.return_value = hist_instance

                result = await svc._run_dynamic_flow(
                    execution_id=1,
                    job_id="job-dyn",
                    config={
                        "nodes": [{"id": "n1"}],
                        "edges": [],
                        "flow_config": {"startingPoints": []},
                    }
                )

            assert result.get("success") is True

    @pytest.mark.asyncio
    async def test_dynamic_flow_kickoff_failure(self):
        svc = _make_service()
        kickoff_result = {"success": False, "error": "crew failed"}
        mock_session = MagicMock(spec=AsyncSession)

        with patch.object(FlowRunnerService, "_safe_session", new=_make_safe_session_patch(mock_session)), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.backend_flow.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_make_smart_session_patch(mock_session)), \
             patch.object(svc, "_emit_error_span", new=AsyncMock()):

            flow_svc_instance = MagicMock()
            flow_svc_instance.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc_instance

            bf_instance = MagicMock()
            bf_instance.kickoff = AsyncMock(return_value=kickoff_result)
            bf_instance.config = {}
            MockBF.return_value = bf_instance

            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)

            result = await svc._run_dynamic_flow(
                execution_id=1,
                job_id="job-fail",
                config={"nodes": [{"id": "n1"}], "edges": [], "flow_config": {}}
            )
            assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_dynamic_flow_hitl_pause(self):
        svc = _make_service()
        mock_session = MagicMock(spec=AsyncSession)

        with patch.object(FlowRunnerService, "_safe_session", new=_make_safe_session_patch(mock_session)), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.backend_flow.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_make_smart_session_patch(mock_session)):

            pause_exc = FlowPausedForApprovalException(
                approval_id="appr-1",
                gate_node_id="gate-x",
                message="Please approve",
                execution_id="job-hitl",
                crew_sequence=0,
                flow_uuid="fv-1"
            )

            bf_instance = MagicMock()
            bf_instance.kickoff = AsyncMock(side_effect=pause_exc)
            bf_instance.config = {}
            MockBF.return_value = bf_instance

            flow_svc_instance = MagicMock()
            flow_svc_instance.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc_instance

            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)

            with patch("src.services.execution_history_service.ExecutionHistoryService") as MockHist:
                hist_instance = MagicMock()
                hist_instance.set_checkpoint_active = AsyncMock()
                MockHist.return_value = hist_instance

                result = await svc._run_dynamic_flow(
                    execution_id=1,
                    job_id="job-hitl",
                    config={"nodes": [{"id": "n1"}], "edges": [], "flow_config": {}}
                )

            assert result.get("hitl_paused") is True
            assert result.get("approval_id") == "appr-1"

    @pytest.mark.asyncio
    async def test_dynamic_flow_kickoff_exception(self):
        svc = _make_service()
        mock_session = MagicMock(spec=AsyncSession)

        with patch.object(FlowRunnerService, "_safe_session", new=_make_safe_session_patch(mock_session)), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.backend_flow.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_make_smart_session_patch(mock_session)), \
             patch.object(svc, "_emit_error_span", new=AsyncMock()):

            bf_instance = MagicMock()
            bf_instance.kickoff = AsyncMock(side_effect=RuntimeError("crew exploded"))
            bf_instance.config = {}
            MockBF.return_value = bf_instance

            flow_svc_instance = MagicMock()
            flow_svc_instance.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc_instance

            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)

            result = await svc._run_dynamic_flow(
                execution_id=1,
                job_id="job-explode",
                config={"nodes": [{"id": "n1"}], "edges": [], "flow_config": {}}
            )
            assert result.get("success") is False
            assert "crew exploded" in result.get("error", "")


# ---------------------------------------------------------------------------
# _run_flow_execution – result type conversion branches
# ---------------------------------------------------------------------------

class TestRunFlowExecutionResultConversion:
    """Exercises the non-dict result conversion branches in _run_flow_execution."""

    def _build_patches(self, kickoff_result):
        """Return all patches needed for _run_flow_execution."""
        from contextlib import asynccontextmanager

        mock_session = MagicMock(spec=AsyncSession)

        @asynccontextmanager
        async def _safe_ctx():
            yield mock_session

        @asynccontextmanager
        async def _smart_ctx():
            yield mock_session

        patches = {
            "safe_session": ("src.engines.crewai.paths.flow.flow_runner_service.FlowRunnerService._safe_session", _safe_ctx()),
            "smart_db": ("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", _smart_ctx()),
        }
        return patches, mock_session, kickoff_result

    @pytest.mark.asyncio
    async def test_run_flow_execution_invalid_uuid_string(self):
        svc = _make_service()
        mock_session = MagicMock(spec=AsyncSession)

        with patch.object(FlowRunnerService, "_safe_session", new=_make_safe_session_patch(mock_session)), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"):

            flow_svc_instance = MagicMock()
            flow_svc_instance.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc_instance

            result = await svc._run_flow_execution(
                execution_id=1,
                flow_id="not-a-uuid",
                job_id="job-bad-id",
                config={}
            )
            assert result["success"] is False
            assert "Invalid UUID" in result["error"]

    @pytest.mark.asyncio
    async def test_run_flow_execution_string_result_conversion(self):
        """Result is a plain string, should be wrapped in dict with 'content' key."""
        svc = _make_service()
        kickoff_result = {"success": True, "result": "plain string output", "flow_uuid": None}
        mock_session = MagicMock(spec=AsyncSession)

        with patch.object(FlowRunnerService, "_safe_session", new=_make_safe_session_patch(mock_session)), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.backend_flow.BackendFlow", MockBF), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_make_smart_session_patch(mock_session)), \
             patch.object(svc, "_emit_error_span", new=AsyncMock()), \
             patch("src.services.model_config_service.ModelConfigService") as MockModelSvc:

            flow_svc_instance = MagicMock()
            flow_svc_instance.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc_instance

            bf_instance = MagicMock()
            bf_instance.kickoff = AsyncMock(return_value=kickoff_result)
            bf_instance.config = {}
            MockBF.return_value = bf_instance

            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)

            model_svc_inst = MagicMock()
            model_svc_inst.get_model_config = AsyncMock(return_value=None)
            MockModelSvc.return_value = model_svc_inst

            fid = uuid.uuid4()
            result = await svc._run_flow_execution(
                execution_id=1,
                flow_id=fid,
                job_id="job-str-res",
                config={"nodes": [{"id": "n1"}]}
            )
            assert result.get("success") is True
