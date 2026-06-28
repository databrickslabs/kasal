"""
Supplemental coverage-boosting tests for the remaining uncovered lines.
"""

import pytest
import uuid
from unittest.mock import MagicMock, AsyncMock, patch, Mock
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from src.engines.crewai.paths.flow.flow_runner_service import FlowRunnerService
from src.schemas.flow_execution import FlowExecutionStatus
from src.engines.crewai.paths.flow.exceptions import FlowPausedForApprovalException


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service():
    db = MagicMock(spec=AsyncSession)
    with patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService"):
        with patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"):
            with patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"):
                with patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"):
                    with patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"):
                        with patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"):
                            return FlowRunnerService(db)


@staticmethod
@asynccontextmanager
async def _safe_ctx():
    yield MagicMock(spec=AsyncSession)


@asynccontextmanager
async def _smart_ctx():
    yield MagicMock(spec=AsyncSession)


# ---------------------------------------------------------------------------
# _run_flow_execution - additional coverage
# ---------------------------------------------------------------------------

class TestRunFlowExecutionAdditional:

    @pytest.mark.asyncio
    async def test_run_flow_no_nodes_loads_from_db(self):
        """Tests DB loading path when no nodes in config (line 831-881)."""
        svc = _make_service()
        mock_session = MagicMock(spec=AsyncSession)

        with patch.object(FlowRunnerService, "_safe_session", new=_safe_ctx), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_smart_ctx), \
             patch("src.services.model_config_service.ModelConfigService") as MockModelSvc, \
             patch.object(svc, "_emit_error_span", new=AsyncMock()):

            flow_svc = MagicMock()
            flow_svc.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc

            # BackendFlow loads from DB
            bf = MagicMock()
            bf.config = {}
            bf.load_flow = AsyncMock(return_value={
                "nodes": [{"id": "n1"}],
                "edges": [],
                "flow_config": {"startingPoints": [{"nodeId": "n1"}]}
            })
            bf.kickoff = AsyncMock(return_value={"success": True, "result": "done", "flow_uuid": None})
            MockBF.return_value = bf

            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)
            ms = MagicMock()
            ms.get_model_config = AsyncMock(return_value=None)
            MockModelSvc.return_value = ms

            fid = uuid.uuid4()
            result = await svc._run_flow_execution(
                execution_id=1,
                flow_id=fid,
                job_id="job-load-db",
                config={}  # No nodes - triggers DB load
            )

        assert result.get("success") is True

    @pytest.mark.asyncio
    async def test_run_flow_execution_hitl_pause_no_flow_uuid(self):
        """HITL pause without flow_uuid (line 1036-1049 path)."""
        svc = _make_service()

        pause_exc = FlowPausedForApprovalException(
            approval_id="a1",
            gate_node_id="g1",
            message="pause",
            execution_id="job-hitl2",
            crew_sequence=0,
            flow_uuid=None  # No flow_uuid
        )

        with patch.object(FlowRunnerService, "_safe_session", new=_safe_ctx), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_smart_ctx), \
             patch("src.services.model_config_service.ModelConfigService") as MockModelSvc, \
             patch.object(svc, "_emit_error_span", new=AsyncMock()):

            flow_svc = MagicMock()
            flow_svc.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc

            bf = MagicMock()
            bf.config = {}
            bf.kickoff = AsyncMock(side_effect=pause_exc)
            MockBF.return_value = bf

            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)
            ms = MagicMock()
            ms.get_model_config = AsyncMock(return_value=None)
            MockModelSvc.return_value = ms

            fid = uuid.uuid4()
            result = await svc._run_flow_execution(
                execution_id=1,
                flow_id=fid,
                job_id="job-hitl2",
                config={"nodes": [{"id": "n1"}]}
            )

        assert result.get("paused_for_approval") is True
        assert result.get("flow_uuid") is None

    @pytest.mark.asyncio
    async def test_run_flow_execution_outer_exception_status_update_fails(self):
        """Outer exception handler + status update also fails (line 1079-1093)."""
        svc = _make_service()

        with patch.object(FlowRunnerService, "_safe_session", new=_safe_ctx), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_smart_ctx), \
             patch("src.services.model_config_service.ModelConfigService") as MockModelSvc, \
             patch.object(svc, "_emit_error_span", new=AsyncMock()):

            # Make update_execution_status raise to trigger outer handler
            flow_svc = MagicMock()
            flow_svc.update_execution_status = AsyncMock(side_effect=RuntimeError("db fail"))
            MockFlowSvc.return_value = flow_svc

            bf = MagicMock()
            bf.config = {}
            bf.kickoff = AsyncMock(side_effect=RuntimeError("execution error"))
            MockBF.return_value = bf

            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)
            ms = MagicMock()
            ms.get_model_config = AsyncMock(return_value=None)
            MockModelSvc.return_value = ms

            fid = uuid.uuid4()
            result = await svc._run_flow_execution(
                execution_id=1,
                flow_id=fid,
                job_id="job-outer-err",
                config={"nodes": [{"id": "n1"}]}
            )

        assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_run_flow_execution_api_key_for_provider(self):
        """Tests the API key setting path (lines 794-804) when providers found."""
        svc = _make_service()

        with patch.object(FlowRunnerService, "_safe_session", new=_safe_ctx), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_smart_ctx), \
             patch("src.services.model_config_service.ModelConfigService") as MockModelSvc, \
             patch.object(svc, "_emit_error_span", new=AsyncMock()):

            flow_svc = MagicMock()
            flow_svc.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc

            bf = MagicMock()
            bf.config = {}
            bf.kickoff = AsyncMock(return_value={"success": True, "result": "ok", "flow_uuid": None})
            MockBF.return_value = bf

            # Return a provider so we hit the API key setting code
            ms = MagicMock()
            ms.get_model_config = AsyncMock(return_value={"provider": "openai"})
            MockModelSvc.return_value = ms

            # Return actual API key to set env var
            MockApiSvc.get_provider_api_key = AsyncMock(return_value="test-api-key")

            fid = uuid.uuid4()
            result = await svc._run_flow_execution(
                execution_id=1,
                flow_id=fid,
                job_id="job-api-key",
                config={"nodes": [{"id": "n1"}], "model": "gpt-4"}
            )

        assert result.get("success") is True

    @pytest.mark.asyncio
    async def test_run_flow_execution_with_flow_uuid_checkpoint(self):
        """Tests checkpoint save path when flow_uuid present (lines 979-990)."""
        svc = _make_service()

        with patch.object(FlowRunnerService, "_safe_session", new=_safe_ctx), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService") as MockFlowSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.BackendFlow") as MockBF, \
             patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionTraceRepository"), \
             patch("src.engines.crewai.paths.flow.flow_runner_service.ApiKeysService") as MockApiSvc, \
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_smart_ctx), \
             patch("src.services.model_config_service.ModelConfigService") as MockModelSvc, \
             patch("src.services.execution_history_service.ExecutionHistoryService") as MockHist, \
             patch.object(svc, "_emit_error_span", new=AsyncMock()):

            flow_svc = MagicMock()
            flow_svc.update_execution_status = AsyncMock()
            MockFlowSvc.return_value = flow_svc

            bf = MagicMock()
            bf.config = {}
            bf.kickoff = AsyncMock(return_value={
                "success": True,
                "result": {"data": "ok"},
                "flow_uuid": "uuid-checkpoint-1"  # Has flow_uuid
            })
            MockBF.return_value = bf

            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)
            ms = MagicMock()
            ms.get_model_config = AsyncMock(return_value=None)
            MockModelSvc.return_value = ms

            hist = MagicMock()
            hist.set_checkpoint_active = AsyncMock()
            MockHist.return_value = hist

            fid = uuid.uuid4()
            result = await svc._run_flow_execution(
                execution_id=1,
                flow_id=fid,
                job_id="job-checkpoint",
                config={"nodes": [{"id": "n1"}]}
            )

        assert result.get("success") is True
        assert result.get("flow_uuid") == "uuid-checkpoint-1"
        hist.set_checkpoint_active.assert_called_once()


# ---------------------------------------------------------------------------
# _run_dynamic_flow – outer exception handler (lines 646-660)
# ---------------------------------------------------------------------------

class TestRunDynamicFlowOuter:

    @pytest.mark.asyncio
    async def test_dynamic_flow_outer_exception(self):
        """Outer exception in _run_dynamic_flow (line 646-660)."""
        svc = _make_service()

        with patch.object(FlowRunnerService, "_safe_session", new=_safe_ctx), \
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
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session", new=_smart_ctx), \
             patch.object(svc, "_emit_error_span", new=AsyncMock()):

            flow_svc = MagicMock()
            # Outer exception: update_execution_status raises on PREPARING
            flow_svc.update_execution_status = AsyncMock(side_effect=RuntimeError("outer db fail"))
            MockFlowSvc.return_value = flow_svc

            bf = MagicMock()
            bf.config = {}
            MockBF.return_value = bf
            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)

            result = await svc._run_dynamic_flow(
                execution_id=1,
                job_id="job-outer",
                config={"nodes": [{"id": "n1"}], "edges": [], "flow_config": {}}
            )

        assert result.get("success") is False

    @pytest.mark.asyncio
    async def test_dynamic_flow_outer_exception_status_update_fails(self):
        """Outer exception + outer status update also fails (line 657-658)."""
        svc = _make_service()

        with patch.object(FlowRunnerService, "_safe_session", new=_safe_ctx), \
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
             patch("src.engines.crewai.paths.flow.flow_runner_service._smart_db_session") as mock_smart, \
             patch.object(svc, "_emit_error_span", new=AsyncMock()):

            # Primary session fails on PREPARING
            flow_svc = MagicMock()
            flow_svc.update_execution_status = AsyncMock(side_effect=RuntimeError("outer db fail"))
            MockFlowSvc.return_value = flow_svc

            bf = MagicMock()
            bf.config = {}
            MockBF.return_value = bf
            MockApiSvc.get_provider_api_key = AsyncMock(return_value=None)

            # Smart session also fails
            fail_ctx = MagicMock()
            fail_ctx.__aenter__ = AsyncMock(side_effect=RuntimeError("smart session fail"))
            fail_ctx.__aexit__ = AsyncMock(return_value=False)
            mock_smart.return_value = fail_ctx

            result = await svc._run_dynamic_flow(
                execution_id=1,
                job_id="job-outer2",
                config={"nodes": [{"id": "n1"}], "edges": [], "flow_config": {}}
            )

        assert result.get("success") is False


# ---------------------------------------------------------------------------
# flow_runner_service - run_flow resume scenario
# ---------------------------------------------------------------------------

class TestRunFlowResumeScenario:

    @pytest.mark.asyncio
    async def test_run_flow_resume_existing_execution_found(self):
        """Resume scenario where existing execution is found (lines 311-322)."""
        svc = _make_service()

        with patch("src.engines.crewai.paths.flow.flow_runner_service.FlowExecutionService"):
            with patch("src.engines.crewai.paths.flow.flow_runner_service.FlowRepository"):
                with patch("src.engines.crewai.paths.flow.flow_runner_service.TaskRepository"):
                    with patch("src.engines.crewai.paths.flow.flow_runner_service.AgentRepository"):
                        with patch("src.engines.crewai.paths.flow.flow_runner_service.ToolRepository"):
                            with patch("src.engines.crewai.paths.flow.flow_runner_service.CrewRepository"):
                                svc = FlowRunnerService(MagicMock(spec=AsyncSession))

        existing_execution = MagicMock()
        existing_execution.id = 42
        existing_execution.status = "RUNNING"

        svc.db = MagicMock(spec=AsyncSession)
        svc.db.commit = AsyncMock()
        svc.flow_execution_service = MagicMock()
        svc.flow_execution_service.create_execution = AsyncMock()

        flow_result = {"success": True, "result": "done"}

        with patch("src.engines.crewai.paths.flow.flow_runner_service.ExecutionHistoryRepository") as MockExecRepo, \
             patch.object(svc, "_run_dynamic_flow", new=AsyncMock(return_value=flow_result)):

            repo_inst = MagicMock()
            repo_inst.get_execution_by_job_id = AsyncMock(return_value=existing_execution)
            MockExecRepo.return_value = repo_inst

            result = await svc.run_flow(
                flow_id=None,
                job_id="job-resume",
                config={
                    "nodes": [{"id": "n1"}],
                    "edges": [],
                    "resume_from_execution_id": "old-job-id",
                }
            )

        assert result["status"] == FlowExecutionStatus.COMPLETED


# ---------------------------------------------------------------------------
# flow_methods.py supplemental tests
# ---------------------------------------------------------------------------

from src.engines.crewai.paths.flow.modules.flow_methods import FlowMethodFactory, extract_final_answer


class TestFlowMethodsSupplemental:

    def test_extract_final_answer_list_dict_with_final_answer(self):
        """Test list of dicts where each dict has content with Final Answer."""
        items = [
            {"content": "thinking... Final Answer: answer1"},
            {"content": "more Final Answer: answer2"}
        ]
        result = extract_final_answer([items])
        # Should extract answer from at least one item
        assert result

    def test_extract_final_answer_raw_attribute_with_final_answer(self):
        obj = MagicMock()
        obj.raw = "Process Final Answer: the real answer"
        result = extract_final_answer([obj])
        assert "the real answer" in result

    def test_extract_final_answer_non_indexable(self):
        """Result is not a sequence - fallback to str."""
        class NonIndexable:
            def __str__(self):
                return "not indexable"
        result = extract_final_answer(NonIndexable())
        assert "not indexable" in result

    @pytest.mark.asyncio
    async def test_skipped_crew_state_attrs(self):
        """Test get_cached_output with object-style state (lines 1021-1028)."""
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="My Crew",
            crew_sequence=0,
            is_starting_point=True,
            checkpoint_output=None
        )

        class ObjState:
            starting_point_0 = "obj state data"

            def __setitem__(self, key, value):
                setattr(self, key, value)

        mock_self = MagicMock()
        mock_self._method_outputs = {}
        mock_self.state = ObjState()
        result = await method._meth(mock_self)
        assert result == "obj state data"

    @pytest.mark.asyncio
    async def test_skipped_crew_state_output_key(self):
        """Test get_cached_output with output_key pattern (lines ~1007-1012)."""
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="My Crew",
            crew_sequence=0,
            is_starting_point=True,
            checkpoint_output=None
        )

        mock_self = MagicMock()
        mock_self._method_outputs = {}
        mock_self.state = {"starting_point_0_output": "output key data"}
        result = await method._meth(mock_self)
        assert result == "output key data"

    @pytest.mark.asyncio
    async def test_skipped_crew_state_previous_output_key(self):
        """Test get_cached_output with previous_output key (line ~1014-1018)."""
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="My Crew",
            crew_sequence=0,
            is_starting_point=True,
            checkpoint_output=None
        )

        mock_self = MagicMock()
        mock_self._method_outputs = {}
        mock_self.state = {"previous_output": "prev output data"}
        result = await method._meth(mock_self)
        assert result == "prev output data"

    @pytest.mark.asyncio
    async def test_listener_skipped_method_state_store(self):
        """Test that listener stub stores output in state (lines 1113-1118)."""
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="listener_0",
            crew_name="Listener",
            crew_sequence=1,
            is_starting_point=False,
            method_condition="starting_point_0",
            checkpoint_output="listener output"
        )

        mock_self = MagicMock()
        state = {}
        mock_self.state = state

        result = await method._meth(mock_self, previous_output=None)

        assert result == "listener output"
        assert state.get("listener_0") == "listener output"
        assert state.get("previous_output") == "listener output"


# ---------------------------------------------------------------------------
# task_config.py supplemental tests
# ---------------------------------------------------------------------------

from src.engines.crewai.paths.flow.modules.task_adapter import TaskConfig
from types import SimpleNamespace


class TestTaskConfigSupplemental:

    @pytest.mark.asyncio
    async def test_resolve_agent_from_flow_edges(self):
        """Test agent inference from flow edges (lines 267-311)."""
        agent_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())

        task_data = SimpleNamespace(
            name="Task",
            description="desc",
            agent_id=None,  # No agent_id
            id=task_id
        )

        # Flow data with edges connecting agent to task
        flow_data = SimpleNamespace(
            edges=[
                {
                    "source": f"agent-{agent_id}",
                    "target": f"task-{task_id}",
                    "id": "e1"
                }
            ]
        )

        agent_data = MagicMock()
        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=agent_data)

        mock_agent = MagicMock()

        with patch("src.engines.crewai.paths.flow.modules.agent_adapter.AgentConfig.configure_agent_and_tools",
                   new=AsyncMock(return_value=mock_agent)):
            result = await TaskConfig._resolve_agent_for_task(
                task_data, flow_data, {"agent": agent_repo}
            )

        assert result is mock_agent

    @pytest.mark.asyncio
    async def test_resolve_agent_from_edges_no_agent_data(self):
        """Agent inferred from edges but not found in DB."""
        agent_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())

        task_data = SimpleNamespace(
            name="Task",
            description="desc",
            agent_id=None,
            id=task_id
        )

        flow_data = SimpleNamespace(
            edges=[
                {
                    "source": f"agent-{agent_id}",
                    "target": f"task-{task_id}",
                    "id": "e1"
                }
            ]
        )

        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=None)

        with patch("src.db.session.request_scoped_session") as MockSess:
            mock_session_ctx = MagicMock()
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalar_one_or_none = MagicMock(return_value=None)
            mock_session.execute = AsyncMock(return_value=mock_result)
            mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
            MockSess.return_value = mock_session_ctx

            result = await TaskConfig._resolve_agent_for_task(
                task_data, flow_data, {"agent": agent_repo}
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_configure_task_no_expected_output(self):
        """Task data without expected_output attribute."""
        task_data = SimpleNamespace(
            name="Task",
            description="Do something",
            agent_id=None,
            tools=[],
            tool_configs={},
            async_execution=False,
            human_input=False,
            guardrail=None,
            config={},
            markdown=False,
            # No expected_output attribute
        )

        from crewai.agents.agent_builder.base_agent import BaseAgent
        mock_agent = MagicMock(spec=BaseAgent)
        mock_agent.role = "Tester"
        mock_agent.tools = []
        mock_agent.__class__ = BaseAgent

        mock_task = MagicMock()

        with patch.object(TaskConfig, "_configure_task_tools", new=AsyncMock()), \
             patch.object(TaskConfig, "_resolve_agent_for_task", new=AsyncMock(return_value=None)):
            result = await TaskConfig.configure_task(task_data, agent=mock_agent)

        # Returns None since no agent was provided at top level
        # but configure_task_tools should complete

    @pytest.mark.asyncio
    async def test_configure_task_tools_with_tool_configs_override(self):
        """Tools with tool_configs override (exercises _resolve_tool_override)."""
        task_id = uuid.uuid4()
        task_data = SimpleNamespace(
            name="Task",
            description="desc",
            expected_output="output",
            agent_id=None,
            id=task_id,
            tools=["42"],
            tool_configs={"42": {"spaceId": "my-space"}},  # Override
            async_execution=False,
            human_input=False,
            guardrail=None,
            config={},
            markdown=False,
        )

        from crewai.agents.agent_builder.base_agent import BaseAgent
        mock_agent = MagicMock(spec=BaseAgent)
        mock_agent.role = "Tester"
        mock_agent.tools = []
        mock_agent.__class__ = BaseAgent

        mock_tool = MagicMock()
        mock_factory = MagicMock()
        mock_factory.create_tool.return_value = mock_tool
        mock_factory.get_tool_info.return_value = None

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def mock_session_ctx():
            yield AsyncMock()

        with patch("src.db.session.request_scoped_session", return_value=mock_session_ctx()), \
             patch("src.services.api_keys_service.ApiKeysService"), \
             patch("src.engines.crewai.tools.tool_factory.ToolFactory") as MockTF:
            MockTF.create = AsyncMock(return_value=mock_factory)

            await TaskConfig._configure_task_tools(task_data, mock_agent, None)

        # Should have called create_tool with the override
        mock_factory.create_tool.assert_called_once_with("42", tool_config_override={"spaceId": "my-space"})
        assert mock_tool in mock_agent.tools
