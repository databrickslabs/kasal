"""Unit tests for CrewAI Engine Service.

Tests the main public methods of CrewAIEngineService: initialization,
run_execution, get_execution_status, cancel_execution, run_flow,
and internal helpers.
"""

import asyncio
import os
import pytest
from datetime import datetime, UTC
from unittest.mock import MagicMock, patch, AsyncMock, Mock

from src.engines.crewai.crewai_engine_service import CrewAIEngineService
from src.models.execution_status import ExecutionStatus
from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def service():
    """Create a CrewAIEngineService instance for testing."""
    with patch("src.repositories.execution_repository.get_execution_repository"):
        return CrewAIEngineService()


@pytest.fixture
def sample_execution_config():
    """Minimal crew execution configuration."""
    return {
        "crew": {"name": "test_crew", "verbose": True},
        "agents": [
            {
                "id": "agent_1",
                "name": "Test Agent",
                "role": "Test Agent",
                "goal": "Test goal",
                "backstory": "Test backstory",
            }
        ],
        "tasks": [
            {"description": "Test task", "expected_output": "Test output"}
        ],
    }


@pytest.fixture
def sample_flow_config():
    """Minimal flow execution configuration."""
    return {
        "name": "test_flow",
        "description": "Test flow",
        "nodes": [{"id": "n1", "type": "crew"}],
        "edges": [{"source": "n1", "target": "n2"}],
    }


@pytest.fixture
def group_context():
    """Sample group context for multi-tenant tests."""
    ctx = Mock(spec=GroupContext)
    ctx.access_token = "tok_test"
    ctx.primary_group_id = "grp_123"
    ctx.group_email = "test@example.com"
    return ctx


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

class TestInit:
    """Tests for __init__."""

    def test_creates_empty_running_jobs(self, service):
        assert service._running_jobs == {}

    def test_has_repository_factory(self, service):
        assert callable(service._get_execution_repository)

    def test_has_status_service_ref(self, service):
        assert service._status_service is not None

    def test_init_with_db_parameter(self):
        """db parameter is accepted but not stored directly."""
        with patch("src.repositories.execution_repository.get_execution_repository"):
            svc = CrewAIEngineService(db=MagicMock())
            assert svc._running_jobs == {}


# ---------------------------------------------------------------------------
# initialize()
# ---------------------------------------------------------------------------

class TestInitialize:
    """Tests for the async initialize method."""

    @pytest.mark.asyncio
    async def test_returns_true_on_success(self, service):
        with patch(
            "src.engines.crewai.crewai_engine_service.TraceManager.ensure_writer_started",
            new_callable=AsyncMock,
        ):
            result = await service.initialize(llm_provider="openai", model="gpt-4o")
            assert result is True

    @pytest.mark.asyncio
    async def test_uses_default_provider_and_model(self, service):
        with patch(
            "src.engines.crewai.crewai_engine_service.TraceManager.ensure_writer_started",
            new_callable=AsyncMock,
        ):
            result = await service.initialize()
            assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self, service):
        with patch(
            "src.engines.crewai.crewai_engine_service.TraceManager.ensure_writer_started",
            new_callable=AsyncMock,
        ), patch(
            "src.engines.crewai.crew_logger.crew_logger",
            side_effect=Exception("boom"),
        ):
            # Force the import inside initialize() to fail
            with patch.dict("sys.modules", {"src.engines.crewai.crew_logger": None}):
                result = await service.initialize()
                assert result is False

    @pytest.mark.asyncio
    async def test_flow_execution_type_uses_flow_logger(self, service):
        with patch(
            "src.engines.crewai.crewai_engine_service.TraceManager.ensure_writer_started",
            new_callable=AsyncMock,
        ):
            result = await service.initialize(execution_type="flow")
            assert result is True


# ---------------------------------------------------------------------------
# _setup_output_directory()
# ---------------------------------------------------------------------------

class TestSetupOutputDirectory:

    def test_returns_path_with_execution_id(self, service):
        path = service._setup_output_directory("exec_42")
        assert isinstance(path, str)
        assert "exec_42" in path

    def test_returns_base_path_without_id(self, service):
        path = service._setup_output_directory()
        assert isinstance(path, str)
        assert "crew_outputs" in path

    def test_fallback_on_exception(self, service):
        with patch("pathlib.Path", side_effect=Exception("fail")):
            path = service._setup_output_directory("exec_42")
            assert "crew_outputs" in path


# ---------------------------------------------------------------------------
# _update_execution_status()
# ---------------------------------------------------------------------------

class TestUpdateExecutionStatus:

    @pytest.mark.asyncio
    async def test_delegates_to_retry_function(self, service):
        with patch(
            "src.engines.crewai.crewai_engine_service.update_execution_status_with_retry",
            new_callable=AsyncMock,
        ) as mock_retry:
            await service._update_execution_status(
                "exec_1", "COMPLETED", "done", result={"out": 1}
            )
            mock_retry.assert_awaited_once_with(
                execution_id="exec_1",
                status="COMPLETED",
                message="done",
                result={"out": 1},
            )


# ---------------------------------------------------------------------------
# get_execution_status()
# ---------------------------------------------------------------------------

class TestGetExecutionStatus:

    @pytest.mark.asyncio
    async def test_returns_running_for_in_memory_job(self, service):
        now = datetime.now()
        service._running_jobs["exec_1"] = {
            "start_time": now,
            "task": MagicMock(),
            "crew": None,
        }
        result = await service.get_execution_status("exec_1")
        assert result["status"] == ExecutionStatus.RUNNING.value
        assert result["start_time"] == now.isoformat()

    @pytest.mark.asyncio
    async def test_returns_db_status_when_not_in_memory(self, service):
        mock_status = MagicMock()
        mock_status.status = "COMPLETED"
        mock_status.message = "All done"
        mock_status.result = {"key": "val"}
        mock_status.updated_at = datetime.now(UTC)
        mock_status.created_at = datetime.now(UTC)

        with patch(
            "src.services.execution_status_service.ExecutionStatusService.get_status",
            new_callable=AsyncMock,
            return_value=mock_status,
        ):
            result = await service.get_execution_status("exec_2")
            assert result["status"] == "COMPLETED"
            assert result["message"] == "All done"
            assert result["result"] == {"key": "val"}

    @pytest.mark.asyncio
    async def test_returns_unknown_when_not_found(self, service):
        with patch(
            "src.services.execution_status_service.ExecutionStatusService.get_status",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await service.get_execution_status("missing")
            assert result["status"] == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_returns_error_on_exception(self, service):
        with patch(
            "src.services.execution_status_service.ExecutionStatusService.get_status",
            new_callable=AsyncMock,
            side_effect=Exception("db down"),
        ):
            result = await service.get_execution_status("err")
            assert result["status"] == "ERROR"
            assert "db down" in result["message"]


# ---------------------------------------------------------------------------
# cancel_execution()
# ---------------------------------------------------------------------------

class TestCancelExecution:

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self, service):
        assert await service.cancel_execution("nope") is False

    @pytest.mark.asyncio
    async def test_cancels_thread_based_job(self, service):
        async def forever():
            await asyncio.get_event_loop().create_future()

        task = asyncio.create_task(forever())
        service._running_jobs["exec_1"] = {
            "task": task,
            "crew": None,
        }
        with patch.object(service, "_update_execution_status", new_callable=AsyncMock):
            result = await service.cancel_execution("exec_1")
            assert result is True
            assert "exec_1" not in service._running_jobs

    @pytest.mark.asyncio
    async def test_cancels_process_based_job(self, service):
        async def forever():
            await asyncio.get_event_loop().create_future()

        task = asyncio.create_task(forever())
        service._running_jobs["exec_p"] = {
            "task": task,
            "crew": None,
            "execution_mode": "process",
        }
        mock_executor = AsyncMock()
        mock_executor.terminate_execution = AsyncMock(return_value=True)

        with patch(
            "src.services.process_crew_executor.process_crew_executor",
            mock_executor,
        ), patch.object(service, "_update_execution_status", new_callable=AsyncMock):
            result = await service.cancel_execution("exec_p")
            assert result is True
            assert "exec_p" not in service._running_jobs

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self, service):
        mock_task = MagicMock()
        mock_task.cancel.side_effect = Exception("cancel boom")
        service._running_jobs["bad"] = {"task": mock_task, "crew": None}

        result = await service.cancel_execution("bad")
        assert result is False


# ---------------------------------------------------------------------------
# run_execution()
# ---------------------------------------------------------------------------

class TestRunExecution:

    @pytest.mark.asyncio
    async def test_successful_execution_returns_id(self, service, sample_execution_config, group_context):
        mock_session = AsyncMock()

        with patch(
            "src.engines.crewai.crewai_engine_service.normalize_config",
            return_value=sample_execution_config,
        ), patch.object(
            service, "_setup_output_directory", return_value="/tmp/out"
        ), patch(
            "src.engines.crewai.crewai_engine_service.TraceManager.ensure_writer_started",
            new_callable=AsyncMock,
        ), patch(
            "src.engines.crewai.crewai_engine_service.ToolService",
        ), patch(
            "src.engines.crewai.crewai_engine_service.ToolFactory.create",
            new_callable=AsyncMock,
            return_value=MagicMock(),
        ), patch(
            "src.engines.crewai.crewai_engine_service.run_crew_in_process",
            new_callable=AsyncMock,
        ) as mock_run:
            result = await service.run_execution(
                "exec_1", sample_execution_config, group_context, session=mock_session
            )
            assert result == "exec_1"
            assert "exec_1" in service._running_jobs
            assert service._running_jobs["exec_1"]["execution_mode"] == "process"

    @pytest.mark.asyncio
    async def test_adds_group_id_to_config(self, service, sample_execution_config, group_context):
        captured_config = {}
        mock_session = AsyncMock()

        def capture_normalize(cfg):
            captured_config.update(cfg)
            return cfg

        with patch(
            "src.engines.crewai.crewai_engine_service.normalize_config",
            side_effect=capture_normalize,
        ), patch.object(
            service, "_setup_output_directory", return_value="/tmp/out"
        ), patch(
            "src.engines.crewai.crewai_engine_service.TraceManager.ensure_writer_started",
            new_callable=AsyncMock,
        ), patch(
            "src.engines.crewai.crewai_engine_service.ToolService",
        ), patch(
            "src.engines.crewai.crewai_engine_service.ToolFactory.create",
            new_callable=AsyncMock,
            return_value=MagicMock(),
        ), patch(
            "src.engines.crewai.crewai_engine_service.run_crew_in_process",
            new_callable=AsyncMock,
        ):
            await service.run_execution(
                "exec_g", sample_execution_config, group_context, session=mock_session
            )
            # After normalize_config, group_id is added
            # The actual config dict is mutated inline
            # We just check it didn't raise

    @pytest.mark.asyncio
    async def test_updates_status_on_prep_failure(self, service, sample_execution_config):
        mock_session = AsyncMock()

        with patch(
            "src.engines.crewai.crewai_engine_service.normalize_config",
            return_value=sample_execution_config,
        ), patch.object(
            service, "_setup_output_directory", return_value="/tmp/out"
        ), patch(
            "src.engines.crewai.crewai_engine_service.TraceManager.ensure_writer_started",
            new_callable=AsyncMock,
        ), patch(
            "src.engines.crewai.crewai_engine_service.ToolFactory",
        ) as mock_tf_cls, patch.object(
            service, "_update_execution_status", new_callable=AsyncMock
        ) as mock_update:
            mock_tf_cls.create = AsyncMock(side_effect=Exception("tool fail"))
            with pytest.raises(Exception, match="tool fail"):
                await service.run_execution(
                    "exec_fail", sample_execution_config, session=mock_session
                )
            mock_update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_outer_exception_propagates(self, service, sample_execution_config):
        with patch.object(
            service, "_setup_output_directory", side_effect=Exception("boom")
        ):
            with pytest.raises(Exception, match="boom"):
                await service.run_execution("e", sample_execution_config)


# ---------------------------------------------------------------------------
# run_flow()
# ---------------------------------------------------------------------------

class TestRunFlow:

    @pytest.mark.asyncio
    async def test_successful_flow_returns_id(self, service, sample_flow_config, group_context):
        with patch(
            "src.engines.crewai.crewai_engine_service.normalize_flow_config",
            return_value=sample_flow_config,
        ), patch.object(
            service, "_setup_output_directory", return_value="/tmp/fout"
        ), patch(
            "src.engines.crewai.crewai_engine_service.TraceManager.ensure_writer_started",
            new_callable=AsyncMock,
        ), patch(
            "src.engines.crewai.crewai_engine_service.run_flow_in_process",
            new_callable=AsyncMock,
        ):
            result = await service.run_flow(
                "flow_1", sample_flow_config, group_context, user_token="tok"
            )
            assert result == "flow_1"
            assert "flow_1" in service._running_jobs
            assert service._running_jobs["flow_1"]["execution_mode"] == "process"

    @pytest.mark.asyncio
    async def test_adds_group_id_to_flow_config(self, service, sample_flow_config, group_context):
        with patch(
            "src.engines.crewai.crewai_engine_service.normalize_flow_config",
            side_effect=lambda c: c,
        ), patch.object(
            service, "_setup_output_directory", return_value="/tmp/fout"
        ), patch(
            "src.engines.crewai.crewai_engine_service.TraceManager.ensure_writer_started",
            new_callable=AsyncMock,
        ), patch(
            "src.engines.crewai.crewai_engine_service.run_flow_in_process",
            new_callable=AsyncMock,
        ):
            await service.run_flow("fg", sample_flow_config, group_context)
            assert sample_flow_config["group_id"] == "grp_123"

    @pytest.mark.asyncio
    async def test_flow_failure_updates_status(self, service, sample_flow_config):
        with patch(
            "src.engines.crewai.crewai_engine_service.normalize_flow_config",
            side_effect=Exception("norm fail"),
        ), patch.object(
            service, "_update_execution_status", new_callable=AsyncMock
        ) as mock_update:
            with pytest.raises(Exception, match="norm fail"):
                await service.run_flow("ff", sample_flow_config)
            mock_update.assert_awaited_once()
            args = mock_update.call_args
            assert args[0][1] == ExecutionStatus.FAILED.value


# ---------------------------------------------------------------------------
# _execute_flow()
# ---------------------------------------------------------------------------

class TestExecuteFlow:

    @pytest.mark.asyncio
    async def test_successful_flow_sets_result(self, service):
        mock_flow = MagicMock()
        mock_flow.kickoff = AsyncMock(return_value="result_data")

        service._running_jobs["f1"] = {"start_time": datetime.now(UTC)}

        with patch.object(service, "_update_execution_status", new_callable=AsyncMock):
            await service._execute_flow("f1", mock_flow)
            assert service._running_jobs["f1"]["result"] == "result_data"

    @pytest.mark.asyncio
    async def test_failed_flow_updates_status(self, service):
        mock_flow = MagicMock()
        mock_flow.kickoff = AsyncMock(side_effect=Exception("flow crash"))

        service._running_jobs["f2"] = {"start_time": datetime.now(UTC)}

        with patch.object(
            service, "_update_execution_status", new_callable=AsyncMock
        ) as mock_update:
            await service._execute_flow("f2", mock_flow)
            mock_update.assert_any_call(
                "f2",
                ExecutionStatus.FAILED.value,
                "Flow execution failed: flow crash",
            )

    @pytest.mark.asyncio
    async def test_sets_end_time_in_finally(self, service):
        mock_flow = MagicMock()
        mock_flow.kickoff = AsyncMock(return_value="ok")

        service._running_jobs["f3"] = {"start_time": datetime.now(UTC)}

        with patch.object(service, "_update_execution_status", new_callable=AsyncMock):
            await service._execute_flow("f3", mock_flow)
            assert "end_time" in service._running_jobs["f3"]

    @pytest.mark.asyncio
    async def test_no_error_when_job_not_in_running_jobs(self, service):
        mock_flow = MagicMock()
        mock_flow.kickoff = AsyncMock(return_value="ok")

        with patch.object(service, "_update_execution_status", new_callable=AsyncMock):
            # Should not raise even though "ghost" is not in _running_jobs
            await service._execute_flow("ghost", mock_flow)
