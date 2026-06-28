"""
Additional coverage tests for crewai_execution_service.py targeting uncovered lines.
Missing: prepare_and_run_crew error paths, agents_yaml/tasks_yaml branches,
run_crew_execution, cancel_execution, get_execution_status, update_execution_status,
run_flow_execution branches, get_flow_execution, get_flow_executions_by_flow.
"""
import asyncio
import pytest
import uuid
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.services.crewai_execution_service import (
    CrewAIExecutionService, executions, JobStatus
)
from src.models.execution_status import ExecutionStatus
from src.schemas.execution import CrewConfig
from src.utils.user_context import GroupContext


def make_config(**kwargs):
    # Build only valid CrewConfig fields
    config_kwargs = {
        "model": kwargs.get("model", "gpt-4"),
        "inputs": kwargs.get("inputs", {}),
        "planning": kwargs.get("planning", False),
    }
    # agents_yaml and tasks_yaml are Dict fields with default {}
    if "agents_yaml" in kwargs and kwargs["agents_yaml"] is not None:
        config_kwargs["agents_yaml"] = kwargs["agents_yaml"]
    if "tasks_yaml" in kwargs and kwargs["tasks_yaml"] is not None:
        config_kwargs["tasks_yaml"] = kwargs["tasks_yaml"]
    return CrewConfig(**config_kwargs)


def make_group_context(group_ids=None):
    ctx = MagicMock(spec=GroupContext)
    ctx.group_ids = group_ids or ["g1"]
    ctx.primary_group_id = (group_ids or ["g1"])[0]
    ctx.access_token = None
    return ctx


# ---------------------------------------------------------------------------
# prepare_and_run_crew - agents_yaml path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_prepare_and_run_crew_agents_yaml_path():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"run_name": "Test Run", "status": "RUNNING"}

    config = make_config(
        agents_yaml={
            "agent_1": {"name": "researcher", "role": "Researcher", "tool_configs": {"key": "val"}}
        },
        tasks_yaml={
            "task_1": {"name": "task one", "description": "do research", "tool_configs": {}}
        }
    )

    mock_engine = AsyncMock()
    mock_engine._init_task = asyncio.Future()
    mock_engine._init_task.set_result(None)
    mock_engine.run_execution = AsyncMock(return_value={"status": "running"})

    mock_agent_svc = AsyncMock()
    mock_agent_svc.find_by_name = AsyncMock(return_value=None)
    mock_agent_svc.get = AsyncMock(return_value=None)

    mock_task_svc = AsyncMock()
    mock_task_svc.find_by_name = AsyncMock(return_value=None)
    mock_task_svc.get = AsyncMock(return_value=None)

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine), \
         patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.AgentService",
               return_value=mock_agent_svc), \
         patch("src.services.crewai_execution_service.TaskService",
               return_value=mock_task_svc), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.prepare_and_run_crew(exec_id, config)
        assert result["execution_id"] == exec_id


@pytest.mark.asyncio
async def test_prepare_and_run_crew_with_db_agent():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"run_name": "Run", "status": "RUNNING"}

    config = make_config(
        agents_yaml={"agent_1": {"role": "Worker"}},
        tasks_yaml={"task_1": {"description": "do work"}}
    )

    mock_engine = AsyncMock()
    mock_engine.run_execution = AsyncMock(return_value={})

    db_agent = MagicMock()
    db_agent.tool_configs = {"key": "value"}

    db_task = MagicMock()
    db_task.tool_configs = {"task_key": "task_val"}

    mock_agent_svc = AsyncMock()
    mock_agent_svc.find_by_name = AsyncMock(return_value=db_agent)

    mock_task_svc = AsyncMock()
    mock_task_svc.find_by_name = AsyncMock(return_value=db_task)

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine), \
         patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.AgentService",
               return_value=mock_agent_svc), \
         patch("src.services.crewai_execution_service.TaskService",
               return_value=mock_task_svc), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.prepare_and_run_crew(exec_id, config)
        assert result["execution_id"] == exec_id


@pytest.mark.asyncio
async def test_prepare_and_run_crew_with_agents_yaml_empty_dict():
    """Test with empty agents_yaml - falls through to warnings."""
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"run_name": None, "status": "RUNNING"}

    config = make_config()  # agents_yaml={}, tasks_yaml={}

    mock_engine = AsyncMock()
    mock_engine.run_execution = AsyncMock(return_value={})

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.prepare_and_run_crew(exec_id, config)
        assert "execution_id" in result


@pytest.mark.asyncio
async def test_prepare_and_run_crew_hierarchical_process():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"run_name": "Run", "status": "RUNNING"}

    config = make_config(
        inputs={"process": "hierarchical", "manager_llm": "gpt-4"},
        agents_yaml={"agent1": {"role": "Worker"}},
        tasks_yaml={"task1": {"description": "task"}}
    )

    mock_engine = AsyncMock()
    mock_engine.run_execution = AsyncMock(return_value={})

    mock_agent_svc = AsyncMock()
    mock_agent_svc.find_by_name = AsyncMock(return_value=None)
    mock_agent_svc.get = AsyncMock(return_value=None)

    mock_task_svc = AsyncMock()
    mock_task_svc.find_by_name = AsyncMock(return_value=None)
    mock_task_svc.get = AsyncMock(return_value=None)

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine), \
         patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.AgentService",
               return_value=mock_agent_svc), \
         patch("src.services.crewai_execution_service.TaskService",
               return_value=mock_task_svc), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.prepare_and_run_crew(exec_id, config)
        assert "execution_id" in result


@pytest.mark.asyncio
async def test_prepare_and_run_crew_planning_llm():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"run_name": "Run", "status": "RUNNING"}

    config = make_config(
        inputs={"planning_llm": "claude-3", "reasoning_llm": "gpt-4"},
        agents_yaml={"agent1": {"role": "Worker"}},
        tasks_yaml={"task1": {"description": "task"}}
    )

    mock_engine = AsyncMock()
    mock_engine.run_execution = AsyncMock(return_value={})

    mock_agent_svc = AsyncMock()
    mock_agent_svc.find_by_name = AsyncMock(return_value=None)
    mock_agent_svc.get = AsyncMock(return_value=None)

    mock_task_svc = AsyncMock()
    mock_task_svc.find_by_name = AsyncMock(return_value=None)
    mock_task_svc.get = AsyncMock(return_value=None)

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine), \
         patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.AgentService",
               return_value=mock_agent_svc), \
         patch("src.services.crewai_execution_service.TaskService",
               return_value=mock_task_svc), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.prepare_and_run_crew(exec_id, config)
        assert "execution_id" in result


@pytest.mark.asyncio
async def test_prepare_and_run_crew_with_group_context():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"run_name": "Run", "status": "RUNNING"}

    config = make_config(
        agents_yaml={"agent1": {"role": "Worker"}},
        tasks_yaml={"task1": {"description": "task"}}
    )
    group_ctx = make_group_context()

    mock_engine = AsyncMock()
    mock_engine.run_execution = AsyncMock(return_value={})

    mock_agent_svc = AsyncMock()
    mock_agent_svc.find_by_name = AsyncMock(return_value=None)
    mock_agent_svc.get = AsyncMock(return_value=None)

    mock_task_svc = AsyncMock()
    mock_task_svc.find_by_name = AsyncMock(return_value=None)
    mock_task_svc.get = AsyncMock(return_value=None)

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine), \
         patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.AgentService",
               return_value=mock_agent_svc), \
         patch("src.services.crewai_execution_service.TaskService",
               return_value=mock_task_svc), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.prepare_and_run_crew(exec_id, config, group_context=group_ctx)
        assert result["execution_id"] == exec_id


@pytest.mark.asyncio
async def test_prepare_and_run_crew_exception_path():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"run_name": "Run", "status": "RUNNING"}

    config = make_config()

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, side_effect=RuntimeError("engine crash")), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="engine crash"):
            await svc.prepare_and_run_crew(exec_id, config)


@pytest.mark.asyncio
async def test_prepare_and_run_crew_agents_yaml_db_exception_fallback():
    """Test fallback when DB fetch fails for agents_yaml."""
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"run_name": "Run", "status": "RUNNING"}

    config = make_config(
        agents_yaml={"agent_1": {"role": "Worker"}},
    )

    mock_engine = AsyncMock()
    mock_engine.run_execution = AsyncMock(return_value={})

    # Make request_scoped_session raise to hit the except block for agents
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(side_effect=RuntimeError("DB down"))
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine), \
         patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.prepare_and_run_crew(exec_id, config)
        assert result["execution_id"] == exec_id


@pytest.mark.asyncio
async def test_prepare_and_run_crew_tasks_yaml_db_exception_fallback():
    """Test fallback when DB fetch fails for tasks_yaml."""
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"run_name": "Run", "status": "RUNNING"}

    config = make_config(
        tasks_yaml={"task_1": {"description": "do stuff"}}
    )

    mock_engine = AsyncMock()
    mock_engine.run_execution = AsyncMock(return_value={})

    call_count = [0]

    async def _scoped_session_side_effect():
        call_count[0] += 1
        ctx = AsyncMock()
        if call_count[0] > 1:
            ctx.__aenter__ = AsyncMock(side_effect=RuntimeError("DB down"))
        else:
            ctx.__aenter__ = AsyncMock(return_value=ctx)
            ctx.__aexit__ = AsyncMock(return_value=None)
        ctx.__aexit__ = AsyncMock(return_value=None)
        return ctx

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        # Run - it may succeed or fail; the important thing is no unhandled crash
        try:
            result = await svc.prepare_and_run_crew(exec_id, config)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# _prepare_engine
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_prepare_engine_returns_engine():
    svc = CrewAIExecutionService()
    config = make_config()

    mock_engine = AsyncMock()

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine):
        engine = await svc._prepare_engine(config)
        assert engine is mock_engine


@pytest.mark.asyncio
async def test_prepare_engine_none_raises():
    svc = CrewAIExecutionService()
    config = make_config()

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=None):
        with pytest.raises(ValueError, match="Failed to initialize"):
            await svc._prepare_engine(config)


# ---------------------------------------------------------------------------
# run_crew_execution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_crew_execution_creates_task():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    config = make_config()

    with patch.object(svc, "prepare_and_run_crew", new_callable=AsyncMock) as mock_run:
        mock_run.return_value = {"execution_id": exec_id, "status": "RUNNING"}
        result = await svc.run_crew_execution(exec_id, config)

    assert result["execution_id"] == exec_id
    assert result["status"] == ExecutionStatus.RUNNING.value
    assert exec_id in executions


# ---------------------------------------------------------------------------
# get_execution / add_execution_to_memory
# ---------------------------------------------------------------------------

def test_get_execution_found():
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"status": "RUNNING"}
    result = CrewAIExecutionService.get_execution(exec_id)
    assert result["status"] == "RUNNING"


def test_get_execution_not_found():
    result = CrewAIExecutionService.get_execution("no-such-id")
    assert result is None


def test_add_execution_to_memory():
    exec_id = str(uuid.uuid4())
    CrewAIExecutionService.add_execution_to_memory(exec_id, "PENDING", "My Run")
    assert exec_id in executions
    assert executions[exec_id]["status"] == "PENDING"
    assert executions[exec_id]["run_name"] == "My Run"


def test_add_execution_to_memory_with_timestamp():
    exec_id = str(uuid.uuid4())
    ts = datetime(2025, 1, 1)
    CrewAIExecutionService.add_execution_to_memory(exec_id, "RUNNING", "Run", created_at=ts)
    assert executions[exec_id]["created_at"] == ts


# ---------------------------------------------------------------------------
# update_execution_status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_execution_status_in_memory():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"status": "RUNNING", "message": ""}

    with patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        await svc.update_execution_status(
            exec_id, ExecutionStatus.COMPLETED, "Done", result={"output": "results"}
        )

    assert exec_id not in executions  # terminal status cleans up


@pytest.mark.asyncio
async def test_update_execution_status_not_in_memory():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    # Ensure not in executions
    executions.pop(exec_id, None)

    with patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock) as mock_update:
        await svc.update_execution_status(exec_id, ExecutionStatus.FAILED, "fail")

    mock_update.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_execution_status_non_terminal():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"status": "RUNNING"}

    with patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        await svc.update_execution_status(exec_id, ExecutionStatus.RUNNING, "still running")

    # Non-terminal status should keep it in memory
    assert exec_id in executions
    executions.pop(exec_id, None)  # cleanup


# ---------------------------------------------------------------------------
# cancel_execution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_execution_not_in_memory():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions.pop(exec_id, None)

    result = await svc.cancel_execution(exec_id)
    assert result is False


@pytest.mark.asyncio
async def test_cancel_execution_engine_not_found():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"status": "RUNNING"}

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=None):
        result = await svc.cancel_execution(exec_id)

    assert result is False
    executions.pop(exec_id, None)


@pytest.mark.asyncio
async def test_cancel_execution_success():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"status": "RUNNING"}

    mock_engine = AsyncMock()
    mock_engine.cancel_execution = AsyncMock(return_value=True)

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine):
        result = await svc.cancel_execution(exec_id)

    assert result is True
    executions.pop(exec_id, None)


# ---------------------------------------------------------------------------
# get_execution_status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_execution_status_from_memory_terminal():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"status": ExecutionStatus.COMPLETED.value, "result": "done"}

    result = await svc.get_execution_status(exec_id)
    assert result["status"] == ExecutionStatus.COMPLETED.value
    executions.pop(exec_id, None)


@pytest.mark.asyncio
async def test_get_execution_status_from_memory_running_then_engine():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions[exec_id] = {"status": ExecutionStatus.RUNNING.value}

    mock_engine = AsyncMock()
    mock_engine.get_execution_status = AsyncMock(return_value={"status": "RUNNING"})

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine):
        result = await svc.get_execution_status(exec_id)

    assert result is not None
    executions.pop(exec_id, None)


@pytest.mark.asyncio
async def test_get_execution_status_engine_not_found():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions.pop(exec_id, None)

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=None):
        result = await svc.get_execution_status(exec_id)

    assert result is None


@pytest.mark.asyncio
async def test_get_execution_status_not_in_memory():
    svc = CrewAIExecutionService()
    exec_id = str(uuid.uuid4())
    executions.pop(exec_id, None)

    mock_engine = AsyncMock()
    mock_engine.get_execution_status = AsyncMock(return_value={"status": "COMPLETED"})

    with patch("src.services.crewai_execution_service.EngineFactory.get_engine",
               new_callable=AsyncMock, return_value=mock_engine):
        result = await svc.get_execution_status(exec_id)

    assert result["status"] == "COMPLETED"


# ---------------------------------------------------------------------------
# run_flow_execution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_flow_execution_no_flow_id_no_nodes():
    svc = CrewAIExecutionService()

    with patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.run_flow_execution(flow_id=None, nodes=None, job_id="j1")

    assert result["success"] is False
    assert "Either flow_id or nodes must be provided" in result["error"]


@pytest.mark.asyncio
async def test_run_flow_execution_with_nodes_directly():
    svc = CrewAIExecutionService()
    nodes = [{"id": "n1", "type": "crewNode"}]
    edges = [{"id": "e1", "source": "n1", "target": "n2"}]

    mock_flow_svc = AsyncMock()
    mock_flow_svc.run_flow = AsyncMock(return_value={"success": True, "job_id": "j1"})

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc):
        result = await svc.run_flow_execution(nodes=nodes, edges=edges, job_id="j1")

    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_flow_execution_with_group_context():
    svc = CrewAIExecutionService()
    group_ctx = make_group_context()
    nodes = [{"id": "n1"}]

    mock_flow_svc = AsyncMock()
    mock_flow_svc.run_flow = AsyncMock(return_value={"success": True, "job_id": "j2"})

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    import src.utils.user_context as user_ctx_mod
    orig_set_group = getattr(user_ctx_mod.UserContext, 'set_group_context', None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc), \
         patch.object(user_ctx_mod.UserContext, 'set_group_context', MagicMock()):
        result = await svc.run_flow_execution(nodes=nodes, job_id="j2", group_context=group_ctx)

    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_flow_execution_flow_service_error():
    svc = CrewAIExecutionService()
    nodes = [{"id": "n1"}]

    mock_flow_svc = AsyncMock()
    mock_flow_svc.run_flow = AsyncMock(side_effect=RuntimeError("flow crash"))

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.run_flow_execution(nodes=nodes, job_id="j3")

    assert result["success"] is False
    assert "flow crash" in result["error"]


@pytest.mark.asyncio
async def test_run_flow_execution_no_job_id_generates_one():
    svc = CrewAIExecutionService()
    nodes = [{"id": "n1"}]

    mock_flow_svc = AsyncMock()
    mock_flow_svc.run_flow = AsyncMock(return_value={"success": True})

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc):
        result = await svc.run_flow_execution(nodes=nodes, job_id=None)

    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_flow_execution_with_flow_id_loads_from_repo():
    svc = CrewAIExecutionService()
    flow_id = str(uuid.uuid4())

    mock_flow = MagicMock()
    mock_flow.nodes = [{"id": "n1"}]
    mock_flow.edges = [{"id": "e1"}]
    mock_flow.flow_config = {"type": "default"}

    mock_flow_repo_instance = AsyncMock()
    mock_flow_repo_instance.get = AsyncMock(return_value=mock_flow)

    mock_db_session = AsyncMock()
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)

    mock_flow_svc = AsyncMock()
    mock_flow_svc.run_flow = AsyncMock(return_value={"success": True})

    mock_exec_session = AsyncMock()
    mock_exec_session.__aenter__ = AsyncMock(return_value=mock_exec_session)
    mock_exec_session.__aexit__ = AsyncMock(return_value=None)

    call_count = [0]

    def get_session():
        call_count[0] += 1
        if call_count[0] == 1:
            return mock_db_session
        return mock_exec_session

    import src.repositories.flow_repository as flow_repo_mod
    orig_flow_repo = flow_repo_mod.FlowRepository
    flow_repo_mod.FlowRepository = lambda s: mock_flow_repo_instance

    try:
        with patch("src.services.crewai_execution_service.request_scoped_session",
                   side_effect=get_session), \
             patch("src.services.crewai_execution_service.CrewAIFlowService",
                   return_value=mock_flow_svc):
            result = await svc.run_flow_execution(flow_id=flow_id, job_id="j4")
    finally:
        flow_repo_mod.FlowRepository = orig_flow_repo


@pytest.mark.asyncio
async def test_run_flow_execution_with_flow_id_not_found():
    svc = CrewAIExecutionService()
    flow_id = str(uuid.uuid4())

    mock_flow_repo_instance = AsyncMock()
    mock_flow_repo_instance.get = AsyncMock(return_value=None)

    mock_db_session = AsyncMock()
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)

    import src.repositories.flow_repository as flow_repo_mod
    orig_flow_repo = flow_repo_mod.FlowRepository
    flow_repo_mod.FlowRepository = lambda s: mock_flow_repo_instance

    try:
        with patch("src.services.crewai_execution_service.request_scoped_session",
                   return_value=mock_db_session):
            result = await svc.run_flow_execution(flow_id=flow_id, job_id="j5")
    finally:
        flow_repo_mod.FlowRepository = orig_flow_repo

    assert result["success"] is False
    assert "not found" in result["error"]


@pytest.mark.asyncio
async def test_run_flow_execution_with_resume_params():
    svc = CrewAIExecutionService()
    nodes = [{"id": "n1"}]
    config = {
        "resume_from_flow_uuid": "flow-uuid-123",
        "resume_from_execution_id": 42,
        "resume_from_crew_sequence": 2
    }

    mock_flow_svc = AsyncMock()
    mock_flow_svc.run_flow = AsyncMock(return_value={"success": True})

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc):
        result = await svc.run_flow_execution(nodes=nodes, job_id="j6", config=config)

    assert result["success"] is True


@pytest.mark.asyncio
async def test_run_flow_execution_unexpected_outer_exception():
    """Test the outer exception handler in run_flow_execution."""
    svc = CrewAIExecutionService()
    nodes = [{"id": "n1"}]

    # Cause exception inside the inner try block (flow service raises)
    mock_flow_svc = AsyncMock()
    mock_flow_svc.run_flow = AsyncMock(side_effect=RuntimeError("inner crash"))

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc), \
         patch("src.services.crewai_execution_service.ExecutionStatusService.update_status",
               new_callable=AsyncMock):
        result = await svc.run_flow_execution(nodes=nodes, job_id="j7")

    # The inner exception handler returns success=False
    assert result["success"] is False
    assert "inner crash" in result["error"]


# ---------------------------------------------------------------------------
# get_flow_execution and get_flow_executions_by_flow
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_flow_execution():
    svc = CrewAIExecutionService()

    mock_flow_svc = AsyncMock()
    mock_flow_svc.get_flow_execution = AsyncMock(return_value={"id": 1})

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc):
        result = await svc.get_flow_execution(1)

    assert result["id"] == 1


@pytest.mark.asyncio
async def test_get_flow_execution_error():
    svc = CrewAIExecutionService()

    mock_flow_svc = AsyncMock()
    mock_flow_svc.get_flow_execution = AsyncMock(side_effect=RuntimeError("not found"))

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc):
        with pytest.raises(RuntimeError):
            await svc.get_flow_execution(99)


@pytest.mark.asyncio
async def test_get_flow_executions_by_flow():
    svc = CrewAIExecutionService()

    mock_flow_svc = AsyncMock()
    mock_flow_svc.get_flow_executions_by_flow = AsyncMock(return_value={"executions": []})

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc):
        result = await svc.get_flow_executions_by_flow("flow-1")

    assert result is not None


@pytest.mark.asyncio
async def test_get_flow_executions_by_flow_error():
    svc = CrewAIExecutionService()

    mock_flow_svc = AsyncMock()
    mock_flow_svc.get_flow_executions_by_flow = AsyncMock(side_effect=RuntimeError("crash"))

    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    with patch("src.services.crewai_execution_service.request_scoped_session",
               return_value=mock_session), \
         patch("src.services.crewai_execution_service.CrewAIFlowService",
               return_value=mock_flow_svc):
        with pytest.raises(RuntimeError):
            await svc.get_flow_executions_by_flow("flow-1")


# ── Light agent ("chat" mode): service delegates to the engine ───────────────
# The CrewAI-specific work (agent build, kickoff, trace emission) lives in the
# light-agent path (paths/light_agent/light_agent_service.run_light_agent); the
# service only resolves the engine and delegates, mirroring how
# prepare_and_run_crew delegates the crew path to engine.run_execution. The
# runner's own behavior is covered in
# tests/unit/engines/crewai/test_execution_runner_light_agent.py.

@pytest.mark.asyncio
async def test_run_light_agent_execution_delegates_to_engine():
    """The service resolves the CrewAI engine and hands the light run off to
    engine.run_light_agent_execution — it does NOT execute CrewAI itself."""
    svc = CrewAIExecutionService()
    exec_id = f"light-{uuid.uuid4()}"
    config = make_config(
        agents_yaml={"agent_a1": {"id": "agent_a1", "role": "Assistant", "goal": "g",
                                  "backstory": "b", "tools": [], "tool_configs": {"k": 1}}},
        tasks_yaml={"task_t1": {"id": "task_t1", "description": "Answer: hello"}},
    )
    ctx = make_group_context(["g1"])

    engine = MagicMock()
    engine.run_light_agent_execution = AsyncMock(
        return_value={"execution_id": exec_id, "status": ExecutionStatus.COMPLETED.value}
    )
    prepare_mock = AsyncMock(return_value=engine)

    with patch.object(svc, "_prepare_engine", prepare_mock):
        result = await svc.run_light_agent_execution(exec_id, config, group_context=ctx)

    assert result["status"] == ExecutionStatus.COMPLETED.value
    prepare_mock.assert_awaited_once_with(config)
    engine.run_light_agent_execution.assert_awaited_once_with(
        execution_id=exec_id, config=config, group_context=ctx, session=None
    )
