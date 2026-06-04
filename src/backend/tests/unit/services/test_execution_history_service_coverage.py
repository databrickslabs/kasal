"""
Additional coverage tests for execution_history_service.py.
Targets uncovered lines: _mask_inputs_sensitive_data, get_execution_by_id,
check_execution_exists, get_execution_outputs, get_debug_outputs,
delete_all_executions, delete_execution, delete_execution_by_job_id,
get_execution_by_job_id, get_checkpoints_for_flow, expire_checkpoint,
set_checkpoint_active, mark_checkpoint_resumed, update_result,
get_execution_groups_with_counts.
"""
import pytest
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime


def make_run(job_id=None, **kwargs):
    """Build a mock ExecutionHistory row using SimpleNamespace."""
    _id = kwargs.get("id", 1)
    _job_id = job_id or str(uuid.uuid4())
    _status = kwargs.get("status", "completed")
    _result = kwargs.get("result", {"content": "ok"})
    _inputs = kwargs.get("inputs", {})
    _created_at = datetime.utcnow()
    _group_id = kwargs.get("group_id", "g1")

    run = SimpleNamespace(
        id=_id,
        job_id=_job_id,
        status=_status,
        result=_result,
        inputs=_inputs,
        created_at=_created_at,
        group_id=_group_id,
    )
    return run


def make_log(id=1, execution_id="exec-1", content="log content", timestamp=None):
    return SimpleNamespace(
        id=id,
        execution_id=execution_id,
        content=content,
        timestamp=timestamp or datetime.utcnow()
    )


def make_service(session=None, history_repo=None, logs_repo=None):
    from src.services.execution_history_service import ExecutionHistoryService
    s = session or AsyncMock()
    h = history_repo or AsyncMock()
    l = logs_repo or AsyncMock()
    return ExecutionHistoryService(
        session=s,
        execution_history_repository=h,
        execution_logs_repository=l
    )


# ---------------------------------------------------------------------------
# _mask_inputs_sensitive_data
# ---------------------------------------------------------------------------

def test_mask_inputs_sensitive_data_empty():
    svc = make_service()
    assert svc._mask_inputs_sensitive_data({}) == {}
    assert svc._mask_inputs_sensitive_data(None) is None


def test_mask_inputs_sensitive_data_with_agents():
    svc = make_service()
    inputs = {
        "agents_yaml": {
            "agent1": {
                "role": "researcher",
                "tool_configs": {"api_key": "secret123"}
            }
        },
        "tasks_yaml": {
            "task1": {
                "description": "do stuff",
                "tool_configs": {"password": "mysecret"}
            }
        }
    }
    result = svc._mask_inputs_sensitive_data(inputs)
    # Should return a copy with masked fields
    assert result is not inputs  # deep copy


def test_mask_inputs_no_tool_configs():
    svc = make_service()
    inputs = {
        "agents_yaml": {"agent1": {"role": "researcher"}},
        "tasks_yaml": {"task1": {"description": "work"}}
    }
    result = svc._mask_inputs_sensitive_data(inputs)
    assert "agents_yaml" in result


# ---------------------------------------------------------------------------
# get_execution_by_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_execution_by_id_not_found():
    svc = make_service()
    svc.history_repo.get_execution_by_id = AsyncMock(return_value=None)
    result = await svc.get_execution_by_id(999)
    assert result is None


@pytest.mark.asyncio
async def test_get_execution_by_id_with_string_result():
    svc = make_service()
    run = make_run()
    run.result = "string result"
    run.__dict__["result"] = "string result"
    svc.history_repo.get_execution_by_id = AsyncMock(return_value=run)

    result = await svc.get_execution_by_id(1)
    assert result is not None


@pytest.mark.asyncio
async def test_get_execution_by_id_with_inputs_yaml():
    svc = make_service()
    run = make_run()
    run.inputs = {
        "agents_yaml": {"agent1": {"role": "r"}},
        "tasks_yaml": {"task1": {"description": "d"}}
    }
    run.__dict__["inputs"] = run.inputs
    svc.history_repo.get_execution_by_id = AsyncMock(return_value=run)

    result = await svc.get_execution_by_id(1)
    assert result is not None


@pytest.mark.asyncio
async def test_get_execution_by_id_db_error():
    svc = make_service()
    svc.history_repo.get_execution_by_id = AsyncMock(side_effect=SQLAlchemyError("db err"))
    with pytest.raises(SQLAlchemyError):
        await svc.get_execution_by_id(1)


@pytest.mark.asyncio
async def test_get_execution_by_id_generic_error():
    svc = make_service()
    svc.history_repo.get_execution_by_id = AsyncMock(side_effect=RuntimeError("generic"))
    with pytest.raises(RuntimeError):
        await svc.get_execution_by_id(1)


@pytest.mark.asyncio
async def test_get_execution_by_id_with_tenant_ids():
    svc = make_service()
    run = make_run()
    svc.history_repo.get_execution_by_id = AsyncMock(return_value=run)
    result = await svc.get_execution_by_id(1, tenant_ids=["t1"])
    assert result is not None


# ---------------------------------------------------------------------------
# check_execution_exists
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_execution_exists_true():
    svc = make_service()
    svc.history_repo.check_execution_exists = AsyncMock(return_value=True)
    assert await svc.check_execution_exists(1) is True


@pytest.mark.asyncio
async def test_check_execution_exists_false():
    svc = make_service()
    svc.history_repo.check_execution_exists = AsyncMock(return_value=False)
    assert await svc.check_execution_exists(999) is False


@pytest.mark.asyncio
async def test_check_execution_exists_db_error():
    svc = make_service()
    svc.history_repo.check_execution_exists = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.check_execution_exists(1)


@pytest.mark.asyncio
async def test_check_execution_exists_generic_error():
    svc = make_service()
    svc.history_repo.check_execution_exists = AsyncMock(side_effect=RuntimeError("err"))
    with pytest.raises(RuntimeError):
        await svc.check_execution_exists(1)


# ---------------------------------------------------------------------------
# get_execution_outputs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_execution_outputs_no_tenant():
    svc = make_service()
    log = make_log(id=1, execution_id="exec-1")
    svc.logs_repo.get_logs_by_execution_id = AsyncMock(return_value=[log])
    svc.logs_repo.count_by_execution_id = AsyncMock(return_value=1)

    result = await svc.get_execution_outputs("exec-1", tenant_ids=None)
    assert result.total == 1
    assert len(result.outputs) == 1


@pytest.mark.asyncio
async def test_get_execution_outputs_with_tenant_not_found():
    svc = make_service()
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=None)

    result = await svc.get_execution_outputs("exec-1", tenant_ids=["t1"])
    assert result.total == 0
    assert result.outputs == []


@pytest.mark.asyncio
async def test_get_execution_outputs_with_tenant_found():
    svc = make_service()
    run = make_run()
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=run)
    log = make_log(id=1, execution_id="exec-1")
    svc.logs_repo.get_logs_by_execution_id = AsyncMock(return_value=[log])
    svc.logs_repo.count_by_execution_id = AsyncMock(return_value=1)

    result = await svc.get_execution_outputs("exec-1", tenant_ids=["t1"])
    assert result.total == 1


@pytest.mark.asyncio
async def test_get_execution_outputs_db_error():
    svc = make_service()
    svc.logs_repo.get_logs_by_execution_id = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.get_execution_outputs("exec-1")


@pytest.mark.asyncio
async def test_get_execution_outputs_generic_error():
    svc = make_service()
    svc.logs_repo.get_logs_by_execution_id = AsyncMock(side_effect=RuntimeError("err"))
    with pytest.raises(RuntimeError):
        await svc.get_execution_outputs("exec-1")


# ---------------------------------------------------------------------------
# get_debug_outputs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_debug_outputs_not_found():
    svc = make_service()
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=None)
    result = await svc.get_debug_outputs("exec-1")
    assert result is None


@pytest.mark.asyncio
async def test_get_debug_outputs_success():
    svc = make_service()
    run = make_run()
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=run)
    log = make_log(id=1, execution_id="exec-1", content="debug output text")
    svc.logs_repo.get_logs_by_execution_id = AsyncMock(return_value=[log])

    result = await svc.get_debug_outputs("exec-1")
    assert result is not None
    assert result.total_outputs == 1


@pytest.mark.asyncio
async def test_get_debug_outputs_empty_content():
    svc = make_service()
    run = make_run()
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=run)
    log = make_log(id=1, execution_id="exec-1", content=None)
    svc.logs_repo.get_logs_by_execution_id = AsyncMock(return_value=[log])

    result = await svc.get_debug_outputs("exec-1")
    assert result is not None


@pytest.mark.asyncio
async def test_get_debug_outputs_db_error():
    svc = make_service()
    svc.history_repo.get_execution_by_job_id = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.get_debug_outputs("exec-1")


# ---------------------------------------------------------------------------
# Helpers for patching local imports in execution_history_service
# ---------------------------------------------------------------------------

import sys
import contextlib

@contextlib.contextmanager
def patch_deletion_services(mock_trace_svc, mock_logs_svc):
    """
    Patch ExecutionTraceService and ExecutionLogsService used inside
    the execution_history_service methods via local imports.
    """
    import src.services.execution_trace_service as trace_mod
    import src.services.execution_logs_service as logs_mod

    original_trace = trace_mod.ExecutionTraceService
    original_logs = logs_mod.ExecutionLogsService

    trace_mod.ExecutionTraceService = lambda s: mock_trace_svc
    logs_mod.ExecutionLogsService = lambda s: mock_logs_svc
    try:
        yield
    finally:
        trace_mod.ExecutionTraceService = original_trace
        logs_mod.ExecutionLogsService = original_logs


# ---------------------------------------------------------------------------
# delete_execution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_execution_not_found():
    svc = make_service()
    svc.history_repo.get_execution_by_id = AsyncMock(return_value=None)
    result = await svc.delete_execution(999)
    assert result is None


@pytest.mark.asyncio
async def test_delete_execution_success():
    from src.services.execution_service import ExecutionService
    import src.services.crewai_execution_service as crewai_mod

    svc = make_service()
    run = make_run(job_id="job-123")
    svc.history_repo.get_execution_by_id = AsyncMock(return_value=run)
    svc.history_repo.delete_execution = AsyncMock(return_value={
        "task_status_count": 0, "error_trace_count": 0
    })

    mock_trace_repo = AsyncMock()
    mock_trace_repo.delete_by_job_id = AsyncMock(return_value=0)
    mock_trace_svc = MagicMock()
    mock_trace_svc.repository = mock_trace_repo

    mock_logs_svc = AsyncMock()
    mock_logs_svc.delete_by_execution_id = AsyncMock(return_value=0)

    crewai_mod.executions.pop("job-123", None)
    ExecutionService.executions.pop("job-123", None)

    with patch_deletion_services(mock_trace_svc, mock_logs_svc):
        result = await svc.delete_execution(1)

    assert result is not None
    assert result.success is True


@pytest.mark.asyncio
async def test_delete_execution_with_job_in_memory():
    from src.services.execution_service import ExecutionService
    import src.services.crewai_execution_service as crewai_mod

    svc = make_service()
    run = make_run(job_id="job-mem")
    svc.history_repo.get_execution_by_id = AsyncMock(return_value=run)
    svc.history_repo.delete_execution = AsyncMock(return_value={
        "task_status_count": 1, "error_trace_count": 0
    })

    mock_trace_repo = AsyncMock()
    mock_trace_repo.delete_by_job_id = AsyncMock(return_value=1)
    mock_trace_svc = MagicMock()
    mock_trace_svc.repository = mock_trace_repo
    mock_logs_svc = AsyncMock()
    mock_logs_svc.delete_by_execution_id = AsyncMock(return_value=2)

    ExecutionService.executions["job-mem"] = {"status": "RUNNING"}
    crewai_mod.executions["job-mem"] = {"status": "RUNNING"}

    with patch_deletion_services(mock_trace_svc, mock_logs_svc):
        result = await svc.delete_execution(1)

    assert result.success is True
    assert "job-mem" not in ExecutionService.executions
    assert "job-mem" not in crewai_mod.executions


@pytest.mark.asyncio
async def test_delete_execution_db_error():
    svc = make_service()
    svc.history_repo.get_execution_by_id = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.delete_execution(1)


# ---------------------------------------------------------------------------
# delete_execution_by_job_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_execution_by_job_id_not_found():
    svc = make_service()
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=None)
    result = await svc.delete_execution_by_job_id("no-such-job")
    assert result is None


@pytest.mark.asyncio
async def test_delete_execution_by_job_id_success():
    from src.services.execution_service import ExecutionService
    import src.services.crewai_execution_service as crewai_mod

    svc = make_service()
    run = make_run(job_id="job-xyz", id=42)
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=run)
    svc.history_repo.delete_execution_by_job_id = AsyncMock(return_value={
        "task_status_count": 0, "error_trace_count": 0
    })

    mock_trace_repo = AsyncMock()
    mock_trace_repo.delete_by_job_id = AsyncMock(return_value=0)
    mock_trace_svc = MagicMock()
    mock_trace_svc.repository = mock_trace_repo
    mock_logs_svc = AsyncMock()
    mock_logs_svc.delete_by_execution_id = AsyncMock(return_value=0)

    crewai_mod.executions.pop("job-xyz", None)
    ExecutionService.executions.pop("job-xyz", None)

    with patch_deletion_services(mock_trace_svc, mock_logs_svc):
        result = await svc.delete_execution_by_job_id("job-xyz")

    assert result is not None
    assert result.success is True


@pytest.mark.asyncio
async def test_delete_execution_by_job_id_with_memory_cleanup():
    from src.services.execution_service import ExecutionService
    import src.services.crewai_execution_service as crewai_mod

    svc = make_service()
    run = make_run(job_id="job-abc", id=10)
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=run)
    svc.history_repo.delete_execution_by_job_id = AsyncMock(return_value={
        "task_status_count": 0, "error_trace_count": 0
    })

    mock_trace_repo = AsyncMock()
    mock_trace_repo.delete_by_job_id = AsyncMock(return_value=0)
    mock_trace_svc = MagicMock()
    mock_trace_svc.repository = mock_trace_repo
    mock_logs_svc = AsyncMock()
    mock_logs_svc.delete_by_execution_id = AsyncMock(return_value=0)

    ExecutionService.executions["job-abc"] = {"status": "RUNNING"}
    crewai_mod.executions["job-abc"] = {"status": "RUNNING"}

    with patch_deletion_services(mock_trace_svc, mock_logs_svc):
        result = await svc.delete_execution_by_job_id("job-abc")

    assert result.success is True
    assert "job-abc" not in ExecutionService.executions
    assert "job-abc" not in crewai_mod.executions


# ---------------------------------------------------------------------------
# get_execution_by_job_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_execution_by_job_id_not_found():
    svc = make_service()
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=None)
    result = await svc.get_execution_by_job_id("no-such")
    assert result is None


@pytest.mark.asyncio
async def test_get_execution_by_job_id_string_result():
    svc = make_service()
    run = make_run()
    run.result = "text result"
    run.__dict__["result"] = "text result"
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=run)
    result = await svc.get_execution_by_job_id("some-job")
    assert result is not None


@pytest.mark.asyncio
async def test_get_execution_by_job_id_with_yaml_inputs():
    svc = make_service()
    run = make_run()
    run.inputs = {
        "agents_yaml": {"agent1": {"role": "r"}},
        "tasks_yaml": {"t1": {"description": "d"}}
    }
    run.__dict__["inputs"] = run.inputs
    svc.history_repo.get_execution_by_job_id = AsyncMock(return_value=run)
    result = await svc.get_execution_by_job_id("some-job")
    assert result is not None


@pytest.mark.asyncio
async def test_get_execution_by_job_id_db_error():
    svc = make_service()
    svc.history_repo.get_execution_by_job_id = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.get_execution_by_job_id("job-1")


@pytest.mark.asyncio
async def test_get_execution_by_job_id_generic_error():
    svc = make_service()
    svc.history_repo.get_execution_by_job_id = AsyncMock(side_effect=RuntimeError("err"))
    with pytest.raises(RuntimeError):
        await svc.get_execution_by_job_id("job-1")


# ---------------------------------------------------------------------------
# get_checkpoints_for_flow
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_checkpoints_for_flow_success():
    svc = make_service()
    checkpoints = [make_run(), make_run()]
    svc.history_repo.get_checkpoints_for_flow = AsyncMock(return_value=checkpoints)
    result = await svc.get_checkpoints_for_flow(flow_id="flow-123", group_id="g1")
    assert len(result) == 2


@pytest.mark.asyncio
async def test_get_checkpoints_for_flow_db_error():
    svc = make_service()
    svc.history_repo.get_checkpoints_for_flow = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.get_checkpoints_for_flow(flow_id="flow-123")


@pytest.mark.asyncio
async def test_get_checkpoints_for_flow_generic_error():
    svc = make_service()
    svc.history_repo.get_checkpoints_for_flow = AsyncMock(side_effect=RuntimeError("err"))
    with pytest.raises(RuntimeError):
        await svc.get_checkpoints_for_flow(flow_id="flow-123")


# ---------------------------------------------------------------------------
# expire_checkpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_expire_checkpoint_success():
    svc = make_service()
    svc.history_repo.update_checkpoint_status = AsyncMock(return_value=True)
    result = await svc.expire_checkpoint(1, group_id="g1")
    assert result is True


@pytest.mark.asyncio
async def test_expire_checkpoint_db_error():
    svc = make_service()
    svc.history_repo.update_checkpoint_status = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.expire_checkpoint(1)


@pytest.mark.asyncio
async def test_expire_checkpoint_generic_error():
    svc = make_service()
    svc.history_repo.update_checkpoint_status = AsyncMock(side_effect=RuntimeError("err"))
    with pytest.raises(RuntimeError):
        await svc.expire_checkpoint(1)


# ---------------------------------------------------------------------------
# set_checkpoint_active
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_checkpoint_active_success():
    session = AsyncMock()
    svc = make_service(session=session)
    svc.history_repo.set_checkpoint_info = AsyncMock(return_value=True)
    session.commit = AsyncMock()

    result = await svc.set_checkpoint_active(1, "flow-uuid", checkpoint_method="step1")
    assert result is True
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_set_checkpoint_active_db_error():
    session = AsyncMock()
    svc = make_service(session=session)
    svc.history_repo.set_checkpoint_info = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.set_checkpoint_active(1, "flow-uuid")


@pytest.mark.asyncio
async def test_set_checkpoint_active_generic_error():
    session = AsyncMock()
    svc = make_service(session=session)
    svc.history_repo.set_checkpoint_info = AsyncMock(side_effect=RuntimeError("err"))
    with pytest.raises(RuntimeError):
        await svc.set_checkpoint_active(1, "flow-uuid")


# ---------------------------------------------------------------------------
# mark_checkpoint_resumed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_checkpoint_resumed_success():
    svc = make_service()
    svc.history_repo.update_checkpoint_status = AsyncMock(return_value=True)
    result = await svc.mark_checkpoint_resumed(1, new_execution_id=2)
    assert result is True


@pytest.mark.asyncio
async def test_mark_checkpoint_resumed_db_error():
    svc = make_service()
    svc.history_repo.update_checkpoint_status = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.mark_checkpoint_resumed(1, new_execution_id=2)


@pytest.mark.asyncio
async def test_mark_checkpoint_resumed_generic_error():
    svc = make_service()
    svc.history_repo.update_checkpoint_status = AsyncMock(side_effect=RuntimeError("err"))
    with pytest.raises(RuntimeError):
        await svc.mark_checkpoint_resumed(1, new_execution_id=2)


# ---------------------------------------------------------------------------
# update_result
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_result_success():
    svc = make_service()
    svc.history_repo.update_execution_result = AsyncMock(return_value=True)
    result = await svc.update_result("job-123", {"output": "done"})
    assert result["success"] is True
    assert result["job_id"] == "job-123"
    assert "updated_at" in result


@pytest.mark.asyncio
async def test_update_result_not_found():
    svc = make_service()
    svc.history_repo.update_execution_result = AsyncMock(return_value=False)
    result = await svc.update_result("job-missing", {})
    assert result["success"] is False


@pytest.mark.asyncio
async def test_update_result_with_group_ids():
    svc = make_service()
    svc.history_repo.update_execution_result = AsyncMock(return_value=True)
    result = await svc.update_result("job-123", {"data": "x"}, group_ids=["g1", "g2"])
    assert result["success"] is True


@pytest.mark.asyncio
async def test_update_result_db_error():
    svc = make_service()
    svc.history_repo.update_execution_result = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.update_result("job-1", {})


@pytest.mark.asyncio
async def test_update_result_generic_error():
    svc = make_service()
    svc.history_repo.update_execution_result = AsyncMock(side_effect=RuntimeError("err"))
    with pytest.raises(RuntimeError):
        await svc.update_result("job-1", {})


# ---------------------------------------------------------------------------
# get_execution_groups_with_counts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_execution_groups_with_counts_success():
    session = AsyncMock()
    svc = make_service(session=session)

    # Mock the session.execute result
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [("group-1", 5), ("group-2", 3)]
    session.execute = AsyncMock(return_value=mock_result)

    result = await svc.get_execution_groups_with_counts()
    assert len(result) == 2
    assert result[0] == ("group-1", 5)


@pytest.mark.asyncio
async def test_get_execution_groups_with_counts_error():
    session = AsyncMock()
    svc = make_service(session=session)
    session.execute = AsyncMock(side_effect=RuntimeError("db error"))
    with pytest.raises(RuntimeError):
        await svc.get_execution_groups_with_counts()


# ---------------------------------------------------------------------------
# delete_all_executions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_all_executions_no_group():
    from src.services.execution_service import ExecutionService
    import src.services.crewai_execution_service as crewai_mod

    session = AsyncMock()
    svc = make_service(session=session)

    mock_trace_repo = AsyncMock()
    mock_trace_repo.delete_all = AsyncMock(return_value=5)
    mock_trace_svc = MagicMock()
    mock_trace_svc.repository = mock_trace_repo

    mock_logs_svc = AsyncMock()
    mock_logs_svc.delete_all_logs = AsyncMock(return_value=10)
    mock_logs_svc.delete_by_execution_id = AsyncMock(return_value=0)

    svc.history_repo.delete_all_executions = AsyncMock(return_value={
        "run_count": 3, "task_status_count": 0, "error_trace_count": 0
    })

    ExecutionService.executions["test-no-group"] = {}
    crewai_mod.executions["test-no-group-2"] = {}

    with patch_deletion_services(mock_trace_svc, mock_logs_svc):
        result = await svc.delete_all_executions(group_ids=None)

    assert result.success is True
    assert "test-no-group" not in ExecutionService.executions


@pytest.mark.asyncio
async def test_delete_all_executions_with_group_no_jobs():
    session = AsyncMock()
    svc = make_service(session=session)

    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    session.execute = AsyncMock(return_value=mock_result)

    mock_trace_svc = MagicMock()
    mock_trace_svc.repository = AsyncMock()
    mock_logs_svc = AsyncMock()

    with patch_deletion_services(mock_trace_svc, mock_logs_svc):
        result = await svc.delete_all_executions(group_ids=["g1"])

    assert result.success is True
    assert "No executions found" in result.message


@pytest.mark.asyncio
async def test_delete_all_executions_with_group_and_jobs():
    from src.services.execution_service import ExecutionService
    import src.services.crewai_execution_service as crewai_mod

    session = AsyncMock()
    svc = make_service(session=session)

    mock_result = MagicMock()
    mock_result.fetchall.return_value = [("job-g1",), ("job-g2",)]
    session.execute = AsyncMock(return_value=mock_result)

    mock_trace_repo = AsyncMock()
    mock_trace_repo.delete_by_job_id = AsyncMock(return_value=1)
    mock_trace_svc = MagicMock()
    mock_trace_svc.repository = mock_trace_repo

    mock_logs_svc = AsyncMock()
    mock_logs_svc.delete_by_execution_id = AsyncMock(return_value=2)

    svc.history_repo.delete_all_executions = AsyncMock(return_value={
        "run_count": 2, "task_status_count": 0, "error_trace_count": 0
    })

    ExecutionService.executions["job-g1"] = {}
    crewai_mod.executions["job-g2"] = {}

    with patch_deletion_services(mock_trace_svc, mock_logs_svc):
        result = await svc.delete_all_executions(group_ids=["g1"])

    assert result.success is True
    assert "job-g1" not in ExecutionService.executions
    assert "job-g2" not in crewai_mod.executions


@pytest.mark.asyncio
async def test_delete_all_executions_db_error():
    session = AsyncMock()
    svc = make_service(session=session)
    session.execute = AsyncMock(side_effect=SQLAlchemyError("err"))

    with pytest.raises(SQLAlchemyError):
        await svc.delete_all_executions(group_ids=["g1"])


# ---------------------------------------------------------------------------
# get_execution_history - error path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_execution_history_db_error():
    svc = make_service()
    svc.history_repo.get_execution_history = AsyncMock(side_effect=SQLAlchemyError("err"))
    with pytest.raises(SQLAlchemyError):
        await svc.get_execution_history()


@pytest.mark.asyncio
async def test_get_execution_history_generic_error():
    svc = make_service()
    svc.history_repo.get_execution_history = AsyncMock(side_effect=RuntimeError("err"))
    with pytest.raises(RuntimeError):
        await svc.get_execution_history()
