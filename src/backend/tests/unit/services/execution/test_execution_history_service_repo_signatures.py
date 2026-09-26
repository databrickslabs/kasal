"""ExecutionHistoryService must call its repositories with their real signatures.

The service took `tenant_ids` and passed them on as `tenant_ids=`, but the
repositories name the parameter `group_ids`; and it passed the session as the
execution id to `count_by_execution_id`. Plain AsyncMocks accept any call, so the
TypeError only showed up in production (GET /executions/history/{id}, /outputs,
/outputs/debug). Autospec mocks bind each call against the real method.
"""

from unittest.mock import AsyncMock, create_autospec

import pytest

from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.repositories.execution_logs_repository import ExecutionLogsRepository
from src.services.execution.history import ExecutionHistoryService


def _service() -> ExecutionHistoryService:
    history = create_autospec(ExecutionHistoryRepository, instance=True)
    logs = create_autospec(ExecutionLogsRepository, instance=True)
    history.get_execution_by_id.return_value = None
    history.get_execution_by_job_id.return_value = None
    logs.get_logs_by_execution_id.return_value = []
    logs.count_by_execution_id.return_value = 0
    return ExecutionHistoryService(
        session=AsyncMock(),
        execution_history_repository=history,
        execution_logs_repository=logs,
    )


@pytest.mark.asyncio
async def test_get_execution_by_id_passes_group_ids() -> None:
    svc = _service()
    assert await svc.get_execution_by_id(1, tenant_ids=["g1"]) is None
    svc.history_repo.get_execution_by_id.assert_awaited_once_with(1, group_ids=["g1"])


@pytest.mark.asyncio
async def test_get_execution_outputs_calls_repositories_correctly() -> None:
    svc = _service()
    svc.history_repo.get_execution_by_job_id.return_value = object()
    result = await svc.get_execution_outputs("exec-1", tenant_ids=["g1"])
    assert result.total == 0
    svc.history_repo.get_execution_by_job_id.assert_awaited_once_with(
        "exec-1", group_ids=["g1"]
    )
    svc.logs_repo.count_by_execution_id.assert_awaited_once_with(execution_id="exec-1")


@pytest.mark.asyncio
async def test_get_debug_outputs_passes_group_ids() -> None:
    svc = _service()
    await svc.get_debug_outputs("exec-1", tenant_ids=["g1"])
    svc.history_repo.get_execution_by_job_id.assert_awaited_once_with(
        "exec-1", group_ids=["g1"]
    )
