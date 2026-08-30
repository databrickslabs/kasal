"""``get_execution_record`` resolves both identities of a run.

The runs table is keyed by integer id, but chat anchors its messages by the
run's JOB id (a UUID) — "schedule this run" arrives with one. Both must reach
the same row, with the same group scoping.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution.service import ExecutionService


def _service():
    svc = ExecutionService.__new__(ExecutionService)
    return svc


def _patched_repo():
    repo = MagicMock()
    repo.get_execution_by_id = AsyncMock(return_value="by-id")
    repo.get_execution_by_job_id = AsyncMock(return_value="by-job-id")
    return repo


@pytest.mark.asyncio
async def test_a_job_uuid_resolves_via_the_job_id_lookup_with_group_scope():
    svc = _service()
    repo = _patched_repo()
    with (
        patch.object(ExecutionService, "_require_session", return_value=MagicMock()),
        patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=repo,
        ),
    ):
        out = await svc.get_execution_record(
            "2d0d43ed-fa8e-4381-bd47-048e459bacd9", group_ids=["g1"]
        )
    assert out == "by-job-id"
    repo.get_execution_by_job_id.assert_awaited_once_with(
        "2d0d43ed-fa8e-4381-bd47-048e459bacd9", group_ids=["g1"]
    )
    repo.get_execution_by_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_integer_id_and_a_numeric_string_use_the_pk_lookup():
    svc = _service()
    repo = _patched_repo()
    with (
        patch.object(ExecutionService, "_require_session", return_value=MagicMock()),
        patch(
            "src.repositories.execution_history_repository.ExecutionHistoryRepository",
            return_value=repo,
        ),
    ):
        assert await svc.get_execution_record(42, group_ids=["g1"]) == "by-id"
        assert await svc.get_execution_record("42", group_ids=["g1"]) == "by-id"
    assert repo.get_execution_by_id.await_count == 2
    repo.get_execution_by_id.assert_awaited_with(42, group_ids=["g1"])
    repo.get_execution_by_job_id.assert_not_awaited()
