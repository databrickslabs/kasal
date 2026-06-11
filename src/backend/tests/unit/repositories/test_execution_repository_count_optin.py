"""Regression tests for PERF-018: get_execution_history must not run a
COUNT(*) unless asked — every hot-path caller discarded the count, doubling
DB round trips on the most-polled endpoint."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.repositories.execution_repository import ExecutionRepository


def _repo_with_session():
    session = MagicMock()
    rows_result = MagicMock()
    rows_result.scalars.return_value.all.return_value = ["row1", "row2"]
    count_result = MagicMock()
    count_result.scalar.return_value = 42

    async def execute(stmt):
        # COUNT statements have no ORDER BY / LIMIT; detect via string form.
        if "count" in str(stmt).lower():
            return count_result
        return rows_result

    session.execute = AsyncMock(side_effect=execute)
    repo = ExecutionRepository(session)
    return repo, session


@pytest.mark.asyncio
async def test_count_skipped_by_default():
    repo, session = _repo_with_session()

    executions, total = await repo.get_execution_history(limit=10, group_ids=["g1"])

    assert executions == ["row1", "row2"]
    assert total == 0  # not computed
    assert session.execute.await_count == 1  # rows query only


@pytest.mark.asyncio
async def test_count_runs_when_opted_in():
    repo, session = _repo_with_session()

    executions, total = await repo.get_execution_history(
        limit=10, group_ids=["g1"], include_count=True
    )

    assert executions == ["row1", "row2"]
    assert total == 42
    assert session.execute.await_count == 2  # count + rows
