"""``has_completed_trace`` with a minimum span age — the zombie sweep must not
read a light-agent ``response_run`` span as "answered but never completed"
while the run is still composing its surface."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.repositories.execution_trace_repository import (
    ExecutionTraceRepository,
    _span_age_seconds,
)


def _repo_with_row(row):
    session = AsyncMock()
    result = MagicMock()
    result.first.return_value = row
    session.execute = AsyncMock(return_value=result)
    return ExecutionTraceRepository(session)


def test_span_age_counts_a_missing_timestamp_as_old():
    assert _span_age_seconds(None) == float("inf")
    assert _span_age_seconds(datetime.utcnow() - timedelta(seconds=100)) >= 100


@pytest.mark.asyncio
async def test_a_fresh_span_is_absent_under_a_minimum_age():
    repo = _repo_with_row(({"content": "x"}, datetime.utcnow()))
    assert await repo.has_completed_trace("j", "response_run", min_age_seconds=300) == (
        False,
        None,
    )


@pytest.mark.asyncio
async def test_an_old_span_is_found_under_a_minimum_age():
    repo = _repo_with_row(
        ({"content": "x"}, datetime.utcnow() - timedelta(seconds=400))
    )
    assert await repo.has_completed_trace("j", "response_run", min_age_seconds=300) == (
        True,
        {"content": "x"},
    )


@pytest.mark.asyncio
async def test_no_minimum_age_keeps_the_old_behaviour():
    repo = _repo_with_row(({"content": "x"}, datetime.utcnow()))
    assert await repo.has_completed_trace("j") == (True, {"content": "x"})
