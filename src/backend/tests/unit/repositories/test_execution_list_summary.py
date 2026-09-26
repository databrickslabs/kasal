"""Run LISTS return summaries: no ``result``/``inputs`` blobs, a short preview.

Runs against a real SQLite database so the SQL itself is exercised: the JSON
path extraction for ``execution_type``/``flow_id``/``model`` and the
``CAST(result AS TEXT)`` preview.
"""

from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.execution_history import ExecutionHistory
from src.repositories.execution_history_repository import (
    RESULT_PREVIEW_CHARS,
    ExecutionHistoryRepository,
)
from src.repositories.execution_repository import ExecutionRepository
from src.services.execution.listing import summary_row

BIG = "x" * 50_000


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync: ExecutionHistory.__table__.create(sync, checkfirst=True)
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        now = datetime(2026, 9, 1, 12, 0, 0)
        s.add_all(
            [
                ExecutionHistory(
                    job_id="crew-1",
                    status="completed",
                    group_id="g1",
                    group_email="a@example.com",
                    run_name="crew run",
                    execution_type="crew",
                    created_at=now,
                    inputs={
                        "model": "databricks-claude-sonnet-4-5",
                        "agents_yaml": {"a": {"role": BIG}},
                        "tasks_yaml": {"t": {"description": BIG}},
                    },
                    result={"content": "The answer. " + BIG},
                ),
                ExecutionHistory(
                    job_id="flow-legacy",
                    status="running",
                    group_id="g1",
                    run_name="legacy flow",
                    created_at=now + timedelta(minutes=1),
                    inputs={"execution_type": "flow", "flow_id": "f-123"},
                    result=None,
                ),
                ExecutionHistory(
                    job_id="other-tenant",
                    status="completed",
                    group_id="g2",
                    created_at=now,
                    inputs={},
                ),
                ExecutionHistory(
                    job_id="no-group",
                    status="completed",
                    group_id=None,
                    created_at=now,
                    inputs={},
                ),
            ]
        )
        await s.commit()
        # A row written before the column existed: the ORM default would
        # otherwise fill it in on insert.
        await s.execute(
            update(ExecutionHistory)
            .where(ExecutionHistory.job_id == "flow-legacy")
            .values(execution_type=None)
        )
        await s.commit()
        yield s
    await engine.dispose()


@pytest.mark.asyncio
async def test_summaries_carry_no_payload_and_a_bounded_preview(session):
    rows = await ExecutionRepository(session).get_execution_summaries(group_ids=["g1"])
    entries = {e["execution_id"]: e for e in map(summary_row, rows)}

    assert set(entries) == {"crew-1", "flow-legacy"}  # strict group isolation
    crew = entries["crew-1"]
    for heavy in ("result", "inputs", "agents_yaml", "tasks_yaml"):
        assert heavy not in crew
    assert crew["model"] == "databricks-claude-sonnet-4-5"
    assert crew["result_preview"].startswith('{"content": "The answer.')
    assert len(crew["result_preview"]) == RESULT_PREVIEW_CHARS
    assert crew["execution_type"] == "crew"

    legacy = entries["flow-legacy"]
    # Rows written before the columns existed fall back to the JSON inputs.
    assert legacy["execution_type"] == "flow"
    assert legacy["flow_id"] == "f-123"
    assert legacy["result_preview"] is None
    # Newest first, as before.
    assert [r.job_id for r in rows] == ["flow-legacy", "crew-1"]


@pytest.mark.asyncio
async def test_summaries_fail_closed_without_groups(session):
    repo = ExecutionRepository(session)
    assert await repo.get_execution_summaries(group_ids=[]) == []
    assert await repo.get_execution_summaries(group_ids=None) == []


@pytest.mark.asyncio
async def test_history_repository_summary_by_default_full_on_request(session):
    repo = ExecutionHistoryRepository(session)
    rows, total = await repo.get_execution_history(group_ids=["g1"])
    assert total == 2
    assert not hasattr(rows[0], "inputs")
    assert not hasattr(rows[0], "result")
    assert rows[1].model == "databricks-claude-sonnet-4-5"

    full, _ = await repo.get_execution_history(group_ids=["g1"], full=True)
    assert isinstance(full[0], ExecutionHistory)
    assert full[1].result["content"].startswith("The answer.")
