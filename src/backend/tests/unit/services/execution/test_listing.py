"""The run list's two row shapes come from one builder.

``summary_row`` reads a projection (JSON fields extracted in SQL, a truncated
preview); ``full_row`` (``include_payload=true``) reads the whole ORM row. They
used to be two hand-written dicts that disagreed. Exercised on a real SQLite
database so both see what production sees.
"""

import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.execution_history import ExecutionHistory
from src.repositories.execution_history_repository import (
    RESULT_PREVIEW_CHARS,
    ExecutionHistoryRepository,
)
from src.services.execution.listing import PAYLOAD_KEYS, full_row, summary_row

BIG = "x" * 5_000


def _mask(inputs):
    masked = dict(inputs)
    if "tool_configs" in masked:
        masked["tool_configs"] = "***"
    return masked


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync: ExecutionHistory.__table__.create(sync, checkfirst=True)
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 9, 1, 12, 0, 0)
    async with maker() as s:
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
                        "tasks_yaml": "already: text",
                        "tool_configs": {"token": "secret"},
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
                    job_id="odd-model",
                    status="completed",
                    group_id="g1",
                    created_at=now + timedelta(minutes=2),
                    inputs={"model": {"name": "not a string"}},
                    result="plain text result",
                ),
            ]
        )
        await s.commit()
        # Written before the column existed (the ORM default fills it on insert).
        await s.execute(
            update(ExecutionHistory)
            .where(ExecutionHistory.job_id == "flow-legacy")
            .values(execution_type=None)
        )
        await s.commit()
        yield s
    await engine.dispose()


async def _both(session):
    repo = ExecutionHistoryRepository(session)
    summaries, _ = await repo.get_execution_history(group_ids=["g1"])
    fulls, _ = await repo.get_execution_history(group_ids=["g1"], full=True)
    return (
        {e["execution_id"]: e for e in map(summary_row, summaries)},
        {e["execution_id"]: e for e in (full_row(r, _mask) for r in fulls)},
    )


@pytest.mark.asyncio
async def test_a_payload_row_is_the_summary_row_plus_the_payload(session):
    summaries, fulls = await _both(session)
    assert set(summaries) == set(fulls) == {"crew-1", "flow-legacy", "odd-model"}
    for execution_id, summary in summaries.items():
        full = fulls[execution_id]
        assert set(full) - set(summary) <= set(PAYLOAD_KEYS)
        # Every summary key agrees, value for value.
        assert {k: full[k] for k in summary} == summary, execution_id


@pytest.mark.asyncio
async def test_payload_row_carries_masked_inputs_result_and_yaml(session):
    _, fulls = await _both(session)
    crew = fulls["crew-1"]
    assert crew["result"]["content"].startswith("The answer.")
    assert crew["inputs"]["tool_configs"] == "***"  # masked
    assert json.loads(crew["agents_yaml"]) == {"a": {"role": BIG}}
    assert crew["tasks_yaml"] == "already: text"  # a string is passed through
    assert crew["model"] == "databricks-claude-sonnet-4-5"
    assert len(crew["result_preview"]) == RESULT_PREVIEW_CHARS


@pytest.mark.asyncio
async def test_legacy_rows_fall_back_to_the_inputs_in_both_shapes(session):
    summaries, fulls = await _both(session)
    for entry in (summaries["flow-legacy"], fulls["flow-legacy"]):
        assert entry["execution_type"] == "flow"
        assert entry["flow_id"] == "f-123"
        assert entry["result_preview"] is None
    assert fulls["flow-legacy"]["result"] is None
    assert "agents_yaml" not in fulls["flow-legacy"]


@pytest.mark.asyncio
async def test_a_non_string_model_reads_the_same_in_both_shapes(session):
    """The SQL extraction renders a non-string field as JSON text; the payload
    path used to hand back the raw object instead."""
    summaries, fulls = await _both(session)
    assert summaries["odd-model"]["model"] == '{"name":"not a string"}'
    assert fulls["odd-model"]["model"] == summaries["odd-model"]["model"]
    assert fulls["odd-model"]["result_preview"] == '"plain text result"'


def test_full_row_without_inputs():
    run = SimpleNamespace(
        job_id="j",
        status="failed",
        created_at=None,
        completed_at=None,
        run_name=None,
        error="boom",
        group_email=None,
        group_id="g1",
        inputs=None,
        result=None,
    )
    entry = full_row(run, _mask)
    assert entry["inputs"] is None
    assert entry["execution_type"] == "crew"
    assert entry["harness"] is None and entry["flow_id"] is None
    assert entry["model"] is None and entry["result_preview"] is None
