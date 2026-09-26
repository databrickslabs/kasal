"""The PowerBI/UCMV tools' "latest output" DB fallbacks stay inside one tenant.

Audit V3-1: ``latest_output_for_span_prefix``, ``latest_result_with_keys``,
``find_recent_results_containing`` and ``latest_checkpoint_containing`` used to
filter by span name or JSON key only, so tenant B's run picked up tenant A's
most recent metric-view YAML. Audit V2: ``get_execution_history`` treated
``None``/``[]`` groups as "all tenants".

Everything here runs on a real SQLite database with two tenants, so the WHERE
clauses themselves are exercised, not a mock of them.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.models.execution_history import ExecutionHistory
from src.models.execution_trace import ExecutionTrace
from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.repositories.execution_trace_repository import ExecutionTraceRepository
from src.utils.user_context import GroupContext, UserContext

A, B = "tenant_a", "tenant_b"
YAML_A = {"yaml": {"t": "A-secret-catalog"}, "sql": "select 1", "stats": {}}


@pytest.fixture
def db_url(tmp_path):
    """A file database seeded with tenant A's UCMV outputs (and nothing for B)."""
    path = tmp_path / "tenants.db"
    sync = create_engine(f"sqlite:///{path}")
    ExecutionHistory.__table__.create(sync)
    ExecutionTrace.__table__.create(sync)
    now = datetime(2026, 9, 1, 12, 0, 0)
    content = '{"yaml": {"t": "A-secret-catalog"}}'
    with Session(sync) as s:
        s.add_all(
            [
                ExecutionHistory(
                    job_id="run-a",
                    status="completed",
                    group_id=A,
                    created_at=now,
                    result={**YAML_A, "untranslatable_items": [{"m": "x"}]},
                    checkpoint_data={"ucmv_yaml_edits": {"yaml": "A-edits"}},
                ),
                ExecutionHistory(
                    job_id="run-null",
                    status="completed",
                    group_id=None,
                    created_at=now + timedelta(minutes=1),
                    result={**YAML_A, "untranslatable_items": []},
                    checkpoint_data={"ucmv_yaml_edits": {"yaml": "null-edits"}},
                ),
                ExecutionHistory(
                    job_id="run-b",
                    status="completed",
                    group_id=B,
                    created_at=now - timedelta(minutes=5),
                    result={"content": "unrelated"},
                ),
                ExecutionTrace(
                    job_id="run-a",
                    event_source="tool",
                    event_context="ctx",
                    event_type="tool_usage",
                    span_name="UC Metric View Generator run",
                    output={"content": content},
                    group_id=A,
                    created_at=now,
                ),
                ExecutionTrace(
                    job_id="run-null",
                    event_source="tool",
                    event_context="ctx",
                    event_type="tool_usage",
                    span_name="UC Metric View Generator run",
                    output={"content": content},
                    group_id=None,
                    created_at=now + timedelta(minutes=1),
                ),
            ]
        )
        s.commit()
    sync.dispose()
    return f"sqlite+aiosqlite:///{path}"


@asynccontextmanager
async def _session(db_url):
    engine = create_async_engine(db_url, poolclass=NullPool)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as s:
            yield s
    finally:
        await engine.dispose()


# ── Repository level ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_trace_latest_output_is_tenant_scoped(db_url):
    async with _session(db_url) as s:
        repo = ExecutionTraceRepository(s)
        prefix = "UC Metric View Generator"
        assert "A-secret-catalog" in await repo.latest_output_for_span_prefix(
            prefix, group_ids=[A]
        )
        assert await repo.latest_output_for_span_prefix(prefix, group_ids=[B]) is None
        assert await repo.latest_output_for_span_prefix(prefix, group_ids=[]) is None


@pytest.mark.asyncio
async def test_history_fallbacks_are_tenant_scoped(db_url):
    async with _session(db_url) as s:
        repo = ExecutionHistoryRepository(s)
        keys = ["yaml", "sql", "stats"]
        assert (await repo.latest_result_with_keys(keys, group_ids=[A]))["yaml"]
        assert await repo.latest_result_with_keys(keys, group_ids=[B]) is None
        assert await repo.latest_result_with_keys(keys, group_ids=[]) is None

        cp = await repo.latest_checkpoint_containing("ucmv_yaml_edits", group_ids=[A])
        # Tenant A's own edits, never the newer NULL-group row's.
        assert cp == {"ucmv_yaml_edits": {"yaml": "A-edits"}}
        assert (
            await repo.latest_checkpoint_containing("ucmv_yaml_edits", group_ids=[B])
            is None
        )
        assert (
            await repo.latest_checkpoint_containing("ucmv_yaml_edits", group_ids=[])
            is None
        )

        key = "untranslatable_items"
        runs = await repo.find_recent_results_containing(key, group_ids=[A])
        assert [r.job_id for r in runs] == ["run-a"]
        assert await repo.find_recent_results_containing(key, group_ids=[B]) == []
        assert await repo.find_recent_results_containing(key, group_ids=[]) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", [None, []])
async def test_history_list_without_groups_returns_nothing(db_url, scope):
    """V2: no scope is no rows — the rule ExecutionRepository already applies."""
    async with _session(db_url) as s:
        repo = ExecutionHistoryRepository(s)
        assert await repo.get_execution_history(group_ids=scope) == ([], 0)
        rows, total = await repo.get_execution_history(group_ids=[B])
        assert total == 1 and [r.job_id for r in rows] == ["run-b"]


# ── Tool level: the group comes from the run's GroupContext ──────────────────


@pytest.fixture
def routed_to(db_url, monkeypatch):
    """Route ToolSessionProvider's sessions to the two-tenant database."""
    import time

    import src.db.session as db_session

    monkeypatch.setattr(db_session, "routed_scoped_session", lambda: _session(db_url))
    monkeypatch.setattr(time, "sleep", lambda *_: None)  # the retry loops
    yield
    UserContext.clear_context()


def _as_tenant(group_id):
    UserContext.set_group_context(GroupContext(group_ids=[group_id]))


def test_validator_fallback_reads_only_the_run_tenant(routed_to):
    from src.services.tools.metric_view_validator_tool import MetricViewValidatorTool

    _as_tenant(A)
    assert MetricViewValidatorTool._fetch_latest_ucmv_from_db()["yaml"]
    assert MetricViewValidatorTool._fetch_saved_ucmv_edits_from_db() == {
        "yaml": "A-edits"
    }

    _as_tenant(B)
    assert MetricViewValidatorTool._fetch_latest_ucmv_from_db() == {}
    assert MetricViewValidatorTool._fetch_saved_ucmv_edits_from_db() == {}
    assert MetricViewValidatorTool._fetch_measures_from_db() == []


def test_fallback_without_group_context_reads_nothing(routed_to):
    from src.services.tools.metric_view_validator_tool import MetricViewValidatorTool
    from src.services.tools.tool_session_provider import ToolSessionProvider

    UserContext.clear_context()
    assert ToolSessionProvider.run_group_ids() == []
    assert MetricViewValidatorTool._fetch_latest_ucmv_from_db() == {}
    assert MetricViewValidatorTool._fetch_saved_ucmv_edits_from_db() == {}


@pytest.mark.asyncio
async def test_reevaluation_input_group_cannot_widen_scope(routed_to):
    from src.services.tools.ucmv_reevaluation_tool import UCMVReevaluationTool

    tool = UCMVReevaluationTool()
    _as_tenant(B)
    # B asks for A's group through the tool input: nothing comes back.
    assert await tool._load_executions([], A, 10) == []
    _as_tenant(A)
    rows = await tool._load_executions([], None, 10)
    assert [r["key"] for r in rows] == ["run-a"]
