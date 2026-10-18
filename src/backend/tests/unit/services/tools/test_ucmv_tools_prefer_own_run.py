"""The Genie config generator and the visual mapper read their own run's UCMV.

When a flow did not inject ``ucmv_output``, both tools read it from the DB. A
run's own UC Metric View Generator output wins over a newer one from another
run of the same teamspace. Only a run with no output of its own falls back to
the latest in the teamspace, and that fallback is logged.

Real SQLite database; the tools' sessions are routed to it.
"""

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.models.execution_history import ExecutionHistory
from src.models.execution_trace import ExecutionTrace
from src.services.tools.pbi_visual_ucmv_mapper_tool import PBIVisualUCMVMapperTool
from src.services.tools.ucmv_genie_config_generator_tool import (
    UCMVGenieConfigGeneratorTool,
)
from src.utils.user_context import GroupContext, UserContext

GROUP = "team_a"
REPORTS = json.dumps(
    {"reports": [{"pages": [{"visuals": [{"visual_id": "v1", "measures": ["m"]}]}]}]}
)


def _span(job_id, view, created_at):
    content = json.dumps({"yaml": {view: "version: 0.1"}, "sql": {}})
    return ExecutionTrace(
        job_id=job_id,
        event_source="tool",
        event_context="ctx",
        event_type="tool_usage",
        span_name="UC Metric View Generator run",
        output={"content": content},
        group_id=GROUP,
        created_at=created_at,
    )


@pytest.fixture
def routed(tmp_path, monkeypatch):
    """run-own's generator ran first; run-other's ran later, same teamspace."""
    import time

    import src.db.session as db_session

    path = tmp_path / "ucmv.db"
    sync = create_engine(f"sqlite:///{path}")
    ExecutionHistory.__table__.create(sync)
    ExecutionTrace.__table__.create(sync)
    now = datetime(2026, 10, 1, 12, 0, 0)
    with Session(sync) as s:
        s.add_all(
            [
                _span("run-own", "own_view", now),
                _span("run-other", "other_view", now + timedelta(hours=1)),
            ]
        )
        s.commit()
    sync.dispose()
    url = f"sqlite+aiosqlite:///{path}"

    @asynccontextmanager
    async def _session():
        engine = create_async_engine(url, poolclass=NullPool)
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as s:
                yield s
        finally:
            await engine.dispose()

    monkeypatch.setattr(db_session, "routed_scoped_session", _session)
    monkeypatch.setattr(time, "sleep", lambda *_: None)  # the retry loops
    UserContext.set_group_context(GroupContext(group_ids=[GROUP]))
    yield
    UserContext.clear_context()


def _ucmv_read_by_genie(job_id):
    """The ucmv_output the Genie config generator went on to parse."""
    tool = UCMVGenieConfigGeneratorTool()
    tool.trace_context = {"job_id": job_id}
    with patch.object(
        UCMVGenieConfigGeneratorTool, "_extract_yaml_specs", return_value={}
    ) as parse:
        tool._run()
    return parse.call_args.args[0]


def _ucmv_read_by_mapper(job_id):
    """The ucmv_output the visual mapper went on to parse."""
    tool = PBIVisualUCMVMapperTool()
    tool.trace_context = {"job_id": job_id}
    with patch.object(
        PBIVisualUCMVMapperTool, "_parse_ucmv_output", return_value={}
    ) as parse:
        tool._run(report_references_json=REPORTS)
    return parse.call_args.args[0]


@pytest.mark.parametrize("read", [_ucmv_read_by_genie, _ucmv_read_by_mapper])
def test_the_runs_own_ucmv_output_wins_over_a_newer_one(routed, read):
    raw = read("run-own")
    assert "own_view" in raw and "other_view" not in raw


@pytest.mark.parametrize("read", [_ucmv_read_by_genie, _ucmv_read_by_mapper])
def test_a_run_with_no_output_of_its_own_falls_back_and_says_so(routed, read, caplog):
    with caplog.at_level(logging.INFO):
        raw = read("run-standalone")
    assert "other_view" in raw  # the latest in the teamspace
    assert "has no UCMV output of its own" in caplog.text


@pytest.mark.parametrize("read", [_ucmv_read_by_genie, _ucmv_read_by_mapper])
def test_an_own_run_miss_is_one_query_not_five(routed, read):
    """A flow's crews share its job id, so the run's own output is there on
    the first read or not at all: no retries before the fallback."""
    from src.services.trace.service import ExecutionTraceService

    real = ExecutionTraceService.latest_output_for_span_prefix
    job_ids = []

    async def _spy(self, prefix, *, group_ids, job_id=None):
        job_ids.append(job_id)
        return await real(self, prefix, group_ids=group_ids, job_id=job_id)

    with patch.object(ExecutionTraceService, "latest_output_for_span_prefix", _spy):
        read("run-standalone")

    # One own-run read, then the teamspace fallback (found on its first read).
    assert job_ids == ["run-standalone", None]
