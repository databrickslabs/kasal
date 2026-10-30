"""UCMV edits stay with the run that saved them.

Saving or approving a UCMV result stores the user's edits on that run's
``execution_history`` row (``checkpoint_data.ucmv_yaml_edits``). The Metric View
Validator applies only the edits of the run it belongs to, found by that run's
job id and checked against the run's own group, and otherwise validates the
run's own output. Nothing passes between runs through the filesystem.

Runs on a real SQLite database with two groups, so the save, the lookup and
its WHERE clauses are exercised rather than mocked.
"""

import builtins
import json
import os
import pathlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.models.execution_history import ExecutionHistory
from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.services.tools.metric_view_validator_tool import MetricViewValidatorTool
from src.utils.user_context import GroupContext, UserContext

A, B = "group_a", "group_b"
RECORD = {"checkpoint": {"version": 1, "units": ["crew_1"]}}


def _ucmv(table: str, marker: str) -> dict:
    """A UCMV Generator-shaped result whose every field names its owner."""
    return {
        "yaml": {table: f"name: {table}\nsource: {marker}.schema.{table}\n"},
        "sql": {table: f"CREATE VIEW {marker}.schema.{table}"},
        "stats": {table: {"total": 1}},
        "untranslatable_items": [{"measure": f"{marker}_measure", "dax": "x"}],
    }


A_EDITS = _ucmv("fact_sales_a", "a_catalog")
B_OWN = _ucmv("fact_orders_b", "b_catalog")
A2_OWN = _ucmv("fact_stock_a2", "a2_catalog")


@pytest.fixture
def db_url(tmp_path):
    """Three runs: two in group A (run-a carries a checkpoint record), one in B."""
    path = tmp_path / "ucmv_runs.db"
    sync = create_engine(f"sqlite:///{path}")
    ExecutionHistory.__table__.create(sync)
    now = datetime(2026, 10, 1, 12, 0, 0)
    with Session(sync) as s:
        s.add_all(
            [
                ExecutionHistory(
                    job_id="run-a",
                    status="running",
                    group_id=A,
                    created_at=now - timedelta(minutes=10),
                    checkpoint_data=dict(RECORD),
                ),
                ExecutionHistory(
                    job_id="run-a2",
                    status="running",
                    group_id=A,
                    created_at=now,
                ),
                ExecutionHistory(
                    job_id="run-b",
                    status="running",
                    group_id=B,
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


@pytest.fixture
def file_writes(tmp_path, monkeypatch):
    """Every path opened for writing, with the machine's /tmp kept out of reach.

    Paths under /tmp/ucmv* are redirected into this test's own directory, so
    code that still used a /tmp channel runs (and is caught here) without
    touching the shared /tmp of the machine running the suite.
    """
    real_open, real_exists = builtins.open, os.path.exists
    writes: list[str] = []

    def _redirect(p):
        s = os.fspath(p) if isinstance(p, (str, os.PathLike)) else p
        if isinstance(s, str) and s.startswith("/tmp/ucmv"):
            return str(tmp_path / s.rsplit("/", 1)[-1])
        return p

    def _open(file, mode="r", *args, **kwargs):
        if isinstance(file, (str, os.PathLike)) and any(c in mode for c in "wax+"):
            writes.append(os.fspath(file))
        return real_open(_redirect(file), mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _open)
    monkeypatch.setattr(os.path, "exists", lambda p: real_exists(_redirect(p)))
    return writes


@pytest.fixture
def routed_to(db_url, monkeypatch):
    """Route the tool's sessions to the two-group database; skip retry sleeps."""
    import time

    import src.db.session as db_session

    monkeypatch.setattr(db_session, "routed_scoped_session", lambda: _session(db_url))
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    # The validator's "this run's measures" fallback reads the trace table,
    # which this database does not have; it is not under test here.
    monkeypatch.setattr(
        MetricViewValidatorTool, "_fetch_measures_from_db", lambda self: []
    )
    yield
    UserContext.clear_context()


async def _save_edits(db_url, job_id, edits, group_id):
    """What PATCH /executions/{job_id}/result does for a UCMV body."""
    async with _session(db_url) as s:
        ok = await ExecutionHistoryRepository(s).update_execution_result(
            job_id, edits, group_ids=[group_id]
        )
        await s.commit()
    return ok


async def _checkpoint(db_url, job_id):
    async with _session(db_url) as s:
        return await ExecutionHistoryRepository(s).get_checkpoint_data(job_id)


def _validate(group_id, job_id, own_output):
    """Run the validator as run ``job_id`` of ``group_id`` with its own output."""
    UserContext.set_group_context(GroupContext(group_ids=[group_id]))
    tool = MetricViewValidatorTool(ucmv_output=json.dumps(own_output))
    tool.trace_context = {"job_id": job_id, "execution_id": job_id}
    return json.loads(tool._run())


# ── Saving ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_saved_edits_land_on_the_run_next_to_its_checkpoint(db_url, file_writes):
    """The edits persist on the run's own row; the existing record survives."""
    assert await _save_edits(db_url, "run-a", A_EDITS, A)

    stored = await _checkpoint(db_url, "run-a")
    assert stored["ucmv_yaml_edits"] == A_EDITS
    assert stored["checkpoint"] == RECORD["checkpoint"]
    assert file_writes == []


@pytest.mark.asyncio
async def test_config_edits_also_persist_next_to_a_checkpoint(db_url):
    """Same save path, other key: a review-gate config edit is kept too."""
    edited = {"join_key_map": {"fact": "dim"}}
    assert await _save_edits(db_url, "run-a", edited, A)

    stored = await _checkpoint(db_url, "run-a")
    assert stored["edited_config"] == edited
    assert stored["checkpoint"] == RECORD["checkpoint"]


@pytest.mark.asyncio
async def test_saving_to_another_groups_run_is_refused(db_url, file_writes):
    assert not await _save_edits(db_url, "run-b", A_EDITS, A)
    assert await _checkpoint(db_url, "run-b") is None
    assert file_writes == []


# ── Validating ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_two_groups_do_not_see_each_others_edits(db_url, routed_to, file_writes):
    await _save_edits(db_url, "run-a", A_EDITS, A)

    out = _validate(B, "run-b", B_OWN)

    assert out["yaml"] == B_OWN["yaml"]
    assert out["untranslatable_items"] == B_OWN["untranslatable_items"]
    assert "a_catalog" not in json.dumps(out)
    assert file_writes == []


@pytest.mark.asyncio
async def test_another_groups_job_id_does_not_unlock_its_edits(
    db_url, routed_to, file_writes
):
    """Group B naming group A's run still reads nothing: the group is checked."""
    await _save_edits(db_url, "run-a", A_EDITS, A)

    out = _validate(B, "run-a", B_OWN)

    assert out["yaml"] == B_OWN["yaml"]
    assert "a_catalog" not in json.dumps(out)


@pytest.mark.asyncio
async def test_run_without_saved_edits_uses_its_own_output(
    db_url, routed_to, file_writes
):
    """Edits saved on an earlier run in the SAME group do not leak into this one."""
    await _save_edits(db_url, "run-a", A_EDITS, A)

    out = _validate(A, "run-a2", A2_OWN)

    assert out["yaml"] == A2_OWN["yaml"]
    assert out["untranslatable_items"] == A2_OWN["untranslatable_items"]
    assert "a_catalog" not in json.dumps(out)
    assert file_writes == []


@pytest.mark.asyncio
async def test_run_with_saved_edits_validates_them(db_url, routed_to, file_writes):
    """A single run is unchanged: its reviewed edits win over its raw output."""
    await _save_edits(db_url, "run-a", A_EDITS, A)

    out = _validate(A, "run-a", _ucmv("fact_sales_a", "unreviewed"))

    assert out["yaml"] == A_EDITS["yaml"]
    assert out["untranslatable_items"] == A_EDITS["untranslatable_items"]
    assert file_writes == []


def test_validator_without_a_run_id_uses_its_own_output(routed_to, file_writes):
    """No job id (e.g. an ad-hoc call) means no saved edits are looked up."""
    UserContext.set_group_context(GroupContext(group_ids=[A]))
    tool = MetricViewValidatorTool(ucmv_output=json.dumps(A2_OWN))

    out = json.loads(tool._run())

    assert out["yaml"] == A2_OWN["yaml"]
    assert file_writes == []


# ── No shared files ─────────────────────────────────────────────────────────

_SRC = pathlib.Path(__file__).resolve().parents[4] / "src"
_UCMV_MODULES = [
    "repositories/execution_history_repository.py",
    "services/tools/metric_view_validator_tool.py",
    "services/tools/uc_metric_view_generator_tool.py",
]


@pytest.mark.parametrize("module", _UCMV_MODULES)
def test_no_fixed_tmp_paths_in_the_ucmv_modules(module):
    """No hand-built /tmp path: data must not pass between runs through files."""
    assert '"/tmp' not in (_SRC / module).read_text()
