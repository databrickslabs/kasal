"""A review gate replays only the edit saved under its own approval.

Flow ``A -> G1 -> B -> G2 -> C``. Each resume reuses the job id, runs A and B as
skip stubs and passes G1 and G2 again. An edit saved at G1 must reach B on every
pass (a retry of B needs it again), and must never replace B's output at G2.

Runs the real save path (ExecutionHistoryService.update_result), the real HITL
and execution services and the real gate method on a SQLite database; only the
gate's session is pointed at that database.
"""

import json
from contextlib import asynccontextmanager
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from src.models.execution_history import ExecutionHistory
from src.models.hitl_approval import HITLApproval, HITLApprovalStatus
from src.services.flow_builder.modules.flow_methods import FlowMethodFactory
from src.services.flow_builder.modules.gate_edits import edit_for_gate

GROUP, JOB = "group_a", "job-1"
G1_EDIT = {"join_key_map": {"fact": "dim"}, "edited_at": "G1"}
B_OUTPUT = '{"from": "crew B"}'
A_OUTPUT = '{"from": "crew A"}'


@pytest.fixture
def db_url(tmp_path):
    path = tmp_path / "gates.db"
    sync = create_engine(f"sqlite:///{path}")
    ExecutionHistory.__table__.create(sync)
    HITLApproval.__table__.create(sync)
    with Session(sync) as s:
        s.add(
            ExecutionHistory(
                job_id=JOB,
                status="waiting_for_approval",
                group_id=GROUP,
                created_at=datetime(2026, 10, 1, 12, 0, 0),
                checkpoint_data={"checkpoint": {"version": 1}},
            )
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
def gate_db(db_url, monkeypatch):
    import src.db.session as db_session

    monkeypatch.setattr(db_session, "get_isolated_db_session", lambda: _session(db_url))
    return db_url


async def _raise_approval(db_url, gate, sequence):
    async with _session(db_url) as s:
        row = HITLApproval(
            execution_id=JOB,
            flow_id="flow-1",
            gate_node_id=gate,
            crew_sequence=sequence,
            gate_config={},
            group_id=GROUP,
            status=HITLApprovalStatus.PENDING,
        )
        s.add(row)
        await s.commit()
        return row.id


async def _set_status(db_url, approval_id, status):
    async with _session(db_url) as s:
        row = await s.get(HITLApproval, approval_id)
        row.status = status
        await s.commit()


async def _save(db_url, edit):
    """PATCH /executions/{job}/result, as the Config Editor / Save & Approve do."""
    from src.services.execution.history import ExecutionHistoryService

    async with _session(db_url) as s:
        result = await ExecutionHistoryService(s).update_result(
            JOB, edit, group_ids=[GROUP]
        )
        await s.commit()
    assert result["success"]


async def _checkpoint(db_url):
    async with _session(db_url) as s:
        return (await s.get(ExecutionHistory, 1)).checkpoint_data


async def _pass(gate, previous_method, previous_output):
    """What the gate hands the next crew when the flow resumes past it."""
    group = type("G", (), {"primary_group_id": GROUP})()
    method = FlowMethodFactory.create_hitl_gate_method(
        method_name=f"hitl_{gate}",
        gate_node_id=gate,
        gate_config={},
        previous_method_name=previous_method,
        crew_sequence=1,
        callbacks={"job_id": JOB, "flow_id": "flow-1"},
        group_context=group,
    )
    flow = type("Flow", (), {"state": {}})()
    return await method._meth(flow, previous_output=previous_output)


async def _g1_edited_then_g2_approved(db_url):
    g1 = await _raise_approval(db_url, "G1", 1)
    await _save(db_url, G1_EDIT)  # Save & Approve at G1
    await _set_status(db_url, g1, HITLApprovalStatus.APPROVED)
    g2 = await _raise_approval(db_url, "G2", 2)
    await _set_status(db_url, g2, HITLApprovalStatus.APPROVED)  # no edit at G2
    return g1, g2


@pytest.mark.asyncio
async def test_a_later_gate_hands_on_its_own_crew_output(gate_db):
    await _g1_edited_then_g2_approved(gate_db)

    assert await _pass("G1", "crew_a", A_OUTPUT) == json.dumps(G1_EDIT)
    assert await _pass("G2", "crew_b", B_OUTPUT) == B_OUTPUT


@pytest.mark.asyncio
async def test_a_retry_of_the_next_crew_still_gets_the_edit(gate_db):
    """Reading the edit does not consume it: every pass of G1 hands it on."""
    await _g1_edited_then_g2_approved(gate_db)

    for _ in range(3):
        assert await _pass("G1", "crew_a", A_OUTPUT) == json.dumps(G1_EDIT)
    assert (await _checkpoint(gate_db))["edited_config"] == G1_EDIT


@pytest.mark.asyncio
async def test_an_edit_before_reject_and_retry_is_not_applied_to_the_new_attempt(
    gate_db,
):
    first = await _raise_approval(gate_db, "G1", 1)
    await _save(gate_db, G1_EDIT)
    await _set_status(gate_db, first, HITLApprovalStatus.RETRY)
    second = await _raise_approval(gate_db, "G1", 1)
    await _set_status(gate_db, second, HITLApprovalStatus.APPROVED)

    assert await _pass("G1", "crew_a", A_OUTPUT) == A_OUTPUT


@pytest.mark.asyncio
async def test_the_save_is_stamped_with_the_waiting_approval(gate_db):
    g1 = await _raise_approval(gate_db, "G1", 1)
    await _save(gate_db, G1_EDIT)

    stored = await _checkpoint(gate_db)
    assert stored["edited_config_gate"] == {"approval_id": g1, "gate_node_id": "G1"}
    assert stored["checkpoint"] == {"version": 1}


@pytest.mark.asyncio
async def test_a_save_with_no_waiting_gate_is_replayed_nowhere(gate_db):
    g1 = await _raise_approval(gate_db, "G1", 1)
    await _save(gate_db, G1_EDIT)
    await _set_status(gate_db, g1, HITLApprovalStatus.APPROVED)

    await _save(gate_db, {"join_key_map": {}, "edited_at": "later"})

    stored = await _checkpoint(gate_db)
    assert "edited_config_gate" not in stored
    assert await _pass("G1", "crew_a", A_OUTPUT) == A_OUTPUT


@pytest.mark.asyncio
async def test_the_stamp_reads_only_approvals_of_the_runs_own_group(gate_db):
    """A caller with two groups in scope saves on a run of one of them. The
    waiting gate is that run's group's approval; a row for the same job id in
    the caller's other group is not the run's and is not counted."""
    from src.services.execution.history import ExecutionHistoryService

    g1 = await _raise_approval(gate_db, "G1", 1)
    async with _session(gate_db) as s:
        s.add(
            HITLApproval(
                execution_id=JOB,
                flow_id="flow-other",
                gate_node_id="G9",
                crew_sequence=1,
                gate_config={},
                group_id="group_b",
                status=HITLApprovalStatus.PENDING,
            )
        )
        await s.commit()

    async with _session(gate_db) as s:
        result = await ExecutionHistoryService(s).update_result(
            JOB, G1_EDIT, group_ids=["group_b", GROUP]
        )
        await s.commit()
    assert result["success"]

    stored = await _checkpoint(gate_db)
    assert stored["edited_config_gate"] == {"approval_id": g1, "gate_node_id": "G1"}


def test_edit_for_gate_needs_a_matching_stamp():
    data = {
        "edited_config": {"k": 1},
        "edited_config_gate": {"approval_id": 7, "gate_node_id": "G1"},
    }
    assert edit_for_gate(data, "G1", 7) == json.dumps({"k": 1})
    assert edit_for_gate(data, "G2", 7) is None
    assert edit_for_gate(data, "G1", 8) is None
    assert edit_for_gate({"edited_config": {"k": 1}}, "G1", 7) is None
    assert edit_for_gate(None, "G1", 7) is None
