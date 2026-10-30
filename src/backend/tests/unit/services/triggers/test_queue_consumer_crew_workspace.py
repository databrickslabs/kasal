"""A trigger's crew target runs only a crew of the trigger row's own workspace.

The crew id in a target comes from whoever enqueued the event, and enqueueing
does not check that the id is theirs. So the consumer resolves the crew with the
row's ``GroupContext``. A crew of another workspace (or any crew, for a row with
no workspace) is "not found": the row is dead-lettered with that error, no run
record is written, and the foreign crew's name is never read into one. A crew of
the row's own workspace still runs, each task on its edge agent and each agent
with its row settings.

Real catalog services over a real SQLite database; only the run record, the
launch and the queue bookkeeping are mocked.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from src.services.triggers.queue_consumer_service import TriggerQueueConsumerService
from tests.unit.services.catalog.crew_workspaces import (
    A_CREW,
    A_RESEARCHER,
    B_CREW,
    B_CREW_NAME,
    CALLER_A,
    A,
    B,
    routed_to,
    seeded_database,
)

MODULE = "src.services.triggers.queue_consumer_service"
EXECUTION = "src.services.execution.service.ExecutionService"


@pytest_asyncio.fixture
async def db():
    async with seeded_database() as maker:
        yield maker


async def _dispatch(maker, group_id, crew_id):
    """Dispatch one crew-target row; returns the mocks it touched."""
    repo = MagicMock()
    for name in ("mark_dispatched", "mark_failed", "requeue"):
        setattr(repo, name, AsyncMock())
    mocks = {
        "repo": repo,
        "record": AsyncMock(),
        "run": AsyncMock(),
        "announce": AsyncMock(),
    }
    snap = {
        "id": 11,
        "group_id": group_id,
        "target": {"kind": "crew", "id": str(crew_id)},
        "payload": {"inputs": {}},
        "attempts": 1,
    }
    with (
        patch(f"{MODULE}.routed_scoped_session", routed_to(maker)),
        patch(f"{MODULE}.TriggerQueueRepository", return_value=repo),
        patch(f"{EXECUTION}.create_run_record", mocks["record"]),
        patch(f"{EXECUTION}.run_crew_execution", mocks["run"]),
        patch(
            "src.services.execution.status.ExecutionStatusService."
            "broadcast_execution_created",
            mocks["announce"],
        ),
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=None),
        ),
    ):
        await TriggerQueueConsumerService()._dispatch(snap)
    return mocks


def _assert_refused(mocks):
    mocks["record"].assert_not_awaited()
    mocks["run"].assert_not_awaited()
    mocks["announce"].assert_not_awaited()
    mocks["repo"].mark_dispatched.assert_not_awaited()
    failed = mocks["repo"].mark_failed
    failed.assert_awaited_once()
    assert failed.await_args.kwargs.get("dead") is True
    message = failed.await_args.args[1]
    assert "not found" in message
    return message


@pytest.mark.asyncio
async def test_a_target_naming_another_workspaces_crew_does_not_run(db):
    mocks = await _dispatch(db, A, B_CREW)

    message = _assert_refused(mocks)
    assert B_CREW_NAME not in message + str(mocks["repo"].mock_calls)


@pytest.mark.asyncio
async def test_a_row_with_no_workspace_runs_no_crew(db):
    # A NULL group_id row: the old projection read every row unscoped.
    _assert_refused(await _dispatch(db, None, A_CREW))


@pytest.mark.asyncio
async def test_the_run_name_is_read_only_from_the_rows_workspace(db):
    job_id = str(uuid.uuid4())
    consumer = TriggerQueueConsumerService()
    async with db() as session:
        foreign = await consumer._resolve_run_name(
            session, {"id": str(B_CREW)}, "crew", job_id, CALLER_A
        )
        own = await consumer._resolve_run_name(
            session, {"id": str(A_CREW)}, "crew", job_id, CALLER_A
        )

    assert foreign == f"Event crew {job_id[:8]}"
    assert own == "Alpha crew"


@pytest.mark.asyncio
async def test_a_flow_run_name_is_read_only_from_the_rows_workspace(db):
    from src.models.flow import Flow

    own_flow, foreign_flow, unowned_flow = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with db() as session:
        # The shared seed creates only the catalog tables.
        connection = await session.connection()
        await connection.run_sync(Flow.__table__.create, checkfirst=True)
        session.add_all(
            [
                Flow(id=own_flow, name="Alpha flow", group_id=A),
                Flow(id=foreign_flow, name="Bravo secret flow", group_id=B),
                Flow(id=unowned_flow, name="Unowned flow", group_id=None),
            ]
        )
        await session.commit()

    job_id = str(uuid.uuid4())
    consumer = TriggerQueueConsumerService()
    async with db() as session:
        names = [
            await consumer._resolve_run_name(
                session, {"id": str(flow_id)}, "flow", job_id, CALLER_A
            )
            for flow_id in (own_flow, foreign_flow, unowned_flow)
        ]

    generic = f"Event flow {job_id[:8]}"
    assert names == ["Alpha flow", generic, generic]


@pytest.mark.asyncio
async def test_a_crew_of_the_rows_workspace_runs_on_its_edge_agents(db):
    mocks = await _dispatch(db, A, A_CREW)

    mocks["repo"].mark_failed.assert_not_awaited()
    record = mocks["record"].await_args.kwargs
    assert record["run_name"] == "Alpha crew"
    assert record["crew_id"] == str(A_CREW)
    assert record["group_id"] == A
    config = mocks["run"].await_args.kwargs["config"]
    assert {k: t["agent"] for k, t in config.tasks_yaml.items()} == {
        "research": "Researcher",
        "write": "Writer",
    }
    researcher = config.agents_yaml["Researcher"]
    assert (researcher["id"], researcher["temperature"]) == (A_RESEARCHER, 30)
    mocks["repo"].mark_dispatched.assert_awaited_once_with(11)
