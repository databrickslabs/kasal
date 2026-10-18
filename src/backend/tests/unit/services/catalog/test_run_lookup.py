"""Run-time catalog reads by id: the run's workspace only, closed by default.

``get_with_group_check`` lets any row through when the context carries no group
ids. The run-scoped reads do not: no group ids, no row. A row of another
workspace reads exactly like an absent one, and the warning names the id only.
"""

import logging
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.agent import Agent
from src.models.crew import Crew
from src.models.task import Task
from src.services.catalog.agents import AgentService
from src.services.catalog.crews import CrewService
from src.services.catalog.run_lookup import (
    agent_for_run,
    crew_for_run,
    require_run_workspace,
    run_group_ids,
    task_for_run,
)
from src.services.catalog.tasks import TaskService
from src.utils.user_context import GroupContext

A, B = "ws-a", "ws-b"
IDS = {
    (A, "agent"): "aaaaaaaa-0000-4000-8000-00000000000a",
    (B, "agent"): "bbbbbbbb-0000-4000-8000-00000000000b",
    (A, "task"): "aaaaaaaa-1111-4000-8000-00000000000a",
    (B, "task"): "bbbbbbbb-1111-4000-8000-00000000000b",
    (A, "crew"): uuid.UUID("aaaaaaaa-2222-4000-8000-00000000000a"),
    (B, "crew"): uuid.UUID("bbbbbbbb-2222-4000-8000-00000000000b"),
}
SECRET = {"SerperDevTool": {"serper_api_key": "secret"}}


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for model in (Agent, Task, Crew):
            await conn.run_sync(lambda sync, m=model: m.__table__.create(sync))
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        for group in (A, B):
            s.add(
                Agent(
                    id=IDS[(group, "agent")],
                    name="n",
                    role="r",
                    goal="g",
                    group_id=group,
                    tools=[],
                    tool_configs=SECRET,
                )
            )
            s.add(
                Task(
                    id=IDS[(group, "task")],
                    name="n",
                    description="d",
                    expected_output="o",
                    group_id=group,
                    tools=[],
                    tool_configs=SECRET,
                )
            )
            s.add(Crew(id=IDS[(group, "crew")], name="c", group_id=group))
        await s.commit()
        yield s
    await engine.dispose()


def _reads(session):
    return {
        "agent": (agent_for_run, AgentService(session)),
        "task": (task_for_run, TaskService(session)),
        "crew": (crew_for_run, CrewService(session)),
    }


KINDS = ["agent", "task", "crew"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_a_row_of_the_runs_workspace_is_returned(session, kind):
    read, service = _reads(session)[kind]
    row = await read(service, IDS[(A, kind)], GroupContext(group_ids=[A]))
    assert row is not None and row.group_id == A


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_a_row_of_another_workspace_reads_as_not_found(session, kind):
    read, service = _reads(session)[kind]
    log = MagicMock()
    row = await read(service, IDS[(B, kind)], GroupContext(group_ids=[A]), log)
    assert row is None
    (level, message, *args), _ = log.log.call_args
    assert level == logging.WARNING  # an id that misses is worth a warning
    assert str(IDS[(B, kind)]) in (message % tuple(args))
    assert "secret" not in str(log.log.call_args)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["agent", "task"])
async def test_a_key_that_is_not_an_id_misses_at_debug(session, kind):
    # A name-keyed spec (no id) or a generated chat agent cannot name a row.
    read, service = _reads(session)[kind]
    log = MagicMock()
    row = await read(service, "Research Analyst", GroupContext(group_ids=[A]), log)
    assert row is None
    assert log.log.call_args.args[0] == logging.DEBUG


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("context", [None, GroupContext(group_ids=[])])
async def test_a_run_with_no_group_ids_reads_nothing(session, kind, context):
    read, service = _reads(session)[kind]
    assert await read(service, IDS[(A, kind)], context, MagicMock()) is None


@pytest.mark.asyncio
async def test_a_crew_id_that_is_not_a_uuid_is_a_miss(session):
    log = MagicMock()
    row = await crew_for_run(
        CrewService(session), "not-a-uuid", GroupContext(group_ids=[A]), log
    )
    assert row is None
    log.debug.assert_called_once()


@pytest.mark.asyncio
async def test_a_crew_id_given_as_a_string_still_resolves(session):
    row = await crew_for_run(
        CrewService(session), str(IDS[(A, "crew")]), GroupContext(group_ids=[A])
    )
    assert row is not None and row.id == IDS[(A, "crew")]


@pytest.mark.asyncio
async def test_a_new_run_records_only_a_crew_of_its_workspace(session):
    from src.services.catalog.run_lookup import crew_id_to_record

    in_a = GroupContext(group_ids=[A])
    own, other = IDS[(A, "crew")], IDS[(B, "crew")]
    assert await crew_id_to_record(session, str(own), in_a) == own
    assert await crew_id_to_record(session, other, in_a) is None
    assert await crew_id_to_record(session, "not-a-uuid", in_a) is None
    assert await crew_id_to_record(session, own, GroupContext(group_ids=[])) is None


def test_a_context_narrows_to_its_runs_group_never_beyond_it():
    from src.services.catalog.run_lookup import scope_to_run

    both = GroupContext(group_ids=[A, B], group_email="ab@example.com")
    narrowed = scope_to_run(both, SimpleNamespace(group_id=B))
    assert (narrowed.group_ids, narrowed.group_email) == ([B], "ab@example.com")
    # A group outside the caller's scope, or none, leaves the context as is.
    assert scope_to_run(both, SimpleNamespace(group_id="c")) is both
    assert scope_to_run(both, SimpleNamespace(group_id=None)) is both
    assert scope_to_run(None, SimpleNamespace(group_id=B)) is None


def test_run_group_ids_drops_empty_entries():
    assert run_group_ids(GroupContext(group_ids=[A, "", None])) == [A]
    assert run_group_ids(None) == []


def test_a_run_with_no_workspace_is_refused():
    assert require_run_workspace(GroupContext(group_ids=[A])) == [A]
    for context in (None, GroupContext(group_ids=[])):
        with pytest.raises(ValueError, match="no workspace"):
            require_run_workspace(context)
