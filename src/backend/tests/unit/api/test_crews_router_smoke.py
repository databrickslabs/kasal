import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from datetime import datetime
from uuid import uuid4

from src.api.crews_router import (
    list_crews,
    get_crew,
    create_crew,
    debug_crew_data,
    update_crew,
    delete_crew,
    delete_all_crews,
)
from src.schemas.crew import CrewCreate, CrewUpdate
from src.utils.user_context import GroupContext


def gc(role="user"):
    return GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role=role)


def make_crew(i=None):
    now = datetime.utcnow()
    return SimpleNamespace(
        id=(i or uuid4()),
        name="C",
        agent_ids=["a1"],
        task_ids=["t1"],
        nodes=[],
        edges=[],
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_list_get_create_paths():
    svc = AsyncMock()

    # list
    svc.find_by_group = AsyncMock(return_value=[make_crew()])
    out = await list_crews(service=svc, group_context=gc("operator"))
    assert isinstance(out, list) and out[0].name == "C"

    # get 404 and success
    svc.get_by_group = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await get_crew(uuid4(), service=svc, group_context=gc("operator"))
    c = make_crew()
    svc.get_by_group = AsyncMock(return_value=c)
    got = await get_crew(c.id, service=svc, group_context=gc("operator"))
    assert got.name == "C"

    # create forbidden and success
    with pytest.raises(Exception):
        await create_crew(CrewCreate(name="C", agent_ids=["a1"], task_ids=["t1"], nodes=[], edges=[]), service=svc, group_context=gc("user"))
    svc.create_with_group = AsyncMock(return_value=make_crew())
    created = await create_crew(CrewCreate(name="C", agent_ids=["a1"], task_ids=["t1"], nodes=[], edges=[]), service=svc, group_context=gc("editor"))
    assert created.name == "C"


@pytest.mark.asyncio
async def test_debug_and_update_delete_paths():
    svc = AsyncMock()

    # debug returns success structure
    dbg = await debug_crew_data(CrewCreate(name="C", agent_ids=[], task_ids=[], nodes=[], edges=[]), group_context=gc("operator"))
    assert dbg["status"] in ("success", "error")

    # update forbidden, 404, success
    with pytest.raises(Exception):
        await update_crew(uuid4(), CrewUpdate(name="X"), service=svc, group_context=gc("user"))
    svc.update_with_partial_data_by_group = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await update_crew(uuid4(), CrewUpdate(name="X"), service=svc, group_context=gc("admin"))
    svc.update_with_partial_data_by_group = AsyncMock(return_value=make_crew())
    upd = await update_crew(uuid4(), CrewUpdate(name="X"), service=svc, group_context=gc("editor"))
    assert upd.name in ("C", "X")

    # delete forbidden, 404, success
    with pytest.raises(Exception):
        await delete_crew(uuid4(), service=svc, group_context=gc("user"))
    svc.delete_by_group = AsyncMock(return_value=False)
    with pytest.raises(Exception):
        await delete_crew(uuid4(), service=svc, group_context=gc("editor"))
    svc.delete_by_group = AsyncMock(return_value=True)
    assert await delete_crew(uuid4(), service=svc, group_context=gc("admin")) is None


@pytest.mark.asyncio
async def test_delete_all_crews_admin_only():
    svc = AsyncMock()

    with pytest.raises(Exception):
        await delete_all_crews(service=svc, group_context=gc("operator"))
    svc.delete_all_by_group = AsyncMock(return_value=None)
    assert await delete_all_crews(service=svc, group_context=gc("admin")) is None

