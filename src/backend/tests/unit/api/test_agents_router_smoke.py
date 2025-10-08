import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from datetime import datetime

from src.api.agents_router import (
    list_agents,
    create_agent,
    get_agent,
    update_agent_full,
    update_agent,
    delete_agent,
    delete_all_agents,
)
from src.schemas.agent import AgentCreate, AgentUpdate, AgentLimitedUpdate
from src.utils.user_context import GroupContext


def gc(role="user", valid=True):
    if valid:
        return GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role=role)
    return GroupContext()


def make_agent(i="a1"):
    now = datetime.utcnow()
    # return a simple object with attributes accessed by code/tests
    return SimpleNamespace(id=i, name="A", role="r", goal="g", backstory="b", created_at=now, updated_at=now)


@pytest.mark.asyncio
async def test_list_and_create_permissions_and_success():
    svc = AsyncMock()

    # list with valid context
    svc.find_by_group = AsyncMock(return_value=[make_agent("a1")])
    out = await list_agents(service=svc, group_context=gc("operator"))
    assert isinstance(out, list) and out[0].id == "a1"

    # list with invalid context -> empty
    out2 = await list_agents(service=svc, group_context=gc(valid=False))
    assert out2 == []

    # create forbidden for user
    with pytest.raises(Exception):
        await create_agent(AgentCreate(name="A", role="r", goal="g", backstory="b"), service=svc, group_context=gc("user"))

    # create success for admin
    svc.create_with_group = AsyncMock(return_value=make_agent("a2"))
    out3 = await create_agent(AgentCreate(name="A", role="r", goal="g", backstory="b"), service=svc, group_context=gc("admin"))
    assert out3.id == "a2"


@pytest.mark.asyncio
async def test_get_and_updates_and_delete_paths():
    svc = AsyncMock()

    # get 404
    svc.get_with_group_check = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await get_agent("missing", service=svc, group_context=gc("operator"))

    # get success
    svc.get_with_group_check = AsyncMock(return_value=make_agent("a3"))
    assert (await get_agent("a3", service=svc, group_context=gc("operator"))).id == "a3"

    # update full forbidden for user
    with pytest.raises(Exception):
        await update_agent_full("a3", AgentUpdate(name="X"), service=svc, group_context=gc("user"))

    # update full 404
    svc.update_with_group_check = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await update_agent_full("a3", AgentUpdate(name="X"), service=svc, group_context=gc("admin"))

    # update full success
    svc.update_with_group_check = AsyncMock(return_value=make_agent("a3"))
    assert (await update_agent_full("a3", AgentUpdate(name="X"), service=svc, group_context=gc("editor"))).id == "a3"

    # update limited forbidden for user
    with pytest.raises(Exception):
        await update_agent("a3", AgentLimitedUpdate(name="Y"), service=svc, group_context=gc("user"))

    # update limited 404
    svc.update_limited_with_group_check = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await update_agent("a3", AgentLimitedUpdate(name="Y"), service=svc, group_context=gc("editor"))

    # update limited success
    svc.update_limited_with_group_check = AsyncMock(return_value=make_agent("a3"))
    assert (await update_agent("a3", AgentLimitedUpdate(name="Y"), service=svc, group_context=gc("admin"))).id == "a3"

    # delete forbidden for user
    with pytest.raises(Exception):
        await delete_agent("a3", service=svc, group_context=gc("user"))

    # delete 404
    svc.delete_with_group_check = AsyncMock(return_value=False)
    with pytest.raises(Exception):
        await delete_agent("a3", service=svc, group_context=gc("editor"))

    # delete success
    svc.delete_with_group_check = AsyncMock(return_value=True)
    assert await delete_agent("a3", service=svc, group_context=gc("admin")) is None


@pytest.mark.asyncio
async def test_delete_all_agents_conflict_and_success():
    svc = AsyncMock()

    # forbidden for non-admin
    with pytest.raises(Exception):
        await delete_all_agents(service=svc, group_context=gc("operator"))

    # conflict IntegrityError
    from sqlalchemy.exc import IntegrityError
    svc.delete_all_for_group = AsyncMock(side_effect=IntegrityError("msg", "params", Exception("orig")))
    with pytest.raises(Exception) as ei:
        await delete_all_agents(service=svc, group_context=gc("admin"))
    assert ei.value.status_code == 409

    # success
    svc.delete_all_for_group = AsyncMock(return_value=None)
    assert await delete_all_agents(service=svc, group_context=gc("admin")) is None

