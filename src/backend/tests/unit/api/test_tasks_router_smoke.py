import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from datetime import datetime

from src.api.tasks_router import (
    list_tasks,
    create_task,
    get_task,
    update_task_full,
    update_task,
    delete_task,
    delete_all_tasks,
)
from src.schemas.task import TaskCreate, TaskUpdate
from src.utils.user_context import GroupContext


def gc(role="user"):
    return GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role=role)


def make_task(i="t1"):
    now = datetime.utcnow()
    return SimpleNamespace(id=i, title="T", description="d", created_at=now, updated_at=now, tool_configs={})


@pytest.mark.asyncio
async def test_list_and_create_permissions_and_success():
    svc = AsyncMock()

    svc.find_by_group = AsyncMock(return_value=[make_task("t1")])
    out = await list_tasks(service=svc, group_context=gc("operator"))
    assert isinstance(out, list) and out[0].id == "t1"

    # create forbidden for user
    with pytest.raises(Exception):
        await create_task(TaskCreate(name="T", description="d", expected_output="o", agent_id=None), service=svc, group_context=gc("user"))

    # create success for editor
    svc.create_with_group = AsyncMock(return_value=make_task("t2"))
    out2 = await create_task(TaskCreate(name="T", description="d", expected_output="o", agent_id=None), service=svc, group_context=gc("editor"))
    assert out2.id == "t2"


@pytest.mark.asyncio
async def test_get_update_delete_paths():
    svc = AsyncMock()

    # get 404 and success
    svc.get_with_group_check = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await get_task("missing", service=svc, group_context=gc("operator"))
    svc.get_with_group_check = AsyncMock(return_value=make_task("t3"))
    assert (await get_task("t3", service=svc, group_context=gc("operator"))).id == "t3"

    # update full forbidden, 404, success
    with pytest.raises(Exception):
        await update_task_full("t3", {"title": "X"}, service=svc, group_context=gc("user"))
    svc.update_full_with_group_check = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await update_task_full("t3", {"title": "X"}, service=svc, group_context=gc("admin"))
    svc.update_full_with_group_check = AsyncMock(return_value=make_task("t3"))
    assert (await update_task_full("t3", {"title": "X"}, service=svc, group_context=gc("editor"))).id == "t3"

    # update partial forbidden, 404, success
    with pytest.raises(Exception):
        await update_task("t3", TaskUpdate(title="Y"), service=svc, group_context=gc("user"))
    svc.update_with_group_check = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await update_task("t3", TaskUpdate(title="Y"), service=svc, group_context=gc("editor"))
    svc.update_with_group_check = AsyncMock(return_value=make_task("t3"))
    assert (await update_task("t3", TaskUpdate(title="Y"), service=svc, group_context=gc("admin"))).id == "t3"

    # delete forbidden, 404, success
    with pytest.raises(Exception):
        await delete_task("t3", service=svc, group_context=gc("user"))
    svc.delete_with_group_check = AsyncMock(return_value=False)
    with pytest.raises(Exception):
        await delete_task("t3", service=svc, group_context=gc("editor"))
    svc.delete_with_group_check = AsyncMock(return_value=True)
    assert await delete_task("t3", service=svc, group_context=gc("admin")) is None


@pytest.mark.asyncio
async def test_delete_all_tasks_admin_and_error():
    svc = AsyncMock()

    # non-admin forbidden
    with pytest.raises(Exception):
        await delete_all_tasks(service=svc, group_context=gc("operator"))

    # admin ok
    svc.delete_all_for_group = AsyncMock(return_value=None)
    assert await delete_all_tasks(service=svc, group_context=gc("admin")) is None

