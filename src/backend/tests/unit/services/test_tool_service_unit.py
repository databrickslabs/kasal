import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.services.tool_service import ToolService
from src.utils.user_context import GroupContext


from datetime import datetime

def mk_tool(id=1, title="T", enabled=True, group_id=None, config=None, icon="i", description="d"):
    now = datetime.utcnow()
    return SimpleNamespace(id=id, title=title, enabled=enabled, group_id=group_id, config=config or {}, icon=icon, description=description, created_at=now, updated_at=now)


@pytest.mark.asyncio
async def test_get_all_tools_for_group_override_logic():
    svc = ToolService(session=SimpleNamespace())
    # Patch repository on service
    t_base = mk_tool(1, title="A", group_id=None)
    t_group = mk_tool(2, title="A", group_id="g1", enabled=False)
    t_other = mk_tool(3, title="B", group_id=None)
    svc.repository = AsyncMock()
    svc.repository.list = AsyncMock(return_value=[t_base, t_group, t_other])

    gc = GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin")
    resp = await svc.get_all_tools_for_group(gc)
    titles = sorted([t.title for t in resp.tools])
    # group version of A should override base; B should remain
    assert titles == ["A", "B"]
    assert any(getattr(t, "id", None) == 2 for t in resp.tools)

    # no group context -> only base tools
    resp2 = await svc.get_all_tools_for_group(None)
    assert all(getattr(t, "group_id", None) is None for t in resp2.tools)


@pytest.mark.asyncio
async def test_get_enabled_tools_for_group_merges_config_and_filters():
    svc = ToolService(session=SimpleNamespace())
    base1 = mk_tool(1, title="A", group_id=None, config={"x": 1}, enabled=True)
    base2 = mk_tool(2, title="B", group_id=None, config={"y": 2}, enabled=True)
    groupA = SimpleNamespace(tool_id=1, config={"x": 9, "z": 3})
    # base3 disabled should be filtered
    base3 = mk_tool(3, title="C", group_id=None, enabled=False)

    svc.repository = AsyncMock()
    svc.repository.find_enabled = AsyncMock(return_value=[base1, base2])

    # Patch GroupToolRepository used inside method by monkeypatching attribute on service module class instance
    from src.services import tool_service as module
    class FakeGRepo:
        def __init__(self, session):
            self.session = session
        async def list_enabled_for_group(self, gid):
            assert gid == "g1"
            return [groupA]
    module.GroupToolRepository = FakeGRepo

    gc = GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin")
    # primary_group_id is a property in dataclass via group_ids[0]
    out = await svc.get_enabled_tools_for_group(gc)
    # Only A is mapped; B should be excluded; config should merge and prefer group
    assert out.count == 1
    t = out.tools[0]
    assert t.title == "A" and t.config.get("x") == 9 and t.config.get("z") == 3


from fastapi import HTTPException
from src.schemas.tool import ToolListResponse, ToolResponse, ToolUpdate


@pytest.mark.asyncio
async def test_get_tool_by_id_and_with_group_check_paths():
    svc = ToolService(session=SimpleNamespace())
    svc.repository = AsyncMock()

    # not found
    svc.repository.get = AsyncMock(return_value=None)
    with pytest.raises(HTTPException) as ei:
        await svc.get_tool_by_id(123)
    assert ei.value.status_code == 404

    # default tool accessible to anyone
    tool_default = mk_tool(1, title="D", group_id=None)
    svc.repository.get = AsyncMock(return_value=tool_default)
    out = await svc.get_tool_with_group_check(1, group_context=None)
    assert out.id == 1 and out.title == "D"

    # group-specific tool forbidden for other groups (returns 404)
    tool_g2 = mk_tool(2, title="G", group_id="g2")
    svc.repository.get = AsyncMock(return_value=tool_g2)
    with pytest.raises(HTTPException) as ei2:
        await svc.get_tool_with_group_check(2, group_context=GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="editor"))
    assert ei2.value.status_code == 404


@pytest.mark.asyncio
async def test_update_and_delete_tool_paths():
    svc = ToolService(session=SimpleNamespace())
    svc.repository = AsyncMock()

    # update: not found
    svc.repository.get = AsyncMock(return_value=None)
    with pytest.raises(HTTPException):
        await svc.update_tool(5, ToolUpdate(description="z"))

    # update: success
    existing = mk_tool(5, title="E")
    updated = mk_tool(5, title="E", description="z")
    svc.repository.get = AsyncMock(return_value=existing)
    svc.repository.update = AsyncMock(return_value=updated)
    out = await svc.update_tool(5, ToolUpdate(description="z"))
    assert out.description == "z"

    # update with group check: forbidden
    t_g2 = mk_tool(7, title="TG", group_id="g2")
    svc.repository.get = AsyncMock(return_value=t_g2)
    with pytest.raises(HTTPException) as ei:
        await svc.update_tool_with_group_check(7, ToolUpdate(description="x"), GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="editor"))
    assert ei.value.status_code == 404

    # update with group check: success
    t_g1 = mk_tool(8, title="TG1", group_id="g1")
    svc.repository.get = AsyncMock(return_value=t_g1)
    svc.repository.update = AsyncMock(return_value=mk_tool(8, title="TG1", group_id="g1", description="ok"))
    out2 = await svc.update_tool_with_group_check(8, ToolUpdate(description="ok"), GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="editor"))
    assert out2.description == "ok"

    # delete: not found
    svc.repository.get = AsyncMock(return_value=None)
    with pytest.raises(HTTPException):
        await svc.delete_tool(10)

    # delete: success
    svc.repository.get = AsyncMock(return_value=mk_tool(10))
    svc.repository.delete = AsyncMock(return_value=True)
    assert await svc.delete_tool(10) is True

    # delete with group check: forbidden
    svc.repository.get = AsyncMock(return_value=mk_tool(11, group_id="g2"))
    with pytest.raises(HTTPException) as ei3:
        await svc.delete_tool_with_group_check(11, GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="editor"))
    assert ei3.value.status_code == 404

    # delete with group check: success
    svc.repository.get = AsyncMock(return_value=mk_tool(12, group_id="g1"))
    svc.repository.delete = AsyncMock(return_value=True)
    assert await svc.delete_tool_with_group_check(12, GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="editor")) is True


@pytest.mark.asyncio
async def test_toggle_paths_base_and_group():
    svc = ToolService(session=SimpleNamespace())
    svc.repository = AsyncMock()

    # toggle (simple): not found
    svc.repository.toggle_enabled = AsyncMock(return_value=None)
    with pytest.raises(HTTPException) as ei:
        await svc.toggle_tool_enabled(1)
    assert ei.value.status_code == 404

    # toggle (simple): success
    toggled = mk_tool(1, enabled=False)
    svc.repository.toggle_enabled = AsyncMock(return_value=toggled)
    res = await svc.toggle_tool_enabled(1)
    assert res.enabled is False and "successfully" in res.message

    # toggle with group check: base tool -> create copy when no existing mapping
    base = mk_tool(20, title="B", group_id=None, enabled=True)
    svc.repository.get = AsyncMock(return_value=base)
    from src.services import tool_service as module
    svc.repository.find_by_title_and_group = AsyncMock(return_value=None)
    svc.repository.create = AsyncMock(return_value=mk_tool(21, title="B", group_id="g1", enabled=False))
    out = await svc.toggle_tool_enabled_with_group_check(20, GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin"))
    assert out.enabled is False

    # toggle with group check: base tool -> toggle existing group tool
    svc.repository.get = AsyncMock(return_value=base)
    existing_group_tool = mk_tool(22, title="B", group_id="g1", enabled=True)
    svc.repository.find_by_title_and_group = AsyncMock(return_value=existing_group_tool)
    svc.repository.toggle_enabled = AsyncMock(return_value=mk_tool(22, title="B", group_id="g1", enabled=False))
    out2 = await svc.toggle_tool_enabled_with_group_check(20, GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin"))
    assert out2.enabled is False

    # toggle with group check: group tool forbidden for other groups
    svc.repository.get = AsyncMock(return_value=mk_tool(30, title="G", group_id="g2", enabled=True))
    with pytest.raises(HTTPException) as ei2:
        await svc.toggle_tool_enabled_with_group_check(30, GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="editor"))
    assert ei2.value.status_code == 404


@pytest.mark.asyncio
async def test_config_endpoints():
    svc = ToolService(session=SimpleNamespace())
    svc.repository = AsyncMock()

    # get_tool_config_by_name
    svc.repository.find_by_title = AsyncMock(return_value=None)
    assert await svc.get_tool_config_by_name("X") is None
    svc.repository.find_by_title = AsyncMock(return_value=mk_tool(1, title="X", config={"a": 1}))
    assert (await svc.get_tool_config_by_name("X")) == {"a": 1}

    # update_tool_configuration_by_title not found
    svc.repository.update_configuration_by_title = AsyncMock(return_value=None)
    with pytest.raises(HTTPException) as ei:
        await svc.update_tool_configuration_by_title("Y", {"b": 2})
    assert ei.value.status_code == 404

    # update_tool_configuration_by_title success
    svc.repository.update_configuration_by_title = AsyncMock(return_value=mk_tool(2, title="Y", config={"b": 2}))
    out = await svc.update_tool_configuration_by_title("Y", {"b": 2})
    assert out.config.get("b") == 2

    # get_all_tool_configurations_for_group
    tool1 = ToolResponse.model_validate(mk_tool(3, title="A", config={"x": 1}))
    tool2 = ToolResponse.model_validate(mk_tool(4, title="B", config={"y": 2}))
    svc.get_all_tools_for_group = AsyncMock(return_value=ToolListResponse(tools=[tool1, tool2], count=2))
    cfgs = await svc.get_all_tool_configurations_for_group(GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin"))
    assert cfgs == {"A": {"x": 1}, "B": {"y": 2}}

    # get_tool_configuration_with_group_check prefers group
    svc.repository.find_by_title_and_group = AsyncMock(return_value=mk_tool(5, title="A", group_id="g1", config={"g": 9}))
    assert (await svc.get_tool_configuration_with_group_check("A", GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin"))) == {"g": 9}
    # falls back to base
    svc.repository.find_by_title_and_group = AsyncMock(return_value=None)
    svc.repository.find_base_by_title = AsyncMock(return_value=mk_tool(6, title="A", group_id=None, config={"b": 1}))
    assert (await svc.get_tool_configuration_with_group_check("A", GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin"))) == {"b": 1}

    # update_tool_configuration_group_scoped requires group context
    with pytest.raises(HTTPException) as ei2:
        await svc.update_tool_configuration_group_scoped("A", {"q": 1}, None)
    assert ei2.value.status_code == 403

    # update existing group-specific tool
    svc.repository.find_by_title_and_group = AsyncMock(return_value=mk_tool(7, title="A", group_id="g1", config={"old": 0}))
    svc.repository.update_configuration_for_title_and_group = AsyncMock(return_value=mk_tool(7, title="A", group_id="g1", config={"q": 1}))
    out2 = await svc.update_tool_configuration_group_scoped("A", {"q": 1}, GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin"))
    assert out2.config == {"q": 1}

    # create new group-specific from base when none exists
    svc.repository.find_by_title_and_group = AsyncMock(return_value=None)
    svc.repository.find_base_by_title = AsyncMock(return_value=mk_tool(8, title="A", group_id=None, config={"base": True}, enabled=True))
    svc.repository.create = AsyncMock(return_value=mk_tool(9, title="A", group_id="g1", config={"q": 2}))
    out3 = await svc.update_tool_configuration_group_scoped("A", {"q": 2}, GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin"))
    assert out3.config == {"q": 2}

    # create brand new tool when neither group nor base exists
    svc.repository.find_by_title_and_group = AsyncMock(return_value=None)
    svc.repository.find_base_by_title = AsyncMock(return_value=None)
    svc.repository.create = AsyncMock(return_value=mk_tool(10, title="A", group_id="g1", config={"n": 1}, enabled=True))
    out4 = await svc.update_tool_configuration_group_scoped("A", {"n": 1}, GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin"))
    assert out4.config == {"n": 1}
