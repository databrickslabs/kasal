import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from datetime import datetime
from uuid import uuid4

from src.api.flows_router import (
    get_all_flows,
    get_flow,
    create_flow,
    debug_flow_data,
    update_flow,
    delete_flow,
    delete_all_flows,
)
from src.schemas.flow import FlowCreate, FlowUpdate
from src.utils.user_context import GroupContext


def gc():
    return GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role="admin")


def make_flow(i=None):
    now = datetime.utcnow()
    return SimpleNamespace(
        id=(i or uuid4()),
        name="F",
        crew_id=uuid4(),
        nodes=[],
        edges=[],
        flow_config={},
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_list_get_create_update_delete_paths():
    svc = AsyncMock()

    # list
    svc.get_all_flows_for_group = AsyncMock(return_value=[make_flow()])
    flows = await get_all_flows(service=svc, group_context=gc())
    assert isinstance(flows, list) and flows[0].name == "F"

    # get
    f = make_flow()
    svc.get_flow_with_group_check = AsyncMock(return_value=f)
    got = await get_flow(f.id, service=svc, group_context=gc())
    assert got.name == "F"

    # create
    svc.create_flow_with_group = AsyncMock(return_value=make_flow())
    created = await create_flow(FlowCreate(name="F", crew_id=uuid4(), nodes=[], edges=[], flow_config={}), service=svc, group_context=gc())
    assert created.name == "F"

    # debug
    svc.validate_flow_data = AsyncMock(return_value={"ok": True})
    dbg = await debug_flow_data(FlowCreate(name="F", crew_id=uuid4(), nodes=[], edges=[], flow_config={}), service=svc, group_context=gc())
    assert dbg["ok"] is True

    # update
    svc.update_flow_with_group_check = AsyncMock(return_value=make_flow())
    upd = await update_flow(uuid4(), FlowUpdate(name="X"), service=svc, group_context=gc())
    assert upd.name in ("F", "X")

    # delete
    svc.force_delete_flow_with_executions_with_group_check = AsyncMock(return_value=True)
    resp = await delete_flow(uuid4(), service=svc, group_context=gc())
    assert resp["status"] == "success"

    # delete all
    svc.delete_all_flows_for_group = AsyncMock(return_value=None)
    resp2 = await delete_all_flows(service=svc, group_context=gc())
    assert resp2["status"] == "success"

