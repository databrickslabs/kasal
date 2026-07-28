"""Role gates on the flow catalog.

Operators may run a flow and read its results; they may not author one. These
cases pin the "operator cannot delete or publish" rule that the crew catalog
already had and the flow catalog did not — the UI hid nothing, so an operator
saw delete/publish controls that the API happily executed.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.api.flows_router import (
    create_flow,
    delete_all_flows,
    delete_flow,
    publish_flow,
    unpublish_flow,
    update_flow,
    update_flow_publication,
)
from src.core.exceptions import ForbiddenError
from src.schemas.crew_publication import CrewPublicationCreate, CrewPublicationUpdate
from src.schemas.flow import FlowCreate, FlowUpdate
from src.utils.user_context import GroupContext


def gc(role="operator"):
    return GroupContext(
        group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role=role
    )


def make_flow():
    from datetime import datetime

    now = datetime.utcnow()
    return SimpleNamespace(
        id=uuid4(),
        name="F",
        crew_id=uuid4(),
        nodes=[],
        edges=[],
        flow_config={},
        created_at=now,
        updated_at=now,
    )


def _flow_create():
    return FlowCreate(name="F", crew_id=uuid4(), nodes=[], edges=[], flow_config={})


@pytest.mark.asyncio
async def test_operator_cannot_create_update_or_delete_a_flow():
    svc = AsyncMock()

    with pytest.raises(ForbiddenError):
        await create_flow(_flow_create(), service=svc, group_context=gc())

    with pytest.raises(ForbiddenError):
        await update_flow(
            uuid4(), FlowUpdate(name="X"), service=svc, group_context=gc()
        )

    with pytest.raises(ForbiddenError):
        await delete_flow(uuid4(), service=svc, group_context=gc())

    # Nothing reached the service — the gate is before any work.
    svc.create_flow_with_group.assert_not_awaited()
    svc.update_flow_with_group_check.assert_not_awaited()
    svc.force_delete_flow_with_executions_with_group_check.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_all_flows_is_admin_only():
    svc = AsyncMock()

    for role in ("operator", "editor"):
        with pytest.raises(ForbiddenError):
            await delete_all_flows(service=svc, group_context=gc(role))

    svc.delete_all_flows_for_group = AsyncMock(return_value=None)
    result = await delete_all_flows(service=svc, group_context=gc("admin"))
    assert result["status"] == "success"


@pytest.mark.asyncio
async def test_operator_cannot_publish_change_or_unpublish_a_flow():
    """Publication is the highest-consequence action here: it makes the flow
    callable from outside the workspace."""
    flow_svc = AsyncMock()
    pub_svc = AsyncMock()

    with pytest.raises(ForbiddenError):
        await publish_flow(
            uuid4(),
            CrewPublicationCreate(
                external_name="f", description="d", protocols=["mcp"]
            ),
            flow_service=flow_svc,
            service=pub_svc,
            group_context=gc(),
        )

    with pytest.raises(ForbiddenError):
        await update_flow_publication(
            uuid4(),
            CrewPublicationUpdate(protocols=["mcp"]),
            service=pub_svc,
            group_context=gc(),
        )

    with pytest.raises(ForbiddenError):
        await unpublish_flow(uuid4(), service=pub_svc, group_context=gc())

    pub_svc.publish.assert_not_awaited()
    pub_svc.update.assert_not_awaited()
    pub_svc.unpublish.assert_not_awaited()


@pytest.mark.asyncio
async def test_editor_can_still_publish_and_delete():
    """The gate must not lock out the roles that are supposed to author."""
    flow_svc = AsyncMock()
    flow = make_flow()
    flow_svc.get_flow_with_group_check = AsyncMock(return_value=flow)
    flow_svc.force_delete_flow_with_executions_with_group_check = AsyncMock(
        return_value=True
    )

    deleted = await delete_flow(flow.id, service=flow_svc, group_context=gc("editor"))
    assert deleted["status"] == "success"
