"""Chat ``/delete crew|flow``: editors and admins only, exact names, confirmed.

Driven through ``DispatcherService.dispatch`` with the real slash-command
parser, so these cover the path a chat message takes.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.schemas.dispatcher import DispatcherRequest
from src.services.chat.dispatcher import DispatcherService
from src.services.chat.slash_commands import detect_slash_command
from src.utils.user_context import GroupContext

CREW_ID = "0b6c1f0e-4a8e-4d51-9a43-6f0c2b7d9e11"
FLOW_ID = "5d2a9c3e-7b14-4f6a-8e25-1c9d0b3a7f42"


def _context(role):
    return GroupContext(
        group_ids=["team_1"], group_email="member@example.com", user_role=role
    )


def _item(item_id, name, updated=datetime(2026, 1, 2)):
    return SimpleNamespace(
        id=item_id,
        name=name,
        created_at=datetime(2026, 1, 1),
        updated_at=updated,
        nodes=[],
        edges=[],
        flow_config={},
    )


def _service(crews=(), flows=()):
    svc = DispatcherService.__new__(DispatcherService)
    svc.session = MagicMock()
    svc.log_service = AsyncMock()
    svc.template_service = AsyncMock()
    svc.agent_service = AsyncMock()
    svc.task_service = AsyncMock()
    svc.crew_service = AsyncMock()
    svc.catalog_service = AsyncMock()
    svc.catalog_service.find_by_group = AsyncMock(return_value=list(crews))
    svc.catalog_service.delete_by_group = AsyncMock(return_value=True)
    svc.flow_service = AsyncMock()
    svc.flow_service.get_all_flows_for_group = AsyncMock(return_value=list(flows))
    svc.flow_service.force_delete_flow_with_executions_with_group_check = AsyncMock(
        return_value=True
    )
    svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
    svc._log_llm_interaction = AsyncMock()
    return svc


async def _send(svc, message, role):
    svc.detect_intent = AsyncMock(return_value=detect_slash_command(message))
    request = DispatcherRequest(message=message, model="m")
    result = await svc.dispatch(request, group_context=_context(role))
    return result["generation_result"]


def _deleted(svc):
    return (
        svc.catalog_service.delete_by_group.await_count
        + svc.flow_service.force_delete_flow_with_executions_with_group_check.await_count
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    [
        "/delete crew Weekly Report",
        "/delete flow Weekly Report",
        "/delete crew Weekly Report --confirm",
        "/delete flow Weekly Report --confirm",
    ],
)
async def test_operator_delete_is_refused(message):
    svc = _service(
        crews=[_item(CREW_ID, "Weekly Report")],
        flows=[_item(FLOW_ID, "Weekly Report")],
    )

    result = await _send(svc, message, "operator")

    assert _deleted(svc) == 0
    assert "Only editors and admins" in result["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message", ["/delete flow report", "/delete flow report --confirm"]
)
async def test_editor_partial_name_deletes_nothing(message):
    svc = _service(flows=[_item(FLOW_ID, "Quarterly Revenue Report")])

    result = await _send(svc, message, "editor")

    assert _deleted(svc) == 0
    assert "Quarterly Revenue Report" in result["message"]


@pytest.mark.asyncio
async def test_editor_exact_name_asks_for_confirmation_first():
    svc = _service(crews=[_item(CREW_ID, "Weekly Report")])

    result = await _send(svc, "/delete crew Weekly Report", "editor")

    assert _deleted(svc) == 0
    assert f"/delete crew {CREW_ID} --confirm" in result["message"]


@pytest.mark.asyncio
async def test_editor_confirmed_exact_name_deletes_case_insensitively():
    svc = _service(crews=[_item(CREW_ID, "Weekly Report")])

    result = await _send(svc, "/delete crew weekly report --confirm", "editor")

    svc.catalog_service.delete_by_group.assert_awaited_once()
    assert svc.catalog_service.delete_by_group.await_args.args[0] == CREW_ID
    assert "has been deleted" in result["message"]


@pytest.mark.asyncio
async def test_admin_confirmed_flow_delete_by_id():
    svc = _service(flows=[_item(FLOW_ID, "Weekly Report")])

    result = await _send(svc, f"/delete flow {FLOW_ID} --confirm", "admin")

    deleter = svc.flow_service.force_delete_flow_with_executions_with_group_check
    deleter.assert_awaited_once()
    assert deleter.await_args.args[0] == FLOW_ID
    assert "has been deleted" in result["message"]


@pytest.mark.asyncio
async def test_duplicate_names_delete_nothing_and_list_ids():
    other = "9e8d7c6b-5a4f-4e3d-8c2b-1a0f9e8d7c6b"
    svc = _service(
        crews=[
            _item(CREW_ID, "Weekly Report", updated=datetime(2026, 1, 1)),
            _item(other, "weekly report", updated=datetime(2026, 1, 5)),
        ]
    )

    result = await _send(svc, "/delete crew Weekly Report --confirm", "editor")

    assert _deleted(svc) == 0
    assert CREW_ID in result["message"] and other in result["message"]


@pytest.mark.asyncio
async def test_help_lists_delete_only_for_editors_and_admins():
    svc = _service()

    operator_help = await _send(svc, "/help", "operator")
    editor_help = await _send(svc, "/help", "editor")

    assert "/delete" not in operator_help["message"]
    assert "/delete crew" in editor_help["message"]
