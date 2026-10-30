"""Who may wipe a workspace's traces: DELETE /traces/.

The same rule as wiping the run history: an admin of one selected workspace,
or anyone in their own personal workspace.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.execution_trace_router import get_execution_trace_service, router
from src.dependencies.providers import get_group_context
from src.schemas.execution_trace import DeleteTraceResponse
from src.utils.user_context import GroupContext
from tests.unit.api.conftest import register_exception_handlers

_PERSONAL = "user_7a1b2c3d4e5f40718293a4b5c6d7e8f9"


def _client(role, group_ids=("team_1",)):
    service = AsyncMock()
    service.delete_all_traces_for_group.return_value = DeleteTraceResponse(
        deleted_traces=3, message="deleted"
    )
    context = GroupContext(
        group_ids=list(group_ids),
        group_email="member@example.com",
        user_role=role,
        current_user=SimpleNamespace(
            is_system_admin=False,
            is_personal_workspace_manager=False,
            personal_group_id=_PERSONAL,
        ),
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.dependency_overrides[get_execution_trace_service] = lambda: service
    app.dependency_overrides[get_group_context] = lambda: context
    return TestClient(app), service


@pytest.mark.parametrize("role", ["operator", "editor"])
def test_only_admins_wipe_a_teams_traces(role):
    client, service = _client(role)

    assert client.delete("/traces/").status_code == 403
    service.delete_all_traces_for_group.assert_not_called()


def test_admin_wipes_the_selected_workspace():
    client, service = _client("admin")

    assert client.delete("/traces/").status_code == 200
    service.delete_all_traces_for_group.assert_awaited_once()


def test_a_user_clears_their_own_personal_traces():
    client, service = _client(None, group_ids=(_PERSONAL,))

    assert client.delete("/traces/").status_code == 200
    service.delete_all_traces_for_group.assert_awaited_once()


def test_wipe_needs_one_selected_workspace():
    client, service = _client("admin", group_ids=("team_1", "team_2"))

    assert client.delete("/traces/").status_code == 400
    service.delete_all_traces_for_group.assert_not_called()
