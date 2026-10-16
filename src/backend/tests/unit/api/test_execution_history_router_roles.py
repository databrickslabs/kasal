"""Who may delete run history.

Wiping a workspace's history (runs, traces, logs and its recipe library) is an
admin action and needs one selected workspace. Deleting a single run needs
editor or admin. Operators run and monitor; they delete nothing.
"""

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.execution_history_router import router
from src.dependencies.providers import get_group_context
from src.schemas.execution_history import DeleteResponse
from src.services.execution.history import get_execution_history_service
from src.utils.user_context import GroupContext
from tests.unit.api.conftest import register_exception_handlers

_DELETED = DeleteResponse(message="deleted")


def _client(role, group_ids=("team_1",)):
    service = AsyncMock()
    service.delete_all_executions.return_value = _DELETED
    service.delete_execution.return_value = _DELETED
    service.delete_execution_by_job_id.return_value = _DELETED
    context = GroupContext(
        group_ids=list(group_ids), group_email="member@example.com", user_role=role
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    async def _service():
        return service

    async def _context():
        return context

    app.dependency_overrides[get_execution_history_service] = _service
    app.dependency_overrides[get_group_context] = _context
    return TestClient(app), service


@pytest.mark.parametrize("role", ["operator", "editor"])
def test_only_admins_wipe_the_history(role):
    client, service = _client(role)

    response = client.delete("/executions/history")

    assert response.status_code == 403
    service.delete_all_executions.assert_not_called()


def test_admin_wipes_the_selected_workspace():
    client, service = _client("admin")

    response = client.delete("/executions/history")

    assert response.status_code == 200
    service.delete_all_executions.assert_awaited_once_with(group_ids=["team_1"])


def test_wipe_needs_one_selected_workspace():
    """With no workspace selected the scope is every membership; refuse it."""
    client, service = _client("admin", group_ids=("team_1", "team_2"))

    response = client.delete("/executions/history")

    assert response.status_code == 400
    service.delete_all_executions.assert_not_called()


def test_wipe_refuses_a_single_run_request():
    """``?job_id=`` asks for one run; this route would delete them all."""
    client, service = _client("admin")

    response = client.delete("/executions/history?job_id=job-1")

    assert response.status_code == 400
    service.delete_all_executions.assert_not_called()


@pytest.mark.parametrize("path", ["/executions/history/7", "/executions/job-1"])
def test_operator_cannot_delete_a_run(path):
    client, service = _client("operator")

    response = client.delete(path)

    assert response.status_code == 403
    service.delete_execution.assert_not_called()
    service.delete_execution_by_job_id.assert_not_called()


@pytest.mark.parametrize("path", ["/executions/history/7", "/executions/job-1"])
def test_editor_deletes_a_run(path):
    client, _ = _client("editor")

    assert client.delete(path).status_code == 200
