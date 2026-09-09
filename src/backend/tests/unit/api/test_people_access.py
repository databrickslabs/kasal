from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.users_router import get_user_service, router
from src.db.database_router import get_smart_db_session
from src.dependencies.admin_auth import require_authenticated_user
from src.dependencies.providers import get_group_context


@pytest.fixture
def setup():
    user = SimpleNamespace(
        id="admin",
        email="admin@example.com",
        username="admin",
        is_system_admin=True,
        is_personal_workspace_manager=True,
        role="regular",
        status="active",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        last_login=None,
    )
    app = FastAPI()
    app.include_router(router)
    service = AsyncMock()
    service.provision_user.return_value = user
    service.get_user_complete.return_value = user
    app.dependency_overrides[get_user_service] = lambda: service
    app.dependency_overrides[require_authenticated_user] = lambda: user
    app.dependency_overrides[get_smart_db_session] = lambda: AsyncMock()
    app.dependency_overrides[get_group_context] = lambda: SimpleNamespace(
        group_email=user.email, current_user=user
    )
    with patch(
        "src.dependencies.admin_auth.require_authenticated_user",
        AsyncMock(return_value=user),
    ):
        yield TestClient(app), service, user


def test_me_records_only_the_authenticated_identity(setup):
    client, service, user = setup
    assert client.get("/users/me").status_code == 200
    service.record_login.assert_awaited_once_with(user.id)


def test_system_admin_can_add_without_recording_target_login(setup):
    client, service, _ = setup
    assert client.post("/users", json={"email": "new@example.com"}).status_code == 200
    service.provision_user.assert_awaited_once_with("new@example.com")
    service.record_login.assert_not_awaited()


@pytest.mark.parametrize(
    "payload",
    [{"email": "user@"}, {"email": "new@example.com", "is_system_admin": True}],
)
def test_provision_rejects_invalid_identity_and_permission_injection(setup, payload):
    client, service, _ = setup
    assert client.post("/users", json=payload).status_code == 422
    service.provision_user.assert_not_awaited()


@pytest.mark.parametrize(
    "path,method", [("/users", "post"), ("/users/directory?search=ada", "get")]
)
def test_workspace_admin_cannot_provision_or_search_directory(setup, path, method):
    client, service, user = setup
    user.is_system_admin = False
    user.role = "admin"
    response = getattr(client, method)(
        path, **({"json": {"email": "new@example.com"}} if method == "post" else {})
    )
    assert response.status_code == 403
    service.provision_user.assert_not_awaited()
