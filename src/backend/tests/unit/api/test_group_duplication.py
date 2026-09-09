from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.group_router import get_duplication_service, router
from src.db.database_router import get_smart_db_session
from src.dependencies.providers import get_group_context


@pytest.fixture
def setup():
    actor = SimpleNamespace(id="admin", email="admin@example.com", is_system_admin=True)
    app = FastAPI()
    app.include_router(router)
    service = AsyncMock()
    service.duplicate.return_value = {
        "id": "new",
        "name": "Copy",
        "status": "active",
        "auto_created": False,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "user_count": 1,
    }
    app.dependency_overrides[get_duplication_service] = lambda: service
    app.dependency_overrides[get_smart_db_session] = lambda: AsyncMock()
    app.dependency_overrides[get_group_context] = lambda: SimpleNamespace(
        group_email=actor.email
    )
    with patch(
        "src.dependencies.admin_auth.require_authenticated_user",
        AsyncMock(return_value=actor),
    ):
        yield TestClient(app), service, actor


def test_admin_duplicates_the_explicit_source(setup):
    client, service, actor = setup
    response = client.post(
        "/groups/source/duplicate", json={"name": "Copy", "include_members": False}
    )
    assert response.status_code == 201
    source, request, caller = service.duplicate.call_args.args
    assert source == "source"
    assert request.include_members is False
    assert caller is actor


def test_non_system_admin_cannot_copy_credentials_or_members(setup):
    client, service, actor = setup
    actor.is_system_admin = False
    response = client.post("/groups/source/duplicate", json={"name": "Copy"})
    assert response.status_code == 403
    service.duplicate.assert_not_awaited()


@pytest.mark.parametrize(
    "payload",
    [{"name": "   "}, {"name": "x" * 81}, {"name": "Copy", "target_group_id": "other"}],
)
def test_invalid_copy_requests_are_rejected(setup, payload):
    client, service, _ = setup
    assert client.post("/groups/source/duplicate", json=payload).status_code == 422
    service.duplicate.assert_not_awaited()
