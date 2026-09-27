"""``get_admin_user`` authorises on the CURRENT workspace, never "admin anywhere".

Audit V4-5: ``highest_role`` ("admin in any group") used to satisfy
``get_admin_user``, so an admin of team A passed admin checks while working in
team B. The contexts here come from the real ``GroupContext._scope_for_selection``
so the test exercises the role the request would actually carry.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from src.api.group_router import router
from src.db.database_router import get_smart_db_session
from src.dependencies.admin_auth import get_admin_user
from src.dependencies.providers import get_group_context
from src.models.enums import GroupStatus
from src.utils.user_context import GroupContext

TEAM_A = "team_a"
TEAM_B = "team_b"
PERSONAL = "user_member_example_com"
ROLES = {TEAM_A: "admin", TEAM_B: "operator"}  # admin of A, not of B


def _user(is_system_admin=False):
    return SimpleNamespace(
        id="u-1",
        email="member@example.com",
        is_system_admin=is_system_admin,
        is_personal_workspace_manager=False,
    )


def _context(user, selected, roles=ROLES):
    group_ids, user_role = GroupContext._scope_for_selection(
        user.email, selected, user, roles, PERSONAL
    )
    return GroupContext(
        group_ids=group_ids,
        group_email=user.email,
        user_role=user_role,
        highest_role="admin" if "admin" in roles.values() else None,
        current_user=user,
    )


async def _run(user, ctx):
    with patch(
        "src.dependencies.admin_auth.require_authenticated_user",
        new=AsyncMock(return_value=user),
    ):
        return await get_admin_user(AsyncMock(), ctx)


class TestDependency:
    @pytest.mark.asyncio
    async def test_admin_of_a_is_refused_in_b(self):
        user = _user()
        ctx = _context(user, TEAM_B)
        assert ctx.highest_role == "admin"  # the old, too-wide signal
        with pytest.raises(HTTPException) as exc:
            await _run(user, ctx)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_of_a_is_refused_in_personal_workspace(self):
        user = _user()
        with pytest.raises(HTTPException) as exc:
            await _run(user, _context(user, PERSONAL))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_of_a_is_refused_with_no_selection(self):
        # No selection = union of workspaces, least-privileged role (operator).
        user = _user()
        with pytest.raises(HTTPException) as exc:
            await _run(user, _context(user, None))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_of_a_passes_in_a(self):
        user = _user()
        assert await _run(user, _context(user, TEAM_A)) is user

    @pytest.mark.asyncio
    async def test_system_admin_passes_in_a_workspace_they_do_not_administer(self):
        user = _user(is_system_admin=True)
        assert await _run(user, _context(user, TEAM_B)) is user


# ---------------------------------------------------------------------------
# Through the router: the real dependency, only identity and data mocked.
# ---------------------------------------------------------------------------


def _client(user, ctx, service):
    app = FastAPI()
    app.include_router(router)
    from tests.unit.api.conftest import register_exception_handlers

    register_exception_handlers(app)
    app.dependency_overrides[get_smart_db_session] = lambda: MagicMock()
    app.dependency_overrides[get_group_context] = lambda: ctx
    # get_admin_user calls require_authenticated_user directly, not via Depends.
    patchers = [
        patch("src.api.group_router.GroupService", return_value=service),
        patch(
            "src.dependencies.admin_auth.require_authenticated_user",
            new=AsyncMock(return_value=user),
        ),
    ]
    for patcher in patchers:
        patcher.start()
    return TestClient(app), _Stop(patchers)


class _Stop:
    def __init__(self, patchers):
        self._patchers = patchers

    def stop(self):
        for patcher in self._patchers:
            patcher.stop()


def _service(target_role):
    service = AsyncMock()
    group = MagicMock(
        id=TEAM_A,
        status=GroupStatus.ACTIVE,
        description="",
        auto_created=False,
        created_by_email=None,
    )
    group.name = "Team A"
    group.created_at = group.updated_at = datetime(2026, 1, 1)
    service.get_group_by_id.return_value = group
    service.create_group.return_value = group
    service.get_group_user_count.return_value = 1
    service.get_user_group_membership.return_value = (
        SimpleNamespace(role=target_role) if target_role else None
    )
    return service


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("get", f"/groups/{TEAM_A}", None),
        ("put", f"/groups/{TEAM_A}", {"description": "x"}),
        ("post", "/groups/", {"name": "New team"}),
    ],
)
def test_route_refuses_admin_of_a_working_in_b(method, path, body):
    user = _user()
    client, patcher = _client(user, _context(user, TEAM_B), _service("admin"))
    try:
        response = client.request(method, path, json=body)
    finally:
        patcher.stop()
    assert response.status_code == 403


def test_get_group_refuses_admin_of_current_workspace_reading_another():
    """Audit L1: admin of B, B selected, reading A (not a member) is refused."""
    user = _user()
    roles = {TEAM_A: "operator", TEAM_B: "admin"}
    client, patcher = _client(user, _context(user, TEAM_B, roles), _service(None))
    try:
        response = client.get(f"/groups/{TEAM_A}")
    finally:
        patcher.stop()
    assert response.status_code == 403


@pytest.mark.parametrize("system_admin", [False, True])
def test_get_group_allows_legitimate_admins(system_admin):
    user = _user(is_system_admin=system_admin)
    selected = TEAM_B if system_admin else TEAM_A
    client, patcher = _client(user, _context(user, selected), _service("admin"))
    try:
        response = client.get(f"/groups/{TEAM_A}")
    finally:
        patcher.stop()
    assert response.status_code == 200
    assert response.json()["id"] == TEAM_A


def test_create_group_allows_admin_of_current_workspace():
    user = _user()
    client, patcher = _client(user, _context(user, TEAM_A), _service("admin"))
    try:
        response = client.post("/groups/", json={"name": "New team"})
    finally:
        patcher.stop()
    assert response.status_code == 201
