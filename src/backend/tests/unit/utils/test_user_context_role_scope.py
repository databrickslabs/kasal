"""The role in a GroupContext holds in every workspace its scope covers (M2, V3-3).

Before: selecting the personal workspace gave ``user_role = highest_role`` with
every team id in ``group_ids``, so a viewer in A who is admin in B passed admin
checks over A's data; and with no ``group_id`` header the FIRST membership's
role applied to the union of every workspace.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.core.permissions import check_role_in_context, get_effective_role
from src.utils.user_context import GroupContext, _least_privileged_role

EMAIL = "member@corp.com"
PERSONAL = "user_member_corp_com"


def _user(**flags):
    return SimpleNamespace(
        id="u1",
        email=EMAIL,
        personal_group_id=PERSONAL,
        is_system_admin=flags.get("system_admin", False),
        is_personal_workspace_manager=flags.get("manager", False),
    )


async def _context(memberships, group_id=None, **flags):
    groups = [(SimpleNamespace(id=gid, name=gid), role) for gid, role in memberships]
    with patch.object(
        GroupContext,
        "_get_user_group_memberships_with_roles",
        AsyncMock(return_value=(_user(**flags), groups)),
    ):
        return await GroupContext.from_email(EMAIL, group_id=group_id)


VIEWER_IN_A_ADMIN_IN_B = [("team_a", "operator"), ("team_b", "admin")]


@pytest.mark.asyncio
async def test_personal_workspace_is_personal_scope_only():
    ctx = await _context(VIEWER_IN_A_ADMIN_IN_B, group_id=PERSONAL)

    assert ctx.group_ids == [PERSONAL]
    assert ctx.user_role is None  # no team role applies here
    assert ctx.highest_role == "admin"  # still reported, never used for scope
    # Personal role: editor, as for users in no group and in the frontend.
    assert get_effective_role(ctx) == "editor"
    assert not check_role_in_context(ctx, ["admin"])


@pytest.mark.asyncio
@pytest.mark.parametrize("flag", ["manager", "system_admin"])
async def test_personal_workspace_manager_is_admin_of_it(flag):
    ctx = await _context(VIEWER_IN_A_ADMIN_IN_B, group_id=PERSONAL, **{flag: True})
    assert ctx.group_ids == [PERSONAL]
    assert get_effective_role(ctx) == "admin"


@pytest.mark.asyncio
@pytest.mark.parametrize("gid,role", [("team_a", "operator"), ("team_b", "admin")])
async def test_selected_team_workspace_uses_its_own_role(gid, role):
    ctx = await _context(VIEWER_IN_A_ADMIN_IN_B, group_id=gid)
    assert ctx.group_ids == [gid]
    assert get_effective_role(ctx) == role


@pytest.mark.asyncio
async def test_no_selection_takes_the_least_role_across_the_union():
    """The union stays (MCP/A2A identity-only callers list every teamspace),
    but an admin check must not pass over team_a, where the user is operator."""
    ctx = await _context(VIEWER_IN_A_ADMIN_IN_B)

    assert set(ctx.group_ids) == {"team_a", "team_b", PERSONAL}
    assert ctx.user_role == "operator"
    assert not check_role_in_context(ctx, ["admin", "editor"])
    assert check_role_in_context(ctx, ["admin", "editor", "operator"])


@pytest.mark.asyncio
async def test_no_selection_order_does_not_change_the_role():
    forward = await _context(VIEWER_IN_A_ADMIN_IN_B)
    backward = await _context(list(reversed(VIEWER_IN_A_ADMIN_IN_B)))
    assert forward.user_role == backward.user_role == "operator"


@pytest.mark.asyncio
async def test_no_selection_counts_the_personal_role():
    """Admin of every team, but not a personal-workspace manager: the personal
    workspace is in scope, where the user is an editor."""
    ctx = await _context([("team_a", "admin"), ("team_b", "admin")])
    assert ctx.user_role == "editor"

    managed = await _context([("team_a", "admin")], manager=True)
    assert managed.user_role == "admin"


@pytest.mark.asyncio
async def test_unknown_role_fails_closed_without_selection():
    ctx = await _context([("team_a", "admin"), ("team_b", None)])
    assert ctx.user_role is None
    assert get_effective_role(ctx) is None


@pytest.mark.asyncio
async def test_other_users_personal_workspace_is_refused():
    with pytest.raises(ValueError, match="Access denied"):
        await _context(VIEWER_IN_A_ADMIN_IN_B, group_id="user_someone_else_com")


def test_least_privileged_role():
    assert _least_privileged_role(["admin", "EDITOR", "admin"]) == "EDITOR"
    assert _least_privileged_role(["operator", "admin"]) == "operator"
    assert _least_privileged_role(["admin", "owner"]) is None
    assert _least_privileged_role([]) is None
