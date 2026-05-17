"""
Unit tests for the permissions module.

Tests role-based access control helpers, workspace admin logic,
effective role resolution, and the require_roles decorator family.
"""

import pytest
from unittest.mock import MagicMock
from fastapi import HTTPException

from src.utils.user_context import GroupContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_current_user(is_system_admin=False, is_personal_workspace_manager=False):
    """Return a MagicMock that looks like a User model."""
    user = MagicMock()
    user.is_system_admin = is_system_admin
    user.is_personal_workspace_manager = is_personal_workspace_manager
    return user


def _make_ctx(
    user_role=None,
    group_ids=None,
    is_system_admin=False,
    is_personal_workspace_manager=False,
    include_current_user=True,
):
    """
    Build a real GroupContext instance.

    primary_group_id is computed from group_ids[0], so pass group_ids
    explicitly when you need a specific primary_group_id value.
    e.g. group_ids=["team_456"] → primary_group_id == "team_456"
         group_ids=["user_123"] → primary_group_id == "user_123"
    """
    current_user = (
        _make_current_user(is_system_admin, is_personal_workspace_manager)
        if include_current_user
        else None
    )
    return GroupContext(
        user_role=user_role,
        group_ids=group_ids or [],
        current_user=current_user,
    )


# ---------------------------------------------------------------------------
# is_admin
# ---------------------------------------------------------------------------

class TestIsAdmin:
    """Tests for is_admin()."""

    def test_returns_true_for_admin(self):
        from src.core.permissions import is_admin
        assert is_admin("admin") is True

    def test_returns_true_for_ADMIN_uppercase(self):
        from src.core.permissions import is_admin
        assert is_admin("ADMIN") is True

    def test_returns_false_for_editor(self):
        from src.core.permissions import is_admin
        assert not is_admin("editor")

    def test_returns_false_for_operator(self):
        from src.core.permissions import is_admin
        assert not is_admin("operator")

    def test_returns_falsy_for_none(self):
        from src.core.permissions import is_admin
        # The implementation returns `role and …` so None yields None (falsy)
        assert not is_admin(None)

    def test_returns_falsy_for_empty_string(self):
        from src.core.permissions import is_admin
        assert not is_admin("")


# ---------------------------------------------------------------------------
# is_editor_or_above
# ---------------------------------------------------------------------------

class TestIsEditorOrAbove:
    """Tests for is_editor_or_above()."""

    def test_true_for_admin(self):
        from src.core.permissions import is_editor_or_above
        assert is_editor_or_above("admin") is True

    def test_true_for_editor(self):
        from src.core.permissions import is_editor_or_above
        assert is_editor_or_above("editor") is True

    def test_false_for_operator(self):
        from src.core.permissions import is_editor_or_above
        assert not is_editor_or_above("operator")

    def test_false_for_none(self):
        from src.core.permissions import is_editor_or_above
        assert not is_editor_or_above(None)

    def test_case_insensitive_EDITOR(self):
        from src.core.permissions import is_editor_or_above
        assert is_editor_or_above("EDITOR") is True


# ---------------------------------------------------------------------------
# is_operator_or_above
# ---------------------------------------------------------------------------

class TestIsOperatorOrAbove:
    """Tests for is_operator_or_above()."""

    def test_true_for_admin(self):
        from src.core.permissions import is_operator_or_above
        assert is_operator_or_above("admin") is True

    def test_true_for_editor(self):
        from src.core.permissions import is_operator_or_above
        assert is_operator_or_above("editor") is True

    def test_true_for_operator(self):
        from src.core.permissions import is_operator_or_above
        assert is_operator_or_above("operator") is True

    def test_false_for_none(self):
        from src.core.permissions import is_operator_or_above
        assert not is_operator_or_above(None)

    def test_false_for_unknown_role(self):
        from src.core.permissions import is_operator_or_above
        assert not is_operator_or_above("viewer")

    def test_case_insensitive_OPERATOR(self):
        from src.core.permissions import is_operator_or_above
        assert is_operator_or_above("OPERATOR") is True


# ---------------------------------------------------------------------------
# check_role_in_context
# ---------------------------------------------------------------------------

class TestCheckRoleInContext:
    """Tests for check_role_in_context()."""

    def test_returns_true_when_role_in_allowed(self):
        from src.core.permissions import check_role_in_context
        ctx = _make_ctx(user_role="editor", group_ids=["team_1"])
        assert check_role_in_context(ctx, ["admin", "editor"]) is True

    def test_returns_false_when_role_not_allowed(self):
        from src.core.permissions import check_role_in_context
        ctx = _make_ctx(user_role="operator", group_ids=["team_1"])
        assert check_role_in_context(ctx, ["admin", "editor"]) is False

    def test_system_admin_always_passes(self):
        from src.core.permissions import check_role_in_context
        ctx = _make_ctx(is_system_admin=True)
        assert check_role_in_context(ctx, ["admin"]) is True

    def test_returns_false_when_no_role(self):
        from src.core.permissions import check_role_in_context
        ctx = _make_ctx(user_role=None, include_current_user=False)
        assert check_role_in_context(ctx, ["admin", "editor", "operator"]) is False

    def test_case_insensitive_comparison(self):
        from src.core.permissions import check_role_in_context
        ctx = _make_ctx(user_role="ADMIN", group_ids=["team_1"])
        assert check_role_in_context(ctx, ["admin"]) is True


# ---------------------------------------------------------------------------
# is_system_admin
# ---------------------------------------------------------------------------

class TestIsSystemAdmin:
    """Tests for is_system_admin()."""

    def test_returns_true_when_is_system_admin_flag_set(self):
        from src.core.permissions import is_system_admin
        ctx = _make_ctx(is_system_admin=True)
        assert is_system_admin(ctx) is True

    def test_returns_false_when_flag_not_set(self):
        from src.core.permissions import is_system_admin
        ctx = _make_ctx(is_system_admin=False)
        assert is_system_admin(ctx) is False

    def test_returns_false_when_no_current_user(self):
        from src.core.permissions import is_system_admin
        ctx = _make_ctx(include_current_user=False)
        assert is_system_admin(ctx) is False

    def test_returns_false_for_none_context(self):
        from src.core.permissions import is_system_admin
        assert is_system_admin(None) is False  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# is_workspace_admin
# ---------------------------------------------------------------------------

class TestIsWorkspaceAdmin:
    """Tests for is_workspace_admin()."""

    def test_system_admin_is_workspace_admin(self):
        from src.core.permissions import is_workspace_admin
        ctx = _make_ctx(is_system_admin=True)
        assert is_workspace_admin(ctx) is True

    def test_personal_workspace_manager_is_admin(self):
        from src.core.permissions import is_workspace_admin
        ctx = _make_ctx(
            group_ids=["user_123"],
            is_personal_workspace_manager=True,
        )
        assert is_workspace_admin(ctx) is True

    def test_personal_workspace_non_manager_is_not_admin(self):
        from src.core.permissions import is_workspace_admin
        ctx = _make_ctx(
            group_ids=["user_123"],
            is_personal_workspace_manager=False,
        )
        assert is_workspace_admin(ctx) is False

    def test_team_workspace_admin_role_is_admin(self):
        from src.core.permissions import is_workspace_admin
        ctx = _make_ctx(
            group_ids=["team_456"],
            user_role="admin",
            include_current_user=False,
        )
        assert is_workspace_admin(ctx) is True

    def test_team_workspace_editor_role_is_not_admin(self):
        from src.core.permissions import is_workspace_admin
        ctx = _make_ctx(
            group_ids=["team_456"],
            user_role="editor",
            include_current_user=False,
        )
        assert is_workspace_admin(ctx) is False

    def test_returns_false_for_none_context(self):
        from src.core.permissions import is_workspace_admin
        assert is_workspace_admin(None) is False  # type: ignore[arg-type]

    def test_no_current_user_falls_through_to_user_role(self):
        from src.core.permissions import is_workspace_admin
        ctx = _make_ctx(user_role="admin", include_current_user=False)
        assert is_workspace_admin(ctx) is True


# ---------------------------------------------------------------------------
# get_effective_role
# ---------------------------------------------------------------------------

class TestGetEffectiveRole:
    """Tests for get_effective_role()."""

    def test_system_admin_returns_admin(self):
        from src.core.permissions import get_effective_role
        ctx = _make_ctx(is_system_admin=True)
        assert get_effective_role(ctx) == "admin"

    def test_personal_workspace_manager_returns_admin(self):
        from src.core.permissions import get_effective_role
        ctx = _make_ctx(
            group_ids=["user_999"],
            is_personal_workspace_manager=True,
        )
        assert get_effective_role(ctx) == "admin"

    def test_personal_workspace_non_manager_returns_editor(self):
        from src.core.permissions import get_effective_role
        ctx = _make_ctx(
            group_ids=["user_999"],
            is_personal_workspace_manager=False,
        )
        assert get_effective_role(ctx) == "editor"

    def test_team_workspace_uses_assigned_role(self):
        from src.core.permissions import get_effective_role
        ctx = _make_ctx(user_role="operator", group_ids=["team_1"])
        assert get_effective_role(ctx) == "operator"

    def test_returns_none_for_none_context(self):
        from src.core.permissions import get_effective_role
        assert get_effective_role(None) is None  # type: ignore[arg-type]

    def test_returns_user_role_when_no_current_user(self):
        from src.core.permissions import get_effective_role
        ctx = _make_ctx(user_role="editor", include_current_user=False)
        assert get_effective_role(ctx) == "editor"

    def test_returns_none_when_no_user_role_and_no_current_user(self):
        from src.core.permissions import get_effective_role
        ctx = _make_ctx(user_role=None, include_current_user=False)
        assert get_effective_role(ctx) is None


# ---------------------------------------------------------------------------
# require_roles decorator
# ---------------------------------------------------------------------------

class TestRequireRoles:
    """Tests for the require_roles() async decorator.

    The decorator scans kwargs for any value that is an instance of GroupContext
    (isinstance check in the source). We therefore pass a real GroupContext via
    a kwarg; the kwarg name is arbitrary.
    """

    @pytest.mark.asyncio
    async def test_allows_matching_role(self):
        from src.core.permissions import require_roles

        ctx = _make_ctx(user_role="admin", group_ids=["team_1"])

        @require_roles(["admin"])
        async def endpoint(ctx=None):
            return "ok"

        result = await endpoint(ctx=ctx)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_denies_non_matching_role(self):
        from src.core.permissions import require_roles

        ctx = _make_ctx(user_role="operator", include_current_user=False)

        @require_roles(["admin", "editor"])
        async def endpoint(ctx=None):
            return "ok"

        with pytest.raises(HTTPException) as exc_info:
            await endpoint(ctx=ctx)
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_raises_500_when_no_group_context_in_kwargs(self):
        from src.core.permissions import require_roles

        @require_roles(["admin"])
        async def endpoint(name="nobody"):
            return "ok"

        with pytest.raises(HTTPException) as exc_info:
            await endpoint(name="nobody")
        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_raises_403_when_no_effective_role(self):
        from src.core.permissions import require_roles

        ctx = _make_ctx(user_role=None, include_current_user=False)

        @require_roles(["admin"])
        async def endpoint(ctx=None):
            return "ok"

        with pytest.raises(HTTPException) as exc_info:
            await endpoint(ctx=ctx)
        assert exc_info.value.status_code == 403
        assert "No role assigned" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_case_insensitive_role_comparison(self):
        from src.core.permissions import require_roles

        ctx = _make_ctx(user_role="EDITOR", include_current_user=False)

        @require_roles(["editor"])
        async def endpoint(ctx=None):
            return "ok"

        result = await endpoint(ctx=ctx)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_system_admin_passes_any_role_gate(self):
        from src.core.permissions import require_roles

        ctx = _make_ctx(is_system_admin=True)

        @require_roles(["admin"])
        async def endpoint(ctx=None):
            return "reached"

        result = await endpoint(ctx=ctx)
        assert result == "reached"

    @pytest.mark.asyncio
    async def test_error_detail_mentions_required_roles(self):
        from src.core.permissions import require_roles

        ctx = _make_ctx(user_role="operator", include_current_user=False)

        @require_roles(["admin", "editor"])
        async def endpoint(ctx=None):
            return "ok"

        with pytest.raises(HTTPException) as exc_info:
            await endpoint(ctx=ctx)
        assert "admin" in exc_info.value.detail or "editor" in exc_info.value.detail


# ---------------------------------------------------------------------------
# require_admin / require_editor_or_admin / require_operator_or_above
# ---------------------------------------------------------------------------

class TestConvenienceDecorators:
    """Tests for require_admin(), require_editor_or_admin(), require_operator_or_above()."""

    @pytest.mark.asyncio
    async def test_require_admin_allows_admin(self):
        from src.core.permissions import require_admin

        ctx = _make_ctx(user_role="admin", include_current_user=False)

        @require_admin()
        async def endpoint(ctx=None):
            return "ok"

        assert await endpoint(ctx=ctx) == "ok"

    @pytest.mark.asyncio
    async def test_require_admin_denies_editor(self):
        from src.core.permissions import require_admin

        ctx = _make_ctx(user_role="editor", include_current_user=False)

        @require_admin()
        async def endpoint(ctx=None):
            return "ok"

        with pytest.raises(HTTPException) as exc_info:
            await endpoint(ctx=ctx)
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_require_editor_or_admin_allows_editor(self):
        from src.core.permissions import require_editor_or_admin

        ctx = _make_ctx(user_role="editor", include_current_user=False)

        @require_editor_or_admin()
        async def endpoint(ctx=None):
            return "ok"

        assert await endpoint(ctx=ctx) == "ok"

    @pytest.mark.asyncio
    async def test_require_editor_or_admin_denies_operator(self):
        from src.core.permissions import require_editor_or_admin

        ctx = _make_ctx(user_role="operator", include_current_user=False)

        @require_editor_or_admin()
        async def endpoint(ctx=None):
            return "ok"

        with pytest.raises(HTTPException) as exc_info:
            await endpoint(ctx=ctx)
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_require_operator_or_above_allows_operator(self):
        from src.core.permissions import require_operator_or_above

        ctx = _make_ctx(user_role="operator", include_current_user=False)

        @require_operator_or_above()
        async def endpoint(ctx=None):
            return "ok"

        assert await endpoint(ctx=ctx) == "ok"

    @pytest.mark.asyncio
    async def test_require_operator_or_above_allows_admin(self):
        from src.core.permissions import require_operator_or_above

        ctx = _make_ctx(user_role="admin", include_current_user=False)

        @require_operator_or_above()
        async def endpoint(ctx=None):
            return "ok"

        assert await endpoint(ctx=ctx) == "ok"

    @pytest.mark.asyncio
    async def test_require_editor_or_admin_allows_admin(self):
        from src.core.permissions import require_editor_or_admin

        ctx = _make_ctx(user_role="admin", include_current_user=False)

        @require_editor_or_admin()
        async def endpoint(ctx=None):
            return "ok"

        assert await endpoint(ctx=ctx) == "ok"

    @pytest.mark.asyncio
    async def test_require_operator_or_above_allows_editor(self):
        from src.core.permissions import require_operator_or_above

        ctx = _make_ctx(user_role="editor", include_current_user=False)

        @require_operator_or_above()
        async def endpoint(ctx=None):
            return "ok"

        assert await endpoint(ctx=ctx) == "ok"
