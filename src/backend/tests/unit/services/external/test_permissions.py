"""Teamspace roles on the external surfaces.

The boundary that matters: an operator may RUN published capabilities and may
not create anything. These tests pin that, and pin that the roles are the
workspace's own rather than a second ladder invented for the protocol.
"""

from unittest.mock import patch

import pytest

from src.services.external.identity import ExternalCaller
from src.services.external.permissions import (
    AUTHOR_ROLES,
    RUN_ROLES,
    ExternalPermissionError,
    require_role,
)


class _Ctx:
    def __init__(self, role):
        self.group_ids = ["acme_corp"]
        self.group_email = f"{role}@acme.com"
        self.access_token = "tok"
        self.user_role = role
        self.highest_role = role
        self.current_user = None

    @property
    def primary_group_id(self):
        return "acme_corp"


def _caller(role):
    return ExternalCaller(
        group_context=_Ctx(role), protocol="mcp", identifier=f"{role}@acme.com"
    )


def _as(role):
    """Resolve every role check against ``role``, as the workspace would."""
    return (
        patch(
            "src.services.external.permissions.check_role_in_context",
            side_effect=lambda ctx, roles: role in [r.lower() for r in roles],
        ),
        patch(
            "src.services.external.permissions.get_effective_role", return_value=role
        ),
    )


class TestTheRoleLadder:
    @pytest.mark.parametrize("role", ["admin", "editor", "operator"])
    def test_every_member_may_run(self, role):
        """Operators exist to run things."""
        a, b = _as(role)
        with a, b:
            require_role(_caller(role), RUN_ROLES)

    @pytest.mark.parametrize("role", ["admin", "editor"])
    def test_admin_and_editor_may_author(self, role):
        a, b = _as(role)
        with a, b:
            require_role(_caller(role), AUTHOR_ROLES)

    def test_operator_may_not_author(self):
        """The distinction the whole module exists for."""
        a, b = _as("operator")
        with a, b:
            with pytest.raises(ExternalPermissionError):
                require_role(_caller("operator"), AUTHOR_ROLES)

    def test_no_role_may_do_nothing(self):
        a, b = _as("")
        with a, b:
            for roles in (RUN_ROLES, AUTHOR_ROLES):
                with pytest.raises(ExternalPermissionError):
                    require_role(_caller("none"), roles)


class TestTheRefusal:
    def test_names_the_role_required_and_the_role_held(self):
        """An agent told only "forbidden" cannot tell a permission boundary from
        a misconfigured token, and will keep retrying."""
        a, b = _as("operator")
        with a, b:
            with pytest.raises(ExternalPermissionError) as exc:
                require_role(_caller("operator"), AUTHOR_ROLES)

        assert exc.value.required_roles == AUTHOR_ROLES
        assert exc.value.actual_role == "operator"
        assert "admin or editor" in exc.value.detail
        assert "operator" in exc.value.detail

    def test_is_not_an_auth_error(self):
        """Collapsing the two would tell an already-authenticated caller to
        authenticate, and it would retry with the same credentials forever."""
        from src.services.external.identity import ExternalAuthError

        assert not issubclass(ExternalPermissionError, ExternalAuthError)


class TestNoSecondModel:
    def test_the_role_lists_are_the_workspace_s_own(self):
        """AUTHOR_ROLES mirrors what crews_router guards its writes with. If
        these ever diverge, the external surface has grown its own policy."""
        assert AUTHOR_ROLES == ["admin", "editor"]
        assert RUN_ROLES == ["admin", "editor", "operator"]

    def test_it_delegates_to_the_shared_check(self):
        """Not a reimplementation — the same function every router calls."""
        with patch(
            "src.services.external.permissions.check_role_in_context",
            return_value=True,
        ) as shared:
            require_role(_caller("admin"), AUTHOR_ROLES)
        shared.assert_called_once()
