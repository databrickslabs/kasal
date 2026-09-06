"""Personal workspace ids are one-to-one with users (audit F06 / R2-06).

The derived id collapsed '@', '.', '-' and '+' to '_', so alice.smith@ and
alice-smith@ shared one workspace. A user's id is now allocated once on the
row; a collision gets a disambiguated, deterministic form.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.utils.user_context import GroupContext

A, B = "alice.smith@example.com", "alice-smith@example.com"
STORED = "user_alice_smith_example_com_1a2b3c4d"


class TestTheDisambiguatedForm:
    def test_the_derived_form_collides_and_the_disambiguated_one_does_not(self):
        assert GroupContext.generate_individual_group_id(
            A
        ) == GroupContext.generate_individual_group_id(B)
        da, db = (GroupContext.disambiguated_individual_group_id(e) for e in (A, B))
        assert da != db
        assert da.startswith("user_alice_smith_example_com_")
        assert da == GroupContext.disambiguated_individual_group_id(A.upper())

    def test_either_form_is_recognised_as_the_users_own(self):
        legacy, dis = GroupContext.personal_workspace_candidates(A)
        assert GroupContext.is_personal_workspace_of(legacy, A)
        assert GroupContext.is_personal_workspace_of(dis, A)
        assert GroupContext.is_personal_workspace_of(dis.upper(), A)
        assert not GroupContext.is_personal_workspace_of("team_x", A)
        assert not GroupContext.is_personal_workspace_of(None, A)

    def test_the_stored_id_wins_over_the_derivation(self):
        row = SimpleNamespace(personal_group_id=STORED)
        assert GroupContext.personal_workspace_id_of(row, A) == STORED
        for user in (SimpleNamespace(), None):
            with pytest.raises(ValueError, match="Access denied"):
                GroupContext.personal_workspace_id_of(user, A)


def _user():
    return SimpleNamespace(id="u-b", is_system_admin=False, personal_group_id=STORED)


class TestTheContextUsesTheStoredId:
    @pytest.mark.asyncio
    async def test_a_user_without_groups_runs_under_the_stored_id(self):
        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            AsyncMock(return_value=(_user(), [])),
        ):
            ctx = await GroupContext.from_email(B)
        assert ctx.group_ids == [STORED]

    @pytest.mark.asyncio
    async def test_the_colliding_derived_id_is_refused_as_a_selection(self):
        """Selecting the workspace by the derived id — the other user's, under
        the collision — is refused; the stored id is the only personal id."""
        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            AsyncMock(return_value=(_user(), [])),
        ):
            with pytest.raises(ValueError, match="Access denied"):
                await GroupContext.from_email(
                    B, group_id=GroupContext.generate_individual_group_id(B)
                )

    @pytest.mark.asyncio
    async def test_a_lookup_failure_denies_instead_of_guessing(self):
        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            AsyncMock(side_effect=RuntimeError("db down")),
        ):
            with pytest.raises(ValueError, match="Access denied"):
                await GroupContext.from_email(A)


class TestAllocation:
    @pytest.mark.asyncio
    async def test_an_allocated_row_is_left_alone(self):
        from src.services.groups.users import UserService

        svc = UserService.__new__(UserService)
        svc.user_repo = AsyncMock()
        user = SimpleNamespace(id="u-a", email=A, personal_group_id="user_allocated")
        assert await svc.ensure_personal_workspace_id(user) == "user_allocated"
        svc.user_repo.allocate_personal_group_id.assert_not_awaited()
