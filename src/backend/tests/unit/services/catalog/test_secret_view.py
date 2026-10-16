"""ToolSecretView: who reads stored tool credentials, and masked copies."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.schemas.agent import Agent as AgentSchema
from src.services.catalog.secret_view import ToolSecretView, masked_response
from src.utils.sensitive_data_utils import REDACTED_PLACEHOLDER
from src.utils.user_context import GroupContext

LOOKUP = "src.services.groups.groups.GroupService.get_user_group_membership"


def _context(role=None, group="team_ws", user=None, user_id="u1"):
    return GroupContext(
        group_ids=[group],
        group_email="u@example.com",
        email_domain="example.com",
        user_id=user_id,
        user_role=role,
        current_user=user,
    )


async def _visible(context, membership=None):
    lookup = AsyncMock(return_value=membership)
    with patch(LOOKUP, lookup):
        visible = await ToolSecretView(session=SimpleNamespace()).visible_to(context)
    return visible, lookup


@pytest.mark.parametrize("role", ["admin", "editor", "Editor"])
async def test_builder_roles_see_secrets_without_a_lookup(role):
    visible, lookup = await _visible(_context(role))
    assert visible is True
    lookup.assert_not_awaited()


async def test_a_system_admin_sees_secrets():
    user = SimpleNamespace(is_system_admin=True)
    visible, _ = await _visible(_context("operator", user=user))
    assert visible is True


async def test_the_personal_workspace_owner_sees_secrets():
    user = SimpleNamespace(
        is_system_admin=False,
        is_personal_workspace_manager=False,
        personal_group_id="user_u_example_com",
    )
    visible, _ = await _visible(_context(None, group="user_u_example_com", user=user))
    assert visible is True  # the personal workspace resolves to editor


@pytest.mark.parametrize(
    "membership, expected",
    [
        (None, False),
        (SimpleNamespace(allow_agent_builder=None, allow_flow_builder=None), False),
        (SimpleNamespace(allow_agent_builder=False, allow_flow_builder=False), False),
        (SimpleNamespace(allow_agent_builder=True, allow_flow_builder=None), True),
        (SimpleNamespace(allow_agent_builder=None, allow_flow_builder=True), True),
    ],
)
async def test_an_operator_needs_a_builder_override(membership, expected):
    visible, lookup = await _visible(_context("operator"), membership)
    assert visible is expected
    lookup.assert_awaited_once_with("u1", "team_ws")


async def test_without_a_user_or_a_context_secrets_stay_masked():
    assert (await _visible(_context("operator", user_id=None)))[0] is False
    assert (await _visible(None))[0] is False


def test_masked_response_is_a_copy():
    now = datetime(2026, 10, 1)
    configs = {"T": {"client_secret": "s", "tenant_id": "t"}}
    row = SimpleNamespace(
        id="a1",
        name="a",
        role="r",
        goal="g",
        backstory="b",
        tool_configs=configs,
        created_at=now,
        updated_at=now,
    )
    response = masked_response(AgentSchema, row, ("tool_configs",))
    assert response.tool_configs == {
        "T": {"client_secret": REDACTED_PLACEHOLDER, "tenant_id": "t"}
    }
    assert row.tool_configs is configs
    assert configs["T"]["client_secret"] == "s"
