"""An exported app's conversation belongs to whoever started it (audit F11).

The id was the only key: anyone who knew one could read the history, poll the
progress and surface, and cancel the turn. The first turn now claims the
conversation, and the turn path and every read route check the claim.
"""

import pytest
from fastapi import HTTPException

from tests.unit.services.export.conftest import purge_agent_server_modules


@pytest.fixture(autouse=True)
def _fresh_modules():
    purge_agent_server_modules()
    yield
    purge_agent_server_modules()


def _fresh_store():
    from agent_server import state_store

    state_store._reset_for_tests()


@pytest.mark.asyncio
async def test_the_first_turn_claims_and_later_turns_are_checked(app_bundle):
    from agent_server import ownership

    _fresh_store()
    assert ownership.owner_of("c1") is None
    assert ownership.owned_by("c1", "alice") is False  # unclaimed is nobody's
    ownership.ensure_owner("c1", "alice")
    assert ownership.owner_of("c1") == "alice"
    assert ownership.owned_by("c1", "alice") is True
    assert ownership.owned_by("c1", "bob") is False
    with pytest.raises(ownership.ConversationOwnedByAnother):
        ownership.ensure_owner("c1", "bob")
    ownership.ensure_owner("c1", "alice")  # the owner keeps going


@pytest.mark.asyncio
async def test_without_a_proxy_everything_is_the_anonymous_users(app_bundle):
    from agent_server import ownership

    _fresh_store()
    ownership.ensure_owner("local-1", None)
    assert ownership.owned_by("local-1", None) is True
    assert ownership.owned_by("local-1", "someone") is False


@pytest.mark.asyncio
async def test_identity_is_derived_one_way(app_bundle):
    from agent_server import ownership

    assert (
        ownership.identity_from_headers(
            {
                "x-forwarded-email": "a@example.com",
                "x-forwarded-preferred-username": "alice",
            }
        )
        == "alice"
    )
    assert ownership.identity_from_headers({"x-forwarded-user": "u-1"}) == "u-1"
    assert ownership.identity_from_headers({}) is None


@pytest.mark.asyncio
async def test_the_read_routes_refuse_another_users_conversation(app_bundle):
    from agent_server import ownership

    _fresh_store()
    ownership.ensure_owner("c2", "alice")
    ownership.require_owner("c2", {"x-forwarded-preferred-username": "alice"})
    with pytest.raises(HTTPException) as refused:
        ownership.require_owner("c2", {"x-forwarded-preferred-username": "bob"})
    assert refused.value.status_code == 404
    with pytest.raises(HTTPException):
        ownership.require_owner(
            "never-claimed", {"x-forwarded-preferred-username": "bob"}
        )


@pytest.mark.asyncio
async def test_the_server_routes_check_ownership(app_bundle):
    """The routes take the request and call the guard — read as text, since
    importing start_server boots the MLflow agent server."""
    src = (app_bundle / "agent_server" / "start_server.py").read_text()
    for route in ("/progress/", "/conversations/", "/a2ui/", "/cancel/"):
        assert route in src
    assert src.count("ownership.require_owner(conversation_id, request.headers)") == 4
