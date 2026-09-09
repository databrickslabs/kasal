"""An exported app's conversation belongs to whoever started it (audit F11).

The id was the only key: anyone who knew one could read the history, poll the
progress and surface, and cancel the turn. The first turn now claims the
conversation, and the turn path and every read route check the claim.
"""

import pytest
from fastapi import HTTPException

from tests.unit.services.export.conftest import purge_agent_server_modules


@pytest.mark.asyncio
async def test_expired_conversation_cannot_restore_previous_owners_cache(
    app_bundle, monkeypatch
):
    from agent_server import conversation, ownership, state_store

    state_store._backend = state_store._MemoryBackend()
    state_store._writes_since_prune = 0
    monkeypatch.setattr(state_store.time, "time", lambda: 1000)
    ownership.ensure_owner("expired", "alice")
    conversation._save_history("expired", [{"role": "user", "content": "private"}])
    monkeypatch.setattr(
        state_store.time, "time", lambda: 1001 + state_store._ROW_TTL_SECONDS
    )
    state_store._writes_since_prune = state_store._PRUNE_EVERY_WRITES - 1
    state_store.set_json("other", "progress", {})
    ownership.ensure_owner("expired", "bob")
    ownership.require_owner("expired", {"x-forwarded-user": "bob"})
    assert conversation.get_history("expired") == []
    assert "expired" not in conversation._HISTORY


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


@pytest.mark.asyncio
async def test_history_from_before_ownership_is_nobodys(app_bundle):
    """R2-02. An unowned id that already holds messages is legacy state; a
    turn must not claim it and the routes must not serve it."""
    from agent_server import ownership, state_store

    _fresh_store()
    state_store.set_json("legacy", "history", [{"role": "user", "content": "old"}])
    with pytest.raises(ownership.ConversationUnclaimable):
        ownership.ensure_owner("legacy", "bob")
    assert ownership.owned_by("legacy", "bob") is False
    assert ownership.owner_of("legacy") is None


@pytest.mark.asyncio
async def test_two_first_turns_racing_for_one_id_yield_one_owner(app_bundle):
    import threading

    from agent_server import ownership

    _fresh_store()
    barrier = threading.Barrier(2)
    outcomes = {}

    def claim(user):
        barrier.wait()
        try:
            ownership.ensure_owner("raced", user)
            outcomes[user] = "owner"
        except ownership.ConversationOwnedByAnother:
            outcomes[user] = "refused"

    threads = [threading.Thread(target=claim, args=(u,)) for u in ("alice", "bob")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(outcomes.values()) == ["owner", "refused"]
    assert outcomes[ownership.owner_of("raced")] == "owner"


@pytest.mark.asyncio
async def test_the_claim_is_refreshed_on_every_authorized_turn(app_bundle):
    """The owner row must not age out from under a live conversation."""
    import json

    from agent_server import ownership, state_store

    _fresh_store()
    ownership.ensure_owner("live", "alice")
    backend = state_store._get_backend()
    _, first = backend.get("live", "owner")
    backend.set("live", "owner", json.dumps({"user": "alice"}), first - 100)
    ownership.ensure_owner("live", "alice")
    _, refreshed = backend.get("live", "owner")
    assert refreshed > first - 100


@pytest.mark.asyncio
async def test_a_store_that_cannot_answer_fails_closed(app_bundle, monkeypatch):
    from agent_server import ownership, state_store

    _fresh_store()
    ownership.ensure_owner("c9", "alice")

    def boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(state_store._get_backend(), "get", boom)
    assert ownership.owned_by("c9", "alice") is False
    with pytest.raises(state_store.StorageUnavailable):
        ownership.ensure_owner("c9", "alice")
