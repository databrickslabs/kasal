"""The remote-agent registry service.

Policy, not protocol: group scoping, credential handling, and what happens to a
configuration row when the remote it points at is unreachable.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.a2a_agent import A2AAgentCreate, A2AAgentUpdate
from src.services.a2a_agent_service import A2AAgentService


class _Ctx:
    group_ids = ["acme_corp"]
    primary_group_id = "acme_corp"
    group_email = "admin@example.com"
    access_token = "user-token"


def _service():
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.delete = AsyncMock()
    service = A2AAgentService(session)
    service.repository = MagicMock()
    service.repository.find_by_name = AsyncMock(return_value=None)
    service.repository.find_for_group = AsyncMock(return_value=None)
    service.repository.list_for_group = AsyncMock(return_value=[])
    return service


def _row(**overrides):
    row = SimpleNamespace(
        id=1,
        name="Researcher",
        card_url="https://remote.example.com",
        description=None,
        auth_type="obo",
        encrypted_api_key=None,
        enabled=True,
        global_enabled=False,
        timeout_seconds=300,
        cached_card=None,
        card_fetched_at=None,
        last_error=None,
        group_id="acme_corp",
    )
    for k, v in overrides.items():
        setattr(row, k, v)
    return row


def _card(card=None, fails=None):
    return patch(
        "src.services.a2a.client.fetch_card",
        new=AsyncMock(return_value=card or {"name": "Remote"}, side_effect=fails),
    )


class TestCreate:
    @pytest.mark.asyncio
    async def test_the_card_is_fetched_immediately(self):
        """So a typo in the URL is a message on the form, not a tool that
        silently does nothing at run time."""
        service = _service()
        with _card({"name": "Remote", "skills": [{"id": "s"}]}) as fetch:
            await service.create_agent(
                A2AAgentCreate(name="R", card_url="https://remote.example.com"), _Ctx()
            )
        fetch.assert_awaited()

    @pytest.mark.asyncio
    async def test_an_unreachable_remote_is_still_saved(self):
        """The operator usually needs the row saved precisely so they can fix
        the URL on it; rolling back loses what they typed."""
        service = _service()
        with _card(fails=RuntimeError("no such host")):
            agent = await service.create_agent(
                A2AAgentCreate(name="R", card_url="https://nope.example.com"), _Ctx()
            )

        assert agent.last_error and "no such host" in agent.last_error
        service.session.add.assert_called_once()

    @pytest.mark.asyncio
    async def test_the_key_is_encrypted_and_the_plaintext_is_not_kept(self):
        service = _service()
        with (
            _card(),
            patch(
                "src.utils.encryption_utils.EncryptionUtils.encrypt_value",
                return_value="ENC",
            ),
        ):
            agent = await service.create_agent(
                A2AAgentCreate(
                    name="R", card_url="https://remote.example.com", api_key="s3cret"
                ),
                _Ctx(),
            )

        assert agent.encrypted_api_key == "ENC"
        assert not hasattr(agent, "api_key")

    @pytest.mark.asyncio
    async def test_a_duplicate_name_in_the_same_workspace_is_refused(self):
        service = _service()
        service.repository.find_by_name = AsyncMock(return_value=_row())
        with pytest.raises(ValueError, match="already exists"):
            await service.create_agent(
                A2AAgentCreate(name="Researcher", card_url="https://x.example.com"),
                _Ctx(),
            )

    @pytest.mark.asyncio
    async def test_a_caller_with_no_workspace_cannot_configure_one(self):
        service = _service()

        class _NoGroup:
            group_ids = []
            primary_group_id = None
            group_email = "x@example.com"
            access_token = None

        with pytest.raises(ValueError, match="workspace is required"):
            await service.create_agent(
                A2AAgentCreate(name="R", card_url="https://x.example.com"), _NoGroup()
            )

    @pytest.mark.asyncio
    async def test_an_unknown_auth_type_is_refused(self):
        with pytest.raises(ValueError, match="auth_type"):
            await _service().create_agent(
                A2AAgentCreate(
                    name="R", card_url="https://x.example.com", auth_type="magic"
                ),
                _Ctx(),
            )


class TestUpdate:
    @pytest.mark.asyncio
    async def test_an_empty_key_clears_it_and_none_leaves_it_alone(self):
        """Without the distinction there is no way to remove a credential once
        it has been set."""
        service = _service()
        row = _row(encrypted_api_key="ENC")
        service.repository.find_for_group = AsyncMock(return_value=row)

        with _card():
            await service.update_agent(1, A2AAgentUpdate(api_key=""), _Ctx())
        assert row.encrypted_api_key is None

        row.encrypted_api_key = "ENC"
        await service.update_agent(1, A2AAgentUpdate(enabled=False), _Ctx())
        assert row.encrypted_api_key == "ENC"

    @pytest.mark.asyncio
    async def test_a_row_in_another_workspace_is_not_found(self):
        """404, not 403 — an id must not be probable for what other workspaces
        have configured."""
        service = _service()
        service.repository.find_for_group = AsyncMock(return_value=None)
        assert await service.update_agent(1, A2AAgentUpdate(name="x"), _Ctx()) is None

    @pytest.mark.asyncio
    async def test_changing_the_url_refetches_the_card(self):
        service = _service()
        service.repository.find_for_group = AsyncMock(return_value=_row())
        with _card() as fetch:
            await service.update_agent(
                1, A2AAgentUpdate(card_url="https://new.example.com"), _Ctx()
            )
        fetch.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_cosmetic_change_does_not_refetch(self):
        """A rename should not depend on a remote being up."""
        service = _service()
        service.repository.find_for_group = AsyncMock(return_value=_row())
        with _card() as fetch:
            await service.update_agent(1, A2AAgentUpdate(description="notes"), _Ctx())
        fetch.assert_not_awaited()


class TestResolveForCall:
    @pytest.mark.asyncio
    async def test_a_disabled_remote_cannot_be_reached(self):
        """A remote an operator turned off must not be callable through a stale
        tool config."""
        service = _service()
        service.repository.find_by_name = AsyncMock(return_value=_row(enabled=False))
        assert await service.resolve_for_call("Researcher", ["acme_corp"]) is None

    @pytest.mark.asyncio
    async def test_it_returns_a_call_plan_rather_than_the_row(self):
        """So a credential cannot travel somewhere it does not belong by
        accident, attached to a model object."""
        service = _service()
        service.repository.find_by_name = AsyncMock(
            return_value=_row(
                cached_card={
                    "interfaces": [{"url": "https://remote.example.com/a2a/v1"}],
                    "skills": [{"id": "research", "name": "Research"}],
                }
            )
        )
        resolved = await service.resolve_for_call("Researcher", ["acme_corp"])

        assert resolved["interface_url"] == "https://remote.example.com/a2a/v1"
        assert [s["id"] for s in resolved["skills"]] == ["research"]
        assert set(resolved) == {
            "name",
            "interface_url",
            "api_key",
            "auth_type",
            "timeout_seconds",
            "skills",
        }


class TestObo:
    @pytest.mark.asyncio
    async def test_the_callers_token_is_only_forwarded_to_obo_remotes(self):
        """Sending a user's Databricks token to a remote that authenticates its
        own way would leak it to a third party for no benefit."""
        service = _service()
        service.repository.find_for_group = AsyncMock(
            return_value=_row(auth_type="api_key", encrypted_api_key=None)
        )
        with _card() as fetch:
            await service.test_connection(1, _Ctx())
        assert fetch.await_args.kwargs["token"] is None

        service.repository.find_for_group = AsyncMock(return_value=_row())
        with _card() as fetch:
            await service.test_connection(1, _Ctx())
        assert fetch.await_args.kwargs["token"] == "user-token"
