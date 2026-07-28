"""The remote-agent registry service.

Policy, not protocol: group scoping, credential handling, and what happens to a
configuration row when the remote it points at is unreachable.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.a2a_agent import A2AAgentCreate, A2AAgentUpdate
from src.services.a2a.a2a_client.agent_service import A2AAgentService


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
    service.repository.get = AsyncMock(return_value=None)
    service.repository.find_base_by_name = AsyncMock(return_value=None)
    service.repository.find_by_name_and_group = AsyncMock(return_value=None)
    service.repository.list_enabled_for_group = AsyncMock(return_value=[])
    service.repository.list_for_group_scope = AsyncMock(return_value=[])
    service.repository.delete_overrides_by_name = AsyncMock(return_value=0)
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
        group_id=None,
    )
    for k, v in overrides.items():
        setattr(row, k, v)
    return row


def _card(card=None, fails=None):
    return patch(
        "src.services.a2a.a2a_client.client.fetch_card",
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
    async def test_a_duplicate_global_name_is_refused(self):
        service = _service()
        service.repository.find_base_by_name = AsyncMock(return_value=_row())
        with pytest.raises(ValueError, match="already exists"):
            await service.create_agent(
                A2AAgentCreate(name="Researcher", card_url="https://x.example.com"),
                _Ctx(),
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
        service.repository.get = AsyncMock(return_value=row)

        with _card():
            await service.update_agent(1, A2AAgentUpdate(api_key=""), _Ctx())
        assert row.encrypted_api_key is None

        row.encrypted_api_key = "ENC"
        await service.update_agent(1, A2AAgentUpdate(enabled=False), _Ctx())
        assert row.encrypted_api_key == "ENC"

    @pytest.mark.asyncio
    async def test_an_unknown_row_is_not_found(self):
        """404, not 403 — an id must not be probable for what exists."""
        service = _service()
        service.repository.get = AsyncMock(return_value=None)
        assert await service.update_agent(1, A2AAgentUpdate(name="x"), _Ctx()) is None

    @pytest.mark.asyncio
    async def test_changing_the_url_refetches_the_card(self):
        service = _service()
        service.repository.get = AsyncMock(return_value=_row())
        with _card() as fetch:
            await service.update_agent(
                1, A2AAgentUpdate(card_url="https://new.example.com"), _Ctx()
            )
        fetch.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_cosmetic_change_does_not_refetch(self):
        """A rename should not depend on a remote being up."""
        service = _service()
        service.repository.get = AsyncMock(return_value=_row())
        with _card() as fetch:
            await service.update_agent(1, A2AAgentUpdate(description="notes"), _Ctx())
        fetch.assert_not_awaited()


class TestResolveForCall:
    @pytest.mark.asyncio
    async def test_a_disabled_remote_cannot_be_reached(self):
        """A remote an operator turned off must not be callable through a stale
        tool config."""
        service = _service()
        # The repository filters disabled rows out in SQL, so nothing comes back.
        service.repository.list_enabled_for_group = AsyncMock(return_value=[])
        assert await service.resolve_for_call("Researcher", ["acme_corp"]) is None

    @pytest.mark.asyncio
    async def test_it_returns_a_call_plan_rather_than_the_row(self):
        """So a credential cannot travel somewhere it does not belong by
        accident, attached to a model object."""
        service = _service()
        service.repository.list_enabled_for_group = AsyncMock(
            return_value=[
                _row(
                    cached_card={
                        "interfaces": [{"url": "https://remote.example.com/a2a/v1"}],
                        "skills": [{"id": "research", "name": "Research"}],
                    }
                )
            ]
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
        service.repository.get = AsyncMock(
            return_value=_row(auth_type="api_key", encrypted_api_key=None)
        )
        with _card() as fetch:
            await service.test_connection(1, _Ctx())
        assert fetch.await_args.kwargs["token"] is None

        service.repository.get = AsyncMock(return_value=_row())
        with _card() as fetch:
            await service.test_connection(1, _Ctx())
        assert fetch.await_args.kwargs["token"] == "user-token"


class TestGlobalVersusWorkspace:
    """Who may do what.

    A remote agent carries an outbound URL and a credential, so registering one
    is a Kasal-admin act. A workspace admin only chooses whether their workspace
    uses it — the same split MCP servers already have.
    """

    @pytest.mark.asyncio
    async def test_a_new_agent_is_registered_globally(self):
        """group_id NULL: there is no workspace-scoped create at all."""
        service = _service()
        with _card():
            agent = await service.create_agent(
                A2AAgentCreate(name="R", card_url="https://remote.example.com"), _Ctx()
            )
        assert agent.group_id is None

    @pytest.mark.asyncio
    async def test_a_workspace_override_cannot_be_edited(self):
        """An override is a COPY of a base. Editing one would leave a workspace
        silently calling a different remote than the catalogue says it is."""
        service = _service()
        service.repository.get = AsyncMock(return_value=_row(group_id="acme_corp"))
        assert (
            await service.update_agent(
                1, A2AAgentUpdate(card_url="https://evil.example.com"), _Ctx()
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_enabling_an_inherited_agent_copies_it_for_the_workspace(self):
        """The base is never mutated, so one workspace's choice cannot reach
        another's."""
        service = _service()
        base = _row(card_url="https://remote.example.com", enabled=True)
        service.repository.get = AsyncMock(return_value=base)

        override = await service.set_enabled_for_group(1, "acme_corp", False)

        assert override.group_id == "acme_corp"
        assert override.enabled is False
        assert override.card_url == base.card_url
        assert base.enabled is True

    @pytest.mark.asyncio
    async def test_toggling_the_workspaces_own_row_flips_it_in_place(self):
        """Rather than accumulating a second override per toggle."""
        service = _service()
        own = _row(group_id="acme_corp", enabled=True)
        service.repository.get = AsyncMock(return_value=own)

        result = await service.set_enabled_for_group(1, "acme_corp", False)

        assert result is own
        assert own.enabled is False
        service.session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_another_workspaces_row_is_not_found(self):
        """Not forbidden — an id must not be probable for what other workspaces
        have turned on."""
        service = _service()
        service.repository.get = AsyncMock(return_value=_row(group_id="other_corp"))
        assert await service.set_enabled_for_group(1, "acme_corp", True) is None

    @pytest.mark.asyncio
    async def test_global_availability_only_applies_to_a_base_row(self):
        service = _service()
        service.repository.get = AsyncMock(return_value=_row(group_id="acme_corp"))
        assert await service.set_global_availability(1, False) is None

    @pytest.mark.asyncio
    async def test_deleting_a_global_agent_removes_every_workspace_opt_in(self):
        """Orphaned overrides would keep it callable in workspaces that had
        enabled it, long after the row defining it was gone."""
        service = _service()
        service.repository.get = AsyncMock(return_value=_row(name="Researcher"))

        assert await service.delete_agent(1) is True
        service.repository.delete_overrides_by_name.assert_awaited_with("Researcher")

    @pytest.mark.asyncio
    async def test_a_workspace_override_cannot_be_deleted_through_the_global_path(self):
        service = _service()
        service.repository.get = AsyncMock(return_value=_row(group_id="acme_corp"))
        assert await service.delete_agent(1) is False
