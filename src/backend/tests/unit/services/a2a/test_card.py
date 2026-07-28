"""The Agent Card — Kasal's A2A discovery document.

The card is a public contract and a promise: what it advertises, callers rely
on. These tests pin both halves — that skills come from the shared publication
query, and that capability flags are not claimed before the behaviour exists.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.schemas.crew_publication import PublishedCapability
from src.services.a2a.card import build_card
from src.services.external.identity import ExternalCaller


class _Ctx:
    def __init__(self, group_ids=("acme_corp",)):
        self.group_ids = list(group_ids)
        self.group_email = "agent@example.com"
        self.user_role = "admin"
        self.highest_role = "admin"
        self.current_user = None
        self.access_token = "tok"

    @property
    def primary_group_id(self):
        return self.group_ids[0]


def _caller():
    return ExternalCaller(
        group_context=_Ctx(), protocol="a2a", identifier="agent@example.com"
    )


async def _build(caps, **kwargs):
    with patch("src.services.a2a.card.PublicationService") as service_cls:
        service_cls.return_value.list_capabilities = AsyncMock(return_value=caps)
        return await build_card(
            _caller(), base_url="https://kasal.example.com", **kwargs
        )


class TestSkills:
    @pytest.mark.asyncio
    async def test_skills_come_from_published_capabilities(self):
        card = await _build(
            [
                PublishedCapability(
                    entity_id="c1",
                    name="analyse_powerbi_model",
                    description="Analyse a PowerBI semantic model.",
                )
            ]
        )
        assert [s.id for s in card.skills] == ["analyse_powerbi_model"]
        assert card.skills[0].description == "Analyse a PowerBI semantic model."

    @pytest.mark.asyncio
    async def test_a_workspace_with_nothing_published_advertises_no_skills(self):
        card = await _build([])
        assert card.skills == []

    @pytest.mark.asyncio
    async def test_the_input_schema_is_carried_to_the_skill(self):
        schema = {"type": "object", "properties": {"model": {"type": "string"}}}
        card = await _build(
            [
                PublishedCapability(
                    entity_id="c1", name="x", description="d", input_schema=schema
                )
            ]
        )
        assert card.skills[0].inputSchema == schema

    @pytest.mark.asyncio
    async def test_the_listing_is_scoped_to_the_a2a_protocol(self):
        """A crew published only over MCP must not appear on the card."""
        with patch("src.services.a2a.card.PublicationService") as service_cls:
            service_cls.return_value.list_capabilities = AsyncMock(return_value=[])
            await build_card(_caller(), base_url="https://x")

        assert (
            service_cls.return_value.list_capabilities.await_args.kwargs["protocol"]
            == "a2a"
        )


class TestCapabilitiesAreAPromise:
    @pytest.mark.asyncio
    async def test_push_notifications_are_not_claimed_before_delivery_exists(self):
        """Advertising a webhook that never arrives is worse than advertising
        none. The flag flips when the dispatcher ships, not before."""
        card = await _build([])
        assert card.capabilities.pushNotifications is False

    @pytest.mark.asyncio
    async def test_streaming_is_not_claimed_before_it_is_translated(self):
        """SSE exists internally, but until it is rendered as
        TaskStatusUpdateEvent the card must not promise it."""
        card = await _build([])
        assert card.capabilities.streaming is False


class TestContract:
    @pytest.mark.asyncio
    async def test_advertises_the_obo_security_scheme(self):
        """The card has to tell a caller HOW to authenticate — work runs on
        their own Databricks token."""
        card = await _build([])
        assert "databricks_obo" in card.securitySchemes
        assert card.securitySchemes["databricks_obo"].scheme == "bearer"
        assert card.security == [{"databricks_obo": []}]

    @pytest.mark.asyncio
    async def test_interface_points_at_the_versioned_endpoint(self):
        card = await _build([])
        assert card.interfaces[0].url == "https://kasal.example.com/a2a/v1"

    @pytest.mark.asyncio
    async def test_pins_the_protocol_version(self):
        """v1.0 is recent and the well-known URI is still pending registration;
        the card states which revision it was written against."""
        card = await _build([])
        assert card.protocolVersion == "1.0"

    @pytest.mark.asyncio
    async def test_the_description_tells_callers_tasks_are_asynchronous(self):
        card = await _build([])
        assert "asynchronous" in card.description.lower()

    @pytest.mark.asyncio
    async def test_uses_the_spec_field_names_verbatim(self):
        """An A2A client parses this against the spec, not against Kasal. Any
        renaming here would force every client to special-case us."""
        card = await _build([])
        dumped = card.model_dump()
        for field in (
            "protocolVersion",
            "capabilities",
            "securitySchemes",
            "interfaces",
            "skills",
            "defaultInputModes",
        ):
            assert field in dumped
