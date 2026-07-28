"""The publication registry: which crews are exposed, to whom, over what.

One record per crew, listing its protocols — see ``models/crew_publication.py``
for why that is one record and not a flag per protocol.

The service exists so both adapters read capabilities through the SAME call.
``list_capabilities`` is what an MCP ``list_crews`` returns and what an A2A
Agent Card's ``skills[]`` is built from; because it is one function they cannot
advertise different capabilities, which is the invariant the whole design rests
on.
"""

import logging
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.crew_publication import CrewPublication
from src.repositories.crew_publication_repository import CrewPublicationRepository
from src.schemas.crew_publication import (
    CrewPublicationCreate,
    CrewPublicationUpdate,
    PublishedCapability,
)
from src.services.external.identity import ExternalCaller
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class PublicationService:
    """CRUD plus the one group-scoped read both adapters depend on."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = CrewPublicationRepository(session)

    async def list_capabilities(
        self, caller: ExternalCaller, protocol: Optional[str] = None
    ) -> List[PublishedCapability]:
        """Capabilities this caller may see, as the adapters render them.

        Protocol-neutral on purpose: the MCP tool list and the A2A ``skills[]``
        are two renderings of this one list. Defaults ``protocol`` to the
        caller's own, so an adapter cannot accidentally list capabilities that
        are not published to its surface.
        """
        rows = await self.repository.list_published_for_group(
            group_ids=caller.group_ids,
            protocol=protocol if protocol is not None else caller.protocol,
        )
        return [
            PublishedCapability(
                crew_id=row.crew_id,
                name=row.external_name,
                description=row.description,
                input_schema=row.input_schema,
            )
            for row in rows
        ]

    async def resolve_capability(
        self, caller: ExternalCaller, external_name: str
    ) -> Optional[CrewPublication]:
        """The publication behind a name the caller used, or None.

        Returns None both when the name does not exist and when it exists in
        another tenant — the caller must not be able to tell those apart, or the
        surface becomes an oracle for other workspaces' capability names.

        Also returns None when the capability is not published to the caller's
        protocol: being on the A2A card must not make it invocable over MCP.
        """
        row = await self.repository.find_by_external_name(
            external_name=external_name, group_ids=caller.group_ids
        )
        if row is None:
            return None
        if caller.protocol not in (row.protocols or []):
            logger.info(
                "[external] %s is not published over %s; refusing",
                external_name,
                caller.protocol,
            )
            return None
        return row

    async def publish(
        self,
        crew_id: str,
        data: CrewPublicationCreate,
        group_context: GroupContext,
    ) -> CrewPublication:
        """Publish a crew, or update its publication if it already has one.

        Idempotent by crew: publishing twice adjusts the existing record rather
        than creating a second one, which the unique constraint would reject
        anyway.
        """
        group_id = group_context.primary_group_id
        if not group_id:
            raise ValueError("Cannot publish without a group context.")

        existing = await self.repository.find_by_crew_id(
            crew_id=crew_id, group_ids=group_context.group_ids or []
        )
        if existing is not None:
            existing.external_name = data.external_name
            existing.description = data.description
            existing.protocols = list(data.protocols)
            existing.input_schema = data.input_schema
            await self.session.flush()
            return existing

        row = CrewPublication(
            crew_id=crew_id,
            external_name=data.external_name,
            description=data.description,
            protocols=list(data.protocols),
            input_schema=data.input_schema,
            group_id=group_id,
            created_by_email=group_context.group_email,
        )
        self.session.add(row)
        await self.session.flush()
        logger.info(
            "[external] published crew %s as %s over %s (group %s)",
            crew_id,
            data.external_name,
            data.protocols,
            group_id,
        )
        return row

    async def update(
        self,
        crew_id: str,
        data: CrewPublicationUpdate,
        group_context: GroupContext,
    ) -> Optional[CrewPublication]:
        """Adjust an existing publication. Omitted fields are left alone."""
        row = await self.repository.find_by_crew_id(
            crew_id=crew_id, group_ids=group_context.group_ids or []
        )
        if row is None:
            return None

        if data.external_name is not None:
            row.external_name = data.external_name
        if data.description is not None:
            row.description = data.description
        if data.protocols is not None:
            row.protocols = list(data.protocols)
        if data.input_schema is not None:
            row.input_schema = data.input_schema

        await self.session.flush()
        return row

    async def unpublish(self, crew_id: str, group_context: GroupContext) -> bool:
        """Withdraw a crew from every external surface. True if a row went."""
        removed = await self.repository.delete_by_crew_id(
            crew_id=crew_id, group_ids=group_context.group_ids or []
        )
        if removed:
            logger.info("[external] unpublished crew %s", crew_id)
        return removed > 0
