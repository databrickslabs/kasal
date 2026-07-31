"""The publication registry: which crews are reachable, by whom, over what.

One record per crew, listing its protocols — see ``models/crew_publication.py``
for why that is one record and not a flag per protocol.

The service exists so every surface reads capabilities through the SAME call.
``list_capabilities_for_group`` is what an MCP ``list_crews`` returns, what an
A2A Agent Card's ``skills[]`` is built from, and what the ChatMode "Use existing"
router picks over; because it is one function they cannot advertise different
capabilities, which is the invariant the whole design rests on.

The registry is protocol-NEUTRAL, which is why it no longer lives under
``external/``: ``chat`` is an internal protocol reaching the same rows through
the same group filter. See this package's ``__init__`` for why the move mattered.

Two shapes of read, and the difference matters:

* ``*_for_group`` take primitives and are the core. Internal callers — who
  already hold a trusted ``GroupContext`` — use these.
* ``list_capabilities`` / ``resolve_capability`` take an ``ExternalCaller`` and
  delegate. The adapters keep the API they have.

**Do not build an ``ExternalCaller`` for internal traffic.** ``identity.py`` opens
with "An MCP client or an A2A agent is, by definition, outside the workspace" —
its job is turning untrusted headers into a tenant. Wrapping a ``GroupContext``
in one drags in the external role double-gating and stamps external-origin
attribution on internal traffic, polluting the external audit trail.
"""

import logging
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.crew_publication import Publication
from src.repositories.crew_publication_repository import PublicationRepository
from src.schemas.crew_publication import (
    CrewPublicationCreate,
    CrewPublicationUpdate,
    PublishedCapability,
)
from src.utils.user_context import GroupContext

if TYPE_CHECKING:  # pragma: no cover
    # Type-only: the two adapter-facing delegations below are typed for it, but
    # the registry must not depend on the external trust boundary at runtime.
    from src.services.external.identity import ExternalCaller

logger = logging.getLogger(__name__)


class PublicationService:
    """CRUD plus the one group-scoped read both adapters depend on."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = PublicationRepository(session)

    async def list_capabilities_for_group(
        self, group_ids: List[str], protocol: str
    ) -> List[PublishedCapability]:
        """Capabilities visible to ``group_ids`` on one protocol.

        The core read. Protocol-neutral on purpose: the MCP tool list, the A2A
        ``skills[]`` and the ChatMode route catalog are three renderings of this
        one list.

        An empty ``group_ids`` returns ``[]``, not everything — see the
        repository, where that guarantee lives.
        """
        rows = await self.repository.list_published_for_group(
            group_ids=group_ids, protocol=protocol
        )
        return [
            PublishedCapability(
                entity_type=row.entity_type,
                entity_id=row.entity_id,
                name=row.external_name,
                description=row.description,
                input_schema=row.input_schema,
            )
            for row in rows
        ]

    async def resolve_capability_for_group(
        self, group_ids: List[str], protocol: str, external_name: str
    ) -> Optional[Publication]:
        """The publication behind a name, or None. The single authorisation choke point.

        Returns None both when the name does not exist and when it exists in
        another tenant — a caller must not be able to tell those apart, or the
        surface becomes an oracle for other workspaces' capability names.

        Also returns None when the capability is not published to ``protocol``:
        being on the A2A card must not make it invocable over MCP, and being
        chat-routable must not make it either.

        Every surface resolves through here. Reaching past it to
        ``find_by_external_name``, or resolving a name through the catalogue
        instead, creates a second visibility semantic where an unpublished crew
        quietly becomes invocable.
        """
        row = await self.repository.find_by_external_name(
            external_name=external_name, group_ids=group_ids
        )
        if row is None:
            return None
        if protocol not in (row.protocols or []):
            logger.info(
                "[publication] %s is not published over %s; refusing",
                external_name,
                protocol,
            )
            return None
        return row

    async def list_capabilities(
        self, caller: "ExternalCaller", protocol: Optional[str] = None
    ) -> List[PublishedCapability]:
        """``list_capabilities_for_group`` for an external caller.

        Defaults ``protocol`` to the caller's own, so an adapter cannot
        accidentally list capabilities that are not published to its surface.
        """
        return await self.list_capabilities_for_group(
            caller.group_ids,
            protocol if protocol is not None else caller.protocol,
        )

    async def resolve_capability(
        self, caller: "ExternalCaller", external_name: str
    ) -> Optional[Publication]:
        """``resolve_capability_for_group`` for an external caller."""
        return await self.resolve_capability_for_group(
            caller.group_ids, caller.protocol, external_name
        )

    async def publish(
        self,
        entity_id: str,
        data: CrewPublicationCreate,
        group_context: GroupContext,
        entity_type: str = "crew",
    ) -> Publication:
        """Publish a crew, or update its publication if it already has one.

        Idempotent by crew: publishing twice adjusts the existing record rather
        than creating a second one, which the unique constraint would reject
        anyway.
        """
        group_id = group_context.primary_group_id
        if not group_id:
            raise ValueError("Cannot publish without a group context.")

        existing = await self.repository.find_by_entity(
            entity_type=entity_type,
            entity_id=entity_id,
            group_ids=group_context.group_ids or [],
        )
        if existing is not None:
            existing.external_name = data.external_name
            existing.description = data.description
            existing.protocols = list(data.protocols)
            existing.input_schema = data.input_schema
            await self.session.flush()
            return existing

        row = Publication(
            entity_type=entity_type,
            entity_id=entity_id,
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
            "[external] published %s %s as %s over %s (group %s)",
            entity_type,
            entity_id,
            data.external_name,
            data.protocols,
            group_id,
        )
        return row

    async def update(
        self,
        entity_id: str,
        data: CrewPublicationUpdate,
        group_context: GroupContext,
        entity_type: str = "crew",
    ) -> Optional[Publication]:
        """Adjust an existing publication. Omitted fields are left alone."""
        row = await self.repository.find_by_entity(
            entity_type=entity_type,
            entity_id=entity_id,
            group_ids=group_context.group_ids or [],
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

    async def unpublish(
        self,
        entity_id: str,
        group_context: GroupContext,
        entity_type: str = "crew",
    ) -> bool:
        """Withdraw a crew or flow from every external surface."""
        removed = await self.repository.delete_by_entity(
            entity_type=entity_type,
            entity_id=entity_id,
            group_ids=group_context.group_ids or [],
        )
        if removed:
            logger.info("[external] unpublished %s %s", entity_type, entity_id)
        return removed > 0
