from typing import List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.crew_publication import Publication


class PublicationRepository(BaseRepository[Publication]):
    """Data access for crew publications.

    Every read here is group-filtered without exception. This repository backs
    surfaces that are reachable by callers OUTSIDE the workspace, so a query
    that forgets the filter is a cross-tenant data leak, not a display bug.
    There is deliberately no ``list_all``.
    """

    def __init__(self, session: AsyncSession):
        super().__init__(Publication, session)

    async def list_published_for_group(
        self, group_ids: List[str], protocol: Optional[str] = None
    ) -> List[Publication]:
        """Publications visible to ``group_ids``, optionally for one protocol.

        The single most security-sensitive query in the external-invocation work:
        it is what an MCP ``list_crews`` and an A2A Agent Card's ``skills[]`` both
        read. The natural bug is returning the workspace catalogue.

        An empty ``group_ids`` returns NOTHING rather than everything. A caller
        whose group could not be resolved must see an empty capability list, not
        every tenant's.

        ``protocol`` is filtered in Python, not SQL: ``protocols`` is a JSON
        column and JSON containment differs across SQLite / PostgreSQL /
        Lakebase. The group filter — the one that matters — is done in SQL.
        """
        if not group_ids:
            return []

        query = select(self.model).where(self.model.group_id.in_(group_ids))
        result = await self.session.execute(query)
        rows = list(result.scalars().all())

        if protocol is None:
            return rows
        return [r for r in rows if protocol in (r.protocols or [])]

    async def find_by_entity(
        self, entity_type: str, entity_id: str, group_ids: List[str]
    ) -> Optional[Publication]:
        """The publication for one crew or flow, if the caller's group may see it."""
        if not group_ids:
            return None

        query = select(self.model).where(
            self.model.entity_type == entity_type,
            self.model.entity_id == entity_id,
            self.model.group_id.in_(group_ids),
        )
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_by_external_name(
        self, external_name: str, group_ids: List[str]
    ) -> Optional[Publication]:
        """Resolve the name a caller used back to a publication.

        This is the lookup an adapter performs on every inbound invocation, so it
        is the point where a caller could reach another tenant's crew by guessing
        its name. Group-filtered for that reason.
        """
        if not group_ids:
            return None

        query = select(self.model).where(
            self.model.external_name == external_name,
            self.model.group_id.in_(group_ids),
        )
        result = await self.session.execute(query)
        return result.scalars().first()

    async def delete_by_entity(
        self, entity_type: str, entity_id: str, group_ids: List[str]
    ) -> int:
        """Unpublish a crew or flow. Returns the number of rows removed."""
        if not group_ids:
            return 0

        query = delete(self.model).where(
            self.model.entity_type == entity_type,
            self.model.entity_id == entity_id,
            self.model.group_id.in_(group_ids),
        )
        result = await self.session.execute(query)
        return result.rowcount or 0

    async def delete_publications(
        self,
        entity_type: str,
        entity_ids: Optional[List[str]] = None,
        group_ids: Optional[List[str]] = None,
    ) -> int:
        """Remove publications for a kind of entity. Returns rows removed.

        The write that keeps the registry from outliving what it names, called
        from every crew and flow delete path. ``None`` on either filter means
        "do not narrow by it" — which is why this is a DELETE and not a read:
        the no-filter rule at the top of this class exists because an unfiltered
        READ leaks other tenants' capability names, and a delete cannot. Its
        callers are the unscoped ``delete_all`` paths, which are already
        removing every crew or flow in the database; leaving their publications
        behind is what created the dangling rows this exists to prevent.

        A group-scoped caller still passes ``group_ids``, and should.
        """
        conditions = [self.model.entity_type == entity_type]
        if entity_ids is not None:
            if not entity_ids:
                return 0
            conditions.append(self.model.entity_id.in_(entity_ids))
        if group_ids is not None:
            if not group_ids:
                return 0
            conditions.append(self.model.group_id.in_(group_ids))

        result = await self.session.execute(delete(self.model).where(*conditions))
        return result.rowcount or 0


#: Named CrewPublicationRepository while only crews were publishable.
CrewPublicationRepository = PublicationRepository
