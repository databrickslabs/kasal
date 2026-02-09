from typing import List, Optional, Dict
import logging

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.powerbi_context_config import PowerBIBusinessMapping, PowerBIFieldSynonym

# Set up logger
logger = logging.getLogger(__name__)


class PowerBIBusinessMappingRepository(BaseRepository[PowerBIBusinessMapping]):
    """Repository for PowerBI business terminology mappings."""

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(PowerBIBusinessMapping, session)

    async def get_by_model(
        self,
        group_id: str,
        semantic_model_id: str
    ) -> List[PowerBIBusinessMapping]:
        """
        Get all business mappings for a specific semantic model and group.

        Args:
            group_id: Group ID for multi-tenant isolation
            semantic_model_id: Power BI semantic model ID

        Returns:
            List of business mappings
        """
        query = select(self.model).where(
            self.model.group_id == group_id,
            self.model.semantic_model_id == semantic_model_id
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_by_term(
        self,
        group_id: str,
        semantic_model_id: str,
        natural_term: str
    ) -> Optional[PowerBIBusinessMapping]:
        """
        Get a specific business mapping by natural term.

        Args:
            group_id: Group ID for multi-tenant isolation
            semantic_model_id: Power BI semantic model ID
            natural_term: Natural language business term

        Returns:
            Business mapping if found, else None
        """
        query = select(self.model).where(
            self.model.group_id == group_id,
            self.model.semantic_model_id == semantic_model_id,
            self.model.natural_term == natural_term
        )
        result = await self.session.execute(query)
        return result.scalars().first()

    async def delete_by_model(
        self,
        group_id: str,
        semantic_model_id: str
    ) -> int:
        """
        Delete all business mappings for a specific semantic model.

        Args:
            group_id: Group ID for multi-tenant isolation
            semantic_model_id: Power BI semantic model ID

        Returns:
            Number of deleted records
        """
        stmt = delete(self.model).where(
            self.model.group_id == group_id,
            self.model.semantic_model_id == semantic_model_id
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount

    async def get_as_dict(
        self,
        group_id: str,
        semantic_model_id: str
    ) -> Dict[str, str]:
        """
        Get business mappings as a dictionary (natural_term -> dax_expression).
        Used for tool integration.

        Args:
            group_id: Group ID for multi-tenant isolation
            semantic_model_id: Power BI semantic model ID

        Returns:
            Dictionary mapping natural terms to DAX expressions
        """
        mappings = await self.get_by_model(group_id, semantic_model_id)
        return {m.natural_term: m.dax_expression for m in mappings}


class PowerBIFieldSynonymRepository(BaseRepository[PowerBIFieldSynonym]):
    """Repository for PowerBI field synonyms."""

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(PowerBIFieldSynonym, session)

    async def get_by_model(
        self,
        group_id: str,
        semantic_model_id: str
    ) -> List[PowerBIFieldSynonym]:
        """
        Get all field synonyms for a specific semantic model and group.

        Args:
            group_id: Group ID for multi-tenant isolation
            semantic_model_id: Power BI semantic model ID

        Returns:
            List of field synonyms
        """
        query = select(self.model).where(
            self.model.group_id == group_id,
            self.model.semantic_model_id == semantic_model_id
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_by_field(
        self,
        group_id: str,
        semantic_model_id: str,
        field_name: str
    ) -> Optional[PowerBIFieldSynonym]:
        """
        Get field synonyms for a specific field.

        Args:
            group_id: Group ID for multi-tenant isolation
            semantic_model_id: Power BI semantic model ID
            field_name: Canonical field name

        Returns:
            Field synonym if found, else None
        """
        query = select(self.model).where(
            self.model.group_id == group_id,
            self.model.semantic_model_id == semantic_model_id,
            self.model.field_name == field_name
        )
        result = await self.session.execute(query)
        return result.scalars().first()

    async def delete_by_model(
        self,
        group_id: str,
        semantic_model_id: str
    ) -> int:
        """
        Delete all field synonyms for a specific semantic model.

        Args:
            group_id: Group ID for multi-tenant isolation
            semantic_model_id: Power BI semantic model ID

        Returns:
            Number of deleted records
        """
        stmt = delete(self.model).where(
            self.model.group_id == group_id,
            self.model.semantic_model_id == semantic_model_id
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount

    async def get_as_dict(
        self,
        group_id: str,
        semantic_model_id: str
    ) -> Dict[str, List[str]]:
        """
        Get field synonyms as a dictionary (field_name -> list of synonyms).
        Used for tool integration.

        Args:
            group_id: Group ID for multi-tenant isolation
            semantic_model_id: Power BI semantic model ID

        Returns:
            Dictionary mapping field names to lists of synonyms
        """
        synonyms = await self.get_by_model(group_id, semantic_model_id)
        return {s.field_name: s.synonyms for s in synonyms}
