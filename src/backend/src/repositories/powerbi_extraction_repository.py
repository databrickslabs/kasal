"""
PowerBI Extraction repository.

Data-access for :class:`PowerBIExtraction` — the raw artifacts the Pipeline
Config Generator extracts per run. Read helpers are scoped by the common query
axes (execution, workspace, dataset, group) so callers never hand-write filters.
"""

from typing import List, Optional

from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.powerbi_extraction import PowerBIExtraction


class PowerBIExtractionRepository(BaseRepository[PowerBIExtraction]):
    """Repository for PowerBIExtraction (one row per config-gen run)."""

    def __init__(self, session: AsyncSession):
        super().__init__(PowerBIExtraction, session)

    async def find_by_execution_id(self, execution_id: str) -> List[PowerBIExtraction]:
        """All extractions recorded for a crew/flow execution (newest first)."""
        query = (
            select(self.model)
            .where(self.model.execution_id == execution_id)
            .order_by(desc(self.model.created_at))
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_by_dataset(
        self,
        dataset_id: str,
        group_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[PowerBIExtraction]:
        """Extractions for a PBI dataset (optionally group-scoped), newest first."""
        query = select(self.model).where(self.model.dataset_id == dataset_id)
        if group_id is not None:
            query = query.where(self.model.group_id == group_id)
        query = query.order_by(desc(self.model.created_at)).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_by_group(
        self,
        group_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[PowerBIExtraction]:
        """Extractions for a group (tenant), newest first, paginated."""
        query = (
            select(self.model)
            .where(self.model.group_id == group_id)
            .order_by(desc(self.model.created_at))
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_latest_for_dataset(
        self,
        dataset_id: str,
        group_id: Optional[str] = None,
    ) -> Optional[PowerBIExtraction]:
        """Most recent extraction for a dataset (optionally group-scoped)."""
        rows = await self.find_by_dataset(dataset_id, group_id=group_id, limit=1)
        return rows[0] if rows else None
