"""
Repository for PowerBI Semantic Model Cache operations.

Handles database operations for semantic model metadata caching.
"""

from datetime import date
from typing import Optional, Dict, Any
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.powerbi_semantic_model_cache import PowerBISemanticModelCache


class PowerBISemanticModelCacheRepository:
    """Repository for PowerBI semantic model cache CRUD operations."""

    def __init__(self, session: AsyncSession):
        """
        Initialize repository with database session.

        Args:
            session: SQLAlchemy async session
        """
        self.session = session

    async def get_cache_for_today(
        self,
        group_id: str,
        dataset_id: str,
        workspace_id: str,
        report_id: Optional[str] = None
    ) -> Optional[PowerBISemanticModelCache]:
        """
        Get cached metadata for today if it exists.

        Args:
            group_id: Multi-tenant group ID
            dataset_id: Power BI dataset/semantic model ID
            workspace_id: Power BI workspace ID
            report_id: Optional report ID (if filters are report-specific)

        Returns:
            Cache object if found and valid for today, None otherwise
        """
        today = date.today()

        # Build query based on whether report_id is provided
        if report_id:
            query = select(PowerBISemanticModelCache).where(
                and_(
                    PowerBISemanticModelCache.group_id == group_id,
                    PowerBISemanticModelCache.dataset_id == dataset_id,
                    PowerBISemanticModelCache.workspace_id == workspace_id,
                    PowerBISemanticModelCache.report_id == report_id,
                    PowerBISemanticModelCache.cached_date == today
                )
            )
        else:
            query = select(PowerBISemanticModelCache).where(
                and_(
                    PowerBISemanticModelCache.group_id == group_id,
                    PowerBISemanticModelCache.dataset_id == dataset_id,
                    PowerBISemanticModelCache.workspace_id == workspace_id,
                    PowerBISemanticModelCache.report_id.is_(None),
                    PowerBISemanticModelCache.cached_date == today
                )
            )

        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def create_cache(
        self,
        group_id: str,
        dataset_id: str,
        workspace_id: str,
        metadata: Dict[str, Any],
        report_id: Optional[str] = None
    ) -> PowerBISemanticModelCache:
        """
        Create new cache entry for today.

        Args:
            group_id: Multi-tenant group ID
            dataset_id: Power BI dataset/semantic model ID
            workspace_id: Power BI workspace ID
            metadata: Cached metadata dictionary
            report_id: Optional report ID

        Returns:
            Created cache object
        """
        cache = PowerBISemanticModelCache(
            group_id=group_id,
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            report_id=report_id,
            cached_date=date.today(),
            cache_data=metadata
        )

        self.session.add(cache)
        await self.session.commit()
        await self.session.refresh(cache)

        return cache

    async def update_cache(
        self,
        cache: PowerBISemanticModelCache,
        metadata: Dict[str, Any]
    ) -> PowerBISemanticModelCache:
        """
        Update existing cache entry with new metadata.

        Args:
            cache: Existing cache object
            metadata: Updated metadata dictionary

        Returns:
            Updated cache object
        """
        cache.cache_data = metadata
        cache.cached_date = date.today()  # Refresh cache date

        await self.session.commit()
        await self.session.refresh(cache)

        return cache

    async def delete_old_caches(self, days_to_keep: int = 7) -> int:
        """
        Delete cache entries older than specified days.

        Args:
            days_to_keep: Number of days to keep cache entries

        Returns:
            Number of deleted entries
        """
        from datetime import timedelta

        cutoff_date = date.today() - timedelta(days=days_to_keep)

        result = await self.session.execute(
            select(PowerBISemanticModelCache).where(
                PowerBISemanticModelCache.cached_date < cutoff_date
            )
        )
        old_caches = result.scalars().all()

        for cache in old_caches:
            await self.session.delete(cache)

        await self.session.commit()

        return len(old_caches)
