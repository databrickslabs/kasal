"""
Repository for PowerBI Semantic Model Cache operations.

Handles database operations for semantic model metadata caching.
"""

from datetime import date, timedelta
from typing import Optional, Dict, Any
from sqlalchemy import select, and_, or_
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
        report_id: Optional[str] = None,
        any_report_id: bool = False,
    ) -> Optional[PowerBISemanticModelCache]:
        """
        Get cached metadata if a valid entry exists within the TTL window.

        Args:
            group_id: Multi-tenant group ID
            dataset_id: Power BI dataset/semantic model ID
            workspace_id: Power BI workspace ID
            report_id: Optional report ID (if filters are report-specific)
            any_report_id: If True, match any report_id (ignore report_id filter)

        Returns:
            Most recent cache object within TTL, None otherwise
        """
        from src.models.powerbi_semantic_model_cache import PowerBISemanticModelCache as _M
        cutoff = date.today() - timedelta(days=_M.CACHE_TTL_DAYS)

        if any_report_id:
            # Match any report_id except 'reduced' — used by tools that need
            # the full model cache (e.g., Metadata Reducer). The 'reduced'
            # report_id is a synthetic entry created by the Reducer itself.
            query = select(PowerBISemanticModelCache).where(
                and_(
                    PowerBISemanticModelCache.group_id == group_id,
                    PowerBISemanticModelCache.dataset_id == dataset_id,
                    PowerBISemanticModelCache.workspace_id == workspace_id,
                    PowerBISemanticModelCache.cached_date >= cutoff,
                    or_(
                        PowerBISemanticModelCache.report_id.is_(None),
                        PowerBISemanticModelCache.report_id != "reduced",
                    ),
                )
            )
        elif report_id:
            query = select(PowerBISemanticModelCache).where(
                and_(
                    PowerBISemanticModelCache.group_id == group_id,
                    PowerBISemanticModelCache.dataset_id == dataset_id,
                    PowerBISemanticModelCache.workspace_id == workspace_id,
                    PowerBISemanticModelCache.report_id == report_id,
                    PowerBISemanticModelCache.cached_date >= cutoff,
                )
            )
        else:
            query = select(PowerBISemanticModelCache).where(
                and_(
                    PowerBISemanticModelCache.group_id == group_id,
                    PowerBISemanticModelCache.dataset_id == dataset_id,
                    PowerBISemanticModelCache.workspace_id == workspace_id,
                    PowerBISemanticModelCache.report_id.is_(None),
                    PowerBISemanticModelCache.cached_date >= cutoff,
                )
            )

        # Return the most recent entry within the TTL window
        result = await self.session.execute(
            query.order_by(PowerBISemanticModelCache.cached_date.desc())
        )
        return result.scalars().first()

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
