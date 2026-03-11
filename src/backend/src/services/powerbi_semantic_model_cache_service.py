"""
Service for managing PowerBI Semantic Model Cache.

Handles cache retrieval, storage, and validation for semantic model metadata.
"""

from typing import Optional, Dict, Any
from datetime import date
from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.powerbi_semantic_model_cache_repository import PowerBISemanticModelCacheRepository
from src.models.powerbi_semantic_model_cache import PowerBISemanticModelCache


class PowerBISemanticModelCacheService:
    """Service for managing PowerBI semantic model metadata cache."""

    def __init__(self, session: AsyncSession):
        """
        Initialize service with database session.

        Args:
            session: SQLAlchemy async session
        """
        self.repository = PowerBISemanticModelCacheRepository(session)

    async def get_cached_metadata(
        self,
        group_id: str,
        dataset_id: str,
        workspace_id: str,
        report_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get cached metadata for today if available.

        Args:
            group_id: Multi-tenant group ID
            dataset_id: Power BI dataset/semantic model ID
            workspace_id: Power BI workspace ID
            report_id: Optional report ID

        Returns:
            Cached metadata dictionary if found, None otherwise
        """
        cache = await self.repository.get_cache_for_today(
            group_id=group_id,
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            report_id=report_id
        )

        if cache and cache.is_valid_for_today():
            return cache.cache_data

        return None

    async def save_metadata(
        self,
        group_id: str,
        dataset_id: str,
        workspace_id: str,
        metadata: Dict[str, Any],
        report_id: Optional[str] = None
    ) -> PowerBISemanticModelCache:
        """
        Save or update cached metadata for today.

        Args:
            group_id: Multi-tenant group ID
            dataset_id: Power BI dataset/semantic model ID
            workspace_id: Power BI workspace ID
            metadata: Metadata dictionary to cache
            report_id: Optional report ID

        Returns:
            Cache object (created or updated)
        """
        # Check if cache already exists for today
        existing_cache = await self.repository.get_cache_for_today(
            group_id=group_id,
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            report_id=report_id
        )

        if existing_cache:
            # Update existing cache
            return await self.repository.update_cache(existing_cache, metadata)
        else:
            # Create new cache
            return await self.repository.create_cache(
                group_id=group_id,
                dataset_id=dataset_id,
                workspace_id=workspace_id,
                metadata=metadata,
                report_id=report_id
            )

    async def cleanup_old_caches(self, days_to_keep: int = 7) -> int:
        """
        Remove cache entries older than specified days.

        Args:
            days_to_keep: Number of days to keep cache entries

        Returns:
            Number of deleted entries
        """
        return await self.repository.delete_old_caches(days_to_keep)

    def build_metadata_dict(
        self,
        measures: list,
        relationships: list,
        schema: Dict[str, Any],
        sample_data: Dict[str, Any],
        default_filters: Optional[Dict[str, Any]] = None,
        slicers: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Build metadata dictionary for caching.

        Args:
            measures: List of measure definitions
            relationships: List of table relationships
            schema: Schema information (tables, columns)
            sample_data: Sample data values
            default_filters: Optional default filters from report
            slicers: Optional list of slicer definitions from report

        Returns:
            Metadata dictionary ready for caching
        """
        metadata = {
            "measures": measures,
            "relationships": relationships,
            "schema": schema,
            "sample_data": sample_data
        }

        if default_filters:
            metadata["default_filters"] = default_filters

        if slicers:
            metadata["slicers"] = slicers

        return metadata
