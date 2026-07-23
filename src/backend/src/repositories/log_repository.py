from typing import List, Optional, Dict, Any
from datetime import datetime

from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.log import LLMLog


class LLMLogRepository(BaseRepository[LLMLog]):
    """
    Repository for LLMLog model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """

    def __init__(self, session: AsyncSession):
        """Initialize the repository with session."""
        super().__init__(LLMLog, session)

    async def get_logs_paginated(
        self,
        page: int = 0,
        per_page: int = 10,
        endpoint: Optional[str] = None
    ) -> List[LLMLog]:
        """
        Get paginated logs with optional endpoint filtering.

        Args:
            page: Page number (0-indexed)
            per_page: Items per page
            endpoint: Optional endpoint to filter by

        Returns:
            List of LLM logs for the specified page
        """
        query = select(self.model).order_by(desc(self.model.created_at))
        if endpoint and endpoint != 'all':
            query = query.where(self.model.endpoint == endpoint)
        query = query.offset(page * per_page).limit(per_page)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def count_logs(self, endpoint: Optional[str] = None) -> int:
        """
        Count logs with optional endpoint filtering.

        Args:
            endpoint: Optional endpoint to filter by

        Returns:
            Total count of matching logs
        """
        # Start with a base query
        query = select(self.model)

        # Apply endpoint filter if provided
        if endpoint and endpoint != 'all':
            query = query.where(self.model.endpoint == endpoint)

        # Execute query
        result = await self.session.execute(query)
        return len(list(result.scalars().all()))
    
    async def get_unique_endpoints(self) -> List[str]:
        """
        Get list of unique endpoints in the logs.

        Returns:
            List of unique endpoint strings
        """
        query = select(self.model.endpoint).distinct()
        result = await self.session.execute(query)
        return [endpoint for (endpoint,) in result.all()]
    
    async def create(self, obj_in: Dict[str, Any]) -> LLMLog:
        """
        Create a new log entry.

        Args:
            obj_in: Dictionary of values to create model with

        Returns:
            The created model instance
        """
        # Normalize datetime objects to be timezone-naive
        normalized_obj = obj_in.copy()
        for key, value in normalized_obj.items():
            if isinstance(value, datetime) and value.tzinfo is not None:
                # Convert to naive datetime by replacing with the same values but without timezone
                normalized_obj[key] = value.replace(tzinfo=None)

        db_obj = self.model(**normalized_obj)
        self.session.add(db_obj)
        await self.session.flush()  # Use flush instead of commit
        await self.session.refresh(db_obj)
        return db_obj
    
    # Tenant-aware methods
    async def get_logs_paginated_by_tenant(
        self, 
        page: int = 0, 
        per_page: int = 10, 
        endpoint: Optional[str] = None,
        tenant_ids: List[str] = None
    ) -> List[LLMLog]:
        """
        Get paginated logs with optional endpoint filtering for specific tenants.
        
        Args:
            page: Page number (0-indexed)
            per_page: Items per page
            endpoint: Optional endpoint to filter by
            tenant_ids: List of tenant IDs to filter by
            
        Returns:
            List of LLM logs for the specified page and tenants
        """
        if not tenant_ids:
            return []
        
        # Start with a base query filtered by tenant
        query = select(self.model).where(
            self.model.tenant_id.in_(tenant_ids)
        ).order_by(desc(self.model.created_at))

        # Apply endpoint filter if provided
        if endpoint and endpoint != 'all':
            query = query.where(self.model.endpoint == endpoint)
            
            # Apply pagination
            query = query.offset(page * per_page).limit(per_page)
            
            # Execute query
            result = await session.execute(query)
            return list(result.scalars().all())

    async def count_logs_by_tenant(
        self, 
        endpoint: Optional[str] = None, 
        tenant_ids: List[str] = None
    ) -> int:
        """
        Count logs with optional endpoint filtering for specific tenants.
        
        Args:
            endpoint: Optional endpoint to filter by
            tenant_ids: List of tenant IDs to filter by
            
        Returns:
            Total count of matching logs for tenants
        """
        if not tenant_ids:
            return 0
        
        # Start with a base query filtered by tenant
        query = select(self.model).where(
            self.model.tenant_id.in_(tenant_ids)
        )

        # Apply endpoint filter if provided
        if endpoint and endpoint != 'all':
            query = query.where(self.model.endpoint == endpoint)

        # Execute query
        result = await self.session.execute(query)
        return len(list(result.scalars().all()))

    async def get_unique_endpoints_by_tenant(self, tenant_ids: List[str] = None) -> List[str]:
        """
        Get list of unique endpoints in the logs for specific tenants.
        
        Args:
            tenant_ids: List of tenant IDs to filter by
        
        Returns:
            List of unique endpoint strings for tenants
        """
        if not tenant_ids:
            return []
        
        query = select(self.model.endpoint).where(
            self.model.tenant_id.in_(tenant_ids)
        ).distinct()
        result = await self.session.execute(query)
        return [endpoint for (endpoint,) in result.all()]
    
    # Group-aware methods
    async def get_logs_paginated_by_group(
        self, 
        page: int = 0, 
        per_page: int = 10, 
        endpoint: Optional[str] = None,
        group_ids: List[str] = None
    ) -> List[LLMLog]:
        """
        Get paginated logs with optional endpoint filtering for specific groups.
        
        Args:
            page: Page number (0-indexed)
            per_page: Items per page
            endpoint: Optional endpoint to filter by
            group_ids: List of group IDs to filter by
            
        Returns:
            List of LLM logs for the specified page and groups
        """
        if not group_ids:
            return []
        
        # Start with a base query filtered by group
        query = select(self.model).where(
            self.model.group_id.in_(group_ids)
        ).order_by(desc(self.model.created_at))

        # Apply endpoint filter if provided
        if endpoint and endpoint != 'all':
            query = query.where(self.model.endpoint == endpoint)

        # Apply pagination
        query = query.offset(page * per_page).limit(per_page)

        # Execute query
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def count_logs_by_group(
        self, 
        endpoint: Optional[str] = None, 
        group_ids: List[str] = None
    ) -> int:
        """
        Count logs with optional endpoint filtering for specific groups.
        
        Args:
            endpoint: Optional endpoint to filter by
            group_ids: List of group IDs to filter by
            
        Returns:
            Total count of matching logs for groups
        """
        if not group_ids:
            return 0
        
        # Start with a base query filtered by group
        query = select(self.model).where(
            self.model.group_id.in_(group_ids)
        )

        # Apply endpoint filter if provided
        if endpoint and endpoint != 'all':
            query = query.where(self.model.endpoint == endpoint)

        # Execute query
        result = await self.session.execute(query)
        return len(list(result.scalars().all()))

    async def get_unique_endpoints_by_group(self, group_ids: List[str] = None) -> List[str]:
        """
        Get list of unique endpoints in the logs for specific groups.
        
        Args:
            group_ids: List of group IDs to filter by
        
        Returns:
            List of unique endpoint strings for groups
        """
        if not group_ids:
            return []
        
        query = select(self.model.endpoint).where(
            self.model.group_id.in_(group_ids)
        ).distinct()
        result = await self.session.execute(query)
        return [endpoint for (endpoint,) in result.all()] 