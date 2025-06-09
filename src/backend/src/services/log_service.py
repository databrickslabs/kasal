from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.log import LLMLog
from src.repositories.log_repository import LLMLogRepository
from src.utils.user_context import GroupContext


class LLMLogService:
    """
    Service for LLMLog model with business logic.
    Handles log retrieval, creation, and analysis.
    """
    
    def __init__(self, repository: LLMLogRepository):
        """
        Initialize the service with its repository.
        
        Args:
            repository: LLMLogRepository instance
        """
        self.repository = repository
    
    @classmethod
    def create(cls) -> 'LLMLogService':
        """
        Factory method to create a properly configured instance of the service.
        
        This method abstracts the creation of dependencies while maintaining
        proper separation of concerns.
        
        Returns:
            An instance of LLMLogService with all required dependencies
        """
        from src.repositories.log_repository import LLMLogRepository
        return cls(repository=LLMLogRepository())
    
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
        return await self.repository.get_logs_paginated(page, per_page, endpoint)
    
    async def count_logs(self, endpoint: Optional[str] = None) -> int:
        """
        Count logs with optional endpoint filtering.
        
        Args:
            endpoint: Optional endpoint to filter by
            
        Returns:
            Total count of matching logs
        """
        return await self.repository.count_logs(endpoint)
        
    async def get_unique_endpoints(self) -> List[str]:
        """
        Get list of unique endpoints in the logs.
        
        Returns:
            List of unique endpoint strings
        """
        return await self.repository.get_unique_endpoints()
    
    async def create_log(
        self,
        endpoint: str,
        prompt: str,
        response: str,
        model: str,
        status: str,
        tokens_used: Optional[int] = None,
        duration_ms: Optional[int] = None,
        error_message: Optional[str] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        group_context: Optional[GroupContext] = None
    ) -> LLMLog:
        """
        Create a new LLM log entry with optional tenant context.
        
        Args:
            endpoint: The API endpoint that was called
            prompt: The prompt sent to the LLM
            response: The response from the LLM
            model: The model name used
            status: Status of the request ('success' or 'error')
            tokens_used: Number of tokens used
            duration_ms: Duration in milliseconds
            error_message: Error message if any
            extra_data: Any additional data to store
            group_context: Optional group context for multi-group isolation
            
        Returns:
            The created LLM log
        """
        log_data = {
            "endpoint": endpoint,
            "prompt": prompt,
            "response": response,
            "model": model,
            "status": status,
            "tokens_used": tokens_used,
            "duration_ms": duration_ms,
            "error_message": error_message,
            "extra_data": extra_data or {},
            "created_at": datetime.now(timezone.utc)
        }
        
        # Add group fields if group context is provided and has valid values
        if group_context and group_context.primary_group_id:
            log_data["group_id"] = group_context.primary_group_id
            log_data["group_email"] = group_context.group_email
        
        return await self.repository.create(log_data)
    
    async def get_log_stats(self, days: int = 30) -> Dict[str, Any]:
        """
        Get statistics about LLM usage.
        
        Args:
            days: Number of days to include in stats
            
        Returns:
            Dictionary with usage statistics
        """
        # This would be a more complex implementation in a real system
        # but for this example, we'll just return a simple count
        total_count = await self.count_logs()
        endpoints = await self.get_unique_endpoints()
        
        # Count by endpoint
        endpoint_counts = {}
        for endpoint in endpoints:
            endpoint_counts[endpoint] = await self.count_logs(endpoint)
            
        return {
            "total_logs": total_count,
            "endpoints": endpoints,
            "counts_by_endpoint": endpoint_counts,
            "days_included": days
        }
    
    # Group-aware methods
    async def get_logs_paginated_by_group(
        self, 
        page: int = 0, 
        per_page: int = 10, 
        endpoint: Optional[str] = None,
        group_context: GroupContext = None
    ) -> List[LLMLog]:
        """
        Get paginated logs with optional endpoint filtering for a specific group.
        
        Args:
            page: Page number (0-indexed)
            per_page: Items per page
            endpoint: Optional endpoint to filter by
            group_context: Group context for filtering
            
        Returns:
            List of LLM logs for the specified page and group
        """
        if not group_context or not group_context.group_ids:
            # If no group context, return empty list for security
            return []
        
        return await self.repository.get_logs_paginated_by_group(
            page, per_page, endpoint, group_context.group_ids
        )

    async def count_logs_by_group(
        self, 
        endpoint: Optional[str] = None, 
        group_context: GroupContext = None
    ) -> int:
        """
        Count logs with optional endpoint filtering for a specific group.
        
        Args:
            endpoint: Optional endpoint to filter by
            group_context: Group context for filtering
            
        Returns:
            Total count of matching logs for group
        """
        if not group_context or not group_context.group_ids:
            return 0
        
        return await self.repository.count_logs_by_group(endpoint, group_context.group_ids)

    async def get_unique_endpoints_by_group(self, group_context: GroupContext = None) -> List[str]:
        """
        Get list of unique endpoints in the logs for a specific group.
        
        Args:
            group_context: Group context for filtering
        
        Returns:
            List of unique endpoint strings for group
        """
        if not group_context or not group_context.group_ids:
            return []
        
        return await self.repository.get_unique_endpoints_by_group(group_context.group_ids)

    async def get_log_stats_by_group(
        self, 
        days: int = 30, 
        group_context: GroupContext = None
    ) -> Dict[str, Any]:
        """
        Get statistics about LLM usage for a specific group.
        
        Args:
            days: Number of days to include in stats
            group_context: Group context for filtering
            
        Returns:
            Dictionary with usage statistics for group
        """
        if not group_context or not group_context.group_ids:
            return {
                "total_logs": 0,
                "endpoints": [],
                "counts_by_endpoint": {},
                "days_included": days
            }
        
        total_count = await self.count_logs_by_group(None, group_context)
        endpoints = await self.get_unique_endpoints_by_group(group_context)
        
        # Count by endpoint
        endpoint_counts = {}
        for endpoint in endpoints:
            endpoint_counts[endpoint] = await self.count_logs_by_group(endpoint, group_context)
            
        return {
            "total_logs": total_count,
            "endpoints": endpoints,
            "counts_by_endpoint": endpoint_counts,
            "days_included": days
        } 