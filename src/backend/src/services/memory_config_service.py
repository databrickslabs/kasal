"""
Memory configuration service for managing active memory backend configurations.

This module handles the logic for determining which memory backend configuration
is active for a given context.
"""
from typing import Optional, Dict, Any
from src.models.memory_backend import MemoryBackend
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    DatabricksMemoryConfig,
    MemoryBackendType
)
from src.core.logger import LoggerManager
from src.core.unit_of_work import UnitOfWork

logger = LoggerManager.get_instance().system


class MemoryConfigService:
    """Service for managing memory backend configuration retrieval."""
    
    def __init__(self, uow: UnitOfWork):
        """
        Initialize the service.
        
        Args:
            uow: Unit of Work instance
        """
        self.uow = uow
    
    async def get_active_config(self, group_id: str = None) -> Optional[MemoryBackendConfig]:
        """
        Get the active memory backend configuration.
        
        Priority order:
        1. If group_id provided: Get the latest updated active configuration for that group
        2. Otherwise: Get any default configuration marked with is_default=true
        
        Args:
            group_id: Group ID (optional - will try system default if not provided)
            
        Returns:
            Active memory backend configuration or None
        """
        # Don't re-enter the UnitOfWork context - it's already active
        repo = self.uow.memory_backend_repository
        
        # If group_id is provided, get the latest active configuration for that group
        if group_id:
            # Get all active backends for the group, sorted by updated_at desc
            backends = await repo.get_by_group_id(group_id)
            
            # Filter for active backends and get the most recently updated one
            active_backends = [b for b in backends if b.is_active]
            if active_backends:
                # Prefer Databricks backend if available, otherwise get the most recently updated
                databricks_backends = [b for b in active_backends if b.backend_type == MemoryBackendType.DATABRICKS]
                if databricks_backends:
                    # If there are Databricks backends, use the most recent one
                    latest_backend = sorted(databricks_backends, key=lambda x: x.updated_at, reverse=True)[0]
                    logger.info(f"Preferring Databricks backend: {latest_backend.name}")
                else:
                    # Otherwise, use the most recently updated backend
                    latest_backend = sorted(active_backends, key=lambda x: x.updated_at, reverse=True)[0]
                
                # Convert databricks_config dict to DatabricksMemoryConfig if needed
                databricks_config = None
                if latest_backend.databricks_config and latest_backend.backend_type == MemoryBackendType.DATABRICKS:
                    databricks_config = DatabricksMemoryConfig(**latest_backend.databricks_config)
                
                logger.info(f"Found latest active backend for group {group_id}: {latest_backend.name} (type: {latest_backend.backend_type})")
                
                return MemoryBackendConfig(
                    backend_type=latest_backend.backend_type,
                    databricks_config=databricks_config,
                    enable_short_term=latest_backend.enable_short_term,
                    enable_long_term=latest_backend.enable_long_term,
                    enable_entity=latest_backend.enable_entity,
                    custom_config=latest_backend.custom_config
                )
        
        # If no group-specific backend, try to find any active backend with is_default=true
        # This is a fallback for system-wide defaults
        all_backends = await repo.get_all()
        for backend in all_backends:
            if backend.is_active and backend.is_default:
                # Convert databricks_config dict to DatabricksMemoryConfig if needed
                databricks_config = None
                if backend.databricks_config and backend.backend_type == MemoryBackendType.DATABRICKS:
                    databricks_config = DatabricksMemoryConfig(**backend.databricks_config)
                
                return MemoryBackendConfig(
                    backend_type=backend.backend_type,
                    databricks_config=databricks_config,
                    enable_short_term=backend.enable_short_term,
                    enable_long_term=backend.enable_long_term,
                    enable_entity=backend.enable_entity,
                    custom_config=backend.custom_config
                )
        
        return None