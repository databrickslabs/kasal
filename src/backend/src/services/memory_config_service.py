"""
Memory configuration service for managing active memory backend configurations.

This module handles the logic for determining which memory backend configuration
is active for a given context.
"""
import os
from typing import Optional, Dict, Any
from src.models.memory_backend import MemoryBackend
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    DatabricksMemoryConfig,
    MemoryBackendType
)
from src.core.logger import LoggerManager
from src.core.unit_of_work import UnitOfWork
from src.utils.databricks_auth import is_databricks_apps_environment

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
                    config_dict = latest_backend.databricks_config.copy()
                    
                    # In Databricks Apps, override workspace_url with DATABRICKS_HOST if not set
                    if is_databricks_apps_environment() and not config_dict.get('workspace_url'):
                        databricks_host = os.getenv("DATABRICKS_HOST")
                        if databricks_host:
                            workspace_url = databricks_host if databricks_host.startswith("http") else f"https://{databricks_host}"
                            config_dict['workspace_url'] = workspace_url
                            logger.info(f"Auto-populating workspace_url from DATABRICKS_HOST: {workspace_url}")
                    
                    databricks_config = DatabricksMemoryConfig(**config_dict)
                
                logger.info(f"Found latest active backend for group {group_id}: {latest_backend.name} (type: {latest_backend.backend_type})")
                logger.info(f"Backend enable_relationship_retrieval value: {latest_backend.enable_relationship_retrieval}")
                
                config = MemoryBackendConfig(
                    backend_type=latest_backend.backend_type,
                    databricks_config=databricks_config,
                    enable_short_term=latest_backend.enable_short_term,
                    enable_long_term=latest_backend.enable_long_term,
                    enable_entity=latest_backend.enable_entity,
                    enable_relationship_retrieval=latest_backend.enable_relationship_retrieval,
                    custom_config=latest_backend.custom_config
                )
                
                logger.info(f"Created MemoryBackendConfig with enable_relationship_retrieval: {config.enable_relationship_retrieval}")
                return config
        
        # If no group-specific backend, try to find any active backend with is_default=true
        # This is a fallback for system-wide defaults
        all_backends = await repo.get_all()
        for backend in all_backends:
            if backend.is_active and backend.is_default:
                # Convert databricks_config dict to DatabricksMemoryConfig if needed
                databricks_config = None
                if backend.databricks_config and backend.backend_type == MemoryBackendType.DATABRICKS:
                    config_dict = backend.databricks_config.copy()
                    
                    # In Databricks Apps, override workspace_url with DATABRICKS_HOST if not set
                    if is_databricks_apps_environment() and not config_dict.get('workspace_url'):
                        databricks_host = os.getenv("DATABRICKS_HOST")
                        if databricks_host:
                            workspace_url = databricks_host if databricks_host.startswith("http") else f"https://{databricks_host}"
                            config_dict['workspace_url'] = workspace_url
                            logger.info(f"Auto-populating workspace_url from DATABRICKS_HOST: {workspace_url}")
                    
                    databricks_config = DatabricksMemoryConfig(**config_dict)
                
                return MemoryBackendConfig(
                    backend_type=backend.backend_type,
                    databricks_config=databricks_config,
                    enable_short_term=backend.enable_short_term,
                    enable_long_term=backend.enable_long_term,
                    enable_entity=backend.enable_entity,
                    enable_relationship_retrieval=backend.enable_relationship_retrieval,
                    custom_config=backend.custom_config
                )
        
        return None