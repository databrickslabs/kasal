"""
Service for engine configuration operations.

This module provides business logic for engine configuration operations,
including retrieving and managing engine configurations.
"""

import logging
from typing import Dict, Any, Optional, List
from fastapi import HTTPException

from src.core.logger import LoggerManager
from src.repositories.engine_config_repository import EngineConfigRepository
from src.models.engine_config import EngineConfig

logger = LoggerManager.get_instance().crew

class EngineConfigService:
    """Service for engine configuration operations."""
    
    def __init__(self, repository: EngineConfigRepository):
        """
        Initialize the service with repository.
        
        Args:
            repository: EngineConfigRepository instance
        """
        self.repository = repository
    
    @classmethod
    async def from_unit_of_work(cls, uow):
        """
        Create a service instance from a UnitOfWork.
        
        Args:
            uow: UnitOfWork instance
            
        Returns:
            EngineConfigService: Service instance using the UnitOfWork's repository
        """
        return cls(repository=uow.engine_config_repository)
    
    async def find_all(self) -> List[EngineConfig]:
        """
        Get all engine configurations from the repository.
        
        Returns:
            List of all engine configurations
        """
        return await self.repository.find_all()
    
    async def find_enabled_configs(self) -> List[EngineConfig]:
        """
        Get all enabled engine configurations from the repository.
        
        Returns:
            List of enabled engine configurations
        """
        return await self.repository.find_enabled_configs()
    
    async def find_by_engine_name(self, engine_name: str) -> Optional[EngineConfig]:
        """
        Get an engine configuration by its name from the repository.
        
        Args:
            engine_name: The engine name to find
            
        Returns:
            Engine configuration if found, None otherwise
        """
        return await self.repository.find_by_engine_name(engine_name)
    
    async def find_by_engine_and_key(self, engine_name: str, config_key: str) -> Optional[EngineConfig]:
        """
        Get an engine configuration by engine name and config key.
        
        Args:
            engine_name: The engine name to find
            config_key: The configuration key to find
            
        Returns:
            Engine configuration if found, None otherwise
        """
        return await self.repository.find_by_engine_and_key(engine_name, config_key)
    
    async def find_by_engine_type(self, engine_type: str) -> List[EngineConfig]:
        """
        Get all engine configurations by engine type.
        
        Args:
            engine_type: The engine type to find
            
        Returns:
            List of engine configurations
        """
        return await self.repository.find_by_engine_type(engine_type)
    
    async def create_engine_config(self, config_data):
        """
        Create a new engine configuration.
        
        Args:
            config_data: Data for the new engine configuration
            
        Returns:
            Created engine configuration
            
        Raises:
            ValueError: If engine configuration with the same engine name and config key already exists
        """
        # Check if engine config already exists for this specific engine_name and config_key
        existing_config = await self.repository.find_by_engine_and_key(config_data.engine_name, config_data.config_key)
        if existing_config:
            raise ValueError(f"Engine configuration with name {config_data.engine_name} and key {config_data.config_key} already exists")
        
        # Convert Pydantic model to dict if needed
        if hasattr(config_data, "model_dump"):
            config_dict = config_data.model_dump()
        elif hasattr(config_data, "dict"):
            config_dict = config_data.dict()
        else:
            config_dict = dict(config_data)
        
        # Create new engine config
        return await self.repository.create(config_dict)
    
    async def update_engine_config(self, engine_name: str, config_data):
        """
        Update an existing engine configuration.
        
        Args:
            engine_name: Name of the engine to update
            config_data: Updated configuration data
            
        Returns:
            Updated engine configuration, or None if not found
        """
        # Check if engine config exists
        existing_config = await self.repository.find_by_engine_name(engine_name)
        if not existing_config:
            return None
        
        # Convert Pydantic model to dict if needed
        if hasattr(config_data, "model_dump"):
            config_dict = config_data.model_dump(exclude_unset=True)
        elif hasattr(config_data, "dict"):
            config_dict = config_data.dict(exclude_unset=True)
        else:
            config_dict = dict(config_data)
            
        # Update engine config
        return await self.repository.update(existing_config.id, config_dict)
    
    async def toggle_engine_enabled(self, engine_name: str, enabled: bool) -> Optional[EngineConfig]:
        """
        Toggle the enabled status of an engine configuration.
        
        Args:
            engine_name: Name of the engine to toggle
            enabled: New enabled status
            
        Returns:
            Updated engine configuration, or None if not found
        """
        try:
            # Use the direct DML method to avoid locking
            updated = await self.repository.toggle_enabled(engine_name, enabled)
            
            if not updated:
                return None
                
            # Get the updated engine config
            return await self.repository.find_by_engine_name(engine_name)
        except Exception as e:
            # Log the error at service level but don't expose internal details
            logger.error(f"Error in toggle_engine_enabled for engine={engine_name}: {str(e)}")
            # Re-raise for controller layer to handle
            raise
    
    async def update_config_value(self, engine_name: str, config_key: str, config_value: str) -> Optional[EngineConfig]:
        """
        Update the configuration value for a specific engine and key.
        
        Args:
            engine_name: Name of the engine
            config_key: Configuration key
            config_value: New configuration value
            
        Returns:
            Updated engine configuration, or None if not found
        """
        try:
            # Use the repository method to update config value
            updated = await self.repository.update_config_value(engine_name, config_key, config_value)
            
            if not updated:
                return None
                
            # Get the updated engine config
            return await self.repository.find_by_engine_and_key(engine_name, config_key)
        except Exception as e:
            # Log the error at service level but don't expose internal details
            logger.error(f"Error in update_config_value for {engine_name}.{config_key}: {str(e)}")
            # Re-raise for controller layer to handle
            raise
    
    async def get_crewai_flow_enabled(self) -> bool:
        """
        Get the CrewAI flow enabled status.
        
        Returns:
            True if flow is enabled, False otherwise (defaults to True if not found)
        """
        try:
            return await self.repository.get_crewai_flow_enabled()
        except Exception as e:
            logger.error(f"Error getting CrewAI flow enabled status: {str(e)}")
            return True  # Default to enabled on error
    
    async def set_crewai_flow_enabled(self, enabled: bool) -> bool:
        """
        Set the CrewAI flow enabled status.
        
        Args:
            enabled: Whether flow should be enabled
            
        Returns:
            True if successful
        """
        try:
            return await self.repository.set_crewai_flow_enabled(enabled)
        except Exception as e:
            logger.error(f"Error setting CrewAI flow enabled status: {str(e)}")
            raise
    
    async def delete_engine_config(self, engine_name: str) -> bool:
        """
        Delete an engine configuration.
        
        Args:
            engine_name: Name of the engine to delete
            
        Returns:
            True if deleted, False if not found
        """
        logger.info(f"Service: Attempting to delete engine config with name: {engine_name}")
        
        # Find the engine config first
        config = await self.repository.find_by_engine_name(engine_name)
        if not config:
            logger.warning(f"Engine config with name {engine_name} not found for deletion")
            return False
        
        # Delete the engine config
        try:
            await self.repository.delete(config.id)
            logger.info(f"Successfully deleted engine config with name {engine_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting engine config with name {engine_name}: {str(e)}")
            raise 