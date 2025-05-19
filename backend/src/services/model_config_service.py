"""
Service for model configuration operations.

This module provides business logic for model configuration operations,
including retrieving and managing model configurations.
"""

import logging
from typing import Dict, Any, Optional, List
from fastapi import HTTPException

from src.utils.model_config import get_model_config
from src.core.logger import LoggerManager
from src.services.api_keys_service import ApiKeysService
from src.repositories.model_config_repository import ModelConfigRepository
from src.models.model_config import ModelConfig

logger = LoggerManager.get_instance().crew

class ModelConfigService:
    """Service for model configuration operations."""
    
    def __init__(self, repository: ModelConfigRepository):
        """
        Initialize the service with repository.
        
        Args:
            repository: ModelConfigRepository instance
        """
        self.repository = repository
    
    @classmethod
    async def from_unit_of_work(cls, uow):
        """
        Create a service instance from a UnitOfWork.
        
        Args:
            uow: UnitOfWork instance
            
        Returns:
            ModelConfigService: Service instance using the UnitOfWork's repository
        """
        return cls(repository=uow.model_config_repository)
    
    async def find_all(self) -> List[ModelConfig]:
        """
        Get all model configurations from the repository.
        
        Returns:
            List of all model configurations
        """
        return await self.repository.find_all()
    
    async def find_enabled_models(self) -> List[ModelConfig]:
        """
        Get all enabled model configurations from the repository.
        
        Returns:
            List of enabled model configurations
        """
        return await self.repository.find_enabled_models()
    
    async def find_by_key(self, key: str) -> Optional[ModelConfig]:
        """
        Get a model configuration by its key from the repository.
        
        Args:
            key: The model key to find
            
        Returns:
            Model configuration if found, None otherwise
        """
        return await self.repository.find_by_key(key)
    
    async def create_model_config(self, model_data):
        """
        Create a new model configuration.
        
        Args:
            model_data: Data for the new model configuration
            
        Returns:
            Created model configuration
            
        Raises:
            ValueError: If model with the same key already exists
        """
        # Check if model already exists
        existing_model = await self.repository.find_by_key(model_data.key)
        if existing_model:
            raise ValueError(f"Model with key {model_data.key} already exists")
        
        # Convert Pydantic model to dict if needed
        if hasattr(model_data, "model_dump"):
            model_dict = model_data.model_dump()
        elif hasattr(model_data, "dict"):
            model_dict = model_data.dict()
        else:
            model_dict = dict(model_data)
        
        # Create new model
        return await self.repository.create(model_dict)
    
    async def update_model_config(self, key: str, model_data):
        """
        Update an existing model configuration.
        
        Args:
            key: Key of the model to update
            model_data: Updated model data
            
        Returns:
            Updated model configuration, or None if not found
        """
        # Check if model exists
        existing_model = await self.repository.find_by_key(key)
        if not existing_model:
            return None
        
        # Convert Pydantic model to dict if needed
        if hasattr(model_data, "model_dump"):
            model_dict = model_data.model_dump(exclude_unset=True)
        elif hasattr(model_data, "dict"):
            model_dict = model_data.dict(exclude_unset=True)
        else:
            model_dict = dict(model_data)
            
        # Update model
        return await self.repository.update(existing_model.id, model_dict)
    
    async def toggle_model_enabled(self, key: str, enabled: bool) -> Optional[ModelConfig]:
        """
        Toggle the enabled status of a model configuration.
        
        Args:
            key: Key of the model to toggle
            enabled: New enabled status
            
        Returns:
            Updated model configuration, or None if not found
        """
        try:
            # Use the direct DML method to avoid locking
            updated = await self.repository.toggle_enabled(key, enabled)
            
            if not updated:
                return None
                
            # Get the updated model
            return await self.repository.find_by_key(key)
        except Exception as e:
            # Log the error at service level but don't expose internal details
            logger.error(f"Error in toggle_model_enabled for key={key}: {str(e)}")
            # Re-raise for controller layer to handle
            raise
    
    async def delete_model_config(self, key: str) -> bool:
        """
        Delete a model configuration.
        
        Args:
            key: Key of the model to delete
            
        Returns:
            True if deleted, False if not found
        """
        logger.info(f"Service: Attempting to delete model with key: {key}")
        
        # Use the dedicated repository method for deletion by key
        return await self.repository.delete_by_key(key)
    
    async def enable_all_models(self) -> List[ModelConfig]:
        """
        Enable all model configurations.
        
        Returns:
            List of all model configurations after enabling
        """
        try:
            # Enable all models with a single operation
            success = await self.repository.enable_all_models()
            if not success:
                logger.warning("Failed to enable all models")
                
            # Return all models
            return await self.find_all()
        except Exception as e:
            logger.error(f"Error enabling all models: {str(e)}")
            raise
    
    async def disable_all_models(self) -> List[ModelConfig]:
        """
        Disable all model configurations.
        
        Returns:
            List of all model configurations after disabling
        """
        try:
            # Disable all models with a single operation
            success = await self.repository.disable_all_models()
            if not success:
                logger.warning("Failed to disable all models")
                
            # Return all models
            return await self.find_all()
        except Exception as e:
            logger.error(f"Error disabling all models: {str(e)}")
            raise
    
    async def get_model_config(self, model: str) -> Dict[str, Any]:
        """
        Get configuration for a specific model.
        
        Args:
            model: Name of the model to get configuration for
            
        Returns:
            Dictionary containing model configuration
            
        Raises:
            HTTPException: If model configuration is not found
        """
        try:
            # Try to get from repository first
            model_config = await self.repository.find_by_key(model)
            if model_config:
                config = {
                    "key": model_config.key,
                    "name": model_config.name,
                    "provider": model_config.provider,
                    "temperature": model_config.temperature,
                    "context_window": model_config.context_window,
                    "max_output_tokens": model_config.max_output_tokens,
                    "extended_thinking": model_config.extended_thinking,
                    "enabled": model_config.enabled
                }
            else:
                # Fall back to utility function
                config = get_model_config(model)
                if not config:
                    raise ValueError(f"Model configuration not found for model: {model}")
            
            # Get API key for the provider using class method
            provider = config["provider"].lower()
            api_key = await ApiKeysService.get_provider_api_key(provider)
            if not api_key:
                raise ValueError(f"No API key found for provider: {provider}")
            
            # Add API key to config
            config["api_key"] = api_key
            return config
            
        except Exception as e:
            logger.error(f"Error getting model configuration: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get model configuration: {str(e)}"
            ) 