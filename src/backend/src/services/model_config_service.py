"""
Service for model configuration operations.

This module provides business logic for model configuration operations,
including retrieving and managing model configurations.
"""

import logging
from typing import Dict, Any, Optional, List
from fastapi import HTTPException, status

from src.utils.model_config import get_model_config
from src.core.logger import LoggerManager
from src.services.api_keys_service import ApiKeysService
from src.repositories.model_config_repository import ModelConfigRepository
from sqlalchemy.ext.asyncio import AsyncSession
from src.models.model_config import ModelConfig
from src.utils.user_context import GroupContext

logger = LoggerManager.get_instance().crew

class ModelConfigService:
    """Service for model configuration operations."""

    def __init__(self, session: AsyncSession, group_id: Optional[str] = None):
        """
        Initialize the service with session.

        Args:
            session: Database session
            group_id: Group ID for multi-tenant isolation (optional for read operations,
                     REQUIRED for API key operations)

        Note:
            group_id is optional for reading model configurations (which are not tenant-specific),
            but REQUIRED for any operations involving API keys (multi-tenant isolation).
        """
        self.repository = ModelConfigRepository(session)
        # SECURITY: Store group_id for multi-tenant API key isolation
        # Will be validated when needed (e.g., in API key operations)
        self.group_id = group_id


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
            # Normalize model key (handle provider-prefixed routes like "databricks/model-key")
            normalized_key = model.rsplit('/', 1)[-1] if isinstance(model, str) else model

            # Try to get from repository first using normalized key
            model_config = await self.repository.find_by_key(normalized_key)
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
                # Fall back to utility function with normalized key (best-effort; may be None without a sync DB session)
                config = get_model_config(normalized_key)
                if not config:
                    raise ValueError(f"Model configuration not found for model: {model}")

            # Get API key for the provider using class method
            provider = config["provider"].lower()

            # Check if we're using Databricks provider - unified auth handles it
            if provider == "databricks":
                logger.info("Databricks provider - unified auth will handle authentication")
                # Don't add API key for Databricks - unified auth handles it
                return config

            # For non-Databricks providers, get API key
            # SECURITY: group_id is REQUIRED for API key operations (multi-tenant isolation)
            if not self.group_id:
                raise ValueError(
                    f"SECURITY: group_id is REQUIRED for fetching API keys for provider '{provider}'. "
                    "All API key operations must be scoped to a group for multi-tenant isolation."
                )
            api_key = await ApiKeysService.get_provider_api_key(provider, group_id=self.group_id)
            if not api_key:
                # Try to use unified auth for external providers if available
                try:
                    from src.utils.databricks_auth import get_auth_context
                    auth = await get_auth_context()
                    if auth and auth.auth_method in ["obo", "service_principal"]:
                        logger.warning(f"No API key found for provider {provider} - this may cause issues if the model requires external API access")
                        # Allow the request to proceed - the actual LLM call might fail, but that's better than failing here
                        return config
                except ImportError:
                    pass
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
    # Group-aware methods for multi-tenant support

    async def find_all_for_group(self, group_context: GroupContext) -> List[ModelConfig]:
        """
        Get all model configurations for a specific group.

        Shows:
        1. Default models (group_id = null) - visible to everyone
        2. Group-specific models - visible only to members of that group
        3. If a model has both default and group versions, the group version takes precedence

        Args:
            group_context: Group context with group IDs

        Returns:
            List of model configurations for the group
        """
        all_models = await self.repository.find_all()

        # If no group context, show only default models
        if not group_context or not group_context.group_ids:
            default_models = [
                model for model in all_models
                if model.group_id is None
            ]
            return default_models

        # Build a dictionary to handle overrides: model_key -> model
        models_by_key = {}

        # First, add all default models (group_id = null)
        for model in all_models:
            if model.group_id is None:
                models_by_key[model.key] = model

        # Then, override with group-specific models if they exist
        for model in all_models:
            if model.group_id in group_context.group_ids:
                # This will override the default if it exists
                models_by_key[model.key] = model

        # Convert back to list
        return list(models_by_key.values())

    async def find_enabled_models_for_group(self, group_context: GroupContext) -> List[ModelConfig]:
        """
        Get all enabled model configurations for a specific group.

        Applies group overrides first, then filters by enabled. This ensures that
        a workspace-level disable will properly hide a model even if the global
        default is enabled.

        Args:
            group_context: Group context with group IDs

        Returns:
            List of enabled model configurations effective for the group
        """
        # First compute the effective models for the group (with overrides applied)
        effective_models = await self.find_all_for_group(group_context)

        # Then filter to only enabled ones
        return [m for m in effective_models if getattr(m, "enabled", True)]

    async def find_all_global(self) -> List[ModelConfig]:
        """Return all global (system-wide) model configurations (group_id is None)."""
        return await self.repository.find_all_global()

    async def toggle_global_enabled(self, key: str, enabled: bool) -> Optional[ModelConfig]:
        """Toggle enabled on the global model by key (does not create group override)."""
        updated = await self.repository.toggle_global_enabled(key, enabled)
        if not updated:
            return None
        return await self.repository.find_global_by_key(key)


    async def toggle_model_enabled_with_group(self, key: str, enabled: bool, group_context: GroupContext) -> Optional[ModelConfig]:
        """
        Toggle the enabled status of a model with group verification.

        For default models (group_id = null):
        - Creates a group-specific copy with the toggled state
        - Ensures each group has their own enabled/disabled settings

        For group-specific models:
        - Only the owning group can toggle them

        Args:
            key: Key of the model to toggle
            enabled: New enabled status
            group_context: Group context with group IDs

        Returns:
            Updated model configuration, or None if not found

        Raises:
            HTTPException: If not authorized or toggle fails
        """
        try:
            # First get the model (could be default or group-specific)
            all_models = await self.repository.find_all()

            # Find the model (prefer group-specific over default)
            target_model = None
            default_model = None

            for model in all_models:
                if model.key == key:
                    if model.group_id is None:
                        default_model = model
                    elif group_context and model.group_id in group_context.group_ids:
                        target_model = model
                        break

            # If no group-specific model found, use default
            if not target_model:
                target_model = default_model

            if not target_model:
                logger.warning(f"Model with key {key} not found")
                return None

            # Must have a valid group context to toggle models
            if not group_context or not group_context.group_ids:
                logger.warning(f"No group context provided for toggling model {key}")
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Group context required to toggle models"
                )

            primary_group_id = group_context.primary_group_id

            # If it's a default model (group_id = null), create a group-specific copy
            if target_model.group_id is None:
                # Check if a group-specific version already exists
                existing_group_model = await self.repository.find_by_key_and_group(
                    key,
                    primary_group_id
                )

                if existing_group_model:
                    # Toggle the existing group-specific model in its group scope
                    await self.repository.toggle_enabled_in_group(existing_group_model.key, primary_group_id, enabled)
                    return await self.repository.find_by_key_and_group(key, primary_group_id)
                else:
                    # Create a new group-specific copy with toggled state
                    model_data = {
                        'key': target_model.key,
                        'name': target_model.name,
                        'provider': target_model.provider if hasattr(target_model, 'provider') else None,
                        'temperature': target_model.temperature if hasattr(target_model, 'temperature') else None,
                        'context_window': target_model.context_window if hasattr(target_model, 'context_window') else None,
                        'max_output_tokens': target_model.max_output_tokens if hasattr(target_model, 'max_output_tokens') else None,
                        'extended_thinking': target_model.extended_thinking if hasattr(target_model, 'extended_thinking') else False,
                        'enabled': enabled,  # Use the requested state
                        'group_id': primary_group_id,
                        'created_by_email': group_context.group_email
                    }
                    return await self.repository.create(model_data)

            # For group-specific models, check authorization
            if target_model.group_id not in group_context.group_ids:
                logger.warning(f"Model with key {key} not authorized for group")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,  # Return 404 not 403 to avoid information leakage
                    detail=f"Model with key {key} not found"
                )

            # Toggle the group-specific model in its group scope
            await self.repository.toggle_enabled_in_group(target_model.key, target_model.group_id, enabled)
            return await self.repository.find_by_key_and_group(key, target_model.group_id)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle model: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to toggle model: {str(e)}"
            )
