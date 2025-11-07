from typing import Annotated, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status
import logging

from src.core.dependencies import SessionDep, GroupContextDep
from src.core.permissions import check_role_in_context
from src.models.model_config import ModelConfig
from src.schemas.model_config import (
    ModelConfigCreate,
    ModelConfigUpdate,
    ModelConfigResponse,
    ModelListResponse,
    ModelToggleUpdate
)
from src.services.model_config_service import ModelConfigService

router = APIRouter(
    prefix="/models",
    tags=["models"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

async def get_model_config_service(
    session: SessionDep,
    group_context: GroupContextDep
) -> ModelConfigService:
    """
    Dependency provider for ModelConfigService.

    Creates service with session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI
        group_context: Group context for multi-tenant isolation (REQUIRED for security)

    Returns:
        ModelConfigService instance with session and group_id

    Raises:
        ValueError: If group_context is None or has no primary_group_id
    """
    # SECURITY: group_id is REQUIRED for ModelConfigService
    if not group_context or not group_context.primary_group_id:
        raise ValueError(
            "SECURITY: group_id is REQUIRED for ModelConfigService. "
            "All API key operations must be scoped to a group for multi-tenant isolation."
        )
    return ModelConfigService(session, group_id=group_context.primary_group_id)


# Type alias for cleaner function signatures
ModelConfigServiceDep = Annotated[ModelConfigService, Depends(get_model_config_service)]


@router.get("", response_model=ModelListResponse)
async def get_models(
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Get all model configurations.

    Args:
        service: ModelConfig service injected by dependency

    Returns:
        List of model configurations
    """
    try:
        logger.info("API call: GET /models")

        models = await service.find_all_for_group(group_context)
        logger.info(f"Found {len(models)} models for group")

        # Log first few models for debugging
        for model in models[:3]:
            logger.debug(f"Model example: {model.key}, {model.name}, {model.provider}, enabled={model.enabled}")

        return ModelListResponse(models=models, count=len(models))
    except Exception as e:
        logger.error(f"Error getting models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enabled", response_model=ModelListResponse)
async def get_enabled_models(
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Get only enabled model configurations.

    Args:
        service: ModelConfig service injected by dependency

    Returns:
        List of enabled model configurations
    """
    try:
        logger.info("API call: GET /models/enabled")

        enabled_models = await service.find_enabled_models_for_group(group_context)
        logger.info(f"Found {len(enabled_models)} enabled models for group")

        return ModelListResponse(models=enabled_models, count=len(enabled_models))
    except Exception as e:
        logger.error(f"Error getting enabled models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/global", response_model=ModelListResponse)
async def get_global_models(
    service: ModelConfigServiceDep,
):
    """
    Get global (system-wide) model configurations (group_id is NULL).
    """
    try:
        logger.info("API call: GET /models/global")
        models = await service.find_all_global()
        return ModelListResponse(models=models, count=len(models))
    except Exception as e:
        logger.error(f"Error getting global models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/global/{model_key}/toggle", response_model=ModelConfigResponse)
async def toggle_global_model(
    model_key: str,
    toggle_data: ModelToggleUpdate,
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Toggle enabled on a global (system-wide) model configuration.
    Requires admin permissions.
    """
    # Check permissions - system admin or admin in any context
    is_allowed = False
    try:
        from src.core.permissions import get_effective_role
        role = get_effective_role(group_context) if group_context else None
        is_allowed = (role and role.lower() == "admin") or (
            hasattr(group_context, "current_user") and getattr(group_context.current_user, "is_system_admin", False)
        )
    except Exception:
        is_allowed = False

    if not is_allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can toggle global model configurations"
        )

    try:
        logger.info(f"API call: PATCH /models/global/{model_key}/toggle - enabled={toggle_data.enabled}")
        updated = await service.toggle_global_enabled(model_key, toggle_data.enabled)
        if not updated:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Model with key {model_key} not found")
        return updated
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error toggling global model {model_key}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/{model_key}", response_model=ModelConfigResponse)
async def get_model(
    model_key: str,
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Get a specific model configuration by key.

    Args:
        model_key: Key of the model configuration to get
        service: ModelConfig service injected by dependency

    Returns:
        Model configuration if found

    Raises:
        HTTPException: If model not found
    """
    try:
        logger.info(f"API call: GET /models/{model_key}")

        model = await service.find_by_key(model_key)
        if not model:
            logger.warning(f"Model with key {model_key} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model with key {model_key} not found"
            )

        return model
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting model {model_key}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=ModelConfigResponse, status_code=status.HTTP_201_CREATED)
async def create_model(
    model: ModelConfigCreate,
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Create a new model configuration.
    Only Admins can create model configurations.

    Args:
        model: Model configuration data
        service: ModelConfig service injected by dependency

    Returns:
        Created model configuration

    Raises:
        HTTPException: If model with the same key already exists
    """
    # Check permissions - only admins can create model configurations
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can create model configurations"
        )

    try:
        logger.info(f"API call: POST /models - Creating model {model.key}")

        created_model = await service.create_model_config(model)
        logger.info(f"Model {model.key} created successfully")

        return created_model
    except ValueError as ve:
        # Value error indicates model already exists
        logger.error(f"Model with key {model.key} already exists")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error creating model {model.key}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{model_key}", response_model=ModelConfigResponse)
async def update_model(
    model_key: str,
    model: ModelConfigUpdate,
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Update an existing model configuration.
    Only Admins can update model configurations.

    Args:
        model_key: Key of the model configuration to update
        model: Updated model configuration data
        service: ModelConfig service injected by dependency

    Returns:
        Updated model configuration

    Raises:
        HTTPException: If model not found
    """
    # Check permissions - only admins can update model configurations
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can update model configurations"
        )

    try:
        logger.info(f"API call: PUT /models/{model_key}")

        updated_model = await service.update_model_config(model_key, model)
        if not updated_model:
            logger.warning(f"Model with key {model_key} not found for update")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model with key {model_key} not found"
            )

        logger.info(f"Model {model_key} updated successfully")
        return updated_model
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating model {model_key}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{model_key}/toggle", response_model=ModelConfigResponse)
async def toggle_model(
    model_key: str,
    toggle_data: ModelToggleUpdate,
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Enable or disable a model configuration.
    Only Admins can toggle model configurations.

    Args:
        model_key: Key of the model configuration to toggle
        toggle_data: Toggle data with enabled flag
        service: ModelConfig service injected by dependency
        group_context: Group context for permissions

    Returns:
        Updated model configuration

    Raises:
        HTTPException: If model not found or user lacks permissions
    """
    # Check permissions - only admins can toggle model configurations
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can toggle model configurations"
        )

    try:
        logger.info(f"API call: PATCH /models/{model_key}/toggle - Setting enabled={toggle_data.enabled}")

        updated_model = await service.toggle_model_enabled_with_group(model_key, toggle_data.enabled, group_context)
        if not updated_model:
            logger.warning(f"Model with key {model_key} not found for toggle")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model with key {model_key} not found"
            )

        logger.info(f"Model {model_key} toggled to {toggle_data.enabled} successfully")
        return updated_model
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error toggling model {model_key}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{model_key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_model(
    model_key: str,
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Delete a model configuration.
    Only Admins can delete model configurations.

    Args:
        model_key: Key of the model configuration to delete
        service: ModelConfig service injected by dependency

    Raises:
        HTTPException: If model not found
    """
    # Check permissions - only admins can delete model configurations
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can delete model configurations"
        )

    try:
        logger.info(f"API call: DELETE /models/{model_key}")

        deleted = await service.delete_model_config(model_key)
        if not deleted:
            logger.warning(f"Model with key {model_key} not found for deletion")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model with key {model_key} not found"
            )

        logger.info(f"Model {model_key} deleted successfully")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting model {model_key}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/enable-all", response_model=ModelListResponse)
async def enable_all_models(
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Enable all model configurations.
    Only Admins can enable all model configurations.

    Args:
        service: ModelConfig service injected by dependency

    Returns:
        List of all model configurations after enabling
    """
    # Check permissions - only admins can enable all models
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can enable all model configurations"
        )

    try:
        logger.info("API call: POST /models/enable-all")

        models = await service.enable_all_models()
        logger.info(f"All {len(models)} models enabled successfully")

        return ModelListResponse(models=models, count=len(models))
    except Exception as e:
        logger.error(f"Error enabling all models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/disable-all", response_model=ModelListResponse)
async def disable_all_models(
    service: ModelConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Disable all model configurations.
    Only Admins can disable all model configurations.

    Args:
        service: ModelConfig service injected by dependency

    Returns:
        List of all model configurations after disabling
    """
    # Check permissions - only admins can disable all models
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can disable all model configurations"
        )

    try:
        logger.info("API call: POST /models/disable-all")

        models = await service.disable_all_models()
        logger.info(f"All {len(models)} models disabled successfully")

        return ModelListResponse(models=models, count=len(models))
    except Exception as e:
        logger.error(f"Error disabling all models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))