from typing import Annotated, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status
import logging

from src.core.dependencies import SessionDep
from src.core.unit_of_work import UnitOfWork
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

# Dependency to get ModelConfigService
async def get_model_config_service() -> ModelConfigService:
    async with UnitOfWork() as uow:
        return await ModelConfigService.from_unit_of_work(uow)


@router.get("", response_model=ModelListResponse)
async def get_models(
    service: Annotated[ModelConfigService, Depends(get_model_config_service)],
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
        
        models = await service.find_all()
        logger.info(f"Found {len(models)} models in database")
        
        # Log first few models for debugging
        for model in models[:3]:
            logger.debug(f"Model example: {model.key}, {model.name}, {model.provider}, enabled={model.enabled}")
            
        return ModelListResponse(models=models, count=len(models))
    except Exception as e:
        logger.error(f"Error getting models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enabled", response_model=ModelListResponse)
async def get_enabled_models(
    service: Annotated[ModelConfigService, Depends(get_model_config_service)],
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
        
        models = await service.find_enabled_models()
        logger.info(f"Found {len(models)} enabled models in database")
        
        return ModelListResponse(models=models, count=len(models))
    except Exception as e:
        logger.error(f"Error getting enabled models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{model_key}", response_model=ModelConfigResponse)
async def get_model(
    model_key: str,
    service: Annotated[ModelConfigService, Depends(get_model_config_service)],
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
    service: Annotated[ModelConfigService, Depends(get_model_config_service)],
):
    """
    Create a new model configuration.
    
    Args:
        model: Model configuration data
        service: ModelConfig service injected by dependency
        
    Returns:
        Created model configuration
        
    Raises:
        HTTPException: If model with the same key already exists
    """
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
    service: Annotated[ModelConfigService, Depends(get_model_config_service)],
):
    """
    Update an existing model configuration.
    
    Args:
        model_key: Key of the model configuration to update
        model: Updated model configuration data
        service: ModelConfig service injected by dependency
        
    Returns:
        Updated model configuration
        
    Raises:
        HTTPException: If model not found
    """
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
    service: Annotated[ModelConfigService, Depends(get_model_config_service)],
):
    """
    Enable or disable a model configuration.
    
    Args:
        model_key: Key of the model configuration to toggle
        toggle_data: Toggle data with enabled flag
        service: ModelConfig service injected by dependency
        
    Returns:
        Updated model configuration
        
    Raises:
        HTTPException: If model not found
    """
    try:
        logger.info(f"API call: PATCH /models/{model_key}/toggle - Setting enabled={toggle_data.enabled}")
        
        updated_model = await service.toggle_model_enabled(model_key, toggle_data.enabled)
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
    service: Annotated[ModelConfigService, Depends(get_model_config_service)],
):
    """
    Delete a model configuration.
    
    Args:
        model_key: Key of the model configuration to delete
        service: ModelConfig service injected by dependency
        
    Raises:
        HTTPException: If model not found
    """
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
    service: Annotated[ModelConfigService, Depends(get_model_config_service)],
):
    """
    Enable all model configurations.
    
    Args:
        service: ModelConfig service injected by dependency
        
    Returns:
        List of all model configurations after enabling
    """
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
    service: Annotated[ModelConfigService, Depends(get_model_config_service)],
):
    """
    Disable all model configurations.
    
    Args:
        service: ModelConfig service injected by dependency
        
    Returns:
        List of all model configurations after disabling
    """
    try:
        logger.info("API call: POST /models/disable-all")
        
        models = await service.disable_all_models()
        logger.info(f"All {len(models)} models disabled successfully")
        
        return ModelListResponse(models=models, count=len(models))
    except Exception as e:
        logger.error(f"Error disabling all models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 