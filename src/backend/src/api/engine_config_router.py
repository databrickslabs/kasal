from typing import Annotated, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status
import logging

from src.core.dependencies import SessionDep
from src.core.unit_of_work import UnitOfWork
from src.models.engine_config import EngineConfig
from src.schemas.engine_config import (
    EngineConfigCreate,
    EngineConfigUpdate,
    EngineConfigResponse,
    EngineConfigListResponse,
    EngineConfigToggleUpdate,
    EngineConfigValueUpdate,
    CrewAIFlowConfigUpdate
)
from src.services.engine_config_service import EngineConfigService

router = APIRouter(
    prefix="/engine-config",
    tags=["engine-config"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

# Dependency to get EngineConfigService
async def get_engine_config_service() -> EngineConfigService:
    async with UnitOfWork() as uow:
        return await EngineConfigService.from_unit_of_work(uow)


@router.get("", response_model=EngineConfigListResponse)
async def get_engine_configs(
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Get all engine configurations.
    
    Args:
        service: EngineConfig service injected by dependency
        
    Returns:
        List of engine configurations
    """
    try:
        logger.info("API call: GET /engine-config")
        
        configs = await service.find_all()
        logger.info(f"Found {len(configs)} engine configurations in database")
        
        return EngineConfigListResponse(configs=configs, count=len(configs))
    except Exception as e:
        logger.error(f"Error getting engine configurations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enabled", response_model=EngineConfigListResponse)
async def get_enabled_engine_configs(
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Get only enabled engine configurations.
    
    Args:
        service: EngineConfig service injected by dependency
        
    Returns:
        List of enabled engine configurations
    """
    try:
        logger.info("API call: GET /engine-config/enabled")
        
        configs = await service.find_enabled_configs()
        logger.info(f"Found {len(configs)} enabled engine configurations in database")
        
        return EngineConfigListResponse(configs=configs, count=len(configs))
    except Exception as e:
        logger.error(f"Error getting enabled engine configurations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/engine/{engine_name}", response_model=EngineConfigResponse)
async def get_engine_config(
    engine_name: str,
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Get a specific engine configuration by engine name.
    
    Args:
        engine_name: Name of the engine configuration to get
        service: EngineConfig service injected by dependency
        
    Returns:
        Engine configuration if found
        
    Raises:
        HTTPException: If engine configuration not found
    """
    try:
        logger.info(f"API call: GET /engine-config/engine/{engine_name}")
        
        config = await service.find_by_engine_name(engine_name)
        if not config:
            logger.warning(f"Engine configuration with name {engine_name} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Engine configuration with name {engine_name} not found"
            )
            
        return config
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting engine configuration {engine_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/engine/{engine_name}/config/{config_key}", response_model=EngineConfigResponse)
async def get_engine_config_by_key(
    engine_name: str,
    config_key: str,
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Get a specific engine configuration by engine name and config key.
    
    Args:
        engine_name: Name of the engine
        config_key: Configuration key
        service: EngineConfig service injected by dependency
        
    Returns:
        Engine configuration if found
        
    Raises:
        HTTPException: If engine configuration not found
    """
    try:
        logger.info(f"API call: GET /engine-config/engine/{engine_name}/config/{config_key}")
        
        config = await service.find_by_engine_and_key(engine_name, config_key)
        if not config:
            logger.warning(f"Engine configuration {engine_name}.{config_key} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Engine configuration {engine_name}.{config_key} not found"
            )
            
        return config
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting engine configuration {engine_name}.{config_key}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/type/{engine_type}", response_model=EngineConfigListResponse)
async def get_engine_configs_by_type(
    engine_type: str,
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Get all engine configurations by engine type.
    
    Args:
        engine_type: Type of the engine
        service: EngineConfig service injected by dependency
        
    Returns:
        List of engine configurations
    """
    try:
        logger.info(f"API call: GET /engine-config/type/{engine_type}")
        
        configs = await service.find_by_engine_type(engine_type)
        logger.info(f"Found {len(configs)} engine configurations for type {engine_type}")
        
        return EngineConfigListResponse(configs=configs, count=len(configs))
    except Exception as e:
        logger.error(f"Error getting engine configurations by type {engine_type}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=EngineConfigResponse, status_code=status.HTTP_201_CREATED)
async def create_engine_config(
    config: EngineConfigCreate,
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Create a new engine configuration.
    
    Args:
        config: Engine configuration data
        service: EngineConfig service injected by dependency
        
    Returns:
        Created engine configuration
        
    Raises:
        HTTPException: If engine configuration with the same name already exists
    """
    try:
        logger.info(f"API call: POST /engine-config - Creating engine config {config.engine_name}")
        
        created_config = await service.create_engine_config(config)
        logger.info(f"Engine config {config.engine_name} created successfully")
        
        return created_config
    except ValueError as ve:
        # Value error indicates engine config already exists
        logger.error(f"Engine config with name {config.engine_name} and key {config.config_key} already exists")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error creating engine config {config.engine_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/engine/{engine_name}", response_model=EngineConfigResponse)
async def update_engine_config(
    engine_name: str,
    config: EngineConfigUpdate,
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Update an existing engine configuration.
    
    Args:
        engine_name: Name of the engine configuration to update
        config: Updated engine configuration data
        service: EngineConfig service injected by dependency
        
    Returns:
        Updated engine configuration
        
    Raises:
        HTTPException: If engine configuration not found
    """
    try:
        logger.info(f"API call: PUT /engine-config/engine/{engine_name}")
        
        updated_config = await service.update_engine_config(engine_name, config)
        if not updated_config:
            logger.warning(f"Engine configuration with name {engine_name} not found for update")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Engine configuration with name {engine_name} not found"
            )
            
        logger.info(f"Engine config {engine_name} updated successfully")
        return updated_config
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating engine config {engine_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/engine/{engine_name}/toggle", response_model=EngineConfigResponse)
async def toggle_engine_config(
    engine_name: str,
    toggle_data: EngineConfigToggleUpdate,
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Toggle the enabled status of an engine configuration.
    
    Args:
        engine_name: Name of the engine configuration to toggle
        toggle_data: Toggle data containing new enabled status
        service: EngineConfig service injected by dependency
        
    Returns:
        Updated engine configuration
        
    Raises:
        HTTPException: If engine configuration not found
    """
    try:
        logger.info(f"API call: PATCH /engine-config/engine/{engine_name}/toggle - enabled={toggle_data.enabled}")
        
        updated_config = await service.toggle_engine_enabled(engine_name, toggle_data.enabled)
        if not updated_config:
            logger.warning(f"Engine configuration with name {engine_name} not found for toggle")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Engine configuration with name {engine_name} not found"
            )
            
        logger.info(f"Engine config {engine_name} toggled to enabled={toggle_data.enabled}")
        return updated_config
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error toggling engine config {engine_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/engine/{engine_name}/config/{config_key}/value", response_model=EngineConfigResponse)
async def update_config_value(
    engine_name: str,
    config_key: str,
    value_data: EngineConfigValueUpdate,
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Update the configuration value for a specific engine and key.
    
    Args:
        engine_name: Name of the engine
        config_key: Configuration key
        value_data: New configuration value
        service: EngineConfig service injected by dependency
        
    Returns:
        Updated engine configuration
        
    Raises:
        HTTPException: If engine configuration not found
    """
    try:
        logger.info(f"API call: PATCH /engine-config/engine/{engine_name}/config/{config_key}/value")
        
        updated_config = await service.update_config_value(engine_name, config_key, value_data.config_value)
        if not updated_config:
            logger.warning(f"Engine configuration {engine_name}.{config_key} not found for value update")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Engine configuration {engine_name}.{config_key} not found"
            )
            
        logger.info(f"Engine config {engine_name}.{config_key} value updated successfully")
        return updated_config
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating config value {engine_name}.{config_key}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/crewai/flow-enabled")
async def get_crewai_flow_enabled(
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Get the CrewAI flow enabled status.
    
    Args:
        service: EngineConfig service injected by dependency
        
    Returns:
        Flow enabled status
    """
    try:
        logger.info("API call: GET /engine-config/crewai/flow-enabled")
        
        enabled = await service.get_crewai_flow_enabled()
        logger.info(f"CrewAI flow enabled status: {enabled}")
        
        return {"flow_enabled": enabled}
    except Exception as e:
        logger.error(f"Error getting CrewAI flow enabled status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/crewai/flow-enabled")
async def set_crewai_flow_enabled(
    config_data: CrewAIFlowConfigUpdate,
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Set the CrewAI flow enabled status.
    
    Args:
        config_data: Flow configuration data
        service: EngineConfig service injected by dependency
        
    Returns:
        Success status
    """
    try:
        logger.info(f"API call: PATCH /engine-config/crewai/flow-enabled - enabled={config_data.flow_enabled}")
        
        success = await service.set_crewai_flow_enabled(config_data.flow_enabled)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update CrewAI flow configuration"
            )
            
        logger.info(f"CrewAI flow enabled status updated to: {config_data.flow_enabled}")
        return {"success": True, "flow_enabled": config_data.flow_enabled}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error setting CrewAI flow enabled status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/engine/{engine_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_engine_config(
    engine_name: str,
    service: Annotated[EngineConfigService, Depends(get_engine_config_service)],
):
    """
    Delete an engine configuration.
    
    Args:
        engine_name: Name of the engine configuration to delete
        service: EngineConfig service injected by dependency
        
    Raises:
        HTTPException: If engine configuration not found
    """
    try:
        logger.info(f"API call: DELETE /engine-config/engine/{engine_name}")
        
        deleted = await service.delete_engine_config(engine_name)
        if not deleted:
            logger.warning(f"Engine configuration with name {engine_name} not found for deletion")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Engine configuration with name {engine_name} not found"
            )
            
        logger.info(f"Engine config {engine_name} deleted successfully")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting engine config {engine_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 