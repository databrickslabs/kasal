import logging
from typing import Annotated, Any, Dict, List

from fastapi import APIRouter, Depends, status

from src.core.exceptions import ForbiddenError, KasalError, NotFoundError

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.permissions import check_role_in_context, is_system_admin
from src.models.engine_config import EngineConfig
from src.schemas.engine_config import (
    CrewAIFlowConfigUpdate,
    EngineConfigCreate,
    EngineConfigListResponse,
    EngineConfigResponse,
    EngineConfigToggleUpdate,
    EngineConfigUpdate,
    EngineConfigValueUpdate,
    OtelAppTelemetryConfigUpdate,
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
def get_engine_config_service(session: SessionDep) -> EngineConfigService:
    """
    Dependency provider for EngineConfigService.

    Creates service with session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI (from core.dependencies)

    Returns:
        EngineConfigService instance with session
    """
    return EngineConfigService(session)


# Type alias for cleaner function signatures
EngineConfigServiceDep = Annotated[
    EngineConfigService, Depends(get_engine_config_service)
]


@router.get("", response_model=EngineConfigListResponse)
async def get_engine_configs(
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Get all engine configurations.

    Args:
        service: EngineConfig service injected by dependency

    Returns:
        List of engine configurations
    """
    logger.info("API call: GET /engine-config")

    configs = await service.find_all()
    logger.info(f"Found {len(configs)} engine configurations in database")

    return EngineConfigListResponse(configs=configs, count=len(configs))


@router.get("/enabled", response_model=EngineConfigListResponse)
async def get_enabled_engine_configs(
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Get only enabled engine configurations.

    Args:
        service: EngineConfig service injected by dependency

    Returns:
        List of enabled engine configurations
    """
    logger.info("API call: GET /engine-config/enabled")

    configs = await service.find_enabled_configs()
    logger.info(f"Found {len(configs)} enabled engine configurations in database")

    return EngineConfigListResponse(configs=configs, count=len(configs))


@router.get("/engine/{engine_name}", response_model=EngineConfigResponse)
async def get_engine_config(
    engine_name: str,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
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
    logger.info(f"API call: GET /engine-config/engine/{engine_name}")

    config = await service.find_by_engine_name(engine_name)
    if not config:
        logger.warning(f"Engine configuration with name {engine_name} not found")
        raise NotFoundError(f"Engine configuration with name {engine_name} not found")

    return config


@router.get(
    "/engine/{engine_name}/config/{config_key}", response_model=EngineConfigResponse
)
async def get_engine_config_by_key(
    engine_name: str,
    config_key: str,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
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
    logger.info(
        f"API call: GET /engine-config/engine/{engine_name}/config/{config_key}"
    )

    config = await service.find_by_engine_and_key(engine_name, config_key)
    if not config:
        logger.warning(f"Engine configuration {engine_name}.{config_key} not found")
        raise NotFoundError(f"Engine configuration {engine_name}.{config_key} not found")

    return config


@router.get("/type/{engine_type}", response_model=EngineConfigListResponse)
async def get_engine_configs_by_type(
    engine_type: str,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Get all engine configurations by engine type.

    Args:
        engine_type: Type of the engine
        service: EngineConfig service injected by dependency

    Returns:
        List of engine configurations
    """
    logger.info(f"API call: GET /engine-config/type/{engine_type}")

    configs = await service.find_by_engine_type(engine_type)
    logger.info(f"Found {len(configs)} engine configurations for type {engine_type}")

    return EngineConfigListResponse(configs=configs, count=len(configs))


@router.post(
    "", response_model=EngineConfigResponse, status_code=status.HTTP_201_CREATED
)
async def create_engine_config(
    config: EngineConfigCreate,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Create a new engine configuration.
    Only Admins can create engine configurations.

    Args:
        config: Engine configuration data
        service: EngineConfig service injected by dependency

    Returns:
        Created engine configuration

    Raises:
        HTTPException: If engine configuration with the same name already exists
    """
    # Check permissions - only admins can create engine configurations
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can create engine configurations")

    logger.info(
        f"API call: POST /engine-config - Creating engine config {config.engine_name}"
    )

    created_config = await service.create_engine_config(config)
    logger.info(f"Engine config {config.engine_name} created successfully")

    return created_config


@router.put("/engine/{engine_name}", response_model=EngineConfigResponse)
async def update_engine_config(
    engine_name: str,
    config: EngineConfigUpdate,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Update an existing engine configuration.
    Only Admins can update engine configurations.

    Args:
        engine_name: Name of the engine configuration to update
        config: Updated engine configuration data
        service: EngineConfig service injected by dependency

    Returns:
        Updated engine configuration

    Raises:
        HTTPException: If engine configuration not found
    """
    # Check permissions - only admins can update engine configurations
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can update engine configurations")

    logger.info(f"API call: PUT /engine-config/engine/{engine_name}")

    updated_config = await service.update_engine_config(engine_name, config)
    if not updated_config:
        logger.warning(
            f"Engine configuration with name {engine_name} not found for update"
        )
        raise NotFoundError(f"Engine configuration with name {engine_name} not found")

    logger.info(f"Engine config {engine_name} updated successfully")
    return updated_config


@router.patch("/engine/{engine_name}/toggle", response_model=EngineConfigResponse)
async def toggle_engine_config(
    engine_name: str,
    toggle_data: EngineConfigToggleUpdate,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Toggle the enabled status of an engine configuration.
    Only Admins can toggle engine configurations.

    Args:
        engine_name: Name of the engine configuration to toggle
        toggle_data: Toggle data containing new enabled status
        service: EngineConfig service injected by dependency

    Returns:
        Updated engine configuration

    Raises:
        HTTPException: If engine configuration not found
    """
    # Check permissions - only admins can toggle engine configurations
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can toggle engine configurations")

    logger.info(
        f"API call: PATCH /engine-config/engine/{engine_name}/toggle - enabled={toggle_data.enabled}"
    )

    updated_config = await service.toggle_engine_enabled(
        engine_name, toggle_data.enabled
    )
    if not updated_config:
        logger.warning(
            f"Engine configuration with name {engine_name} not found for toggle"
        )
        raise NotFoundError(f"Engine configuration with name {engine_name} not found")

    logger.info(f"Engine config {engine_name} toggled to enabled={toggle_data.enabled}")
    return updated_config


@router.patch(
    "/engine/{engine_name}/config/{config_key}/value",
    response_model=EngineConfigResponse,
)
async def update_config_value(
    engine_name: str,
    config_key: str,
    value_data: EngineConfigValueUpdate,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Update the configuration value for a specific engine and key.
    Only Admins can update engine configuration values.

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
    # Check permissions - only admins can update engine configuration values
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can update engine configuration values")

    logger.info(
        f"API call: PATCH /engine-config/engine/{engine_name}/config/{config_key}/value"
    )

    updated_config = await service.update_config_value(
        engine_name, config_key, value_data.config_value
    )
    if not updated_config:
        logger.warning(
            f"Engine configuration {engine_name}.{config_key} not found for value update"
        )
        raise NotFoundError(f"Engine configuration {engine_name}.{config_key} not found")

    logger.info(f"Engine config {engine_name}.{config_key} value updated successfully")
    return updated_config


@router.get("/crewai/flow-enabled")
async def get_crewai_flow_enabled(
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Get the CrewAI flow enabled status.
    Only system administrators can access engine configuration.

    Args:
        service: EngineConfig service injected by dependency

    Returns:
        Flow enabled status
    """
    # Check permissions - only system admins can access engine configuration
    if not is_system_admin(group_context):
        raise ForbiddenError("Only system administrators can access engine configuration")

    logger.info("API call: GET /engine-config/crewai/flow-enabled")

    enabled = await service.get_crewai_flow_enabled()
    logger.info(f"CrewAI flow enabled status: {enabled}")

    return {"flow_enabled": enabled}


@router.patch("/crewai/flow-enabled")
async def set_crewai_flow_enabled(
    config_data: CrewAIFlowConfigUpdate,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Set the CrewAI flow enabled status.
    Only system administrators can manage engine configuration.

    Args:
        config_data: Flow configuration data
        service: EngineConfig service injected by dependency

    Returns:
        Success status
    """
    # Check permissions - only system admins can manage engine configuration
    if not is_system_admin(group_context):
        raise ForbiddenError("Only system administrators can manage engine configuration")

    logger.info(
        f"API call: PATCH /engine-config/crewai/flow-enabled - enabled={config_data.flow_enabled}"
    )

    success = await service.set_crewai_flow_enabled(config_data.flow_enabled)
    if not success:
        raise KasalError("Failed to update CrewAI flow configuration")

    logger.info(f"CrewAI flow enabled status updated to: {config_data.flow_enabled}")
    return {"success": True, "flow_enabled": config_data.flow_enabled}


@router.get("/kasal/otel-app-telemetry")
async def get_otel_app_telemetry_enabled(
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """Get the OTel App Telemetry enabled status (system-level, Preview).

    Only system administrators can access this configuration.
    """
    if not is_system_admin(group_context):
        raise ForbiddenError("Only system administrators can access OTel App Telemetry configuration")

    enabled = await service.get_otel_app_telemetry_enabled()
    return {"otel_app_telemetry_enabled": enabled}


@router.patch("/kasal/otel-app-telemetry")
async def set_otel_app_telemetry_enabled(
    config_data: OtelAppTelemetryConfigUpdate,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """Set the OTel App Telemetry enabled status (system-level, Preview).

    Only system administrators can manage this configuration.
    When enabled, structured OpenTelemetry logs are exported to Unity Catalog
    tables via the Databricks App Telemetry sidecar.
    """
    if not is_system_admin(group_context):
        raise ForbiddenError("Only system administrators can manage OTel App Telemetry configuration")

    success = await service.set_otel_app_telemetry_enabled(config_data.enabled)
    if not success:
        raise KasalError("Failed to update OTel App Telemetry configuration")

    # Apply the change to the running logger
    from src.core.logger import LoggerManager
    logger_manager = LoggerManager.get_instance()
    logger_manager.enable_otel_app_telemetry(enabled=config_data.enabled)

    return {"success": True, "otel_app_telemetry_enabled": config_data.enabled}


@router.delete("/engine/{engine_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_engine_config(
    engine_name: str,
    service: EngineConfigServiceDep,
    group_context: GroupContextDep,
):
    """
    Delete an engine configuration.
    Only Admins can delete engine configurations.

    Args:
        engine_name: Name of the engine configuration to delete
        service: EngineConfig service injected by dependency

    Raises:
        HTTPException: If engine configuration not found
    """
    # Check permissions - only admins can delete engine configurations
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can delete engine configurations")

    logger.info(f"API call: DELETE /engine-config/engine/{engine_name}")

    deleted = await service.delete_engine_config(engine_name)
    if not deleted:
        logger.warning(
            f"Engine configuration with name {engine_name} not found for deletion"
        )
        raise NotFoundError(f"Engine configuration with name {engine_name} not found")

    logger.info(f"Engine config {engine_name} deleted successfully")
