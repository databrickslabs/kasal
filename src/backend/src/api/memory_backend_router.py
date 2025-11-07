"""
Memory backend configuration API endpoints.

This module provides API endpoints for managing memory backend configurations,
including validation, testing connections, and retrieving available indexes.
"""
import os
from typing import Dict, List, Any, Optional, Annotated
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.permissions import check_role_in_context, is_workspace_admin
from src.core.logger import LoggerManager
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    DatabricksMemoryConfig,
    MemoryBackendCreate,
    MemoryBackendUpdate,
    MemoryBackendResponse,
    MemoryBackendType
)
from src.services.memory_backend_service import MemoryBackendService
from src.models.memory_backend import MemoryBackend
from src.utils.databricks_auth import extract_user_token_from_request

logger = LoggerManager.get_instance().api

router = APIRouter(prefix="/memory-backend", tags=["memory-backend"])

# Dependency to get MemoryBackendService with injected session
def get_memory_backend_service(session: SessionDep) -> MemoryBackendService:
    """Get MemoryBackendService instance with injected session."""
    return MemoryBackendService(session)


@router.get("/databricks/workspace-url")
async def get_workspace_url(
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    group_context: GroupContextDep,
) -> Dict[str, Any]:
    """
    Get the Databricks workspace URL from environment or configuration.
    
    Returns:
        Dict with workspace URL if available, or None
    """
    result = await service.get_workspace_url()
    return result


@router.post("/validate")
async def validate_memory_config(
    config: MemoryBackendConfig,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    group_context: GroupContextDep,
) -> Dict[str, Any]:
    """
    Validate memory backend configuration.
    
    Args:
        config: Memory backend configuration to validate
        group_context: Current group context
        
    Returns:
        Validation result with any errors
    """
    try:
        errors = []
        
        # Basic validation
        if config.backend_type == "databricks":
            if not config.databricks_config:
                errors.append("Databricks configuration is required for Databricks backend")
            else:
                if not config.databricks_config.endpoint_name:
                    errors.append("Endpoint name is required")
                if not config.databricks_config.short_term_index:
                    errors.append("Short-term memory index is required")
                if config.databricks_config.embedding_dimension < 1:
                    errors.append("Embedding dimension must be positive")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
        
    except Exception as e:
        logger.error(f"Error validating memory config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/databricks/test-connection")
async def test_databricks_connection(
    config: DatabricksMemoryConfig,
    request: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Test connection to Databricks Vector Search.
    
    Args:
        config: Databricks configuration
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Connection test result
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)
        
        # Service is injected via dependency
        result = await service.test_databricks_connection(config, user_token)
        return result
        
    except Exception as e:
        logger.error(f"Error testing Databricks connection: {e}")
        return {
            "success": False,
            "message": f"Connection test failed: {str(e)}",
            "details": {
                "error": str(e)
            }
        }


@router.post("/databricks/indexes")
async def get_databricks_indexes(
    config: DatabricksMemoryConfig,
    request: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Get available indexes for a Databricks endpoint.
    
    Args:
        config: Databricks configuration
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        List of available indexes
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)
        
        # Get indexes from the service
        result = await service.get_databricks_indexes(config, user_token)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching Databricks indexes: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/databricks/create-index")
async def create_databricks_index(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Create a new Databricks Vector Search index.
    Only workspace admins can create Databricks indexes.

    Args:
        request: Request containing index creation parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Index creation result
    """
    # Check permissions - only workspace admins can create indexes
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins can create Databricks indexes"
        )

    try:
        # Extract parameters
        try:
            config = DatabricksMemoryConfig(**request.get("config", {}))
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid Databricks configuration: {str(e)}"
            )
        
        index_type = request.get("index_type")
        catalog = request.get("catalog")
        schema = request.get("schema")
        table_name = request.get("table_name")
        primary_key = request.get("primary_key", "id")
        
        # Validate required parameters
        if not all([index_type, catalog, schema, table_name]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="index_type, catalog, schema, and table_name are required"
            )
        
        if index_type not in ["short_term", "long_term", "entity", "document"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="index_type must be one of: short_term, long_term, entity, document"
            )
        
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(req)
        
        # Create the index
        result = await service.create_databricks_index(
            config=config,
            index_type=index_type,
            catalog=catalog,
            schema=schema,
            table_name=table_name,
            primary_key=primary_key,
            user_token=user_token
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating Databricks index: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/configs", response_model=MemoryBackendResponse)
async def create_memory_config(
    config: MemoryBackendCreate,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> MemoryBackendResponse:
    """
    Create a new memory backend configuration.
    Only workspace admins can create memory configurations for their workspace.

    Args:
        config: Memory backend configuration
        group_context: Current group context
        service: Memory backend service

    Returns:
        Created memory backend configuration
    """
    # Check permissions - only workspace admins can create memory configs
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins can create memory configurations"
        )

    try:
        # Service is injected via dependency
        backend = await service.create_memory_backend(group_context.primary_group_id, config)
        return MemoryBackendResponse.model_validate(backend)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating memory config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/configs", response_model=List[MemoryBackendResponse])
async def get_memory_configs(
    request: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> List[MemoryBackendResponse]:
    """
    Get all memory backend configurations for the current user.
    
    Args:
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        List of memory backend configurations
    """
    try:
        # Only log group context at debug level for frequently called endpoint
        logger.debug(f"Getting memory backends for group: {group_context.primary_group_id}")
        backends = await service.get_memory_backends(group_context.primary_group_id)
        logger.debug(f"Found {len(backends)} backends for group")
        
        return [MemoryBackendResponse.model_validate(backend) for backend in backends]
        
    except Exception as e:
        logger.error(f"Error fetching memory configs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/configs/{backend_id}", response_model=MemoryBackendResponse)
async def get_memory_config_by_id(
    backend_id: str,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> MemoryBackendResponse:
    """
    Get a specific memory backend configuration.
    
    Args:
        backend_id: Backend ID
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Memory backend configuration
    """
    try:
        # Service is injected via dependency
        backend = await service.get_memory_backend(group_context.primary_group_id, backend_id)
        
        if not backend:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Memory backend configuration not found"
            )
        
        return MemoryBackendResponse.model_validate(backend)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching memory config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/configs/default", response_model=Optional[MemoryBackendResponse])
async def get_default_memory_config(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Optional[MemoryBackendResponse]:
    """
    Get the default memory backend configuration for the current user.
    
    Args:
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Default memory backend configuration or None
    """
    try:
        # Service is injected via dependency
        logger.debug(f"Getting default memory backend for group: {group_context.primary_group_id}")
        backend = await service.get_default_memory_backend(group_context.primary_group_id)

        if backend:
            logger.debug(f"Found default backend: {backend.name}")
            return MemoryBackendResponse.model_validate(backend)
        else:
            logger.debug(f"No default backend found for group: {group_context.primary_group_id}")
            return None
        
    except Exception as e:
        logger.error(f"Error fetching default memory config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.put("/configs/{backend_id}", response_model=MemoryBackendResponse)
async def update_memory_config(
    backend_id: str,
    update_data: MemoryBackendUpdate,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> MemoryBackendResponse:
    """
    Update a memory backend configuration.
    Only workspace admins can update memory configurations for their workspace.

    Args:
        backend_id: Backend ID
        update_data: Update data
        group_context: Current group context
        service: Memory backend service

    Returns:
        Updated memory backend configuration
    """
    # Check permissions - only workspace admins can update memory configs
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins can update memory configurations"
        )

    try:
        # Service is injected via dependency
        backend = await service.update_memory_backend(group_context.primary_group_id, backend_id, update_data)
        
        if not backend:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Memory backend configuration not found"
            )
        
        return MemoryBackendResponse.model_validate(backend)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating memory config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/configs/{backend_id}")
async def delete_memory_config(
    backend_id: str,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete a memory backend configuration.
    Only workspace admins can delete memory configurations for their workspace.

    Args:
        backend_id: Backend ID
        group_context: Current group context
        service: Memory backend service

    Returns:
        Success status
    """
    # Check permissions - only workspace admins can delete memory configs
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins can delete memory configurations"
        )

    try:
        # Service is injected via dependency
        success = await service.delete_memory_backend(group_context.primary_group_id, backend_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Memory backend configuration not found"
            )
        
        return {"success": True, "message": "Memory backend configuration deleted"}
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting memory config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/configs/{backend_id}/set-default")
async def set_default_memory_config(
    backend_id: str,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Set a memory backend configuration as default.
    
    Args:
        backend_id: Backend ID
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Success status
    """
    try:
        # Service is injected via dependency
        success = await service.set_default_backend(group_context.primary_group_id, backend_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Memory backend configuration not found"
            )
        
        return {"success": True, "message": "Default memory backend configuration set"}
        
    except Exception as e:
        logger.error(f"Error setting default memory config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/stats/{crew_id}")
async def get_memory_stats(
    crew_id: str,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Get memory usage statistics for a crew.
    
    Args:
        crew_id: Crew identifier
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Memory usage statistics
    """
    try:
        # Service is injected via dependency
        stats = await service.get_memory_stats(group_context.primary_group_id, crew_id)
        return stats
        
    except Exception as e:
        logger.error(f"Error fetching memory stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/databricks/one-click-setup")
async def one_click_databricks_setup(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    One-click setup for Databricks Vector Search.
    Creates all endpoints and indexes automatically.
    Only workspace admins can set up memory backend for their workspace.

    Args:
        request: Request containing workspace_url, catalog, and schema
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Setup result with created resources
    """
    # Check permissions - only workspace admins can set up memory backend
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins can set up memory backend"
        )

    try:
        # CRITICAL: Set UserContext for authentication system to access group_id
        # The authentication system needs group_id to look up PAT tokens from database
        from src.utils.user_context import UserContext
        UserContext.set_group_context(group_context)
        logger.info(f"[ONE-CLICK-SETUP] Set UserContext with group_id: {group_context.primary_group_id}")

        # Get workspace URL from unified auth or user request
        workspace_url = request.get("workspace_url")

        # Try to get from unified auth first
        if not workspace_url:
            try:
                from src.utils.databricks_auth import get_auth_context
                auth = await get_auth_context()
                if auth and auth.workspace_url:
                    workspace_url = auth.workspace_url
                    logger.info(f"Using workspace URL from unified {auth.auth_method} auth: {workspace_url}")
            except Exception as e:
                logger.warning(f"Failed to get unified auth: {e}")

        if not workspace_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="workspace_url is required and not available from unified auth"
            )

        catalog = request.get("catalog", "ml")
        schema = request.get("schema", "agents")
        embedding_dimension = request.get("embedding_dimension", 768)  # Default to 768 if not provided

        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(req)

        # Run one-click setup with user_id from group context
        logger.info(f"Starting one-click setup for group: {group_context.primary_group_id}")
        logger.info(f"Workspace URL: {workspace_url}, Catalog: {catalog}, Schema: {schema}, Embedding dimension: {embedding_dimension}")

        result = await service.one_click_databricks_setup(
            workspace_url=workspace_url,
            catalog=catalog,
            schema=schema,
            embedding_dimension=embedding_dimension,
            user_token=user_token,
            group_id=group_context.primary_group_id  # Pass group_id from group context
        )

        logger.info(f"One-click setup result: {result}")

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in one-click setup: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/clear/{crew_id}")
async def clear_crew_memory(
    crew_id: str,
    request: Dict[str, List[str]],
    group_context: GroupContextDep,
) -> Dict[str, Any]:
    """
    Clear memory for a specific crew.
    
    Args:
        crew_id: Crew identifier
        request: Memory types to clear
        group_context: Current group context
        
    Returns:
        Success status
    """
    try:
        memory_types = request.get("memory_types", [])
        if not memory_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No memory types specified"
            )
        
        # In a real implementation, clear the actual memory
        logger.info(f"Clearing {memory_types} memory for crew {crew_id}")
        
        return {
            "success": True,
            "message": f"Cleared {', '.join(memory_types)} memory for crew {crew_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error clearing memory: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/databricks/verify-resources")
async def verify_databricks_resources(
    workspace_url: str,
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    backend_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Verify which Databricks resources actually exist.
    
    Args:
        workspace_url: Databricks workspace URL
        group_context: Current group context
        service: Memory backend service
        backend_id: Optional backend ID to verify specific configuration
        
    Returns:
        Dict with existing resources information
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(req)
        logger.info(f"Extracted user token from request: {bool(user_token)}")
        
        # Get the configuration to check
        config = None
        if backend_id:
            # Get specific backend configuration
            config = await service.get_memory_backend(group_context.primary_group_id, backend_id)
            logger.info(f"Using specific backend config: {backend_id}")
        else:
            # Get default configuration
            config = await service.get_default_memory_backend(group_context.primary_group_id)
            logger.info(f"Using default backend config")
        
        # Use the service to verify resources
        result = await service.verify_databricks_resources(workspace_url, user_token, config)
        
        return result
        
    except Exception as e:
        logger.error(f"Error verifying Databricks resources: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/databricks/endpoint-status")
async def get_databricks_endpoint_status(
    workspace_url: str,
    endpoint_name: str,
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Get the status of a Databricks Vector Search endpoint.
    
    Args:
        workspace_url: Databricks workspace URL
        endpoint_name: Name of the endpoint to check
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Dict with endpoint status information
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(req)
        
        # Use the service to get endpoint status
        result = await service.get_databricks_endpoint_status(
            workspace_url=workspace_url,
            endpoint_name=endpoint_name,
            user_token=user_token
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting endpoint status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/databricks/index")
async def delete_databricks_index(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete a Databricks Vector Search index.
    Only workspace admins can delete Databricks indexes.

    Args:
        request: Request containing deletion parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Deletion result
    """
    # Check permissions - only workspace admins can delete indexes
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins can delete Databricks indexes"
        )

    try:
        # Extract parameters
        workspace_url = request.get("workspace_url")
        index_name = request.get("index_name")
        endpoint_name = request.get("endpoint_name")
        
        # Validate required parameters
        if not all([workspace_url, index_name, endpoint_name]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="workspace_url, index_name, and endpoint_name are required"
            )
        
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(req)
        
        # Delete the index
        result = await service.delete_databricks_index(
            workspace_url=workspace_url,
            index_name=index_name,
            endpoint_name=endpoint_name,
            user_token=user_token
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting Databricks index: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/databricks/endpoint")
async def delete_databricks_endpoint(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete a Databricks Vector Search endpoint.
    Only workspace admins can delete Databricks endpoints.

    Args:
        request: Request containing deletion parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Deletion result
    """
    # Check permissions - only workspace admins can delete endpoints
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins can delete Databricks endpoints"
        )

    try:
        # Extract parameters
        workspace_url = request.get("workspace_url")
        endpoint_name = request.get("endpoint_name")
        
        # Validate required parameters
        if not all([workspace_url, endpoint_name]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="workspace_url and endpoint_name are required"
            )
        
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(req)
        
        # Delete the endpoint
        result = await service.delete_databricks_endpoint(
            workspace_url=workspace_url,
            endpoint_name=endpoint_name,
            user_token=user_token
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting Databricks endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/configs/databricks/all")
async def delete_all_databricks_configs(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete all Databricks memory backend configurations for the current group.
    This is used when switching to disabled mode to ensure clean state.
    
    Args:
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Success status with count of deleted configurations
    """
    try:
        # Get all memory backends for the group
        backends = await service.get_memory_backends(group_context.primary_group_id)
        
        deleted_count = 0
        for backend in backends:
            # Only delete Databricks backends
            if backend.backend_type == MemoryBackendType.DATABRICKS:
                success = await service.delete_memory_backend(group_context.primary_group_id, backend.id)
                if success:
                    deleted_count += 1
                    logger.info(f"Deleted Databricks backend: {backend.id}")
        
        return {
            "success": True, 
            "message": f"Deleted {deleted_count} Databricks configurations",
            "deleted_count": deleted_count
        }
        
    except Exception as e:
        logger.error(f"Error deleting all Databricks configs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/configs/switch-to-disabled")
async def switch_to_disabled_mode(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Switch to disabled mode by deleting all memory backend configurations
    and creating a new disabled configuration.
    Only workspace admins can switch to disabled mode.

    Args:
        group_context: Current group context
        service: Memory backend service

    Returns:
        Success status with deleted count and new disabled configuration
    """
    # Check permissions - only workspace admins can switch to disabled mode
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins can switch memory backend to disabled mode"
        )

    try:
        # Delete all configurations and create disabled one
        result = await service.delete_all_and_create_disabled(group_context.primary_group_id)
        
        if not result["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result["message"]
            )
        
        logger.info(f"Switched to disabled mode for group {group_context.primary_group_id}: {result['message']}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error switching to disabled mode: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/configs/disabled/cleanup")
async def cleanup_disabled_configs(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete all disabled (DEFAULT type) memory backend configurations.
    This is used when switching from disabled to enabled mode.
    
    Args:
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Success status with count of deleted configurations
    """
    try:
        # Delete all disabled configurations
        deleted_count = await service.delete_disabled_configurations(group_context.primary_group_id)
        
        logger.info(f"Cleaned up {deleted_count} disabled configurations for group {group_context.primary_group_id}")
        
        return {
            "success": True,
            "deleted_count": deleted_count,
            "message": f"Deleted {deleted_count} disabled configurations"
        }
        
    except Exception as e:
        logger.error(f"Error cleaning up disabled configs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/databricks/index-info")
async def get_index_info(
    workspace_url: str,
    index_name: str,
    endpoint_name: str,
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Get information about a Databricks Vector Search index including document count.
    
    Args:
        workspace_url: Databricks workspace URL
        index_name: Full index name (catalog.schema.table)
        endpoint_name: Endpoint name that hosts the index
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Index information including document count
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(req)
        
        # Get index info
        result = await service.get_index_info(
            workspace_url=workspace_url,
            index_name=index_name,
            endpoint_name=endpoint_name,
            user_token=user_token
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting index info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/databricks/empty-index")
async def empty_index(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Empty a Databricks Vector Search index by deleting and recreating it.
    Only workspace admins can empty Databricks indexes.

    Args:
        request: Request containing index parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Operation result
    """
    # Check permissions - only workspace admins can empty indexes
    if not is_workspace_admin(group_context):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only workspace admins can empty Databricks indexes"
        )

    try:
        # Extract parameters
        workspace_url = request.get("workspace_url")
        index_name = request.get("index_name")
        endpoint_name = request.get("endpoint_name")
        index_type = request.get("index_type")
        embedding_dimension = request.get("embedding_dimension", 768)
        
        # Validate required parameters
        if not all([workspace_url, index_name, endpoint_name, index_type]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="workspace_url, index_name, endpoint_name, and index_type are required"
            )
        
        if index_type not in ["short_term", "long_term", "entity", "document"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="index_type must be one of: short_term, long_term, entity, document"
            )
        
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(req)
        
        # Empty the index
        result = await service.empty_index(
            workspace_url=workspace_url,
            index_name=index_name,
            endpoint_name=endpoint_name,
            index_type=index_type,
            embedding_dimension=embedding_dimension,
            user_token=user_token
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error emptying index: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/databricks/index-documents")
async def get_index_documents(
    index_name: str = Query(..., description="Full name of the index (catalog.schema.index)"),
    workspace_url: str = Query(..., description="Databricks workspace URL"),
    endpoint_name: str = Query(..., description="Vector Search endpoint name"),
    index_type: Optional[str] = Query(None, description="Type of index (short_term, long_term, entity, document)"),
    backend_id: Optional[str] = Query(None, description="Backend configuration ID"),
    limit: int = Query(30, description="Maximum number of documents to return"),
    request: Request = None,
    group_context: GroupContextDep = None,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)] = None,
) -> Dict[str, Any]:
    """
    Retrieve documents from any Databricks Vector Search index.
    
    This endpoint fetches the most recent documents from a specified index
    for viewing and inspection purposes.
    
    Args:
        index_name: Full name of the index (catalog.schema.index)
        workspace_url: Databricks workspace URL
        endpoint_name: Name of the Vector Search endpoint
        index_type: Type of index (short_term, long_term, entity, document)
        backend_id: Backend configuration ID to retrieve embedding dimension
        limit: Maximum number of documents to return (default: 30)
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Dictionary containing documents and metadata
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request) if request else None
        
        # Get embedding dimension from backend config if backend_id is provided
        embedding_dimension = 1024  # Default
        if backend_id and group_context:
            try:
                backend = await service.get_memory_backend(group_context.primary_group_id, backend_id)
                if backend and backend.databricks_config:
                    db_config = backend.databricks_config
                    if hasattr(db_config, 'embedding_dimension'):
                        embedding_dimension = db_config.embedding_dimension or 1024
                    elif isinstance(db_config, dict):
                        embedding_dimension = db_config.get('embedding_dimension', 1024)
                    else:
                        embedding_dimension = 1024
                    logger.info(f"Retrieved embedding dimension {embedding_dimension} from backend config {backend_id}")
            except Exception as e:
                logger.warning(f"Could not retrieve embedding dimension from backend config: {e}")
        
        # Get documents from the service
        result = await service.get_index_documents(
            workspace_url=workspace_url,
            endpoint_name=endpoint_name,
            index_name=index_name,
            index_type=index_type,
            embedding_dimension=embedding_dimension,
            limit=limit,
            user_token=user_token
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching index documents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/databricks/entity-data")
async def get_entity_data(
    index_name: str = Query(..., description="Name of the entity memory index"),
    workspace_url: str = Query(..., description="Databricks workspace URL"),
    endpoint_name: str = Query(..., description="Vector Search endpoint name"),
    embedding_dimension: int = Query(1024, description="Dimension of embedding vectors"),
    limit: int = Query(100, description="Maximum number of entities to return"),
    request: Request = None,
    group_context: GroupContextDep = None,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)] = None,
) -> Dict[str, Any]:
    """
    Retrieve entity data from the entity memory index for visualization.
    
    This endpoint fetches entities and their relationships from the Databricks
    Vector Search entity memory index and formats them for graph visualization.
    
    Args:
        index_name: Full name of the entity memory index (catalog.schema.index)
        workspace_url: Databricks workspace URL
        endpoint_name: Name of the Vector Search endpoint
        embedding_dimension: Dimension of embedding vectors (default: 1024)
        limit: Maximum number of entities to return (default: 100)
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service
        
    Returns:
        Dictionary containing entities and relationships for visualization
    """
    # Import the databricks logger
    from src.core.logger import LoggerManager
    databricks_logger = LoggerManager.get_instance().databricks_vector_search
    
    databricks_logger.info(f"[ENTITY] API endpoint called: /databricks/entity-data")
    databricks_logger.info(f"[ENTITY] Parameters: index_name={index_name}, workspace_url={workspace_url}, endpoint_name={endpoint_name}, limit={limit}")
    
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request) if request else None
        databricks_logger.info(f"[ENTITY] User token extracted: {'Yes' if user_token else 'No'}")
        
        # Get the index service
        from src.services.databricks_index_service import DatabricksIndexService
        index_service = DatabricksIndexService()
        
        # Query the actual entity data from Databricks Vector Search
        result = await index_service.query_entity_data(
            workspace_url=workspace_url,
            endpoint_name=endpoint_name,
            index_name=index_name,
            embedding_dimension=embedding_dimension,
            limit=limit,
            user_token=user_token
        )
        
        databricks_logger.info(f"[ENTITY] Query result: success={result.get('success')}, entities={len(result.get('entities', []))}, relationships={len(result.get('relationships', []))}")
        
        # Return the actual data from the index
        return result
        
    except Exception as e:
        databricks_logger.error(f"[ENTITY] Error retrieving entity data: {e}")
        logger.error(f"Error retrieving entity data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve entity data: {str(e)}"
        )