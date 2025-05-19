"""
Router for handling Databricks secrets.

This module provides a router for Databricks secrets CRUD operations.
"""

from typing import Annotated, List, Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Path, status
import logging
import os

from src.core.dependencies import SessionDep
from src.services.databricks_secrets_service import DatabricksSecretsService
from src.schemas.databricks_secret import (
    SecretBase,
    SecretCreate,
    SecretUpdate,
    SecretResponse,
    DatabricksTokenRequest,
)

router = APIRouter(
    prefix="/databricks-secrets",
    tags=["databricks-secrets"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

# Dependency to get SecretService
async def get_secret_service():
    """
    Get a properly initialized DatabricksSecretsService instance using UnitOfWork.
    
    Returns:
        Initialized DatabricksSecretsService
    """
    from src.core.unit_of_work import UnitOfWork
    async with UnitOfWork() as uow:
        # Get the DatabricksService first
        from src.services.databricks_service import DatabricksService
        databricks_service = await DatabricksService.from_unit_of_work(uow)
        
        # Create DatabricksSecretsService
        service = DatabricksSecretsService(uow.databricks_config_repository)
        service.set_databricks_service(databricks_service)
        service.set_api_key_repository(uow.api_key_repository)
        
        yield service


@router.get("", response_model=List[SecretResponse])
async def get_databricks_secrets(
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """
    Get all secrets from Databricks secret store.
    
    Args:
        service: Secret service injected by dependency
        
    Returns:
        List of secrets
    """
    try:
        # Get secrets from Databricks if configured
        databricks_secrets = []
        try:
            config = await service.databricks_service.get_databricks_config()
            if config and config.is_enabled and config.workspace_url and config.secret_scope:
                # Use token from environment variable
                token = os.getenv("DATABRICKS_TOKEN", "")
                if token:
                    # Get secrets list from Databricks
                    databricks_results = await service.get_databricks_secrets(config.secret_scope)
                    
                    # Return the results directly
                    return databricks_results
        except Exception as e:
            logger.warning(f"Error getting Databricks secrets: {str(e)}")
            
        return []
    except Exception as e:
        logger.error(f"Error getting Databricks secrets: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=SecretResponse, status_code=status.HTTP_201_CREATED)
async def create_databricks_secret(
    secret_data: SecretCreate,
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """
    Create a new secret in Databricks.
    
    Args:
        secret_data: Secret data for creation
        service: Secret service injected by dependency
        
    Returns:
        Created secret
    """
    try:
        # Try to store in Databricks
        config = await service.databricks_service.get_databricks_config()
        if config and config.is_enabled and config.workspace_url and config.secret_scope:
            # Set secret in Databricks
            success = await service.set_databricks_secret_value(
                config.secret_scope, 
                secret_data.name, 
                secret_data.value
            )
            
            if success:
                # Create a response object
                return {
                    "id": 1000,  # Use a high ID to avoid conflicts
                    "name": secret_data.name,
                    "value": secret_data.value,
                    "description": secret_data.description or "",
                    "scope": config.secret_scope,
                    "source": "databricks"
                }
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to create secret in Databricks"
                )
        else:
            # Databricks not configured
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Databricks not properly configured for secret storage"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating Databricks secret: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{secret_name}", response_model=SecretResponse)
async def update_databricks_secret(
    secret_name: str,
    secret_data: SecretUpdate,
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """
    Update an existing secret in Databricks.
    
    Args:
        secret_name: Name of the secret to update
        secret_data: Secret data for update
        service: Secret service injected by dependency
        
    Returns:
        Updated secret
    """
    try:
        # Log the request for debugging
        logger.info(f"Attempting to update Databricks secret: {secret_name}")
        
        # Try to update in Databricks
        config = await service.databricks_service.get_databricks_config()
        if config and config.is_enabled and config.workspace_url and config.secret_scope:
            success = await service.set_databricks_secret_value(
                config.secret_scope,
                secret_name,
                secret_data.value
            )
            
            if success:
                # Return updated secret
                logger.info(f"Secret updated in Databricks: {secret_name}")
                return {
                    "id": 1000,  # Use a high ID to avoid conflicts
                    "name": secret_name,
                    "value": secret_data.value,
                    "description": secret_data.description or "",
                    "scope": config.secret_scope,
                    "source": "databricks"
                }
            else:
                error_msg = f"Failed to update secret '{secret_name}' in Databricks"
                logger.error(error_msg)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=error_msg
                )
        else:
            # Databricks not configured
            error_msg = "Databricks not properly configured for secret storage"
            logger.error(error_msg)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_msg
            )
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Error updating Databricks secret: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.delete("/{secret_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_databricks_secret(
    secret_name: str,
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """
    Delete a secret from Databricks.
    
    Args:
        secret_name: Name of the secret to delete
        service: Secret service injected by dependency
    """
    try:
        # Try to delete from Databricks
        config = await service.databricks_service.get_databricks_config()
        if config and config.is_enabled and config.workspace_url and config.secret_scope:
            success = await service.delete_databricks_secret(
                config.secret_scope,
                secret_name
            )
            
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Secret '{secret_name}' not found in Databricks"
                )
        else:
            # Databricks not configured
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Databricks not properly configured for secret storage"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting Databricks secret: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scopes", status_code=status.HTTP_200_OK)
async def create_databricks_secret_scope(
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """
    Create a secret scope in Databricks if it doesn't exist.
    
    Args:
        service: Secret service injected by dependency
        
    Returns:
        Success status
    """
    try:
        config = await service.databricks_service.get_databricks_config()
        if not config or not config.is_enabled or not config.workspace_url or not config.secret_scope:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Databricks not properly configured"
            )
            
        token = os.getenv("DATABRICKS_TOKEN", "")
        if not token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="DATABRICKS_TOKEN environment variable not set"
            )
            
        success = await service.create_databricks_secret_scope(
            config.workspace_url,
            token,
            config.secret_scope
        )
        
        if success:
            return {"status": "success", "message": f"Scope '{config.secret_scope}' created or already exists"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create scope '{config.secret_scope}'"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating Databricks secret scope: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Legacy routes for backward compatibility (matching old routing paths)
@router.get("/secrets", response_model=List[Dict])
async def get_secrets(
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """Legacy endpoint for getting all secrets from a specific Databricks scope."""
    try:
        try:
            workspace_url, scope = await service.validate_databricks_config()
            secrets_list = await service.get_databricks_secrets(scope)
            if secrets_list is None:
                return []
            return secrets_list
        except Exception as e:
            logger.error(f"Error getting secrets: {str(e)}")
            return []
    except Exception as e:
        logger.error(f"Error getting secrets: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting secrets: {str(e)}")


@router.put("/secrets/{key}", status_code=status.HTTP_200_OK)
async def set_secret(
    key: str,
    secret_data: SecretUpdate,
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """Legacy endpoint for setting a secret value in Databricks."""
    try:
        workspace_url, scope = await service.validate_databricks_config()
        success = await service.set_databricks_secret_value(scope, key, secret_data.value)
        if success:
            return {"status": "success", "message": f"Secret '{key}' set in scope '{scope}'"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to set secret '{key}'"
            )
    except Exception as e:
        logger.error(f"Error setting secret: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error setting secret: {str(e)}")


@router.delete("/secrets/{key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_secret_endpoint(
    key: str,
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """Legacy endpoint for deleting a secret from Databricks."""
    try:
        workspace_url, scope = await service.validate_databricks_config()
        success = await service.delete_databricks_secret(scope, key)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Secret '{key}' not found in scope '{scope}'"
            )
    except Exception as e:
        logger.error(f"Error deleting secret: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting secret: {str(e)}")


@router.post("/secret-scopes", status_code=status.HTTP_200_OK)
async def create_secret_scope_endpoint(
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """Legacy endpoint for creating a secret scope if it doesn't exist."""
    try:
        workspace_url, scope = await service.validate_databricks_config()
        token = os.getenv("DATABRICKS_TOKEN", "")
        if not token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="DATABRICKS_TOKEN environment variable not set"
            )
        
        success = await service.create_databricks_secret_scope(workspace_url, token, scope)
        if success:
            return {"status": "success", "message": f"Scope '{scope}' created or already exists"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create scope '{scope}'"
            )
    except Exception as e:
        logger.error(f"Error creating secret scope: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating secret scope: {str(e)}")


@router.post("/databricks/token", status_code=status.HTTP_200_OK, response_model=Dict[str, str])
async def set_databricks_token(
    request: DatabricksTokenRequest,
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """Set Databricks token in the configuration."""
    try:
        # Validate that Databricks is configured and enabled
        config = await service.databricks_service.get_databricks_config()
        if not config or not config.is_enabled or not config.workspace_url or not config.secret_scope:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Databricks not properly configured"
            )
        
        # Set the token in the environment so it can be used
        os.environ["DATABRICKS_TOKEN"] = request.token
        
        # Store the token in Databricks scopes for later use
        success = await service.set_databricks_token(config.secret_scope, request.token)
        
        if success:
            return {"status": "success", "message": f"Token set for scope '{config.secret_scope}'"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to set Databricks token"
            )
    except Exception as e:
        logger.error(f"Error setting Databricks token: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error setting Databricks token: {str(e)}")


# Legacy API key endpoints - preserved for backward compatibility
# These are identical to the routes in the old file but now use properly separation
# of concerns between API keys and Databricks secrets
@router.get("/api-keys", response_model=List[SecretResponse])
async def get_legacy_api_keys(
    service: DatabricksSecretsService = Depends(get_secret_service),
    source: Optional[str] = None,
):
    """Legacy endpoint for getting all API keys."""
    logger.info("Legacy API keys GET endpoint called - redirecting to Databricks secrets")
    return await get_databricks_secrets(service=service)


@router.post("/api-key", response_model=SecretResponse)
async def create_legacy_api_key(
    secret_data: SecretCreate,
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """Legacy endpoint for creating a new API key."""
    logger.info(f"Legacy API key CREATE endpoint called for key '{secret_data.name}' - redirecting to Databricks secrets")
    return await create_databricks_secret(secret_data, service)


@router.put("/api-keys/{secret_name}", response_model=SecretResponse)
async def update_legacy_api_key(
    secret_name: str,
    secret_data: SecretUpdate,
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """Legacy endpoint for updating an API key."""
    logger.info(f"Legacy API key UPDATE endpoint called for key '{secret_name}' - redirecting to Databricks secrets")
    return await update_databricks_secret(secret_name, secret_data, service)


@router.delete("/api-key/{secret_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_legacy_api_key(
    secret_name: str,
    service: DatabricksSecretsService = Depends(get_secret_service),
):
    """Legacy endpoint for deleting an API key."""
    logger.info(f"Legacy API key DELETE endpoint called for key '{secret_name}' - redirecting to Databricks secrets")
    await delete_databricks_secret(secret_name, service) 