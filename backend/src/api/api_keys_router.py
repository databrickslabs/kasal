from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
import logging

from src.core.dependencies import SessionDep
from src.services.api_keys_service import ApiKeysService
from src.schemas.api_key import ApiKeyCreate, ApiKeyUpdate, ApiKeyResponse

router = APIRouter(
    prefix="/api-keys",
    tags=["api-keys"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)

# Dependency to get ApiKeyService
def get_api_key_service(session: SessionDep) -> ApiKeysService:
    return ApiKeysService(session)


@router.get("", response_model=List[ApiKeyResponse])
async def get_api_keys(
    service: Annotated[ApiKeysService, Depends(get_api_key_service)],
):
    """
    Get all API keys stored in the local database.
    
    Args:
        service: API key service injected by dependency
        
    Returns:
        List of API keys
    """
    try:
        api_keys = await service.get_all_api_keys()
        return api_keys
    except Exception as e:
        logger.error(f"Error getting API keys: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=ApiKeyResponse, status_code=status.HTTP_201_CREATED)
async def create_api_key(
    api_key_data: ApiKeyCreate,
    service: Annotated[ApiKeysService, Depends(get_api_key_service)],
):
    """
    Create a new API key.
    
    Args:
        api_key_data: API key data for creation
        service: API key service injected by dependency
        
    Returns:
        Created API key
    """
    try:
        # Check if API key already exists
        existing_key = await service.find_by_name(api_key_data.name)
        if existing_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"API key with name '{api_key_data.name}' already exists"
            )
            
        # Create in database
        return await service.create_api_key(api_key_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating API key: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{api_key_name}", response_model=ApiKeyResponse)
async def update_api_key(
    api_key_name: str,
    api_key_data: ApiKeyUpdate,
    service: Annotated[ApiKeysService, Depends(get_api_key_service)],
):
    """
    Update an existing API key.
    
    Args:
        api_key_name: Name of the API key to update
        api_key_data: API key data for update
        service: API key service injected by dependency
        
    Returns:
        Updated API key
    """
    try:
        # Log the request for debugging
        logger.info(f"Attempting to update API key: {api_key_name}")
        
        # Check if API key exists in database
        existing_key = await service.find_by_name(api_key_name)
        
        if not existing_key:
            error_msg = f"API key '{api_key_name}' not found"
            logger.error(error_msg)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=error_msg
            )
            
        # Update in database
        updated_key = await service.update_api_key(api_key_name, api_key_data)
        if not updated_key:
            error_msg = f"API key '{api_key_name}' update failed"
            logger.error(error_msg)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=error_msg
            )
            
        logger.info(f"API key updated successfully: {api_key_name}")
        return updated_key
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Error updating API key: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.delete("/{api_key_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_api_key(
    api_key_name: str,
    service: Annotated[ApiKeysService, Depends(get_api_key_service)],
):
    """
    Delete an API key.
    
    Args:
        api_key_name: Name of the API key to delete
        service: API key service injected by dependency
    """
    try:
        # Check if API key exists in database
        existing_key = await service.find_by_name(api_key_name)
        
        if not existing_key:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"API key '{api_key_name}' not found"
            )
            
        # Delete from database
        deleted = await service.delete_api_key(api_key_name)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"API key '{api_key_name}' not found"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting API key: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 