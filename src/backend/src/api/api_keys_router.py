from typing import Annotated, List

from fastapi import APIRouter, Depends, HTTPException, status
import logging

from src.core.dependencies import SessionDep, GroupContextDep
from src.core.permissions import check_role_in_context
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
def get_api_key_service(
    session: SessionDep,
    group_context: GroupContextDep
) -> ApiKeysService:
    """
    Dependency provider for ApiKeysService with multi-tenant isolation.

    Creates service with session and group_id following the pattern:
    Router → Service → Repository → DB

    SECURITY: group_id is REQUIRED for multi-tenant isolation.
    All API key operations are scoped to the user's group.

    Args:
        session: Database session from FastAPI DI (from core.dependencies)
        group_context: Group context from FastAPI DI (provides group_id)

    Returns:
        ApiKeysService instance with session and group_id
    """
    group_id = group_context.primary_group_id if group_context else None
    return ApiKeysService(session, group_id=group_id)

# Type alias for cleaner function signatures
ApiKeysServiceDep = Annotated[ApiKeysService, Depends(get_api_key_service)]


@router.get("", response_model=List[ApiKeyResponse])
async def get_api_keys_metadata(
    service: ApiKeysServiceDep,
):
    """
    Get API keys metadata (names, descriptions) without actual values.

    Returns only safe metadata - no actual key values are returned.

    Args:
        service: API key service injected by dependency (with group_id)

    Returns:
        List of API keys with empty values (metadata only)
    """
    try:
        api_keys = await service.get_api_keys_metadata()
        return api_keys
    except Exception as e:
        logger.error(f"Error getting API keys metadata: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=ApiKeyResponse, status_code=status.HTTP_201_CREATED)
async def create_api_key(
    api_key_data: ApiKeyCreate,
    group_context: GroupContextDep,
    service: ApiKeysServiceDep,
):
    """
    Create a new API key.
    Only Admins and Editors can create API keys.

    Args:
        api_key_data: API key data for creation
        group_context: Group context for permission checking
        service: API key service injected by dependency (with group_id)

    Returns:
        Created API key
    """
    # Check permissions - only admins and editors can create API keys
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins and editors can create API keys"
        )

    try:
        # Check if API key already exists
        existing_key = await service.find_by_name(api_key_data.name)
        if existing_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"API key with name '{api_key_data.name}' already exists"
            )

        # Get user email from group context
        created_by_email = group_context.group_email if group_context else None

        # Create in database
        return await service.create_api_key(api_key_data, created_by_email=created_by_email)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating API key: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{api_key_name}", response_model=ApiKeyResponse)
async def update_api_key(
    api_key_name: str,
    api_key_data: ApiKeyUpdate,
    group_context: GroupContextDep,
    service: ApiKeysServiceDep,
):
    """
    Update an existing API key.
    Only Admins and Editors can update API keys.

    Args:
        api_key_name: Name of the API key to update
        api_key_data: API key data for update
        group_context: Group context for permission checking
        service: API key service injected by dependency (with group_id)

    Returns:
        Updated API key
    """
    # Check permissions - only admins and editors can update API keys
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins and editors can update API keys"
        )

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
    group_context: GroupContextDep,
    service: ApiKeysServiceDep,
):
    """
    Delete an API key.
    Only Admins and Editors can delete API keys.

    Args:
        api_key_name: Name of the API key to delete
        group_context: Group context for permission checking
        service: API key service injected by dependency (with group_id)
    """
    # Check permissions - only admins and editors can delete API keys
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins and editors can delete API keys"
        )

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