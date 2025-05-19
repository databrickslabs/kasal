from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
from src.dependencies.auth import get_current_user, check_user_role
from src.services.identity_provider_service import IdentityProviderService
from src.schemas.user import (
    IdentityProviderResponse, 
    IdentityProviderCreate, 
    IdentityProviderUpdate, 
    IdentityProviderListResponse,
    IdentityProviderUsageStatsResponse
)
from src.models.user import User

# Create router
router = APIRouter(
    prefix="/identity-providers",
    tags=["identity-providers"],
)

# Admin only dependency
admin_only = lambda: check_user_role(allowed_roles=["admin"])

@router.get(
    "/",
    response_model=List[IdentityProviderResponse],
    summary="Get all identity providers",
    description="Get a list of all identity providers. Regular users can only see enabled providers.",
)
async def get_identity_providers(
    skip: int = 0,
    limit: int = 100,
    enabled_only: bool = False,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Non-admin users can only see enabled providers
    if current_user.role != "admin":
        enabled_only = True
    
    service = IdentityProviderService(session)
    providers = await service.get_providers(skip=skip, limit=limit, enabled_only=enabled_only)
    
    # For non-admin users, remove sensitive data like provider config
    if current_user.role != "admin":
        for provider in providers:
            provider.config = None
    
    return providers

@router.post(
    "/",
    response_model=IdentityProviderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new identity provider",
    description="Create a new identity provider. Admin only.",
    dependencies=[Depends(admin_only)],
)
async def create_identity_provider(
    provider_data: IdentityProviderCreate,
    session: AsyncSession = Depends(get_db),
):
    service = IdentityProviderService(session)
    try:
        provider = await service.create_provider(provider_data)
        return provider
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

@router.get(
    "/{provider_id}",
    response_model=IdentityProviderResponse,
    summary="Get identity provider by ID",
    description="Get an identity provider by ID. Regular users can only access enabled providers.",
)
async def get_identity_provider(
    provider_id: str,
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    service = IdentityProviderService(session)
    provider = await service.get_provider(provider_id)
    
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Identity provider not found",
        )
    
    # Non-admin users can only access enabled providers
    if current_user.role != "admin" and not provider.enabled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Identity provider not found",
        )
    
    # For non-admin users, remove sensitive data like provider config
    if current_user.role != "admin":
        provider.config = None
    
    return provider

@router.put(
    "/{provider_id}",
    response_model=IdentityProviderResponse,
    summary="Update an identity provider",
    description="Update an identity provider. Admin only.",
    dependencies=[Depends(admin_only)],
)
async def update_identity_provider(
    provider_id: str,
    provider_data: IdentityProviderUpdate,
    session: AsyncSession = Depends(get_db),
):
    service = IdentityProviderService(session)
    try:
        updated_provider = await service.update_provider(provider_id, provider_data)
        if not updated_provider:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Identity provider not found",
            )
        return updated_provider
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

@router.delete(
    "/{provider_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an identity provider",
    description="Delete an identity provider. Admin only.",
    dependencies=[Depends(admin_only)],
)
async def delete_identity_provider(
    provider_id: str,
    session: AsyncSession = Depends(get_db),
):
    service = IdentityProviderService(session)
    try:
        success = await service.delete_provider(provider_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Identity provider not found",
            )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    
    return None

@router.patch(
    "/{provider_id}/toggle",
    response_model=IdentityProviderResponse,
    summary="Toggle an identity provider's status",
    description="Enable or disable an identity provider. Admin only.",
    dependencies=[Depends(admin_only)],
)
async def toggle_identity_provider(
    provider_id: str,
    enabled: bool,
    session: AsyncSession = Depends(get_db),
):
    service = IdentityProviderService(session)
    updated_provider = await service.toggle_provider_status(provider_id, enabled)
    
    if not updated_provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Identity provider not found",
        )
    
    return updated_provider

@router.get(
    "/{provider_id}/stats",
    response_model=IdentityProviderUsageStatsResponse,
    summary="Get identity provider usage statistics",
    description="Get usage statistics for an identity provider. Admin only.",
    dependencies=[Depends(admin_only)],
)
async def get_identity_provider_stats(
    provider_id: str,
    session: AsyncSession = Depends(get_db),
):
    service = IdentityProviderService(session)
    stats = await service.get_provider_usage_stats(provider_id)
    
    if "error" in stats:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=stats["error"],
        )
    
    return stats 