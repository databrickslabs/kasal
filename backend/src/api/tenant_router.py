"""
API router for tenant management.

Provides endpoints for manual tenant creation and user assignment.
This is the admin interface for the simple multi-tenant foundation.
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from src.core.dependencies import SessionDep, TenantContextDep
from src.services.tenant_service import TenantService
from src.models.tenant import Tenant, TenantUser
from src.schemas.tenant import (
    TenantResponse,
    TenantCreateRequest,
    TenantUpdateRequest,
    TenantUserResponse,
    TenantUserCreateRequest,
    TenantUserUpdateRequest
)
from src.core.logger import LoggerManager

class TenantContextResponse(BaseModel):
    """Response showing current tenant context for testing."""
    tenant_id: Optional[str] = None
    tenant_email: Optional[str] = None  
    email_domain: Optional[str] = None
    user_id: Optional[str] = None
    access_token_present: bool = False
    message: str

logger = LoggerManager.get_instance().api

router = APIRouter(
    prefix="/tenants",
    tags=["tenants"],
    responses={404: {"description": "Not found"}},
)


@router.get("/", response_model=List[TenantResponse])
async def list_tenants(
    session: SessionDep,
    skip: int = 0,
    limit: int = 100
) -> List[TenantResponse]:
    """
    List all tenants with user counts.
    
    Admin endpoint for viewing all tenants in the system.
    """
    tenant_service = TenantService(session)
    
    try:
        # TODO: Add proper pagination and filtering
        tenants = await tenant_service.list_tenants(skip=skip, limit=limit)
        
        # Add user counts to each tenant
        response_tenants = []
        for tenant in tenants:
            user_count = await tenant_service.get_tenant_user_count(tenant.id)
            tenant_dict = tenant.__dict__.copy()
            tenant_dict['user_count'] = user_count
            response_tenants.append(TenantResponse(**tenant_dict))
        
        return response_tenants
        
    except Exception as e:
        logger.error(f"Error listing tenants: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list tenants"
        )


@router.post("/", response_model=TenantResponse, status_code=status.HTTP_201_CREATED)
async def create_tenant(
    tenant_data: TenantCreateRequest,
    session: SessionDep,
    tenant_context: TenantContextDep
) -> TenantResponse:
    """
    Create a new tenant manually.
    
    Admin endpoint for manual tenant creation with full control.
    """
    tenant_service = TenantService(session)
    
    try:
        # Create tenant with admin context
        tenant = await tenant_service.create_tenant(
            name=tenant_data.name,
            email_domain=tenant_data.email_domain,
            description=tenant_data.description,
            created_by_email=tenant_context.tenant_email or "admin"
        )
        
        # Get user count (will be 0 for new tenant)
        user_count = await tenant_service.get_tenant_user_count(tenant.id)
        
        tenant_dict = tenant.__dict__.copy()
        tenant_dict['user_count'] = user_count
        
        logger.info(f"Created tenant {tenant.id} by {tenant_context.tenant_email}")
        return TenantResponse(**tenant_dict)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating tenant: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create tenant"
        )


@router.get("/{tenant_id}", response_model=TenantResponse)
async def get_tenant(
    tenant_id: str,
    session: SessionDep
) -> TenantResponse:
    """Get a specific tenant by ID."""
    tenant_service = TenantService(session)
    
    try:
        tenant = await tenant_service.get_tenant_by_id(tenant_id)
        if not tenant:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tenant {tenant_id} not found"
            )
        
        user_count = await tenant_service.get_tenant_user_count(tenant.id)
        tenant_dict = tenant.__dict__.copy()
        tenant_dict['user_count'] = user_count
        
        return TenantResponse(**tenant_dict)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting tenant {tenant_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get tenant"
        )


@router.put("/{tenant_id}", response_model=TenantResponse)
async def update_tenant(
    tenant_id: str,
    tenant_data: TenantUpdateRequest,
    session: SessionDep,
    tenant_context: TenantContextDep
) -> TenantResponse:
    """Update a tenant."""
    tenant_service = TenantService(session)
    
    try:
        tenant = await tenant_service.update_tenant(
            tenant_id=tenant_id,
            **tenant_data.dict(exclude_unset=True)
        )
        
        user_count = await tenant_service.get_tenant_user_count(tenant.id)
        tenant_dict = tenant.__dict__.copy()
        tenant_dict['user_count'] = user_count
        
        logger.info(f"Updated tenant {tenant_id} by {tenant_context.tenant_email}")
        return TenantResponse(**tenant_dict)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error updating tenant {tenant_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update tenant"
        )


@router.get("/{tenant_id}/users", response_model=List[TenantUserResponse])
async def list_tenant_users(
    tenant_id: str,
    session: SessionDep,
    skip: int = 0,
    limit: int = 100
) -> List[TenantUserResponse]:
    """List all users in a tenant."""
    tenant_service = TenantService(session)
    
    try:
        # Verify tenant exists
        tenant = await tenant_service.get_tenant_by_id(tenant_id)
        if not tenant:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tenant {tenant_id} not found"
            )
        
        tenant_users_with_emails = await tenant_service.list_tenant_users(
            tenant_id=tenant_id,
            skip=skip,
            limit=limit
        )
        
        # Construct responses with emails
        responses = []
        for tenant_user, email in tenant_users_with_emails:
            response_data = tenant_user.__dict__.copy()
            response_data['email'] = email
            responses.append(TenantUserResponse(**response_data))
        
        return responses
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing users for tenant {tenant_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list tenant users"
        )


@router.post("/{tenant_id}/users", response_model=TenantUserResponse, status_code=status.HTTP_201_CREATED)
async def assign_user_to_tenant(
    tenant_id: str,
    user_data: TenantUserCreateRequest,
    session: SessionDep,
    tenant_context: TenantContextDep
) -> TenantUserResponse:
    """
    Assign a user to a tenant manually.
    
    Admin endpoint for manual user assignment with role control.
    """
    tenant_service = TenantService(session)
    
    try:
        # Verify tenant exists
        tenant = await tenant_service.get_tenant_by_id(tenant_id)
        if not tenant:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tenant {tenant_id} not found"
            )
        
        # Create or update user assignment
        tenant_user, user_email = await tenant_service.assign_user_to_tenant(
            tenant_id=tenant_id,
            user_email=user_data.user_email,
            role=user_data.role,
            assigned_by_email=tenant_context.tenant_email or "admin"
        )
        
        # Construct response with email
        response_data = tenant_user.__dict__.copy()
        response_data['email'] = user_email
        
        logger.info(f"Assigned user {user_data.user_email} to tenant {tenant_id} by {tenant_context.tenant_email}")
        return TenantUserResponse(**response_data)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error assigning user to tenant {tenant_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to assign user to tenant"
        )


@router.put("/{tenant_id}/users/{user_id}", response_model=TenantUserResponse)
async def update_tenant_user(
    tenant_id: str,
    user_id: str,
    user_data: TenantUserUpdateRequest,
    session: SessionDep,
    tenant_context: TenantContextDep
) -> TenantUserResponse:
    """Update a user's role or status in a tenant."""
    tenant_service = TenantService(session)
    
    try:
        tenant_user = await tenant_service.update_tenant_user(
            tenant_id=tenant_id,
            user_id=user_id,
            **user_data.dict(exclude_unset=True)
        )
        
        logger.info(f"Updated user {user_id} in tenant {tenant_id} by {tenant_context.tenant_email}")
        return TenantUserResponse(**tenant_user.__dict__)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error updating user {user_id} in tenant {tenant_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update tenant user"
        )


@router.delete("/{tenant_id}/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_user_from_tenant(
    tenant_id: str,
    user_id: str,
    session: SessionDep,
    tenant_context: TenantContextDep
):
    """Remove a user from a tenant."""
    tenant_service = TenantService(session)
    
    try:
        await tenant_service.remove_user_from_tenant(
            tenant_id=tenant_id,
            user_id=user_id
        )
        
        logger.info(f"Removed user {user_id} from tenant {tenant_id} by {tenant_context.tenant_email}")
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error removing user {user_id} from tenant {tenant_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to remove user from tenant"
        )


@router.get("/debug/context", response_model=TenantContextResponse)
async def get_tenant_context_debug(
    tenant_context: TenantContextDep
) -> TenantContextResponse:
    """
    Debug endpoint to show current tenant context.
    
    This endpoint helps verify that tenant isolation is working correctly
    by showing what tenant context is extracted from the request headers.
    """
    return TenantContextResponse(
        tenant_id=tenant_context.tenant_id,
        tenant_email=tenant_context.tenant_email,
        email_domain=tenant_context.email_domain,
        user_id=tenant_context.user_id,
        access_token_present=bool(tenant_context.access_token),
        message=f"Tenant context extracted successfully for {tenant_context.tenant_email or 'anonymous user'}"
    )