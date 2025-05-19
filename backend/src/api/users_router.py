from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
from src.schemas.user import (
    UserInDB, UserUpdate, UserProfileUpdate, UserWithProfile, 
    UserRoleAssign, UserComplete, ExternalIdentityInDB
)
from src.models.user import User
from src.dependencies.auth import get_current_user, get_current_active_user, check_user_role
from src.services.user_service import UserService

router = APIRouter(
    prefix="/users",
    tags=["users"],
    responses={401: {"description": "Unauthorized"}},
)

@router.get("/me", response_model=UserWithProfile)
async def read_users_me(
    current_user: User = Depends(get_current_active_user),
    session: AsyncSession = Depends(get_db),
):
    """Get current user's information"""
    user_service = UserService(session)
    return await user_service.get_user_with_profile(current_user.id)

@router.put("/me", response_model=UserWithProfile)
async def update_users_me(
    user_update: UserUpdate,
    current_user: User = Depends(get_current_active_user),
    session: AsyncSession = Depends(get_db),
):
    """Update current user's information"""
    user_service = UserService(session)
    return await user_service.update_user(current_user.id, user_update)

@router.put("/me/profile", response_model=UserWithProfile)
async def update_users_profile(
    profile_update: UserProfileUpdate,
    current_user: User = Depends(get_current_active_user),
    session: AsyncSession = Depends(get_db),
):
    """Update current user's profile"""
    user_service = UserService(session)
    return await user_service.update_user_profile(current_user.id, profile_update)

@router.get("/me/external-identities", response_model=List[ExternalIdentityInDB])
async def read_users_external_identities(
    current_user: User = Depends(get_current_active_user),
    session: AsyncSession = Depends(get_db),
):
    """Get current user's external identities"""
    user_service = UserService(session)
    return await user_service.get_user_external_identities(current_user.id)

@router.delete("/me/external-identities/{provider}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_external_identity(
    provider: str,
    current_user: User = Depends(get_current_active_user),
    session: AsyncSession = Depends(get_db),
):
    """Remove an external identity from current user"""
    user_service = UserService(session)
    success = await user_service.remove_external_identity(current_user.id, provider)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No external identity found for provider: {provider}"
        )

# Admin endpoints
@router.get("", response_model=List[UserInDB], dependencies=[Depends(check_user_role(["admin"]))])
async def read_users(
    skip: int = 0,
    limit: int = 100,
    role: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    session: AsyncSession = Depends(get_db),
):
    """Get list of users (admin only)"""
    user_service = UserService(session)
    filters = {}
    
    if role:
        filters["role"] = role
    if status:
        filters["status"] = status
    
    users = await user_service.get_users(skip=skip, limit=limit, filters=filters, search=search)
    return users

@router.get("/{user_id}", response_model=UserComplete, dependencies=[Depends(check_user_role(["admin"]))])
async def read_user(
    user_id: str,
    session: AsyncSession = Depends(get_db),
):
    """Get user by ID (admin only)"""
    user_service = UserService(session)
    user = await user_service.get_user_complete(user_id)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return user

@router.put("/{user_id}", response_model=UserInDB, dependencies=[Depends(check_user_role(["admin"]))])
async def update_user(
    user_id: str,
    user_update: UserUpdate,
    session: AsyncSession = Depends(get_db),
):
    """Update user information (admin only)"""
    user_service = UserService(session)
    user = await user_service.update_user(user_id, user_update)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return user

@router.put("/{user_id}/role", response_model=UserInDB, dependencies=[Depends(check_user_role(["admin"]))])
async def assign_user_role(
    user_id: str,
    role_assign: UserRoleAssign,
    session: AsyncSession = Depends(get_db),
):
    """Assign a role to a user (admin only)"""
    user_service = UserService(session)
    user = await user_service.assign_role(user_id, role_assign.role_id)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return user

@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(check_user_role(["admin"]))])
async def delete_user(
    user_id: str,
    session: AsyncSession = Depends(get_db),
):
    """Delete a user (admin only)"""
    user_service = UserService(session)
    success = await user_service.delete_user(user_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        ) 