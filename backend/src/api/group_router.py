"""
API router for group management.

Provides endpoints for manual group creation and user assignment.
This is the admin interface for the simple multi-group foundation.
"""
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from src.core.dependencies import SessionDep, GroupContextDep
from src.dependencies.admin_auth import AdminUserDep, AdminUserPrivilegesDep
from src.services.group_service import GroupService
from src.models.group import Group, GroupUser
from src.models.user import User
from src.schemas.group import (
    GroupResponse,
    GroupCreateRequest,
    GroupUpdateRequest,
    GroupUserResponse,
    GroupUserCreateRequest,
    GroupUserUpdateRequest,
    GroupStatsResponse
)
from src.core.logger import LoggerManager

class GroupContextResponse(BaseModel):
    """Response showing current group context for testing."""
    group_id: Optional[str] = None
    group_email: Optional[str] = None  
    email_domain: Optional[str] = None
    user_id: Optional[str] = None
    access_token_present: bool = False
    message: str

logger = LoggerManager.get_instance().api

router = APIRouter(
    prefix="/groups",
    tags=["groups"],
    responses={404: {"description": "Not found"}},
)


@router.get("/", response_model=List[GroupResponse])
async def list_groups(
    session: SessionDep,
    admin_user: AdminUserDep,
    skip: int = 0,
    limit: int = 100
) -> List[GroupResponse]:
    """
    List all groups with user counts.
    
    Admin endpoint for viewing all groups in the system.
    Requires admin privileges.
    """
    group_service = GroupService(session)
    
    try:
        # Get groups with user counts
        groups_with_counts = await group_service.list_groups(skip=skip, limit=limit)
        
        # Convert to response objects
        response_groups = []
        for group_data in groups_with_counts:
            response_groups.append(GroupResponse(**group_data))
        
        return response_groups
        
    except Exception as e:
        logger.error(f"Error listing groups: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list groups"
        )


@router.post("/", response_model=GroupResponse, status_code=status.HTTP_201_CREATED)
async def create_group(
    group_data: GroupCreateRequest,
    session: SessionDep,
    admin_user: AdminUserDep
) -> GroupResponse:
    """
    Create a new group manually.
    
    Admin endpoint for manual group creation with full control.
    Requires admin privileges.
    """
    group_service = GroupService(session)
    
    try:
        # Create group with admin context
        group = await group_service.create_group(
            name=group_data.name,
            email_domain=group_data.email_domain,
            description=group_data.description,
            created_by_email=admin_user.email
        )
        
        # Get user count (will be 0 for new group)
        user_count = await group_service.get_group_user_count(group.id)
        
        # Convert Group object to dict for response
        group_dict = {
            'id': group.id,
            'name': group.name,
            'email_domain': group.email_domain,
            'status': group.status,
            'description': group.description,
            'auto_created': group.auto_created,
            'created_by_email': group.created_by_email,
            'created_at': group.created_at,
            'updated_at': group.updated_at,
            'user_count': user_count
        }
        
        logger.info(f"Created group {group.id} by {admin_user.email}")
        return GroupResponse(**group_dict)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating group: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create group"
        )


@router.get("/{group_id}", response_model=GroupResponse)
async def get_group(
    group_id: str,
    session: SessionDep,
    admin_user: AdminUserDep
) -> GroupResponse:
    """Get a specific group by ID. Requires admin privileges."""
    group_service = GroupService(session)
    
    try:
        group = await group_service.get_group_by_id(group_id)
        if not group:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Group {group_id} not found"
            )
        
        user_count = await group_service.get_group_user_count(group.id)
        group_dict = {
            'id': group.id,
            'name': group.name,
            'email_domain': group.email_domain,
            'status': group.status,
            'description': group.description,
            'auto_created': group.auto_created,
            'created_by_email': group.created_by_email,
            'created_at': group.created_at,
            'updated_at': group.updated_at,
            'user_count': user_count
        }
        
        return GroupResponse(**group_dict)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting group {group_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get group"
        )


@router.put("/{group_id}", response_model=GroupResponse)
async def update_group(
    group_id: str,
    group_data: GroupUpdateRequest,
    session: SessionDep,
    admin_user: AdminUserDep
) -> GroupResponse:
    """Update a group. Requires admin privileges."""
    group_service = GroupService(session)
    
    try:
        group = await group_service.update_group(
            group_id=group_id,
            **group_data.dict(exclude_unset=True)
        )
        
        user_count = await group_service.get_group_user_count(group.id)
        group_dict = {
            'id': group.id,
            'name': group.name,
            'email_domain': group.email_domain,
            'status': group.status,
            'description': group.description,
            'auto_created': group.auto_created,
            'created_by_email': group.created_by_email,
            'created_at': group.created_at,
            'updated_at': group.updated_at,
            'user_count': user_count
        }
        
        logger.info(f"Updated group {group_id} by {admin_user.email}")
        return GroupResponse(**group_dict)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error updating group {group_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update group"
        )


@router.delete("/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_group(
    group_id: str,
    session: SessionDep,
    admin_user: AdminUserDep
):
    """
    Delete a group and all associated data.
    
    Warning: This permanently removes the group and all associated:
    - User assignments
    - Execution history
    - All related data
    
    This action cannot be undone.
    Requires admin privileges.
    """
    group_service = GroupService(session)
    
    try:
        await group_service.delete_group(group_id)
        logger.info(f"Deleted group {group_id} by {admin_user.email}")
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error deleting group {group_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete group"
        )


@router.get("/{group_id}/users", response_model=List[GroupUserResponse])
async def list_group_users(
    group_id: str,
    session: SessionDep,
    admin_user: AdminUserDep,
    skip: int = 0,
    limit: int = 100
) -> List[GroupUserResponse]:
    """List all users in a group. Requires admin privileges."""
    group_service = GroupService(session)
    
    try:
        # Verify group exists
        group = await group_service.get_group_by_id(group_id)
        if not group:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Group {group_id} not found"
            )
        
        group_users_with_details = await group_service.list_group_users(
            group_id=group_id,
            skip=skip,
            limit=limit
        )
        
        # Construct responses 
        responses = []
        for group_user_data in group_users_with_details:
            responses.append(GroupUserResponse(**group_user_data))
        
        return responses
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing users for group {group_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list group users"
        )


@router.post("/{group_id}/users", response_model=GroupUserResponse, status_code=status.HTTP_201_CREATED)
async def assign_user_to_group(
    group_id: str,
    user_data: GroupUserCreateRequest,
    session: SessionDep,
    admin_user: AdminUserDep
) -> GroupUserResponse:
    """
    Assign a user to a group manually.
    
    Admin endpoint for manual user assignment with role control.
    Requires admin privileges.
    """
    group_service = GroupService(session)
    
    try:
        # Verify group exists
        group = await group_service.get_group_by_id(group_id)
        if not group:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Group {group_id} not found"
            )
        
        # Create or update user assignment
        group_user_data = await group_service.assign_user_to_group(
            group_id=group_id,
            user_email=user_data.user_email,
            role=user_data.role,
            assigned_by_email=admin_user.email
        )
        
        logger.info(f"Assigned user {user_data.user_email} to group {group_id} by {admin_user.email}")
        return GroupUserResponse(**group_user_data)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error assigning user to group {group_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to assign user to group"
        )


@router.put("/{group_id}/users/{user_id}", response_model=GroupUserResponse)
async def update_group_user(
    group_id: str,
    user_id: str,
    user_data: GroupUserUpdateRequest,
    session: SessionDep,
    admin_user: AdminUserDep
) -> GroupUserResponse:
    """Update a user's role or status in a group. Requires admin privileges."""
    group_service = GroupService(session)
    
    try:
        group_user = await group_service.update_group_user(
            group_id=group_id,
            user_id=user_id,
            **user_data.dict(exclude_unset=True)
        )
        
        # Get email for response
        from sqlalchemy import select
        user_stmt = select(User).where(User.id == user_id)
        result = await session.execute(user_stmt)
        user = result.scalar_one_or_none()
        email = user.email if user else f"{user_id}@databricks.com"
        
        response_data = {
            'id': group_user.id,
            'group_id': group_user.group_id,
            'user_id': group_user.user_id,
            'role': group_user.role,
            'status': group_user.status,
            'joined_at': group_user.joined_at,
            'auto_created': group_user.auto_created,
            'created_at': group_user.created_at,
            'updated_at': group_user.updated_at,
            'email': email
        }
        
        logger.info(f"Updated user {user_id} in group {group_id} by {admin_user.email}")
        return GroupUserResponse(**response_data)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error updating user {user_id} in group {group_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update group user"
        )


@router.delete("/{group_id}/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_user_from_group(
    group_id: str,
    user_id: str,
    session: SessionDep,
    admin_user: AdminUserDep
):
    """Remove a user from a group. Requires admin privileges."""
    group_service = GroupService(session)
    
    try:
        await group_service.remove_user_from_group(
            group_id=group_id,
            user_id=user_id
        )
        
        logger.info(f"Removed user {user_id} from group {group_id} by {admin_user.email}")
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error removing user {user_id} from group {group_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to remove user from group"
        )


@router.get("/stats", response_model=GroupStatsResponse)
async def get_group_stats(
    session: SessionDep,
    admin_user: AdminUserDep
) -> GroupStatsResponse:
    """
    Get group statistics.
    
    Admin endpoint for viewing system-wide group statistics.
    Requires admin privileges.
    """
    group_service = GroupService(session)
    
    try:
        stats = await group_service.get_group_stats()
        return GroupStatsResponse(**stats)
        
    except Exception as e:
        logger.error(f"Error getting group stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get group statistics"
        )


@router.get("/debug/context", response_model=GroupContextResponse)
async def get_group_context_debug(
    group_context: GroupContextDep
) -> GroupContextResponse:
    """
    Debug endpoint to show current group context.
    
    This endpoint helps verify that group isolation is working correctly
    by showing what group context is extracted from the request headers.
    """
    return GroupContextResponse(
        group_id=group_context.primary_group_id,
        group_email=group_context.group_email,
        email_domain=group_context.email_domain,
        user_id=group_context.user_id,
        access_token_present=bool(group_context.access_token),
        message=f"Group context extracted successfully for {group_context.group_email or 'anonymous user'}"
    )


# Legacy compatibility removed - migration complete