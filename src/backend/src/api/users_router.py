from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, status

from src.core.exceptions import ForbiddenError, NotFoundError
from src.dependencies.admin_auth import AuthenticatedUserDep, SystemAdminUserDep
from src.dependencies.providers import GroupContextDep, SessionDep
from src.schemas.user import UserInDB, UserPermissionUpdate, UserUpdate
from src.services.groups.users import UserService

router = APIRouter(
    prefix="/users",
    tags=["users"],
    responses={401: {"description": "Unauthorized"}},
)


# Dependency to get UserService with explicit SessionDep
def get_user_service(session: SessionDep) -> UserService:
    """
    Factory function for UserService with explicit session dependency.

    Args:
        session: Database session from FastAPI DI

    Returns:
        UserService instance with injected session
    """
    return UserService(session)


@router.get("/me", response_model=UserInDB)
async def read_users_me(
    current_user: AuthenticatedUserDep,
    service: Annotated[UserService, Depends(get_user_service)],
    group_context: GroupContextDep,
):
    """Get current user's information"""
    import logging

    logger = logging.getLogger(__name__)
    logger.info(
        f"[ENDPOINT DEBUG] /users/me called for user: {current_user.email}, is_system_admin: {current_user.is_system_admin}, is_personal_workspace_manager: {current_user.is_personal_workspace_manager}"
    )
    return await service.get_user_complete(current_user.id)


@router.put("/me", response_model=UserInDB)
async def update_users_me(
    user_update: UserUpdate,
    current_user: AuthenticatedUserDep,
    service: Annotated[UserService, Depends(get_user_service)],
    group_context: GroupContextDep,
):
    """Update current user's information"""
    return await service.update_user(current_user.id, user_update)


# /me/profile endpoint removed - display_name is now part of User model

# External identity endpoints removed - using simplified auth


# Global user administration — SYSTEM admins only.
#
# These routes list, read, change and delete ANY user row. They used to accept
# an admin of any workspace, and a user's email is what authentication resolves
# an identity by: a workspace admin could point a system admin's row at an
# email they control and sign in as that admin (audit F01). Workspace admins
# manage their members through the groups routes, not here.
@router.get("", response_model=List[UserInDB])
async def read_users(
    service: Annotated[UserService, Depends(get_user_service)],
    admin_user: SystemAdminUserDep,
    group_context: GroupContextDep,
    skip: int = 0,
    limit: int = 100,
    role: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
):
    """Get list of users (system admin only)"""
    filters = {}

    if role:
        filters["role"] = role
    if status:
        filters["status"] = status

    users = await service.get_users(
        skip=skip, limit=limit, filters=filters, search=search
    )
    return users


@router.get("/{user_id}")
async def read_user(
    user_id: str,
    service: Annotated[UserService, Depends(get_user_service)],
    admin_user: SystemAdminUserDep,
    group_context: GroupContextDep,
):
    """Get user by ID (system admin only)"""
    # Use injected service
    user = await service.get_user_complete(user_id)

    if not user:
        raise NotFoundError("User not found")

    return user


@router.put("/{user_id}", response_model=UserInDB)
async def update_user(
    user_id: str,
    user_update: UserUpdate,
    service: Annotated[UserService, Depends(get_user_service)],
    admin_user: SystemAdminUserDep,
    group_context: GroupContextDep,
):
    """Update a user's username or status (system admin only). Email is
    immutable through the API — it is the identity authentication resolves."""
    # Use injected service
    user = await service.update_user(user_id, user_update)

    if not user:
        raise NotFoundError("User not found")

    return user


# System admin endpoint for managing user permissions
@router.put("/{user_id}/permissions", response_model=UserInDB)
async def update_user_permissions(
    user_id: str,
    permission_update: UserPermissionUpdate,
    service: Annotated[UserService, Depends(get_user_service)],
    admin_user: SystemAdminUserDep,
    group_context: GroupContextDep,
):
    """Update user permissions (system admin only)"""
    # Check if the current user is a system admin
    if (
        not group_context
        or not group_context.current_user
        or not group_context.current_user.is_system_admin
    ):
        raise ForbiddenError("Only system admins can manage user permissions")

    # Update the user permissions
    user = await service.update_user_permissions(user_id, permission_update)

    if not user:
        raise NotFoundError("User not found")

    return user


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: str,
    service: Annotated[UserService, Depends(get_user_service)],
    admin_user: SystemAdminUserDep,
    group_context: GroupContextDep,
):
    """Delete a user (system admin only)"""
    # Use injected service
    success = await service.delete_user(user_id)

    if not success:
        raise NotFoundError("User not found")
