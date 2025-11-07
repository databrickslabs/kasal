"""
Permission decorators for role-based access control in the backend.

This module provides decorators for enforcing the three-tier authorization model:
- Admin: Full access to all resources
- Editor: Can create/edit agents, tasks, crews, execute workflows
- Operator: Can only execute and monitor, no creation/editing rights
"""

from functools import wraps
from typing import Callable, List, Optional
from fastapi import HTTPException, status, Depends, Request
from src.core.dependencies import get_group_context
from src.utils.user_context import GroupContext
from src.models.enums import GroupUserRole


def require_roles(allowed_roles: List[str]):
    """
    Decorator to enforce role-based access control on API endpoints.

    Uses the new permission resolution algorithm to determine effective role.

    Args:
        allowed_roles: List of roles that are allowed to access the endpoint

    Example:
        @router.post("/agents")
        @require_roles(["admin", "editor"])
        async def create_agent(...):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get group_context from kwargs (injected by FastAPI dependency)
            group_context = None
            for key, value in kwargs.items():
                if isinstance(value, GroupContext):
                    group_context = value
                    break

            if not group_context:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Unable to verify permissions - no group context"
                )

            # Get effective role using the new resolution algorithm
            effective_role = get_effective_role(group_context)

            if not effective_role:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="No role assigned for this user in the selected workspace"
                )

            # Check if effective role is allowed
            if effective_role.lower() not in [role.lower() for role in allowed_roles]:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Access denied. Required roles: {', '.join(allowed_roles)}. Your role: {effective_role}"
                )

            # Call the original function
            return await func(*args, **kwargs)

        return wrapper
    return decorator


def require_admin():
    """
    Decorator to require admin role for accessing an endpoint.

    Example:
        @router.delete("/groups/{group_id}")
        @require_admin()
        async def delete_group(...):
            ...
    """
    return require_roles(["admin"])


def require_editor_or_admin():
    """
    Decorator to require editor or admin role for accessing an endpoint.

    Example:
        @router.post("/agents")
        @require_editor_or_admin()
        async def create_agent(...):
            ...
    """
    return require_roles(["admin", "editor"])


def require_operator_or_above():
    """
    Decorator to require operator, editor, or admin role for accessing an endpoint.
    This is for endpoints that all authenticated users can access.

    Example:
        @router.get("/executions")
        @require_operator_or_above()
        async def list_executions(...):
            ...
    """
    return require_roles(["admin", "editor", "operator"])


def check_group_ownership(func: Callable) -> Callable:
    """
    Decorator to ensure users can only access resources within their group.
    This decorator should be used AFTER role checks.

    Example:
        @router.get("/executions/{execution_id}")
        @require_operator_or_above()
        @check_group_ownership
        async def get_execution(execution_id: str, group_context: GroupContext = Depends(get_group_context)):
            ...
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Get group_context from kwargs
        group_context = None
        for key, value in kwargs.items():
            if isinstance(value, GroupContext):
                group_context = value
                break

        if not group_context:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Unable to verify group ownership - no group context"
            )

        user_group_id = group_context.primary_group_id

        if not user_group_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No group context found"
            )

        # The actual resource group validation should happen within the service layer
        # This decorator ensures group_context is available for validation
        return await func(*args, **kwargs)

    return wrapper


# Role hierarchy helper functions
def is_admin(role: Optional[str]) -> bool:
    """Check if the role is admin."""
    return role and role.lower() == "admin"


def is_editor_or_above(role: Optional[str]) -> bool:
    """Check if the role is editor or admin."""
    return role and role.lower() in ["admin", "editor"]


def is_operator_or_above(role: Optional[str]) -> bool:
    """Check if the role is operator, editor, or admin."""
    return role and role.lower() in ["admin", "editor", "operator"]


def check_role_in_context(group_context: GroupContext, allowed_roles: List[str]) -> bool:
    """
    Check if the user's role in the group context is allowed.

    Uses the new permission resolution algorithm to determine effective role.

    Args:
        group_context: The group context containing user role
        allowed_roles: List of allowed roles

    Returns:
        True if the user's role is allowed, False otherwise
    """
    effective_role = get_effective_role(group_context)

    if not effective_role:
        return False

    return effective_role.lower() in [role.lower() for role in allowed_roles]


def is_system_admin(group_context: GroupContext) -> bool:
    """
    Check if the user is a system administrator.
    System administrators have full access to all workspaces and system-level operations.

    Args:
        group_context: The group context containing user info

    Returns:
        True if the user is a system administrator, False otherwise
    """
    if not group_context:
        return False

    # Check if user is a system admin
    if hasattr(group_context, 'current_user') and group_context.current_user:
        return getattr(group_context.current_user, 'is_system_admin', False)

    return False


def is_workspace_admin(group_context: GroupContext) -> bool:
    """
    Check if the user is an admin of their current workspace.
    This is used to determine if a user can configure workspace-specific settings.

    Implements the permission resolution algorithm:
    1. System admin (is_system_admin) always gets admin role
    2. Personal workspace with is_personal_workspace_manager gets admin role
    3. Team workspace uses explicitly assigned role

    Args:
        group_context: The group context containing user role and user info

    Returns:
        True if the user is an admin in their current workspace, False otherwise
    """
    if not group_context:
        return False

    # Check if user is a system admin (they have admin role everywhere)
    if hasattr(group_context, 'current_user') and group_context.current_user:
        if getattr(group_context.current_user, 'is_system_admin', False):
            return True

        # Personal workspace - check if user has personal workspace manager permission
        if group_context.primary_group_id and group_context.primary_group_id.startswith("user_"):
            # User needs is_personal_workspace_manager to have admin rights in personal workspace
            return getattr(group_context.current_user, 'is_personal_workspace_manager', False)

    # Team workspace - check if user has admin role in their current group
    return group_context.user_role and group_context.user_role.lower() == "admin"


def get_effective_role(group_context: GroupContext) -> Optional[str]:
    """
    Get the effective role for a user in the current workspace.

    Implements the permission resolution algorithm:
    1. System admin always gets "admin" role
    2. Personal workspace: is_personal_workspace_manager gets "admin", others get "editor"
    3. Team workspace: uses explicitly assigned role

    Args:
        group_context: The group context containing user role and user info

    Returns:
        The effective role string ("admin", "editor", "operator") or None
    """
    if not group_context:
        return None

    # Check if user is a system admin
    if hasattr(group_context, 'current_user') and group_context.current_user:
        if getattr(group_context.current_user, 'is_system_admin', False):
            return "admin"

        # Personal workspace logic
        if group_context.primary_group_id and group_context.primary_group_id.startswith("user_"):
            if getattr(group_context.current_user, 'is_personal_workspace_manager', False):
                return "admin"
            else:
                return "editor"  # Default role in personal workspace

    # Team workspace - use assigned role
    return group_context.user_role