"""
Authentication and authorization dependencies for admin-only endpoints.
"""

from typing import Annotated, Optional

from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies.providers import GroupContextDep, SessionDep
from src.models.user import User


async def _create_user_from_forwarded_email(
    session: AsyncSession, email: str
) -> Optional[User]:
    """Compatibility entry point for fallback forwarded-identity provisioning."""
    from src.services.groups.forwarded_identity import get_or_create_forwarded_user

    return await get_or_create_forwarded_user(session, email)


async def get_current_user_from_email(
    session: SessionDep, group_context: GroupContextDep
) -> Optional[User]:
    """
    Get the current user based on the X-Forwarded-Email header.
    Uses UserService to ensure first user admin setup logic is triggered.

    Args:
        session: Database session
        group_context: Group context containing user email

    Returns:
        User object if found, None otherwise
    """
    import logging

    logger = logging.getLogger(__name__)
    logger.debug(
        f"[AUTH DEBUG] get_current_user_from_email called with email: {group_context.group_email if group_context.group_email else 'None'}"
    )

    if not group_context.group_email:
        logger.debug("[AUTH DEBUG] No group email found in context")
        return None

    # Use UserService instead of UserRepository to trigger first user setup logic
    from src.services.groups.users import UserService

    logger.debug(
        f"[AUTH DEBUG] Creating UserService and calling get_or_create_user_by_email for {group_context.group_email}"
    )
    user_service = UserService(session)
    user = await user_service.get_or_create_user_by_email(group_context.group_email)
    logger.debug(
        f"[AUTH DEBUG] get_or_create_user_by_email returned user: {user.email if user else 'None'}"
    )

    return user


async def require_authenticated_user(
    session: SessionDep, group_context: GroupContextDep
) -> User:
    """
    Dependency to ensure user is authenticated via X-Forwarded-Email header.
    Automatically creates users from X-Forwarded-Email if they don't exist.

    Args:
        session: Database session
        group_context: Group context containing user email

    Returns:
        User object

    Raises:
        HTTPException: If user is not authenticated or not found
    """
    import logging

    logger = logging.getLogger(__name__)
    logger.debug(
        f"[AUTH DEBUG] require_authenticated_user called with email: {group_context.group_email if group_context.group_email else 'None'}"
    )

    if not group_context.group_email:
        logger.debug(
            "[AUTH DEBUG] No group email in require_authenticated_user - returning 401"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. X-Forwarded-Email header not found.",
        )

    logger.debug(
        "[AUTH DEBUG] About to call get_current_user_from_email in require_authenticated_user"
    )
    user = await get_current_user_from_email(session, group_context)

    if not user:
        # Auto-create user from X-Forwarded-Email header with source tracking
        user = await _create_user_from_forwarded_email(
            session, group_context.group_email
        )

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"User with email {group_context.group_email} could not be created or found.",
            )

    return user


async def get_authenticated_user(
    session: SessionDep, group_context: GroupContextDep
) -> User:
    """
    General dependency for any authenticated endpoint that auto-creates users from X-Forwarded-Email.
    This is the main authentication dependency that should be used for most endpoints.

    Args:
        session: Database session
        group_context: Group context containing user email

    Returns:
        User object

    Raises:
        HTTPException: If user is not authenticated or not found
    """
    return await require_authenticated_user(session, group_context)


async def get_admin_user(session: SessionDep, group_context: GroupContextDep) -> User:
    """
    Dependency to ensure the current user has admin privileges based on group roles.

    Args:
        session: Database session
        group_context: Group context containing user email and roles

    Returns:
        User object if user has admin privileges

    Raises:
        HTTPException: If user doesn't have admin privileges
    """
    # First ensure user is authenticated
    user = await require_authenticated_user(session, group_context)

    # Check user permissions using two-tier system:
    # 1. System Admin (user-level) - has access to everything
    # 2. Group Admin (group-level) - has access within their groups
    has_admin_privileges = False

    # First check: System Admin permission (user-level)
    if user.is_system_admin:
        has_admin_privileges = True
    else:
        # Second check: Group Admin role (group-level)
        # Use highest_role if available (user has admin in ANY group)
        # Otherwise check user_role for the current selected group
        if hasattr(group_context, "highest_role") and group_context.highest_role:
            has_admin_privileges = group_context.highest_role.lower() == "admin"
        elif group_context.user_role:
            has_admin_privileges = group_context.user_role.lower() == "admin"

    if not has_admin_privileges:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient privileges. Admin role required.",
        )

    return user


async def get_system_admin_user(
    session: SessionDep,
    group_context: GroupContextDep,
) -> User:
    """
    Dependency to ensure the current user is a SYSTEM administrator.

    Unlike get_admin_user, this does NOT accept "admin in any group"
    (highest_role) — it requires the user-level is_system_admin flag. Use this
    for global / cross-tenant operations (e.g. listing every group, system-wide
    stats, app-wide database/Lakebase management) where being an admin of one's
    own workspace must not grant access to other tenants' data.

    Args:
        session: Database session
        group_context: Group context containing user email and roles

    Returns:
        User object if the user is a system administrator

    Raises:
        HTTPException: 403 if the user is not a system administrator
    """
    user = await require_authenticated_user(session, group_context)

    if not getattr(user, "is_system_admin", False):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient privileges. System administrator role required.",
        )

    return user


# Complex privilege system removed - using simplified decorator-based permissions
# Admins are identified by role, permissions checked via @require_roles() decorators


# Type aliases for dependency injection
AuthenticatedUserDep = Annotated[User, Depends(require_authenticated_user)]
GeneralUserDep = Annotated[
    User, Depends(get_authenticated_user)
]  # General auth for any endpoint
AdminUserDep = Annotated[
    User, Depends(get_admin_user)
]  # Admin in ANY group (workspace-level)
SystemAdminUserDep = Annotated[
    User, Depends(get_system_admin_user)
]  # System admin (global)

# Readable alias for use in route-level `dependencies=[Depends(require_system_admin)]`.
require_system_admin = get_system_admin_user
