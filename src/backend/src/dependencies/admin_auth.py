"""
Authentication and authorization dependencies for admin-only endpoints.
"""
import os
from typing import Annotated, Optional
from fastapi import Depends, HTTPException, status, Header
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dependencies import SessionDep, GroupContextDep
from src.repositories.user_repository import UserRepository
from src.models.user import User
from src.models.enums import UserRole, UserStatus
from src.utils.user_context import GroupContext


async def _create_user_from_forwarded_email(session: AsyncSession, email: str) -> Optional[User]:
    """
    Create a user from X-Forwarded-Email header and track the source.
    
    This function works in both development and production modes.
    
    Args:
        session: Database session
        email: User email from X-Forwarded-Email header
        
    Returns:
        Created User object or None
    """
    from sqlalchemy import select
    from datetime import datetime
    import re
    import logging

    logger = logging.getLogger(__name__)
    is_local_dev = os.getenv("ENVIRONMENT", "development").lower() in ("development", "dev", "local")

    try:
        # Check if user already exists
        result = await session.execute(
            select(User).filter(User.email == email)
        )
        existing_user = result.scalars().first()

        if existing_user:
            logger.info(f"User {email} already exists from X-Forwarded-Email")
            # Update last login
            existing_user.last_login = datetime.utcnow()
            await session.commit()
            return existing_user

        # Extract username from email and sanitize it
        base_username = email.split("@")[0]
        # Replace invalid characters with underscores (only allow letters, numbers, underscores, hyphens)
        sanitized_username = re.sub(r'[^a-zA-Z0-9_-]', '_', base_username)
        username = sanitized_username

        # Check if username already exists and make it unique
        result = await session.execute(
            select(User).filter(User.username == username)
        )
        existing_username = result.scalars().first()

        if existing_username:
            # Create unique username by appending part of email domain
            domain_part = re.sub(r'[^a-zA-Z0-9_-]', '_', email.split("@")[1].split(".")[0])
            username = f"{sanitized_username}_{domain_part}"
            logger.info(f"Username {sanitized_username} exists, using {username}")

        # Determine user role based on configuration
        default_role = UserRole.REGULAR  # Default for production
        if is_local_dev:
            # In development, check if this is a known admin email
            admin_emails = os.getenv("ADMIN_EMAILS", "").split(",")
            admin_patterns = ["admin@localhost", "admin@", "testadmin@"]

            if (email in admin_emails or
                any(pattern in email for pattern in admin_patterns)):
                default_role = UserRole.ADMIN
                logger.info(f"Assigning admin role to {email} in development")

        # Create user
        user = User(
            username=username,
            email=email,
            hashed_password="auto_generated_from_forwarded_email",  # Placeholder password
            role=default_role,
            status=UserStatus.ACTIVE
        )

        session.add(user)
        await session.commit()
        await session.refresh(user)

        logger.info(f"Successfully created user {email} from X-Forwarded-Email with username {username}")
        return user

    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to create user from X-Forwarded-Email {email}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


async def get_current_user_from_email(
    session: SessionDep,
    group_context: GroupContextDep
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
    logger.info(f"[AUTH DEBUG] get_current_user_from_email called with email: {group_context.group_email if group_context.group_email else 'None'}")

    if not group_context.group_email:
        logger.info("[AUTH DEBUG] No group email found in context")
        return None

    # Use UserService instead of UserRepository to trigger first user setup logic
    from src.services.user_service import UserService
    logger.info(f"[AUTH DEBUG] Creating UserService and calling get_or_create_user_by_email for {group_context.group_email}")
    user_service = UserService(session)
    user = await user_service.get_or_create_user_by_email(group_context.group_email)
    logger.info(f"[AUTH DEBUG] get_or_create_user_by_email returned user: {user.email if user else 'None'}")

    return user


async def require_authenticated_user(
    session: SessionDep,
    group_context: GroupContextDep
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
    logger.info(f"[AUTH DEBUG] require_authenticated_user called with email: {group_context.group_email if group_context.group_email else 'None'}")

    if not group_context.group_email:
        logger.info("[AUTH DEBUG] No group email in require_authenticated_user - returning 401")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. X-Forwarded-Email header not found."
        )

    logger.info(f"[AUTH DEBUG] About to call get_current_user_from_email in require_authenticated_user")
    user = await get_current_user_from_email(session, group_context)
    
    if not user:
        # Auto-create user from X-Forwarded-Email header with source tracking
        user = await _create_user_from_forwarded_email(session, group_context.group_email)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"User with email {group_context.group_email} could not be created or found."
            )
    
    return user


async def get_authenticated_user(
    session: SessionDep,
    group_context: GroupContextDep
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


async def get_admin_user(
    session: SessionDep,
    group_context: GroupContextDep
) -> User:
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
        if hasattr(group_context, 'highest_role') and group_context.highest_role:
            has_admin_privileges = group_context.highest_role.lower() == "admin"
        elif group_context.user_role:
            has_admin_privileges = group_context.user_role.lower() == "admin"

    if not has_admin_privileges:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient privileges. Admin role required."
        )

    return user


# Complex privilege system removed - using simplified decorator-based permissions
# Admins are identified by role, permissions checked via @require_roles() decorators


# Type aliases for dependency injection
AuthenticatedUserDep = Annotated[User, Depends(require_authenticated_user)]
GeneralUserDep = Annotated[User, Depends(get_authenticated_user)]  # General auth for any endpoint
AdminUserDep = Annotated[User, Depends(get_admin_user)]