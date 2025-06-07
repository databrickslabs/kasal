"""
Authentication and authorization dependencies for admin-only endpoints.
"""
import os
from typing import Annotated, Optional
from fastapi import Depends, HTTPException, status, Header
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dependencies import SessionDep, GroupContextDep
from src.services.databricks_role_service import DatabricksRoleService
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
    from src.models.user import Role, UserRole as UserRoleAssignment, ExternalIdentity
    from sqlalchemy import select
    from datetime import datetime
    import uuid
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
        import re
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
        await session.flush()  # Get the user ID
        logger.info(f"Created user {email} from X-Forwarded-Email with username {username}")
        
        # Create external identity to track source
        external_identity = ExternalIdentity(
            id=str(uuid.uuid4()),
            user_id=user.id,
            provider="x-forwarded-email",
            provider_user_id=email,
            email=email,
            profile_data='{"source": "X-Forwarded-Email", "auto_created": true}',
            created_at=datetime.utcnow(),
            last_login=datetime.utcnow()
        )
        session.add(external_identity)
        logger.info(f"Created external identity record for {email} with provider 'x-forwarded-email'")
        
        # If user should be admin, assign admin role via RBAC
        if default_role == UserRole.ADMIN:
            result = await session.execute(
                select(Role).filter(Role.name == 'admin')
            )
            admin_role = result.scalars().first()
            
            if admin_role:
                user_role_assignment = UserRoleAssignment(
                    id=str(uuid.uuid4()),
                    user_id=user.id,
                    role_id=admin_role.id,
                    assigned_at=datetime.utcnow(),
                    assigned_by='auto_assignment_from_forwarded_email'
                )
                session.add(user_role_assignment)
                logger.info(f"Assigned admin role to user {email} via RBAC")
            else:
                logger.warning("Admin role not found in RBAC system for auto-assignment")
        
        await session.commit()
        await session.refresh(user)
        
        logger.info(f"Successfully created and configured user {email} from X-Forwarded-Email")
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
    
    Args:
        session: Database session
        group_context: Group context containing user email
        
    Returns:
        User object if found, None otherwise
    """
    if not group_context.group_email:
        return None
    
    user_repository = UserRepository(User, session)
    user = await user_repository.get_by_email(group_context.group_email)
    
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
    if not group_context.group_email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. X-Forwarded-Email header not found."
        )
    
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
    Dependency to ensure the current user has admin privileges using RBAC.
    
    Args:
        session: Database session
        group_context: Group context containing user email
        
    Returns:
        User object if user has admin privileges
        
    Raises:
        HTTPException: If user doesn't have admin privileges
    """
    # First ensure user is authenticated
    user = await require_authenticated_user(session, group_context)
    
    # Check if user has admin role using RBAC
    databricks_role_service = DatabricksRoleService(session)
    has_admin_role = await databricks_role_service.check_user_admin_access(user.id)
    
    if not has_admin_role:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient privileges. Admin role required."
        )
    
    return user


async def get_admin_user_with_privileges(
    session: SessionDep,
    group_context: GroupContextDep
) -> tuple[User, list[str]]:
    """
    Dependency to get the current admin user and their privileges.
    
    Args:
        session: Database session
        group_context: Group context containing user email
        
    Returns:
        Tuple of (User object, list of privilege names)
        
    Raises:
        HTTPException: If user doesn't have admin privileges
    """
    # First ensure user is authenticated and has admin role
    user = await get_admin_user(session, group_context)
    
    # Get user's privileges
    databricks_role_service = DatabricksRoleService(session)
    privileges = await databricks_role_service.get_user_privileges(user.id)
    
    return user, privileges


# Type aliases for dependency injection
AuthenticatedUserDep = Annotated[User, Depends(require_authenticated_user)]
GeneralUserDep = Annotated[User, Depends(get_authenticated_user)]  # General auth for any endpoint
AdminUserDep = Annotated[User, Depends(get_admin_user)]
AdminUserPrivilegesDep = Annotated[tuple[User, list[str]], Depends(get_admin_user_with_privileges)]