"""Fallback user provisioning for trusted forwarded identities.

The API owns header parsing; this service owns the existing role, username and
transaction behavior when normal identity lookup cannot supply a user.
"""

import os
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.enums import UserRole, UserStatus
from src.models.user import User
from src.repositories.user_repository import UserRepository


async def get_or_create_forwarded_user(
    session: AsyncSession, email: str
) -> Optional[User]:
    """
    Create a user from X-Forwarded-Email header and track the source.

    This function works in both development and production modes.

    Args:
        session: Database session
        email: User email from X-Forwarded-Email header

    Returns:
        Created User object or None
    """
    import logging
    import re
    from datetime import datetime

    logger = logging.getLogger("src.dependencies.admin_auth")
    is_local_dev = os.getenv("ENVIRONMENT", "development").lower() in (
        "development",
        "dev",
        "local",
    )

    try:
        # Check if user already exists
        repository = UserRepository(User, session)
        existing_user = await repository.get_by_email(email)

        if existing_user:
            logger.info(f"User {email} already exists from X-Forwarded-Email")
            # Update last login
            existing_user.last_login = datetime.utcnow()
            await session.commit()
            return existing_user

        # Extract username from email and sanitize it
        base_username = email.split("@")[0]
        # Replace invalid characters with underscores (only allow letters, numbers, underscores, hyphens)
        sanitized_username = re.sub(r"[^a-zA-Z0-9_-]", "_", base_username)
        username = sanitized_username

        # Check if username already exists and make it unique
        existing_username = await repository.get_by_username(username)

        if existing_username:
            # Create unique username by appending part of email domain
            domain_part = re.sub(
                r"[^a-zA-Z0-9_-]", "_", email.split("@")[1].split(".")[0]
            )
            username = f"{sanitized_username}_{domain_part}"
            logger.info(f"Username {sanitized_username} exists, using {username}")

        # Determine user role based on configuration
        default_role = UserRole.REGULAR  # Default for production
        if is_local_dev:
            # In development, check if this is a known admin email
            admin_emails = os.getenv("ADMIN_EMAILS", "").split(",")
            admin_patterns = ["admin@localhost", "admin@", "testadmin@"]

            if email in admin_emails or any(
                pattern in email for pattern in admin_patterns
            ):
                default_role = UserRole.ADMIN
                logger.info(f"Assigning admin role to {email} in development")

        # Create user
        user = User(
            username=username, email=email, role=default_role, status=UserStatus.ACTIVE
        )

        await repository.insert(user)
        await session.commit()
        await session.refresh(user)

        logger.info(
            f"Successfully created user {email} from X-Forwarded-Email with username {username}"
        )
        return user

    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to create user from X-Forwarded-Email {email}: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return None
