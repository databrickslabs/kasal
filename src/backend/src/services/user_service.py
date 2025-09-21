from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
import json
import logging

from src.models.user import User
from src.repositories.user_repository import UserRepository
from src.schemas.user import UserUpdate, UserRole, UserPermissionUpdate
# Removed password hash import - using OAuth proxy authentication

logger = logging.getLogger(__name__)

class UserService:
    """Service for user management operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.user_repo = UserRepository(User, session)
        # UserProfileRepository removed - display_name moved to User model
        # External identity repository removed - using simplified auth
    
    async def get_user(self, user_id: str) -> Optional[User]:
        """Get a user by ID"""
        return await self.user_repo.get(user_id)
    
    async def get_users(
        self, skip: int = 0, limit: int = 100, filters: Dict[str, Any] = None, search: Optional[str] = None
    ) -> List[User]:
        """Get a list of users with filtering and search"""
        # Handle search parameter
        if search:
            # Example of a simple search implementation
            # In a real application, you might want a more sophisticated search
            users = []
            
            # Search by username (exact or partial match)
            username_matches = await self.user_repo.list(
                filters={"username": {"$like": f"%{search}%"}},
                skip=skip,
                limit=limit
            )
            users.extend(username_matches)
            
            # Search by email (exact or partial match)
            email_matches = await self.user_repo.list(
                filters={"email": {"$like": f"%{search}%"}},
                skip=skip,
                limit=limit
            )
            users.extend(email_matches)
            
            # Remove duplicates (users found by both username and email)
            unique_users = []
            user_ids = set()
            for user in users:
                if user.id not in user_ids:
                    unique_users.append(user)
                    user_ids.add(user.id)
            
            return unique_users[:limit]
        
        # Regular filtering - use simple list method since list_with_filters doesn't exist
        return await self.user_repo.list(skip=skip, limit=limit)
    
    async def get_user_complete(self, user_id: str) -> Optional[User]:
        """Get a user with complete information"""
        return await self.user_repo.get(user_id)
    
    async def update_user(self, user_id: str, user_update: UserUpdate) -> Optional[User]:
        """Update a user"""
        # Check if user exists
        user = await self.user_repo.get(user_id)
        if not user:
            return None

        # Prepare update data
        update_data = user_update.model_dump(exclude_unset=True, exclude_none=True)

        # Check if username is being updated and is unique
        if "username" in update_data:
            existing_user = await self.user_repo.get_by_username(update_data["username"])
            if existing_user and existing_user.id != user_id:
                raise ValueError("Username already taken")

        # Check if email is being updated and is unique
        if "email" in update_data:
            existing_user = await self.user_repo.get_by_email(update_data["email"])
            if existing_user and existing_user.id != user_id:
                raise ValueError("Email already registered")

        # Update user
        await self.user_repo.update(user_id, update_data)

        # Return updated user
        return await self.user_repo.get(user_id)
    
    async def update_user_permissions(self, user_id: str, permission_update: UserPermissionUpdate) -> Optional[User]:
        """Update user permissions (system admin only)"""
        # Check if user exists
        user = await self.user_repo.get(user_id)
        if not user:
            return None

        # Prepare update data
        update_data = permission_update.model_dump(exclude_unset=True, exclude_none=True)

        # Update user permissions
        await self.user_repo.update(user_id, update_data)

        # Return updated user
        return await self.user_repo.get(user_id)

    # update_user_profile removed - display_name is now part of User model
    
    # Password update removed - using OAuth proxy authentication only
    
    async def assign_role(self, user_id: str, role: str) -> Optional[User]:
        """Assign a role to a user"""
        user = await self.user_repo.get(user_id)
        if not user:
            return None

        # Update user's role
        await self.user_repo.update(user_id, {"role": role})

        # Return updated user
        return await self.user_repo.get(user_id)

    async def get_or_create_user_by_email(self, email: str, update_login: bool = False) -> Optional[User]:
        """
        Get or create a user by email address.
        This is used for proxy-based authentication where users are auto-created.

        Args:
            email: User's email address
            update_login: Whether to update the last_login timestamp (default: False to prevent locking)

        Returns:
            User: The existing or newly created user
        """
        logger.info(f"get_or_create_user_by_email called for email: {email}")

        # Use try-catch to handle race conditions where user might be created between check and create
        try:
            # Check if user exists
            user = await self.user_repo.get_by_email(email)

            if user:
                # User exists, optionally update last login (disabled by default to prevent SQLite locking)
                if update_login:
                    try:
                        await self.user_repo.update_last_login(user.id)
                    except Exception as e:
                        # Log but don't fail on last_login update errors
                        logger.warning(f"Failed to update last_login for {email}: {e}")
                logger.debug(f"Existing user found: {email}")

                # Check if this existing user should be granted admin privileges (if no system admins exist)
                await self._handle_first_user_admin_setup(user, is_new_user=False)
                return user
            # Create new user (OAuth proxy authentication - no password needed)

            # Generate username from email (replace invalid characters)
            username_base = email.split("@")[0]
            # Replace dots and other invalid characters with underscores
            username_base = username_base.replace(".", "_").replace("-", "_")
            # Remove any other invalid characters (keep only letters, numbers, underscores)
            import re
            username_base = re.sub(r'[^a-zA-Z0-9_]', '_', username_base)

            username = username_base
            i = 1

            # Ensure unique username
            while await self.user_repo.get_by_username(username):
                username = f"{username_base}{i}"
                i += 1

            # Create user with regular role initially (no password needed for OAuth proxy)
            from src.schemas.user import UserRole
            user_data = {
                "username": username,
                "email": email,
                "display_name": username,  # Set display_name directly
                "role": UserRole.REGULAR
            }

            try:
                user = await self.user_repo.create(user_data)
                # No separate profile creation needed - display_name is now part of User

                logger.info(f"Created new user via proxy auth: {email}")

                # Check if this is the first user and needs admin setup
                await self._handle_first_user_admin_setup(user, is_new_user=True)
                return user

            except Exception as create_error:
                # Handle race condition where user was created between our check and create attempt
                if "UNIQUE constraint failed" in str(create_error) or "unique constraint" in str(create_error).lower():
                    logger.warning(f"Race condition detected: User {email} was created by another request. Fetching existing user.")
                    # Fetch the user that was created by the other request
                    existing_user = await self.user_repo.get_by_email(email)
                    if existing_user:
                        logger.info(f"Successfully retrieved user created by concurrent request: {email}")
                        # Still check for admin setup since this might be needed
                        await self._handle_first_user_admin_setup(existing_user, is_new_user=False)
                        return existing_user
                    else:
                        logger.error(f"UNIQUE constraint error but user {email} still not found after race condition")
                        raise

                # Re-raise other errors
                logger.error(f"Error creating user {email}: {create_error}")
                raise

        except Exception as e:
            logger.error(f"Error in get_or_create_user_by_email for {email}: {e}")
            raise

    async def _handle_first_user_admin_setup(self, user: User, is_new_user: bool = False) -> None:
        """
        Check if this is the first user in the system.
        If so, grant them system admin permissions.

        Args:
            user: The user to potentially grant admin privileges to
            is_new_user: Whether this user was just created (vs existing user login)
        """
        logger.info(f"_handle_first_user_admin_setup called for user {user.email}, is_new_user={is_new_user}")
        try:
            # If this is an existing user, check if they already have admin privileges
            if not is_new_user:
                if user.is_system_admin:
                    logger.debug(f"User {user.email} already has system admin privileges")
                    return

                # Check if any system admins exist
                from sqlalchemy import select, func
                query = select(func.count(self.user_repo.model.id)).where(
                    self.user_repo.model.is_system_admin == True
                )
                result = await self.session.execute(query)
                admin_count = result.scalar() or 0

                if admin_count == 0:
                    logger.info(f"No system admins exist. Granting system admin privileges to existing user {user.email}")

                    # Grant system admin permission
                    await self.user_repo.update(user.id, {
                        "is_system_admin": True,
                        "is_personal_workspace_manager": True  # System admins also get personal workspace access
                    })

                    logger.info(f"Granted system admin privileges to existing user {user.email}")
                return

            # For new users, check if this is the first user in the system
            total_users = await self.user_repo.count()

            # If this is the only user (count = 1 after creation), make them system admin
            if total_users == 1:
                logger.info(f"First user in system detected. Granting system admin privileges to {user.email}")

                # Grant system admin permission
                await self.user_repo.update(user.id, {
                    "is_system_admin": True,
                    "is_personal_workspace_manager": True  # System admins also get personal workspace access
                })

                logger.info(f"Granted system admin privileges to {user.email}")

        except Exception as e:
            # Log the error but don't fail user creation
            logger.error(f"Error during first user admin setup: {e}")
            # Don't raise the exception - allow user creation to continue
    
    async def delete_user(self, user_id: str) -> bool:
        """Delete a user"""
        user = await self.user_repo.get(user_id)
        if not user:
            return False

        # First, remove user from all groups (to avoid foreign key constraint violations)
        from src.services.group_service import GroupService
        group_service = GroupService(self.session)

        # Get all groups the user belongs to
        user_groups = await group_service.get_user_groups(user_id)

        # Remove user from each group
        for group in user_groups:
            await group_service.remove_user_from_group(group.id, user_id)

        # Now delete the user
        await self.user_repo.delete(user_id)

        return True
    
    # External identity methods removed - using simplified auth system 