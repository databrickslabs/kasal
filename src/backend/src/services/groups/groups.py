"""
Group service for managing multi-group isolation.

This service handles automatic group creation and user management
for the simple multi-group foundation that can later evolve into
Unity Catalog integration.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.models.enums import (
    GroupStatus,
    GroupUserRole,
    GroupUserStatus,
    UserRole,
    UserStatus,
)
from src.models.group import Group, GroupUser
from src.models.user import User
from src.repositories.group_repository import GroupRepository, GroupUserRepository

logger = LoggerManager.get_instance().system


class GroupService:
    """Service for managing groups and group users."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.group_repo = GroupRepository(session)
        self.group_user_repo = GroupUserRepository(session)
        # User lookups belong to the user repository, not to a raw select here.
        from src.models.user import User as _User
        from src.repositories.user_repository import UserRepository

        self.user_repo = UserRepository(_User, session)

    async def get_user_groups(self, user_id: str) -> List[Group]:
        """
        Get all groups a user belongs to.

        Args:
            user_id: User ID to look up

        Returns:
            List[Group]: List of groups the user belongs to
        """
        group_users = await self.group_user_repo.get_groups_by_user(user_id)
        return [
            gu.group
            for gu in group_users
            if gu.status == GroupUserStatus.ACTIVE
            and gu.group.status == GroupStatus.ACTIVE
        ]

    async def get_user_memberships(self, user_id: str) -> List["GroupUser"]:
        """Active memberships (full rows, ``.group`` loaded) for a user.

        The my-groups endpoint reads role AND the per-surface capability
        overrides off each row; the (group, role) tuple shape of
        ``get_user_groups_with_roles`` stays untouched for its other callers.
        """
        group_users = await self.group_user_repo.get_groups_by_user(user_id)
        return [
            gu
            for gu in group_users
            if gu.status == GroupUserStatus.ACTIVE
            and gu.group.status == GroupStatus.ACTIVE
        ]

    async def get_user_groups_with_roles(self, user_id: str) -> List[tuple]:
        """
        Get all groups a user belongs to along with their role in each group.

        Args:
            user_id: User ID to look up

        Returns:
            List[tuple]: List of tuples containing (group, role)
        """
        group_users = await self.group_user_repo.get_groups_by_user(user_id)
        return [
            (gu.group, gu.role)
            for gu in group_users
            if gu.status == GroupUserStatus.ACTIVE
            and gu.group.status == GroupStatus.ACTIVE
        ]

    async def create_group(
        self,
        name: str,
        description: Optional[str] = None,
        created_by_email: Optional[str] = None,
    ) -> Group:
        """
        Create a new group manually.

        Args:
            name: Human-readable group name
            description: Optional description
            created_by_email: Email of creator

        Returns:
            Group: Created group
        """
        # Generate unique group ID from name
        group_id = Group.generate_group_id(name)

        # Create new group (no need to check for duplicates since ID is always unique)
        group = Group(
            id=group_id,
            name=name,
            status=GroupStatus.ACTIVE,
            description=description,
            auto_created=False,
            created_by_email=created_by_email,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )

        group = await self.group_repo.add(group)

        logger.info(f"Created group {group_id} manually")
        return group

    async def list_groups(
        self, skip: int = 0, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List all groups with user counts.

        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List[Dict]: List of groups with user counts
        """
        return await self.group_repo.list_with_user_counts(skip, limit)

    async def get_group_name(self, group_id: str) -> Optional[str]:
        """The group's display name, or None.

        Just the name — mlflow needs it to label an experiment and used to build
        ``GroupRepository`` itself rather than pull the whole row through a service.
        """
        return await self.group_repo.get_name(group_id)

    async def get_group_by_id(self, group_id: str) -> Optional[Group]:
        """
        Get group by ID.

        Args:
            group_id: Group ID

        Returns:
            Group: Group if found, None otherwise
        """
        return await self.group_repo.get(group_id)

    async def update_group(self, group_id: str, **updates: Any) -> Group:
        """
        Update a group.

        Args:
            group_id: Group ID to update
            **updates: Fields to update

        Returns:
            Group: Updated group
        """
        group = await self.group_repo.get(group_id)

        if not group:
            raise ValueError(f"Group {group_id} not found")

        # Update fields
        for field, value in updates.items():
            if hasattr(group, field):
                setattr(group, field, value)

        group.updated_at = datetime.utcnow()
        # The row is already loaded and modified in this session: flush it.
        # (`update(id, values)` takes a key and a dict, not the object.)
        return await self.group_repo.add(group)

    async def get_group_user_count(self, group_id: str) -> int:
        """
        Get count of users in a group.

        Args:
            group_id: Group ID

        Returns:
            int: Number of users in group
        """
        return await self.group_user_repo.count_active_users(group_id)

    async def list_group_users(
        self, group_id: str, skip: int = 0, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List users in a group.

        Args:
            group_id: Group ID
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List[Dict]: List of group users with user details
        """
        group_users = await self.group_user_repo.get_users_by_group(
            group_id, skip, limit
        )

        # Enhanced results with user emails
        enhanced_results = []
        for group_user in group_users:
            email = (
                group_user.user.email
                if group_user.user
                else f"{group_user.user_id}@databricks.com"
            )

            result = {
                "id": group_user.id,
                "group_id": group_user.group_id,
                "user_id": group_user.user_id,
                "email": email,
                "role": group_user.role,
                "status": group_user.status,
                "allow_agent_builder": getattr(group_user, "allow_agent_builder", None),
                "allow_flow_builder": getattr(group_user, "allow_flow_builder", None),
                "joined_at": group_user.joined_at,
                "auto_created": group_user.auto_created,
                "created_at": group_user.created_at,
                "updated_at": group_user.updated_at,
            }
            enhanced_results.append(result)

        return enhanced_results

    async def assign_user_to_group(
        self,
        group_id: str,
        user_email: str,
        role: GroupUserRole = GroupUserRole.OPERATOR,
        assigned_by_email: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Assign a user to a group manually.

        Args:
            group_id: Group ID
            user_email: User email
            role: Role to assign
            assigned_by_email: Email of admin assigning user

        Returns:
            Dict: Created or updated group user with email
        """
        # Generate user_id from email (simple approach)
        user_id = user_email.split("@")[0]

        # Ensure User record exists
        user = await self.user_repo.get_by_email(user_email)

        if not user:
            # Create a basic User record
            from uuid import uuid4

            # Emails with the same local part can belong to different people.
            # Keep the familiar username when free, otherwise allocate a unique
            # one without changing the email used for authentication.
            username = user_id
            if await self.user_repo.get_by_username(username):
                username = f"{user_id[:16]}_{uuid4().hex}"
            user = User(
                id=str(uuid4()),
                username=username,
                email=user_email,
                personal_group_id=f"user_{uuid4().hex}",
                role=UserRole.REGULAR,
                status=UserStatus.ACTIVE,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            # Through the repository: it owns User persistence, and the flush
            # (not a commit) keeps this inside the caller's transaction so the
            # user row and the group association land together.
            await self.user_repo.insert(user)

        # Use the actual user ID
        actual_user_id = user.id

        # Check if association already exists
        group_user = await self.group_user_repo.get_by_group_and_user(
            group_id, actual_user_id
        )

        if group_user:
            # Update existing association
            update_data = {
                "role": role,
                "status": GroupUserStatus.ACTIVE,
                "updated_at": datetime.utcnow(),
            }
            group_user = await self.group_user_repo.update(group_user.id, update_data)
        else:
            # Create new association
            group_user = GroupUser(
                id=f"{group_id}_{actual_user_id}",
                group_id=group_id,
                user_id=actual_user_id,
                role=role,
                status=GroupUserStatus.ACTIVE,
                joined_at=datetime.utcnow(),
                auto_created=False,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            group_user = await self.group_user_repo.add(group_user)
        if group_user is None:  # the row vanished between the read and the update
            raise ValueError(f"User {actual_user_id} not found in group {group_id}")

        logger.info(f"Assigned user {user_email} to group {group_id} with role {role}")

        # Membership changed — drop cached resolution so the next request
        # re-reads this user's groups/role instead of serving a stale entry.
        from src.utils.user_context import clear_membership_cache

        clear_membership_cache(user_email)

        return {
            "id": group_user.id,
            "group_id": group_user.group_id,
            "user_id": group_user.user_id,
            "email": user_email,
            "role": group_user.role,
            "status": group_user.status,
            "joined_at": group_user.joined_at,
            "auto_created": group_user.auto_created,
            "created_at": group_user.created_at,
            "updated_at": group_user.updated_at,
        }

    async def update_group_user(
        self, group_id: str, user_id: str, **updates: Any
    ) -> GroupUser:
        """
        Update a group user.

        Args:
            group_id: Group ID
            user_id: User ID
            **updates: Fields to update

        Returns:
            GroupUser: Updated group user
        """
        group_user = await self.group_user_repo.get_by_group_and_user(group_id, user_id)

        if not group_user:
            raise ValueError(f"User {user_id} not found in group {group_id}")

        # Update fields
        update_data = {}
        for field, value in updates.items():
            if hasattr(group_user, field):
                update_data[field] = value

        update_data["updated_at"] = datetime.utcnow()
        updated = await self.group_user_repo.update(group_user.id, update_data)
        if updated is None:  # the row vanished between the read and the update
            raise ValueError(f"User {user_id} not found in group {group_id}")

        # Role/status changed — invalidate cached memberships. We only have
        # user_id here, not email, so clear the whole (small, short-TTL) cache.
        from src.utils.user_context import clear_membership_cache

        clear_membership_cache()

        return updated

    async def remove_user_from_group(self, group_id: str, user_id: str) -> None:
        """
        Remove a user from a group.

        Args:
            group_id: Group ID
            user_id: User ID
        """
        success = await self.group_user_repo.remove_user_from_group(group_id, user_id)

        if not success:
            raise ValueError(f"User {user_id} not found in group {group_id}")

        logger.info(f"Removed user {user_id} from group {group_id}")

        # Membership changed — invalidate cached memberships (user_id only,
        # so clear the whole short-TTL cache).
        from src.utils.user_context import clear_membership_cache

        clear_membership_cache()

    async def delete_group(self, group_id: str) -> None:
        """
        Delete a group and all associated data.

        This will remove:
        - The group record
        - All group user associations
        - All related execution history and data

        Args:
            group_id: ID of the group to delete

        Raises:
            ValueError: If group not found or cannot be deleted
        """
        # Check if group exists
        group = await self.group_repo.get(group_id)
        if not group:
            raise ValueError(f"Group {group_id} not found")

        try:
            # Delete the group (cascade will handle group_users)
            await self.group_repo.delete(group_id)

            logger.info(f"Deleted group {group_id} and all associated data")

            # Group (and its memberships) gone — invalidate cached resolutions.
            from src.utils.user_context import clear_membership_cache

            clear_membership_cache()

        except Exception as e:
            logger.error(f"Error deleting group {group_id}: {e}")
            raise ValueError(f"Failed to delete group: {str(e)}")

    async def get_group_stats(self) -> Dict[str, Any]:
        """
        Get group statistics.

        Returns:
            Dict: Statistics about groups and users
        """
        return await self.group_repo.get_stats()

    async def get_user_group_membership(
        self, user_id: str, group_id: str
    ) -> Optional[GroupUser]:
        """
        Get a user's membership in a specific group.

        Args:
            user_id: User ID
            group_id: Group ID

        Returns:
            GroupUser: The user's membership in the group, or None if not a member
        """
        return await self.group_user_repo.get_by_group_and_user(group_id, user_id)


# Legacy compatibility - maintain old names for backward compatibility during migration
TenantService = GroupService
