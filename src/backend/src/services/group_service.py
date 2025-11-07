"""
Group service for managing multi-group isolation.

This service handles automatic group creation and user management
for the simple multi-group foundation that can later evolve into
Unity Catalog integration.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.group import Group, GroupUser
from src.models.enums import GroupStatus, GroupUserRole, GroupUserStatus, UserRole, UserStatus
from src.models.user import User
from src.repositories.group_repository import GroupRepository, GroupUserRepository
from src.utils.user_context import GroupContext  # Will be updated from TenantContext
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().system


class GroupService:
    """Service for managing groups and group users."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.group_repo = GroupRepository(session)
        self.group_user_repo = GroupUserRepository(session)
    
    async def ensure_group_exists(self, group_context) -> Optional[Group]:
        """
        Ensure group exists, creating it automatically if needed.
        
        This is the core auto-group creation logic for Databricks Apps deployment.
        When a user accesses the system, we automatically create their group if needed.
        
        Args:
            group_context: Context with group_id

        Returns:
            Group: The existing or newly created group
        """
        # Support both GroupContext and legacy TenantContext during migration
        primary_group_id = getattr(group_context, 'primary_group_id', None) or getattr(group_context, 'primary_tenant_id', None)
        group_email = getattr(group_context, 'group_email', None) or getattr(group_context, 'tenant_email', None)

        if not primary_group_id:
            logger.warning("Cannot create group: missing primary_group_id")
            return None

        # Check if group already exists
        group = await self.group_repo.get(primary_group_id)

        if group:
            logger.debug(f"Group {primary_group_id} already exists")
            return group

        # Generate a name based on the group_id
        if primary_group_id.startswith("user_"):
            # Personal workspace - use email-based name
            name = f"Personal Workspace - {group_email}" if group_email else "Personal Workspace"
        else:
            # Regular group - clean up the group_id for display
            name = primary_group_id.replace("_", " ").title()

        # Auto-create group
        group = Group(
            id=primary_group_id,
            name=name,
            status=GroupStatus.ACTIVE,
            description=f"Auto-created group",
            auto_created=True,
            created_by_email=group_email,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )

        group = await self.group_repo.add(group)

        logger.info(f"Auto-created group {primary_group_id}")
        return group
    
    async def ensure_group_user_exists(
        self, 
        group_context, 
        user_id: str
    ) -> Optional[GroupUser]:
        """
        Ensure group user association exists, creating it automatically if needed.
        
        Args:
            group_context: Context with group information
            user_id: User ID to associate with group
            
        Returns:
            GroupUser: The existing or newly created group user association
        """
        # Support both GroupContext and legacy TenantContext during migration
        primary_group_id = getattr(group_context, 'primary_group_id', None) or getattr(group_context, 'primary_tenant_id', None)
        
        if not primary_group_id:
            return None
        
        # Check if group user already exists
        group_user = await self.group_user_repo.get_by_group_and_user(primary_group_id, user_id)
        
        if group_user:
            return group_user
        
        # Auto-create group user association
        group_user = GroupUser(
            id=f"{primary_group_id}_{user_id}",
            group_id=primary_group_id,
            user_id=user_id,
            role=GroupUserRole.OPERATOR,  # Default role
            status=GroupUserStatus.ACTIVE,
            joined_at=datetime.utcnow(),
            auto_created=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        group_user = await self.group_user_repo.add(group_user)
        
        logger.info(f"Auto-created group user association for {user_id} in group {primary_group_id}")
        return group_user
    
    async def get_user_groups(self, user_id: str) -> List[Group]:
        """
        Get all groups a user belongs to.

        Args:
            user_id: User ID to look up

        Returns:
            List[Group]: List of groups the user belongs to
        """
        group_users = await self.group_user_repo.get_groups_by_user(user_id)
        return [gu.group for gu in group_users if gu.status == GroupUserStatus.ACTIVE and gu.group.status == GroupStatus.ACTIVE]

    async def get_user_groups_with_roles(self, user_id: str) -> List[tuple]:
        """
        Get all groups a user belongs to along with their role in each group.

        Args:
            user_id: User ID to look up

        Returns:
            List[tuple]: List of tuples containing (group, role)
        """
        group_users = await self.group_user_repo.get_groups_by_user(user_id)
        return [(gu.group, gu.role) for gu in group_users if gu.status == GroupUserStatus.ACTIVE and gu.group.status == GroupStatus.ACTIVE]
    
    async def get_user_group_memberships(self, email: str) -> List[Group]:
        """
        Get all groups a user belongs to by email address.
        
        Args:
            email: User email address to look up
            
        Returns:
            List[Group]: List of groups the user belongs to
        """
        # First get the user
        from sqlalchemy import select
        user_stmt = select(User).where(User.email == email)
        result = await self.session.execute(user_stmt)
        user = result.scalar_one_or_none()
        
        if not user:
            return []
        
        return await self.get_user_groups(user.id)
    
    async def create_group(
        self,
        name: str,
        description: str = None,
        created_by_email: str = None
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
            updated_at=datetime.utcnow()
        )

        group = await self.group_repo.add(group)

        logger.info(f"Created group {group_id} manually")
        return group
    
    async def list_groups(self, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List all groups with user counts.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List[Dict]: List of groups with user counts
        """
        return await self.group_repo.list_with_user_counts(skip, limit)
    
    async def get_group_by_id(self, group_id: str) -> Optional[Group]:
        """
        Get group by ID.
        
        Args:
            group_id: Group ID
            
        Returns:
            Group: Group if found, None otherwise
        """
        return await self.group_repo.get(group_id)
    
    async def update_group(
        self,
        group_id: str,
        **updates
    ) -> Group:
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
        return await self.group_repo.update(group)
    
    async def get_group_user_count(self, group_id: str) -> int:
        """
        Get count of users in a group.
        
        Args:
            group_id: Group ID
            
        Returns:
            int: Number of users in group
        """
        from sqlalchemy import func, select
        
        stmt = select(func.count(GroupUser.id)).where(
            GroupUser.group_id == group_id,
            GroupUser.status == GroupUserStatus.ACTIVE
        )
        result = await self.session.execute(stmt)
        return result.scalar() or 0
    
    async def list_group_users(
        self, 
        group_id: str, 
        skip: int = 0, 
        limit: int = 100
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
        group_users = await self.group_user_repo.get_users_by_group(group_id, skip, limit)
        
        # Enhanced results with user emails
        enhanced_results = []
        for group_user in group_users:
            email = group_user.user.email if group_user.user else f"{group_user.user_id}@databricks.com"
            
            result = {
                'id': group_user.id,
                'group_id': group_user.group_id,
                'user_id': group_user.user_id,
                'email': email,
                'role': group_user.role,
                'status': group_user.status,
                'joined_at': group_user.joined_at,
                'auto_created': group_user.auto_created,
                'created_at': group_user.created_at,
                'updated_at': group_user.updated_at
            }
            enhanced_results.append(result)
        
        return enhanced_results
    
    async def assign_user_to_group(
        self,
        group_id: str,
        user_email: str,
        role: GroupUserRole = GroupUserRole.OPERATOR,
        assigned_by_email: str = None
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
        user_id = user_email.split('@')[0]
        
        # Ensure User record exists
        from sqlalchemy import select
        user_stmt = select(User).where(User.email == user_email)
        user_result = await self.session.execute(user_stmt)
        user = user_result.scalar_one_or_none()
        
        if not user:
            # Create a basic User record
            from uuid import uuid4
            user = User(
                id=str(uuid4()),
                username=user_id,
                email=user_email,
                hashed_password="",  # No password for group-assigned users
                role=UserRole.REGULAR,
                status=UserStatus.ACTIVE,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            self.session.add(user)
            await self.session.flush()  # Get the ID without committing
        
        # Use the actual user ID
        actual_user_id = user.id
        
        # Check if association already exists
        group_user = await self.group_user_repo.get_by_group_and_user(group_id, actual_user_id)
        
        if group_user:
            # Update existing association
            update_data = {
                'role': role,
                'status': GroupUserStatus.ACTIVE,
                'updated_at': datetime.utcnow()
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
                updated_at=datetime.utcnow()
            )
            group_user = await self.group_user_repo.add(group_user)
        
        logger.info(f"Assigned user {user_email} to group {group_id} with role {role}")
        
        return {
            'id': group_user.id,
            'group_id': group_user.group_id,
            'user_id': group_user.user_id,
            'email': user_email,
            'role': group_user.role,
            'status': group_user.status,
            'joined_at': group_user.joined_at,
            'auto_created': group_user.auto_created,
            'created_at': group_user.created_at,
            'updated_at': group_user.updated_at
        }
    
    async def update_group_user(
        self,
        group_id: str,
        user_id: str,
        **updates
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

        update_data['updated_at'] = datetime.utcnow()
        return await self.group_user_repo.update(group_user.id, update_data)
    
    async def remove_user_from_group(
        self,
        group_id: str,
        user_id: str
    ):
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


    async def get_total_group_count(self) -> int:
        """
        Get the total count of groups in the system.

        Returns:
            int: Total number of groups
        """
        stats = await self.group_repo.get_stats()
        return stats.get('total_groups', 0)

    async def create_first_admin_group_for_user(self, user) -> tuple[Group, GroupUser]:
        """
        Create the first admin group and assign the user as admin.
        This is called when the first user logs in and no groups exist.

        Args:
            user: The first user logging in

        Returns:
            tuple: (created_group, group_user_membership)
        """
        # Create the first admin group
        group_data = {
            "name": "Admin Group",
            "description": "First admin group - automatically created",
            "created_by_email": user.email
        }

        # Generate unique ID for the group using email username part
        username_part = user.email.split('@')[0] if '@' in user.email else 'admin'
        group_id = f"admin_group_{username_part.replace('.', '_').replace('-', '_')}"

        group = Group(
            id=group_id,
            name=group_data["name"],
            status=GroupStatus.ACTIVE,
            description=group_data["description"],
            auto_created=True,
            created_by_email=group_data["created_by_email"],
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )

        # Use repository to add the group
        group = await self.group_repo.add(group)

        # Create group user membership with ADMIN role
        group_user = GroupUser(
            id=f"{group.id}_{user.id}",
            group_id=group.id,
            user_id=user.id,
            role=GroupUserRole.ADMIN,  # Set as ADMIN role
            status=GroupUserStatus.ACTIVE,
            joined_at=datetime.utcnow(),
            auto_created=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )

        # Use repository to add the group user
        group_user = await self.group_user_repo.add(group_user)

        logger.info(f"Created first admin group {group.id} and assigned user {user.email} as ADMIN")

        return group, group_user

    async def get_user_group_membership(self, user_id: str, group_id: str) -> Optional[GroupUser]:
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