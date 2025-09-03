"""
Repository for Group and GroupUser models.

Handles database operations for group management and user membership.
"""
from typing import List, Optional, Dict, Any
from sqlalchemy import select, update, delete, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.models.group import Group, GroupUser
from src.models.user import User
from src.core.base_repository import BaseRepository


class GroupRepository(BaseRepository[Group]):
    """Repository for Group model operations"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(Group, session)
    
    async def get_by_email_domain(self, email_domain: str) -> Optional[Group]:
        """Get a group by email domain"""
        query = select(self.model).where(self.model.email_domain == email_domain)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_with_users(self, group_id: str) -> Optional[Group]:
        """Get a group with its users loaded"""
        query = select(self.model).options(
            selectinload(self.model.group_users).selectinload(GroupUser.user)
        ).where(self.model.id == group_id)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def list_with_user_counts(self, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Get groups with user counts"""
        query = select(
            self.model,
            func.count(GroupUser.id).label('user_count')
        ).outerjoin(GroupUser).group_by(self.model.id).offset(skip).limit(limit)
        
        result = await self.session.execute(query)
        groups_with_counts = []
        
        for group, user_count in result:
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
            groups_with_counts.append(group_dict)
        
        return groups_with_counts
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get group statistics"""
        # Total groups
        total_query = select(func.count(self.model.id))
        total_result = await self.session.execute(total_query)
        total_groups = total_result.scalar()
        
        # Active groups
        active_query = select(func.count(self.model.id)).where(self.model.status == "ACTIVE")
        active_result = await self.session.execute(active_query)
        active_groups = active_result.scalar()
        
        # Total users across all groups
        users_query = select(func.count(GroupUser.id))
        users_result = await self.session.execute(users_query)
        total_users = users_result.scalar()
        
        # Groups by status
        status_query = select(self.model.status, func.count(self.model.id)).group_by(self.model.status)
        status_result = await self.session.execute(status_query)
        groups_by_status = {status: count for status, count in status_result}
        
        return {
            'total_groups': total_groups,
            'active_groups': active_groups,
            'total_users': total_users,
            'groups_by_status': groups_by_status
        }


class GroupUserRepository(BaseRepository[GroupUser]):
    """Repository for GroupUser model operations"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(GroupUser, session)
    
    async def get_by_group_and_user(self, group_id: str, user_id: str) -> Optional[GroupUser]:
        """Get a group user membership by group and user IDs"""
        query = select(self.model).where(
            and_(
                self.model.group_id == group_id,
                self.model.user_id == user_id
            )
        )
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_users_by_group(self, group_id: str, skip: int = 0, limit: int = 100) -> List[GroupUser]:
        """Get all users in a group with user details"""
        query = select(self.model).options(
            selectinload(self.model.user)
        ).where(self.model.group_id == group_id).offset(skip).limit(limit)
        
        result = await self.session.execute(query)
        return result.scalars().all()
    
    async def get_groups_by_user(self, user_id: str) -> List[GroupUser]:
        """Get all groups a user belongs to"""
        query = select(self.model).options(
            selectinload(self.model.group)
        ).where(self.model.user_id == user_id)
        
        result = await self.session.execute(query)
        return result.scalars().all()
    
    async def get_user_emails_by_group(self, group_id: str) -> List[str]:
        """Get all user emails in a group"""
        query = select(User.email).join(
            self.model, User.id == self.model.user_id
        ).where(self.model.group_id == group_id)
        
        result = await self.session.execute(query)
        return [email for email in result.scalars()]
    
    async def remove_user_from_group(self, group_id: str, user_id: str) -> bool:
        """Remove a user from a group"""
        query = delete(self.model).where(
            and_(
                self.model.group_id == group_id,
                self.model.user_id == user_id
            )
        )
        result = await self.session.execute(query)
        await self.session.commit()
        return result.rowcount > 0
    
    async def update_user_role(self, group_id: str, user_id: str, role: str) -> Optional[GroupUser]:
        """Update a user's role in a group"""
        query = update(self.model).where(
            and_(
                self.model.group_id == group_id,
                self.model.user_id == user_id
            )
        ).values(role=role)
        
        await self.session.execute(query)
        await self.session.commit()
        
        # Return updated GroupUser
        return await self.get_by_group_and_user(group_id, user_id)
    
    async def get_user_groups_with_roles(self, user_id: str) -> List[Dict[str, Any]]:
        """Get user's group memberships with roles"""
        query = select(self.model, Group).join(
            Group, self.model.group_id == Group.id
        ).where(self.model.user_id == user_id)
        
        result = await self.session.execute(query)
        memberships = []
        
        for group_user, group in result:
            membership = {
                'group_id': group.id,
                'group_name': group.name,
                'group_email_domain': group.email_domain,
                'role': group_user.role,
                'status': group_user.status,
                'joined_at': group_user.joined_at,
                'auto_created': group_user.auto_created
            }
            memberships.append(membership)
        
        return memberships


# Legacy compatibility - maintain old names for backward compatibility during migration
TenantRepository = GroupRepository
TenantUserRepository = GroupUserRepository