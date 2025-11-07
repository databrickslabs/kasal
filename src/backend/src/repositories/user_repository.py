from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from sqlalchemy import select, update, delete, and_, or_, func
from src.models.user import User, RefreshToken
from src.core.base_repository import BaseRepository


class UserRepository(BaseRepository[User]):
    """Repository for User model"""

    async def get_by_email(self, email: str) -> Optional[User]:
        """Get a user by email"""
        query = select(self.model).where(self.model.email == email)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def get_by_username(self, username: str) -> Optional[User]:
        """Get a user by username"""
        query = select(self.model).where(self.model.username == username)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def update_last_login(self, user_id: str) -> None:
        """Update user's last login timestamp"""
        query = update(self.model).where(self.model.id == user_id).values(last_login=datetime.utcnow())
        await self.session.execute(query)

    async def search_users(self, search_term: str, limit: int = 10) -> List[User]:
        """Search users by email or username"""
        query = select(self.model).where(
            or_(
                self.model.email.ilike(f"%{search_term}%"),
                self.model.username.ilike(f"%{search_term}%")
            )
        ).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def count(self) -> int:
        """Get total count of users"""
        query = select(func.count(self.model.id))
        result = await self.session.execute(query)
        return result.scalar() or 0


# UserProfileRepository removed - display_name moved to User model


class RefreshTokenRepository(BaseRepository[RefreshToken]):
    """Repository for RefreshToken model"""

    async def get_by_token(self, token: str) -> Optional[RefreshToken]:
        """Get a refresh token by token value"""
        query = select(self.model).where(self.model.token == token)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def get_by_user_id(self, user_id: str) -> List[RefreshToken]:
        """Get all refresh tokens for a user"""
        query = select(self.model).where(self.model.user_id == user_id)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def revoke_token(self, token: str) -> None:
        """Revoke a refresh token"""
        query = update(self.model).where(self.model.token == token).values(is_revoked=True)
        await self.session.execute(query)

    async def revoke_all_user_tokens(self, user_id: str) -> None:
        """Revoke all refresh tokens for a user"""
        query = update(self.model).where(self.model.user_id == user_id).values(is_revoked=True)
        await self.session.execute(query)

    async def delete_expired_tokens(self) -> None:
        """Delete expired refresh tokens"""
        query = delete(self.model).where(self.model.expires_at < datetime.utcnow())
        await self.session.execute(query)


# Legacy compatibility - maintain old names for backward compatibility during migration
ExternalIdentityRepository = None  # Removed - using simplified auth
RoleRepository = None  # Removed - using simplified group-based roles
PrivilegeRepository = None  # Removed - using simplified group-based roles
RolePrivilegeRepository = None  # Removed - using simplified group-based roles
UserRoleRepository = None  # Removed - using simplified group-based roles
IdentityProviderRepository = None  # Removed - using simplified auth
UserProfileRepository = None  # Removed - display_name moved to User model