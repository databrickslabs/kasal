from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from sqlalchemy import select, update, delete, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from src.models.user import User, UserProfile, RefreshToken, ExternalIdentity, Role, Privilege, RolePrivilege, IdentityProvider
from src.core.base_repository import BaseRepository


class UserRepository(BaseRepository[User]):
    """Repository for User model"""
    
    async def get_by_username(self, username: str) -> Optional[User]:
        """Get a user by username"""
        query = select(self.model).where(self.model.username == username)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_by_email(self, email: str) -> Optional[User]:
        """Get a user by email"""
        query = select(self.model).where(self.model.email == email)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_by_username_or_email(self, username_or_email: str) -> Optional[User]:
        """Get a user by username or email"""
        query = select(self.model).where(
            or_(
                self.model.username == username_or_email,
                self.model.email == username_or_email
            )
        )
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def update_last_login(self, user_id: str) -> None:
        """Update user's last login timestamp"""
        query = update(self.model).where(self.model.id == user_id).values(last_login=datetime.utcnow())
        await self.session.execute(query)


class UserProfileRepository(BaseRepository[UserProfile]):
    """Repository for UserProfile model"""
    
    async def get_by_user_id(self, user_id: str) -> Optional[UserProfile]:
        """Get profile by user_id"""
        query = select(self.model).where(self.model.user_id == user_id)
        result = await self.session.execute(query)
        return result.scalars().first()


class RefreshTokenRepository(BaseRepository[RefreshToken]):
    """Repository for RefreshToken model"""
    
    async def get_by_token(self, token: str) -> Optional[RefreshToken]:
        """Get a refresh token by token value"""
        query = select(self.model).where(self.model.token == token)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_valid_token(self, token: str, current_time: datetime) -> Optional[RefreshToken]:
        """Get a valid refresh token"""
        query = select(self.model).where(
            and_(
                self.model.token == token,
                self.model.expires_at > current_time,
                self.model.is_revoked == False
            )
        )
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def revoke_token(self, token: str) -> None:
        """Revoke a refresh token"""
        query = update(self.model).where(self.model.token == token).values(is_revoked=True)
        await self.session.execute(query)
    
    async def revoke_all_for_user(self, user_id: str) -> None:
        """Revoke all refresh tokens for a user"""
        query = update(self.model).where(self.model.user_id == user_id).values(is_revoked=True)
        await self.session.execute(query)


class ExternalIdentityRepository(BaseRepository[ExternalIdentity]):
    """Repository for ExternalIdentity model"""
    
    async def get_by_provider_and_id(self, provider: str, provider_user_id: str) -> Optional[ExternalIdentity]:
        """Get external identity by provider and provider_user_id"""
        query = select(self.model).where(
            and_(
                self.model.provider == provider,
                self.model.provider_user_id == provider_user_id
            )
        )
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_by_user_id_and_provider(self, user_id: str, provider: str) -> Optional[ExternalIdentity]:
        """Get external identity by user_id and provider"""
        query = select(self.model).where(
            and_(
                self.model.user_id == user_id,
                self.model.provider == provider
            )
        )
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_all_by_user_id(self, user_id: str) -> List[ExternalIdentity]:
        """Get all external identities for a user"""
        query = select(self.model).where(self.model.user_id == user_id)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def update_last_login(self, identity_id: str) -> None:
        """Update external identity's last login timestamp"""
        query = update(self.model).where(self.model.id == identity_id).values(last_login=datetime.utcnow())
        await self.session.execute(query)


class RoleRepository(BaseRepository[Role]):
    """Repository for Role model"""
    
    async def get_by_name(self, name: str) -> Optional[Role]:
        """Get a role by name"""
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_with_privileges(self, role_id: str) -> Optional[Role]:
        """Get a role with its privileges"""
        query = select(self.model).where(self.model.id == role_id)
        result = await self.session.execute(query)
        role = result.scalars().first()
        
        if role:
            # Load the privileges
            privilege_query = select(Privilege).join(
                RolePrivilege, RolePrivilege.privilege_id == Privilege.id
            ).where(RolePrivilege.role_id == role_id)
            privilege_result = await self.session.execute(privilege_query)
            role.privileges = list(privilege_result.scalars().all())
            
        return role


class PrivilegeRepository(BaseRepository[Privilege]):
    """Repository for Privilege model"""
    
    async def get_by_name(self, name: str) -> Optional[Privilege]:
        """Get a privilege by name"""
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_by_names(self, names: List[str]) -> List[Privilege]:
        """Get privileges by names"""
        query = select(self.model).where(self.model.name.in_(names))
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_all_privileges(self) -> List[Privilege]:
        """Get all privileges"""
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())


class RolePrivilegeRepository(BaseRepository[RolePrivilege]):
    """Repository for RolePrivilege model"""
    
    async def get_by_role_and_privilege(self, role_id: str, privilege_id: str) -> Optional[RolePrivilege]:
        """Get a role-privilege mapping by role and privilege IDs"""
        query = select(self.model).where(
            and_(
                self.model.role_id == role_id,
                self.model.privilege_id == privilege_id
            )
        )
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def delete_by_role_id(self, role_id: str) -> None:
        """Delete all role-privilege mappings for a role"""
        query = delete(self.model).where(self.model.role_id == role_id)
        await self.session.execute(query)


class IdentityProviderRepository(BaseRepository[IdentityProvider]):
    """Repository for IdentityProvider model"""
    
    async def get_by_name(self, name: str) -> Optional[IdentityProvider]:
        """Get identity provider by name"""
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def get_enabled_providers(self) -> List[IdentityProvider]:
        """Get all enabled identity providers"""
        query = select(self.model).where(self.model.enabled == True)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_default_provider(self) -> Optional[IdentityProvider]:
        """Get the default identity provider"""
        query = select(self.model).where(self.model.is_default == True)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def set_as_default(self, provider_id: str) -> None:
        """Set an identity provider as the default"""
        # First, clear default from all providers
        clear_default = update(self.model).values(is_default=False)
        await self.session.execute(clear_default)
        
        # Then set the new default
        set_default = update(self.model).where(self.model.id == provider_id).values(is_default=True)
        await self.session.execute(set_default) 