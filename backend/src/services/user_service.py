from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
import json

from src.models.user import User, UserProfile, ExternalIdentity
from src.repositories.user_repository import UserRepository, UserProfileRepository, ExternalIdentityRepository
from src.schemas.user import UserUpdate, UserProfileUpdate, UserRole
from src.services.auth_service import get_password_hash

class UserService:
    """Service for user management operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.user_repo = UserRepository(User, session)
        self.profile_repo = UserProfileRepository(UserProfile, session)
        self.external_identity_repo = ExternalIdentityRepository(ExternalIdentity, session)
    
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
            username_matches = await self.user_repo.get_all(
                filters={"username": {"$like": f"%{search}%"}},
                skip=skip,
                limit=limit
            )
            users.extend(username_matches)
            
            # Search by email (exact or partial match)
            email_matches = await self.user_repo.get_all(
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
        
        # Regular filtering
        return await self.user_repo.get_all(filters=filters, skip=skip, limit=limit)
    
    async def get_user_with_profile(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get a user with their profile"""
        user = await self.user_repo.get(user_id)
        if not user:
            return None
        
        profile = await self.profile_repo.get_by_user_id(user_id)
        
        # Convert to dict and add profile
        user_dict = {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "status": user.status,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
            "last_login": user.last_login,
            "profile": profile
        }
        
        return user_dict
    
    async def get_user_complete(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get a user with all related information"""
        user_with_profile = await self.get_user_with_profile(user_id)
        if not user_with_profile:
            return None
        
        # Add external identities
        external_identities = await self.external_identity_repo.get_all_by_user_id(user_id)
        
        # Process each external identity to convert profile_data JSON string to dict
        processed_identities = []
        for identity in external_identities:
            identity_dict = {
                "id": identity.id,
                "user_id": identity.user_id,
                "provider": identity.provider,
                "provider_user_id": identity.provider_user_id,
                "email": identity.email,
                "created_at": identity.created_at,
                "last_login": identity.last_login,
            }
            
            # Parse profile_data JSON if it exists
            if identity.profile_data:
                try:
                    identity_dict["profile_data"] = json.loads(identity.profile_data)
                except json.JSONDecodeError:
                    identity_dict["profile_data"] = None
            else:
                identity_dict["profile_data"] = None
                
            processed_identities.append(identity_dict)
            
        user_with_profile["external_identities"] = processed_identities
        
        return user_with_profile
    
    async def get_user_external_identities(self, user_id: str) -> List[ExternalIdentity]:
        """Get all external identities for a user"""
        return await self.external_identity_repo.get_all_by_user_id(user_id)
    
    async def update_user(self, user_id: str, user_update: UserUpdate) -> Optional[Dict[str, Any]]:
        """Update a user"""
        # Check if user exists
        user = await self.user_repo.get(user_id)
        if not user:
            return None
        
        # Prepare update data
        update_data = user_update.dict(exclude_unset=True, exclude_none=True)
        
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
        
        # Return updated user with profile
        return await self.get_user_with_profile(user_id)
    
    async def update_user_profile(self, user_id: str, profile_update: UserProfileUpdate) -> Optional[Dict[str, Any]]:
        """Update a user's profile"""
        # Check if user exists
        user = await self.user_repo.get(user_id)
        if not user:
            return None
        
        # Get existing profile or create if not exists
        profile = await self.profile_repo.get_by_user_id(user_id)
        
        update_data = profile_update.dict(exclude_unset=True, exclude_none=True)
        
        if profile:
            # Update existing profile
            await self.profile_repo.update(profile.id, update_data)
        else:
            # Create new profile
            update_data["user_id"] = user_id
            await self.profile_repo.create(update_data)
        
        # Return updated user with profile
        return await self.get_user_with_profile(user_id)
    
    async def update_password(self, user_id: str, new_password: str) -> bool:
        """Update a user's password"""
        user = await self.user_repo.get(user_id)
        if not user:
            return False
        
        # Hash the new password
        hashed_password = get_password_hash(new_password)
        
        # Update user
        await self.user_repo.update(user_id, {"hashed_password": hashed_password})
        
        return True
    
    async def assign_role(self, user_id: str, role: str) -> Optional[User]:
        """Assign a role to a user"""
        user = await self.user_repo.get(user_id)
        if not user:
            return None
        
        # Update user's role
        await self.user_repo.update(user_id, {"role": role})
        
        # Return updated user
        return await self.user_repo.get(user_id)
    
    async def delete_user(self, user_id: str) -> bool:
        """Delete a user"""
        user = await self.user_repo.get(user_id)
        if not user:
            return False
        
        # Delete user (cascading delete will handle related entities)
        await self.user_repo.delete(user_id)
        
        return True
    
    async def remove_external_identity(self, user_id: str, provider: str) -> bool:
        """Remove an external identity from a user"""
        # Get the identity
        identity = await self.external_identity_repo.get_by_user_id_and_provider(user_id, provider)
        if not identity:
            return False
        
        # Delete the identity
        await self.external_identity_repo.delete(identity.id)
        
        return True 