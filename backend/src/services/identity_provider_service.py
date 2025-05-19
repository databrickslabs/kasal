from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
import json

from src.models.user import IdentityProvider, ExternalIdentity
from src.repositories.user_repository import IdentityProviderRepository, ExternalIdentityRepository
from src.schemas.user import IdentityProviderCreate, IdentityProviderUpdate

class IdentityProviderService:
    """Service for identity provider management operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.provider_repo = IdentityProviderRepository(IdentityProvider, session)
        self.external_identity_repo = ExternalIdentityRepository(ExternalIdentity, session)
    
    async def get_provider(self, provider_id: str) -> Optional[IdentityProvider]:
        """Get a provider by ID"""
        return await self.provider_repo.get(provider_id)
    
    async def get_provider_by_name(self, name: str) -> Optional[IdentityProvider]:
        """Get a provider by name"""
        return await self.provider_repo.get_by_name(name)
    
    async def get_providers(self, skip: int = 0, limit: int = 100, enabled_only: bool = False) -> List[IdentityProvider]:
        """Get a list of identity providers"""
        filters = None
        if enabled_only:
            filters = {"enabled": True}
        return await self.provider_repo.get_all(filters=filters, skip=skip, limit=limit)
    
    async def create_provider(self, provider_data: IdentityProviderCreate) -> IdentityProvider:
        """Create a new identity provider"""
        # Check if provider name already exists
        existing_provider = await self.provider_repo.get_by_name(provider_data.name)
        if existing_provider:
            raise ValueError(f"Provider with name '{provider_data.name}' already exists")
        
        # Convert config to JSON string if it's a dictionary
        data_dict = provider_data.dict()
        if isinstance(data_dict.get("config"), dict):
            data_dict["config"] = json.dumps(data_dict["config"])
        
        # Create provider
        provider = await self.provider_repo.create(data_dict)
        
        return provider
    
    async def update_provider(self, provider_id: str, provider_data: IdentityProviderUpdate) -> Optional[IdentityProvider]:
        """Update an identity provider"""
        # Check if provider exists
        provider = await self.provider_repo.get(provider_id)
        if not provider:
            return None
        
        # Prepare update data
        update_data = provider_data.dict(exclude_unset=True, exclude_none=True)
        
        # If name is being updated, check if it's unique
        if "name" in update_data and update_data["name"] != provider.name:
            existing_provider = await self.provider_repo.get_by_name(update_data["name"])
            if existing_provider:
                raise ValueError(f"Provider with name '{update_data['name']}' already exists")
        
        # Convert config to JSON string if it's a dictionary
        if update_data.get("config") and isinstance(update_data["config"], dict):
            update_data["config"] = json.dumps(update_data["config"])
        
        # Update provider
        await self.provider_repo.update(provider_id, update_data)
        
        # Return updated provider
        return await self.provider_repo.get(provider_id)
    
    async def delete_provider(self, provider_id: str) -> bool:
        """Delete an identity provider"""
        provider = await self.provider_repo.get(provider_id)
        if not provider:
            return False
        
        # Check if there are external identities using this provider
        external_identities = await self.external_identity_repo.get_all_by_provider_name(provider.name)
        if external_identities:
            raise ValueError(f"Cannot delete provider '{provider.name}' because it's being used by {len(external_identities)} users")
        
        # Delete provider
        await self.provider_repo.delete(provider_id)
        
        return True
    
    async def toggle_provider_status(self, provider_id: str, enabled: bool) -> Optional[IdentityProvider]:
        """Enable or disable an identity provider"""
        provider = await self.provider_repo.get(provider_id)
        if not provider:
            return None
        
        # Update provider
        await self.provider_repo.update(provider_id, {"enabled": enabled})
        
        # Return updated provider
        return await self.provider_repo.get(provider_id)
    
    async def get_provider_config(self, provider_id: str) -> Optional[Dict[str, Any]]:
        """Get the configuration of an identity provider"""
        provider = await self.provider_repo.get(provider_id)
        if not provider or not provider.config:
            return None
        
        # Parse config from JSON string
        try:
            config = json.loads(provider.config)
            return config
        except json.JSONDecodeError:
            return None
    
    async def get_provider_usage_stats(self, provider_id: str) -> Dict[str, Any]:
        """Get usage statistics for an identity provider"""
        provider = await self.provider_repo.get(provider_id)
        if not provider:
            return {"error": "Provider not found", "count": 0}
        
        # Count external identities using this provider
        external_identities = await self.external_identity_repo.get_all_by_provider_name(provider.name)
        
        return {
            "provider_id": provider_id,
            "provider_name": provider.name,
            "provider_type": provider.provider_type,
            "count": len(external_identities),
            "enabled": provider.enabled
        } 