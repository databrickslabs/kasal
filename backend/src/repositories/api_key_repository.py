from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.api_key import ApiKey


class ApiKeyRepository(BaseRepository[ApiKey]):
    """
    Repository for ApiKey model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(ApiKey, session)
    
    async def find_by_name(self, name: str) -> Optional[ApiKey]:
        """
        Find an API key by name.
        
        Args:
            name: Name to search for
            
        Returns:
            ApiKey if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    def find_by_name_sync(self, name: str) -> Optional[ApiKey]:
        """
        Find an API key by name synchronously.
        
        Args:
            name: Name to search for
            
        Returns:
            ApiKey if found, else None
        """
        if not isinstance(self.session, Session):
            raise TypeError("Session must be a synchronous SQLAlchemy Session for find_by_name_sync")
        
        query = select(self.model).where(self.model.name == name)
        result = self.session.execute(query)
        return result.scalars().first()
    
    async def find_all(self) -> List[ApiKey]:
        """
        Find all API keys.
        
        Returns:
            List of all API keys
        """
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def get_api_key_value(self, key_name: str) -> Optional[str]:
        """
        Get the decrypted value of an API key by name.
        
        Args:
            key_name: Name of the API key
            
        Returns:
            Decrypted API key value if found, None otherwise
        """
        api_key = await self.find_by_name(key_name)
        if not api_key:
            return None
        
        # Decrypt the API key value
        try:
            from src.utils.encryption_utils import EncryptionUtils
            return EncryptionUtils.decrypt_value(api_key.encrypted_value)
        except Exception as e:
            return None
    
    async def get_provider_api_key(self, provider: str) -> Optional[str]:
        """
        Get API key for a specific provider.
        
        Args:
            provider: Provider name (e.g., 'openai', 'databricks')
            
        Returns:
            Decrypted API key value if found, None otherwise
        """
        key_name = f"{provider.upper()}_API_KEY"
        return await self.get_api_key_value(key_name) 