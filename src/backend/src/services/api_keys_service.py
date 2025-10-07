"""
Service for managing API keys.

This module provides a centralized service for retrieving, setting,
and managing API keys stored in the local database.
"""

import logging
import os
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_service import BaseService
from src.models.api_key import ApiKey
from src.repositories.api_key_repository import ApiKeyRepository
from src.schemas.api_key import ApiKeyCreate, ApiKeyUpdate
from src.utils.encryption_utils import EncryptionUtils

# Initialize logger
logger = logging.getLogger(__name__)


class ApiKeysService(BaseService):
    """Service for managing API keys."""
    
    def __init__(self, session: AsyncSession, group_id: Optional[str] = None):
        """
        Initialize the service with session.

        Args:
            session: Database session from FastAPI DI
            group_id: Group ID for multi-tenant filtering
        """
        self.repository = ApiKeyRepository(session)
        self.encryption_utils = EncryptionUtils()
        self.group_id = group_id
        self.session = None  # For compatibility with older code
        self.is_async = True  # Always async now
    
    async def find_by_name(self, name: str) -> Optional[ApiKey]:
        """
        Find an API key by name within the current group context.

        SECURITY: This method REQUIRES a group_id for multi-tenant isolation.
        API keys are ALWAYS scoped to a group and cannot be queried system-wide.

        Args:
            name: Name to search for

        Returns:
            ApiKey if found, else None

        Raises:
            ValueError: If group_id is None (multi-tenant isolation required)
        """
        if not self.is_async:
            # If using a sync session, call the sync method
            return self.find_by_name_sync(name)

        # SECURITY CHECK: ALWAYS enforce group_id filtering
        if self.group_id is None:
            raise ValueError(
                "SECURITY: Cannot query API keys without group_id. "
                "All API keys must be scoped to a group for multi-tenant isolation. "
                "Provide a valid group_id when creating ApiKeysService."
            )

        return await self.repository.find_by_name(name, group_id=self.group_id)
    
    def find_by_name_sync(self, name: str) -> Optional[ApiKey]:
        """
        Find an API key by name synchronously within the current group context.

        SECURITY: This method REQUIRES a group_id for multi-tenant isolation.
        API keys are ALWAYS scoped to a group and cannot be queried system-wide.

        Args:
            name: Name to search for

        Returns:
            ApiKey if found, else None

        Raises:
            ValueError: If group_id is None (multi-tenant isolation required)
        """
        # Make sure we're using a synchronous session
        if not isinstance(self.session, Session):
            raise TypeError("This method requires a synchronous session")

        # SECURITY CHECK: ALWAYS enforce group_id filtering
        if self.group_id is None:
            raise ValueError(
                "SECURITY: Cannot query API keys without group_id. "
                "All API keys must be scoped to a group for multi-tenant isolation. "
                "Provide a valid group_id when creating ApiKeysService."
            )

        return self.repository.find_by_name_sync(name, group_id=self.group_id)
    
    async def create_api_key(self, api_key_data: ApiKeyCreate, created_by_email: Optional[str] = None) -> ApiKey:
        """
        Create a new API key with encrypted value.
        
        Args:
            api_key_data: API key data for creation
            created_by_email: Email of the user creating the key
            
        Returns:
            Created API key
        """
        # Encrypt the API key value
        encrypted_value = EncryptionUtils.encrypt_value(api_key_data.value)
        
        # Create API key data dictionary
        api_key_dict = {
            "name": api_key_data.name,
            "encrypted_value": encrypted_value,
            "description": api_key_data.description or "",
            "group_id": self.group_id,
            "created_by_email": created_by_email
        }
        
        # Save to database
        created_key = await self.repository.create(api_key_dict)
        
        # For the response, we need to set the decrypted value
        # This won't be saved to the database, it's just for the API response
        created_key.value = api_key_data.value
        
        return created_key
    
    async def update_api_key(self, name: str, api_key_data: ApiKeyUpdate) -> Optional[ApiKey]:
        """
        Update an existing API key.
        
        Args:
            name: Name of the API key to update
            api_key_data: API key data for update
            
        Returns:
            Updated API key if successful, else None
        """
        # Find the API key
        api_key = await self.find_by_name(name)
        if not api_key:
            return None
        
        # Create update dictionary
        update_dict = {
            "encrypted_value": EncryptionUtils.encrypt_value(api_key_data.value)
        }
        
        if api_key_data.description is not None:
            update_dict["description"] = api_key_data.description
        
        # Update in database
        updated_key = await self.repository.update(api_key.id, update_dict)
        
        # For the response, we need to set the decrypted value
        # This won't be saved to the database, it's just for the API response
        updated_key.value = api_key_data.value
        
        return updated_key
    
    async def delete_api_key(self, name: str) -> bool:
        """
        Delete an API key.
        
        Args:
            name: Name of the API key to delete
            
        Returns:
            True if deleted, False otherwise
        """
        # Find the API key
        api_key = await self.find_by_name(name)
        if not api_key:
            return False
        
        # Delete from database
        return await self.repository.delete(api_key.id)
    
    async def get_all_api_keys(self) -> List[ApiKey]:
        """
        Get all API keys with decrypted values for the current group.
        
        Returns:
            List of all API keys with decrypted values
        """
        api_keys = await self.repository.find_all(group_id=self.group_id)
        
        # Decrypt values for the response
        for key in api_keys:
            try:
                # Set a plain attribute for the value
                key.value = EncryptionUtils.decrypt_value(key.encrypted_value)
            except Exception as e:
                logger.error(f"Error decrypting API key '{key.name}': {str(e)}")
                # If decryption fails, set empty value
                key.value = ""
        
        return api_keys
    
    async def get_api_keys_metadata(self) -> List[ApiKey]:
        """
        Get all API keys metadata without values for frontend display.
        
        Returns only metadata (names, descriptions, IDs) with status indicator.
        Safe for HTTP API exposure.
        
        Returns:
            List of API keys with metadata and set/not set status
        """
        api_keys = await self.repository.find_all(group_id=self.group_id)
        
        # Set status indicator instead of actual values for security
        for key in api_keys:
            # Check if key has an encrypted value
            has_value = bool(key.encrypted_value and key.encrypted_value.strip())
            key.value = "Set" if has_value else "Not set"  # Status indicator instead of actual value
        
        return api_keys
    
    @classmethod
    async def get_api_key_value(cls, db: AsyncSession = None, key_name: str = None, group_id: str = None):
        """
        Get the value of an API key by name (decrypted).

        SECURITY: Requires group_id for multi-tenant isolation.

        Args:
            db: Database session (deprecated, kept for backwards compatibility)
            key_name: Name of the API key
            group_id: Group ID for multi-tenant filtering (REQUIRED)

        Returns:
            Decrypted API key value if found, else None

        Raises:
            ValueError: If group_id is not provided
        """
        # If key_name was passed as first argument and db as second or not at all
        if db is not None and isinstance(db, str):
            key_name = db
            db = None

        # SECURITY CHECK: group_id is required
        if group_id is None:
            raise ValueError(
                "SECURITY: group_id is required for get_api_key_value(). "
                "All API key operations must be scoped to a group."
            )

        # Create a service instance using session factory
        from src.db.session import async_session_factory
        async with async_session_factory() as session:
            service = cls(session, group_id=group_id)

            # Find the API key
            api_key = await service.find_by_name(key_name)
            if not api_key:
                return None

            # Decrypt and return the value
            try:
                return EncryptionUtils.decrypt_value(api_key.encrypted_value)
            except Exception as e:
                logger.error(f"Error decrypting API key '{key_name}': {str(e)}")
                return None
    
    @classmethod
    async def setup_provider_api_key(cls, db: AsyncSession, key_name: str) -> bool:
        """
        Set up an API key for any provider from the database.
        
        This is a generic function that can be used to set up any API key.
        It retrieves the key from the database and sets it as an environment variable.
        
        Args:
            db: Database session
            key_name: Name of the API key
            
        Returns:
            True if successful, False otherwise
        """
        value = await cls.get_api_key_value(db, key_name)
        if value:
            os.environ[key_name] = value
            logger.info(f"API key '{key_name}' set up successfully")
            return True
        else:
            logger.warning(f"API key '{key_name}' not found in database")
            return False
    
    @staticmethod
    def setup_provider_api_key_sync(db: Session, key_name: str) -> bool:
        """
        Set up an API key for any provider from the database (sync version).
        
        Args:
            db: Database session (synchronous)
            key_name: Name of the API key
            
        Returns:
            True if successful, False otherwise
        """
        # Create a service instance with a synchronous session
        service = ApiKeysService(db)
        
        # Get the API key from the database
        try:
            # Find the API key by name
            api_key = service.find_by_name_sync(key_name)
            
            if api_key and api_key.encrypted_value:
                # Decrypt the value
                value = EncryptionUtils.decrypt_value(api_key.encrypted_value)
                
                # Set as environment variable
                os.environ[key_name] = value
                logger.info(f"API key '{key_name}' set up successfully (sync)")
                return True
            else:
                logger.warning(f"API key '{key_name}' not found in database (sync)")
                return False
        except Exception as e:
            logger.error(f"Error setting up API key '{key_name}': {str(e)}")
            return False
            
    @classmethod
    async def setup_openai_api_key(cls, db: AsyncSession = None, group_id: str = None) -> bool:
        """
        Set up the OpenAI API key from the database.

        SECURITY: Requires group_id for multi-tenant isolation.

        Args:
            db: Optional database session (for backwards compatibility)
            group_id: Group ID for multi-tenant filtering (REQUIRED)

        Returns:
            True if key was found and set up successfully, False otherwise

        Raises:
            ValueError: If group_id is not provided
        """
        if group_id is None:
            raise ValueError(
                "SECURITY: group_id is REQUIRED for setup_openai_api_key(). "
                "All API key operations must be scoped to a group."
            )
        try:
            # Use the class method that handles session internally
            value = await cls.get_provider_api_key("openai", group_id=group_id)
            if value:
                os.environ["OPENAI_API_KEY"] = value
                logger.info("OpenAI API key set up successfully")
                return True
            else:
                logger.warning("OpenAI API key not found in database")
                return False
        except Exception as e:
            logger.error(f"Error setting up OpenAI API key: {str(e)}")
            return False
        
    @classmethod
    async def setup_anthropic_api_key(cls, db: AsyncSession = None, group_id: str = None) -> bool:
        """
        Set up the Anthropic API key from the database.

        SECURITY: Requires group_id for multi-tenant isolation.

        Args:
            db: Optional database session (for backwards compatibility)
            group_id: Group ID for multi-tenant filtering (REQUIRED)

        Returns:
            True if key was found and set up successfully, False otherwise

        Raises:
            ValueError: If group_id is not provided
        """
        if group_id is None:
            raise ValueError(
                "SECURITY: group_id is REQUIRED for setup_anthropic_api_key(). "
                "All API key operations must be scoped to a group."
            )
        try:
            # Use the class method that handles session internally
            value = await cls.get_provider_api_key("anthropic", group_id=group_id)
            if value:
                os.environ["ANTHROPIC_API_KEY"] = value
                logger.info("Anthropic API key set up successfully")
                return True
            else:
                logger.warning("Anthropic API key not found in database")
                return False
        except Exception as e:
            logger.error(f"Error setting up Anthropic API key: {str(e)}")
            return False
        
    @classmethod
    async def setup_deepseek_api_key(cls, db: AsyncSession = None, group_id: str = None) -> bool:
        """
        Set up the DeepSeek API key from the database.

        SECURITY: Requires group_id for multi-tenant isolation.

        Args:
            db: Optional database session (for backwards compatibility)
            group_id: Group ID for multi-tenant filtering (REQUIRED)

        Returns:
            True if key was found and set up successfully, False otherwise

        Raises:
            ValueError: If group_id is not provided
        """
        if group_id is None:
            raise ValueError(
                "SECURITY: group_id is REQUIRED for setup_deepseek_api_key(). "
                "All API key operations must be scoped to a group."
            )
        try:
            # Use the class method that handles session internally
            value = await cls.get_provider_api_key("deepseek", group_id=group_id)
            if value:
                os.environ["DEEPSEEK_API_KEY"] = value
                logger.info("DeepSeek API key set up successfully")
                return True
            else:
                logger.warning("DeepSeek API key not found in database")
                return False
        except Exception as e:
            logger.error(f"Error setting up DeepSeek API key: {str(e)}")
            return False
            
    @classmethod
    async def setup_gemini_api_key(cls, db: AsyncSession = None, group_id: str = None) -> bool:
        """
        Set up the Gemini API key from the database.

        SECURITY: Requires group_id for multi-tenant isolation.

        Args:
            db: Optional database session (for backwards compatibility)
            group_id: Group ID for multi-tenant filtering (REQUIRED)

        Returns:
            True if key was found and set up successfully, False otherwise

        Raises:
            ValueError: If group_id is not provided
        """
        if group_id is None:
            raise ValueError(
                "SECURITY: group_id is REQUIRED for setup_gemini_api_key(). "
                "All API key operations must be scoped to a group."
            )
        try:
            # Use the class method that handles session internally
            value = await cls.get_provider_api_key("gemini", group_id=group_id)
            if value:
                os.environ["GEMINI_API_KEY"] = value
                logger.info("Gemini API key set up successfully")
                return True
            else:
                logger.warning("Gemini API key not found in database")
                return False
        except Exception as e:
            logger.error(f"Error setting up Gemini API key: {str(e)}")
            return False
        
    @classmethod
    async def setup_all_api_keys(cls, db = None, group_id: str = None) -> None:
        """
        Set up all supported API keys from the database.

        SECURITY: Requires group_id for multi-tenant isolation.

        Args:
            db: Optional database session (for backwards compatibility)
            group_id: Group ID for multi-tenant filtering (REQUIRED)

        Raises:
            ValueError: If group_id is not provided
        """
        if group_id is None:
            raise ValueError(
                "SECURITY: group_id is REQUIRED for setup_all_api_keys(). "
                "All API key operations must be scoped to a group."
            )

        # Check if we have a synchronous session
        if db is not None and isinstance(db, Session):
            # NOTE: Sync methods are deprecated and don't support group_id properly
            # This path should not be used in production
            logger.warning("Using deprecated sync API key setup - security may be compromised")
            ApiKeysService.setup_provider_api_key_sync(db, "OPENAI_API_KEY")
            ApiKeysService.setup_provider_api_key_sync(db, "ANTHROPIC_API_KEY")
            ApiKeysService.setup_provider_api_key_sync(db, "DEEPSEEK_API_KEY")
            ApiKeysService.setup_provider_api_key_sync(db, "GEMINI_API_KEY")
        else:
            # Use the async methods that now use UnitOfWork
            await cls.setup_openai_api_key(group_id=group_id)
            await cls.setup_anthropic_api_key(group_id=group_id)
            await cls.setup_deepseek_api_key(group_id=group_id)
            await cls.setup_gemini_api_key(group_id=group_id)
    
    @classmethod
    async def get_provider_api_key(cls, provider: str, group_id: str) -> Optional[str]:
        """
        Get API key for a specific provider using the repository pattern.
        This method handles encryption/decryption and doesn't require a db session.

        SECURITY: Requires group_id for multi-tenant isolation.

        Args:
            provider: Provider name (e.g., 'openai', 'anthropic', 'deepseek')
            group_id: Group ID for multi-tenant filtering (REQUIRED)

        Returns:
            Decrypted API key if found, None otherwise

        Raises:
            ValueError: If group_id is not provided
        """
        # SECURITY CHECK: group_id is required
        if group_id is None:
            raise ValueError(
                "SECURITY: group_id is required for get_provider_api_key(). "
                "All API key operations must be scoped to a group."
            )

        try:
            # Create a service instance using session factory
            from src.db.session import async_session_factory
            async with async_session_factory() as session:
                service = cls(session, group_id=group_id)

                # Find the API key by name (provider name with _API_KEY suffix)
                key_name = f"{provider.upper()}_API_KEY"
                api_key = await service.find_by_name(key_name)
                if not api_key:
                    logger.warning(f"No API key found for provider: {provider} with group_id: {group_id}")
                    return None

                # Decrypt the API key value
                try:
                    decrypted_value = EncryptionUtils.decrypt_value(api_key.encrypted_value)
                    return decrypted_value
                except Exception as e:
                    logger.error(f"Error decrypting API key for provider {provider}: {str(e)}")
                    return None

        except Exception as e:
            logger.error(f"Error getting provider API key: {str(e)}")
            return None 