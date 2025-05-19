"""
Service for managing Databricks secrets.

This module provides a centralized service for retrieving, setting,
and managing secrets in Databricks Secret Store.
"""

import os
import logging
import aiohttp
import base64
from typing import List, Optional, Dict, Tuple, Any
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_service import BaseService
from src.repositories.databricks_config_repository import DatabricksConfigRepository
from src.repositories.api_key_repository import ApiKeyRepository

# Initialize logger
logger = logging.getLogger(__name__)


class DatabricksSecretsService(BaseService):
    """Service for managing Databricks secrets."""
    
    def __init__(self, databricks_repository: DatabricksConfigRepository):
        """
        Initialize the service with a repository instance.
        
        Args:
            databricks_repository: Repository for Databricks configuration
        """
        self.databricks_repository = databricks_repository
        self.api_key_repository = None
        self.databricks_service = None  # Will be set later
    
    def set_databricks_service(self, databricks_service):
        """
        Set the databricks_service to resolve circular dependency.
        
        Args:
            databricks_service: DatabricksService instance
        """
        # This method is needed to prevent circular dependency issues
        # when DatabricksService and DatabricksSecretsService reference each other
        if not hasattr(self, 'databricks_service') or self.databricks_service is None:
            self.databricks_service = databricks_service
    
    def set_api_key_repository(self, api_key_repository: ApiKeyRepository):
        """
        Set the API key repository
        
        Args:
            api_key_repository: ApiKeyRepository instance
        """
        self.api_key_repository = api_key_repository
    
    async def validate_databricks_config(self) -> Tuple[str, str]:
        """
        Get Databricks configuration from database.
        
        Returns:
            Tuple containing workspace URL and secret scope
            
        Raises:
            ValueError: If Databricks is not configured properly
        """
        # If databricks_service is not set, we need to import it here
        if self.databricks_service is None:
            # Import here to avoid circular imports
            from src.services.databricks_service import DatabricksService
            self.databricks_service = DatabricksService(self.session)
            
        config = await self.databricks_service.get_databricks_config()
        if not config:
            raise ValueError("Databricks configuration not found")
        
        # Check if Databricks is enabled
        if not config.is_enabled:
            raise ValueError("Databricks integration is disabled")
        
        # Use a default workspace_url if not provided
        workspace_url = config.workspace_url or ""
        
        return workspace_url, config.secret_scope
    
    async def get_databricks_secrets(self, scope: str) -> List[Dict[str, str]]:
        """
        Get all secrets from Databricks for a specific scope.
        
        Args:
            scope: Secret scope in Databricks
            
        Returns:
            List of secrets
        """
        try:
            # If databricks_service is not set, we need to import it here
            if self.databricks_service is None:
                # Import here to avoid circular imports
                from src.services.databricks_service import DatabricksService
                self.databricks_service = DatabricksService(self.session)
            
            # Get workspace URL and token
            config = await self.databricks_service.get_databricks_config()
            if not config or not config.is_enabled or not config.workspace_url:
                logger.warning("Databricks not configured properly")
                return []
            
            # Use token from environment variable
            token = os.getenv("DATABRICKS_TOKEN", "")
            if not token:
                logger.warning("DATABRICKS_TOKEN environment variable not set")
                return []
            
            # Make REST API call to list secrets in scope
            url = f"{config.workspace_url}/api/2.0/secrets/list"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            data = {"scope": scope}
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        secrets = result.get("secrets", [])
                        
                        # For each secret key, make another call to get its value
                        secrets_with_values = []
                        for secret in secrets:
                            secret_key = secret.get("key")
                            secret_value = await self.get_databricks_secret_value(scope, secret_key)
                            secrets_with_values.append({
                                "id": hash(f"{scope}:{secret_key}") % 10000,  # Some "unique" ID
                                "name": secret_key,
                                "value": secret_value,
                                "description": "",  # Databricks doesn't store descriptions
                                "scope": scope,
                                "source": "databricks"
                            })
                        
                        return secrets_with_values
                    else:
                        error_text = await response.text()
                        logger.error(f"Error listing Databricks secrets: {error_text}")
                        return []
        except Exception as e:
            logger.error(f"Error getting Databricks secrets: {str(e)}")
            return []
    
    async def get_databricks_secret_value(self, scope: str, key: str) -> str:
        """
        Get the value of a specific secret from Databricks.
        
        Args:
            scope: Secret scope in Databricks
            key: Secret key
            
        Returns:
            Secret value if found, else empty string
        """
        try:
            # If databricks_service is not set, we need to import it here
            if self.databricks_service is None:
                # Import here to avoid circular imports
                from src.services.databricks_service import DatabricksService
                self.databricks_service = DatabricksService(self.session)
            
            # Get workspace URL and token
            config = await self.databricks_service.get_databricks_config()
            if not config or not config.is_enabled or not config.workspace_url:
                logger.warning("Databricks not configured properly")
                return ""
            
            # Use token from environment variable
            token = os.getenv("DATABRICKS_TOKEN", "")
            if not token:
                logger.warning("DATABRICKS_TOKEN environment variable not set")
                return ""
            
            # Make REST API call to get secret value
            url = f"{config.workspace_url}/api/2.0/secrets/get"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            data = {"scope": scope, "key": key}
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        # Handle base64 encoded values from Databricks
                        secret_value_encoded = result.get("value", "")
                        try:
                            secret_value = base64.b64decode(secret_value_encoded).decode('utf-8')
                            return secret_value
                        except:
                            return secret_value_encoded
                    else:
                        error_text = await response.text()
                        logger.error(f"Error getting Databricks secret value: {error_text}")
                        return ""
        except Exception as e:
            logger.error(f"Error getting Databricks secret value: {str(e)}")
            return ""
    
    async def set_databricks_secret_value(self, scope: str, key: str, value: str) -> bool:
        """
        Set the value of a specific secret in Databricks.
        
        Args:
            scope: Secret scope in Databricks
            key: Secret key
            value: Secret value
            
        Returns:
            True if successful, else False
        """
        try:
            # If databricks_service is not set, we need to import it here
            if self.databricks_service is None:
                # Import here to avoid circular imports
                from src.services.databricks_service import DatabricksService
                self.databricks_service = DatabricksService(self.session)
            
            # Get workspace URL and token
            config = await self.databricks_service.get_databricks_config()
            if not config or not config.is_enabled or not config.workspace_url:
                logger.warning("Databricks not configured properly")
                return False
            
            # Use token from environment variable
            token = os.getenv("DATABRICKS_TOKEN", "")
            if not token:
                logger.warning("DATABRICKS_TOKEN environment variable not set")
                return False
            
            # Ensure the scope exists
            await self.create_databricks_secret_scope(config.workspace_url, token, scope)
            
            # Make REST API call to set secret value
            url = f"{config.workspace_url}/api/2.0/secrets/put"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            data = {
                "scope": scope,
                "key": key,
                "string_value": value
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=headers) as response:
                    if response.status == 200:
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Error setting Databricks secret value: {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Error setting Databricks secret value: {str(e)}")
            return False
    
    async def delete_databricks_secret(self, scope: str, key: str) -> bool:
        """
        Delete a specific secret from Databricks.
        
        Args:
            scope: Secret scope in Databricks
            key: Secret key
            
        Returns:
            True if successful, else False
        """
        try:
            # If databricks_service is not set, we need to import it here
            if self.databricks_service is None:
                # Import here to avoid circular imports
                from src.services.databricks_service import DatabricksService
                self.databricks_service = DatabricksService(self.session)
            
            # Get workspace URL and token
            config = await self.databricks_service.get_databricks_config()
            if not config or not config.is_enabled or not config.workspace_url:
                logger.warning("Databricks not configured properly")
                return False
            
            # Use token from environment variable
            token = os.getenv("DATABRICKS_TOKEN", "")
            if not token:
                logger.warning("DATABRICKS_TOKEN environment variable not set")
                return False
            
            # Make REST API call to delete secret
            url = f"{config.workspace_url}/api/2.0/secrets/delete"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            data = {"scope": scope, "key": key}
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=headers) as response:
                    if response.status == 200:
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Error deleting Databricks secret: {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Error deleting Databricks secret: {str(e)}")
            return False
    
    async def create_databricks_secret_scope(self, workspace_url: str, token: str, scope: str) -> bool:
        """
        Create a secret scope in Databricks.
        
        Args:
            workspace_url: Databricks workspace URL
            token: Databricks access token
            scope: Secret scope name to create
            
        Returns:
            True if successful, else False
        """
        try:
            # Make REST API call to create secret scope
            url = f"{workspace_url}/api/2.0/secrets/scopes/create"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            data = {"scope": scope, "initial_manage_principal": "users"}
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=headers) as response:
                    if response.status == 200:
                        return True
                    elif response.status == 400:
                        # Check if error is because scope already exists
                        error_text = await response.text()
                        if "already exists" in error_text or "RESOURCE_ALREADY_EXISTS" in error_text:
                            logger.info(f"Secret scope '{scope}' already exists")
                            return True
                        else:
                            logger.error(f"Error creating Databricks secret scope: {error_text}")
                            return False
                    else:
                        error_text = await response.text()
                        logger.error(f"Error creating Databricks secret scope: {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Error creating Databricks secret scope: {str(e)}")
            return False
            
    async def set_databricks_token(self, scope: str, token: str) -> bool:
        """
        Set Databricks token in the specified scope.
        
        Args:
            scope: Secret scope in Databricks
            token: Databricks token value
            
        Returns:
            True if successful, else False
        """
        return await self.set_databricks_secret_value(scope, "DATABRICKS_TOKEN", token)

    @classmethod
    async def setup_provider_api_key(cls, db: AsyncSession, key_name: str) -> bool:
        """
        Set up an API key for a provider from database.
        
        Args:
            db: Database session
            key_name: Name of the API key to set up
            
        Returns:
            True if successful, else False
        """
        try:
            # Use ApiKeysService to get the key first
            from src.services.api_keys_service import ApiKeysService
            
            # Try to get API key from API keys table
            value = await ApiKeysService.get_api_key_value(db, key_name)
            
            # If not found in API keys, try Databricks secrets
            if not value:
                # Create an instance to use instance methods
                service = cls(db)
                
                try:
                    # Since we're creating a new instance, databricks_service won't be set
                    # Import DatabricksService here to avoid circular imports
                    from src.services.databricks_service import DatabricksService
                    service.databricks_service = DatabricksService(db)
                    
                    # Get secret scope from config
                    workspace_url, scope = await service.validate_databricks_config()
                    if workspace_url and scope:
                        # Try to get from Databricks
                        value = await service.get_databricks_secret_value(scope, key_name)
                except Exception as e:
                    logger.warning(f"Could not get secret scope for key '{key_name}': {str(e)}")
            
            # Set environment variable if value found
            if value:
                os.environ[key_name] = value
                logger.info(f"API key '{key_name}' set up successfully")
                return True
            else:
                logger.warning(f"API key '{key_name}' not found")
                return False
        except Exception as e:
            logger.error(f"Error setting up API key '{key_name}': {str(e)}")
            return False
            
    @staticmethod
    def _setup_provider_api_key_sync(db: Session, key_name: str) -> bool:
        """
        Set up an API key for any provider from the database (synchronous).
        
        Args:
            db: Database session
            key_name: Name of the API key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Import here to avoid circular imports
            from src.services.api_keys_service import ApiKeysService
            
            # Use the dedicated static method for synchronous API key setup
            return ApiKeysService.setup_provider_api_key_sync(db, key_name)
        except Exception as e:
            logger.error(f"Error setting up API key '{key_name}' (sync): {str(e)}")
            return False

    async def get_personal_access_token(self) -> str:
        """
        Get the Databricks personal access token from the repository.
        
        Returns:
            str: The personal access token or empty string if not found
        """
        try:
            token = await self.api_key_repository.get_api_key_value("DATABRICKS_PERSONAL_ACCESS_TOKEN")
            return token or ""
        except Exception as e:
            logger.error(f"Error getting personal access token: {str(e)}")
            return ""
    
    async def get_provider_api_key(self, provider: str) -> str:
        """
        Get the provider API key from the repository.
        
        Args:
            provider: The provider name
            
        Returns:
            str: The provider API key or empty string if not found
        """
        try:
            key = await self.api_key_repository.get_provider_api_key(provider)
            return key or ""
        except Exception as e:
            logger.error(f"Error getting provider API key: {str(e)}")
            return ""
    
    async def get_all_databricks_tokens(self) -> List[str]:
        """
        Get all available Databricks tokens from the repository.
        
        Returns:
            List[str]: List of available tokens
        """
        tokens = []
        try:
            # Check for all possible Databricks token types
            token_keys = [
                "DATABRICKS_TOKEN",
                "DATABRICKS_API_KEY",
                "DATABRICKS_PERSONAL_ACCESS_TOKEN"
            ]
            
            for key in token_keys:
                value = await self.api_key_repository.get_api_key_value(key)
                if value:
                    tokens.append(value)
            
            return tokens
        except Exception as e:
            logger.error(f"Error getting all Databricks tokens: {str(e)}")
            return [] 