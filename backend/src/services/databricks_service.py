import os
import logging
import requests
import base64
from typing import Dict, Tuple, Optional, Any

from fastapi import HTTPException

from src.repositories.databricks_config_repository import DatabricksConfigRepository
from src.schemas.databricks_config import DatabricksConfigCreate, DatabricksConfigResponse
from src.services.databricks_secrets_service import DatabricksSecretsService

logger = logging.getLogger(__name__)


class DatabricksService:
    """
    Service for Databricks integration operations.
    """
    
    def __init__(self, databricks_repository: DatabricksConfigRepository):
        """
        Initialize the service with a repository instance.
        
        Args:
            databricks_repository: Repository for database operations
        """
        self.repository = databricks_repository
        self.secrets_service = DatabricksSecretsService(databricks_repository)
        # Set self in secrets_service to resolve circular dependency
        self.secrets_service.set_databricks_service(self)
    
    async def set_databricks_config(self, config_in: DatabricksConfigCreate) -> Dict:
        """
        Set Databricks configuration.
        
        Args:
            config_in: Configuration data
            
        Returns:
            Configuration response with success message
        """
        try:
            # Create configuration data dictionary
            config_data = {
                "workspace_url": config_in.workspace_url,
                "warehouse_id": config_in.warehouse_id,
                "catalog": config_in.catalog,
                "schema": config_in.db_schema,
                "secret_scope": config_in.secret_scope,
                "is_active": True,
                "is_enabled": config_in.enabled,
                "apps_enabled": config_in.apps_enabled
            }
            
            # Create the new configuration through repository
            new_config = await self.repository.create_config(config_data)
            
            # Return the response
            return {
                "status": "success",
                "message": f"Databricks configuration {'enabled' if config_in.enabled else 'disabled'} successfully",
                "config": DatabricksConfigResponse(
                    workspace_url=new_config.workspace_url,
                    warehouse_id=new_config.warehouse_id,
                    catalog=new_config.catalog,
                    schema=new_config.schema,
                    secret_scope=new_config.secret_scope,
                    enabled=new_config.is_enabled,
                    apps_enabled=new_config.apps_enabled
                )
            }
        except Exception as e:
            logger.error(f"Error setting Databricks configuration: {e}")
            raise HTTPException(status_code=500, detail=f"Error setting Databricks configuration: {str(e)}")
    
    async def get_databricks_config(self) -> DatabricksConfigResponse:
        """
        Get the current Databricks configuration.
        
        Returns:
            Current Databricks configuration
        """
        try:
            config = await self.repository.get_active_config()
            
            if not config:
                raise HTTPException(status_code=404, detail="Databricks configuration not found")
            
            logger.info(f"Databricks config from DB: schema={config.schema}, catalog={config.catalog}")
            
            return DatabricksConfigResponse(
                workspace_url=config.workspace_url,
                warehouse_id=config.warehouse_id,
                catalog=config.catalog,
                schema=config.schema,
                secret_scope=config.secret_scope,
                enabled=config.is_enabled,
                apps_enabled=config.apps_enabled
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting Databricks configuration: {e}")
            raise HTTPException(status_code=500, detail=f"Error getting Databricks configuration: {str(e)}")
    
    async def check_personal_token_required(self) -> Dict:
        """
        Check if personal access token is required for Databricks.
        
        Returns:
            Status indicating if personal token is required
        """
        try:
            config = await self.repository.get_active_config()
            
            if not config:
                return {
                    "personal_token_required": False,
                    "message": "Databricks is not configured"
                }
            
            # If Databricks is not enabled, no token is required
            if not config.is_enabled:
                return {
                    "personal_token_required": False,
                    "message": "Databricks integration is disabled"
                }
            
            # If apps are enabled, token is required
            if config.apps_enabled:
                return {
                    "personal_token_required": True,
                    "message": "Databricks is configured to use a personal access token"
                }
            
            # Otherwise, check if all required fields are set
            required_fields = ["warehouse_id", "catalog", "schema", "secret_scope"]
            for field in required_fields:
                value = getattr(config, field)
                if not value:
                    return {
                        "personal_token_required": True,
                        "message": f"Databricks configuration is missing {field}"
                    }
            
            # All required fields are set, token is not required
            return {
                "personal_token_required": False,
                "message": "Databricks is not configured to use a personal access token"
            }
        except Exception as e:
            logger.error(f"Error checking personal token requirement: {e}")
            raise HTTPException(status_code=500, detail=f"Error checking personal token requirement: {str(e)}")

    # Methods for Databricks token management
    
    async def check_apps_configuration(self) -> Tuple[bool, str]:
        """
        Check if 'Databricks Apps Integration' is disabled but 'Databricks Settings' is enabled
        and determine if a personal access token should be used.
        
        Returns:
            Tuple[bool, str]: (should_use_personal_token, personal_access_token)
        """
        try:
            config = await self.repository.get_active_config()
            if not config:
                return False, ""
                
            # Check if Databricks is enabled but Apps Integration is disabled
            if hasattr(config, 'is_enabled') and config.is_enabled and hasattr(config, 'apps_enabled') and not config.apps_enabled:
                # This is the case we're looking for
                logger.info("Databricks is enabled but Apps Integration is disabled, using personal access token")
                token = await self.secrets_service.get_personal_access_token()
                return True, token
                
            return False, ""
        except Exception as e:
            logger.error(f"Error checking Databricks apps configuration: {str(e)}")
            return False, ""

    @staticmethod
    def setup_endpoint(config) -> bool:
        """
        Set up the DATABRICKS_ENDPOINT and DATABRICKS_API_BASE environment variables from the configuration.
        
        Args:
            config: Databricks configuration object with workspace_url attribute
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if config and hasattr(config, 'workspace_url') and config.workspace_url:
                workspace_url = config.workspace_url.rstrip('/')
                
                # Set the API_BASE to the workspace URL - this is used by LiteLLM
                os.environ["DATABRICKS_API_BASE"] = workspace_url
                logger.info(f"Set DATABRICKS_API_BASE to {workspace_url}")
                
                # Ensure the endpoint URL ends with /serving-endpoints
                if not workspace_url.endswith('/serving-endpoints'):
                    endpoint_url = f"{workspace_url}/serving-endpoints"
                else:
                    endpoint_url = workspace_url
                    
                os.environ["DATABRICKS_ENDPOINT"] = endpoint_url
                logger.info(f"Set DATABRICKS_ENDPOINT to {endpoint_url}")
                return True
            else:
                logger.warning("No workspace_url found in Databricks configuration")
                return False
        except Exception as e:
            logger.error(f"Error setting up Databricks endpoint: {str(e)}")
            return False

    @classmethod
    async def setup_token(cls) -> bool:
        """
        Set up Databricks token from API key or personal access token.
        
        Returns:
            bool: True if token was set up successfully, False otherwise
        """
        try:
            # Get configuration
            from src.repositories.databricks_config_repository import DatabricksConfigRepository
            from src.db.session import async_session_factory
            
            # Create repository and secrets service instances
            async with async_session_factory() as db:
                repository = DatabricksConfigRepository(db)
                # Create a temporary instance for the necessary methods
                instance = cls(repository)
                
                # Get configuration
                config = await repository.get_active_config()
                
                # Setup the endpoint URL
                if config:
                    cls.setup_endpoint(config)
                
                # Check configuration to see if personal token should be used
                should_use_personal_token, personal_token = await instance.check_apps_configuration()
                
                if should_use_personal_token:
                    if personal_token:
                        # Set the environment variable
                        os.environ["DATABRICKS_TOKEN"] = personal_token
                        os.environ["DATABRICKS_API_KEY"] = personal_token  # Set both for compatibility
                        logger.info("Successfully set DATABRICKS_TOKEN from personal access token")
                        return True
                    else:
                        logger.warning("Personal access token needed but not found or empty")
                
                # If we get here, either we don't need a personal token or it wasn't found
                logger.info("Trying to set up provider API key")
                
                # Try to get the provider API key
                api_key = await instance.secrets_service.get_provider_api_key("databricks")
                if api_key:
                    os.environ["DATABRICKS_TOKEN"] = api_key
                    os.environ["DATABRICKS_API_KEY"] = api_key  # Set both for compatibility
                    logger.info("Successfully set up DATABRICKS_TOKEN from provider API key")
                    return True
                
                # Try other token types as fallback
                tokens = await instance.secrets_service.get_all_databricks_tokens()
                if tokens:
                    # Use the first valid token found
                    os.environ["DATABRICKS_TOKEN"] = tokens[0]
                    os.environ["DATABRICKS_API_KEY"] = tokens[0]  # Set both for compatibility
                    logger.info(f"Successfully set DATABRICKS_TOKEN from fallback method")
                    return True
                
                logger.warning("Failed to set up DATABRICKS_TOKEN - no token found")
                return False
        except Exception as e:
            logger.error(f"Error setting up Databricks token: {str(e)}")
            return False

    @classmethod
    def setup_token_sync(cls) -> bool:
        """
        Synchronous version of setup_token method that can be called from synchronous code.
        Uses create_and_run_loop to execute the async method.
        
        Returns:
            bool: True if token was set up successfully, False otherwise
        """
        try:
            from src.utils.asyncio_utils import create_and_run_loop
            
            # Run async method in a new event loop
            return create_and_run_loop(cls.setup_token())
        except Exception as e:
            logger.error(f"Error in setup_token_sync: {str(e)}")
            return False

    @classmethod
    async def from_unit_of_work(cls, uow):
        """
        Create a service instance from a UnitOfWork.
        
        Args:
            uow: UnitOfWork instance
            
        Returns:
            DatabricksService: Service instance using the UnitOfWork's repository
        """
        service = cls(databricks_repository=uow.databricks_config_repository)
        # Set the API key repository in the secrets service
        service.secrets_service.set_api_key_repository(uow.api_key_repository)
        return service
        
    async def check_databricks_connection(self) -> Dict[str, Any]:
        """
        Check connection to Databricks.
        
        Returns:
            Dictionary with connection status
        """
        config = await self.repository.get_active_config()
        
        if not config:
            return {
                "status": "error",
                "message": "Databricks configuration not found",
                "connected": False
            }
        
        if not config.is_enabled:
            return {
                "status": "disabled",
                "message": "Databricks integration is disabled",
                "connected": False
            } 