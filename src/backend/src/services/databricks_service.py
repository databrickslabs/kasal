import os
import logging
import requests
import base64
from typing import Dict, Tuple, Optional, Any

from fastapi import HTTPException

from src.repositories.databricks_config_repository import DatabricksConfigRepository
from src.schemas.databricks_config import DatabricksConfigCreate, DatabricksConfigResponse

logger = logging.getLogger(__name__)


class DatabricksService:
    """
    Service for Databricks integration operations.
    """
    
    def __init__(self, session, group_id: Optional[str] = None):
        """
        Initialize the service with session.

        Args:
            session: Database session from FastAPI DI (from core.dependencies)
            group_id: Group ID for multi-tenant filtering
        """
        self.session = session
        self.repository = DatabricksConfigRepository(session)
        self.group_id = group_id
        # Don't create secrets_service here to avoid circular dependency
        self._secrets_service = None

    @property
    def secrets_service(self):
        """Lazy load secrets_service to avoid circular dependency."""
        if self._secrets_service is None:
            # Import here to avoid circular imports at module level
            from src.services.databricks_secrets_service import DatabricksSecretsService
            self._secrets_service = DatabricksSecretsService(self.session)
        return self._secrets_service
    
    async def set_databricks_config(self, config_in: DatabricksConfigCreate, created_by_email: Optional[str] = None) -> Dict:
        """
        Set Databricks configuration.
        
        Args:
            config_in: Configuration data
            created_by_email: Email of the user creating the config
            
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
                "is_active": True,
                "is_enabled": config_in.enabled,
                "mlflow_enabled": getattr(config_in, "mlflow_enabled", False),
                "mlflow_experiment_name": getattr(config_in, "mlflow_experiment_name", "kasal-crew-execution-traces"),
                "evaluation_enabled": getattr(config_in, "evaluation_enabled", False),
                "evaluation_judge_model": getattr(config_in, "evaluation_judge_model", None),
                "group_id": self.group_id,
                "created_by_email": created_by_email,
                # Volume configuration fields
                "volume_enabled": config_in.volume_enabled,
                "volume_path": config_in.volume_path,
                "volume_file_format": config_in.volume_file_format,
                "volume_create_date_dirs": config_in.volume_create_date_dirs,
                # Knowledge source volume configuration
                "knowledge_volume_enabled": config_in.knowledge_volume_enabled,
                "knowledge_volume_path": config_in.knowledge_volume_path,
                "knowledge_chunk_size": config_in.knowledge_chunk_size,
                "knowledge_chunk_overlap": config_in.knowledge_chunk_overlap
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
                    enabled=new_config.is_enabled,
                    mlflow_enabled=new_config.mlflow_enabled if hasattr(new_config, 'mlflow_enabled') else False,
                    mlflow_experiment_name=new_config.mlflow_experiment_name if hasattr(new_config, 'mlflow_experiment_name') else "kasal-crew-execution-traces",
                    evaluation_enabled=new_config.evaluation_enabled if hasattr(new_config, 'evaluation_enabled') else False,
                    evaluation_judge_model=new_config.evaluation_judge_model if hasattr(new_config, 'evaluation_judge_model') else None,
                    # Volume configuration fields
                    volume_enabled=new_config.volume_enabled if hasattr(new_config, 'volume_enabled') else False,
                    volume_path=new_config.volume_path if hasattr(new_config, 'volume_path') else None,
                    volume_file_format=new_config.volume_file_format if hasattr(new_config, 'volume_file_format') else 'json',
                    volume_create_date_dirs=new_config.volume_create_date_dirs if hasattr(new_config, 'volume_create_date_dirs') else True,
                    # Knowledge source volume configuration
                    knowledge_volume_enabled=new_config.knowledge_volume_enabled if hasattr(new_config, 'knowledge_volume_enabled') else False,
                    knowledge_volume_path=new_config.knowledge_volume_path if hasattr(new_config, 'knowledge_volume_path') else None,
                    knowledge_chunk_size=new_config.knowledge_chunk_size if hasattr(new_config, 'knowledge_chunk_size') else 1000,
                    knowledge_chunk_overlap=new_config.knowledge_chunk_overlap if hasattr(new_config, 'knowledge_chunk_overlap') else 200
                )
            }
        except Exception as e:
            logger.error(f"Error setting Databricks configuration: {e}")
            raise HTTPException(status_code=500, detail=f"Error setting Databricks configuration: {str(e)}")
    
    async def get_databricks_config(self) -> Optional[DatabricksConfigResponse]:
        """
        Get the current Databricks configuration for the group.

        Returns:
            Current Databricks configuration or None if not found
        """
        try:
            config = await self.repository.get_active_config(group_id=self.group_id)

            if not config:
                return None
            
            logger.debug(f"Databricks config from DB: schema={config.schema}, catalog={config.catalog}")
            
            return DatabricksConfigResponse(
                workspace_url=config.workspace_url,
                warehouse_id=config.warehouse_id,
                catalog=config.catalog,
                schema=config.schema,
                enabled=config.is_enabled,
                # MLflow configuration
                mlflow_enabled=config.mlflow_enabled if hasattr(config, 'mlflow_enabled') else False,
                mlflow_experiment_name=config.mlflow_experiment_name if hasattr(config, 'mlflow_experiment_name') else "kasal-crew-execution-traces",
                evaluation_enabled=config.evaluation_enabled if hasattr(config, 'evaluation_enabled') else False,
                evaluation_judge_model=config.evaluation_judge_model if hasattr(config, 'evaluation_judge_model') else None,
                # Volume configuration fields
                volume_enabled=config.volume_enabled if hasattr(config, 'volume_enabled') else False,
                volume_path=config.volume_path if hasattr(config, 'volume_path') else None,
                volume_file_format=config.volume_file_format if hasattr(config, 'volume_file_format') else 'json',
                volume_create_date_dirs=config.volume_create_date_dirs if hasattr(config, 'volume_create_date_dirs') else True,
                # Knowledge source volume configuration
                knowledge_volume_enabled=config.knowledge_volume_enabled if hasattr(config, 'knowledge_volume_enabled') else False,
                knowledge_volume_path=config.knowledge_volume_path if hasattr(config, 'knowledge_volume_path') else None,
                knowledge_chunk_size=config.knowledge_chunk_size if hasattr(config, 'knowledge_chunk_size') else 1000,
                knowledge_chunk_overlap=config.knowledge_chunk_overlap if hasattr(config, 'knowledge_chunk_overlap') else 200
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

            # Check if all required fields are set
            required_fields = ["warehouse_id", "catalog", "schema"]
            for field in required_fields:
                value = getattr(config, field)
                if not value:
                    return {
                        "personal_token_required": True,
                        "message": f"Databricks configuration is missing {field}"
                    }

            # All required fields are set, using unified authentication (OBO→PAT→SPN)
            return {
                "personal_token_required": False,
                "message": "Databricks is configured with unified authentication (OBO→PAT→SPN)"
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
                
            # Check if Databricks is enabled
            if hasattr(config, 'is_enabled') and config.is_enabled:
                logger.info("Databricks is enabled, checking for personal access token")
                token = await self.secrets_service.get_personal_access_token()
                if token:
                    return True, token

            return False, ""
        except Exception as e:
            logger.error(f"Error checking Databricks apps configuration: {str(e)}")
            return False, ""

    @staticmethod
    def setup_endpoint(config) -> bool:
        """
        DEPRECATED: Use get_auth_context() from databricks_auth.py instead.

        This method sets process-wide environment variables which causes race conditions
        in concurrent requests. Use get_auth_context() to get thread-safe AuthContext.

        Set up the DATABRICKS_ENDPOINT and DATABRICKS_API_BASE environment variables from the configuration.

        Args:
            config: Databricks configuration object with workspace_url attribute

        Returns:
            bool: True if successful, False otherwise
        """
        import warnings
        warnings.warn(
            "setup_endpoint() is deprecated and will be removed. "
            "Use get_auth_context() from databricks_auth.py instead for thread-safe authentication.",
            DeprecationWarning,
            stacklevel=2
        )
        try:
            if config and hasattr(config, 'workspace_url') and config.workspace_url:
                workspace_url = config.workspace_url.rstrip('/')
                
                # Set the API_BASE to include /serving-endpoints - this is required by LiteLLM
                # LiteLLM expects DATABRICKS_API_BASE to point to the serving endpoints
                os.environ["DATABRICKS_API_BASE"] = f"{workspace_url}/serving-endpoints"
                logger.info(f"Set DATABRICKS_API_BASE to {workspace_url}/serving-endpoints")
                
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
    def from_session(cls, session, api_keys_service=None):
        """
        Create a service instance from a database session.
        
        Args:
            session: Database session
            api_keys_service: Optional ApiKeysService instance
            
        Returns:
            DatabricksService: Service instance with all dependencies
        """
        from src.repositories.databricks_config_repository import DatabricksConfigRepository
        
        # Create repository
        databricks_repository = DatabricksConfigRepository(session)
        
        # Create service
        service = cls(session)

        # Set the API keys service if provided
        if api_keys_service:
            service.secrets_service.set_api_keys_service(api_keys_service)
        
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

        # Check required fields for Databricks integration
        # workspace_url can fall back to DATABRICKS_HOST environment variable
        required_fields = ["warehouse_id", "catalog", "schema"]
        missing_fields = []

        for field in required_fields:
            value = getattr(config, field, None)
            if not value:
                missing_fields.append(field)

        if missing_fields:
            return {
                "status": "error",
                "message": f"Missing required fields: {', '.join(missing_fields)}",
                "connected": False
            }

        # All required fields are present, now test actual connection
        try:
            # Prepare the workspace URL - fall back to environment variable if not in config
            import os
            workspace_url = config.workspace_url or os.getenv("DATABRICKS_HOST", "")

            if not workspace_url:
                return {
                    "status": "error",
                    "message": "workspace_url not configured and DATABRICKS_HOST environment variable not set",
                    "connected": False
                }
            if not workspace_url.startswith('https://'):
                workspace_url = f"https://{workspace_url}"
            if workspace_url.endswith('/'):
                workspace_url = workspace_url[:-1]
            
            # Try to list warehouses as a connection test
            test_url = f"{workspace_url}/api/2.0/sql/warehouses"
            
            # Get authentication using unified auth system
            try:
                from src.utils.databricks_auth import get_auth_context

                # Use unified authentication system (supports OBO, OAuth, and PAT)
                auth = await get_auth_context()
                if not auth:
                    return {
                        "status": "error",
                        "message": "No authentication credentials available",
                        "connected": False
                    }

                headers = auth.get_headers()

            except Exception as e:
                logger.error(f"Failed to get authentication context: {e}")
                return {
                    "status": "error",
                    "message": f"Authentication error: {str(e)}",
                    "connected": False
                }

            if not headers or not auth:
                return {
                    "status": "error",
                    "message": "No authentication credentials available (PAT or OAuth)",
                    "connected": False
                }
            
            # Make the actual API call to test connection
            response = requests.get(test_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return {
                    "status": "success",
                    "message": "Successfully connected to Databricks",
                    "connected": True,
                    "config": {
                        "workspace_url": workspace_url,
                        "warehouse_id": config.warehouse_id,
                        "catalog": config.catalog,
                        "schema": config.schema
                    }
                }
            elif response.status_code == 401:
                return {
                    "status": "error",
                    "message": "Authentication failed - invalid credentials",
                    "connected": False
                }
            elif response.status_code == 403:
                return {
                    "status": "error",
                    "message": "Access forbidden - check permissions",
                    "connected": False
                }
            else:
                return {
                    "status": "error",
                    "message": f"Connection failed with status {response.status_code}: {response.text}",
                    "connected": False
                }
                
        except requests.exceptions.ConnectionError:
            return {
                "status": "error",
                "message": f"Failed to connect to {workspace_url} - check workspace URL",
                "connected": False
            }
        except requests.exceptions.Timeout:
            return {
                "status": "error",
                "message": "Connection timeout - check network and workspace URL",
                "connected": False
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Connection test failed: {str(e)}",
                "connected": False
            } 