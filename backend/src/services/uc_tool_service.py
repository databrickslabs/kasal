import os
import logging
from typing import List, Dict, Any, Optional

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.models.api_key import ApiKey
from src.schemas.uc_tool import UCToolSchema, UCToolListResponse
from src.core.logger import LoggerManager
from src.core.uc_client import UCClient
from src.repositories.databricks_config_repository import DatabricksConfigRepository
from src.repositories.api_key_repository import ApiKeyRepository
from src.services.databricks_service import DatabricksService
from src.core.unit_of_work import UnitOfWork

# Initialize logger
logger = LoggerManager.get_instance().system

class UCToolService:
    """
    Service for Unity Catalog tools operations.
    Acts as an intermediary between the API router and the UC client.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize service with database session.
        
        Args:
            session: Database session
        """
        self.session = session
    
    async def _check_databricks_token_exists(self) -> bool:
        """
        Check if either DATABRICKS_TOKEN or DATABRICKS_PERSONAL_ACCESS_TOKEN exists in the database.
        
        Returns:
            bool: True if either token exists, False otherwise
        """
        # Check for DATABRICKS_TOKEN
        query_regular = select(ApiKey).where(ApiKey.name == "DATABRICKS_TOKEN")
        result_regular = await self.session.execute(query_regular)
        token_record_regular = result_regular.scalars().first()
        
        # Check for DATABRICKS_PERSONAL_ACCESS_TOKEN
        query_personal = select(ApiKey).where(ApiKey.name == "DATABRICKS_PERSONAL_ACCESS_TOKEN")
        result_personal = await self.session.execute(query_personal)
        token_record_personal = result_personal.scalars().first()
        
        return token_record_regular is not None or token_record_personal is not None
    
    async def get_all_uc_tools(self) -> UCToolListResponse:
        """
        Get all available Unity Catalog tools.
        
        Returns:
            UCToolListResponse with list of UC tools and count
            
        Raises:
            HTTPException: If operation fails
        """
        try:
            # Get configuration from database
            repository = DatabricksConfigRepository(self.session)
            config = await repository.get_active_config()
            if not config:
                logger.warning("Databricks configuration not found")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Databricks configuration not found. Please set the configuration first."
                )

            # Check if Databricks is enabled
            if hasattr(config, 'is_enabled') and not config.is_enabled:
                logger.warning("Databricks integration is disabled")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Databricks integration is disabled. Please enable it in the Configuration page."
                )

            # Check if token exists in database
            token_exists = await self._check_databricks_token_exists()
            if not token_exists:
                logger.warning("No Databricks token found in the database")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No Databricks token found in the database. Please add a token in the API Keys section with the name DATABRICKS_TOKEN or DATABRICKS_PERSONAL_ACCESS_TOKEN."
                )

            # Set up Databricks token using the proper UnitOfWork pattern
            # This ensures the service has access to both databricks_config and api_key repositories
            async with UnitOfWork() as uow:
                databricks_service = await DatabricksService.from_unit_of_work(uow)
                token_setup_success = await databricks_service.setup_token()
                
                if not token_setup_success:
                    logger.error("Failed to set up Databricks token")
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Failed to set up Databricks token. Please check your API keys configuration and ensure your token is valid."
                    )
            
            # Get token from environment variable after setup
            token = os.getenv("DATABRICKS_TOKEN", "")
            
            # Check if we have a token
            if not token:
                logger.error("Databricks token is not available after setup")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Databricks token is not available after setup. Please check the logs for more details."
                )

            # Map configuration fields
            catalog_name = config.catalog
            schema_name = config.schema
            host = config.workspace_url
            warehouse_id = config.warehouse_id
            
            # Check if workspace_url is provided
            if not host:
                logger.warning("Databricks workspace URL is not provided")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Databricks workspace URL is not provided. Please configure it in the Configuration page."
                )

            # Initialize client
            client = UCClient(host=host, token=token)
            
            # List functions
            logger.info(f"Fetching UC tools for catalog: {catalog_name}, schema: {schema_name}")
            functions = client.list_functions(catalog_name=catalog_name, schema_name=schema_name)
            
            # Return empty response if no functions found
            if not functions:
                logger.info("No functions found")
                return UCToolListResponse(tools=[], count=0)

            # Format the response
            tools = []
            for func in functions:
                try:
                    # Get detailed function info
                    func_details = client.get_function_details(
                        catalog_name, 
                        schema_name, 
                        func.name
                    )
                    
                    tools.append(UCToolSchema(
                        name=func.name,
                        full_name=f"{catalog_name}.{schema_name}.{func.name}",
                        catalog=catalog_name,
                        schema=schema_name,
                        comment=getattr(func, 'comment', None),
                        return_type=getattr(func_details, 'return_type', None),
                        input_params=[
                            {
                                "name": param.name,
                                "type": getattr(param, 'type', {}).get('type_name', 'unknown'),
                                "required": not getattr(param.type, 'nullable', True)
                            }
                            for param in getattr(func_details, 'input_params', []) or []
                        ]
                    ))
                except Exception as e:
                    logger.warning(f"Error processing function {func.name}: {str(e)}")
                    continue
            
            logger.info(f"Found {len(tools)} UC tools")
            return UCToolListResponse(tools=tools, count=len(tools))
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting UC tools: {str(e)}", exc_info=True)
            error_details = str(e)
            
            # Provide more specific error messages for common issues
            if "ModuleNotFoundError: No module named 'databricks'" in error_details:
                error_details = "Databricks SDK is not installed. Please install it with: pip install databricks-sdk"
            elif "No such key" in error_details or "KeyError" in error_details:
                error_details = "Missing key in configuration. Please check that all required fields are set in your Databricks configuration."
            elif "ConnectionError" in error_details or "Connection refused" in error_details:
                error_details = "Failed to connect to Databricks. Please check your workspace URL and network connectivity."
            elif "Unauthorized" in error_details or "Authentication failed" in error_details:
                error_details = "Authentication failed. Please check your Databricks token."
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch Unity Catalog tools: {error_details}"
            ) 