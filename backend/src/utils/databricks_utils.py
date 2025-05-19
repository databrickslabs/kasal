"""
Databricks utility functions.

This module provides utility functions for working with Databricks.
"""

import os
import logging
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

# Configure logging
logger = logging.getLogger(__name__)


async def setup_databricks_token(db: AsyncSession) -> bool:
    """
    Set up the Databricks token from the database.
    
    This function tries to fetch the Databricks token from API keys first,
    then falls back to Databricks secrets if needed.
    
    Args:
        db: Database session
        
    Returns:
        True if token was set up successfully, False otherwise
    """
    try:
        # Import services here to avoid circular dependencies
        from src.services.api_keys_service import ApiKeysService
        from src.services.databricks_secrets_service import DatabricksSecretsService
        
        # First try to get from API keys
        token = await ApiKeysService.get_api_key_value(db, "DATABRICKS_TOKEN")
        
        # If not found in API keys, try Databricks secrets
        if not token:
            # Get the secret scope from config
            databricks_service = DatabricksSecretsService(db)
            try:
                workspace_url, scope = await databricks_service.validate_databricks_config()
                if scope:
                    # Try to get from Databricks secrets
                    token = await databricks_service.get_databricks_secret_value(scope, "DATABRICKS_TOKEN")
            except:
                logger.warning("Could not get Databricks configuration or secret scope")
        
        # Set the token if found
        if token:
            os.environ["DATABRICKS_TOKEN"] = token
            logger.info("Databricks token set up successfully")
            return True
        else:
            logger.warning("Databricks token not found in database")
            return False
    except Exception as e:
        logger.error(f"Error setting up Databricks token: {str(e)}")
        return False

# Alias for setup_databricks_token to match import in llm_config.py
async def setup_databricks_token_async(db: AsyncSession) -> bool:
    """
    Alias for setup_databricks_token for backward compatibility.
    
    Args:
        db: Database session
        
    Returns:
        True if token was set up successfully, False otherwise
    """
    return await setup_databricks_token(db)


def setup_databricks_token_sync(db: Session) -> bool:
    """
    Set up Databricks token from the database (sync version).
    
    Args:
        db: Database session
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Import services here to avoid circular dependencies
        from src.services.api_keys_service import ApiKeysService
        
        # Use the static method for synchronous API key retrieval
        success = ApiKeysService.setup_provider_api_key_sync(db, "DATABRICKS_TOKEN")
        
        if success:
            logger.info("Databricks token set up successfully (sync)")
            return True
        else:
            logger.warning("Databricks token not found in database (sync)")
            return False
    except Exception as e:
        logger.error(f"Error setting up Databricks token (sync): {str(e)}")
        return False


async def get_databricks_config(db: AsyncSession) -> Optional[object]:
    """
    Get the Databricks configuration from the database.
    
    Args:
        db: Database session
        
    Returns:
        Databricks configuration object if found, None otherwise
    """
    try:
        # Import services here to avoid circular dependencies
        from src.services.databricks_service import DatabricksService
        
        # Create service instance
        service = DatabricksService(db)
        
        # Get configuration
        return await service.get_databricks_config()
    except Exception as e:
        logger.error(f"Error getting Databricks configuration: {str(e)}")
        return None 