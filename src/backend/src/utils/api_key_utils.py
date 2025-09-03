"""
Utility functions for API key management.

This module provides a simplified interface for retrieving and setting up API keys
from the database, delegating the actual implementation to the api_keys_service module.

These functions are kept in the utils module for backward compatibility and convenience.
For new code, consider using the ApiKeysService methods directly.
"""

import logging
import os
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession


from src.services.api_keys_service import ApiKeysService

# Configure logging
logger = logging.getLogger(__name__)

async def async_setup_provider_api_key(db: AsyncSession, key_name: str) -> bool:
    """
    Set up an API key for any provider from the database using async.
    
    Args:
        db: Database session
        key_name: Name of the API key
        
    Returns:
        True if successful, False otherwise
    """
    return await ApiKeysService.setup_provider_api_key(db, key_name)

def setup_provider_api_key(db: Session, key_name: str) -> bool:
    """
    Set up an API key for any provider from the database (sync version).
    
    Args:
        db: Database session
        key_name: Name of the API key
        
    Returns:
        True if successful, False otherwise
    """
    return ApiKeysService.setup_provider_api_key_sync(db, key_name)

async def async_setup_openai_api_key(db: AsyncSession) -> bool:
    """Set up the OpenAI API key from the database using async."""
    return await ApiKeysService.setup_openai_api_key(db)

async def async_setup_anthropic_api_key(db: AsyncSession) -> bool:
    """Set up the Anthropic API key from the database using async."""
    return await ApiKeysService.setup_anthropic_api_key(db)

async def async_setup_deepseek_api_key(db: AsyncSession) -> bool:
    """Set up the DeepSeek API key from the database using async."""
    return await ApiKeysService.setup_deepseek_api_key(db)

async def async_setup_all_api_keys(db) -> None:
    """
    Set up all supported API keys from the database using async.
    This function accepts both synchronous and asynchronous sessions.
    
    Args:
        db: Database session (can be sync or async)
    """
    await ApiKeysService.setup_all_api_keys(db)

def setup_openai_api_key(db: Session) -> bool:
    """Set up the OpenAI API key from the database."""
    return ApiKeysService.setup_provider_api_key_sync(db, "OPENAI_API_KEY")

def setup_anthropic_api_key(db: Session) -> bool:
    """Set up the Anthropic API key from the database."""
    return ApiKeysService.setup_provider_api_key_sync(db, "ANTHROPIC_API_KEY")

def setup_deepseek_api_key(db: Session) -> bool:
    """Set up the DeepSeek API key from the database."""
    return ApiKeysService.setup_provider_api_key_sync(db, "DEEPSEEK_API_KEY")

def setup_all_api_keys(db: Session) -> None:
    """Set up all supported API keys from the database."""
    setup_openai_api_key(db)
    setup_anthropic_api_key(db)
    setup_deepseek_api_key(db)

async def async_get_databricks_personal_access_token(db: AsyncSession) -> str:
    """Get the Databricks personal access token from the database using async."""
    return await ApiKeysService.get_api_key_value(db, "DATABRICKS_TOKEN") or ""

def get_databricks_personal_access_token(db: Session) -> str:
    """Get the Databricks personal access token from the database."""
    api_key = ApiKeysService.setup_provider_api_key_sync(db, "DATABRICKS_TOKEN")
    return os.environ.get("DATABRICKS_TOKEN", "") 