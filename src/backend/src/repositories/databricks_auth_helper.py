"""
Helper for Databricks authentication token retrieval.

This module provides centralized authentication token retrieval for Databricks REST API calls,
following the established authentication hierarchy.

Authentication priority:
1. OBO (On-Behalf-Of) with user token from X-Forwarded-Access-Token header
2. PAT from database (encrypted storage)
3. PAT from environment variables
"""
import os
from typing import Optional
from src.core.logger import LoggerManager
from src.utils.databricks_auth import is_databricks_apps_environment

logger = LoggerManager.get_instance().system


class DatabricksAuthHelper:
    """Helper class for Databricks authentication token retrieval."""
    
    @staticmethod
    async def get_auth_token(
        workspace_url: Optional[str] = None,
        user_token: Optional[str] = None
    ) -> str:
        """
        Get an authentication token for Databricks REST API calls.
        
        Authentication priority:
        1. OBO with user token (if provided) - Best for user-specific access
        2. PAT from database - Managed tokens
        3. PAT from environment - Simple deployment
        
        Args:
            workspace_url: Databricks workspace URL (optional, used for validation)
            user_token: Optional user token for OBO authentication (from X-Forwarded-Access-Token)
            
        Returns:
            Authentication token
            
        Raises:
            Exception: If no authentication token can be obtained
        """
        # Priority 1: OBO with user token (always first priority)
        if user_token:
            logger.info("Using OBO authentication token")
            return user_token
        
        # Priority 2: PAT from database (encrypted storage)
        try:
            pat_token = await DatabricksAuthHelper._get_pat_from_database()
            if pat_token:
                logger.info("Using PAT from database")
                return pat_token
        except Exception as e:
            logger.debug(f"Database PAT lookup failed: {e}")
        
        # Priority 3: PAT from environment variables
        pat_token = os.getenv("DATABRICKS_TOKEN") or os.getenv("DATABRICKS_API_KEY")
        if pat_token:
            logger.info("Using PAT from environment")
            return pat_token
        
        # No authentication available
        raise Exception(
            "Failed to get authentication token. Please provide either:\n"
            "1. User token for OBO authentication\n"
            "2. PAT token in database (DATABRICKS_TOKEN or DATABRICKS_API_KEY)\n"
            "3. DATABRICKS_TOKEN or DATABRICKS_API_KEY environment variable"
        )
    
    @staticmethod
    async def _get_pat_from_database() -> Optional[str]:
        """
        Get PAT token from database with proper async handling.
        
        This method handles multiple event loop scenarios:
        1. Called from main FastAPI event loop - direct execution
        2. Called from CrewAI event loop - uses thread executor with new loop
        3. Called from sync context - creates new event loop
        
        Returns:
            PAT token if found, None otherwise
        """
        import asyncio
        import concurrent.futures
        import os
        
        def fetch_pat_sync() -> Optional[str]:
            """
            Synchronous function to fetch PAT from database.
            This runs in a completely isolated thread with its own event loop.
            """
            # Ensure USE_NULLPOOL is set for the new event loop
            os.environ["USE_NULLPOOL"] = "true"
            
            # Create a brand new event loop for this thread
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            
            try:
                # Import inside to avoid any loop context issues
                from src.services.api_keys_service import ApiKeysService
                from src.core.unit_of_work import UnitOfWork
                from src.utils.encryption_utils import EncryptionUtils
                
                async def _fetch():
                    """Async function to fetch PAT."""
                    try:
                        async with UnitOfWork() as uow:
                            api_service = await ApiKeysService.from_unit_of_work(uow)
                            
                            # Try both common Databricks token names
                            for key_name in ["DATABRICKS_TOKEN", "DATABRICKS_API_KEY"]:
                                api_key = await api_service.find_by_name(key_name)
                                if api_key and api_key.encrypted_value:
                                    pat_token = EncryptionUtils.decrypt_value(api_key.encrypted_value)
                                    if pat_token:
                                        logger.info(f"Found Databricks API key in database: {key_name}")
                                        return pat_token
                        return None
                    except Exception as db_error:
                        logger.debug(f"Database fetch error: {db_error}")
                        return None
                
                # Run the async function in the new loop
                return new_loop.run_until_complete(_fetch())
                
            finally:
                # Clean up the loop
                new_loop.close()
                asyncio.set_event_loop(None)
        
        try:
            # Check if we're in an event loop
            try:
                current_loop = asyncio.get_running_loop()
                
                # We're in an event loop - use thread executor for database isolation
                logger.debug("Running in event loop context, using thread executor for database isolation")
                
                # Always use thread executor to avoid any possibility of loop conflicts
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(fetch_pat_sync)
                    return await asyncio.wrap_future(future)
                    
            except RuntimeError:
                # No event loop is running - we're in sync context
                logger.debug("No event loop detected, running synchronously")
                return fetch_pat_sync()
                
        except Exception as e:
            logger.warning(f"Database PAT lookup failed: {e}")
            # Fall back to None to continue with environment variables
            return None