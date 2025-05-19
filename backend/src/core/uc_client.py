"""
Unity Catalog client module.

This module provides utilities to interact with Databricks Unity Catalog,
allowing operations like listing functions and retrieving function details.
"""

import os
import logging
from typing import List, Optional, Dict, Any
from databricks.sdk import WorkspaceClient
from src.db.session import SessionLocal
from src.utils.databricks_utils import setup_databricks_token, get_databricks_config

# Configure logging
logger = logging.getLogger(__name__)

class MockFunction:
    """Mock function class for development/testing without Databricks connection."""
    
    def __init__(self, name: str, comment: str = None):
        self.name = name
        self.comment = comment
        self.return_type = "string"
        self.input_params = []


class UCClient:
    """Unity Catalog client for interacting with Databricks Unity Catalog."""
    
    def __init__(self, host: Optional[str] = None, token: Optional[str] = None, mock_mode: bool = False):
        """
        Initialize Unity Catalog client.
        
        Args:
            host: Databricks host URL
            token: Databricks API token
            mock_mode: Whether to use mock mode for development
        """
        self.mock_mode = mock_mode or os.getenv("UC_MOCK_MODE", "").lower() == "true"
        logger.info(f"Initializing Unity Catalog client in {'mock' if self.mock_mode else 'live'} mode")
        
        if not self.mock_mode:
            self.client = self.initialize_uc_client(host, token)

    def initialize_uc_client(self, host: Optional[str] = None, token: Optional[str] = None):
        """
        Initialize Unity Catalog client and set it as global client.
        
        Args:
            host: Databricks host URL
            token: Databricks API token
            
        Returns:
            WorkspaceClient: Initialized Databricks client
            
        Raises:
            ValueError: If host and token are not provided
        """
        try:
            # Use provided host and token if available
            databricks_host = host
            databricks_token = token
            
            # If not provided, try to get from database configuration
            if not databricks_host or not databricks_token:
                # Get session
                db = SessionLocal()
                try:
                    # Get Databricks configuration
                    config = get_databricks_config(db)
                    if config and hasattr(config, 'workspace_url') and config.workspace_url:
                        databricks_host = config.workspace_url
                    
                    # Set up the token using our utility that handles API keys and Databricks secrets
                    setup_databricks_token(db)
                    # Get the token from environment after setting it up
                    databricks_token = os.getenv("DATABRICKS_TOKEN")
                finally:
                    db.close()
            
            # Fall back to environment variables if still not available
            databricks_host = databricks_host or os.getenv("DATABRICKS_HOST")
            databricks_token = databricks_token or os.getenv("DATABRICKS_TOKEN")

            if not databricks_host or not databricks_token:
                raise ValueError("Databricks host and token must be provided either as parameters, "
                                "through database configuration, or through environment variables")

            logger.info(f"Initializing Databricks client with host: {databricks_host}")
            client = WorkspaceClient(host=databricks_host, token=databricks_token, auth_type="pat")
            logger.info("Successfully initialized Databricks client")
            return client
        except Exception as e:
            logger.error(f"Error initializing UC client: {str(e)}")
            raise

    def list_functions(self, catalog_name: str, schema_name: str) -> List[Any]:
        """
        List functions in a specific catalog and schema.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            
        Returns:
            List[Any]: List of function objects
            
        Raises:
            Exception: If there's an error listing functions
        """
        if self.mock_mode:
            # Return mock data for development
            logger.info(f"Mock mode: Returning mock functions for {catalog_name}.{schema_name}")
            return [
                MockFunction("example_function_1", "This is an example function"),
                MockFunction("example_function_2", "Another example function"),
            ]

        try:
            logger.info(f"Listing functions in {catalog_name}.{schema_name}")
            functions = self.client.functions.list(
                catalog_name=catalog_name,
                schema_name=schema_name
            )
            functions_list = list(functions)
            logger.info(f"Found {len(functions_list)} functions in {catalog_name}.{schema_name}")
            return functions_list
        except Exception as e:
            logger.error(f"Error listing functions in {catalog_name}.{schema_name}: {str(e)}")
            raise

    def get_function_details(self, catalog_name: str, schema_name: str, function_name: str) -> Any:
        """
        Get details of a specific function.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            function_name: Name of the function
            
        Returns:
            Any: Function object
            
        Raises:
            ValueError: If function not found
            Exception: If there's an error getting function details
        """
        if self.mock_mode:
            # Return mock data for the specified function
            logger.info(f"Mock mode: Returning mock function details for {catalog_name}.{schema_name}.{function_name}")
            mock_functions = {
                "example_function_1": MockFunction("example_function_1", "This is an example function"),
                "example_function_2": MockFunction("example_function_2", "Another example function"),
            }
            if function_name in mock_functions:
                return mock_functions[function_name]
            logger.warning(f"Mock mode: Function {function_name} not found")
            raise ValueError(f"Function {function_name} not found")

        try:
            logger.info(f"Getting details for function {catalog_name}.{schema_name}.{function_name}")
            functions = self.list_functions(catalog_name, schema_name)
            for func in functions:
                if func.name == function_name:
                    logger.info(f"Found function {function_name}")
                    return func
            logger.warning(f"Function {function_name} not found in {catalog_name}.{schema_name}")
            raise ValueError(f"Function {function_name} not found in {catalog_name}.{schema_name}")
        except ValueError:
            # Re-raise ValueError for not found
            raise
        except Exception as e:
            logger.error(f"Error getting details for function {catalog_name}.{schema_name}.{function_name}: {str(e)}")
            raise 