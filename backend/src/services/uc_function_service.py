"""
Service for Unity Catalog function operations.

This module provides business logic for interacting with Databricks 
Unity Catalog functions through the UCClient.
"""

import logging
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.uc_client import UCClient
from src.schemas.uc_function import (
    UCFunction, 
    UCFunctionListResponse, 
    UCFunctionResponse,
    FunctionParameter
)

# Configure logging
logger = logging.getLogger(__name__)

class UCFunctionService:
    """Service for Unity Catalog function operations."""
    
    def __init__(self, db: AsyncSession):
        """
        Initialize service with database session.
        
        Args:
            db: AsyncSession for database operations
        """
        self.db = db
        # Will be configured to use mock mode based on env variables
        self.uc_client = UCClient()
    
    async def list_functions(self, catalog_name: str, schema_name: str) -> UCFunctionListResponse:
        """
        List all functions in the specified catalog and schema.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            
        Returns:
            UCFunctionListResponse: Response with list of functions
            
        Raises:
            Exception: If there's an error listing functions
        """
        logger.info(f"Listing functions in {catalog_name}.{schema_name}")
        try:
            functions_raw = self.uc_client.list_functions(catalog_name, schema_name)
            
            # Convert to our schema model
            functions = []
            for func in functions_raw:
                # Create function object
                function = UCFunction(
                    name=func.name,
                    comment=getattr(func, 'comment', None),
                    return_type=getattr(func, 'return_type', 'unknown'),
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    input_params=[]
                )
                
                # Add parameters if available
                if hasattr(func, 'input_params') and func.input_params:
                    for param in func.input_params:
                        param_obj = FunctionParameter(
                            name=getattr(param, 'name', 'unnamed'),
                            param_type=getattr(param, 'param_type', 'unknown'),
                            description=getattr(param, 'description', None)
                        )
                        function.input_params.append(param_obj)
                
                functions.append(function)
            
            logger.info(f"Found {len(functions)} functions in {catalog_name}.{schema_name}")
            return UCFunctionListResponse(
                functions=functions,
                count=len(functions),
                catalog_name=catalog_name,
                schema_name=schema_name
            )
        except Exception as e:
            logger.error(f"Error listing functions in {catalog_name}.{schema_name}: {str(e)}")
            raise
    
    async def get_function(self, catalog_name: str, schema_name: str, function_name: str) -> UCFunctionResponse:
        """
        Get details of a specific function.
        
        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            function_name: Name of the function
            
        Returns:
            UCFunctionResponse: Response with function details
            
        Raises:
            ValueError: If function not found
            Exception: If there's an error getting function details
        """
        logger.info(f"Getting function {catalog_name}.{schema_name}.{function_name}")
        try:
            func = self.uc_client.get_function_details(catalog_name, schema_name, function_name)
            
            # Convert to our schema model
            function = UCFunction(
                name=func.name,
                comment=getattr(func, 'comment', None),
                return_type=getattr(func, 'return_type', 'unknown'),
                catalog_name=catalog_name,
                schema_name=schema_name,
                input_params=[]
            )
            
            # Add parameters if available
            if hasattr(func, 'input_params') and func.input_params:
                for param in func.input_params:
                    param_obj = FunctionParameter(
                        name=getattr(param, 'name', 'unnamed'),
                        param_type=getattr(param, 'param_type', 'unknown'),
                        description=getattr(param, 'description', None)
                    )
                    function.input_params.append(param_obj)
            
            logger.info(f"Function {function_name} details retrieved")
            return UCFunctionResponse(
                function=function,
                catalog_name=catalog_name,
                schema_name=schema_name
            )
        except ValueError:
            # Re-raise ValueError for not found
            logger.warning(f"Function {function_name} not found in {catalog_name}.{schema_name}")
            raise
        except Exception as e:
            logger.error(f"Error getting function {catalog_name}.{schema_name}.{function_name}: {str(e)}")
            raise 