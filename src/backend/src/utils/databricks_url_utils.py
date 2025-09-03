"""
Utility module for normalizing and constructing Databricks URLs.

This module provides centralized URL handling to ensure consistent
construction of Databricks API endpoints throughout the application.
"""
import re
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DatabricksURLUtils:
    """Utility class for normalizing and constructing Databricks URLs."""
    
    @staticmethod
    def normalize_workspace_url(url: Optional[str]) -> Optional[str]:
        """
        Normalize a workspace URL to the base format without paths.
        
        This method ensures that:
        - The URL has the https:// protocol
        - Any path components (like /serving-endpoints) are removed
        - The result is just the base workspace URL
        
        Args:
            url: The workspace URL to normalize (can be in various formats)
            
        Returns:
            Normalized workspace URL (e.g., https://workspace.databricks.com) or None if invalid
            
        Examples:
            >>> DatabricksURLUtils.normalize_workspace_url("workspace.databricks.com")
            'https://workspace.databricks.com'
            >>> DatabricksURLUtils.normalize_workspace_url("https://workspace.databricks.com/serving-endpoints")
            'https://workspace.databricks.com'
        """
        if not url:
            return None
            
        # Remove whitespace
        url = url.strip()
        
        # Add https:// if missing
        if not url.startswith(('http://', 'https://')):
            url = f"https://{url}"
        
        # Remove any path components (including /serving-endpoints)
        # Extract just the protocol and hostname
        match = re.match(r'(https?://[^/]+)', url)
        if match:
            normalized_url = match.group(1)
            if url != normalized_url:
                logger.debug(f"Normalized URL from '{url}' to '{normalized_url}'")
            return normalized_url
        
        logger.warning(f"Could not normalize URL: {url}")
        return None
    
    @staticmethod
    def construct_serving_endpoints_url(workspace_url: Optional[str]) -> Optional[str]:
        """
        Construct the serving endpoints base URL from a workspace URL.
        
        This method normalizes the workspace URL first, then appends the
        /serving-endpoints path to create the base URL for all serving endpoint APIs.
        
        Args:
            workspace_url: The workspace URL (will be normalized)
            
        Returns:
            Full serving endpoints URL or None if invalid
            
        Example:
            >>> DatabricksURLUtils.construct_serving_endpoints_url("workspace.databricks.com")
            'https://workspace.databricks.com/serving-endpoints'
        """
        normalized_url = DatabricksURLUtils.normalize_workspace_url(workspace_url)
        if not normalized_url:
            return None
            
        serving_url = f"{normalized_url}/serving-endpoints"
        logger.debug(f"Constructed serving endpoints URL: {serving_url}")
        return serving_url
    
    @staticmethod
    def construct_model_invocation_url(
        workspace_url: Optional[str], 
        model_name: str,
        served_model_name: Optional[str] = None
    ) -> Optional[str]:
        """
        Construct the full model invocation URL.
        
        Supports both direct endpoint invocation and served model invocation formats:
        - Direct: /serving-endpoints/<endpoint_name>/invocations
        - Served: /serving-endpoints/<endpoint_name>/served-models/<served_model_name>/invocations
        
        Args:
            workspace_url: The workspace URL (will be normalized)
            model_name: The endpoint name (databricks/ prefix will be removed if present)
            served_model_name: Optional served model name for the served model format
            
        Returns:
            Full invocation URL or None if invalid
            
        Examples:
            >>> DatabricksURLUtils.construct_model_invocation_url("workspace.databricks.com", "databricks-gte-large-en")
            'https://workspace.databricks.com/serving-endpoints/databricks-gte-large-en/invocations'
        """
        serving_url = DatabricksURLUtils.construct_serving_endpoints_url(workspace_url)
        if not serving_url:
            return None
        
        # Clean model name of any provider prefixes
        clean_model_name = model_name.replace('databricks/', '') if model_name else ''
        
        if not clean_model_name:
            logger.warning("Model name is empty after cleaning")
            return None
        
        # Construct URL based on whether we have a served model name
        if served_model_name:
            invocation_url = f"{serving_url}/{clean_model_name}/served-models/{served_model_name}/invocations"
        else:
            invocation_url = f"{serving_url}/{clean_model_name}/invocations"
            
        logger.debug(f"Constructed invocation URL: {invocation_url}")
        return invocation_url
    
    @staticmethod
    def extract_workspace_from_endpoint(endpoint_url: Optional[str]) -> Optional[str]:
        """
        Extract the workspace URL from a full endpoint URL.
        
        This is useful when you have a full endpoint URL and need to get
        back to the base workspace URL.
        
        Args:
            endpoint_url: A full endpoint URL
            
        Returns:
            The workspace URL or None if invalid
            
        Example:
            >>> DatabricksURLUtils.extract_workspace_from_endpoint("https://workspace.databricks.com/serving-endpoints/model/invocations")
            'https://workspace.databricks.com'
        """
        if not endpoint_url:
            return None
            
        # First normalize to ensure we have a clean URL
        normalized = DatabricksURLUtils.normalize_workspace_url(endpoint_url)
        return normalized
    
    @staticmethod
    def validate_and_fix_environment() -> bool:
        """
        Validate and auto-fix Databricks environment variables.
        
        This method checks common environment variables and ensures they
        contain the correct format. It will auto-fix issues when possible.
        
        Returns:
            True if environment is valid (or was fixed), False otherwise
        """
        import os
        
        issues_found = False
        
        # Check DATABRICKS_HOST
        host = os.getenv("DATABRICKS_HOST")
        if host:
            if "/serving-endpoints" in host or "/api" in host:
                logger.warning(f"DATABRICKS_HOST contains path components: {host}")
                logger.info("Auto-correcting DATABRICKS_HOST to base workspace URL")
                normalized = DatabricksURLUtils.normalize_workspace_url(host)
                if normalized:
                    os.environ["DATABRICKS_HOST"] = normalized
                    logger.info(f"DATABRICKS_HOST corrected to: {normalized}")
                    issues_found = True
                else:
                    logger.error("Could not auto-correct DATABRICKS_HOST")
                    return False
        
        # Check DATABRICKS_ENDPOINT
        endpoint = os.getenv("DATABRICKS_ENDPOINT")
        if endpoint:
            # This one might legitimately contain /serving-endpoints
            # but let's ensure it's properly formatted
            if endpoint.count("/serving-endpoints") > 1:
                logger.warning(f"DATABRICKS_ENDPOINT has duplicate /serving-endpoints: {endpoint}")
                # Try to fix by normalizing and reconstructing
                workspace = DatabricksURLUtils.extract_workspace_from_endpoint(endpoint)
                if workspace:
                    fixed_endpoint = DatabricksURLUtils.construct_serving_endpoints_url(workspace)
                    if fixed_endpoint:
                        os.environ["DATABRICKS_ENDPOINT"] = fixed_endpoint
                        logger.info(f"DATABRICKS_ENDPOINT corrected to: {fixed_endpoint}")
                        issues_found = True
        
        if issues_found:
            logger.info("Environment variables were auto-corrected")
        else:
            logger.debug("Databricks environment variables are properly formatted")
            
        return True