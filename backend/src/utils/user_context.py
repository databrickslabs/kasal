"""
User context utility for handling user tokens from HTTP headers.

This module provides utilities to extract and manage user access tokens
from HTTP headers, particularly for Databricks Apps where user tokens
are forwarded via the X-Forwarded-Access-Token header.
"""

import logging
from contextvars import ContextVar
from typing import Optional, Dict, Any
from fastapi import Request

logger = logging.getLogger(__name__)

# Context variable to store the current user's access token
_user_access_token: ContextVar[Optional[str]] = ContextVar('user_access_token', default=None)
_user_context: ContextVar[Optional[Dict[str, Any]]] = ContextVar('user_context', default=None)


class UserContext:
    """
    Manages user context including access tokens extracted from HTTP headers.
    """
    
    @staticmethod
    def set_user_token(token: str) -> None:
        """
        Set the current user's access token in context.
        
        Args:
            token: The user's access token
        """
        _user_access_token.set(token)
        logger.debug("User access token set in context")
    
    @staticmethod
    def get_user_token() -> Optional[str]:
        """
        Get the current user's access token from context.
        
        Returns:
            The user's access token if available, None otherwise
        """
        return _user_access_token.get()
    
    @staticmethod
    def set_user_context(context: Dict[str, Any]) -> None:
        """
        Set the current user's context information.
        
        Args:
            context: Dictionary containing user context information
        """
        _user_context.set(context)
        logger.debug(f"User context set: {list(context.keys())}")
    
    @staticmethod
    def get_user_context() -> Optional[Dict[str, Any]]:
        """
        Get the current user's context information.
        
        Returns:
            Dictionary containing user context information if available, None otherwise
        """
        return _user_context.get()
    
    @staticmethod
    def clear_context() -> None:
        """Clear all user context information."""
        _user_access_token.set(None)
        _user_context.set(None)
        logger.debug("User context cleared")


def extract_user_token_from_request(request: Request) -> Optional[str]:
    """
    Extract user access token from HTTP request headers.
    
    This function looks for the X-Forwarded-Access-Token header that
    Databricks Apps uses to forward user access tokens.
    
    Args:
        request: FastAPI Request object
        
    Returns:
        User access token if found in headers, None otherwise
    """
    try:
        # Check for Databricks Apps forwarded token
        forwarded_token = request.headers.get('X-Forwarded-Access-Token')
        if forwarded_token:
            logger.debug("Found X-Forwarded-Access-Token header")
            return forwarded_token
        
        # Check for standard Authorization header as fallback
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[7:]  # Remove 'Bearer ' prefix
            logger.debug("Found Authorization Bearer token")
            return token
        
        logger.debug("No user access token found in request headers")
        return None
        
    except Exception as e:
        logger.error(f"Error extracting user token from request: {e}")
        return None


def extract_user_context_from_request(request: Request) -> Dict[str, Any]:
    """
    Extract user context information from HTTP request headers.
    
    Args:
        request: FastAPI Request object
        
    Returns:
        Dictionary containing user context information
    """
    context = {}
    
    try:
        # Extract user token
        user_token = extract_user_token_from_request(request)
        if user_token:
            context['access_token'] = user_token
        
        # Extract other relevant headers
        user_agent = request.headers.get('User-Agent')
        if user_agent:
            context['user_agent'] = user_agent
        
        # Extract Databricks-specific headers if present
        databricks_headers = {}
        for header_name, header_value in request.headers.items():
            if header_name.lower().startswith('x-databricks-'):
                databricks_headers[header_name] = header_value
        
        if databricks_headers:
            context['databricks_headers'] = databricks_headers
        
        # Extract request metadata
        context['client_host'] = getattr(request.client, 'host', None) if request.client else None
        context['method'] = request.method
        context['url'] = str(request.url)
        
        logger.debug(f"Extracted user context with keys: {list(context.keys())}")
        return context
        
    except Exception as e:
        logger.error(f"Error extracting user context from request: {e}")
        return {}


async def user_context_middleware(request: Request, call_next):
    """
    Middleware to extract and set user context from HTTP headers.
    
    This middleware is only active when Databricks Apps is enabled.
    It extracts user context from incoming requests when apps_enabled is true.
    
    Args:
        request: FastAPI Request object
        call_next: Next middleware/handler in the chain
        
    Returns:
        Response from the next handler
    """
    apps_enabled = False
    try:
        # Check if Databricks Apps is enabled before processing user context
        apps_enabled = await _is_databricks_apps_enabled()
        
        if apps_enabled:
            # Extract user context from request
            user_context = extract_user_context_from_request(request)
            
            # Set context for this request
            if user_context:
                UserContext.set_user_context(user_context)
                
                # Set user token separately for easy access
                if 'access_token' in user_context:
                    UserContext.set_user_token(user_context['access_token'])
                    logger.debug("User context middleware: Set user token from X-Forwarded-Access-Token")
        
        # Process the request
        response = await call_next(request)
        
        return response
        
    except Exception as e:
        logger.error(f"Error in user context middleware: {e}")
        # Clear context and continue
        UserContext.clear_context()
        return await call_next(request)
    
    finally:
        # Clear context after request processing if it was set
        if apps_enabled:
            UserContext.clear_context()


async def _is_databricks_apps_enabled() -> bool:
    """
    Check if Databricks Apps is enabled in the configuration.
    
    Returns:
        True if apps_enabled is true in Databricks config, False otherwise
    """
    try:
        from src.services.databricks_service import DatabricksService
        from src.core.unit_of_work import UnitOfWork
        
        async with UnitOfWork() as uow:
            service = await DatabricksService.from_unit_of_work(uow)
            config = await service.get_databricks_config()
            
            if config and hasattr(config, 'apps_enabled'):
                return config.apps_enabled
            
            return False
            
    except Exception as e:
        logger.debug(f"Could not check Databricks apps_enabled status: {e}")
        return False


def is_databricks_app_context() -> bool:
    """
    Check if we're running in a Databricks App context.
    
    This can be determined by checking if we have a user token
    from the X-Forwarded-Access-Token header.
    
    Returns:
        True if running in Databricks App context, False otherwise
    """
    user_context = UserContext.get_user_context()
    if not user_context:
        return False
    
    # Check if we have databricks-specific headers or forwarded token
    return (
        'access_token' in user_context and
        ('databricks_headers' in user_context or 
         any('databricks' in key.lower() for key in user_context.keys()))
    )