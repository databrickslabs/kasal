"""
User and group context utility for handling user tokens and group identification.

This module provides utilities to extract and manage user access tokens
from HTTP headers, particularly for Databricks Apps where user tokens
are forwarded via the X-Forwarded-Access-Token header. It also handles
automatic group extraction from user email domains.
"""

import logging
from contextvars import ContextVar
from typing import Optional, Dict, Any
from dataclasses import dataclass
from fastapi import Request

logger = logging.getLogger(__name__)

# Context variable to store the current user's access token
_user_access_token: ContextVar[Optional[str]] = ContextVar('user_access_token', default=None)
_user_context: ContextVar[Optional[Dict[str, Any]]] = ContextVar('user_context', default=None)
_group_context: ContextVar[Optional['GroupContext']] = ContextVar('group_context', default=None)


@dataclass
class GroupContext:
    """
    Hybrid group context for multi-group data isolation.
    
    Supports two modes:
    1. Individual mode: Users not in any groups get their own private group
    2. Group mode: Users in groups see data from all their assigned groups
    
    This provides both individual privacy and team collaboration.
    """
    group_ids: Optional[list] = None      # All group IDs user belongs to
    group_email: Optional[str] = None     # e.g., "alice@acme-corp.com"
    email_domain: Optional[str] = None    # e.g., "acme-corp.com"
    user_id: Optional[str] = None         # User ID if available
    access_token: Optional[str] = None    # Databricks access token
    
    @property
    def primary_group_id(self) -> Optional[str]:
        """
        Get the primary (first) group ID for creating new data.
        
        This will be either:
        - The user's individual group ID (if not in any groups)
        - The first group group ID (if in groups)
        """
        return self.group_ids[0] if self.group_ids and len(self.group_ids) > 0 else None
    
    @classmethod
    async def from_email(cls, email: str, access_token: str = None, user_id: str = None) -> 'GroupContext':
        """Create GroupContext from user email with hybrid individual/group-based groups."""
        if not email or "@" not in email:
            return cls()
        
        email_domain = email.split("@")[1]
        
        # Get user's group memberships from group management system
        try:
            user_group_ids = await cls._get_user_group_memberships(email)
            
            if not user_group_ids or len(user_group_ids) == 0:
                # User is NOT in any groups - use individual groups
                # Create a unique group ID based on the user's email (sanitized)
                individual_group_id = cls.generate_individual_group_id(email)
                logger.info(f"User {email} not in any groups, using individual group: {individual_group_id}")
                user_group_ids = [individual_group_id]
            else:
                # User IS in groups - use group-based groups
                logger.info(f"User {email} belongs to groups: {user_group_ids}")
            
            return cls(
                group_ids=user_group_ids,
                group_email=email,
                email_domain=email_domain,
                user_id=user_id,
                access_token=access_token
            )
        except Exception as e:
            # Fallback to individual groups if group lookup fails
            logger.warning(f"Failed to lookup user groups for {email}, falling back to individual groups: {e}")
            individual_group_id = cls.generate_individual_group_id(email)
            return cls(
                group_ids=[individual_group_id],
                group_email=email,
                email_domain=email_domain,
                user_id=user_id,
                access_token=access_token
            )
    
    @staticmethod
    def generate_group_id(email_domain: str) -> str:
        """
        Generate group ID from email domain.
        
        Examples:
        - acme-corp.com -> acme_corp
        - tech.startup.io -> tech_startup_io
        """
        return email_domain.replace(".", "_").replace("-", "_").lower()
    
    @staticmethod
    def generate_individual_group_id(email: str) -> str:
        """
        Generate individual group ID from user email for isolated access.
        
        Examples:
        - alice@company.com -> user_alice_company_com
        - bob.smith@startup.io -> user_bob_smith_startup_io
        """
        # Sanitize the full email for use as group ID
        sanitized = email.replace("@", "_").replace(".", "_").replace("-", "_").replace("+", "_")
        return f"user_{sanitized}".lower()
    
    def is_valid(self) -> bool:
        """Check if group context is valid."""
        return bool(self.group_ids and len(self.group_ids) > 0 and self.email_domain)
    
    @staticmethod
    async def _get_user_group_memberships(email: str) -> list:
        """
        Get list of group IDs that the user belongs to.
        
        Args:
            email: User email address
            
        Returns:
            List of group IDs the user is a member of
        """
        try:
            # Import here to avoid circular imports
            from src.services.group_service import GroupService
            from src.db.session import async_session_factory
            
            async with async_session_factory() as session:
                group_service = GroupService(session)
                user_groups = await group_service.get_user_group_memberships(email)
                return [group.id for group in user_groups]
                
        except Exception as e:
            logger.error(f"Error getting user group memberships for {email}: {e}")
            return []


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
    def set_group_context(group_context: GroupContext) -> None:
        """
        Set the current group context.
        
        Args:
            group_context: GroupContext object
        """
        _group_context.set(group_context)
        logger.debug(f"Group context set: {group_context.primary_group_id}")
    
    @staticmethod
    def get_group_context() -> Optional[GroupContext]:
        """
        Get the current group context.
        
        Returns:
            GroupContext object if available, None otherwise
        """
        return _group_context.get()
    
    @staticmethod
    def clear_context() -> None:
        """Clear all user context information."""
        _user_access_token.set(None)
        _user_context.set(None)
        _group_context.set(None)
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


async def extract_group_context_from_request(request: Request) -> Optional[GroupContext]:
    """
    Extract group context from HTTP request headers.
    
    Uses X-Forwarded-Email header from Databricks Apps to determine group.
    
    Args:
        request: FastAPI Request object
        
    Returns:
        GroupContext object if group can be determined, None otherwise
    """
    try:
        # Extract email from Databricks Apps header
        email = request.headers.get('X-Forwarded-Email')
        if not email:
            logger.debug("No X-Forwarded-Email header found")
            return None
        
        # Extract access token
        access_token = extract_user_token_from_request(request)
        
        # Create group context from email
        group_context = await GroupContext.from_email(email, access_token)
        
        if group_context.is_valid():
            logger.debug(f"Extracted group context: {group_context.primary_group_id}, groups: {group_context.group_ids}")
            return group_context
        else:
            logger.debug(f"Invalid group context extracted from email: {email}")
            return None
            
    except Exception as e:
        logger.error(f"Error extracting group context from request: {e}")
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
        
        # Extract Databricks Apps email
        email = request.headers.get('X-Forwarded-Email')
        if email:
            context['email'] = email
        
        # Extract other relevant headers
        user_agent = request.headers.get('User-Agent')
        if user_agent:
            context['user_agent'] = user_agent
        
        # Extract Databricks-specific headers if present
        databricks_headers = {}
        for header_name, header_value in request.headers.items():
            if header_name.lower().startswith('x-databricks-') or header_name.lower().startswith('x-forwarded-'):
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
    Middleware to extract and set user and group context from HTTP headers.
    
    This middleware extracts both user context and group context from Databricks Apps headers.
    It works whether or not Databricks Apps is enabled, but provides richer context when it is.
    
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
        
        # Extract group context from X-Forwarded-Email if present
        group_context = await extract_group_context_from_request(request)
        if group_context:
            UserContext.set_group_context(group_context)
            logger.debug(f"Group context middleware: Set group groups {group_context.group_ids}")
        
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
        # Clear context after request processing
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