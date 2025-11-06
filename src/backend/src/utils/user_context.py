"""Multi-tenant user and group context management for authentication and isolation.

This module provides comprehensive utilities for managing user authentication
contexts and group-based multi-tenant data isolation in the AI agent system.
It handles access token extraction from HTTP headers, group membership management,
and context propagation throughout the execution pipeline.

The module supports hybrid isolation modes:
- Individual mode: Users without groups get private data spaces
- Group mode: Users in groups share data within their assigned groups

Key Features:
    - Access token extraction from HTTP headers (Databricks integration)
    - Automatic group assignment based on email domains
    - Hybrid individual/group isolation modes
    - Context variable management for request-scoped data
    - Group membership lookup and caching

Architecture:
    Uses Python's contextvars for thread-safe, async-compatible context
    management across the entire request lifecycle.

Example:
    >>> context = await GroupContext.from_email(
    ...     email="user@company.com",
    ...     access_token="token_123"
    ... )
    >>> UserContext.set_group_context(context)
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
    """Hybrid group context for multi-tenant data isolation and access control.

    This class manages group-based data isolation with support for both individual
    and collaborative access modes. It provides the foundation for multi-tenant
    data segregation throughout the application.

    Supports two isolation modes:
        1. Individual mode: Users not in any groups get their own private group
           for complete data isolation
        2. Group mode: Users in groups can access data from all their assigned
           groups, enabling team collaboration

    Attributes:
        group_ids: List of all group IDs the user belongs to
        group_email: User's email address for identification
        email_domain: Domain extracted from email for organization mapping
        user_id: Optional user identifier from authentication system
        access_token: Databricks or OAuth access token for API calls
        user_role: User's role in the primary group (admin, editor, operator)
        highest_role: User's highest role across ALL groups (for authorization)
        current_user: The User model instance with permission fields

    Properties:
        primary_group_id: Returns the first group ID for creating new data

    Example:
        >>> context = await GroupContext.from_email(
        ...     email="alice@acme-corp.com",
        ...     access_token="Bearer token_123"
        ... )
        >>> print(context.primary_group_id)  # "acme_corp" or "user_alice_acme_corp_com"
        >>> print(context.user_role)  # "editor"

    Note:
        The context is designed to be immutable once created and should be
        passed through the execution pipeline for consistent isolation.
    """
    group_ids: Optional[list] = None      # All group IDs user belongs to
    group_email: Optional[str] = None     # e.g., "alice@acme-corp.com"
    email_domain: Optional[str] = None    # e.g., "acme-corp.com"
    user_id: Optional[str] = None         # User ID if available
    access_token: Optional[str] = None    # Databricks access token
    user_role: Optional[str] = None       # User's role in primary group (admin/editor/operator)
    highest_role: Optional[str] = None    # User's highest role across ALL groups (for authorization)
    current_user: Optional[Any] = None    # User model instance with permission fields
    
    @property
    def primary_group_id(self) -> Optional[str]:
        """
        Get the primary (first) group ID for creating new data.
        
        This will be either:
        - The user's individual group ID (if not in any groups)
        - The first group group ID (if in groups)
        """
        return self.group_ids[0] if self.group_ids and len(self.group_ids) > 0 else None
    
    def to_dict(self) -> dict:
        """Convert GroupContext to a dictionary for serialization."""
        return {
            'group_ids': self.group_ids,
            'group_email': self.group_email,
            'email_domain': self.email_domain,
            'user_id': self.user_id,
            'access_token': self.access_token,
            'user_role': self.user_role,  # Include user role
            'highest_role': self.highest_role,  # Include highest role across all groups
            'primary_group_id': self.primary_group_id,  # Include computed property
            'group_id': self.primary_group_id,  # Alias for backward compatibility
            # Note: current_user is not included as it's not JSON serializable
            'is_system_admin': getattr(self.current_user, 'is_system_admin', False) if self.current_user else False,
            'is_personal_workspace_manager': getattr(self.current_user, 'is_personal_workspace_manager', False) if self.current_user else False
        }
    
    @classmethod
    async def from_email(cls, email: str, access_token: str = None, user_id: str = None, group_id: str = None) -> 'GroupContext':
        """Create GroupContext from user email with hybrid individual/group-based groups.

        Args:
            email: User's email address
            access_token: Optional access token
            user_id: Optional user ID
            group_id: Optional specific group ID to use (from group_id header)
        """
        if not email or "@" not in email:
            return cls()

        email_domain = email.split("@")[1]

        # Get user's group memberships from group management system
        try:
            user, user_groups_with_roles = await cls._get_user_group_memberships_with_roles(email)

            if not user_groups_with_roles or len(user_groups_with_roles) == 0:
                # User is NOT in any groups - use individual groups
                # Create a unique group ID based on the user's email (sanitized)
                individual_group_id = cls.generate_individual_group_id(email)
                logger.info(f"User {email} not in any groups, using individual group: {individual_group_id}")
                user_group_ids = [individual_group_id]
                user_role = None  # No role for individual workspace
                highest_role = None  # No highest role when not in any groups
            else:
                # User IS in groups - use group-based groups
                user_group_ids = []
                roles_by_group = {}
                for group, role in user_groups_with_roles:
                    user_group_ids.append(group.id)
                    roles_by_group[group.id] = role

                # Determine user's highest role across ALL groups (for authorization)
                highest_role = None
                if any(role == "admin" for group, role in user_groups_with_roles):
                    highest_role = "admin"
                elif any(role == "editor" for group, role in user_groups_with_roles):
                    highest_role = "editor"
                elif any(role == "operator" for group, role in user_groups_with_roles):
                    highest_role = "operator"
                else:
                    highest_role = user_groups_with_roles[0][1] if user_groups_with_roles else None

                # If a specific group_id was provided, validate it
                if group_id:
                    # Check if it's a regular group the user belongs to
                    if group_id in roles_by_group:
                        user_role = roles_by_group[group_id]
                        # Put the selected group first in the list
                        user_group_ids = [group_id] + [gid for gid in user_group_ids if gid != group_id]
                    # Check if it's a personal workspace
                    elif group_id.startswith("user_"):
                        # Validate that the personal workspace matches the user's email
                        expected_personal_workspace = cls.generate_individual_group_id(email)
                        if group_id != expected_personal_workspace:
                            # SECURITY: Reject unauthorized personal workspace access
                            logger.warning(f"SECURITY: User {email} attempted to access unauthorized personal workspace {group_id}")
                            raise ValueError(f"Access denied: User does not have access to group {group_id}")

                        # Personal workspace selected (e.g., user_admin_admin_com)
                        # For personal workspaces, inherit the highest role for authorization
                        # but keep data isolated to personal workspace
                        user_role = highest_role  # Use highest role for authorization

                        # Add the personal workspace as primary for data filtering
                        # This ensures data isolation to personal workspace
                        user_group_ids = [group_id] + user_group_ids
                        logger.info(f"Personal workspace {group_id} selected for {email}, using highest role: {user_role}")
                    else:
                        # SECURITY: Reject unauthorized group access
                        logger.warning(f"SECURITY: User {email} attempted to access unauthorized group {group_id}")
                        raise ValueError(f"Access denied: User does not have access to group {group_id}")
                else:
                    # No specific group_id provided - use the role from the first group
                    user_role = user_groups_with_roles[0][1] if user_groups_with_roles else None

                logger.info(f"User {email} belongs to groups: {user_group_ids} with role: {user_role}, highest role: {highest_role}")

            return cls(
                group_ids=user_group_ids,
                group_email=email,
                email_domain=email_domain,
                user_id=user_id if user_id else (user.id if user else None),
                access_token=access_token,
                user_role=user_role,
                highest_role=highest_role,
                current_user=user  # Include the user object with permission fields
            )
        except ValueError:
            # SECURITY: Re-raise authorization errors - do not fallback
            raise
        except Exception as e:
            # Fallback to individual groups if group lookup fails (non-authorization errors)
            logger.warning(f"Failed to lookup user groups for {email}, falling back to individual groups: {e}")
            individual_group_id = cls.generate_individual_group_id(email)
            return cls(
                group_ids=[individual_group_id],
                group_email=email,
                email_domain=email_domain,
                user_id=user_id,
                access_token=access_token,
                user_role=None,
                highest_role=None,
                current_user=None  # No user object in fallback case
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
        Auto-creates the user if they don't exist (proxy authentication).

        Args:
            email: User email address

        Returns:
            List of group IDs the user is a member of
        """
        try:
            # Import here to avoid circular imports
            from src.services.group_service import GroupService
            from src.services.user_service import UserService
            from src.db.session import async_session_factory

            async with async_session_factory() as session:
                # Get or create the user
                user_service = UserService(session)
                user = await user_service.get_or_create_user_by_email(email)

                if not user:
                    logger.error(f"Failed to get or create user for email: {email}")
                    return []

                # Commit the session to ensure user and any groups are saved
                await session.commit()

                group_service = GroupService(session)
                user_groups = await group_service.get_user_group_memberships(email)

                # Commit any pending changes
                await session.commit()

                return [group.id for group in user_groups]

        except Exception as e:
            logger.error(f"Error getting user group memberships for {email}: {e}")
            return []

    @staticmethod
    async def _get_user_group_memberships_with_roles(email: str) -> tuple:
        """
        Get list of groups and roles that the user belongs to, along with the user object.
        Auto-creates the user if they don't exist (proxy authentication).

        Args:
            email: User email address

        Returns:
            Tuple of (user, groups_with_roles) where groups_with_roles is a list of tuples containing (group, role)
        """
        try:
            # Import here to avoid circular imports
            from src.services.group_service import GroupService
            from src.services.user_service import UserService
            from src.db.session import async_session_factory

            async with async_session_factory() as session:
                # Get or create the user
                logger.info(f"[USER CONTEXT DEBUG] Creating UserService and calling get_or_create_user_by_email for {email}")
                user_service = UserService(session)
                # Don't update last_login to prevent locking
                user = await user_service.get_or_create_user_by_email(email, update_login=False)
                logger.info(f"[USER CONTEXT DEBUG] get_or_create_user_by_email returned user: {user.email if user else 'None'}, is_system_admin: {user.is_system_admin if user else 'N/A'}")

                if not user:
                    logger.error(f"Failed to get or create user for email: {email}")
                    return None, []

                group_service = GroupService(session)
                # This returns list of tuples: (group, role)
                groups_with_roles = await group_service.get_user_groups_with_roles(user.id)

                # Single commit at the end for all changes
                await session.commit()
                logger.info(f"[USER CONTEXT DEBUG] Session committed for user {user.email}")

                return user, groups_with_roles

        except Exception as e:
            logger.error(f"Error getting user group memberships with roles for {email}: {e}")
            return None, []


class UserContext:
    """Thread-safe user context manager for authentication and authorization.
    
    This class provides static methods for managing user authentication context
    throughout the request lifecycle. It uses Python's contextvars to maintain
    thread-safe, async-compatible context that follows the request through all
    layers of the application.
    
    The context manager handles:
    - User access token storage and retrieval
    - User metadata and profile information
    - Group context for multi-tenant isolation
    - Request-scoped context propagation
    
    All methods are static as the class acts as a namespace for context
    operations, with the actual state stored in context variables.
    
    Example:
        >>> # Set context at request entry
        >>> UserContext.set_user_token("Bearer token_123")
        >>> UserContext.set_group_context(group_context)
        >>> 
        >>> # Retrieve context anywhere in the request
        >>> token = UserContext.get_user_token()
        >>> group = UserContext.get_group_context()
    
    Note:
        Context is automatically cleaned up at the end of each request/task
        by the async runtime.
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
    try:
        # Extract group context from X-Forwarded-Email if present
        try:
            group_context = await extract_group_context_from_request(request)
            if group_context:
                UserContext.set_group_context(group_context)
                logger.debug(f"Group context middleware: Set group groups {group_context.group_ids}")
        except Exception as group_error:
            # Handle greenlet_spawn errors during async context initialization
            error_msg = str(group_error)
            if "greenlet_spawn" in error_msg or "await_only" in error_msg:
                logger.warning(f"Async context not ready for group context extraction (likely startup): {group_error}")
            else:
                logger.error(f"Error extracting group context: {group_error}")
            # Continue without group context - it will be created on next request

        # Always extract user context from request headers if present (for OBO authentication)
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