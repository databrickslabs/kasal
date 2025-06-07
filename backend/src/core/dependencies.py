from typing import Annotated, AsyncGenerator, Callable, Type, Optional

from fastapi import Depends, Request, Header
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.core.base_service import BaseService
from src.db.base import Base
from src.db.session import get_db
from src.services.log_service import LLMLogService
from src.repositories.log_repository import LLMLogRepository
from src.utils.user_context import GroupContext

# Type definitions for dependencies
SessionDep = Annotated[AsyncSession, Depends(get_db)]




async def get_group_context(
    request: Request,
    x_forwarded_email: Optional[str] = Header(None, alias="X-Forwarded-Email"),
    x_forwarded_access_token: Optional[str] = Header(None, alias="X-Forwarded-Access-Token"),
    x_auth_request_email: Optional[str] = Header(None, alias="X-Auth-Request-Email"),
    x_auth_request_user: Optional[str] = Header(None, alias="X-Auth-Request-User"),
    x_auth_request_access_token: Optional[str] = Header(None, alias="X-Auth-Request-Access-Token")
) -> GroupContext:
    """
    Extract group context from Databricks Apps or OAuth2-Proxy headers.
    
    For Databricks Apps deployment with OAuth2-Proxy, this extracts group information from:
    - X-Auth-Request-Email: User email from OAuth2-Proxy (preferred)
    - X-Forwarded-Email: User email from Databricks Apps (fallback)
    - X-Auth-Request-Access-Token: Access token from OAuth2-Proxy (preferred)
    - X-Forwarded-Access-Token: Access token from Databricks Apps (fallback)
    
    Args:
        request: FastAPI request object
        x_forwarded_email: User email from Databricks Apps
        x_forwarded_access_token: Access token from Databricks Apps
        x_auth_request_email: User email from OAuth2-Proxy
        x_auth_request_user: Username from OAuth2-Proxy
        x_auth_request_access_token: Access token from OAuth2-Proxy
        
    Returns:
        GroupContext: Extracted group context with group_id, email, etc.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Prefer OAuth2-Proxy headers over direct headers
    user_email = x_auth_request_email or x_forwarded_email
    access_token = x_auth_request_access_token or x_forwarded_access_token
    
    print(f"DEBUG: get_group_context called with:")
    print(f"  X-Auth-Request-Email: {x_auth_request_email}")
    print(f"  X-Forwarded-Email: {x_forwarded_email}")
    print(f"  Final email: {user_email}")
    
    if user_email:
        group_context = await GroupContext.from_email(
            email=user_email,
            access_token=access_token
        )
        print(f"DEBUG: Created group context: primary_group_id={group_context.primary_group_id}, group_ids={group_context.group_ids}, email={group_context.group_email}")
        return group_context
    
    # Fallback: No group context available (single-group mode)
    print("DEBUG: No email header found, returning empty group context")
    return GroupContext()


# Type definitions for group-aware dependencies
GroupContextDep = Annotated[GroupContext, Depends(get_group_context)]


def get_repository(
    repository_class: Type[BaseRepository], model_class: Type[Base]
) -> Callable[[SessionDep], BaseRepository]:
    """
    Factory function for repository dependencies.
    
    Args:
        repository_class: Repository class to instantiate
        model_class: Model class to use with the repository
        
    Returns:
        Callable: Dependency function that returns a repository instance
    """
    
    def _get_repo(session: SessionDep) -> BaseRepository:
        return repository_class(model_class, session)
    
    return _get_repo


def get_service(
    service_class: Type[BaseService],
    repository_class: Type[BaseRepository],
    model_class: Type[Base],
) -> Callable[[SessionDep], BaseService]:
    """
    Factory function for service dependencies.
    
    Args:
        service_class: Service class to instantiate
        repository_class: Repository class to use with the service
        model_class: Model class to use with the repository
        
    Returns:
        Callable: Dependency function that returns a service instance
    """
    
    def _get_service(session: SessionDep) -> BaseService:
        # The consistent pattern across services is to have session as the first parameter,
        # with repository_class and model_class as optional parameters with defaults
        try:
            # Create service with session and default repo/model classes
            service = service_class(session)
            return service
        except Exception as e:
            # Handle any initialization errors with fallback
            # If the service expects additional parameters, this will catch it
            try:
                # Try creating with explicit repository and model classes
                service = service_class(
                    session=session,
                    repository_class=repository_class, 
                    model_class=model_class
                )
                return service
            except Exception as inner_e:
                # Log the error and re-raise
                print(f"Error creating service: {inner_e}")
                raise
    
    return _get_service 

def get_log_service() -> LLMLogService:
    """
    Factory function for creating the log service with its dependencies.
    This should be called at application startup to create a singleton service instance.
    
    Returns:
        LLMLogService: Singleton instance of the log service
    """
    # Create a single repository instance that will be shared
    repository = LLMLogRepository()
    # Create and return the service with the repository
    return LLMLogService(repository) 