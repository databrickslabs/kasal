from typing import Annotated, AsyncGenerator, Callable, Type

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.core.base_service import BaseService
from src.db.base import Base
from src.db.session import get_db
from src.services.log_service import LLMLogService
from src.repositories.log_repository import LLMLogRepository

# Type definitions for dependencies
SessionDep = Annotated[AsyncSession, Depends(get_db)]


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