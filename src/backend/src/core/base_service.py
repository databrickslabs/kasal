from typing import Generic, List, Optional, Type, TypeVar
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository, ModelType

# Define type for schema input
SchemaType = TypeVar("SchemaType")


class BaseService(Generic[ModelType, SchemaType]):
    """
    Base service class implementing common business logic operations.
    Services orchestrate operations using repositories and handle business rules.
    """

    def __init__(self, session):
        """
        Initialize the service with a session.
        
        Args:
            session: SQLAlchemy session (can be async or sync)
        """
        self.session = session
    
    async def get(self, id: int) -> Optional[ModelType]:
        """
        Get a single record by ID.
        
        Args:
            id: ID of the record to get
            
        Returns:
            The model instance if found, else None
        """
        repository = self.repository_class(self.model_class, self.session)
        return await repository.get(id)
    
    async def list(self, skip: int = 0, limit: int = 100) -> List[ModelType]:
        """
        Get multiple records with pagination.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of model instances
        """
        repository = self.repository_class(self.model_class, self.session)
        return await repository.list(skip, limit)
    
    async def create(self, obj_in: SchemaType) -> ModelType:
        """
        Create a new record.
        
        Args:
            obj_in: Schema object with values to create model with
            
        Returns:
            The created model instance
        """
        repository = self.repository_class(self.model_class, self.session)
        return await repository.create(obj_in.model_dump())
    
    async def update(self, id: int, obj_in: SchemaType) -> Optional[ModelType]:
        """
        Update an existing record.
        
        Args:
            id: ID of the record to update
            obj_in: Schema object with values to update model with
            
        Returns:
            The updated model instance if found, else None
        """
        repository = self.repository_class(self.model_class, self.session)
        return await repository.update(id, obj_in.model_dump(exclude_unset=True))
    
    async def delete(self, id: int) -> bool:
        """
        Delete a record by ID.
        
        Args:
            id: ID of the record to delete
            
        Returns:
            True if record was deleted, False if not found
        """
        repository = self.repository_class(self.model_class, self.session)
        return await repository.delete(id) 