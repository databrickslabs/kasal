from typing import Generic, List, Optional, Type, TypeVar, Union
import uuid
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.base import Base

# Define generic type for models
ModelType = TypeVar("ModelType", bound=Base)
IdType = Union[int, uuid.UUID]  # Support both int and UUID primary keys


class BaseRepository(Generic[ModelType]):
    """
    Base class for all repositories implementing common CRUD operations.
    """

    def __init__(self, model: Type[ModelType], session: AsyncSession):
        """
        Initialize repository with model and session.
        
        Args:
            model: SQLAlchemy model class
            session: SQLAlchemy async session
        """
        self.model = model
        self.session = session

    async def get(self, id: IdType) -> Optional[ModelType]:
        """
        Get a single record by ID.
        
        Args:
            id: ID of the record to get (can be int or UUID)
            
        Returns:
            The model instance if found, else None
        """
        try:
            query = select(self.model).where(self.model.id == id)
            result = await self.session.execute(query)
            return result.scalars().first()
        except Exception as e:
            await self.session.rollback()
            raise

    async def list(self, skip: int = 0, limit: int = 100) -> List[ModelType]:
        """
        Get multiple records with pagination.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of model instances
        """
        try:
            query = select(self.model).offset(skip).limit(limit)
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            await self.session.rollback()
            raise

    async def create(self, obj_in: dict) -> ModelType:
        """
        Create a new record.
        
        Args:
            obj_in: Dictionary of values to create model with
            
        Returns:
            The created model instance
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.debug(f"Creating new {self.model.__name__} with data: {obj_in}")
            db_obj = self.model(**obj_in)
            self.session.add(db_obj)
            
            # Flush changes to get generated ID and other DB-generated values
            await self.session.flush()
            
            # Explicitly commit changes to ensure they're persisted to the database
            await self.session.commit()
            
            # Refresh the object to ensure we have all the DB-generated data
            await self.session.refresh(db_obj)
            
            logger.debug(f"Created {self.model.__name__} with ID: {db_obj.id}")
            return db_obj
        except Exception as e:
            logger.error(f"Error creating {self.model.__name__}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # Rollback on error
            await self.session.rollback()
            raise

    async def update(self, id: IdType, obj_in: dict) -> Optional[ModelType]:
        """
        Update an existing record.
        
        Args:
            id: ID of the record to update (can be int or UUID)
            obj_in: Dictionary of values to update model with
            
        Returns:
            The updated model instance if found, else None
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.debug(f"Updating {self.model.__name__} with ID {id}")
            
            # Get current object first to check if it exists
            db_obj = await self.get(id)
            if not db_obj:
                logger.warning(f"{self.model.__name__} with ID {id} not found for update")
                return None
            
            logger.debug(f"Found {self.model.__name__} with ID {id}, updating with: {obj_in}")
            
            # Use SQLAlchemy's update statement instead of ORM-style updates
            # This is more efficient for SQLite and less prone to locking
            stmt = update(self.model).where(self.model.id == id).values(**obj_in)
            
            # Execute direct SQL update
            await self.session.execute(stmt)
            
            # Now explicitly flush and commit to ensure transaction is completed
            await self.session.flush()
            await self.session.commit()
            
            # Refresh to get updated data
            updated_obj = await self.get(id)
            
            logger.debug(f"Successfully updated {self.model.__name__} with ID {id}")
            return updated_obj
        except Exception as e:
            logger.error(f"Error updating {self.model.__name__} with ID {id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            await self.session.rollback()
            raise

    async def delete(self, id: IdType) -> bool:
        """
        Delete a record by ID.
        
        Args:
            id: ID of the record to delete (can be int or UUID)
            
        Returns:
            True if record was deleted, False if not found
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.debug(f"Deleting {self.model.__name__} with ID {id}")
            db_obj = await self.get(id)
            if db_obj:
                logger.debug(f"Found {self.model.__name__} with ID {id}, deleting")
                await self.session.delete(db_obj)
                
                # Always flush and commit to ensure transaction is completed
                await self.session.flush()
                await self.session.commit()
                
                logger.debug(f"Successfully deleted {self.model.__name__} with ID {id}")
                return True
            else:
                logger.warning(f"{self.model.__name__} with ID {id} not found for deletion")
                return False
        except Exception as e:
            logger.error(f"Error deleting {self.model.__name__} with ID {id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            await self.session.rollback()
            raise 