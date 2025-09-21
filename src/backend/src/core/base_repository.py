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

            # Don't commit here - let the session dependency handle it
            # This ensures consistent transaction management across all operations

            # Try to refresh the object to ensure we have all the DB-generated data
            # If the session has been committed elsewhere, this might fail
            try:
                await self.session.refresh(db_obj)
            except Exception as refresh_error:
                # If refresh fails (e.g., object detached), it's okay
                # The object still has the data from flush
                logger.debug(f"Could not refresh {self.model.__name__} (session may be closed): {refresh_error}")

            logger.debug(f"Created {self.model.__name__} with ID: {db_obj.id}")
            return db_obj
        except Exception as e:
            logger.error(f"Error creating {self.model.__name__}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # Rollback on error
            await self.session.rollback()
            raise

    async def add(self, obj: ModelType) -> ModelType:
        """
        Add an existing model object to the database.

        Args:
            obj: Model instance to add to database

        Returns:
            The added model instance with database-generated values
        """
        import logging
        logger = logging.getLogger(__name__)

        try:
            logger.debug(f"Adding {self.model.__name__} object to database")
            self.session.add(obj)

            # Flush changes to get generated ID and other DB-generated values
            await self.session.flush()

            # Don't commit here - let the session dependency handle it
            # This ensures consistent transaction management across all operations

            # Refresh the object to ensure we have all the DB-generated data
            await self.session.refresh(obj)

            logger.debug(f"Added {self.model.__name__} with ID: {obj.id}")
            return obj
        except Exception as e:
            logger.error(f"Error adding {self.model.__name__}: {str(e)}")
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

            # Only flush, don't commit - let the session dependency handle commits
            # This ensures consistent transaction management across all operations
            await self.session.flush()

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
                logger.info(f"[BASE REPO DELETE] Deleting {self.model.__name__} ID={id}")

                # Use SQL DELETE statement instead of ORM delete
                # This ensures the DELETE is actually executed
                from sqlalchemy import delete as sql_delete
                stmt = sql_delete(self.model).where(self.model.id == id)
                result = await self.session.execute(stmt)
                logger.info(f"[BASE REPO DELETE] Executed SQL DELETE for {self.model.__name__} ID={id}, rows affected: {result.rowcount}")

                # Flush to ensure the delete is sent to the database
                await self.session.flush()
                logger.info(f"[BASE REPO DELETE] Flushed session after SQL DELETE")

                logger.debug(f"Successfully deleted {self.model.__name__} with ID {id} (flushed)")
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